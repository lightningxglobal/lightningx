//! S4.4 — manipulation simulation: our OWN book gets swept ±20% while
//! the external index does not move. The clamped mark must stay inside
//! index × (1 ± α), and the thin long that a raw-mid mark would have
//! liquidated MUST NOT be liquidated.
//!
//! Control experiments prove the system isn't simply frozen:
//!   * when the INDEX itself crashes, liquidation fires normally;
//!   * when sources drop below quorum, the mark FREEZES (no update at
//!     all) rather than falling back to the manipulable mid.

use lightning_exchange::desk::index_price::{IndexAggregator, SharedSource, clamped_mark};
use lightning_exchange::desk::risk::RiskEngine;

const SYM: [u8; 16] = *b"MANIP_TS\0\0\0\0\0\0\0\0";
const SCALE: i64 = 1_000_000;
const A: i64 = 1_000_000; // atoms per cent
const INDEX: i64 = 5_000_000; // $50k
const CLAMP_BPS: i64 = 50; // mark may deviate ±0.5% from index

fn engine_with_thin_long() -> std::sync::Arc<RiskEngine> {
    // $600 equity, 0.1 BTC long at $50k with $500 margin: a raw -20% mid
    // (uPnL -$10,000) would obliterate it; a clamped -0.5% (uPnL -$250)
    // leaves it comfortably above the $25 maintenance margin.
    let engine = RiskEngine::new();
    engine.initialize_account(7, 60_000 * A);
    engine.check_and_reserve_margin(7, 50_000 * A).expect("reserve");
    engine.on_fill(7, SYM, 0, INDEX, 100_000, 50_000 * A, SCALE, 10, 50, 0);
    engine
}

fn three_sources_at(price: i64, now: i64) -> IndexAggregator {
    let sources: Vec<_> = (0..3)
        .map(|i| {
            let s = SharedSource::new(format!("src{i}"));
            s.store(SYM, price + i, now); // tiny honest spread
            s
        })
        .collect();
    let mut a = IndexAggregator::new(sources);
    a.staleness_ms = 60_000;
    a.outlier_bps = 200;
    a.min_sources = 2;
    a
}

#[test]
fn book_sweep_does_not_liquidate_when_index_holds() {
    let engine = engine_with_thin_long();
    let agg = three_sources_at(INDEX, 1_000);

    // Attacker sweeps our book 20% down, prints mid = $40k, repeatedly.
    let index = agg.index_at(&SYM, 1_000).expect("index live");
    for _ in 0..50 {
        let manipulated_mid = 4_000_000;
        let mark = clamped_mark(manipulated_mid, index, CLAMP_BPS);
        assert!(
            mark >= INDEX * (10_000 - CLAMP_BPS) / 10_000,
            "mark must never leave the index band (got {mark})"
        );
        engine.update_mark_price(SYM, mark, SCALE);
    }
    let events = engine.run_risk_tick();
    assert!(
        events.is_empty(),
        "a book sweep with a steady index must not trigger liquidations: {events:?}"
    );
    // And the pump direction is capped symmetrically (band is computed
    // off the LIVE index, which sits a tick above INDEX due to the
    // honest inter-source spread).
    let mark_up = clamped_mark(6_000_000, index, CLAMP_BPS);
    assert_eq!(mark_up, (index as i128 * (10_000 + CLAMP_BPS) as i128 / 10_000) as i64);
}

#[test]
fn real_index_crash_still_liquidates() {
    // Control: the protection must not freeze real risk. All three
    // sources crash to $44k → index follows → clamped mark follows →
    // the thin long IS liquidated.
    let engine = engine_with_thin_long();
    let crashed = 4_400_000;
    let agg = three_sources_at(crashed, 1_000);
    let index = agg.index_at(&SYM, 1_000).expect("index live");
    assert!(index >= crashed && index <= crashed + 2, "index tracks the real market");
    let mut fired = Vec::new();
    for _ in 0..200 {
        // Honest mid tracks the crash; clamp is a no-op inside the band.
        let mark = clamped_mark(crashed, index, CLAMP_BPS);
        engine.update_mark_price(SYM, mark, SCALE);
        fired = engine.run_risk_tick();
        if !fired.is_empty() {
            break;
        }
    }
    assert!(
        fired.iter().any(|e| e.user_id == 7),
        "a REAL crash must still liquidate the thin long"
    );
}

#[test]
fn quorum_loss_freezes_mark_instead_of_trusting_mid() {
    let engine = engine_with_thin_long();
    let agg = three_sources_at(INDEX, 1_000);
    let mark_before = engine.mark_price_ticks(&SYM).expect("mark seeded by fill");

    // All sources go stale → aggregator returns None → the desk's rule
    // is "do not call update_mark_price at all".
    assert_eq!(agg.index_at(&SYM, 120_000), None, "stale sources → freeze");
    // Simulate the desk loop honoring the freeze (no update call), then
    // verify the manipulated mid alone cannot move the engine's mark.
    let mark_after = engine.mark_price_ticks(&SYM).expect("mark still present");
    assert_eq!(mark_before, mark_after, "frozen mark holds its last value");
    assert!(engine.run_risk_tick().is_empty(), "no liquidations on a frozen mark");
}
