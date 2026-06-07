//! S2.4 — the swap conservation law, exact in atoms:
//!
//!   Σ(final equity) + insurance_fund = Σ(initial equity)
//!
//! Money in a perps venue never appears or disappears: every fill books
//! equal-and-opposite PnL legs, and a liquidation's fill-vs-settlement
//! spread moves to the insurance fund instead of vanishing. With
//! notional_scale = 1e6 (BTC_USDT) the ×1e6 atoms factor cancels the
//! division exactly, so the law holds to the atom — no tolerance.
//!
//! Drives a seeded pseudo-random open/close sequence across a pool of
//! users (every trade hits both sides of the engine, like the desk
//! does for taker+maker), forces a liquidation mid-run, then closes
//! everything and checks the books. Pure engine-level: no PG, fast.

use lightning_exchange::desk::risk::{PositionSide, RiskEngine};
use std::sync::atomic::Ordering;

const SYM: [u8; 16] = *b"ZSUM_TST\0\0\0\0\0\0\0\0";
const SCALE: i64 = 1_000_000; // exactness depends on scale == ATOMS_PER_CENT
const LEV: u8 = 10;
const MBPS: i64 = 50;
const A: i64 = 1_000_000; // atoms per cent (readability)
const USERS: i64 = 6;
const ROUNDS: usize = 300;

struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 16
    }
    fn range(&mut self, lo: i64, hi: i64) -> i64 {
        lo + (self.next() % (hi - lo + 1) as u64) as i64
    }
}

fn margin_for(price_ticks: i64, qty_lots: i64) -> i64 {
    let notional = lightning_exchange::desk::risk::calc::calc_notional_atoms(
        price_ticks, qty_lots, SCALE,
    );
    lightning_exchange::desk::risk::calc::calc_initial_margin_atoms(notional, LEV)
}

/// One matched trade: buyer and seller each get their on_fill, exactly
/// like desk taker+maker accounting. liq_price != 0 marks `liq_user`'s
/// side as a forced liquidation (spread → fund).
#[allow(clippy::too_many_arguments)]
fn matched_trade(
    engine: &RiskEngine,
    buyer: i64,
    seller: i64,
    price: i64,
    qty: i64,
    liq_user: i64,
    liq_price: i64,
) {
    let m = margin_for(price, qty);
    for (user, side) in [(buyer, 0u8), (seller, 1u8)] {
        let lp = if user == liq_user { liq_price } else { 0 };
        // Liquidation orders carry no reserved margin (close side).
        let fill_margin = if lp != 0 { 0 } else { m };
        if lp == 0 {
            engine
                .check_and_reserve_margin(user, m)
                .expect("reserve — accounts funded amply for the test");
        }
        engine.on_fill(user, SYM, side, price, qty, fill_margin, SCALE, LEV, MBPS, lp);
    }
}

fn total_equity(engine: &RiskEngine) -> i64 {
    engine.accounts.iter().map(|a| a.equity).sum()
}

#[test]
fn equity_plus_insurance_fund_is_conserved_exactly() {
    let engine = RiskEngine::new();
    let mut rng = Lcg(0x5EED_25_4D_0001);

    // Users 1..=USERS trade; user 99 is the deep-pocketed liquidation
    // counterparty (it absorbs the forced close).
    const WHALE: i64 = 99;
    let initial_each: i64 = 10_000_000 * A; // $100k
    for u in 1..=USERS {
        engine.initialize_account(u, initial_each);
    }
    engine.initialize_account(WHALE, 100 * initial_each);
    let e0 = total_equity(&engine);

    // ── Random matched flow ─────────────────────────────────────────────
    for _ in 0..ROUNDS {
        let buyer = rng.range(1, USERS);
        let mut seller = rng.range(1, USERS);
        if seller == buyer {
            seller = if seller == USERS { 1 } else { seller + 1 };
        }
        let price = rng.range(4_500_000, 5_500_000); // $45k–$55k
        let qty = rng.range(1_000, 50_000); // 0.001–0.05 BTC
        matched_trade(&engine, buyer, seller, price, qty, 0, 0);
        // Occasionally move the mark so uPnL/equity churn too.
        if rng.range(0, 4) == 0 {
            engine.update_mark_price(SYM, price, SCALE);
        }
    }

    // ── Forced liquidation: thin user gets squeezed ─────────────────────
    const VICTIM: i64 = 1000;
    engine.initialize_account(VICTIM, 60_000 * A); // $600
    let entry = 5_000_000;
    let vqty = 100_000; // 0.1 BTC, margin $500 at 10x
    // Open: victim long vs whale short.
    matched_trade(&engine, VICTIM, WHALE, entry, vqty, 0, 0);
    let e_with_victim = e0 + 60_000 * A;

    // Crash the mark until the tick demands liquidation.
    let mut events = Vec::new();
    for _ in 0..200 {
        engine.update_mark_price(SYM, 4_400_000, SCALE);
        events = engine.run_risk_tick();
        if !events.is_empty() {
            break;
        }
    }
    assert!(!events.is_empty(), "victim must get liquidated");
    let ev = events.iter().find(|e| e.user_id == VICTIM).expect("victim event");
    assert_eq!(ev.side, PositionSide::Long);

    // Forced close: the long's liquidation order SELLS with limit
    // liq_price, so the actual fill is at or ABOVE it; the user is
    // settled at liq_price (worse for them) and the improvement is the
    // exchange's spread → insurance fund.
    let fill = ev.liq_price_ticks + 20_000; // fills $200 above settlement
    matched_trade(&engine, WHALE, VICTIM, fill, ev.qty_lots, VICTIM, ev.liq_price_ticks);
    assert!(
        engine.insurance_fund() > 0,
        "liquidation spread must move money INTO the fund (got {})",
        engine.insurance_fund()
    );

    // ── Close every remaining position (zero out exposure) ─────────────
    let open: Vec<(i64, PositionSide, i64)> = engine
        .positions
        .iter()
        .map(|e| (e.key().0, e.value().side, e.value().qty_lots))
        .collect();
    for (user, side, qty) in open {
        // The whale takes the other side at the current mark.
        let price = engine.mark_price_ticks(&SYM).unwrap_or(5_000_000);
        match side {
            PositionSide::Long => matched_trade(&engine, WHALE, user, price, qty, 0, 0),
            PositionSide::Short => matched_trade(&engine, user, WHALE, price, qty, 0, 0),
        }
    }
    // (The whale may end up with offsetting longs+shorts that net flat
    // through its own position; whatever残 remains, equity already
    // includes its uPnL at the current mark, which is what we sum.)
    engine.update_mark_price(SYM, engine.mark_price_ticks(&SYM).unwrap(), SCALE);

    // ── The law ─────────────────────────────────────────────────────────
    let e_final = total_equity(&engine);
    let fund = engine.insurance_fund();
    assert_eq!(
        e_final + fund,
        e_with_victim,
        "Σequity + fund must equal Σinitial deposits EXACTLY \
         (final {e_final} + fund {fund} vs initial {e_with_victim})"
    );

    // Sanity: margin reservations all returned.
    let order_margin: i64 = engine
        .accounts
        .iter()
        .map(|a| a.order_margin.load(Ordering::Relaxed))
        .sum();
    assert_eq!(order_margin, 0, "no stranded order margin");
}
