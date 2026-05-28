//! Performance baseline: three scenarios, all with ~5% fill rate.
//!
//! Design:
//!   - Pre-fill N bid levels (BASE-N..BASE-1) + N ask levels (BASE+1..BASE+N), qty=10_000 each.
//!   - Main loop: every 20th order is a buy aggressor at BASE+1 (crosses best ask).
//!     Others alternate resting buy (BASE-300..BASE-399) / sell (BASE+301..BASE+400).
//!   - Aggressors consume 1 lot each from pre-fill ask@BASE+1 (10_000 lots → never depleted).
//!
//! Scenarios:
//!   Single order   2M pool   50_000 orders  ~2_500 trades (~5%)
//!   Batch-20       2M pool   50_000 orders  ~2_500 trades (~5%)
//!   Deep OB Batch  100K pool  5_000 orders  ~  250 trades (~5%)  (400-level pre-fill)
//!
//! Run: cargo run --example bench_baseline --release

use lightning_exchange::{MatchingEngine, PoolConfig, Order, Side, TimeInForce, TradeEvent};
use rtrb::RingBuffer;
use smallvec::SmallVec;
use std::time::Instant;

const BASE: i64 = 5_000_000;

fn make_order(id: u64, side: Side, price_ticks: i64, qty_lots: i64) -> Order {
    Order::new(id, side, price_ticks, qty_lots, TimeInForce::GTC, 0)
}

/// Pre-fill `levels` bid + ask levels around BASE. Returns next free order id.
fn prefill(engine: &mut MatchingEngine, levels: i64) -> u64 {
    for i in 0..levels {
        let _ = engine.place_order(make_order(i as u64 * 2,     Side::Buy,  BASE - levels + i, 10_000));
        let _ = engine.place_order(make_order(i as u64 * 2 + 1, Side::Sell, BASE + 1 + i,      10_000));
    }
    (levels * 2) as u64
}

/// Order for the seq-th slot in the main measurement loop.
/// seq % 20 == 19  → aggressor (buy@BASE+1, crosses best ask, ~5%)
/// seq % 2 == 0    → resting buy  (BASE-300..BASE-399)
/// else            → resting sell (BASE+301..BASE+400)
fn order_for_seq(id: u64, seq: u64) -> Order {
    if seq % 20 == 19 {
        make_order(id, Side::Buy, BASE + 1, 1)
    } else if seq % 2 == 0 {
        make_order(id, Side::Buy, BASE - 300 - (seq as i64 % 100), 10)
    } else {
        make_order(id, Side::Sell, BASE + 301 + (seq as i64 % 100), 10)
    }
}

fn bench_single(pool: PoolConfig, total: u64, pre_levels: i64) -> (f64, u64, u64) {
    let (tx, mut rx) = RingBuffer::<TradeEvent>::new(200_000);
    let mut engine = MatchingEngine::new(pool).unwrap();
    engine.set_trade_event_sender(tx);
    let mut id = prefill(&mut engine, pre_levels);

    let start = Instant::now();
    for seq in 0..total {
        let _ = engine.place_order(order_for_seq(id, seq)).unwrap();
        id += 1;
    }
    let elapsed_ns = start.elapsed().as_nanos() as u64;

    let mut events = 0u64;
    while rx.pop().is_ok() { events += 1; }
    (total as f64 / (elapsed_ns as f64 / 1e9), elapsed_ns / total, events)
}

fn bench_batch(pool: PoolConfig, total: u64, batch_size: usize, pre_levels: i64) -> (f64, u64, u64) {
    let mut engine = MatchingEngine::new(pool).unwrap();
    let mut id = prefill(&mut engine, pre_levels);

    let start = Instant::now();
    let mut trades = 0u64;
    for b in 0..(total / batch_size as u64) {
        let mut batch: SmallVec<[Order; 40]> = SmallVec::new();
        for i in 0..batch_size as u64 {
            batch.push(order_for_seq(id, b * batch_size as u64 + i));
            id += 1;
        }
        let results = engine.match_orders_batch(batch).unwrap();
        for (_, t) in &results {
            trades += t.len() as u64;
        }
    }
    let elapsed_ns = start.elapsed().as_nanos() as u64;
    (total as f64 / (elapsed_ns as f64 / 1e9), elapsed_ns / total, trades)
}

fn main() {
    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║        bench_baseline — ~5% fill rate, new performance baseline  ║");
    println!("╚══════════════════════════════════════════════════════════════════╝\n");

    println!("【Single order — 2M pool, ~5% fill】");
    let _ = bench_single(PoolConfig::default(), 50_000, 500);  // warm-up
    let (tps, ns, ev) = bench_single(PoolConfig::default(), 50_000, 500);
    println!("  50 000 orders, TradeEvents={ev} ({:.1}%)", ev as f64 / 50_000.0 * 100.0);
    println!("  TPS: {:.2}M  ({ns} ns/order)", tps / 1e6);

    println!("\n【Batch-20 — 2M pool, ~5% fill】");
    let _ = bench_batch(PoolConfig::default(), 50_000, 20, 500);  // warm-up
    let (tps, ns, tr) = bench_batch(PoolConfig::default(), 50_000, 20, 500);
    println!("  50 000 orders, Trades={tr} ({:.1}%)", tr as f64 / 50_000.0 * 100.0);
    println!("  TPS: {:.2}M  ({ns} ns/order)", tps / 1e6);

    println!("\n【Deep OB Batch-20 — 100K pool, 400 levels, ~5% fill】");
    let _ = bench_batch(PoolConfig { order_capacity: 100_000, queue_capacity: 100_000 }, 5_000, 20, 400);  // warm-up
    let (tps, ns, tr) = bench_batch(
        PoolConfig { order_capacity: 100_000, queue_capacity: 100_000 },
        5_000, 20, 400,
    );
    println!("  5 000 orders, Trades={tr} ({:.1}%)", tr as f64 / 5_000.0 * 100.0);
    println!("  TPS: {:.2}M  ({ns} ns/order)", tps / 1e6);

    println!();
    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║  Record these numbers in BENCHMARK_SUMMARY.md as the new         ║");
    println!("║  baseline (5% fill, commit+date).                                ║");
    println!("╚══════════════════════════════════════════════════════════════════╝");
}
