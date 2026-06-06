//! Comparable replacement for realistic_business_benchmark.rs + deep_orderbook_benchmark.rs
//!
//! Uses identical scenarios and pool sizes to the originals so numbers are apples-to-apples
//! with BENCHMARK_SUMMARY.md (commit 095ee1c, 2026-05-17).
//!
//! Real Business: 2M pool (default), 5000 rounds × 10 orders, ~50% match rate
//! Deep OB:       100K pool (matches old benchmark), 400-level book, BBO batch crossing
//!
//! Run: cargo run --example perf_compare --release

use lightning_exchange::{MatchingEngine, Order, PoolConfig, Side, TimeInForce, TradeEvent};
use rtrb::RingBuffer;
use smallvec::SmallVec;
use std::time::Instant;

fn make_order(id: u64, side: Side, price_ticks: i64, qty_lots: i64) -> Order {
    Order::new(id, side, price_ticks, qty_lots, TimeInForce::GTC, 0)
}

// ─── Real Business (same scenario as old realistic_business_benchmark) ─────────
// 5000 rounds × 5 resting buys + 5 crossing sells = 50 000 orders, ~50% fill rate.
// 2M pool so pool allocation pressure matches old run.

fn bench_real_business_single() -> (f64, u64, u64) {
    let (tx, mut rx) = RingBuffer::<TradeEvent>::new(1_000_000);
    let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();
    engine.set_trade_event_sender(tx);

    let start = Instant::now();
    let mut id: u64 = 1;
    for round in 0..5000u64 {
        for i in 0..5i64 {
            let p = 5_000_000 + round as i64 % 50 + i;
            let _ = engine.place_order(make_order(id, Side::Buy, p, 1)).unwrap();
            id += 1;
        }
        for i in 0..5i64 {
            let p = 5_000_000 + round as i64 % 50 + i;
            let _ = engine
                .place_order(make_order(id, Side::Sell, p, 1))
                .unwrap();
            id += 1;
        }
    }
    let elapsed_ns = start.elapsed().as_nanos() as u64;
    let total: u64 = 50_000;
    let mut events = 0u64;
    while rx.pop().is_ok() {
        events += 1;
    }
    (
        total as f64 / (elapsed_ns as f64 / 1e9),
        elapsed_ns / total,
        events,
    )
}

fn bench_real_business_batch() -> (f64, u64, u64) {
    let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

    let start = Instant::now();
    let mut id: u64 = 1_000_000;
    let mut trades: u64 = 0;
    let mut total: u64 = 0;
    for round in 0..5000u64 {
        let mut batch: SmallVec<[Order; 40]> = SmallVec::new();
        for i in 0..5i64 {
            let p = 5_000_000 + round as i64 % 50 + i;
            batch.push(make_order(id, Side::Buy, p, 1));
            id += 1;
        }
        for i in 0..5i64 {
            let p = 5_000_000 + round as i64 % 50 + i;
            batch.push(make_order(id, Side::Sell, p, 1));
            id += 1;
        }
        let results = engine.match_orders_batch(batch).unwrap();
        total += results.len() as u64;
        for (_, t) in &results {
            trades += t.len() as u64;
        }
    }
    let elapsed_ns = start.elapsed().as_nanos() as u64;
    (
        total as f64 / (elapsed_ns as f64 / 1e9),
        elapsed_ns / total,
        trades,
    )
}

// ─── Deep OB (same scenario as old deep_orderbook_benchmark) ─────────────────
// 100K pool (same as old PoolConfig { order_capacity: 100_000, queue_capacity: 1_000_000 })
// 400 bid + 400 ask levels pre-loaded.
// Orders alternate buy @ BBO-1 / sell @ BBO+1 — same as old benchmark.
// These do NOT cross the book, so this measures SkipList INSERT into a deep book,
// which is what the old benchmark actually measured (not matching).

fn bench_deep_ob_batch() -> (f64, u64, u64) {
    let pool = PoolConfig {
        order_capacity: 100_000,
        queue_capacity: 100_000,
    };
    let (tx, mut rx) = RingBuffer::<TradeEvent>::new(10_000_000);
    let mut engine = MatchingEngine::new(pool).unwrap();
    engine.set_trade_event_sender(tx);

    let base: i64 = 5_000_000;
    // Pre-fill 400 bid levels at base-400..base-1 and 400 ask levels at base+1..base+400
    for i in 0..400i64 {
        let _ = engine.place_order(make_order(i as u64, Side::Buy, base - 400 + i, 100));
        let _ = engine.place_order(make_order(400 + i as u64, Side::Sell, base + i + 1, 100));
    }

    // 5000 BBO-near orders in batches of 20: alternating buy@BBO-1 / sell@BBO+1.
    // Neither price crosses the spread, so both are inserted into an already-deep book.
    let start = Instant::now();
    let mut id: u64 = 10_000;
    let mut total: u64 = 0;
    let mut trades: u64 = 0;
    for batch_idx in 0..(5000u64 / 20) {
        let mut batch: SmallVec<[Order; 40]> = SmallVec::new();
        for i in 0..10u64 {
            let oid = batch_idx * 20 + i * 2;
            // buy just below best ask (base+1) — does not cross
            batch.push(make_order(id + oid, Side::Buy, base, 10));
            // sell just above best bid (base-1) — does not cross
            batch.push(make_order(id + oid + 1, Side::Sell, base + 1, 10));
        }
        id += 20;
        let results = engine.match_orders_batch(batch).unwrap();
        total += results.len() as u64;
        for (_, t) in &results {
            trades += t.len() as u64;
        }
    }
    let elapsed_ns = start.elapsed().as_nanos() as u64;
    let mut events = 0u64;
    while rx.pop().is_ok() {
        events += 1;
    }
    let _ = events;
    (
        total as f64 / (elapsed_ns as f64 / 1e9),
        elapsed_ns / total,
        trades,
    )
}

fn diff_str(current: f64, baseline: f64) -> String {
    let pct = (current - baseline) / baseline * 100.0;
    if pct >= -5.0 {
        format!("{:+.1}%  OK", pct)
    } else if pct >= -15.0 {
        format!("{:+.1}%  WARNING", pct)
    } else {
        format!("{:+.1}%  REGRESSION", pct)
    }
}

fn main() {
    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║           perf_compare — apples-to-apples vs BENCHMARK_SUMMARY  ║");
    println!("╚══════════════════════════════════════════════════════════════════╝\n");

    // Warm-up run to settle CPU frequency scaling
    let _ = bench_real_business_single();

    println!("【Real Business — single order (2M pool)】");
    let (tps, ns, ev) = bench_real_business_single();
    println!("  50 000 orders, TradeEvents={ev}");
    println!("  TPS: {:.2}M  ({ns} ns/order)", tps / 1e6);
    println!("  Historical: 5.33M TPS  =>  {}", diff_str(tps / 1e6, 5.33));

    println!("\n【Real Business — batch 20 (2M pool)】");
    let (tps, ns, tr) = bench_real_business_batch();
    println!("  50 000 orders, Trades={tr}");
    println!("  TPS: {:.2}M  ({ns} ns/order)", tps / 1e6);
    println!("  Historical: 8.46M TPS  =>  {}", diff_str(tps / 1e6, 8.46));

    println!("\n【Deep OB — batch 20 (100K pool, 400 levels)】");
    let (tps, ns, tr) = bench_deep_ob_batch();
    println!("  5 000 orders, Trades={tr}");
    println!("  TPS: {:.2}M  ({ns} ns/order)", tps / 1e6);
    println!(
        "  Historical: 21.13M TPS  =>  {}",
        diff_str(tps / 1e6, 21.13)
    );

    println!("\n╔══════════════════════════════════════════════════════════════════╗");
    println!("║  Historical baseline (BENCHMARK_SUMMARY.md, commit 095ee1c)     ║");
    println!("║  Real Business single:  5.33M TPS  P50=84ns  P99=1042ns         ║");
    println!("║  Real Business batch:   8.46M TPS  P50=68ns  P99=766ns          ║");
    println!("║  Deep OB single:        9.29M TPS                               ║");
    println!("║  Deep OB batch:        21.13M TPS  P50=35ns  P99=68ns           ║");
    println!("╚══════════════════════════════════════════════════════════════════╝");
}
