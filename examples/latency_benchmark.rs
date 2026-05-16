//! 延迟基准测试 - 单订单 vs 批量订单

use matching_engine::{MatchingEngine, PoolConfig, Order, Side, TimeInForce, TradeEvent};
use std::time::Instant;
use rtrb::RingBuffer;
use smallvec::SmallVec;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 延迟基准测试 ===\n");

    let num_rounds = 10000;
    let price_range = 100;

    // 测试1: 单订单延迟
    println!("【测试1】单订单延迟");
    let mut engine = MatchingEngine::new(PoolConfig::default())?;
    let (tx, _rx) = RingBuffer::<TradeEvent>::new(1_000_000);
    engine.set_trade_event_sender(tx);

    let mut latencies: Vec<u128> = Vec::with_capacity(num_rounds * 2);

    for i in 0..num_rounds {
        let price = 50000.0 + (i % price_range) as f64;

        let buy = Order::new(i as u64 * 2, Side::Buy, price, 10.0, TimeInForce::GTC, 0);
        let start = std::time::Instant::now();
        let mut affected_makers = SmallVec::<[u64; 64]>::new();
        engine.place_order(buy, &mut affected_makers)?;
        latencies.push(start.elapsed().as_nanos());

        let sell = Order::new(i as u64 * 2 + 1, Side::Sell, price, 10.0, TimeInForce::GTC, 0);
        let start = std::time::Instant::now();
        let mut affected_makers = SmallVec::<[u64; 64]>::new();
        engine.place_order(sell, &mut affected_makers)?;
        latencies.push(start.elapsed().as_nanos());
    }

    latencies.sort();
    let p50 = latencies[latencies.len() / 2];
    let p95 = latencies[(latencies.len() * 95) / 100];
    let p99 = latencies[(latencies.len() * 99) / 100];
    let avg = latencies.iter().sum::<u128>() / latencies.len() as u128;

    println!("  平均延迟: {:.0}ns", avg);
    println!("  P50延迟: {:.0}ns", p50);
    println!("  P95延迟: {:.0}ns", p95);
    println!("  P99延迟: {:.0}ns\n", p99);

    // 测试2: 批量订单延迟
    println!("【测试2】批量订单延迟 (20个/批)");
    let mut engine = MatchingEngine::new(PoolConfig::default())?;
    let (tx, _rx) = RingBuffer::<TradeEvent>::new(1_000_000);
    engine.set_trade_event_sender(tx);

    let mut batch_latencies: Vec<u128> = Vec::with_capacity(num_rounds);

    for batch_idx in 0..num_rounds {
        let mut batch: SmallVec<[Order; 20]> = SmallVec::new();

        for i in 0..10 {
            let order_idx = batch_idx * 10 + i;
            let price = 50000.0 + (order_idx % price_range) as f64;

            let buy = Order::new(order_idx as u64 * 2, Side::Buy, price, 10.0, TimeInForce::GTC, 0);
            batch.push(buy);

            let sell = Order::new(order_idx as u64 * 2 + 1, Side::Sell, price, 10.0, TimeInForce::GTC, 0);
            batch.push(sell);
        }

        let start = Instant::now();
        let _ = engine.match_orders_batch(batch)?;
        let batch_time = start.elapsed().as_nanos();

        // 记录每个订单的平均延迟 (20个订单的总时间 / 20)
        batch_latencies.push(batch_time / 20);
    }

    batch_latencies.sort();
    let b_p50 = batch_latencies[batch_latencies.len() / 2];
    let b_p95 = batch_latencies[(batch_latencies.len() * 95) / 100];
    let b_p99 = batch_latencies[(batch_latencies.len() * 99) / 100];
    let b_avg = batch_latencies.iter().sum::<u128>() / batch_latencies.len() as u128;

    println!("  平均延迟: {:.0}ns (每订单)", b_avg);
    println!("  P50延迟: {:.0}ns", b_p50);
    println!("  P95延迟: {:.0}ns", b_p95);
    println!("  P99延迟: {:.0}ns\n", b_p99);

    // 对比
    println!("【延迟对比】");
    println!("  指标     | 单订单   | 批量     | 改善");
    println!("  ---------|----------|----------|----------");
    println!("  平均延迟 | {:.0}ns  | {:.0}ns  | {:.1}%", avg, b_avg, ((avg as f64 - b_avg as f64) / avg as f64) * 100.0);
    println!("  P50延迟  | {:.0}ns  | {:.0}ns  | {:.1}%", p50, b_p50, ((p50 as f64 - b_p50 as f64) / p50 as f64) * 100.0);
    println!("  P95延迟  | {:.0}ns  | {:.0}ns  | {:.1}%", p95, b_p95, ((p95 as f64 - b_p95 as f64) / p95 as f64) * 100.0);
    println!("  P99延迟  | {:.0}ns  | {:.0}ns  | {:.1}%", p99, b_p99, ((p99 as f64 - b_p99 as f64) / p99 as f64) * 100.0);

    println!("\n【目标对比】");
    println!("  单订单延迟: {:.0}ns vs 目标 300ns", avg);
    println!("  批量延迟:   {:.0}ns vs 目标 300ns", b_avg);

    Ok(())
}
