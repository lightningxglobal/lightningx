//! 纯撮合性能基准 - 测试极限TPS和延迟
//! 场景：完全匹配的买卖对，没有任何订单簿积压

use matching_engine::{MatchingEngine, PoolConfig, Order, Side, TimeInForce};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 纯撮合性能基准测试 ===\n");

    let config = PoolConfig::default();
    let mut engine = MatchingEngine::new(config)?;

    // 测试1: 最佳情况 - 每个买单立即被卖单成交，没有积压
    println!("【测试1】最佳情况：完全匹配的买卖对");
    let num_rounds = 10000;

    let start = Instant::now();
    let mut matched_count = 0;

    for i in 0..num_rounds {
        let price = 50000.0 + (i % 100) as f64;

        // 买单
        let buy = Order::new(
            i as u64 * 2,
            Side::Buy,
            price,
            10.0,
            TimeInForce::GTC,
            0,
        );
        engine.place_order(buy)?;

        // 卖单 - 以同样价格成交
        let sell = Order::new(
            i as u64 * 2 + 1,
            Side::Sell,
            price,
            10.0,
            TimeInForce::GTC,
            0,
        );

        if result.filled > 0.0 {
            matched_count += 1;
        }
    }

    let duration = start.elapsed();
    let total_ops = num_rounds as f64 * 2.0; // 买+卖
    let tps = total_ops / duration.as_secs_f64();
    let latency_us = (duration.as_nanos() as f64 / total_ops) / 1000.0;

    println!("  成交对数: {}", matched_count);
    println!("  总操作数: {:.0}", total_ops);
    println!("  总耗时: {:.3}ms", duration.as_secs_f64() * 1000.0);
    println!("  TPS: {:.0} (K={:.1})", tps, tps / 1000.0);
    println!("  平均延迟: {:.3}μs", latency_us);
    println!("  单操作延迟: {:.0}ns\n", latency_us * 1000.0);

    // 测试2: 成交延迟 - 只测量卖单（因为买单本身很快）
    println!("【测试2】纯成交延迟分析");
    engine = MatchingEngine::new(PoolConfig::default())?;

    let num_rounds = 10000;
    let mut latencies = Vec::with_capacity(num_rounds);

    // 先放置所有买单
    for i in 0..num_rounds {
        let price = 50000.0 + (i % 100) as f64;
        let buy = Order::new(i as u64 * 2, Side::Buy, price, 10.0, TimeInForce::GTC, 0);
        engine.place_order(buy)?;
    }

    // 然后测量卖单延迟
    for i in 0..num_rounds {
        let price = 50000.0 + (i % 100) as f64;
        let sell = Order::new(i as u64 * 2 + 1, Side::Sell, price, 10.0, TimeInForce::GTC, 0);

        let start_op = Instant::now();
        engine.place_order(sell)?;
        let elapsed = start_op.elapsed();

        latencies.push(elapsed.as_nanos() as u64);
    }

    latencies.sort();
    let min = latencies[0];
    let max = latencies[latencies.len() - 1];
    let avg = latencies.iter().sum::<u64>() as f64 / latencies.len() as f64;
    let p50 = latencies[latencies.len() / 2];
    let p99 = latencies[(latencies.len() * 99) / 100];
    let p999 = latencies[(latencies.len() * 999) / 1000];

    println!("  成交延迟统计 (纳秒):");
    println!("    Min:   {} ns ({:.3}μs)", min, min as f64 / 1000.0);
    println!("    P50:   {} ns ({:.3}μs)", p50, p50 as f64 / 1000.0);
    println!("    P99:   {} ns ({:.3}μs)", p99, p99 as f64 / 1000.0);
    println!("    P99.9: {} ns ({:.3}μs)", p999, p999 as f64 / 1000.0);
    println!("    Avg:   {:.0} ns ({:.3}μs)", avg, avg / 1000.0);
    println!("    Max:   {} ns ({:.3}μs)\n", max, max as f64 / 1000.0);

    println!("【性能目标对标】");
    println!("  目标 TPS:  7,000,000 (7M)");
    println!("  目标延迟: 0.3μs (300ns)");

    Ok(())
}
