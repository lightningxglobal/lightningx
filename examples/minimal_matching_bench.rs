//! 最小化匹配性能基准 - 只测核心匹配算法，零额外开销
//!
//! 模拟历史基准的条件：
//! - 没有event publishing
//! - 没有market data
//! - 没有trading engine wrapper
//! - 只有raw matching engine

use lightning_exchange::{MatchingEngine, PoolConfig, Order, Side, TimeInForce};
use std::time::Instant;
use hdrhistogram::Histogram;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔═══════════════════════════════════════════════════════════════════╗");
    println!("║            最小化匹配基准 - 核心算法性能测试                      ║");
    println!("╚═══════════════════════════════════════════════════════════════════╝\n");

    println!("【条件】");
    println!("  ✓ 没有event publishing");
    println!("  ✓ 没有market data snapshots");
    println!("  ✓ 没有TradingEngine wrapper");
    println!("  ✓ 纯MatchingEngine + SkipList\n");

    let num_rounds = 10000;  // 增加轮次来获得更精确的测量

    // 预热
    {
        let mut engine = MatchingEngine::new(PoolConfig::default())?;
        for i in 0..1000 {
            let order = Order::new(i, Side::Buy, 50000.0, 10.0, TimeInForce::GTC, 0);
            let _ = engine.place_order(order);
        }
    }

    // 实际测试
    let mut engine = MatchingEngine::new(PoolConfig::default())?;
    let mut latencies = Histogram::<u64>::new(3)?;
    let base_price = 50000.0;

    let total_start = Instant::now();

    for round in 0..num_rounds {
        // 5 buy orders
        for i in 0..5 {
            let price = base_price - 2.0 + i as f64;
            let qty = 10.0;
            let start = Instant::now();
            let order = Order::new(round as u64 * 1000 + i as u64, Side::Buy, price, qty, TimeInForce::GTC, 0);
            let _ = engine.place_order(order)?;
            latencies.record(start.elapsed().as_nanos() as u64)?;
        }

        // 5 sell orders
        for i in 0..5 {
            let price = base_price + i as f64;
            let qty = 10.0;
            let start = Instant::now();
            let order = Order::new(round as u64 * 1000 + 500 + i as u64, Side::Sell, price, qty, TimeInForce::GTC, 0);
            let _ = engine.place_order(order)?;
            latencies.record(start.elapsed().as_nanos() as u64)?;
        }
    }

    let total_elapsed = total_start.elapsed();
    let total_orders = num_rounds as f64 * 10.0;
    let tps = total_orders / total_elapsed.as_secs_f64();

    println!("╔═══════════════════════════════════════════════════════════════════╗");
    println!("║                          性能结果                                ║");
    println!("╠═══════════════════════════════════════════════════════════════════╣");
    println!("║ TPS:          {:.2}M", tps / 1_000_000.0);
    println!("║ P50 延迟:     {:.0} ns ({:.2} μs)", latencies.value_at_percentile(50.0) as f64, latencies.value_at_percentile(50.0) as f64 / 1000.0);
    println!("║ P99 延迟:     {:.0} ns ({:.2} μs)", latencies.value_at_percentile(99.0) as f64, latencies.value_at_percentile(99.0) as f64 / 1000.0);
    println!("║ 总订单数:     {:.0}", total_orders);
    println!("║ 总耗时:       {:.3}s", total_elapsed.as_secs_f64());
    println!("╠═══════════════════════════════════════════════════════════════════╣");
    println!("║ vs 历史6.46M: {:+.1}%", (tps / 1_000_000.0 - 6.46) / 6.46 * 100.0);
    println!("╚═══════════════════════════════════════════════════════════════════╝\n");

    Ok(())
}
