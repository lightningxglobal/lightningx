/// 简化性能测试 - 最纯净的匹配场景
use lightning_exchange::{MatchingEngine, PoolConfig, Order, Side, TimeInForce};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 简化性能测试 ===\n");

    let mut engine = MatchingEngine::new(PoolConfig::default())?;

    // Test 1: Simple matching - 1 buy, 1 sell, repeat
    println!("Test 1: 简单配对 (买单+卖单)");
    let num_rounds = 100_000;
    let base_price = 50000.0;

    let start = Instant::now();
    for round in 0..num_rounds {
        // Buy order
        let buy = Order::new(
            round * 2,
            Side::Buy,
            base_price,
            10.0,
            TimeInForce::GTC,
            0,
        );
        engine.place_order(buy)?;

        // Sell order - immediate match
        let sell = Order::new(
            round * 2 + 1,
            Side::Sell,
            base_price,
            10.0,
            TimeInForce::GTC,
            0,
        );
        engine.place_order(sell)?;
    }
    let elapsed = start.elapsed();

    let tps = (num_rounds as f64 * 2.0) / elapsed.as_secs_f64();
    let per_order_ns = (elapsed.as_nanos() as f64 / (num_rounds * 2) as f64);
    println!("  TPS: {:.2}M", tps / 1_000_000.0);
    println!("  Per order: {:.0}ns", per_order_ns);
    println!("  Time: {:.3}s\n", elapsed.as_secs_f64());

    // Test 2: Orders that don't match (insert only)
    println!("Test 2: 仅插入 (不成交)");
    engine = MatchingEngine::new(PoolConfig::default())?;
    let num_orders = 100_000;

    let start = Instant::now();
    for i in 0..num_orders {
        let price = base_price - (i as f64 * 0.001); // All different prices
        let order = Order::new(
            i as u64,
            Side::Buy,
            price,
            10.0,
            TimeInForce::GTC,
            0,
        );
        engine.place_order(order)?;
    }
    let elapsed = start.elapsed();

    let tps = (num_orders as f64) / elapsed.as_secs_f64();
    let per_order_ns = (elapsed.as_nanos() as f64 / num_orders as f64);
    println!("  TPS: {:.2}M", tps / 1_000_000.0);
    println!("  Per order: {:.0}ns", per_order_ns);
    println!("  Time: {:.3}s\n", elapsed.as_secs_f64());

    Ok(())
}
