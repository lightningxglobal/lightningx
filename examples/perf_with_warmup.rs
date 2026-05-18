//! 带pool预热的性能测试
use lightning_exchange::{MatchingEngine, PoolConfig, Order, Side, TimeInForce};
use std::time::Instant;

fn run_with_warmup() -> f64 {
    let num_rounds = 10000;
    let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();
    let base_price = 50000.0;

    // 预热：充分使用pool和内存
    println!("  Warming up...");
    for i in 0..2000 {
        let order = Order::new(i, Side::Buy, base_price + i as f64 * 0.1, 10.0, TimeInForce::GTC, 0);
        let _ = engine.place_order(order);
    }

    // 清空簿来reset
    engine = MatchingEngine::new(PoolConfig::default()).unwrap();

    // 实际测试
    let total_start = Instant::now();

    for round in 0..num_rounds {
        for i in 0..5 {
            let price = base_price - 2.0 + i as f64;
            let order = Order::new(round as u64 * 1000 + i as u64, Side::Buy, price, 10.0, TimeInForce::GTC, 0);
            let _ = engine.place_order(order);
        }

        for i in 0..5 {
            let price = base_price + i as f64;
            let order = Order::new(round as u64 * 1000 + 500 + i as u64, Side::Sell, price, 10.0, TimeInForce::GTC, 0);
            let _ = engine.place_order(order);
        }
    }

    let total_elapsed = total_start.elapsed();
    (num_rounds as f64 * 10.0) / total_elapsed.as_secs_f64()
}

fn main() {
    println!("带预热的benchmark - 5轮\n");

    let mut results = Vec::new();
    for run in 1..=5 {
        let tps = run_with_warmup();
        results.push(tps);
        println!("Run {}: {:.2}M TPS", run, tps / 1_000_000.0);
    }

    let mean = results.iter().sum::<f64>() / results.len() as f64;
    let max = results.iter().cloned().fold(0.0, f64::max);

    println!("\n╔════════════════════════════════════╗");
    println!("║ Mean:    {:.2}M TPS", mean / 1_000_000.0);
    println!("║ Ceiling: {:.2}M TPS", max / 1_000_000.0);
    println!("║ vs 无预热Mean(4.16M): {:+.1}%", (mean / 1_000_000.0 - 4.16) / 4.16 * 100.0);
    println!("╚════════════════════════════════════╝\n");
}
