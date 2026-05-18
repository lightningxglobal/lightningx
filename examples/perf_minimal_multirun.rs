//! 最小化benchmark多轮运行
use lightning_exchange::{MatchingEngine, PoolConfig, Order, Side, TimeInForce};
use std::time::Instant;

fn run_minimal_bench() -> f64 {
    let num_rounds = 10000;
    let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();
    let base_price = 50000.0;

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
    println!("最小化benchmark多轮测试 - 10轮\n");

    let mut results = Vec::new();
    for run in 1..=10 {
        let tps = run_minimal_bench();
        results.push(tps);
        println!("Run {}: {:.2}M TPS", run, tps / 1_000_000.0);
    }

    let mean = results.iter().sum::<f64>() / results.len() as f64;
    let max = results.iter().cloned().fold(0.0, f64::max);
    let min = results.iter().cloned().fold(f64::INFINITY, f64::min);

    println!("\n╔════════════════════════════════════╗");
    println!("║ Mean:    {:.2}M TPS", mean / 1_000_000.0);
    println!("║ Ceiling: {:.2}M TPS", max / 1_000_000.0);
    println!("║ Floor:   {:.2}M TPS", min / 1_000_000.0);
    println!("║ vs 6.46M historical: {:+.1}%", (mean / 1_000_000.0 - 6.46) / 6.46 * 100.0);
    println!("╚════════════════════════════════════╝\n");
}
