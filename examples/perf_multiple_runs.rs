//! 多轮性能基准测试以滤除噪音
use lightning_exchange::{MatchingEngine, PoolConfig, Order, Side, TimeInForce};
use std::time::Instant;

fn run_benchmark() -> f64 {
    let num_batches = 5000;
    let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();
    let base_price = 50000.0;

    let total_start = Instant::now();

    for round in 0..num_batches {
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
    (num_batches as f64 * 10.0) / total_elapsed.as_secs_f64()
}

fn main() {
    println!("╔═══════════════════════════════════════╗");
    println!("║  多轮性能测试 (过滤噪音) - 10轮      ║");
    println!("╚═══════════════════════════════════════╝\n");

    let mut results = Vec::new();
    
    for run in 1..=10 {
        let tps = run_benchmark();
        results.push(tps);
        println!("Run {}: {:.2}M TPS", run, tps / 1_000_000.0);
    }

    let mean = results.iter().sum::<f64>() / results.len() as f64;
    let variance = results.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / results.len() as f64;
    let stddev = variance.sqrt();
    let min = results.iter().cloned().fold(f64::INFINITY, f64::min);
    let max = results.iter().cloned().fold(0.0, f64::max);

    println!("\n╔═══════════════════════════════════════╗");
    println!("║              统计汇总                ║");
    println!("╠═══════════════════════════════════════╣");
    println!("║ Mean:    {:.2}M TPS", mean / 1_000_000.0);
    println!("║ StdDev:  {:.2}M TPS ({:.1}%)", stddev / 1_000_000.0, stddev / mean * 100.0);
    println!("║ Min:     {:.2}M TPS", min / 1_000_000.0);
    println!("║ Max:     {:.2}M TPS", max / 1_000_000.0);
    println!("║ Range:   {:.2}M TPS", (max - min) / 1_000_000.0);
    println!("║ Ceiling: {:.2}M TPS", max / 1_000_000.0);
    println!("╚═══════════════════════════════════════╝\n");
}
