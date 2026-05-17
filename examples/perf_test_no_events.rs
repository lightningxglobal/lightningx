use matching_engine::{MatchingEngine, PoolConfig, Order, Side, TimeInForce};
use std::time::Instant;
use hdrhistogram::Histogram;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Performance comparison: WITH vs WITHOUT event publishing\n");

    // Test WITHOUT event publishing
    let num_batches = 5000;
    let mut engine = MatchingEngine::new(PoolConfig::default())?;

    let mut latencies = Histogram::<u64>::new(3)?;
    let base_price = 50000.0;

    let total_start = Instant::now();

    for round in 0..num_batches {
        for i in 0..5 {
            let price = base_price - 2.0 + i as f64;
            let qty = 10.0;
            let start = Instant::now();
            let order = Order::new(round as u64 * 1000 + i as u64, Side::Buy, price, qty, TimeInForce::GTC, 0);
            let _ = engine.place_order(order)?;
            latencies.record(start.elapsed().as_nanos() as u64)?;
        }

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
    let tps = (num_batches as f64 * 10.0) / total_elapsed.as_secs_f64();

    println!("WITHOUT event publishing:");
    println!("  TPS: {:.2}M", tps / 1_000_000.0);
    println!("  P50: {:.0} ns", latencies.value_at_percentile(50.0) as f64);
    println!("  P99: {:.0} ns", latencies.value_at_percentile(99.0) as f64);
    println!("  Total: {:.3}s\n", total_elapsed.as_secs_f64());

    Ok(())
}
