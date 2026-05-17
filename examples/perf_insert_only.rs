//! 基准：只插入簿，完全跳过match_order
use matching_engine::{MatchingEngine, PoolConfig, Order, Side, TimeInForce};
use std::time::Instant;
use hdrhistogram::Histogram;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("基准测试：只插入簿（通过Post-Only跳过match）\n");

    let num_batches = 5000;
    let mut engine = MatchingEngine::new(PoolConfig::default())?;
    let mut latencies = Histogram::<u64>::new(3)?;
    let base_price = 50000.0;

    let total_start = Instant::now();

    // Post-Only订单不会触发match逻辑
    for round in 0..num_batches {
        for i in 0..10 {
            let price = base_price + i as f64;
            let qty = 10.0;
            let start = Instant::now();
            let order = Order::new(
                round as u64 * 100 + i as u64, 
                Side::Buy, 
                price, 
                qty, 
                TimeInForce::PostOnly,  // <-- 关键：Post-Only 跳过matching
                0
            );
            let _ = engine.place_order(order)?;
            latencies.record(start.elapsed().as_nanos() as u64)?;
        }
    }

    let total_elapsed = total_start.elapsed();
    let tps = (num_batches as f64 * 10.0) / total_elapsed.as_secs_f64();

    println!("╔════════════════════════════════════╗");
    println!("║ Post-Only（跳过match逻辑）         ║");
    println!("╠════════════════════════════════════╣");
    println!("║ TPS:    {:.2}M", tps / 1_000_000.0);
    println!("║ P50:    {:.0} ns", latencies.value_at_percentile(50.0) as f64);
    println!("║ P99:    {:.0} ns", latencies.value_at_percentile(99.0) as f64);
    println!("╠════════════════════════════════════╣");
    println!("║ vs标准GTC(3.85M): {:+.1}%", (tps / 1_000_000.0 - 3.85) / 3.85 * 100.0);
    println!("╚════════════════════════════════════╝\n");

    Ok(())
}
