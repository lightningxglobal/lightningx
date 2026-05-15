use matching_engine::{
    MatchingEngine, PoolConfig, Order, Side, TimeInForce,
    MarketDataConfig, DepthSnapshotEvent,
};
use rtrb::RingBuffer;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = MatchingEngine::new(PoolConfig::default())?;

    let config = MarketDataConfig::new(
        100_000_000,   // 100ms BBO採样
        false,         // No increments for simplicity
        500_000_000,   
        1_000_000_000,
    );
    engine.set_market_data_config(config);

    let (depth_tx, mut depth_rx) = RingBuffer::<DepthSnapshotEvent>::new(10000);
    engine.set_depth_snapshot_sender(depth_tx);

    println!("Sampling diagnostic - 0.5 second test:");
    println!("Expected samples at ~5 intervals of 100ms\n");

    let base_price = 50000.0;
    let start = Instant::now();
    let mut total_orders = 0;
    let mut order_ids = vec![];

    // Run for 0.5 seconds
    while start.elapsed().as_secs_f64() < 0.5 {
        // Place one order and immediately check for samples
        let order = Order::new(
            total_orders as u64,
            Side::Buy,
            base_price,
            10.0,
            TimeInForce::GTC,
            0,
        );
        engine.place_order(order)?;
        total_orders += 1;

        // Try to consume events
        while let Ok(evt) = depth_rx.pop() {
            order_ids.push(total_orders);
            println!("Sample after {} orders: ts={} ns", total_orders, evt.timestamp);
        }

        // After every 10M orders, check progress
        if total_orders % 10_000_000 == 0 {
            let elapsed = start.elapsed().as_secs_f64();
            println!("[Progress] {} orders, {:.4}s elapsed, {:.1}M ops/sec",
                total_orders, elapsed, total_orders as f64 / elapsed / 1_000_000.0);
        }
    }

    // Final check
    while let Ok(evt) = depth_rx.pop() {
        println!("Final sample: ts={} ns", evt.timestamp);
    }

    let elapsed = start.elapsed();
    println!("\n【Results】");
    println!("Total orders: {}", total_orders);
    println!("Wall-clock time: {:.4}s", elapsed.as_secs_f64());
    println!("TPS: {:.1}M", total_orders as f64 / elapsed.as_secs_f64() / 1_000_000.0);
    println!("Samples: {} (expected ~5)", order_ids.len());

    Ok(())
}
