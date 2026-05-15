use matching_engine::{
    MatchingEngine, PoolConfig, Order, Side, TimeInForce,
    MarketDataConfig, DepthSnapshotEvent,
};
use rtrb::RingBuffer;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool_config = PoolConfig {
        order_capacity: 100_000,   // Larger pool
        orderbook_type: matching_engine::orderbook_impl::OrderBookType::SkipList,
        queue_capacity: 100_000,
    };
    let mut engine = MatchingEngine::new(pool_config)?;

    let config = MarketDataConfig::new(
        10_000_000,    // 10ms BBO采样 (shorter for testing)
        false,
        50_000_000,   
        100_000_000,
    );
    engine.set_market_data_config(config);

    let (depth_tx, mut depth_rx) = RingBuffer::<DepthSnapshotEvent>::new(10000);
    engine.set_depth_snapshot_sender(depth_tx);

    println!("Sampling diagnostic v2 - 1 second test:");
    println!("10ms sampling interval, 100K pool capacity\n");

    let base_price = 50000.0;
    let start = Instant::now();
    let mut total_orders = 0;
    let mut samples_received = 0;

    // Run for 1 second
    while start.elapsed().as_secs_f64() < 1.0 {
        // Place a few orders per iteration
        for i in 0..20 {
            let order = Order::new(
                total_orders as u64,
                if i % 2 == 0 { Side::Buy } else { Side::Sell },
                base_price + (i as f64) * 0.1,
                10.0,
                TimeInForce::GTC,
                0,
            );
            if let Err(e) = engine.place_order(order) {
                println!("Error placing order {}: {:?}", total_orders, e);
                break;
            }
            total_orders += 1;
        }

        // Check for samples
        while let Ok(evt) = depth_rx.pop() {
            samples_received += 1;
            if samples_received <= 5 {
                println!("Sample #{}: ts={} ns ({:.4}ms)", 
                    samples_received, evt.timestamp, evt.timestamp as f64 / 1_000_000.0);
            }
        }
    }

    // Final check
    while let Ok(_) = depth_rx.pop() {
        samples_received += 1;
    }

    let elapsed = start.elapsed();
    println!("\n【Results】");
    println!("Total orders: {}", total_orders);
    println!("Wall-clock time: {:.4}s", elapsed.as_secs_f64());
    println!("TPS: {:.1}M", total_orders as f64 / elapsed.as_secs_f64() / 1_000_000.0);
    println!("Samples received: {} (expected ~100)", samples_received);

    Ok(())
}
