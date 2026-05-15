use matching_engine::{
    MatchingEngine, PoolConfig, Order, Side, TimeInForce,
    MarketDataConfig, DepthSnapshotEvent,
};
use rtrb::RingBuffer;
use std::time::Instant;
use std::sync::atomic::{AtomicU64, Ordering};

static INIT_COUNT: AtomicU64 = AtomicU64::new(0);

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool_config = PoolConfig {
        order_capacity: 1_000_000,
        queue_capacity: 1_000_000,
        orderbook_type: matching_engine::orderbook_impl::OrderBookType::SkipList,
    };
    let mut engine = MatchingEngine::new(pool_config)?;

    let config = MarketDataConfig::new(
        100_000_000,   // 100ms
        false,
        500_000_000,   
        1_000_000_000,
    );
    engine.set_market_data_config(config);

    let (depth_tx, mut depth_rx) = RingBuffer::<DepthSnapshotEvent>::new(100000);
    engine.set_depth_snapshot_sender(depth_tx);

    println!("Engine sampling debug - 5 second test:");
    println!("Pool: 1M orders, 1M queue\n");

    let base_price = 50000.0;
    let start = Instant::now();
    let mut total_orders = 0;
    let mut samples = vec![];

    while start.elapsed().as_secs_f64() < 5.0 {
        // Add orders slowly to avoid pool exhaustion
        for i in 0..10 {
            let order = Order::new(
                total_orders as u64,
                if i % 2 == 0 { Side::Buy } else { Side::Sell },
                base_price,
                10.0,
                TimeInForce::GTC,
                0,
            );
            match engine.place_order(order) {
                Ok(_) => {
                    total_orders += 1;
                },
                Err(e) => {
                    println!("Order error at {}: {:?}", total_orders, e);
                    break;
                }
            }
        }

        // Collect samples
        while let Ok(evt) = depth_rx.pop() {
            let elapsed = start.elapsed().as_secs_f64();
            samples.push((elapsed, evt.timestamp));
            if samples.len() <= 10 {
                println!("[Sample #{}] elapsed={:.4}s, ts={} ns", 
                    samples.len(), elapsed, evt.timestamp);
            }
        }

        if total_orders % 100_000 == 0 {
            println!("[Progress] {} orders, {:.2}s", total_orders, start.elapsed().as_secs_f64());
        }
    }

    let elapsed = start.elapsed();
    println!("\n【Results】");
    println!("Orders placed: {}", total_orders);
    println!("Time: {:.4}s", elapsed.as_secs_f64());
    println!("Samples: {} (expected ~50 with 100ms interval)", samples.len());

    Ok(())
}
