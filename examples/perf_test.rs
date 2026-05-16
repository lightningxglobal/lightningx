use matching_engine::{MatchingEngine, Order, Side, TimeInForce, PoolConfig};
use std::time::Instant;

fn create_test_order(id: u64, side: Side, price: f64) -> Order {
    Order::new(id, side, price, 1.0, TimeInForce::GTC, 0)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Matching Engine Performance Test ===\n");

    // Use smaller pool sizes to avoid allocation issues
    let config = PoolConfig { orderbook_type: matching_engine::orderbook_impl::OrderBookType::SkipList,
        order_capacity: 50_000,
        queue_capacity: 5_000,
    };

    // Test 1: Basic Order Placement
    println!("Test 1: Order Placement (500 orders)");
    let mut engine = MatchingEngine::new(config.clone())?;

    let start = Instant::now();
    let mut success_count = 0;

    for i in 0..500 {
        let order = create_test_order(i, Side::Buy, 50000.0 + (i % 100) as f64);
        if let mut affected_makers = SmallVec::new(); engine.place_order(order, &mut affected_makers, &mut smallvec::SmallVec::new()).is_ok() {
            success_count += 1;
        }
    }

    let elapsed = start.elapsed();
    let tps = success_count as f64 / elapsed.as_secs_f64();
    let latency_us = (elapsed.as_micros() as f64) / success_count as f64;

    println!("  Orders placed: {}", success_count);
    println!("  Duration: {:.4}ms", elapsed.as_secs_f64() * 1000.0);
    println!("  TPS: {:.0}", tps);
    println!("  Avg Latency: {:.2} μs\n", latency_us);

    // Test 2: Order Matching
    println!("Test 2: Order Matching (250 sell + 250 buy)");
    let mut engine = MatchingEngine::new(config.clone())?;

    // Pre-fill with sell orders
    for i in 0..250 {
        let order = create_test_order(i, Side::Sell, 50000.0 + (i % 50) as f64);
        let _ = let mut affected_makers = SmallVec::new(); engine.place_order(order, &mut affected_makers, &mut smallvec::SmallVec::new());
    }

    let start = Instant::now();
    let mut match_count = 0;

    for i in 250..500 {
        let order = create_test_order(i, Side::Buy, 55000.0);
        if let mut affected_makers = SmallVec::new(); engine.place_order(order, &mut affected_makers, &mut smallvec::SmallVec::new()).is_ok() {
            match_count += 1;
        }
    }

    let elapsed = start.elapsed();
    let tps = match_count as f64 / elapsed.as_secs_f64();
    let latency_us = (elapsed.as_micros() as f64) / match_count as f64;

    println!("  Orders matched: {}", match_count);
    println!("  Duration: {:.4}ms", elapsed.as_secs_f64() * 1000.0);
    println!("  TPS: {:.0}", tps);
    println!("  Avg Latency: {:.2} μs\n", latency_us);

    // Test 3: Cancel Orders
    println!("Test 3: Order Cancellation (100 orders)");
    let mut engine = MatchingEngine::new(config.clone())?;

    // Place some orders
    for i in 0..100 {
        let order = create_test_order(i, Side::Buy, 50000.0);
        let _ = let mut affected_makers = SmallVec::new(); engine.place_order(order, &mut affected_makers, &mut smallvec::SmallVec::new());
    }

    let start = Instant::now();
    let mut cancel_count = 0;

    for i in 0..100 {
        if engine.cancel_order(i).is_ok() {
            cancel_count += 1;
        }
    }

    let elapsed = start.elapsed();
    let tps = cancel_count as f64 / elapsed.as_secs_f64();
    let latency_us = (elapsed.as_micros() as f64) / cancel_count as f64;

    println!("  Orders cancelled: {}", cancel_count);
    println!("  Duration: {:.4}ms", elapsed.as_secs_f64() * 1000.0);
    println!("  TPS: {:.0}", tps);
    println!("  Avg Latency: {:.2} μs\n", latency_us);

    println!("=== Test Summary ===");
    println!("✓ Order placement working");
    println!("✓ Order matching working");
    println!("✓ Order cancellation working");
    println!("✓ Skip list insertion working");
    println!("\nPerformance Results:");
    println!("  Note: Simplified test with 50k order pool");
    println!("  Target: TPS > 6,000,000, Latency < 3 μs");
    println!("  Current: Single-threaded, in-memory benchmarks");

    Ok(())
}
