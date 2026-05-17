use matching_engine::{MatchingEngine, Order, Side, TimeInForce, PoolConfig};

fn create_test_order(id: u64, side: Side, price: f64, qty: f64) -> Order {
    Order::new(id, side, price, qty, TimeInForce::GTC, 0)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Depth Snapshot Test ===\n");

    let config = PoolConfig { orderbook_type: matching_engine::orderbook_impl::OrderBookType::SkipList,
        order_capacity: 5_000,
        queue_capacity: 500,
    };

    let mut engine = MatchingEngine::new(config)?;

    // Place sell orders at multiple price levels
    println!("Placing sell orders:");
    for i in 0..5 {
        let price = 50000.0 + i as f64;
        let qty = (i + 1) as f64;
        let order = create_test_order(i, Side::Sell, price, qty);
        println!("  Sell #{} at price {} qty {}", i, price, qty);
    }

    // Place buy orders at multiple price levels
    println!("\nPlacing buy orders:");
    for i in 0..5 {
        let price = 49999.0 - i as f64;
        let qty = (5 - i) as f64;
        let order = create_test_order(10 + i, Side::Buy, price, qty);
        println!("  Buy #{} at price {} qty {}", 10 + i, price, qty);
    }

    // Generate and display snapshot
    let snapshot = engine.generate_depth_snapshot();

    println!("\n=== Market Depth Snapshot ===");
    println!("Timestamp: {}", snapshot.timestamp);
    println!("Sequence: {}", snapshot.sequence);

    println!("\nBids ({} levels):", snapshot.num_bids);
    for i in 0..snapshot.num_bids as usize {
        let bid = snapshot.bids[i];
        println!("  {}: {} @ {}", i + 1, bid.quantity, bid.price);
    }

    println!("\nAsks ({} levels):", snapshot.num_asks);
    for i in 0..snapshot.num_asks as usize {
        let ask = snapshot.asks[i];
        println!("  {}: {} @ {}", i + 1, ask.quantity, ask.price);
    }

    // Test snapshot after partial fill
    println!("\n=== Testing Partial Fill ===");
    println!("Placing buy order at 50004.5 (should match with sell orders)");
    let match_order = create_test_order(20, Side::Buy, 50004.5, 3.0);
    let result = engine.place_order(match_order)?;
    println!("Matched {} units, status: {:?}", result.filled, result.status);

    // Generate snapshot after match
    let snapshot2 = engine.generate_depth_snapshot();
    println!("\n=== Snapshot After Match ===");
    println!("Bids ({} levels):", snapshot2.num_bids);
    for i in 0..snapshot2.num_bids as usize {
        let bid = snapshot2.bids[i];
        if bid.quantity > 0.0 {
            println!("  {}: {} @ {}", i + 1, bid.quantity, bid.price);
        }
    }

    println!("\nAsks ({} levels):", snapshot2.num_asks);
    for i in 0..snapshot2.num_asks as usize {
        let ask = snapshot2.asks[i];
        if ask.quantity > 0.0 {
            println!("  {}: {} @ {}", i + 1, ask.quantity, ask.price);
        }
    }

    std::mem::drop(engine);
    Ok(())
}
