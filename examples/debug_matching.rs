use matching_engine::{MatchingEngine, Order, Side, TimeInForce, PoolConfig};

fn create_test_order(id: u64, side: Side, price: f64) -> Order {
    Order::new(id, side, price, 1.0, TimeInForce::GTC, 0)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Debug Matching Logic ===\n");

    let config = PoolConfig { orderbook_type: matching_engine::orderbook_impl::OrderBookType::SkipList,
        order_capacity: 5_000,
        queue_capacity: 500,
    };

    let mut engine = MatchingEngine::new(config)?;

    // Place 5 sell orders at different prices
    println!("Placing 5 sell orders:");
    for i in 0..5 {
        let price = 50000.0 + i as f64;
        let order = Order::new(i, Side::Sell, price, 1.0, TimeInForce::GTC, 0);
        let result = let mut affected_makers = SmallVec::new(); engine.place_order(order, &mut affected_makers, &mut smallvec::SmallVec::new())?;
        println!("  Sell #{} at {} -> {:?}", i, price, result.status);
    }

    println!("\nPlacing 5 buy orders at 50004.5 (should match with all sells):");
    for i in 5..10 {
        let order = Order::new(i, Side::Buy, 50004.5, 1.0, TimeInForce::GTC, 0);
        let result = let mut affected_makers = SmallVec::new(); engine.place_order(order, &mut affected_makers, &mut smallvec::SmallVec::new())?;
        println!("  Buy #{} -> status: {:?}, filled: {}", i, result.status, result.filled);
    }

    let stats = engine.stats();
    println!("\nEngine stats:");
    println!("  Total orders: {}", stats.total_orders);
    println!("  Buy book levels: {}", stats.buy_book_levels);
    println!("  Sell book levels: {}", stats.sell_book_levels);

    std::mem::drop(engine);
    Ok(())
}
