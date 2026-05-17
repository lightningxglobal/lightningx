use matching_engine::{MatchingEngine, Order, Side, TimeInForce, PoolConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 创建撮合引擎
    let mut engine = MatchingEngine::new(PoolConfig::default())?;

    println!("=== Matching Engine Example ===\n");

    // 创建一个卖单
    let sell_order = Order::new(
        0,
        Side::Sell,
        50000.0,
        10.0,
        TimeInForce::GTC,
        0,
    );

    println!("Placing sell order: price={}, qty={}", sell_order.price, sell_order.quantity);
    println!("Result: status={:?}, filled={}\n", result.status, result.filled);

    // 创建一个买单来撮合
    let buy_order = Order::new(
        0,
        Side::Buy,
        50000.0,
        5.0,
        TimeInForce::GTC,
        0,
    );

    println!("Placing buy order: price={}, qty={}", buy_order.price, buy_order.quantity);
    println!("Result: status={:?}, filled={}\n", result.status, result.filled);

    // 获取统计信息
    let stats = engine.stats();
    println!("Engine stats:");
    println!("  Total orders: {}", stats.total_orders);
    println!("  Buy book levels: {}", stats.buy_book_levels);
    println!("  Sell book levels: {}", stats.sell_book_levels);

    Ok(())
}
