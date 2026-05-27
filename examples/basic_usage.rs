use lightning_exchange::{MatchingEngine, Order, PoolConfig, Side, TimeInForce};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 创建撮合引擎
    let mut engine = MatchingEngine::new(PoolConfig::default())?;

    println!("=== Matching Engine Example ===\n");

    // 创建一个卖单
    let sell_order = Order::new(0, Side::Sell, 50_000, 10, TimeInForce::GTC, 0);

    println!(
        "Placing sell order: price_ticks={}, qty_lots={}",
        sell_order.price_ticks, sell_order.quantity_lots
    );
    let result = engine.place_order(sell_order)?;
    println!(
        "Result: status={:?}, filled_lots={}\n",
        result.status, result.filled_lots
    );

    // 创建一个买单来撮合
    let buy_order = Order::new(0, Side::Buy, 50_000, 5, TimeInForce::GTC, 0);

    println!(
        "Placing buy order: price_ticks={}, qty_lots={}",
        buy_order.price_ticks, buy_order.quantity_lots
    );
    let result = engine.place_order(buy_order)?;
    println!(
        "Result: status={:?}, filled_lots={}\n",
        result.status, result.filled_lots
    );

    // 获取统计信息
    let stats = engine.stats();
    println!("Engine stats:");
    println!("  Total orders: {}", stats.total_orders);
    println!("  Buy book levels: {}", stats.buy_book_levels);
    println!("  Sell book levels: {}", stats.sell_book_levels);

    Ok(())
}
