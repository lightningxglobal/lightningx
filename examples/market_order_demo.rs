use matching_engine::{
    MatchingEngine, Order, Side, TimeInForce, PoolConfig,
};

fn main() {
    println!("=== 市价委托演示 ===\n");

    // 场景 1：买市价 vs 卖限价
    println!("【场景 1】买市价 vs 卖限价");
    println!("  1. 放一个卖限价 50000 USDT，5 个数量");
    let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();
    let sell_limit = Order::new(1, Side::Sell, 50000.0, 5.0, TimeInForce::GTC, 0);
    let _ = engine.place_order(sell_limit).unwrap();

    println!("  2. 买市价 3 个数量（应该以 50000 成交）");
    let buy_market = Order::new_market(2, Side::Buy, 3.0, 0);
    let result = engine.place_order(buy_market).unwrap();
    println!("     成交状态: {:?}", result.status);
    println!("     成交数量: {}", result.filled);
    println!();

    // 场景 2：卖市价 vs 买限价
    println!("【场景 2】卖市价 vs 买限价");

    let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

    println!("  1. 放一个买限价 49500 USDT，10 个数量");
    let buy_limit = Order::new(1, Side::Buy, 49500.0, 10.0, TimeInForce::GTC, 0);
    let _ = engine.place_order(buy_limit).unwrap();

    println!("  2. 卖市价 7 个数量（应该以 49500 成交）");
    let sell_market = Order::new_market(2, Side::Sell, 7.0, 0);
    let result = engine.place_order(sell_market).unwrap();
    println!("     成交状态: {:?}", result.status);
    println!("     成交数量: {}", result.filled);
    println!();

    // 场景 3：市价单遇空盘
    println!("【场景 3】市价单遇空盘");

    let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

    println!("  1. 盘口为空");
    println!("  2. 买市价 10 个数量（应该被拒绝）");
    let buy_market = Order::new_market(1, Side::Buy, 10.0, 0);
    let result = engine.place_order(buy_market).unwrap();
    println!("     成交状态: {:?}", result.status);
    println!("     成交数量: {}", result.filled);
    println!();

    // 场景 4：市价单部分成交
    println!("【场景 4】市价单部分成交");

    let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

    println!("  1. 放一个卖限价 50100 USDT，4 个数量");
    let sell_limit = Order::new(1, Side::Sell, 50100.0, 4.0, TimeInForce::GTC, 0);
    let _ = engine.place_order(sell_limit).unwrap();

    println!("  2. 买市价 10 个数量（只有 4 个可成交）");
    let buy_market = Order::new_market(2, Side::Buy, 10.0, 0);
    let result = engine.place_order(buy_market).unwrap();
    println!("     成交状态: {:?}", result.status);
    println!("     成交数量: {}", result.filled);
    println!();

    println!("✅ 市价委托演示完成");
}
