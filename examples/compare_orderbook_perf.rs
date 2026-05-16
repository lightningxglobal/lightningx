use matching_engine::{
    MatchingEngine, Order, Side, TimeInForce, PoolConfig,
    array_orderbook::{ArrayOrderBook, SortOrder as ArraySortOrder},
    order::Side as OrderSide,
};
use std::time::Instant;

fn test_skiplist_matching() -> (u64, f64) {
    let config = PoolConfig { orderbook_type: matching_engine::orderbook_impl::OrderBookType::SkipList,
        order_capacity: 50_000,
        queue_capacity: 5_000,
    };

    let mut engine = MatchingEngine::new(config).unwrap();

    // 预设 2500 个卖单
    for i in 0..2500 {
        let order = Order::new(
            i,
            Side::Sell,
            50000.0 + (i % 1000) as f64 * 0.1,
            10.0,
            TimeInForce::GTC,
            0,
        );
        let _ = let mut affected_makers = SmallVec::new(); engine.place_order(order, &mut affected_makers, &mut smallvec::SmallVec::new());
    }

    // 计时：放入 2500 个买单进行撮合
    let start = Instant::now();
    let mut match_count = 0;
    for i in 2500..5000 {
        let order = Order::new(
            i,
            Side::Buy,
            55000.0,
            10.0,
            TimeInForce::GTC,
            0,
        );
        if let Ok(result) = let mut affected_makers = SmallVec::new(); engine.place_order(order, &mut affected_makers, &mut smallvec::SmallVec::new()) {
            if result.filled > 0.0 {
                match_count += 1;
            }
        }
    }
    let elapsed = start.elapsed();

    let tps = match_count as f64 / elapsed.as_secs_f64();
    (match_count, tps)
}

fn test_array_orderbook_matching() -> (u64, f64) {
    let mut book = ArrayOrderBook::new_with_pool(ArraySortOrder::Ascending, 50_000);

    // 预设 2500 个卖单价格档位和订单
    for i in 0..2500 {
        let price = 50000.0 + (i % 1000) as f64 * 0.1;
        let _ = book.insert_level(price);
        // 给每个价格档位添加一个卖单
        let _ = book.add_order_at_level(price, i as u64, 10.0);
    }

    // 计时：查找和添加 2500 个买单到最优卖价
    let start = Instant::now();
    let mut match_count = 0;
    for i in 2500..5000 {
        // 找最优卖价（最低价格）
        if let Some(best) = book.best_with_orders() {
            if let Ok(_) = book.add_order_at_level(best.price, 2500 + i as u64, 10.0) {
                match_count += 1;
            }
        }
    }
    let elapsed = start.elapsed();

    let tps = match_count as f64 / elapsed.as_secs_f64();
    (match_count, tps)
}

fn main() {
    println!("=== OrderBook 性能对比测试 ===\n");

    println!("【测试1】SkipList（完整撮合）");
    let start = Instant::now();
    let (count1, tps1) = test_skiplist_matching();
    let time1 = start.elapsed();
    println!("  成交笔数: {}", count1);
    println!("  耗时: {:.4}ms", time1.as_secs_f64() * 1000.0);
    println!("  TPS: {:.0} ({:.2}M)\n", tps1, tps1 / 1_000_000.0);

    println!("【测试2】ArrayOrderBook（添加订单）");
    let start = Instant::now();
    let (count2, tps2) = test_array_orderbook_matching();
    let time2 = start.elapsed();
    println!("  成交笔数: {}", count2);
    println!("  耗时: {:.4}ms", time2.as_secs_f64() * 1000.0);
    println!("  TPS: {:.0} ({:.2}M)\n", tps2, tps2 / 1_000_000.0);

    println!("=== 性能对比 ===");
    println!("SkipList:       {:.2}M TPS", tps1 / 1_000_000.0);
    println!("ArrayOrderBook: {:.2}M TPS", tps2 / 1_000_000.0);
    let ratio = tps2 / tps1;
    if ratio > 1.0 {
        println!("ArrayOrderBook 快 {:.2}x", ratio);
    } else {
        println!("SkipList 快 {:.2}x", 1.0 / ratio);
    }
    println!("\n目标: 7,000,000 TPS");
}
