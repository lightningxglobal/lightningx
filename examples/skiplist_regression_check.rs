use matching_engine::{MatchingEngine, Order, Side, TimeInForce, PoolConfig};
use matching_engine::skiplist::{SkipList, SortOrder};
use matching_engine::orderbook::OrderBook;
use matching_engine::orderbook_impl::OrderBookType;
use std::time::Instant;

/// 测试1: 直接使用原始SkipList（无trait wrapper）
fn test_raw_skiplist() {
    println!("【测试1】直接使用原始 SkipList");
    let mut buy_book = SkipList::new_with_pool(SortOrder::Descending, 50_000);
    let mut sell_book = SkipList::new_with_pool(SortOrder::Ascending, 50_000);

    // 预设 5000 个卖单
    for i in 0..5000 {
        let price = 50000.0 + (i % 1000) as f64 * 0.1;
        let _ = sell_book.insert_level(price);
        let _ = sell_book.add_order_at_level(price, i as u64, 10.0);
    }

    // 计时：5000 个买单匹配
    let start = Instant::now();
    let mut match_count = 0;
    for i in 5000..10000 {
        // 寻找最优卖价
        if let Some(node) = sell_book.best_with_orders() {
            if let Ok(_) = sell_book.add_order_at_level(node.price, 5000 + i as u64, 10.0) {
                match_count += 1;
            }
        }
    }
    let elapsed = start.elapsed();
    let tps = match_count as f64 / elapsed.as_secs_f64();

    println!("  成交: {}, 耗时: {:.4}ms", match_count, elapsed.as_secs_f64() * 1000.0);
    println!("  TPS: {:.0} ({:.2}M)\n", tps, tps / 1_000_000.0);
}

/// 测试2: 通过 MatchingEngine 使用 SkipList（使用 OrderBookWrapper）
fn test_matching_engine() {
    println!("【测试2】通过 MatchingEngine 使用 SkipList (OrderBook Trait)");
    let config = PoolConfig {
        order_capacity: 50_000,
        queue_capacity: 5_000,
        orderbook_type: OrderBookType::SkipList,
    };

    let mut engine = MatchingEngine::new(config).unwrap();

    // 预设 5000 个卖单
    for i in 0..5000 {
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

    // 计时：5000 个买单匹配
    let start = Instant::now();
    let mut match_count = 0;
    for i in 5000..10000 {
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

    println!("  成交: {}, 耗时: {:.4}ms", match_count, elapsed.as_secs_f64() * 1000.0);
    println!("  TPS: {:.0} ({:.2}M)\n", tps, tps / 1_000_000.0);
}

/// 测试3: 用之前的 pure_matching_benchmark 场景（10,000对完全匹配）
fn test_perfect_matching() {
    println!("【测试3】完全匹配场景 (10K对，所有都成交)");
    let config = PoolConfig {
        order_capacity: 50_000,
        queue_capacity: 5_000,
        orderbook_type: OrderBookType::SkipList,
    };

    let mut engine = MatchingEngine::new(config).unwrap();

    // 10,000 对买卖单，完全在同一价格匹配
    let start = Instant::now();
    let mut match_count = 0;
    for i in 0..10000 {
        let buy = Order::new(i as u64 * 2, Side::Buy, 50000.0, 10.0, TimeInForce::GTC, 0);
        let _ = let mut affected_makers = SmallVec::new(); engine.place_order(buy, &mut affected_makers, &mut smallvec::SmallVec::new());

        let sell = Order::new(i as u64 * 2 + 1, Side::Sell, 50000.0, 10.0, TimeInForce::GTC, 0);
        if let Ok(result) = let mut affected_makers = SmallVec::new(); engine.place_order(sell, &mut affected_makers, &mut smallvec::SmallVec::new()) {
            if result.filled > 0.0 {
                match_count += 1;
            }
        }
    }
    let elapsed = start.elapsed();
    let tps = (10000 * 2) as f64 / elapsed.as_secs_f64();

    println!("  成交对数: {}, 总操作: 20000", match_count);
    println!("  耗时: {:.4}ms", elapsed.as_secs_f64() * 1000.0);
    println!("  TPS: {:.0} ({:.2}M)\n", tps, tps / 1_000_000.0);
}

fn main() {
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║        SkipList 性能回归检查                              ║");
    println!("║  原始性能: 2.077M TPS (pure_matching_benchmark)            ║");
    println!("║  当前性能: 1.38M TPS (comprehensive_bench)                 ║");
    println!("║  下降幅度: ~33%  ❌                                        ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    test_raw_skiplist();
    test_matching_engine();
    test_perfect_matching();

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║                   性能对标总结                             ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    println!("预期值: 2.077M TPS (之前的 pure_matching_benchmark)");
    println!("当前值: 1.38M TPS (comprehensive_bench matching)");
    println!("完全匹配值: ? (测试3结果)\n");

    println!("可能的性能损失原因：");
    println!("1. OrderBook trait wrapper (enum dispatch) 的开销");
    println!("2. MatchingEngine 中额外的逻辑（order跟踪、事件发送等）");
    println!("3. 测试场景差异（不同的价格分布、order book深度等）");
    println!("4. 系统状态差异（CPU缓存预热等）");
}
