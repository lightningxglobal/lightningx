//! 原始性能复现基准测试
//! 方式: 先批量放置，再批量成交（不交替）

use matching_engine::{MatchingEngine, PoolConfig, Order, Side, TimeInForce};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 匹配引擎性能复现测试 ===\n");

    let config = PoolConfig { orderbook_type: matching_engine::orderbook_impl::OrderBookType::SkipList,
        order_capacity: 500_000,
        queue_capacity: 50_000,
    };

    // ========== 场景1: 批量放置 ==========
    println!("【场景1】批量放置 10k 个订单");
    let mut engine = MatchingEngine::new(config.clone())?;

    let place_start = Instant::now();
    for i in 0..10_000 {
        let order = Order::new(
            i as u64,
            Side::Buy,
            50000.0 + (i % 100) as f64,
            10.0,
            TimeInForce::GTC,
            0,
        );
        engine.place_order(order)?;
    }
    let place_duration = place_start.elapsed();
    let place_tps = 10_000.0 / place_duration.as_secs_f64();

    println!("  放置TPS: {:.0} (耗时: {:.3}ms)", place_tps, place_duration.as_secs_f64() * 1000.0);

    // ========== 场景2: 批量成交（全部成交) ==========
    println!("\n【场景2】批量放置卖单 + 批量成交买单");

    let mut engine2 = MatchingEngine::new(config.clone())?;

    // 第1步: 放置卖单
    for i in 0..10_000 {
        let order = Order::new(
            i as u64,
            Side::Sell,
            50000.0 + (i % 100) as f64,
            10.0,
            TimeInForce::GTC,
            0,
        );
        let _ = engine2.place_order(order)?;
    }

    // 第2步: 成交买单（计时）
    let match_start = Instant::now();
    let mut matched_count = 0;
    for i in 0..10_000 {
        let order = Order::new(
            (10_000 + i) as u64,
            Side::Buy,
            50000.0 + (i % 100) as f64,
            10.0,
            TimeInForce::GTC,
            0,
        );
        let result = engine2.place_order(order)?;
        if result.filled > 0.0 {
            matched_count += 1;
        }
    }
    let match_duration = match_start.elapsed();
    let match_tps = matched_count as f64 / match_duration.as_secs_f64();

    println!("  放置卖单: 10,000 (预热不计时)");
    println!("  成交买单: {} (耗时: {:.3}ms)", matched_count, match_duration.as_secs_f64() * 1000.0);
    println!("  成交TPS: {:.0}", match_tps);

    // ========== 场景3: 单价极限成交 ==========
    println!("\n【场景3】单价极限成交 (10k 订单 @ 50000.0)");

    let mut engine3 = MatchingEngine::new(config.clone())?;

    // 放置10k卖单
    let price = 50000.0;
    for i in 0..10_000 {
        let order = Order::new(
            i as u64,
            Side::Sell,
            price,
            1.0,
            TimeInForce::GTC,
            0,
        );
        let _ = engine3.place_order(order)?;
    }

    // 成交10k买单
    let extreme_start = Instant::now();
    let mut extreme_count = 0;
    for i in 0..10_000 {
        let order = Order::new(
            (10_000 + i) as u64,
            Side::Buy,
            price,
            1.0,
            TimeInForce::GTC,
            0,
        );
        let result = engine3.place_order(order)?;
        if result.filled > 0.0 {
            extreme_count += 1;
        }
    }
    let extreme_duration = extreme_start.elapsed();
    let extreme_tps = extreme_count as f64 / extreme_duration.as_secs_f64();

    println!("  成交数: {} (耗时: {:.6}ms)", extreme_count, extreme_duration.as_secs_f64() * 1000.0);
    println!("  极限TPS: {:.0}", extreme_tps);

    // ========== 性能总结 ==========
    println!("\n【性能总结】");
    println!("{}", "=".repeat(60));
    println!("放置TPS:       {:.0} (万笔/秒: {:.2})", place_tps, place_tps / 10000.0);
    println!("成交TPS:       {:.0} (万笔/秒: {:.2})", match_tps, match_tps / 10000.0);
    println!("极限TPS:       {:.0} (万笔/秒: {:.2})", extreme_tps, extreme_tps / 10000.0);
    println!();
    println!("目标TPS:       6,000,000");

    if match_tps >= 6_000_000.0 || extreme_tps >= 6_000_000.0 {
        println!("状态:          ✓ 性能恢复到目标");
    } else {
        println!("状态:          ⚠ 仍低于目标 (需要进一步优化)");
    }

    Ok(())
}
