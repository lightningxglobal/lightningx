use lightning_exchange::{MatchingEngine, Order, Side, TimeInForce, PoolConfig};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = PoolConfig { orderbook_type: lightning_exchange::orderbook_impl::OrderBookType::SkipList,
        order_capacity: 50_000,
        queue_capacity: 5_000,
    };

    println!("=== 复现原始测试方法 ===\n");

    // 测试1: 100预设 + 100买匹配
    println!("【测试1】100 sell预设 + 100 buy匹配");
    let mut engine = MatchingEngine::new(config.clone())?;

    for i in 0..100 {
        let order = Order::new(i, Side::Sell, 50000.0 + (i % 50) as f64, 1.0, TimeInForce::GTC, 0);
        let _ = engine.place_order(order);
    }

    let start = Instant::now();
    let mut match_count = 0;
    for i in 100..200 {
        let order = Order::new(i, Side::Buy, 55000.0, 1.0, TimeInForce::GTC, 0);
        if let Ok(result) = engine.place_order(order) {
            if result.filled > 0.0 {
                match_count += 1;
            }
        }
    }
    let elapsed = start.elapsed();
    let tps = match_count as f64 / elapsed.as_secs_f64();

    println!("  成交单数: {}", match_count);
    println!("  耗时: {:.4}ms", elapsed.as_secs_f64() * 1000.0);
    println!("  TPS: {:.0} ({:.1}M)\n", tps, tps / 1_000_000.0);

    // 测试2: 大规模版本
    println!("【测试2】5000 sell预设 + 5000 buy匹配");
    let mut engine2 = MatchingEngine::new(config.clone())?;

    for i in 0..5000 {
        let order = Order::new(i, Side::Sell, 50000.0 + (i % 1000) as f64 * 0.1, 10.0, TimeInForce::GTC, 0);
        let _ = engine2.place_order(order);
    }

    let start = Instant::now();
    let mut match_count = 0;
    for i in 5000..10000 {
        let order = Order::new(i, Side::Buy, 55000.0, 10.0, TimeInForce::GTC, 0);
        if let Ok(result) = engine2.place_order(order) {
            if result.filled > 0.0 {
                match_count += 1;
            }
        }
    }
    let elapsed = start.elapsed();
    let tps = match_count as f64 / elapsed.as_secs_f64();

    println!("  成交单数: {}", match_count);
    println!("  耗时: {:.4}ms", elapsed.as_secs_f64() * 1000.0);
    println!("  TPS: {:.0} ({:.1}M)\n", tps, tps / 1_000_000.0);

    // 测试3: 对比全场景（买+卖都计时）
    println!("【测试3】对比：买卖都计时 (5000轮 = 10k单)");
    let mut engine3 = MatchingEngine::new(config.clone())?;

    let start = Instant::now();
    let mut matches = 0;
    for i in 0..5000 {
        let buy = Order::new(i as u64 * 2, Side::Buy, 50000.0, 10.0, TimeInForce::GTC, 0);
        let _ = engine3.place_order(buy)?;
        let sell = Order::new(i as u64 * 2 + 1, Side::Sell, 50000.0, 10.0, TimeInForce::GTC, 0);
        let result = engine3.place_order(sell)?;
        if result.filled > 0.0 {
            matches += 1;
        }
    }
    let elapsed = start.elapsed();
    let tps = (5000 * 2) as f64 / elapsed.as_secs_f64();

    println!("  成交对数: {}", matches);
    println!("  总操作: 10000");
    println!("  耗时: {:.4}ms", elapsed.as_secs_f64() * 1000.0);
    println!("  TPS: {:.0} ({:.1}M)\n", tps, tps / 1_000_000.0);

    println!("=== 性能对标 ===");
    println!("原始测试方法(仅卖单匹配): 目标可能达到较高值");
    println!("真实全场景(买卖都计时): 1.6M TPS");
    println!("目标要求: 7M TPS");

    Ok(())
}
