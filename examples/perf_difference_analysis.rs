use matching_engine::{MatchingEngine, Order, Side, TimeInForce, PoolConfig};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = PoolConfig::default();

    println!("=== 性能差异分析 ===\n");

    // 方法A: 完全成交的买卖对（我的pure_matching_benchmark）
    println!("【方法A】完全成交买卖对（alternating价格）");
    let mut engine_a = MatchingEngine::new(config.clone())?;
    
    let start = Instant::now();
    let num = 5000;
    for i in 0..num {
        let price = 50000.0 + (i % 100) as f64;  // 变化的价格
        let buy = Order::new(i as u64 * 2, Side::Buy, price, 10.0, TimeInForce::GTC, 0);
        let _ = engine_a.place_order(buy)?;
        let sell = Order::new(i as u64 * 2 + 1, Side::Sell, price, 10.0, TimeInForce::GTC, 0);
        let _ = engine_a.place_order(sell)?;
    }
    let elapsed_a = start.elapsed();
    let tps_a = (num * 2) as f64 / elapsed_a.as_secs_f64();
    println!("  TPS: {:.0}M\n", tps_a / 1_000_000.0);

    // 方法B: 预设订单簿 + 匹配（深度测试用的）
    println!("【方法B】预设订单簿(深度50) + 买单匹配");
    let mut engine_b = MatchingEngine::new(config.clone())?;
    
    // 预置50个卖单（不计时）
    for i in 0..50 {
        let order = Order::new(i as u64, Side::Sell, 50000.0 + (i % 50) as f64 * 0.1, 10.0, TimeInForce::GTC, 0);
        let _ = engine_b.place_order(order);
    }
    
    let start = Instant::now();
    for i in 0..5000 {
        let order = Order::new((50 + i) as u64, Side::Buy, 55000.0, 10.0, TimeInForce::GTC, 0);
        let _ = engine_b.place_order(order);
    }
    let elapsed_b = start.elapsed();
    let tps_b = 5000.0 / elapsed_b.as_secs_f64();
    println!("  TPS: {:.0}M\n", tps_b / 1_000_000.0);

    // 方法C: 相同价格的买卖对
    println!("【方法C】相同价格的买卖对（无订单簿增长）");
    let mut engine_c = MatchingEngine::new(config.clone())?;
    
    let start = Instant::now();
    let num = 5000;
    for i in 0..num {
        let buy = Order::new(i as u64 * 2, Side::Buy, 50000.0, 10.0, TimeInForce::GTC, 0);  // 固定价格
        let _ = engine_c.place_order(buy)?;
        let sell = Order::new(i as u64 * 2 + 1, Side::Sell, 50000.0, 10.0, TimeInForce::GTC, 0);
        let _ = engine_c.place_order(sell)?;
    }
    let elapsed_c = start.elapsed();
    let tps_c = (num * 2) as f64 / elapsed_c.as_secs_f64();
    println!("  TPS: {:.0}M\n", tps_c / 1_000_000.0);

    println!("【对比分析】");
    println!("方法A (变化价格): {:.0}M TPS - 订单簿快速增长", tps_a / 1_000_000.0);
    println!("方法B (预设+匹配): {:.0}M TPS - 对手订单池固定", tps_b / 1_000_000.0);
    println!("方法C (相同价格):  {:.0}M TPS - 单一价格级别", tps_c / 1_000_000.0);
    println!("\n关键发现: 订单簿复杂性严重影响性能！");

    Ok(())
}
