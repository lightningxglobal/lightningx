//! 市场数据系统集成示例
//!
//! 演示匹配引擎和市场数据通道的集成，包括延迟测量。
//!
//! 本示例展示：
//! 1. 创建有界MPMC通道（容量100万）
//! 2. 将TradeEvent发布到通道
//! 3. 测量端到端延迟
//! 4. 验证延迟 < 1微秒目标

use matching_engine::{
    MatchingEngine, PoolConfig, Order, Side, TimeInForce, TradeEvent,
};
use std::time::Instant;
use rtrb::RingBuffer;

fn main() {
    println!("=== 市场数据系统集成示例 ===\n");

    // 1. 创建有界通道，容量1,000,000
    let (sender, mut receiver) = RingBuffer::<TradeEvent>::new(1_000_000);
    println!("✓ 创建有界通道 (capacity: 1,000,000)");

    // 2. 创建匹配引擎
    let pool_config = PoolConfig::default();
    let mut engine = MatchingEngine::new(pool_config)
        .expect("创建匹配引擎失败");
    println!("✓ 创建匹配引擎");

    // 3. 设置交易事件发送器
    engine.set_trade_event_sender(sender);
    println!("✓ 设置交易事件发送器\n");

    // 4. 生成一些交易并测量延迟
    println!("生成交易并测量延迟...\n");

    let mut latencies = Vec::new();
    let num_trades = 1000;
    let mut trade_count = 0;

    for i in 1..=num_trades {
        // 买单
        let buy_order = Order::new(
            i * 2 - 1,
            Side::Buy,
            50000.0,
            10.0,
            TimeInForce::GTC,
            0,
        );

        let _ = let mut affected_makers = SmallVec::new(); engine.place_order(buy_order, &mut affected_makers, &mut smallvec::SmallVec::new());

        // 卖单 - 触发成交
        let sell_order = Order::new(
            i * 2,
            Side::Sell,
            50000.0,
            10.0,
            TimeInForce::IOC,
            0,
        );

        let trade_start = Instant::now();
        let _ = let mut affected_makers = SmallVec::new(); engine.place_order(sell_order, &mut affected_makers, &mut smallvec::SmallVec::new());

        // 尝试从通道接收事件
        if let Ok(_trade_event) = receiver.pop() {
            let elapsed = trade_start.elapsed();
            let latency_ns = elapsed.as_nanos() as u64;
            latencies.push(latency_ns);
            trade_count += 1;

            if i <= 10 || i % 100 == 0 {
                println!(
                    "Trade #{}: latency = {:.2} ns ({:.3} μs)",
                    i,
                    latency_ns,
                    latency_ns as f64 / 1000.0
                );
            }
        }
    }

    println!("\n=== 延迟统计 ===\n");

    if !latencies.is_empty() {
        latencies.sort();

        let min = latencies.first().unwrap();
        let max = latencies.last().unwrap();
        let avg = latencies.iter().sum::<u64>() as f64 / latencies.len() as f64;

        // 计算百分位数
        let p50_idx = latencies.len() / 2;
        let p99_idx = (latencies.len() * 99) / 100;
        let p999_idx = (latencies.len() * 999) / 1000;

        let p50 = latencies[p50_idx];
        let p99 = if p99_idx < latencies.len() {
            latencies[p99_idx]
        } else {
            latencies[latencies.len() - 1]
        };
        let p999 = if p999_idx < latencies.len() {
            latencies[p999_idx]
        } else {
            latencies[latencies.len() - 1]
        };

        println!("总成交数: {}", trade_count);
        println!("成功接收: {} 个TradeEvent\n", latencies.len());

        println!("最小延迟:      {:.2} ns ({:.3} μs)", min, *min as f64 / 1000.0);
        println!("平均延迟:      {:.2} ns ({:.3} μs)", avg, avg / 1000.0);
        println!("中位数(P50):   {:.2} ns ({:.3} μs)", p50, p50 as f64 / 1000.0);
        println!("P99:           {:.2} ns ({:.3} μs)", p99, p99 as f64 / 1000.0);
        println!("P99.9:         {:.2} ns ({:.3} μs)", p999, p999 as f64 / 1000.0);
        println!("最大延迟:      {:.2} ns ({:.3} μs)\n", max, *max as f64 / 1000.0);

        // 检查是否满足 < 1μs 目标
        let target_ns = 1000;  // 1μs in nanoseconds
        let within_target = latencies.iter().filter(|&&l| l < target_ns).count();
        let percentage = (within_target * 100) / latencies.len();

        println!("目标：< 1μs (1000 ns)");
        println!("满足条件: {}/{} ({:.1}%)\n", within_target, latencies.len(), percentage);

        if avg < target_ns as f64 {
            println!("✓ 平均延迟满足 < 1μs 目标!");
        } else {
            println!("✗ 平均延迟未满足 < 1μs 目标 (avg: {:.3} μs)", avg / 1000.0);
        }
    } else {
        println!("✗ 未能接收任何TradeEvent");
    }
}
