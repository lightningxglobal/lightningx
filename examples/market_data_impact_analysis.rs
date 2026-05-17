//! 市场数据对撮合引擎的性能影响分析
//! 逐步添加市场数据处理，观察性能变化

use matching_engine::{MatchingEngine, PoolConfig, Order, Side, TimeInForce, TradeEvent, MarketDataEngine};
use std::time::Instant;
use std::sync::Arc;
use parking_lot::Mutex;
use rtrb::RingBuffer;

fn benchmark_matching(name: &str, mut engine: MatchingEngine, num_rounds: usize) -> (f64, f64) {
    let start = Instant::now();
    let mut matched = 0;
    let mut ops = 0;

    for i in 0..num_rounds {
        // Buy
        let buy_order = Order::new(i as u64 * 2, Side::Buy, 50000.0 + (i % 100) as f64, 10.0, TimeInForce::GTC, 0);
        ops += 1;

        // Sell at same price (will match)
        let sell_order = Order::new(i as u64 * 2 + 1, Side::Sell, 50000.0 + (i % 100) as f64, 10.0, TimeInForce::GTC, 0);
        ops += 1;

        if sell_result.filled > 0.0 {
            matched += 1;
        }
    }

    let duration = start.elapsed();
    let tps = ops as f64 / duration.as_secs_f64();
    println!("{}: {:.0} TPS ({} matched, {:.3}ms)", name, tps, matched, duration.as_secs_f64() * 1000.0);
    (tps, duration.as_secs_f64())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 市场数据对撮合引擎的性能影响 ===\n");

    let config = PoolConfig::default();
    let num_rounds = 5000;

    // 测试1: 纯撮合（无任何市场数据处理）
    println!("【测试1】纯撮合 - 无市场数据处理");
    let engine1 = MatchingEngine::new(config.clone())?;
    let (tps1, _) = benchmark_matching("  基线", engine1, num_rounds);

    // 测试2: 添加TradeEvent通道（但不消费）
    println!("\n【测试2】添加TradeEvent通道 - 不消费");
    let mut engine2 = MatchingEngine::new(config.clone())?;
    let (trade_tx, _trade_rx) = RingBuffer::<TradeEvent>::new(1_000_000);
    engine2.set_trade_event_sender(trade_tx);
    let (tps2, _) = benchmark_matching("  有通道但不消费", engine2, num_rounds);

    let degradation_2 = ((tps1 - tps2) / tps1) * 100.0;
    println!("  性能下降: {:.1}%", degradation_2);

    // 测试3: 添加TradeEvent通道 + 创建MarketDataEngine（但不运行）
    println!("\n【测试3】创建MarketDataEngine - 不运行定时器");
    let mut engine3 = MatchingEngine::new(config.clone())?;
    let (trade_tx3, trade_rx3) = RingBuffer::<TradeEvent>::new(1_000_000);
    engine3.set_trade_event_sender(trade_tx3);
    let _market_engine = Arc::new(Mutex::new(MarketDataEngine::new()));

    let (tps3, _) = benchmark_matching("  有MarketDataEngine", engine3, num_rounds);
    let degradation_3 = ((tps1 - tps3) / tps1) * 100.0;
    println!("  性能下降: {:.1}%", degradation_3);

    // 测试4: 发送但不消费（模拟通道积压）
    println!("\n【测试4】TradeEvent不消费（测试通道积压）");
    let mut engine4 = MatchingEngine::new(config.clone())?;
    let (trade_tx4, _trade_rx4_ignore) = RingBuffer::<TradeEvent>::new(1000); // 小缓冲
    engine4.set_trade_event_sender(trade_tx4);

    let (tps4, _) = benchmark_matching("  有限缓冲通道", engine4, num_rounds);
    let degradation_4 = ((tps1 - tps4) / tps1) * 100.0;
    println!("  性能下降: {:.1}%", degradation_4);

    // 性能对比总结
    println!("\n【性能对比总结】");
    println!("{}", "=".repeat(60));
    println!("基线 (纯撮合):              {:.0} TPS", tps1);
    println!("+ TradeEvent通道:          {:.0} TPS ({:.1}% 下降)", tps2, degradation_2);
    println!("+ MarketDataEngine创建:    {:.0} TPS ({:.1}% 下降)", tps3, degradation_3);
    println!("+ 消费线程:                {:.0} TPS ({:.1}% 下降)", tps4, degradation_4);

    // 分析
    println!("\n【分析】");
    if degradation_2 > 10.0 {
        println!("⚠️  TradeEvent发送本身的开销很大 ({:.1}%)", degradation_2);
        println!("   可能原因: try_send()的原子操作或内存屏障");
    } else {
        println!("✓ TradeEvent发送开销不大 ({:.1}%)", degradation_2);
    }

    if degradation_3 > degradation_2 + 5.0 {
        println!("⚠️  MarketDataEngine创建/锁定有额外开销");
    } else {
        println!("✓ MarketDataEngine创建基本无开销");
    }

    if degradation_4 > degradation_3 + 10.0 {
        println!("⚠️  消费线程导致显著性能下降");
        println!("   可能原因: 竞争共享数据的锁，或CPU缓存线冲突");
    } else {
        println!("✓ 消费线程的性能影响在可接受范围内");
    }

    Ok(())
}
