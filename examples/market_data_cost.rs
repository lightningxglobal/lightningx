//! 测试市场数据处理的真实成本

use lightning_exchange::{MatchingEngine, PoolConfig, Order, Side, TimeInForce, TradeEvent, MarketDataEngine};
use std::time::Instant;
use std::sync::Arc;
use parking_lot::Mutex;
use rtrb::RingBuffer;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 市场数据处理成本分析 ===\n");

    let config = PoolConfig::default();
    let num_rounds = 5000;

    // ========== 测试1: 纯撮合 ==========
    println!("【测试1】纯撮合 (baseline)");
    let mut engine1 = MatchingEngine::new(config.clone())?;

    let start1 = Instant::now();
    for i in 0..num_rounds {
        let buy = Order::new(i as u64 * 2, Side::Buy, 50000.0 + (i % 100) as f64, 10.0, TimeInForce::GTC, 0);
        let _ = engine1.place_order(buy)?;
        let sell = Order::new(i as u64 * 2 + 1, Side::Sell, 50000.0 + (i % 100) as f64, 10.0, TimeInForce::GTC, 0);
        let _ = engine1.place_order(sell)?;
    }
    let duration1 = start1.elapsed();
    let tps1 = (num_rounds as f64 * 2.0) / duration1.as_secs_f64();

    println!("耗时:  {:.3}ms", duration1.as_secs_f64() * 1000.0);
    println!("TPS:   {:.0}\n", tps1);

    // ========== 测试2: 撮合 + TradeEvent发送 + 纯consume_trade_event调用 ==========
    println!("【测试2】撮合 + consume_trade_event");
    let mut engine2 = MatchingEngine::new(config.clone())?;
    let (trade_tx, mut trade_rx) = RingBuffer::<TradeEvent>::new(1_000_000);
    engine2.set_trade_event_sender(trade_tx);

    // 创建一个虚拟的市场数据引擎（不消费通道）
    let (_, _dummy_rx) = RingBuffer::<TradeEvent>::new(1_000_000);
    let market_engine = Arc::new(Mutex::new(MarketDataEngine::new()));

    let mut trades_collected = Vec::new();
    let start2 = Instant::now();

    // 生成trade事件
    for i in 0..num_rounds {
        let buy = Order::new(i as u64 * 2, Side::Buy, 50000.0 + (i % 100) as f64, 10.0, TimeInForce::GTC, 0);
        let _ = engine2.place_order(buy)?;
        let sell = Order::new(i as u64 * 2 + 1, Side::Sell, 50000.0 + (i % 100) as f64, 10.0, TimeInForce::GTC, 0);
        let _ = engine2.place_order(sell)?;

        // 收集事件
        while let Ok(event) = trade_rx.pop() {
            trades_collected.push(event);
        }
    }

    let duration2a = start2.elapsed();
    let tps2a = (num_rounds as f64 * 2.0) / duration2a.as_secs_f64();

    println!("第1部分 (生成+收集): {:.3}ms, TPS: {:.0}", duration2a.as_secs_f64() * 1000.0, tps2a);

    // 现在处理收集到的events（这里使用已收集的events，不需要再从通道pop）
    let start2b = Instant::now();
    for event in trades_collected.iter() {
        let mut md = market_engine.lock();
        md.consume_trade_event(*event);
    }
    let duration2b = start2b.elapsed();

    println!("第2部分 (处理事件): {:.3}ms, 处理了 {} 个事件",
        duration2b.as_secs_f64() * 1000.0, trades_collected.len());

    let duration2_total = duration2a + duration2b;
    let tps2_total = (num_rounds as f64 * 2.0) / duration2_total.as_secs_f64();

    println!("总耗时:  {:.3}ms", duration2_total.as_secs_f64() * 1000.0);
    println!("总TPS:   {:.0}\n", tps2_total);

    // ========== 分析 ==========
    println!("【性能分析】");
    println!("{}", "=".repeat(60));
    println!("基线 (纯撮合):       {:.0} TPS", tps1);
    println!("撮合+市场数据:       {:.0} TPS", tps2_total);
    println!("下降百分比:         {:.1}%", ((tps1 - tps2_total) / tps1) * 100.0);
    println!();

    if tps2a < tps1 * 0.8 {
        println!("⚠️  TradeEvent 发送/收集导致 {:.1}% 性能下降",
            ((tps1 - tps2a) / tps1) * 100.0);
        println!("   可能原因: try_recv() 的原子操作开销");
    } else {
        println!("✓ TradeEvent 发送/收集开销可接受 ({:.1}% 下降)",
            ((tps1 - tps2a) / tps1) * 100.0);
    }

    let processed_per_ms = (trades_collected.len() as f64) / duration2b.as_secs_f64() / 1000.0;
    println!("\nmarket_data consume_trade_event:");
    println!("  事件数:  {}", trades_collected.len());
    println!("  耗时:    {:.3}ms", duration2b.as_secs_f64() * 1000.0);
    println!("  吞吐:    {:.0} events/ms", processed_per_ms);

    Ok(())
}
