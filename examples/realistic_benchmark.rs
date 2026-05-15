//! 真实场景基准测试
//! 包含完整的成交→行情处理流程（模拟生产环境）

use matching_engine::{MatchingEngine, PoolConfig, Order, Side, TimeInForce, TradeEvent, MarketDataEngine};
use std::time::Instant;
use rtrb::RingBuffer;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 真实场景基准测试 ===\n");

    // 测试1: 纯撮合（无行情处理）
    println!("【测试1】纯撮合 - 无行情处理");
    let mut engine1 = MatchingEngine::new(PoolConfig::default())?;
    
    let num_rounds = 5000;
    let start = Instant::now();
    for i in 0..num_rounds {
        let price = 50000.0 + (i % 100) as f64;
        let buy = Order::new(i as u64 * 2, Side::Buy, price, 10.0, TimeInForce::GTC, 0);
        let _ = engine1.place_order(buy)?;
        let sell = Order::new(i as u64 * 2 + 1, Side::Sell, price, 10.0, TimeInForce::GTC, 0);
        let _ = engine1.place_order(sell)?;
    }
    let duration1 = start.elapsed();
    let tps1 = (num_rounds as f64 * 2.0) / duration1.as_secs_f64();
    println!("  TPS: {:.0} ({:.1}K)", tps1, tps1/1000.0);
    println!("  延迟: {:.3}μs\n", duration1.as_secs_f64() * 1_000_000.0 / (num_rounds as f64 * 2.0));

    // 测试2: 撮合 + 行情处理（热路径中）
    println!("【测试2】撮合 + 同步行情处理（无不必要的Mutex）");
    let mut engine2 = MatchingEngine::new(PoolConfig::default())?;
    let (trade_tx2, mut trade_rx2) = RingBuffer::<TradeEvent>::new(1_000_000);
    engine2.set_trade_event_sender(trade_tx2);

    let mut market_engine2 = MarketDataEngine::new();  // 直接分配，不需要Mutex

    let start = Instant::now();
    for i in 0..num_rounds {
        let price = 50000.0 + (i % 100) as f64;
        let buy = Order::new(i as u64 * 2, Side::Buy, price, 10.0, TimeInForce::GTC, 0);
        let _ = engine2.place_order(buy)?;

        let sell = Order::new(i as u64 * 2 + 1, Side::Sell, price, 10.0, TimeInForce::GTC, 0);
        let _ = engine2.place_order(sell)?;

        // 在热路径中处理行情事件（直接消费，无Mutex）
        while let Ok(trade) = trade_rx2.pop() {
            market_engine2.consume_trade_event(trade);
        }
    }
    let duration2 = start.elapsed();
    let tps2 = (num_rounds as f64 * 2.0) / duration2.as_secs_f64();
    let overhead_percent = ((tps1 - tps2) / tps1) * 100.0;
    println!("  TPS: {:.0} ({:.1}K)", tps2, tps2/1000.0);
    println!("  延迟: {:.3}μs", duration2.as_secs_f64() * 1_000_000.0 / (num_rounds as f64 * 2.0));
    println!("  行情开销: {:.1}%\n", overhead_percent);

    // 测试3: 撮合 + 异步行情处理（批量）
    println!("【测试3】撮合 + 批量行情处理（无不必要的Mutex）");
    let mut engine3 = MatchingEngine::new(PoolConfig::default())?;
    let (trade_tx3, mut trade_rx3) = RingBuffer::<TradeEvent>::new(1_000_000);
    engine3.set_trade_event_sender(trade_tx3);

    let mut market_engine3 = MarketDataEngine::new();  // 直接分配

    let start = Instant::now();
    for i in 0..num_rounds {
        let price = 50000.0 + (i % 100) as f64;
        let buy = Order::new(i as u64 * 2, Side::Buy, price, 10.0, TimeInForce::GTC, 0);
        let _ = engine3.place_order(buy)?;

        let sell = Order::new(i as u64 * 2 + 1, Side::Sell, price, 10.0, TimeInForce::GTC, 0);
        let _ = engine3.place_order(sell)?;
    }

    // 撮合完成后再批量处理行情
    let batch_start = Instant::now();
    while let Ok(trade) = trade_rx3.pop() {
        market_engine3.consume_trade_event(trade);
    }
    let batch_duration = batch_start.elapsed();
    let duration3 = start.elapsed();
    let tps3 = (num_rounds as f64 * 2.0) / duration3.as_secs_f64();
    println!("  TPS: {:.0} ({:.1}K)", tps3, tps3/1000.0);
    println!("  延迟: {:.3}μs (含批处理{:.1}ms)", 
        duration3.as_secs_f64() * 1_000_000.0 / (num_rounds as f64 * 2.0),
        batch_duration.as_secs_f64() * 1000.0);
    println!("  行情开销: {:.1}%\n", ((tps1 - tps3) / tps1) * 100.0);

    // 总结
    println!("【对标总结】");
    println!("  纯撮合:      {:.0} TPS ({:.1}K)", tps1, tps1/1000.0);
    println!("  +同步行情:   {:.0} TPS ({:.1}K)  开销{:.1}%", tps2, tps2/1000.0, overhead_percent);
    println!("  +批量行情:   {:.0} TPS ({:.1}K)  开销{:.1}%", tps3, tps3/1000.0, ((tps1 - tps3) / tps1) * 100.0);
    println!("\n  目标:        7,000,000 TPS");

    Ok(())
}
