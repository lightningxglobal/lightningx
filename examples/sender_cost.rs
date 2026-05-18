//! 测试 set_trade_event_sender 的真实成本

use lightning_exchange::{MatchingEngine, PoolConfig, Order, Side, TimeInForce, TradeEvent};
use std::time::Instant;
use rtrb::RingBuffer;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 分析 set_trade_event_sender 的性能成本 ===\n");

    let config = PoolConfig::default();
    let num_rounds = 5000;

    // ========== 对照1: 完全不设置sender ==========
    println!("【对照1】不设置 trade_event_sender");
    let mut engine1 = MatchingEngine::new(config.clone())?;
    // 不调用 set_trade_event_sender

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

    // ========== 对照2: 设置sender但不消费 ==========
    println!("【对照2】设置 trade_event_sender (unbounded, 不消费)");
    let mut engine2 = MatchingEngine::new(config.clone())?;
    let (trade_tx, _trade_rx) = RingBuffer::<TradeEvent>::new(1_000_000);
    engine2.set_trade_event_sender(trade_tx);

    let start2 = Instant::now();
    for i in 0..num_rounds {
        let buy = Order::new(i as u64 * 2, Side::Buy, 50000.0 + (i % 100) as f64, 10.0, TimeInForce::GTC, 0);
        let _ = engine2.place_order(buy)?;
        let sell = Order::new(i as u64 * 2 + 1, Side::Sell, 50000.0 + (i % 100) as f64, 10.0, TimeInForce::GTC, 0);
        let _ = engine2.place_order(sell)?;
    }
    let duration2 = start2.elapsed();
    let tps2 = (num_rounds as f64 * 2.0) / duration2.as_secs_f64();

    println!("耗时:  {:.3}ms", duration2.as_secs_f64() * 1000.0);
    println!("TPS:   {:.0}", tps2);
    let degradation = ((tps1 - tps2) / tps1) * 100.0;
    println!("性能下降: {:.1}%\n", degradation);

    // ========== 对照3: 设置sender,超大容量有限缓冲 ==========
    println!("【对照3】设置 trade_event_sender (capacity=100000)");
    let mut engine3 = MatchingEngine::new(config.clone())?;
    let (trade_tx3, _trade_rx3) = RingBuffer::<TradeEvent>::new(100_000);
    engine3.set_trade_event_sender(trade_tx3);

    let start3 = Instant::now();
    for i in 0..num_rounds {
        let buy = Order::new(i as u64 * 2, Side::Buy, 50000.0 + (i % 100) as f64, 10.0, TimeInForce::GTC, 0);
        let _ = engine3.place_order(buy)?;
        let sell = Order::new(i as u64 * 2 + 1, Side::Sell, 50000.0 + (i % 100) as f64, 10.0, TimeInForce::GTC, 0);
        let _ = engine3.place_order(sell)?;
    }
    let duration3 = start3.elapsed();
    let tps3 = (num_rounds as f64 * 2.0) / duration3.as_secs_f64();

    println!("耗时:  {:.3}ms", duration3.as_secs_f64() * 1000.0);
    println!("TPS:   {:.0}", tps3);
    let degradation3 = ((tps1 - tps3) / tps1) * 100.0;
    println!("性能下降: {:.1}%\n", degradation3);

    // ========== 对照4: 设置sender,小容量通道(100) ==========
    println!("【对照4】设置 trade_event_sender (capacity=100, 通道可能满)");
    let mut engine4 = MatchingEngine::new(config.clone())?;
    let (trade_tx4, _trade_rx4) = RingBuffer::<TradeEvent>::new(100);
    engine4.set_trade_event_sender(trade_tx4);

    let start4 = Instant::now();
    for i in 0..num_rounds {
        let buy = Order::new(i as u64 * 2, Side::Buy, 50000.0 + (i % 100) as f64, 10.0, TimeInForce::GTC, 0);
        let _ = engine4.place_order(buy)?;
        let sell = Order::new(i as u64 * 2 + 1, Side::Sell, 50000.0 + (i % 100) as f64, 10.0, TimeInForce::GTC, 0);
        let _ = engine4.place_order(sell)?;
    }
    let duration4 = start4.elapsed();
    let tps4 = (num_rounds as f64 * 2.0) / duration4.as_secs_f64();

    println!("耗时:  {:.3}ms", duration4.as_secs_f64() * 1000.0);
    println!("TPS:   {:.0}", tps4);
    let degradation4 = ((tps1 - tps4) / tps1) * 100.0;
    println!("性能下降: {:.1}%\n", degradation4);

    // ========== 总结 ==========
    println!("【总结】");
    println!("{}", "=".repeat(60));
    println!("无sender (对照):         {:.0} TPS", tps1);
    println!("+ unbounded sender:      {:.0} TPS ({:.1}% 下降)", tps2, degradation);
    println!("+ bounded(100k) sender:  {:.0} TPS ({:.1}% 下降)", tps3, degradation3);
    println!("+ bounded(100) sender:   {:.0} TPS ({:.1}% 下降)", tps4, degradation4);

    Ok(())
}
