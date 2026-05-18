//! 精确定位瓶颈 - 对比不同场景的性能

use lightning_exchange::{MatchingEngine, PoolConfig, Order, Side, TimeInForce, TradeEvent, MarketDataEngine};
use std::sync::Arc;
use std::time::Instant;
use parking_lot::Mutex;
use rtrb::RingBuffer;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 瓶颈精确定位分析 ===\n");
    
    let num_rounds = 5000;
    let price_range = 100;
    
    // 场景1：纯撮合，无任何市场数据
    println!("【场景1】纯撮合（无市场数据，无rtrb）");
    let mut engine1 = MatchingEngine::new(PoolConfig::default())?;
    let start = Instant::now();
    for i in 0..num_rounds {
        let price = 50000.0 + (i % price_range) as f64;
        let buy = Order::new(i as u64 * 2, Side::Buy, price, 10.0, TimeInForce::GTC, 0);
        let _ = engine1.place_order(buy)?;
        let sell = Order::new(i as u64 * 2 + 1, Side::Sell, price, 10.0, TimeInForce::GTC, 0);
        let _ = engine1.place_order(sell)?;
    }
    let time1 = start.elapsed();
    let tps1 = (num_rounds as f64 * 2.0) / time1.as_secs_f64();
    println!("  TPS: {:.0}", tps1);
    
    // 场景2：撮合 + rtrb发送，但不读取
    println!("\n【场景2】撮合 + rtrb发送，无消费");
    let mut engine2 = MatchingEngine::new(PoolConfig::default())?;
    let (trade_tx, _trade_rx) = RingBuffer::<TradeEvent>::new(1_000_000);
    engine2.set_trade_event_sender(trade_tx);
    let start = Instant::now();
    for i in 0..num_rounds {
        let price = 50000.0 + (i % price_range) as f64;
        let buy = Order::new(i as u64 * 2, Side::Buy, price, 10.0, TimeInForce::GTC, 0);
        let _ = engine2.place_order(buy)?;
        let sell = Order::new(i as u64 * 2 + 1, Side::Sell, price, 10.0, TimeInForce::GTC, 0);
        let _ = engine2.place_order(sell)?;
    }
    let time2 = start.elapsed();
    let tps2 = (num_rounds as f64 * 2.0) / time2.as_secs_f64();
    println!("  TPS: {:.0}", tps2);
    println!("  rtrb发送开销: {:+.1}%", ((tps1 - tps2) / tps1) * 100.0);
    
    // 场景3：撮合 + rtrb发送 + 热路径消费
    println!("\n【场景3】撮合 + rtrb发送 + 热路径消费（无Mutex）");
    let mut engine3 = MatchingEngine::new(PoolConfig::default())?;
    let (trade_tx3, mut trade_rx3) = RingBuffer::<TradeEvent>::new(1_000_000);
    engine3.set_trade_event_sender(trade_tx3);
    let mut market_engine3 = MarketDataEngine::new();
    
    let start = Instant::now();
    for i in 0..num_rounds {
        let price = 50000.0 + (i % price_range) as f64;
        let buy = Order::new(i as u64 * 2, Side::Buy, price, 10.0, TimeInForce::GTC, 0);
        let _ = engine3.place_order(buy)?;
        let sell = Order::new(i as u64 * 2 + 1, Side::Sell, price, 10.0, TimeInForce::GTC, 0);
        let _ = engine3.place_order(sell)?;
        
        // 直接消费，无Mutex
        while let Ok(trade) = trade_rx3.pop() {
            market_engine3.consume_trade_event(trade);
        }
    }
    let time3 = start.elapsed();
    let tps3 = (num_rounds as f64 * 2.0) / time3.as_secs_f64();
    println!("  TPS: {:.0}", tps3);
    println!("  市场数据处理开销: {:+.1}%", ((tps1 - tps3) / tps1) * 100.0);
    
    // 场景4：撮合 + rtrb + Mutex保护的市场数据（真实场景）
    println!("\n【场景4】撮合 + rtrb + Mutex保护的市场数据（真实）");
    let mut engine4 = MatchingEngine::new(PoolConfig::default())?;
    let (trade_tx4, mut trade_rx4) = RingBuffer::<TradeEvent>::new(1_000_000);
    engine4.set_trade_event_sender(trade_tx4);
    let market_engine4 = Arc::new(Mutex::new(MarketDataEngine::new()));
    
    let start = Instant::now();
    for i in 0..num_rounds {
        let price = 50000.0 + (i % price_range) as f64;
        let buy = Order::new(i as u64 * 2, Side::Buy, price, 10.0, TimeInForce::GTC, 0);
        let _ = engine4.place_order(buy)?;
        let sell = Order::new(i as u64 * 2 + 1, Side::Sell, price, 10.0, TimeInForce::GTC, 0);
        let _ = engine4.place_order(sell)?;
        
        // 有Mutex的市场数据处理
        while let Ok(trade) = trade_rx4.pop() {
            let mut md = market_engine4.lock();
            md.consume_trade_event(trade);
        }
    }
    let time4 = start.elapsed();
    let tps4 = (num_rounds as f64 * 2.0) / time4.as_secs_f64();
    println!("  TPS: {:.0}", tps4);
    println!("  Mutex开销: {:+.1}%", ((tps3 - tps4) / tps3) * 100.0);
    println!("  总开销: {:+.1}%", ((tps1 - tps4) / tps1) * 100.0);
    
    println!("\n【总结】");
    println!("  场景1 (纯撮合):     {:.0} TPS (基准)", tps1);
    println!("  场景2 (+rtrb发送):  {:.0} TPS ({:+.1}%)", tps2, ((tps1 - tps2) / tps1) * 100.0);
    println!("  场景3 (+市场数据):  {:.0} TPS ({:+.1}%)", tps3, ((tps1 - tps3) / tps1) * 100.0);
    println!("  场景4 (+Mutex):     {:.0} TPS ({:+.1}%)", tps4, ((tps1 - tps4) / tps1) * 100.0);
    
    Ok(())
}
