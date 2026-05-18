//! 瓶颈精确定位分析 - 修正版（无不必要的Mutex）

use lightning_exchange::{MatchingEngine, PoolConfig, Order, Side, TimeInForce, TradeEvent, MarketDataEngine};
use std::time::Instant;
use rtrb::RingBuffer;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 瓶颈精确定位分析 (修正版) ===\n");
    
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
    println!("  rtrb.push()开销: {:+.1}%", ((tps1 - tps2) / tps1) * 100.0);
    
    // 场景3：撮合 + rtrb发送 + 热路径消费（无Mutex！）
    println!("\n【场景3】撮合 + rtrb + 市场数据消费（无Mutex）");
    let mut engine3 = MatchingEngine::new(PoolConfig::default())?;
    let (trade_tx3, mut trade_rx3) = RingBuffer::<TradeEvent>::new(1_000_000);
    engine3.set_trade_event_sender(trade_tx3);
    let mut market_engine3 = MarketDataEngine::new();  // ← 直接stack分配，无Arc/Mutex
    
    let start = Instant::now();
    for i in 0..num_rounds {
        let price = 50000.0 + (i % price_range) as f64;
        let buy = Order::new(i as u64 * 2, Side::Buy, price, 10.0, TimeInForce::GTC, 0);
        let _ = engine3.place_order(buy)?;
        let sell = Order::new(i as u64 * 2 + 1, Side::Sell, price, 10.0, TimeInForce::GTC, 0);
        let _ = engine3.place_order(sell)?;
        
        // 直接消费，无Mutex，无Arc
        while let Ok(trade) = trade_rx3.pop() {
            market_engine3.consume_trade_event(trade);
        }
    }
    let time3 = start.elapsed();
    let tps3 = (num_rounds as f64 * 2.0) / time3.as_secs_f64();
    println!("  TPS: {:.0}", tps3);
    println!("  市场数据处理开销: {:+.1}%", ((tps1 - tps3) / tps1) * 100.0);
    
    println!("\n【总结】");
    println!("  场景1 (纯撮合):      {:.0} TPS (基准)", tps1);
    println!("  场景2 (+rtrb.push):  {:.0} TPS ({:+.1}% 开销)", tps2, ((tps1 - tps2) / tps1) * 100.0);
    println!("  场景3 (+市场数据):   {:.0} TPS ({:+.1}% 开销)", tps3, ((tps1 - tps3) / tps1) * 100.0);
    
    println!("\n【关键发现】");
    println!("  ✅ 无Mutex：市场数据本身只需要 {:.1}% 开销", ((tps2 - tps3) / tps2) * 100.0);
    println!("  ⚠️  rtrb.push()是最大瓶颈：损失 {:.1}%", ((tps1 - tps2) / tps1) * 100.0);
    println!("  💡 在单线程场景下，Mutex是完全不必要的");

    Ok(())
}
