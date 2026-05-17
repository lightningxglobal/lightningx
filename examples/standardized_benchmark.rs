//! 标准化基准测试 - 所有场景用完全相同条件

use matching_engine::{MatchingEngine, PoolConfig, Order, Side, TimeInForce, TradeEvent, MarketDataEngine};
use std::time::Instant;
use rtrb::RingBuffer;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 标准化基准测试（相同条件） ===\n");
    println!("测试条件：");
    println!("  - 10,000轮（每轮1买1卖）");
    println!("  - 20,000笔订单总数");
    println!("  - 价格范围：50000-50099（100档）");
    println!("  - 完全匹配（每个买单都被对应的卖单成交）");
    println!("  - 无不必要的Mutex/Arc");
    println!();

    let num_rounds = 10000;
    let price_range = 100;

    // 场景1: 纯撮合（无市场数据）
    println!("【场景1】纯撮合（无rtrb，无市场数据）");
    let mut engine = MatchingEngine::new(PoolConfig::default())?;
    let start = Instant::now();
    for i in 0..num_rounds {
        let price = 50000.0 + (i % price_range) as f64;
        let buy = Order::new(i as u64 * 2, Side::Buy, price, 10.0, TimeInForce::GTC, 0);
        engine.place_order(buy)?;
        let sell = Order::new(i as u64 * 2 + 1, Side::Sell, price, 10.0, TimeInForce::GTC, 0);
        engine.place_order(sell)?;
    }
    let tps1 = (num_rounds as f64 * 2.0) / start.elapsed().as_secs_f64();
    println!("  TPS: {:.0}\n", tps1);

    // 场景2: 有rtrb但不读取
    println!("【场景2】撮合 + rtrb发送（无消费）");
    let mut engine = MatchingEngine::new(PoolConfig::default())?;
    let (tx, _rx) = RingBuffer::<TradeEvent>::new(1_000_000);
    engine.set_trade_event_sender(tx);
    let start = Instant::now();
    for i in 0..num_rounds {
        let price = 50000.0 + (i % price_range) as f64;
        let buy = Order::new(i as u64 * 2, Side::Buy, price, 10.0, TimeInForce::GTC, 0);
        engine.place_order(buy)?;
        let sell = Order::new(i as u64 * 2 + 1, Side::Sell, price, 10.0, TimeInForce::GTC, 0);
        engine.place_order(sell)?;
    }
    let tps2 = (num_rounds as f64 * 2.0) / start.elapsed().as_secs_f64();
    println!("  TPS: {:.0}", tps2);
    println!("  开销: {:+.1}%\n", ((tps1 - tps2) / tps1) * 100.0);

    // 场景3: rtrb + 热路径消费（无Mutex）
    println!("【场景3】撮合 + rtrb + 市场数据热路径消费");
    let mut engine = MatchingEngine::new(PoolConfig::default())?;
    let (tx, mut rx) = RingBuffer::<TradeEvent>::new(1_000_000);
    engine.set_trade_event_sender(tx);
    let mut market_engine = MarketDataEngine::new();
    let start = Instant::now();
    for i in 0..num_rounds {
        let price = 50000.0 + (i % price_range) as f64;
        let buy = Order::new(i as u64 * 2, Side::Buy, price, 10.0, TimeInForce::GTC, 0);
        engine.place_order(buy)?;
        let sell = Order::new(i as u64 * 2 + 1, Side::Sell, price, 10.0, TimeInForce::GTC, 0);
        engine.place_order(sell)?;
        
        while let Ok(trade) = rx.pop() {
            market_engine.consume_trade_event(trade);
        }
    }
    let tps3 = (num_rounds as f64 * 2.0) / start.elapsed().as_secs_f64();
    println!("  TPS: {:.0}", tps3);
    println!("  开销: {:+.1}%\n", ((tps1 - tps3) / tps1) * 100.0);

    // 场景4: rtrb + 批量消费（先撮合，后处理）
    println!("【场景4】撮合 + rtrb + 批量处理市场数据");
    let mut engine = MatchingEngine::new(PoolConfig::default())?;
    let (tx, mut rx) = RingBuffer::<TradeEvent>::new(1_000_000);
    engine.set_trade_event_sender(tx);
    let start = Instant::now();
    for i in 0..num_rounds {
        let price = 50000.0 + (i % price_range) as f64;
        let buy = Order::new(i as u64 * 2, Side::Buy, price, 10.0, TimeInForce::GTC, 0);
        engine.place_order(buy)?;
        let sell = Order::new(i as u64 * 2 + 1, Side::Sell, price, 10.0, TimeInForce::GTC, 0);
        engine.place_order(sell)?;
    }
    let matching_time = start.elapsed();

    let batch_start = Instant::now();
    let mut market_engine = MarketDataEngine::new();
    let mut trade_count = 0;
    while let Ok(trade) = rx.pop() {
        market_engine.consume_trade_event(trade);
        trade_count += 1;
    }
    let batch_time = batch_start.elapsed();
    let total_time = matching_time + batch_time;
    let tps4 = (num_rounds as f64 * 2.0) / total_time.as_secs_f64();
    println!("  撮合时间: {:.3}ms, 处理市场数据: {:.3}ms", matching_time.as_secs_f64() * 1000.0, batch_time.as_secs_f64() * 1000.0);
    println!("  TPS: {:.0}", tps4);
    println!("  开销: {:+.1}%\n", ((tps1 - tps4) / tps1) * 100.0);

    println!("【标准化结果汇总】");
    println!("  场景1 (纯撮合):        {:.0} TPS", tps1);
    println!("  场景2 (+rtrb):         {:.0} TPS ({:+.1}%)", tps2, ((tps1 - tps2) / tps1) * 100.0);
    println!("  场景3 (+市场数据热路): {:.0} TPS ({:+.1}%)", tps3, ((tps1 - tps3) / tps1) * 100.0);
    println!("  场景4 (+市场数据批量): {:.0} TPS ({:+.1}%)", tps4, ((tps1 - tps4) / tps1) * 100.0);
    println!("\n  目标: 7,000,000 TPS");

    Ok(())
}
