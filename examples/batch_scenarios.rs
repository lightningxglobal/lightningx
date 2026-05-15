//! 批量订单测试 - 不同场景对比

use matching_engine::{MatchingEngine, PoolConfig, Order, Side, TimeInForce, TradeEvent};
use std::time::Instant;
use rtrb::RingBuffer;
use smallvec::SmallVec;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 批量订单场景对比 ===\n");

    let num_rounds = 5000;

    // 场景1: 单价位 (20个订单全是同一个价格)
    println!("【场景1】单价位批量 - 所有20个订单同价 (50000)");
    let mut engine = MatchingEngine::new(PoolConfig::default())?;
    let (tx, _rx) = RingBuffer::<TradeEvent>::new(1_000_000);
    engine.set_trade_event_sender(tx);

    let start = Instant::now();
    for batch_idx in 0..num_rounds {
        let mut batch: SmallVec<[Order; 20]> = SmallVec::new();

        // 所有订单都是同一个价格50000
        for i in 0..10 {
            let order_idx = batch_idx * 10 + i;

            let buy = Order::new(order_idx as u64 * 2, Side::Buy, 50000.0, 10.0, TimeInForce::GTC, 0);
            batch.push(buy);

            let sell = Order::new(order_idx as u64 * 2 + 1, Side::Sell, 50000.0, 10.0, TimeInForce::GTC, 0);
            batch.push(sell);
        }

        let _ = engine.match_orders_batch(batch)?;
    }
    let single_price_tps = (num_rounds as f64 * 20.0) / start.elapsed().as_secs_f64();
    println!("  TPS: {:.0}\n", single_price_tps);

    // 场景2: 多价位 (20个订单分布在10个价格)
    println!("【场景2】多价位批量 - 20个订单分布在10个不同价格 (50000-50099)");
    let mut engine = MatchingEngine::new(PoolConfig::default())?;
    let (tx, _rx) = RingBuffer::<TradeEvent>::new(1_000_000);
    engine.set_trade_event_sender(tx);

    let start = Instant::now();
    for batch_idx in 0..num_rounds {
        let mut batch: SmallVec<[Order; 20]> = SmallVec::new();

        // 订单分布在不同价位
        for i in 0..10 {
            let order_idx = batch_idx * 10 + i;
            let price = 50000.0 + (order_idx % 100) as f64;

            let buy = Order::new(order_idx as u64 * 2, Side::Buy, price, 10.0, TimeInForce::GTC, 0);
            batch.push(buy);

            let sell = Order::new(order_idx as u64 * 2 + 1, Side::Sell, price, 10.0, TimeInForce::GTC, 0);
            batch.push(sell);
        }

        let _ = engine.match_orders_batch(batch)?;
    }
    let multi_price_tps = (num_rounds as f64 * 20.0) / start.elapsed().as_secs_f64();
    println!("  TPS: {:.0}\n", multi_price_tps);

    // 场景3: 单订单 (逐个下单处理)
    println!("【场景3】单订单 - 逐个place_order处理");
    let mut engine = MatchingEngine::new(PoolConfig::default())?;
    let (tx, _rx) = RingBuffer::<TradeEvent>::new(1_000_000);
    engine.set_trade_event_sender(tx);

    let start = Instant::now();
    for i in 0..num_rounds * 10 {
        let price = 50000.0 + (i % 100) as f64;

        let buy = Order::new(i as u64 * 2, Side::Buy, price, 10.0, TimeInForce::GTC, 0);
        let _ = engine.place_order(buy)?;

        let sell = Order::new(i as u64 * 2 + 1, Side::Sell, price, 10.0, TimeInForce::GTC, 0);
        let _ = engine.place_order(sell)?;
    }
    let single_order_tps = (num_rounds as f64 * 20.0) / start.elapsed().as_secs_f64();
    println!("  TPS: {:.0}\n", single_order_tps);

    // 对比
    println!("【性能对比总结】");
    println!("  单价位批量:    {:.0} TPS", single_price_tps);
    println!("  多价位批量:    {:.0} TPS ({:+.1}%)", multi_price_tps, ((multi_price_tps - single_price_tps) / single_price_tps) * 100.0);
    println!("  单订单模式:    {:.0} TPS ({:+.1}%)", single_order_tps, ((single_order_tps - single_price_tps) / single_price_tps) * 100.0);

    println!("\n  批量 vs 单订单: {:.1}倍性能提升", single_price_tps / single_order_tps);
    println!("  目标: 7,000,000 TPS");

    Ok(())
}
