//! 批量订单撮合基准测试 - 对比单订单vs批量(20个)撮合

use matching_engine::{MatchingEngine, PoolConfig, Order, Side, TimeInForce, TradeEvent};
use std::time::Instant;
use rtrb::RingBuffer;
use smallvec::SmallVec;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 批量订单撮合基准测试 ===\n");
    println!("测试条件：");
    println!("  - 10,000轮测试");
    println!("  - 20,000笔订单总数");
    println!("  - 价格范围：50000-50099（100档）");
    println!("  - 完全匹配（每个买单都被对应的卖单成交）");
    println!();

    let num_rounds = 10000;
    let price_range = 100;

    // 测试1: 单订单撮合
    println!("【测试1】单订单撮合 - 逐个place_order()");
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
    let single_tps = (num_rounds as f64 * 2.0) / start.elapsed().as_secs_f64();
    println!("  TPS: {:.0}\n", single_tps);

    // 测试2: 批量撮合（20个订单一批）
    println!("【测试2】批量撮合 - match_orders_batch() (20个/批)");
    let mut engine = MatchingEngine::new(PoolConfig::default())?;
    let (tx, _rx) = RingBuffer::<TradeEvent>::new(1_000_000);
    engine.set_trade_event_sender(tx);

    let start = Instant::now();
    for batch_idx in 0..num_rounds {
        let mut batch: SmallVec<[Order; 20]> = SmallVec::new();

        // 构造20个订单的batch（10个buy + 10个sell，配对）
        for i in 0..10 {
            let order_idx = batch_idx * 10 + i;
            let price = 50000.0 + (order_idx % price_range) as f64;

            let buy = Order::new(order_idx as u64 * 2, Side::Buy, price, 10.0, TimeInForce::GTC, 0);
            batch.push(buy);

            let sell = Order::new(order_idx as u64 * 2 + 1, Side::Sell, price, 10.0, TimeInForce::GTC, 0);
            batch.push(sell);
        }

        let _ = engine.match_orders_batch(batch)?;
    }
    let batch_tps = (num_rounds as f64 * 20.0) / start.elapsed().as_secs_f64();
    let improvement = ((batch_tps - single_tps) / single_tps) * 100.0;
    println!("  TPS: {:.0}", batch_tps);
    println!("  性能提升: {:+.1}%\n", improvement);

    // 测试3: place_orders()批量API（通过handlers）
    println!("【测试3】place_orders() API - 批量处理（20个/请求）");
    let mut engine = MatchingEngine::new(PoolConfig::default())?;
    let (tx, _rx) = RingBuffer::<TradeEvent>::new(1_000_000);
    engine.set_trade_event_sender(tx);

    let start = Instant::now();
    for batch_idx in 0..num_rounds {
        let mut batch: SmallVec<[Order; 20]> = SmallVec::new();

        for i in 0..10 {
            let order_idx = batch_idx * 10 + i;
            let price = 50000.0 + (order_idx % price_range) as f64;

            let buy = Order::new(order_idx as u64 * 2, Side::Buy, price, 10.0, TimeInForce::GTC, 0);
            batch.push(buy);

            let sell = Order::new(order_idx as u64 * 2 + 1, Side::Sell, price, 10.0, TimeInForce::GTC, 0);
            batch.push(sell);
        }

        let _ = engine.place_orders(batch)?;
    }
    let place_orders_tps = (num_rounds as f64 * 20.0) / start.elapsed().as_secs_f64();
    let place_improvement = ((place_orders_tps - single_tps) / single_tps) * 100.0;
    println!("  TPS: {:.0}", place_orders_tps);
    println!("  性能提升: {:+.1}%\n", place_improvement);

    // 汇总对比
    println!("【性能对比汇总】");
    println!("  单订单模式:        {:.0} TPS (基线)", single_tps);
    println!("  match_orders_batch: {:.0} TPS ({:+.1}%)", batch_tps, improvement);
    println!("  place_orders:       {:.0} TPS ({:+.1}%)", place_orders_tps, place_improvement);
    println!("\n  目标: 7,000,000 TPS");

    Ok(())
}
