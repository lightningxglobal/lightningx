//! 简单性能测试：单订单 vs 批量撮合

use matching_engine::{MatchingEngine, PoolConfig, Order, Side, TimeInForce};
use std::time::Instant;
use smallvec::SmallVec;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let num_rounds = 5000;

    // 测试1：单订单模式
    println!("【测试1】单订单撮合 - 逐个place_order()");
    let mut engine = MatchingEngine::new(PoolConfig::default())?;

    let start = Instant::now();
    for batch_idx in 0..num_rounds {
        for i in 0..10 {
            let order_idx = batch_idx * 10 + i;
            let buy = Order::new(order_idx as u64 * 2, Side::Buy, 50000.0, 10.0, TimeInForce::GTC, 0);
            let _ = engine.place_order(buy)?;

            let sell = Order::new(order_idx as u64 * 2 + 1, Side::Sell, 50000.0, 10.0, TimeInForce::GTC, 0);
            let _ = engine.place_order(sell)?;
        }
    }
    let single_tps = (num_rounds as f64 * 20.0) / start.elapsed().as_secs_f64();
    println!("  TPS: {:.0}\n", single_tps);

    // 测试2：批量撮合（没有rtrb）
    println!("【测试2】批量撮合 - match_orders_batch()");
    let mut engine = MatchingEngine::new(PoolConfig::default())?;

    let start = Instant::now();
    for batch_idx in 0..num_rounds {
        let mut batch: SmallVec<[Order; 20]> = SmallVec::new();

        for i in 0..10 {
            let order_idx = batch_idx * 10 + i;

            let buy = Order::new(order_idx as u64 * 2, Side::Buy, 50000.0, 10.0, TimeInForce::GTC, 0);
            batch.push(buy);

            let sell = Order::new(order_idx as u64 * 2 + 1, Side::Sell, 50000.0, 10.0, TimeInForce::GTC, 0);
            batch.push(sell);
        }

        let _ = engine.match_orders_batch(batch)?;
    }
    let batch_tps = (num_rounds as f64 * 20.0) / start.elapsed().as_secs_f64();
    println!("  TPS: {:.0}\n", batch_tps);

    println!("【性能对比】");
    println!("  单订单:  {:.0} TPS", single_tps);
    println!("  批量撮合: {:.0} TPS ({:+.1}%)", batch_tps, ((batch_tps - single_tps) / single_tps) * 100.0);

    Ok(())
}
