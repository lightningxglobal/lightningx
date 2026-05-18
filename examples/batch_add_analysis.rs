//! 分析batch add_to_book()的开销

use lightning_exchange::{MatchingEngine, PoolConfig, Order, Side, TimeInForce};
use std::time::Instant;
use smallvec::SmallVec;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Batch add_to_book() 开销分析 ===\n");

    let num_rounds = 1000;

    // 场景1: 完全匹配（无unfilled）
    println!("【场景1】完全匹配 - 所有订单都被成交，无add_to_book()");
    let mut engine = MatchingEngine::new(PoolConfig::default())?;

    let start = Instant::now();
    for batch_idx in 0..num_rounds {
        let mut batch: SmallVec<[Order; 20]> = SmallVec::new();

        // 构造完全配对：10个buy同价 + 10个sell同价
        for i in 0..10 {
            let buy = Order::new(batch_idx as u64 * 20 + i as u64 * 2, Side::Buy, 50000.0, 10.0, TimeInForce::GTC, 0);
            batch.push(buy);

            let sell = Order::new(batch_idx as u64 * 20 + i as u64 * 2 + 1, Side::Sell, 50000.0, 10.0, TimeInForce::GTC, 0);
            batch.push(sell);
        }

        let _ = engine.match_orders_batch(batch)?;
    }
    let perfect_match_tps = (num_rounds as f64 * 20.0) / start.elapsed().as_secs_f64();
    println!("  TPS: {:.0} (无add_to_book调用)\n", perfect_match_tps);

    // 场景2: 只有sell被匹配，buy留在book
    println!("【场景2】单向匹配 - 10个buy留在book，10个sell匹配");
    let mut engine = MatchingEngine::new(PoolConfig::default())?;

    let start = Instant::now();
    for batch_idx in 0..num_rounds {
        let mut batch: SmallVec<[Order; 20]> = SmallVec::new();

        // 先放10个buy（同价）
        for i in 0..10 {
            let buy = Order::new(batch_idx as u64 * 20 + i as u64 * 2, Side::Buy, 50000.0, 10.0, TimeInForce::GTC, 0);
            batch.push(buy);
        }

        // 再放10个sell（同价，高于buy）- 这样就无法匹配
        for i in 0..10 {
            let sell = Order::new(batch_idx as u64 * 20 + 100 + i as u64, Side::Sell, 50001.0, 10.0, TimeInForce::GTC, 0);
            batch.push(sell);
        }

        let _ = engine.match_orders_batch(batch)?;
    }
    let no_match_tps = (num_rounds as f64 * 20.0) / start.elapsed().as_secs_f64();
    println!("  TPS: {:.0} (20个add_to_book调用)\n", no_match_tps);

    // 场景3: 部分匹配（每个buy匹配不足）
    println!("【场景3】部分匹配 - 每个订单都有剩余需要add_to_book");
    let mut engine = MatchingEngine::new(PoolConfig::default())?;

    let start = Instant::now();
    for batch_idx in 0..num_rounds {
        let mut batch: SmallVec<[Order; 20]> = SmallVec::new();

        // 构造部分匹配：10个buy(100量) + 10个sell(10量)
        for i in 0..10 {
            let buy = Order::new(batch_idx as u64 * 20 + i as u64 * 2, Side::Buy, 50000.0, 100.0, TimeInForce::GTC, 0);
            batch.push(buy);

            let sell = Order::new(batch_idx as u64 * 20 + i as u64 * 2 + 1, Side::Sell, 50000.0, 10.0, TimeInForce::GTC, 0);
            batch.push(sell);
        }

        let _ = engine.match_orders_batch(batch)?;
    }
    let partial_match_tps = (num_rounds as f64 * 20.0) / start.elapsed().as_secs_f64();
    println!("  TPS: {:.0} (10个add_to_book调用)\n", partial_match_tps);

    // 汇总
    println!("【性能对比汇总】");
    println!("  完全匹配（0个add_to_book）: {:.0} TPS", perfect_match_tps);
    println!("  部分匹配（10个add_to_book）: {:.0} TPS ({:+.1}%)", partial_match_tps, ((partial_match_tps - perfect_match_tps) / perfect_match_tps) * 100.0);
    println!("  无匹配（20个add_to_book）:  {:.0} TPS ({:+.1}%)", no_match_tps, ((no_match_tps - perfect_match_tps) / perfect_match_tps) * 100.0);

    let add_to_book_cost = ((perfect_match_tps - no_match_tps) / perfect_match_tps) * 100.0;
    println!("\n  add_to_book()开销: {:.1}% (20个调用相比0个调用)", add_to_book_cost);

    Ok(())
}
