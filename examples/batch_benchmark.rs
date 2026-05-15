//! 批量下单性能基准 - 对比单个vs批量下单，使用不同价格

use matching_engine::{MatchingEngine, PoolConfig, Order, Side, TimeInForce};
use smallvec::SmallVec;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 批量下单性能基准测试 ===\n");

    // 基准1: 单个订单处理（基线）
    // 使用100000笔订单，相当于5000轮 * 20笔 = 100000笔（与批处理相同规模）
    println!("【基准1】单个订单处理（50000轮，每轮2笔订单，共100000笔，不同价格）");
    let mut engine = MatchingEngine::new(PoolConfig::default())?;
    let num_rounds = 50000;
    let price_range = 50;

    let start = Instant::now();
    let mut matched_count = 0;

    for i in 0..num_rounds {
        let price = 50000.0 + (i % price_range) as f64;

        // 单笔买单
        let buy = Order::new(
            i as u64 * 2,
            Side::Buy,
            price,
            10.0,
            TimeInForce::GTC,
            0,
        );
        let _ = engine.place_order(buy)?;

        // 单笔卖单 - 相同价格以获得更多匹配（测试成交延迟）
        let sell = Order::new(
            i as u64 * 2 + 1,
            Side::Sell,
            price,  // 相同价格
            10.0,
            TimeInForce::GTC,
            0,
        );
        let result = engine.place_order(sell)?;

        if result.filled > 0.0 {
            matched_count += 1;
        }
    }

    let duration_single = start.elapsed();
    let total_ops = num_rounds as f64 * 2.0;
    let tps_single = total_ops / duration_single.as_secs_f64();
    let latency_single_us = (duration_single.as_nanos() as f64 / total_ops) / 1000.0;

    println!("  成交对数: {}", matched_count);
    println!("  总操作数: {:.0}", total_ops);
    println!("  总耗时: {:.3}ms", duration_single.as_secs_f64() * 1000.0);
    println!("  TPS: {:.0} (K={:.1})", tps_single, tps_single / 1000.0);
    println!("  平均延迟: {:.3}μs ({:.0}ns)\n", latency_single_us, latency_single_us * 1000.0);

    // 基准2: 批量下单（批大小 = 20）
    // 同样100000笔订单，但分批处理
    println!("【基准2】批量下单处理（5000轮，每轮20笔买单 + 20笔卖单，批大小=20，相同价格）");
    engine = MatchingEngine::new(PoolConfig::default())?;
    let num_rounds_batch = 5000;  // 5000轮 * 20 * 2 = 200000笔操作，100000笔订单

    let start = Instant::now();
    let mut matched_batch = 0;

    for round in 0..num_rounds_batch {
        // 创建20笔买单，基于相同价格以获得充分匹配
        let mut buy_batch = SmallVec::<[Order; 20]>::new();
        for j in 0..20 {
            let idx = round * 20 + j;
            let price = 50000.0 + (idx % price_range) as f64;
            let buy = Order::new(
                idx as u64 * 2,
                Side::Buy,
                price,
                10.0,
                TimeInForce::GTC,
                0,
            );
            buy_batch.push(buy);
        }

        // 批量放置买单
        let _buy_results = engine.place_orders(buy_batch)?;

        // 创建20笔卖单，相同价格以促进匹配
        let mut sell_batch = SmallVec::<[Order; 20]>::new();
        for j in 0..20 {
            let idx = round * 20 + j;
            let price = 50000.0 + (idx % price_range) as f64;  // 相同价格
            let sell = Order::new(
                idx as u64 * 2 + 1,
                Side::Sell,
                price,
                10.0,
                TimeInForce::GTC,
                0,
            );
            sell_batch.push(sell);
        }

        // 批量放置卖单
        let sell_results = engine.place_orders(sell_batch)?;

        // 统计成交
        for result in &sell_results {
            if result.filled > 0.0 {
                matched_batch += 1;
            }
        }
    }

    let duration_batch = start.elapsed();
    let total_ops_batch = num_rounds_batch as f64 * 2.0 * 20.0;  // 轮 * 2批 * 20笔
    let tps_batch = total_ops_batch / duration_batch.as_secs_f64();
    let latency_batch_us = (duration_batch.as_nanos() as f64 / total_ops_batch) / 1000.0;

    println!("  成交笔数: {}", matched_batch);
    println!("  总操作数: {:.0}", total_ops_batch);
    println!("  总耗时: {:.3}ms", duration_batch.as_secs_f64() * 1000.0);
    println!("  TPS: {:.0} (K={:.1})", tps_batch, tps_batch / 1000.0);
    println!("  平均延迟: {:.3}μs ({:.0}ns)\n", latency_batch_us, latency_batch_us * 1000.0);

    // 性能对比
    println!("【性能对比】");
    println!("  单个处理 TPS:  {:.0} (K={:.1})", tps_single, tps_single / 1000.0);
    println!("  批量处理 TPS:  {:.0} (K={:.1})", tps_batch, tps_batch / 1000.0);
    let improvement = ((tps_batch - tps_single) / tps_single) * 100.0;
    println!("  TPS改善: {:+.2}%", improvement);
    println!();
    println!("  单个处理延迟:  {:.3}μs ({:.0}ns)", latency_single_us, latency_single_us * 1000.0);
    println!("  批量处理延迟:  {:.3}μs ({:.0}ns)", latency_batch_us, latency_batch_us * 1000.0);
    println!();
    println!("【目标对标】");
    println!("  目标 TPS:    7,000,000 (7M)");
    println!("  目标延迟:   0.3μs (300ns)");
    println!("  当前差距:   {:.2}x 倍", 7_000_000.0 / tps_batch.max(tps_single));

    Ok(())
}
