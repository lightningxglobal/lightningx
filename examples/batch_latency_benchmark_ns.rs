//! 批量撮合延迟基准测试（纳秒精度） - 1个vs20个委托的TPS/延迟

use matching_engine::{MatchingEngine, PoolConfig, Order, Side, TimeInForce, TradeEvent};
use std::time::Instant;
use rtrb::RingBuffer;
use smallvec::SmallVec;
use hdrhistogram::Histogram;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║      批量撮合延迟基准测试(纳秒精度) - 1个vs20个委托            ║");
    println!("╚════════════════════════════════════════════════════════════════╝\n");

    // 测试1: 单委托模式
    println!("【测试1】单委托模式 - 逐个委托处理");
    println!("  测试条件: 50,000 轮, 100,000 笔委托, 100档价位\n");

    let mut engine = MatchingEngine::new(PoolConfig::default())?;
    let (tx, _rx) = RingBuffer::<TradeEvent>::new(1_000_000);
    engine.set_trade_event_sender(tx);

    let mut latencies = Histogram::<u64>::new(3)?; // 3位精度，纳秒级

    let num_rounds = 50000;
    let total_start = Instant::now();

    for i in 0..num_rounds {
        let price = 50000.0 + (i % 100) as f64;

        // 买单
        let start = Instant::now();
        let buy = Order::new(i as u64 * 2, Side::Buy, price, 10.0, TimeInForce::GTC, 0);
        let _ = engine.place_order(buy)?;
        let buy_latency = start.elapsed().as_nanos() as u64;
        if buy_latency <= 10_000_000 { // 只记录 <= 10ms 的延迟
            latencies.record(buy_latency)?;
        }

        // 卖单
        let start = Instant::now();
        let sell = Order::new(i as u64 * 2 + 1, Side::Sell, price, 10.0, TimeInForce::GTC, 0);
        let _ = engine.place_order(sell)?;
        let sell_latency = start.elapsed().as_nanos() as u64;
        if sell_latency <= 10_000_000 {
            latencies.record(sell_latency)?;
        }
    }

    let total_elapsed = total_start.elapsed();
    let single_tps = (num_rounds as f64 * 2.0) / total_elapsed.as_secs_f64();

    println!("  ✓ TPS: {:.2}M", single_tps / 1_000_000.0);
    println!("  ✓ 样本数: {}", latencies.len());
    println!("  ✓ P50 延迟: {:.0} ns ({:.2} μs)",
        latencies.value_at_percentile(50.0),
        latencies.value_at_percentile(50.0) as f64 / 1000.0);
    println!("  ✓ P99 延迟: {:.0} ns ({:.2} μs)",
        latencies.value_at_percentile(99.0),
        latencies.value_at_percentile(99.0) as f64 / 1000.0);
    println!("  ✓ Max 延迟: {:.0} ns ({:.2} μs)",
        latencies.max(),
        latencies.max() as f64 / 1000.0);
    println!("  ✓ 总耗时: {:.3}s\n", total_elapsed.as_secs_f64());

    // 测试2: 批量模式（20个委托）
    println!("【测试2】批量模式 - 20个委托/批处理");
    println!("  测试条件: 5,000 轮, 100,000 笔委托, 100档价位\n");

    let mut engine = MatchingEngine::new(PoolConfig::default())?;
    let (tx, _rx) = RingBuffer::<TradeEvent>::new(1_000_000);
    engine.set_trade_event_sender(tx);

    let mut batch_latencies = Histogram::<u64>::new(3)?;

    let num_batch_rounds = 5000;
    let total_start = Instant::now();

    for batch_idx in 0..num_batch_rounds {
        let mut batch: SmallVec<[Order; 20]> = SmallVec::new();

        for i in 0..10 {
            let order_idx = batch_idx * 10 + i;
            let price = 50000.0 + (order_idx % 100) as f64;

            let buy = Order::new(order_idx as u64 * 2, Side::Buy, price, 10.0, TimeInForce::GTC, 0);
            batch.push(buy);

            let sell = Order::new(order_idx as u64 * 2 + 1, Side::Sell, price, 10.0, TimeInForce::GTC, 0);
            batch.push(sell);
        }

        let start = Instant::now();
        let _ = engine.match_orders_batch(batch)?;
        let batch_latency = start.elapsed().as_nanos() as u64;

        // 将批处理延迟分配给20个委托（每个委托的时间）
        let per_order_latency = batch_latency / 20;
        for _ in 0..20 {
            if per_order_latency <= 10_000_000 {
                batch_latencies.record(per_order_latency)?;
            }
        }
    }

    let total_elapsed = total_start.elapsed();
    let batch_tps = (num_batch_rounds as f64 * 20.0) / total_elapsed.as_secs_f64();

    println!("  ✓ TPS: {:.2}M", batch_tps / 1_000_000.0);
    println!("  ✓ 样本数: {}", batch_latencies.len());
    println!("  ✓ P50 延迟: {:.0} ns ({:.2} μs) [单委托等效]",
        batch_latencies.value_at_percentile(50.0),
        batch_latencies.value_at_percentile(50.0) as f64 / 1000.0);
    println!("  ✓ P99 延迟: {:.0} ns ({:.2} μs) [单委托等效]",
        batch_latencies.value_at_percentile(99.0),
        batch_latencies.value_at_percentile(99.0) as f64 / 1000.0);
    println!("  ✓ Max 延迟: {:.0} ns ({:.2} μs)",
        batch_latencies.max(),
        batch_latencies.max() as f64 / 1000.0);
    println!("  ✓ 总耗时: {:.3}s\n", total_elapsed.as_secs_f64());

    // 性能对比
    let p50_single = latencies.value_at_percentile(50.0) as f64;
    let p50_batch = batch_latencies.value_at_percentile(50.0) as f64;
    let p99_single = latencies.value_at_percentile(99.0) as f64;
    let p99_batch = batch_latencies.value_at_percentile(99.0) as f64;

    let tps_improvement = ((batch_tps - single_tps) / single_tps) * 100.0;

    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║                    性能对比汇总                              ║");
    println!("╠════════════════════════════════════════════════════════════════╣");
    println!("║ 指标                    │ 单委托模式  │ 批量(20个)             ║");
    println!("╠════════════════════════════════════════════════════════════════╣");
    println!("║ TPS                    │ {:<10.2}M │ {:<10.2}M             ║",
        single_tps / 1_000_000.0, batch_tps / 1_000_000.0);
    println!("║ P50 延迟 (ns)           │ {:<10.0} │ {:<10.0}             ║",
        p50_single, p50_batch);
    println!("║ P50 延迟 (μs)           │ {:<10.2} │ {:<10.2}             ║",
        p50_single / 1000.0, p50_batch / 1000.0);
    println!("║ P99 延迟 (ns)           │ {:<10.0} │ {:<10.0}             ║",
        p99_single, p99_batch);
    println!("║ P99 延迟 (μs)           │ {:<10.2} │ {:<10.2}             ║",
        p99_single / 1000.0, p99_batch / 1000.0);
    println!("║ Max 延迟 (ns)           │ {:<10.0} │ {:<10.0}             ║",
        latencies.max(), batch_latencies.max());
    println!("╠════════════════════════════════════════════════════════════════╣");
    println!("║ TPS 提升: {:<56.1}% ║", tps_improvement);

    if p50_batch > 0.0 {
        let p50_improvement = ((p50_single - p50_batch) / p50_batch) * 100.0;
        println!("║ P50 改进: {:<56.1}% ║", p50_improvement);
    }
    if p99_batch > 0.0 {
        let p99_improvement = ((p99_single - p99_batch) / p99_batch) * 100.0;
        println!("║ P99 改进: {:<56.1}% ║", p99_improvement);
    }

    println!("╚════════════════════════════════════════════════════════════════╝\n");

    println!("结论:");
    println!("  • TPS:");
    println!("    - 单委托: {:.2}M", single_tps / 1_000_000.0);
    println!("    - 批量:   {:.2}M (提升 {:.1}%)", batch_tps / 1_000_000.0, tps_improvement);
    println!();
    println!("  • P50 延迟:");
    println!("    - 单委托: {:.0} ns ({:.2} μs)", p50_single, p50_single / 1000.0);
    println!("    - 批量:   {:.0} ns ({:.2} μs) [单委托等效]", p50_batch, p50_batch / 1000.0);
    println!();
    println!("  • P99 延迟:");
    println!("    - 单委托: {:.0} ns ({:.2} μs)", p99_single, p99_single / 1000.0);
    println!("    - 批量:   {:.0} ns ({:.2} μs) [单委托等效]", p99_batch, p99_batch / 1000.0);

    Ok(())
}
