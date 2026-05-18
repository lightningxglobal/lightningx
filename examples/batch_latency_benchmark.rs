//! 批量撮合延迟基准测试 - 对比1个和20个委托的TPS/延迟

use lightning_exchange::{MatchingEngine, PoolConfig, Order, Side, TimeInForce, TradeEvent};
use std::time::Instant;
use rtrb::RingBuffer;
use smallvec::SmallVec;
use hdrhistogram::Histogram;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║         批量撮合延迟基准测试 - 1个vs20个委托                   ║");
    println!("╚════════════════════════════════════════════════════════════════╝\n");

    // 测试1: 单委托模式
    println!("【测试1】单委托模式 - 逐个委托处理");
    println!("  测试条件: 10,000 轮, 20,000 笔委托, 100档价位\n");

    let mut engine = MatchingEngine::new(PoolConfig::default())?;
    let (tx, _rx) = RingBuffer::<TradeEvent>::new(1_000_000);
    engine.set_trade_event_sender(tx);

    let mut latencies = Histogram::<u64>::new(3)?; // 3位精度，微秒级

    let num_rounds = 10000;
    let total_start = Instant::now();

    for i in 0..num_rounds {
        let price = 50000.0 + (i % 100) as f64;

        // 买单
        let start = Instant::now();
        let buy = Order::new(i as u64 * 2, Side::Buy, price, 10.0, TimeInForce::GTC, 0);
        engine.place_order(buy)?;
        let buy_latency = start.elapsed().as_micros() as u64;
        latencies.record(buy_latency)?;

        // 卖单
        let start = Instant::now();
        let sell = Order::new(i as u64 * 2 + 1, Side::Sell, price, 10.0, TimeInForce::GTC, 0);
        engine.place_order(sell)?;
        let sell_latency = start.elapsed().as_micros() as u64;
        latencies.record(sell_latency)?;
    }

    let total_elapsed = total_start.elapsed();
    let single_tps = (num_rounds as f64 * 2.0) / total_elapsed.as_secs_f64();

    println!("  ✓ TPS: {:.2}M", single_tps / 1_000_000.0);
    println!("  ✓ 样本数: {}", latencies.len());
    println!("  ✓ P50 延迟: {:.2} μs", latencies.value_at_percentile(50.0));
    println!("  ✓ P99 延迟: {:.2} μs", latencies.value_at_percentile(99.0));
    println!("  ✓ Max 延迟: {:.2} μs", latencies.max());
    println!("  ✓ 总耗时: {:.3}s\n", total_elapsed.as_secs_f64());

    // 测试2: 批量模式（20个委托）
    println!("【测试2】批量模式 - 20个委托/批处理");
    println!("  测试条件: 10,000 轮, 200,000 笔委托, 100档价位\n");

    let mut engine = MatchingEngine::new(PoolConfig::default())?;
    let (tx, _rx) = RingBuffer::<TradeEvent>::new(1_000_000);
    engine.set_trade_event_sender(tx);

    let mut batch_latencies = Histogram::<u64>::new(3)?;

    let total_start = Instant::now();

    for batch_idx in 0..num_rounds {
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
        let batch_latency = start.elapsed().as_micros() as u64;

        // 将批处理延迟分配给20个委托（每个委托的时间）
        let per_order_latency = batch_latency / 20;
        for _ in 0..20 {
            batch_latencies.record(per_order_latency)?;
        }
    }

    let total_elapsed = total_start.elapsed();
    let batch_tps = (num_rounds as f64 * 20.0) / total_elapsed.as_secs_f64();

    println!("  ✓ TPS: {:.2}M", batch_tps / 1_000_000.0);
    println!("  ✓ 样本数: {}", batch_latencies.len());
    println!("  ✓ P50 延迟: {:.2} μs (单委托等效)", batch_latencies.value_at_percentile(50.0));
    println!("  ✓ P99 延迟: {:.2} μs (单委托等效)", batch_latencies.value_at_percentile(99.0));
    println!("  ✓ Max 延迟: {:.2} μs", batch_latencies.max());
    println!("  ✓ 总耗时: {:.3}s\n", total_elapsed.as_secs_f64());

    // 性能对比
    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║                    性能对比汇总                              ║");
    println!("╠════════════════════════════════════════════════════════════════╣");
    println!("║ 模式           │  TPS    │ P50延迟  │ P99延迟  │ Max延迟   ║");
    println!("╠════════════════════════════════════════════════════════════════╣");
    println!("║ 单委托         │ {:>6.2}M │ {:>6.2}μs │ {:>6.2}μs │ {:>6.2}μs ║",
        single_tps / 1_000_000.0,
        latencies.value_at_percentile(50.0),
        latencies.value_at_percentile(99.0),
        latencies.max());
    println!("║ 批量(20个)     │ {:>6.2}M │ {:>6.2}μs │ {:>6.2}μs │ {:>6.2}μs ║",
        batch_tps / 1_000_000.0,
        batch_latencies.value_at_percentile(50.0),
        batch_latencies.value_at_percentile(99.0),
        batch_latencies.max());
    println!("╠════════════════════════════════════════════════════════════════╣");

    let tps_improvement = ((batch_tps - single_tps) / single_tps) * 100.0;
    let p50_improvement = ((latencies.value_at_percentile(50.0) as f64 - batch_latencies.value_at_percentile(50.0) as f64) / batch_latencies.value_at_percentile(50.0) as f64) * 100.0;
    let p99_improvement = ((latencies.value_at_percentile(99.0) as f64 - batch_latencies.value_at_percentile(99.0) as f64) / batch_latencies.value_at_percentile(99.0) as f64) * 100.0;

    println!("║ TPS提升:       {:+.1}%                                        ║", tps_improvement);
    println!("║ P50提升:       {:+.1}%                                        ║", p50_improvement);
    println!("║ P99提升:       {:+.1}%                                        ║", p99_improvement);
    println!("╚════════════════════════════════════════════════════════════════╝\n");

    println!("结论:");
    println!("  • 批量处理 TPS: {:.2}M (单委托: {:.2}M) [{:+.1}%]",
        batch_tps / 1_000_000.0, single_tps / 1_000_000.0, tps_improvement);
    println!("  • 单委托等效延迟:");
    println!("    - P50: {:.2}μs (单委托: {:.2}μs) [{:+.1}%]",
        batch_latencies.value_at_percentile(50.0),
        latencies.value_at_percentile(50.0),
        -p50_improvement);
    println!("    - P99: {:.2}μs (单委托: {:.2}μs) [{:+.1}%]",
        batch_latencies.value_at_percentile(99.0),
        latencies.value_at_percentile(99.0),
        -p99_improvement);

    Ok(())
}
