//! 纯撮合基准 - 不生成OrderUpdate，直接测量核心撮合性能

use matching_engine::{MatchingEngine, PoolConfig, Order, Side, TimeInForce, TradeEvent};
use std::time::Instant;
use rtrb::RingBuffer;
use smallvec::SmallVec;
use hdrhistogram::Histogram;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔═══════════════════════════════════════════════════════════════════╗");
    println!("║        纯撮合基准 - 不含OrderUpdate/Depth采样开销                  ║");
    println!("╚═══════════════════════════════════════════════════════════════════╝\n");

    let num_batches = 5000;

    // ===== 单委托模式 =====
    println!("【单委托模式】\n");

    let mut engine = MatchingEngine::new(PoolConfig::default())?;
    let (_tx, mut _rx) = RingBuffer::<TradeEvent>::new(1_000_000);
    // 不设置trade_event_sender，不生成事件

    let mut latencies = Histogram::<u64>::new(3)?;
    let mut total_orders = 0;
    let base_price = 50000.0;

    let total_start = Instant::now();
    let mut affected_makers = SmallVec::<[u64; 64]>::new();

    for round in 0..num_batches {
        // 5个买单
        for i in 0..5 {
            let price = base_price - 2.0 + i as f64;
            let qty = 10.0;

            let start = Instant::now();
            let order = Order::new(round as u64 * 1000 + i as u64, Side::Buy, price, qty, TimeInForce::GTC, 0);
            let _result = engine.place_order(order, &mut affected_makers)?;
            latencies.record(start.elapsed().as_nanos() as u64)?;
            total_orders += 1;
        }

        // 5个卖单
        for i in 0..5 {
            let price = base_price + i as f64;
            let qty = 10.0;

            let start = Instant::now();
            let order = Order::new(round as u64 * 1000 + 500 + i as u64, Side::Sell, price, qty, TimeInForce::GTC, 0);
            let _result = engine.place_order(order, &mut affected_makers)?;
            latencies.record(start.elapsed().as_nanos() as u64)?;
            total_orders += 1;
        }
    }

    let total_elapsed = total_start.elapsed();
    let single_tps = total_orders as f64 / total_elapsed.as_secs_f64();
    let p50 = latencies.value_at_percentile(50.0) as f64;
    let p99 = latencies.value_at_percentile(99.0) as f64;

    println!("  TPS: {:.2}M", single_tps / 1_000_000.0);
    println!("  P50: {:.0} ns", p50);
    println!("  P99: {:.0} ns", p99);
    println!("  样本: {}\n", latencies.len());

    // ===== 批量模式 =====
    println!("【批量模式(20个订单)】\n");

    let mut engine = MatchingEngine::new(PoolConfig::default())?;
    let mut batch_latencies = Histogram::<u64>::new(3)?;

    let total_start = Instant::now();

    for batch_idx in 0..num_batches {
        let mut batch: SmallVec<[Order; 20]> = SmallVec::new();

        for i in 0..10 {
            let order_idx = batch_idx * 20 + i;
            let price = base_price - 2.0 + (i % 5) as f64;
            let qty = 10.0;
            let buy = Order::new(order_idx as u64 * 2, Side::Buy, price, qty, TimeInForce::GTC, 0);
            batch.push(buy);
        }

        for i in 0..10 {
            let order_idx = batch_idx * 20 + i;
            let price = base_price + (i % 5) as f64;
            let qty = 10.0;
            let sell = Order::new(order_idx as u64 * 2 + 1, Side::Sell, price, qty, TimeInForce::GTC, 0);
            batch.push(sell);
        }

        let start = Instant::now();
        let _results = engine.place_orders(batch)?;
        let batch_latency = start.elapsed().as_nanos() as u64;
        let per_order = batch_latency / 20;

        for _ in 0..20 {
            batch_latencies.record(per_order)?;
        }
    }

    let total_elapsed = total_start.elapsed();
    let batch_tps = (num_batches as f64 * 20.0) / total_elapsed.as_secs_f64();
    let batch_p50 = batch_latencies.value_at_percentile(50.0) as f64;
    let batch_p99 = batch_latencies.value_at_percentile(99.0) as f64;

    println!("  TPS: {:.2}M", batch_tps / 1_000_000.0);
    println!("  P50: {:.0} ns [等效单订单]", batch_p50);
    println!("  P99: {:.0} ns [等效单订单]", batch_p99);
    println!("  样本: {}\n", batch_latencies.len());

    // ===== 对比历史性能 =====
    println!("╔═══════════════════════════════════════════════════════════════════╗");
    println!("║                  历史性能 vs 当前性能对比                          ║");
    println!("╠═══════════════════════════════════════════════════════════════════╣");
    println!("║ 指标          │ 历史最高  │ 当前纯撮合 │ 差异         │ 开销来源  ║");
    println!("╠═══════════════════════════════════════════════════════════════════╣");

    let single_hist = 4.4;
    let single_change = ((single_tps / 1_000_000.0 - single_hist) / single_hist) * 100.0;
    println!("║ 单订单TPS(M)  │ {:.2}    │ {:.2}       │ {:+.1}%     │           ║",
        single_hist, single_tps / 1_000_000.0, single_change);

    let batch_hist = 11.0;
    let batch_change = ((batch_tps / 1_000_000.0 - batch_hist) / batch_hist) * 100.0;
    println!("║ 批量TPS(M)    │ {:.2}    │ {:.2}       │ {:+.1}%      │           ║",
        batch_hist, batch_tps / 1_000_000.0, batch_change);

    println!("║ P50延迟(ns)   │ 229     │ {:.0}      │ {:+.1}%     │           ║",
        p50, ((p50 - 229.0) / 229.0) * 100.0);

    println!("╚═══════════════════════════════════════════════════════════════════╝\n");

    println!("【性能分析】");
    println!("如果纯撮合性能接近4.4M/11M，说明性能损失来自：");
    println!("  - OrderUpdate事件生成");
    println!("  - Depth采样");
    println!("  - HashMap order_info追踪");
    println!();
    println!("如果纯撮合性能也很低(<3M)，说明问题在place_order核心逻辑");

    Ok(())
}
