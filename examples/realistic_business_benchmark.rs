//! 真实业务场景基准测试 - 模拟真实订单流
//!
//! 场景: 持续不断的订单流
//! - 先放入5个买单(价格 49998-50000)
//! - 再放入5个卖单(价格 50000-50002) -> 会与买单成交
//! - 其他订单则需要插入簿
//! - 真实比例: ~50%成交 + ~50%插入

use lightning_exchange::{MatchingEngine, PoolConfig, Order, Side, TimeInForce, TradeEvent};
use std::time::Instant;
use rtrb::RingBuffer;
use smallvec::SmallVec;
use hdrhistogram::Histogram;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔═══════════════════════════════════════════════════════════════════╗");
    println!("║              真实业务场景基准测试 - 模拟实际订单流               ║");
    println!("╚═══════════════════════════════════════════════════════════════════╝\n");

    println!("【场景设定】");
    println!("  基准价: 50000.0");
    println!("  设定1: 先放5个买单 (49998-50000) - 这些会停留在簿中");
    println!("  设定2: 再放5个卖单 (50000-50002) - 这些会与买单成交");
    println!("  设定3: 重复5000次 (产生实际的成交和插入)");
    println!("  评估指标: 成交率、插入率、TradeEvents数量、TPS、延迟\n");

    let num_batches = 5000;

    // ===== 测试1: 单委托模式 =====
    println!("【测试1】单委托模式 - 逐个委托处理");
    println!("  测试轮次: {} (每轮10个委托)\n", num_batches);

    let mut engine = MatchingEngine::new(PoolConfig::default())?;
    let (tx, mut rx) = RingBuffer::<TradeEvent>::new(1_000_000);
    engine.set_trade_event_sender(tx);

    let mut latencies = Histogram::<u64>::new(3)?;

    let base_price = 50000.0;
    let mut total_orders = 0;
    let mut total_filled = 0.0;

    let total_start = Instant::now();

    for round in 0..num_batches {
        // 5个买单
        for i in 0..5 {
            let price = base_price - 2.0 + i as f64; // 49998-50002
            let qty = 10.0;

            let start = Instant::now();
            let order = Order::new(round as u64 * 1000 + i as u64, Side::Buy, price, qty, TimeInForce::GTC, 0);
            let result = engine.place_order(order)?;
            latencies.record(start.elapsed().as_nanos() as u64)?;

            total_orders += 1;
            total_filled += result.filled;
        }

        // 5个卖单 - 会与买单成交
        for i in 0..5 {
            let price = base_price + i as f64; // 50000-50004
            let qty = 10.0;

            let start = Instant::now();
            let order = Order::new(round as u64 * 1000 + 500 + i as u64, Side::Sell, price, qty, TimeInForce::GTC, 0);
            let result = engine.place_order(order)?;
            latencies.record(start.elapsed().as_nanos() as u64)?;

            total_orders += 1;
            total_filled += result.filled;
        }
    }

    let total_elapsed = total_start.elapsed();
    let single_tps = (num_batches as f64 * 10.0) / total_elapsed.as_secs_f64();

    let mut single_events = 0;
    while rx.pop().is_ok() {
        single_events += 1;
    }

    let p50_single = latencies.value_at_percentile(50.0) as f64;
    let p99_single = latencies.value_at_percentile(99.0) as f64;

    println!("  ✓ TPS: {:.2}M", single_tps / 1_000_000.0);
    println!("  ✓ P50 延迟: {:.0} ns ({:.2} μs)", p50_single, p50_single / 1000.0);
    println!("  ✓ P99 延迟: {:.0} ns ({:.2} μs)", p99_single, p99_single / 1000.0);
    println!("  ✓ 样本数: {}", latencies.len());
    println!("  ✓ 总委托数: {}", total_orders);
    println!("  ✓ 总成交量: {:.0}", total_filled);
    println!("  ✓ TradeEvents: {}", single_events);
    println!("  ✓ 总耗时: {:.3}s\n", total_elapsed.as_secs_f64());

    // ===== 测试2: 批量模式（20个委托 - 与Deep OB保持一致） =====
    println!("【测试2】批量模式 - 20个委托/批处理 (OKX标准)");
    let batch_size = 20;
    let batch_rounds = (num_batches * 10 + batch_size - 1) / batch_size;
    println!("  批次大小: {} 委托/批", batch_size);
    println!("  批次数: {}\n", batch_rounds);

    let mut engine = MatchingEngine::new(PoolConfig::default())?;
    let (tx, mut rx) = RingBuffer::<TradeEvent>::new(1_000_000);
    engine.set_trade_event_sender(tx);

    let mut batch_latencies = Histogram::<u64>::new(3)?;

    let mut batch_total_orders = 0;
    let mut batch_total_filled = 0.0;

    let total_start = Instant::now();

    for batch_idx in 0..batch_rounds {
        let mut batch: SmallVec<[Order; 20]> = SmallVec::new();

        // 10个买单 (而不是5个)
        for i in 0..10 {
            let order_idx = batch_idx * batch_size + i;
            if order_idx >= num_batches * 10 { break; }
            let price = base_price - 5.0 + (i % 10) as f64; // 扩大价格范围
            let qty = 10.0;
            let buy = Order::new(order_idx as u64 * 2, Side::Buy, price, qty, TimeInForce::GTC, 0);
            batch.push(buy);
        }

        // 10个卖单 (而不是5个)
        for i in 0..10 {
            let order_idx = batch_idx * batch_size + 10 + i;
            if order_idx >= num_batches * 10 { break; }
            let price = base_price + (i % 10) as f64; // 扩大价格范围
            let qty = 10.0;
            let sell = Order::new(order_idx as u64 * 2 + 1, Side::Sell, price, qty, TimeInForce::GTC, 0);
            batch.push(sell);
        }

        let batch_len = batch.len();
        let start = Instant::now();
        let results = engine.match_orders_batch(batch)?;
        let batch_latency = start.elapsed().as_nanos() as u64;

        // 统计
        for (filled, _trades) in results.iter() {
            batch_total_filled += filled;
        }
        batch_total_orders += batch_len;

        // 将批处理延迟分配给所有委托 (20个而不是10个)
        let per_order_latency = batch_latency / batch_len as u64;
        for _ in 0..batch_len {
            batch_latencies.record(per_order_latency)?;
        }
    }

    let total_elapsed = total_start.elapsed();
    let batch_tps = (num_batches as f64 * 10.0) / total_elapsed.as_secs_f64();

    let mut batch_events = 0;
    while rx.pop().is_ok() {
        batch_events += 1;
    }

    let p50_batch = batch_latencies.value_at_percentile(50.0) as f64;
    let p99_batch = batch_latencies.value_at_percentile(99.0) as f64;

    println!("  ✓ TPS: {:.2}M", batch_tps / 1_000_000.0);
    println!("  ✓ P50 延迟: {:.0} ns ({:.2} μs) [单委托等效]", p50_batch, p50_batch / 1000.0);
    println!("  ✓ P99 延迟: {:.0} ns ({:.2} μs) [单委托等效]", p99_batch, p99_batch / 1000.0);
    println!("  ✓ 样本数: {}", batch_latencies.len());
    println!("  ✓ 总委托数: {}", batch_total_orders);
    println!("  ✓ 总成交量: {:.0}", batch_total_filled);
    println!("  ✓ TradeEvents: {}", batch_events);
    println!("  ✓ 总耗时: {:.3}s\n", total_elapsed.as_secs_f64());

    // ===== 性能对比 =====
    println!("╔═══════════════════════════════════════════════════════════════════╗");
    println!("║                  真实业务场景性能对比汇总                        ║");
    println!("╠═══════════════════════════════════════════════════════════════════╣");
    println!("║ 指标                    │ 单委托模式  │ 批量(10个)  │ 改进      ║");
    println!("╠═══════════════════════════════════════════════════════════════════╣");

    let tps_improvement = ((batch_tps - single_tps) / single_tps) * 100.0;
    println!("║ TPS                    │ {:>9.2}M │ {:>9.2}M  │ {:>6.1}%  ║",
        single_tps / 1_000_000.0, batch_tps / 1_000_000.0, tps_improvement);

    let p50_improvement = -((p50_batch - p50_single) / p50_single) * 100.0;
    println!("║ P50 延迟 (ns)           │ {:>10.0} │ {:>10.0} │ {:>6.1}%  ║",
        p50_single, p50_batch, p50_improvement);

    let p99_improvement = -((p99_batch - p99_single) / p99_single) * 100.0;
    println!("║ P99 延迟 (ns)           │ {:>10.0} │ {:>10.0} │ {:>6.1}%  ║",
        p99_single, p99_batch, p99_improvement);

    println!("╠═══════════════════════════════════════════════════════════════════╣");
    println!("║ TradeEvents            │ {:>10} │ {:>10} │           ║", single_events, batch_events);
    println!("║ 单委托成交率           │      {:>5.1}% │      {:>5.1}% │           ║",
        (total_filled / (total_orders as f64 * 10.0)) * 100.0,
        (batch_total_filled / (batch_total_orders as f64 * 10.0)) * 100.0);
    println!("╚═══════════════════════════════════════════════════════════════════╝\n");

    println!("【最终结论 - 真实业务场景】");
    println!("  单委托模式:");
    println!("    • TPS: {:.2}M", single_tps / 1_000_000.0);
    println!("    • P50: {:.0} ns ({:.2} μs)", p50_single, p50_single / 1000.0);
    println!("    • P99: {:.0} ns ({:.2} μs)", p99_single, p99_single / 1000.0);
    println!("    • TradeEvents: {}", single_events);
    println!();
    println!("  批量模式(10个委托):");
    println!("    • TPS: {:.2}M ({:+.1}%)", batch_tps / 1_000_000.0, tps_improvement);
    println!("    • P50: {:.0} ns ({:.2} μs) ({:+.1}% 改进)", p50_batch, p50_batch / 1000.0, p50_improvement);
    println!("    • P99: {:.0} ns ({:.2} μs) ({:+.1}% 改进)", p99_batch, p99_batch / 1000.0, p99_improvement);
    println!("    • TradeEvents: {}", batch_events);

    Ok(())
}
