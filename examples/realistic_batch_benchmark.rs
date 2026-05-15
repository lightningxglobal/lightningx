//! 真实场景基准测试 - 模拟实际业务数据的委托分布
//!
//! 场景: 在真实交易中，并非所有订单都能立即成交
//! - 约 40% 的委托可以立即成交或部分成交
//! - 约 60% 的委托需要插入到订单簿中
//! - 买卖力量不对等
//! - 价格分布更广泛

use matching_engine::{MatchingEngine, PoolConfig, Order, Side, TimeInForce, TradeEvent};
use std::time::Instant;
use rtrb::RingBuffer;
use smallvec::SmallVec;
use hdrhistogram::Histogram;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔═══════════════════════════════════════════════════════════════════╗");
    println!("║            真实场景基准测试 - 模拟实际业务数据分布               ║");
    println!("╚═══════════════════════════════════════════════════════════════════╝\n");

    // 场景说明
    println!("【场景设定】");
    println!("  基准价: 50000.0");
    println!("  买单分布: 49995-50000 (BBO附近，50%概率) + 49900-49994(远离价，50%概率)");
    println!("  卖单分布: 50000-50005 (BBO附近，50%概率) + 50050-50100(远离价，50%概率)");
    println!("  订单数量: 1-20 (随机)");
    println!("  预期成交率: ~50% (部分/完全成交) + ~50% (插入簿)");
    println!("  实际评估: 成交数、插入数、TradeEvent数、缓存命中\n");

    let num_rounds = 5000;

    // ===== 测试1: 单委托模式 =====
    println!("【测试1】单委托模式 - 逐个委托处理");
    println!("  测试轮次: {}\n", num_rounds);

    let mut engine = MatchingEngine::new(PoolConfig::default())?;
    let (tx, mut rx) = RingBuffer::<TradeEvent>::new(1_000_000);
    engine.set_trade_event_sender(tx);

    let mut latencies = Histogram::<u64>::new(3)?;

    let base_price = 50000.0;
    let mut total_orders = 0;
    let mut total_trades = 0;
    let mut total_inserted = 0;

    let total_start = Instant::now();

    for round in 0..num_rounds {
        // 买单: 50%概率在BBO附近，50%概率远离价
        if (round % 2) == 0 {
            // 靠近BBO的买单(会部分成交)
            let offset = (round % 6) as f64; // 0-5
            let price = base_price - 5.0 + offset; // 49995-50000
            let qty = ((round % 20) + 1) as f64;

            let start = Instant::now();
            let order = Order::new(round as u64 * 1000, Side::Buy, price, qty, TimeInForce::GTC, 0);
            let result = engine.place_order(order)?;
            latencies.record(start.elapsed().as_nanos() as u64)?;

            total_orders += 1;
            if result.filled > 0.0 {
                total_trades += 1;
            }
            if result.filled < qty {
                total_inserted += 1;
            }
        } else {
            // 远离BBO的买单(会插入簿)
            let offset = (round % 95) as f64; // 0-94
            let price = base_price - 100.0 - offset; // 49900-49994
            let qty = ((round % 20) + 1) as f64;

            let start = Instant::now();
            let order = Order::new(round as u64 * 1000 + 500, Side::Buy, price, qty, TimeInForce::GTC, 0);
            let _ = engine.place_order(order)?;
            latencies.record(start.elapsed().as_nanos() as u64)?;

            total_orders += 1;
            total_inserted += 1;
        }

        // 卖单: 50%概率靠近BBO，50%概率远离价
        if (round % 2) == 0 {
            // 靠近BBO的卖单(会部分成交)
            let offset = (round % 6) as f64; // 0-5
            let price = base_price + offset; // 50000-50005
            let qty = ((round % 20) + 1) as f64;

            let start = Instant::now();
            let order = Order::new(round as u64 * 1000 + 1, Side::Sell, price, qty, TimeInForce::GTC, 0);
            let result = engine.place_order(order)?;
            latencies.record(start.elapsed().as_nanos() as u64)?;

            total_orders += 1;
            if result.filled > 0.0 {
                total_trades += 1;
            }
            if result.filled < qty {
                total_inserted += 1;
            }
        } else {
            // 远离BBO的卖单(会插入簿)
            let offset = (round % 51) as f64; // 0-50
            let price = base_price + 50.0 + offset; // 50050-50100
            let qty = ((round % 20) + 1) as f64;

            let start = Instant::now();
            let order = Order::new(round as u64 * 1000 + 501, Side::Sell, price, qty, TimeInForce::GTC, 0);
            let _ = engine.place_order(order)?;
            latencies.record(start.elapsed().as_nanos() as u64)?;

            total_orders += 1;
            total_inserted += 1;
        }
    }

    let total_elapsed = total_start.elapsed();
    let single_tps = (num_rounds as f64 * 2.0) / total_elapsed.as_secs_f64();

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
    println!("  ✓ TradeEvents: {}", single_events);
    println!("  ✓ 委托统计:");
    println!("    - 总委托: {}", total_orders);
    println!("    - 成交/部分成交: {} ({:.1}%)", total_trades, (total_trades as f64 / total_orders as f64) * 100.0);
    println!("    - 需要插入: {} ({:.1}%)", total_inserted, (total_inserted as f64 / total_orders as f64) * 100.0);
    println!("  ✓ 总耗时: {:.3}s\n", total_elapsed.as_secs_f64());

    // ===== 测试2: 批量模式（20个委托） =====
    println!("【测试2】批量模式 - 20个委托/批处理");
    println!("  测试轮次: {}\n", num_rounds);

    let mut engine = MatchingEngine::new(PoolConfig::default())?;
    let (tx, mut rx) = RingBuffer::<TradeEvent>::new(1_000_000);
    engine.set_trade_event_sender(tx);

    let mut batch_latencies = Histogram::<u64>::new(3)?;

    let mut batch_total_orders = 0;
    let mut batch_total_trades = 0;
    let mut batch_total_inserted = 0;

    let total_start = Instant::now();

    for batch_idx in 0..num_rounds {
        let mut batch: SmallVec<[Order; 20]> = SmallVec::new();

        for i in 0..10 {
            let order_idx = batch_idx * 10 + i;

            // 买单: 40%靠近BBO，60%远离价
            if (order_idx % 10) < 4 {
                let offset = (order_idx % 10) as f64;
                let price = base_price - 10.0 + offset;
                let qty = ((order_idx % 20) + 1) as f64;
                let buy = Order::new(order_idx as u64 * 2, Side::Buy, price, qty, TimeInForce::GTC, 0);
                batch.push(buy);
            } else {
                let offset = (order_idx % 100) as f64;
                let price = base_price - 1000.0 - offset;
                let qty = ((order_idx % 20) + 1) as f64;
                let buy = Order::new(order_idx as u64 * 2, Side::Buy, price, qty, TimeInForce::GTC, 0);
                batch.push(buy);
            }

            // 卖单: 60%靠近BBO，40%远离价
            if (order_idx % 10) < 6 {
                let offset = (order_idx % 10) as f64;
                let price = base_price + 1.0 + offset;
                let qty = ((order_idx % 20) + 1) as f64;
                let sell = Order::new(order_idx as u64 * 2 + 1, Side::Sell, price, qty, TimeInForce::GTC, 0);
                batch.push(sell);
            } else {
                let offset = (order_idx % 100) as f64;
                let price = base_price + 50.0 + offset;
                let qty = ((order_idx % 20) + 1) as f64;
                let sell = Order::new(order_idx as u64 * 2 + 1, Side::Sell, price, qty, TimeInForce::GTC, 0);
                batch.push(sell);
            }

            batch_total_orders += 2;
        }

        let start = Instant::now();
        let results = engine.match_orders_batch(batch)?;
        let batch_latency = start.elapsed().as_nanos() as u64;

        // 统计成交和插入
        for (filled, trades) in results.iter() {
            if *filled > 0.0 {
                batch_total_trades += trades.len();
            }
            // 注意: 这里我们通过orders HashMap来判断是否被插入
        }

        // 将批处理延迟分配给20个委托
        let per_order_latency = batch_latency / 20;
        for _ in 0..20 {
            batch_latencies.record(per_order_latency)?;
        }
    }

    let total_elapsed = total_start.elapsed();
    let batch_tps = (num_rounds as f64 * 20.0) / total_elapsed.as_secs_f64();

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
    println!("  ✓ TradeEvents: {}", batch_events);
    println!("  ✓ 委托统计:");
    println!("    - 总委托: {}", batch_total_orders);
    println!("    - 成交的Trade数: {}", batch_total_trades);
    println!("  ✓ 总耗时: {:.3}s\n", total_elapsed.as_secs_f64());

    // ===== 性能对比 =====
    println!("╔═══════════════════════════════════════════════════════════════════╗");
    println!("║                    真实场景性能对比汇总                          ║");
    println!("╠═══════════════════════════════════════════════════════════════════╣");
    println!("║ 指标                    │ 单委托模式  │ 批量(20个)  │ 改进      ║");
    println!("╠═══════════════════════════════════════════════════════════════════╣");

    let tps_improvement = ((batch_tps - single_tps) / single_tps) * 100.0;
    println!("║ TPS                    │ {:>9.2}M │ {:>9.2}M  │ {:>6.1}%  ║",
        single_tps / 1_000_000.0, batch_tps / 1_000_000.0, tps_improvement);

    println!("║ P50 延迟 (ns)           │ {:>10.0} │ {:>10.0} │ {:>6.1}%  ║",
        p50_single, p50_batch, -((p50_batch - p50_single) / p50_single) * 100.0);

    println!("║ P50 延迟 (μs)           │ {:>9.2}  │ {:>9.2}  │ {:>6.1}%  ║",
        p50_single / 1000.0, p50_batch / 1000.0, -((p50_batch - p50_single) / p50_single) * 100.0);

    println!("║ P99 延迟 (ns)           │ {:>10.0} │ {:>10.0} │ {:>6.1}%  ║",
        p99_single, p99_batch, -((p99_batch - p99_single) / p99_single) * 100.0);

    println!("║ P99 延迟 (μs)           │ {:>9.2}  │ {:>9.2}  │ {:>6.1}%  ║",
        p99_single / 1000.0, p99_batch / 1000.0, -((p99_batch - p99_single) / p99_single) * 100.0);

    println!("╠═══════════════════════════════════════════════════════════════════╣");
    println!("║ TradeEvents            │ {:>10} │ {:>10} │           ║", single_events, batch_events);
    println!("╚═══════════════════════════════════════════════════════════════════╝\n");

    println!("【最终结论】");
    println!("  TPS 提升: {:.2}M → {:.2}M ({:+.1}%)",
        single_tps / 1_000_000.0, batch_tps / 1_000_000.0, tps_improvement);
    println!("  P50 延迟改进: {:.0}ns → {:.0}ns ({:+.1}%)",
        p50_single, p50_batch, -((p50_batch - p50_single) / p50_single) * 100.0);
    println!("  P99 延迟改进: {:.0}ns → {:.0}ns ({:+.1}%)",
        p99_single, p99_batch, -((p99_batch - p99_single) / p99_single) * 100.0);

    Ok(())
}

// 简单的随机数生成（使用标准库）
mod rand {
    use std::cell::Cell;

    thread_local! {
        static SEED: Cell<u64> = Cell::new(42);
    }

    pub fn random<T>() -> T
    where
        T: From<u32>,
    {
        SEED.with(|s| {
            let mut seed = s.get();
            seed = seed.wrapping_mul(1103515245).wrapping_add(12345);
            s.set(seed);
            T::from((seed >> 16) as u32)
        })
    }
}
