//! 深委托薄基准测试 - 模仿OKX 400档位的深度
//!
//! 场景:
//! - 初始化: 400档位的买单簿 + 400档位的卖单簿 (共800档)
//! - 100个委托为一批
//! - 单个委托模式 vs 批量模式(20个委托/批)
//! - 评估: TPS、延迟、在深度委托薄中的性能

use matching_engine::{MatchingEngine, PoolConfig, Order, Side, TimeInForce, TradeEvent, orderbook_impl};
use std::time::Instant;
use rtrb::RingBuffer;
use smallvec::SmallVec;
use hdrhistogram::Histogram;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔═══════════════════════════════════════════════════════════════════╗");
    println!("║         深委托薄基准测试 - 400档位(模仿OKX Depth)                 ║");
    println!("╚═══════════════════════════════════════════════════════════════════╝\n");

    println!("【场景设定】");
    println!("  初始委托薄: 400档买单 + 400档卖单 (共800档)");
    println!("  买单价格范围: 49600 - 49999 (400档)");
    println!("  卖单价格范围: 50000 - 50399 (400档)");
    println!("  测试订单: 在BBO附近交易，触发深度");
    println!("  批次大小: 20个委托/批\n");

    let base_price = 50000.0;
    let depth = 400;

    // 初始化深委托簿
    println!("【初始化400档位委托簿】");
    let mut engine = MatchingEngine::new(PoolConfig {
        order_capacity: 100_000,
        queue_capacity: 1_000_000,
        orderbook_type: orderbook_impl::OrderBookType::SkipList,
    })?;
    let (tx, _rx) = RingBuffer::<TradeEvent>::new(10_000_000);
    engine.set_trade_event_sender(tx);

    let init_start = Instant::now();

    // 添加400档买单
    for i in 0..depth {
        let price = base_price - (depth as f64 - i as f64) + 1.0;
        let order = Order::new(i as u64, Side::Buy, price, 100.0, TimeInForce::GTC, 0);
        let _ = let mut affected_makers = SmallVec::new(); engine.place_order(order, &mut affected_makers, &mut smallvec::SmallVec::new())?;
    }

    // 添加400档卖单
    for i in 0..depth {
        let price = base_price + i as f64;
        let order = Order::new((depth + i) as u64, Side::Sell, price, 100.0, TimeInForce::GTC, 0);
        let _ = let mut affected_makers = SmallVec::new(); engine.place_order(order, &mut affected_makers, &mut smallvec::SmallVec::new())?;
    }

    let init_elapsed = init_start.elapsed();
    println!("  ✓ 初始化完成: {:.3}s (800个档位)【", init_elapsed.as_secs_f64());
    println!("  ✓ 委托簿深度: {} 档\n", depth * 2);

    // ===== 测试1: 单委托模式 =====
    println!("【测试1】单委托模式 - 在400档深度上逐个委托");

    let mut engine = MatchingEngine::new(PoolConfig {
        order_capacity: 100_000,
        queue_capacity: 1_000_000,
        orderbook_type: orderbook_impl::OrderBookType::SkipList,
    })?;
    let (tx, mut rx) = RingBuffer::<TradeEvent>::new(10_000_000);
    engine.set_trade_event_sender(tx);

    // 初始化深度
    for i in 0..depth {
        let price = base_price - (depth as f64 - i as f64) + 1.0;
        let order = Order::new(i as u64, Side::Buy, price, 100.0, TimeInForce::GTC, 0);
        let _ = let mut affected_makers = SmallVec::new(); engine.place_order(order, &mut affected_makers, &mut smallvec::SmallVec::new())?;
    }
    for i in 0..depth {
        let price = base_price + i as f64;
        let order = Order::new((depth + i) as u64, Side::Sell, price, 100.0, TimeInForce::GTC, 0);
        let _ = let mut affected_makers = SmallVec::new(); engine.place_order(order, &mut affected_makers, &mut smallvec::SmallVec::new())?;
    }

    let mut latencies = Histogram::<u64>::new(3)?;
    let num_test_orders = 5000;

    let total_start = Instant::now();

    for order_id in 0..num_test_orders {
        // 交替买卖单，在深度中进行交易
        if (order_id % 2) == 0 {
            // 买单 - 在委托簿BBO附近
            let price = base_price - 0.5; // 在卖单簿中间某处
            let qty = 10.0;

            let start = Instant::now();
            let order = Order::new((800 + order_id) as u64, Side::Buy, price, qty, TimeInForce::GTC, 0);
            let _ = let mut affected_makers = SmallVec::new(); engine.place_order(order, &mut affected_makers, &mut smallvec::SmallVec::new())?;
            latencies.record(start.elapsed().as_nanos() as u64)?;
        } else {
            // 卖单 - 在委托簿BBO附近
            let price = base_price + 0.5; // 在买单簿中间某处
            let qty = 10.0;

            let start = Instant::now();
            let order = Order::new((800 + order_id) as u64, Side::Sell, price, qty, TimeInForce::GTC, 0);
            let _ = let mut affected_makers = SmallVec::new(); engine.place_order(order, &mut affected_makers, &mut smallvec::SmallVec::new())?;
            latencies.record(start.elapsed().as_nanos() as u64)?;
        }
    }

    let total_elapsed = total_start.elapsed();
    let single_tps = (num_test_orders as f64) / total_elapsed.as_secs_f64();

    let mut single_events = 0;
    while rx.pop().is_ok() {
        single_events += 1;
    }

    let p50_single = latencies.value_at_percentile(50.0) as f64;
    let p99_single = latencies.value_at_percentile(99.0) as f64;

    println!("  ✓ 样本订单: {}", num_test_orders);
    println!("  ✓ TPS: {:.2}M", single_tps / 1_000_000.0);
    println!("  ✓ P50 延迟: {:.0} ns ({:.2} μs)", p50_single, p50_single / 1000.0);
    println!("  ✓ P99 延迟: {:.0} ns ({:.2} μs)", p99_single, p99_single / 1000.0);
    println!("  ✓ TradeEvents: {}", single_events);
    println!("  ✓ 样本数: {}\n", latencies.len());

    // ===== 测试2: 批量模式（20个委托） =====
    println!("【测试2】批量模式 - 20个委托/批，在400档深度上");

    let mut engine = MatchingEngine::new(PoolConfig {
        order_capacity: 100_000,
        queue_capacity: 1_000_000,
        orderbook_type: orderbook_impl::OrderBookType::SkipList,
    })?;
    let (tx, mut rx) = RingBuffer::<TradeEvent>::new(10_000_000);
    engine.set_trade_event_sender(tx);

    // 初始化深度
    for i in 0..depth {
        let price = base_price - (depth as f64 - i as f64) + 1.0;
        let order = Order::new(i as u64, Side::Buy, price, 100.0, TimeInForce::GTC, 0);
        let _ = let mut affected_makers = SmallVec::new(); engine.place_order(order, &mut affected_makers, &mut smallvec::SmallVec::new())?;
    }
    for i in 0..depth {
        let price = base_price + i as f64;
        let order = Order::new((depth + i) as u64, Side::Sell, price, 100.0, TimeInForce::GTC, 0);
        let _ = let mut affected_makers = SmallVec::new(); engine.place_order(order, &mut affected_makers, &mut smallvec::SmallVec::new())?;
    }

    let mut batch_latencies = Histogram::<u64>::new(3)?;
    let num_batches = num_test_orders / 20;

    let total_start = Instant::now();

    for batch_idx in 0..num_batches {
        let mut batch: SmallVec<[Order; 20]> = SmallVec::new();

        for i in 0..10 {
            let order_id = batch_idx * 20 + i;

            // 买单
            let buy = Order::new((800 + order_id * 2) as u64, Side::Buy, base_price - 0.5, 10.0, TimeInForce::GTC, 0);
            batch.push(buy);

            // 卖单
            let sell = Order::new((800 + order_id * 2 + 1) as u64, Side::Sell, base_price + 0.5, 10.0, TimeInForce::GTC, 0);
            batch.push(sell);
        }

        let start = Instant::now();
        let _ = engine.match_orders_batch(batch)?;
        let batch_latency = start.elapsed().as_nanos() as u64;

        let per_order_latency = batch_latency / 20;
        for _ in 0..20 {
            batch_latencies.record(per_order_latency)?;
        }
    }

    let total_elapsed = total_start.elapsed();
    let batch_tps = (num_test_orders as f64) / total_elapsed.as_secs_f64();

    let mut batch_events = 0;
    while rx.pop().is_ok() {
        batch_events += 1;
    }

    let p50_batch = batch_latencies.value_at_percentile(50.0) as f64;
    let p99_batch = batch_latencies.value_at_percentile(99.0) as f64;

    println!("  ✓ 样本订单: {}", num_test_orders);
    println!("  ✓ 批次数: {}", num_batches);
    println!("  ✓ TPS: {:.2}M", batch_tps / 1_000_000.0);
    println!("  ✓ P50 延迟: {:.0} ns ({:.2} μs) [单委托等效]", p50_batch, p50_batch / 1000.0);
    println!("  ✓ P99 延迟: {:.0} ns ({:.2} μs) [单委托等效]", p99_batch, p99_batch / 1000.0);
    println!("  ✓ TradeEvents: {}", batch_events);
    println!("  ✓ 样本数: {}\n", batch_latencies.len());

    // ===== 性能对比 =====
    let tps_improvement = ((batch_tps - single_tps) / single_tps) * 100.0;
    let p50_improvement = ((p50_single - p50_batch) / p50_batch) * 100.0;
    let p99_improvement = ((p99_single - p99_batch) / p99_batch) * 100.0;

    println!("╔═══════════════════════════════════════════════════════════════════╗");
    println!("║            400档位深委托薄性能对比汇总                            ║");
    println!("╠═══════════════════════════════════════════════════════════════════╣");
    println!("║ 指标                    │ 单委托    │ 批量(20个) │ 改进         ║");
    println!("╠═══════════════════════════════════════════════════════════════════╣");
    println!("║ TPS                    │ {:>7.2}M │ {:>8.2}M  │ {:>6.1}%     ║",
        single_tps / 1_000_000.0, batch_tps / 1_000_000.0, tps_improvement);
    println!("║ P50 延迟 (ns)          │ {:>8.0} │ {:>9.0} │ {:>6.1}%     ║",
        p50_single, p50_batch, p50_improvement);
    println!("║ P50 延迟 (μs)          │ {:>8.2} │ {:>9.2} │ {:>6.1}%     ║",
        p50_single / 1000.0, p50_batch / 1000.0, p50_improvement);
    println!("║ P99 延迟 (ns)          │ {:>8.0} │ {:>9.0} │ {:>6.1}%     ║",
        p99_single, p99_batch, p99_improvement);
    println!("║ P99 延迟 (μs)          │ {:>8.2} │ {:>9.2} │ {:>6.1}%     ║",
        p99_single / 1000.0, p99_batch / 1000.0, p99_improvement);
    println!("╠═══════════════════════════════════════════════════════════════════╣");
    println!("║ TradeEvents            │ {:>8} │ {:>9} │              ║", single_events, batch_events);
    println!("╚═══════════════════════════════════════════════════════════════════╝\n");

    println!("【在OKX级别的深度(400档)上的性能结论】");
    println!("  ✓ 单委托模式: {:.2}M TPS, P50={:.0}ns, P99={:.0}ns",
        single_tps / 1_000_000.0, p50_single, p99_single);
    println!("  ✓ 批量模式: {:.2}M TPS, P50={:.0}ns, P99={:.0}ns [{:+.1}% TPS提升]",
        batch_tps / 1_000_000.0, p50_batch, p99_batch, tps_improvement);

    Ok(())
}
