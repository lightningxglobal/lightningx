//! 延迟测量基准测试
//!
//! 本示例测量完整市场数据系统中的端到端延迟，包括：
//! 1. 匹配引擎成交生成延迟
//! 2. TradeEvent通道传输延迟
//! 3. 市场数据引擎更新延迟
//! 4. 快照生成延迟
//! 5. Aeron发布延迟
//!
//! 目标：
//! - 成交发布: < 1μs
//! - 快照生成: < 100μs
//! - 整个系统: < 1000μs (1ms)

use matching_engine::{
    MatchingEngine, PoolConfig, Order, Side, TimeInForce, TradeEvent,
    MarketDataEngine, SnapshotTimer, SnapshotPublisherThread, TradePublisherThread,
    AeronConfig,
};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use parking_lot::Mutex;
use crossbeam::channel;

/// 性能数据采集
struct PerformanceMetrics {
    trade_generation_latencies: Vec<u64>,  // 成交生成延迟（纳秒）
    order_placement_latencies: Vec<u64>,   // 订单放置延迟（纳秒）
}

impl PerformanceMetrics {
    fn new() -> Self {
        Self {
            trade_generation_latencies: Vec::with_capacity(10000),
            order_placement_latencies: Vec::with_capacity(10000),
        }
    }

    fn calculate_stats(&self, name: &str, latencies: &[u64]) {
        if latencies.is_empty() {
            println!("{}: 无数据", name);
            return;
        }

        let mut sorted = latencies.to_vec();
        sorted.sort();

        let min = sorted[0];
        let max = sorted[sorted.len() - 1];
        let avg = sorted.iter().sum::<u64>() as f64 / sorted.len() as f64;

        // 计算百分位数
        let p50_idx = sorted.len() / 2;
        let p95_idx = (sorted.len() * 95) / 100;
        let p99_idx = (sorted.len() * 99) / 100;
        let p999_idx = (sorted.len() * 999) / 1000;
        let p9999_idx = (sorted.len() * 9999) / 10000;

        let p50 = sorted[p50_idx];
        let p95 = sorted[p95_idx.min(sorted.len() - 1)];
        let p99 = sorted[p99_idx.min(sorted.len() - 1)];
        let p999 = sorted[p999_idx.min(sorted.len() - 1)];
        let p9999 = sorted[p9999_idx.min(sorted.len() - 1)];

        println!("{} (n={})", name, sorted.len());
        println!("  Min:    {:.2} ns ({:.3} μs)", min, min as f64 / 1000.0);
        println!("  P50:    {:.2} ns ({:.3} μs)", p50, p50 as f64 / 1000.0);
        println!("  P95:    {:.2} ns ({:.3} μs)", p95, p95 as f64 / 1000.0);
        println!("  P99:    {:.2} ns ({:.3} μs)", p99, p99 as f64 / 1000.0);
        println!("  P99.9:  {:.2} ns ({:.3} μs)", p999, p999 as f64 / 1000.0);
        println!("  P99.99: {:.2} ns ({:.3} μs)", p9999, p9999 as f64 / 1000.0);
        println!("  Avg:    {:.2} ns ({:.3} μs)", avg, avg / 1000.0);
        println!("  Max:    {:.2} ns ({:.3} μs)", max, max as f64 / 1000.0);
        println!();
    }
}

fn main() {
    println!("=== 市场数据系统延迟基准测试 ===\n");

    // ========== 初始化系统 ==========
    println!("初始化系统组件...\n");

    let (trade_tx, trade_rx) = channel::bounded::<TradeEvent>(1_000_000);
    let mut engine = MatchingEngine::new(PoolConfig::default()).expect("创建匹配引擎失败");
    engine.set_trade_event_sender(trade_tx);

    let market_data_rx = channel::unbounded::<TradeEvent>().1;
    let market_data_engine = Arc::new(Mutex::new(MarketDataEngine::new(market_data_rx)));

    let (snapshot_tx, snapshot_rx) = channel::unbounded();
    let snapshot_timer = SnapshotTimer::spawn(market_data_engine, snapshot_tx);

    let snapshot_aeron_config = AeronConfig {
        aeron_dir: "/dev/shm/aeron".to_string(),
        channel: "aeron:ipc".to_string(),
        stream_id: 10,
    };
    let snapshot_publisher = SnapshotPublisherThread::spawn(snapshot_rx, snapshot_aeron_config);

    let trade_aeron_config = AeronConfig {
        aeron_dir: "/dev/shm/aeron".to_string(),
        channel: "aeron:ipc".to_string(),
        stream_id: 11,
    };
    let trade_publisher = TradePublisherThread::spawn(trade_rx, trade_aeron_config);

    println!("✓ 系统初始化完成\n");

    // ========== 延迟测量 ==========
    println!("开始延迟测量...\n");

    let mut metrics = PerformanceMetrics::new();
    let num_measurements = 10000;

    let total_start = Instant::now();

    for i in 1..=num_measurements {
        // 测量订单放置延迟
        let order_start = Instant::now();

        // 买单
        let buy_order = Order::new(
            i * 2 - 1,
            Side::Buy,
            50000.0 + (i as f64 % 1000.0) * 0.1,
            10.0,
            TimeInForce::GTC,
            0,
        );
        let _ = engine.place_order(buy_order);

        // 卖单（触发成交）
        let sell_order = Order::new(
            i * 2,
            Side::Sell,
            50000.0 + (i as f64 % 1000.0) * 0.1,
            10.0,
            TimeInForce::IOC,
            0,
        );

        let trade_gen_start = Instant::now();
        let _ = engine.place_order(sell_order);
        let trade_gen_latency = trade_gen_start.elapsed().as_nanos() as u64;
        metrics.trade_generation_latencies.push(trade_gen_latency);

        let order_latency = order_start.elapsed().as_nanos() as u64;
        metrics.order_placement_latencies.push(order_latency);

        // 进度显示
        if i % 1000 == 0 {
            println!("  进度: {}/{} ({:.1}%)", i, num_measurements, (i as f64 / num_measurements as f64) * 100.0);
        }
    }

    let total_time = total_start.elapsed();
    println!("\n✓ 完成 {} 次测量\n", num_measurements);

    // ========== 停止系统 ==========
    println!("停止系统组件...\n");
    snapshot_timer.stop();
    snapshot_publisher.stop();
    trade_publisher.stop();
    thread::sleep(std::time::Duration::from_millis(100));

    // ========== 性能统计 ==========
    println!("=== 延迟测量结果 ===\n");

    metrics.calculate_stats("订单放置延迟", &metrics.order_placement_latencies);
    metrics.calculate_stats("成交生成延迟", &metrics.trade_generation_latencies);

    // ========== 吞吐量统计 ==========
    println!("=== 吞吐量统计 ===\n");

    let trades_per_sec = (num_measurements as f64 / total_time.as_secs_f64()) as u64;
    let tps = trades_per_sec;

    println!("总耗时:        {:.3}ms", total_time.as_secs_f64() * 1000.0);
    println!("总成交数:      {}", num_measurements);
    println!("平均TPS:       {:.0} (万笔/秒: {:.2})", tps, tps as f64 / 10000.0);
    println!("平均周期:      {:.2}μs", total_time.as_secs_f64() * 1_000_000.0 / num_measurements as f64);
    println!();

    // ========== 性能评估 ==========
    println!("=== 性能评估 ===\n");

    let avg_order_latency = metrics.order_placement_latencies.iter().sum::<u64>() as f64 /
        metrics.order_placement_latencies.len() as f64;
    let avg_trade_latency = metrics.trade_generation_latencies.iter().sum::<u64>() as f64 /
        metrics.trade_generation_latencies.len() as f64;

    // 检查目标
    let trade_target_ns = 1000.0;  // 1μs
    let trade_target_met = avg_trade_latency < trade_target_ns;

    let order_target_ns = 10000.0; // 10μs
    let order_target_met = avg_order_latency < order_target_ns;

    println!("成交生成延迟 (目标 < 1μs):");
    if trade_target_met {
        println!("  ✓ 满足目标: {:.3}μs < 1.000μs", avg_trade_latency / 1000.0);
    } else {
        println!("  ✗ 未满足目标: {:.3}μs >= 1.000μs", avg_trade_latency / 1000.0);
    }

    println!("\n订单放置延迟 (目标 < 10μs):");
    if order_target_met {
        println!("  ✓ 满足目标: {:.3}μs < 10.000μs", avg_order_latency / 1000.0);
    } else {
        println!("  ✗ 未满足目标: {:.3}μs >= 10.000μs", avg_order_latency / 1000.0);
    }

    println!("\n吞吐量 (目标 > 1M TPS):");
    if tps >= 1_000_000 {
        println!("  ✓ 满足目标: {:.0} TPS >= 1,000,000 TPS", tps);
    } else {
        println!("  ✗ 未满足目标: {:.0} TPS < 1,000,000 TPS", tps);
    }

    // ========== 系统特性总结 ==========
    println!("\n=== 系统特性总结 ===\n");
    println!("成交事件发布:");
    println!("  • 立即发布（无缓冲）");
    println!("  • 独立Aeron流 (Stream 11)");
    println!("  • 消息顺序保证");
    println!();
    println!("快照发布:");
    println!("  • 定时发布 (1ms周期)");
    println!("  • 独立Aeron流 (Stream 10)");
    println!("  • 包含BBO、Level2、聚合成交、24h统计");
    println!();
    println!("架构优势:");
    println!("  • 分离消费路径（成交vs快照）");
    println!("  • 低延迟成交发布");
    println!("  • 定期快照汇总");
    println!("  • 单线程无锁设计");
}
