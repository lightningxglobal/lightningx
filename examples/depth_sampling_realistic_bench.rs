/// 真实深度采样性能测试 - 启用实际的event sender
/// 对比：禁用vs启用深度采样，使用realistic参数，WITH实际的rtrb sender

use lightning_exchange::{
    MatchingEngine, PoolConfig, Order, Side, TimeInForce,
    MarketDataConfig, DepthSnapshotEvent,
};
use rtrb::RingBuffer;
use std::time::Instant;
use hdrhistogram::Histogram;

fn benchmark_with_sender(
    name: &str,
    enable_increments: bool,
    num_batches: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = MatchingEngine::new(PoolConfig::default())?;

    // 配置采样间隔 - 使用更现实的值
    // 10ms采样太频繁，改为100ms，50ms->500ms，100ms->1000ms
    let config = MarketDataConfig::new(
        100_000_000,   // 100ms (not 10ms!)
        enable_increments,
        500_000_000,   // 500ms (not 50ms!)
        1_000_000_000, // 1s (not 100ms!)
    );
    engine.set_market_data_config(config);

    // 创建实际的 ring buffer 和 sender
    let (depth_tx, mut depth_rx) = RingBuffer::<DepthSnapshotEvent>::new(100);
    engine.set_depth_snapshot_sender(depth_tx);

    let mut latencies = Histogram::<u64>::new(3)?;
    let mut snapshot_count = 0;

    let base_price = 50000.0;
    let mut total_orders = 0;

    let total_start = Instant::now();

    for round in 0..num_batches {
        // 5个买单
        for i in 0..5 {
            let price = base_price - 2.0 + i as f64;
            let qty = 10.0;

            let start = Instant::now();
            let order = Order::new(
                round as u64 * 1000 + i as u64,
                Side::Buy,
                price,
                qty,
                TimeInForce::GTC,
                0,
            );
        engine.place_order(order)?;
            latencies.record(start.elapsed().as_nanos() as u64)?;

            total_orders += 1;
        }

        // 5个卖单
        for i in 0..5 {
            let price = base_price + i as f64;
            let qty = 10.0;

            let start = Instant::now();
            let order = Order::new(
                round as u64 * 1000 + 500 + i as u64,
                Side::Sell,
                price,
                qty,
                TimeInForce::GTC,
                0,
            );
        engine.place_order(order)?;
            latencies.record(start.elapsed().as_nanos() as u64)?;

            total_orders += 1;
        }

        // 定期消费ring buffer中的快照
        while let Ok(_snapshot) = depth_rx.pop() {
            snapshot_count += 1;
        }
    }

    // 消费剩余的快照
    while let Ok(_snapshot) = depth_rx.pop() {
        snapshot_count += 1;
    }

    let total_elapsed = total_start.elapsed();
    let tps = total_orders as f64 / total_elapsed.as_secs_f64();
    let p50_ns = latencies.value_at_quantile(0.5);
    let p99_ns = latencies.value_at_quantile(0.99);

    println!(
        "{:<45} | TPS: {:>7.2}M | P50: {:>5} ns | P99: {:>6} ns | Snapshots: {:>5}",
        name,
        tps / 1_000_000.0,
        p50_ns,
        p99_ns,
        snapshot_count
    );

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 真实深度采样性能对比（含实际sender）===\n");

    // 预热
    println!("预热中...");
    let mut engine = MatchingEngine::new(PoolConfig::default())?;
    for i in 0..100 {
        let order = Order::new(i as u64, Side::Buy, 50000.0, 10.0, TimeInForce::GTC, 0);
    }
    println!("预热完成\n");

    let num_batches = 5000;

    println!("【测试配置】");
    println!("  轮次: {} (每轮10个委托，共{}个)", num_batches, num_batches * 10);
    println!("  采样间隔: 100ms (BBO) / 500ms (Depth50) / 1s (Level2)");
    println!("  WITH实际的rtrb sender\n");

    println!("{}", "─".repeat(95));

    // 测试1: 禁用increments（仅BBO采样）
    benchmark_with_sender(
        "禁用Increments（仅BBO，100ms）",
        false,
        num_batches,
    )?;

    // 测试2: 启用increments（全部三层采样）
    benchmark_with_sender(
        "启用Increments（BBO+D50+L2）",
        true,
        num_batches,
    )?;

    println!("{}", "─".repeat(95));

    println!("\n【说明】");
    println!("- 采样间隔已调整为现实的毫秒级（100ms/500ms/1s）");
    println!("- WITH实际的ring buffer sender，因此包含实际发送开销");
    println!("- Snapshots列显示实际发送的深度快照数量");

    Ok(())
}
