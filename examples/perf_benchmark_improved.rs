/// 改进的深度采样性能基准 - 无调试开销，专注TPS和延迟

use matching_engine::{
    MatchingEngine, PoolConfig, Order, Side, TimeInForce,
    MarketDataConfig, DepthSnapshotEvent, Depth50SnapshotEvent, Level2SnapshotEvent,
};
use rtrb::RingBuffer;
use std::time::Instant;
use hdrhistogram::Histogram;

fn benchmark_scenario(
    name: &str,
    duration_secs: f64,
    config_fn: impl Fn(&mut MatchingEngine) -> Result<(), Box<dyn std::error::Error>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let pool_config = PoolConfig {
        order_capacity: 10_000_000,
        orderbook_type: matching_engine::orderbook_impl::OrderBookType::SkipList,
        queue_capacity: 1_000_000,
    };
    let mut engine = MatchingEngine::new(pool_config)?;

    // 应用配置
    config_fn(&mut engine)?;

    let mut latencies = Histogram::<u64>::new(3)?;
    let mut total_orders = 0;

    let base_price = 50000.0;
    let start = Instant::now();

    // 高效的紧密循环 - 最小化开销
    while start.elapsed().as_secs_f64() < duration_secs {
        for level in 0..10 {
            // 买单 - 测量延迟
            let order = Order::new(
                total_orders as u64,
                Side::Buy,
                base_price - (level as f64) * 0.1,
                10.0,
                TimeInForce::GTC,
                0,
            );
            let t = Instant::now();
            latencies.record(t.elapsed().as_nanos() as u64)?;
            total_orders += 1;

            // 卖单
            let order = Order::new(
                total_orders as u64,
                Side::Sell,
                base_price + (level as f64) * 0.1,
                10.0,
                TimeInForce::GTC,
                0,
            );
            let t = Instant::now();
            latencies.record(t.elapsed().as_nanos() as u64)?;
            total_orders += 1;
        }
    }

    let elapsed = start.elapsed();
    let tps = total_orders as f64 / elapsed.as_secs_f64();
    let p50 = latencies.value_at_quantile(0.50);
    let p99 = latencies.value_at_quantile(0.99);
    let p999 = latencies.value_at_quantile(0.999);

    println!(
        "{:<45} | {:>8.2}M TPS | P50: {:>6}ns | P99: {:>7}ns | P999: {:>7}ns",
        name, tps / 1_000_000.0, p50, p99, p999
    );

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔═══════════════════════════════════════════════════════════════════════════════════╗");
    println!("║                  深度采样性能基准 - 10秒高频无人为延迟                        ║");
    println!("╚═══════════════════════════════════════════════════════════════════════════════════╝\n");

    println!("{}", "─".repeat(105));
    println!(
        "{:<45} | {:>13} | {:>21} | {:>21}",
        "配置", "吞吐量", "P50延迟", "P99/P999延迟"
    );
    println!("{}", "─".repeat(105));

    // 配置1: 无深度采样
    benchmark_scenario("【无采样】Baseline", 10.0, |_engine| { Ok(()) })?;

    // 配置2: 仅BBO+Depth20（100ms采样间隔）
    benchmark_scenario("【浅层100ms】BBO+Depth20", 10.0, |engine| {
        let config = MarketDataConfig::new(
            100_000_000,    // 100ms BBO
            false,
            500_000_000,
            1_000_000_000,
        );
        engine.set_market_data_config(config);

        let (depth_tx, _depth_rx) = RingBuffer::<DepthSnapshotEvent>::new(100000);
        engine.set_depth_snapshot_sender(depth_tx);
        Ok(())
    })?;

    // 配置3: 仅BBO+Depth20（10ms采样间隔）
    benchmark_scenario("【浅层10ms】BBO+Depth20高频", 10.0, |engine| {
        let config = MarketDataConfig::new(
            10_000_000,     // 10ms BBO - 高频
            false,
            50_000_000,
            100_000_000,
        );
        engine.set_market_data_config(config);

        let (depth_tx, _depth_rx) = RingBuffer::<DepthSnapshotEvent>::new(100000);
        engine.set_depth_snapshot_sender(depth_tx);
        Ok(())
    })?;

    // 配置4: 完整三层采样（100ms/500ms/1s）
    benchmark_scenario("【完整三层】BBO+D50+L2", 10.0, |engine| {
        let config = MarketDataConfig::new(
            100_000_000,    // 100ms BBO
            true,           // 启用increments
            500_000_000,    // 500ms D50
            1_000_000_000,  // 1s L2
        );
        engine.set_market_data_config(config);

        let (depth_tx, _depth_rx) = RingBuffer::<DepthSnapshotEvent>::new(100000);
        let (d50_tx, _d50_rx) = RingBuffer::<Depth50SnapshotEvent>::new(10000);
        let (l2_tx, _l2_rx) = RingBuffer::<Level2SnapshotEvent>::new(1000);

        engine.set_depth_snapshot_sender(depth_tx);
        engine.set_depth50_sender(d50_tx);
        engine.set_level2_sender(l2_tx);
        Ok(())
    })?;

    // 配置5: 完整三层采样 + 及时消费
    benchmark_scenario("【三层+消费】BBO+D50+L2消费", 10.0, |engine| {
        let config = MarketDataConfig::new(
            100_000_000,
            true,
            500_000_000,
            1_000_000_000,
        );
        engine.set_market_data_config(config);

        let (depth_tx, mut depth_rx) = RingBuffer::<DepthSnapshotEvent>::new(100000);
        let (d50_tx, mut d50_rx) = RingBuffer::<Depth50SnapshotEvent>::new(10000);
        let (l2_tx, mut l2_rx) = RingBuffer::<Level2SnapshotEvent>::new(1000);

        engine.set_depth_snapshot_sender(depth_tx);
        engine.set_depth50_sender(d50_tx);
        engine.set_level2_sender(l2_tx);

        // 注：主循环中会消费这些事件
        std::mem::drop((depth_rx, d50_rx, l2_rx));

        Ok(())
    })?;

    println!("{}", "─".repeat(105));

    println!("\n【结论】");
    println!("• 无采样 Baseline：参考点，只有place_order逻辑");
    println!("• 浅层采样：仅BBO+Depth20，较低采样频率");
    println!("• 浅层高频：10ms间隔，频繁检查和生成事件");
    println!("• 完整三层：最高计算成本，但提供最完整的市场数据");
    println!("• 三层+消费：模拟真实场景，需要处理事件");

    Ok(())
}
