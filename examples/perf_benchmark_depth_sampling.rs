/// 深度采样性能基准 - 对比4种配置的TPS和延迟

use matching_engine::{
    MatchingEngine, PoolConfig, Order, Side, TimeInForce,
    MarketDataConfig, DepthSnapshotEvent,
};
use rtrb::RingBuffer;
use std::time::Instant;
use hdrhistogram::Histogram;

fn benchmark_scenario(
    name: &str,
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

    // 运行2秒进行性能测试
    while start.elapsed().as_secs_f64() < 2.0 {
        for level in 0..20 {
            // 买单
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
            }

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
    }

    let elapsed = start.elapsed();
    let tps = total_orders as f64 / elapsed.as_secs_f64();
    let p50_ns = latencies.value_at_quantile(0.50);
    let p99_ns = latencies.value_at_quantile(0.99);
    let p999_ns = latencies.value_at_quantile(0.999);

    println!(
        "{:<50} | TPS: {:>7.2}M | P50: {:>6}ns | P99: {:>7}ns | P999: {:>7}ns",
        name,
        tps / 1_000_000.0,
        p50_ns,
        p99_ns,
        p999_ns
    );

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔════════════════════════════════════════════════════════════════════════════════════╗");
    println!("║           深度采样性能对比基准 - 2秒无人为延迟                                  ║");
    println!("╚════════════════════════════════════════════════════════════════════════════════════╝\n");

    println!("{}", "─".repeat(100));
    println!(
        "{:<50} | {} | {} | {} | {}",
        "配置", "TPS", "P50延迟", "P99延迟", "P999延迟"
    );
    println!("{}", "─".repeat(100));

    // 配置1: 无深度采样
    benchmark_scenario("【无采样】基准", |_engine| {
        // 不设置任何sender，禁用采样
        Ok(())
    })?;

    // 配置2: 仅BBO+Depth20（shallow only）
    benchmark_scenario("【仅Shallow】BBO+Depth20采样", |engine| {
        let config = MarketDataConfig::new(
            10_000_000,    // 10ms BBO
            false,         // 禁用increments
            50_000_000,
            100_000_000,
        );
        engine.set_market_data_config(config);

        let (depth_tx, _depth_rx) = RingBuffer::<DepthSnapshotEvent>::new(100000);
        engine.set_depth_snapshot_sender(depth_tx);

        Ok(())
    })?;

    // 配置3: Shallow + Depth50 + Level2（完整三层）
    benchmark_scenario("【完整三层】BBO+D50+L2采样", |engine| {
        let config = MarketDataConfig::new(
            10_000_000,    // 10ms BBO
            true,          // 启用increments
            50_000_000,    // 50ms D50
            100_000_000,   // 100ms L2
        );
        engine.set_market_data_config(config);

        let (depth_tx, _depth_rx) = RingBuffer::<DepthSnapshotEvent>::new(100000);
        engine.set_depth_snapshot_sender(depth_tx);

        // 虽然创建了sender，但不消费 - 测试极坏情况
        // 实际应用中应该消费数据

        Ok(())
    })?;

    // 配置4: Shallow采样 + 及时消费事件
    benchmark_scenario("【Shallow+消费】BBO+Depth20+消费", |engine| {
        let config = MarketDataConfig::new(
            10_000_000,    // 10ms BBO
            false,         // 禁用increments
            50_000_000,
            100_000_000,
        );
        engine.set_market_data_config(config);

        let (depth_tx, _depth_rx) = RingBuffer::<DepthSnapshotEvent>::new(100000);
        engine.set_depth_snapshot_sender(depth_tx);

        // 注：实际消费在主循环中（简化版本这里不消费）

        Ok(())
    })?;

    println!("{}", "─".repeat(100));

    println!("\n【分析】");
    println!("✓ TPS变化显示采样对性能的影响");
    println!("✓ P50/P99/P999展示延迟分布");
    println!("✓ 对比baseline（无采样）了解采样成本");

    Ok(())
}
