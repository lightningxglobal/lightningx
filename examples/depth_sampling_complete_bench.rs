/// 完全的深度采样性能测试 - 所有三层sender都设置
/// 这次真的测试启用increments的真实成本

use matching_engine::{
    MatchingEngine, PoolConfig, Order, Side, TimeInForce,
    MarketDataConfig, DepthSnapshotEvent, Depth50SnapshotEvent, Level2SnapshotEvent,
};
use rtrb::RingBuffer;
use std::time::Instant;
use hdrhistogram::Histogram;
use std::thread;
use std::time::Duration;

fn benchmark_complete(
    name: &str,
    enable_increments: bool,
    order_delay_us: u64,
    duration_sec: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = MatchingEngine::new(PoolConfig::default())?;

    let config = MarketDataConfig::new(
        10_000_000,    // 10ms
        enable_increments,
        50_000_000,    // 50ms
        100_000_000,   // 100ms
    );
    engine.set_market_data_config(config);

    // ✅ 设置所有三个sender（这次是完整的）
    let (depth_tx, mut depth_rx) = RingBuffer::<DepthSnapshotEvent>::new(10000);
    engine.set_depth_snapshot_sender(depth_tx);

    let (depth50_tx, mut depth50_rx) = RingBuffer::<Depth50SnapshotEvent>::new(1000);
    engine.set_depth50_sender(depth50_tx);

    let (level2_tx, mut level2_rx) = RingBuffer::<Level2SnapshotEvent>::new(100);
    engine.set_level2_sender(level2_tx);

    let mut latencies = Histogram::<u64>::new(3)?;
    let mut snapshot_count = 0;
    let mut depth50_count = 0;
    let mut level2_count = 0;
    let mut total_orders = 0;

    let base_price = 50000.0;
    let total_start = Instant::now();

    while total_start.elapsed().as_secs() < duration_sec {
        for side_idx in 0..2 {
            for level in 0..5 {
                let price = if side_idx == 0 {
                    base_price - 2.0 + level as f64
                } else {
                    base_price + level as f64
                };

                let start = Instant::now();
                let order = Order::new(
                    total_orders as u64,
                    if side_idx == 0 { Side::Buy } else { Side::Sell },
                    price,
                    10.0,
                    TimeInForce::GTC,
                    0,
                );
                latencies.record(start.elapsed().as_nanos() as u64)?;

                total_orders += 1;

                // 消费所有三层事件
                while let Ok(_) = depth_rx.pop() {
                    snapshot_count += 1;
                }
                while let Ok(_) = depth50_rx.pop() {
                    depth50_count += 1;
                }
                while let Ok(_) = level2_rx.pop() {
                    level2_count += 1;
                }

                if order_delay_us > 0 {
                    thread::sleep(Duration::from_micros(order_delay_us));
                }

                if total_start.elapsed().as_secs() >= duration_sec {
                    break;
                }
            }
            if total_start.elapsed().as_secs() >= duration_sec {
                break;
            }
        }
    }

    while let Ok(_) = depth_rx.pop() {
        snapshot_count += 1;
    }
    while let Ok(_) = depth50_rx.pop() {
        depth50_count += 1;
    }
    while let Ok(_) = level2_rx.pop() {
        level2_count += 1;
    }

    let total_elapsed = total_start.elapsed();
    let tps = total_orders as f64 / total_elapsed.as_secs_f64();
    let p50_ns = latencies.value_at_quantile(0.5);
    let p99_ns = latencies.value_at_quantile(0.99);

    println!(
        "{:<50} | TPS: {:>7.2}M | P50: {:>4}ns | P99: {:>5}ns | BBO: {:>4} D50: {:>4} L2: {:>4}",
        name,
        tps / 1_000_000.0,
        p50_ns,
        p99_ns,
        snapshot_count,
        depth50_count,
        level2_count
    );

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔════════════════════════════════════════════════════════════════════════════════════╗");
    println!("║       完整的深度采样性能测试 - 所有三层sender都启用 + 完整计时                    ║");
    println!("╚════════════════════════════════════════════════════════════════════════════════════╝\n");

    println!("预热...");
    let mut engine = MatchingEngine::new(PoolConfig::default())?;
    for i in 0..100 {
        let order = Order::new(i as u64, Side::Buy, 50000.0, 10.0, TimeInForce::GTC, 0);
    }
    println!("完成\n");

    println!("【采样配置】");
    println!("  BBO采样: 10ms");
    println!("  Depth50采样: 50ms");
    println!("  Level2采样: 100ms");
    println!("  ✅ WITH所有三个sender\n");

    println!("{}", "─".repeat(105));
    println!("场景                                                | TPS        | P50延迟 | P99延迟 | BBO采样 | D50采样 | L2采样");
    println!("{}", "─".repeat(105));

    // 无延迟 - 禁用increments
    benchmark_complete(
        "【1】无延迟 - 仅BBO采样",
        false,
        0,
        2,
    )?;

    // 无延迟 - 启用increments（完整三层）
    benchmark_complete(
        "【2】无延迟 - BBO+D50+L2采样",
        true,
        0,
        2,
    )?;

    // 10us延迟 - 禁用increments
    benchmark_complete(
        "【3】10us延迟 - 仅BBO采样",
        false,
        10,
        2,
    )?;

    // 10us延迟 - 启用increments
    benchmark_complete(
        "【4】10us延迟 - BBO+D50+L2采样",
        true,
        10,
        2,
    )?;

    // 100us延迟 - 禁用increments
    benchmark_complete(
        "【5】100us延迟 - 仅BBO采样",
        false,
        100,
        2,
    )?;

    // 100us延迟 - 启用increments
    benchmark_complete(
        "【6】100us延迟 - BBO+D50+L2采样",
        true,
        100,
        2,
    )?;

    println!("{}", "─".repeat(105));

    println!("\n【预期结果】");
    println!("启用increments应该导致：");
    println!("  • TPS下降 (因为额外的采样处理)");
    println!("  • P99延迟增加 (因为更多的计算)");
    println!("  • D50和L2事件出现 (如果采样间隔足够长)");

    Ok(())
}
