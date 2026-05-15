/// 真实深度采样性能测试 - 带实际延迟来验证采样
/// 模拟真实交易环境：订单之间有微小延迟（模拟网络/处理延迟）

use matching_engine::{
    MatchingEngine, PoolConfig, Order, Side, TimeInForce,
    MarketDataConfig, DepthSnapshotEvent,
};
use rtrb::RingBuffer;
use std::time::Instant;
use hdrhistogram::Histogram;
use std::thread;
use std::time::Duration;

fn benchmark_with_realistic_timing(
    name: &str,
    enable_increments: bool,
    order_delay_us: u64,  // 每个订单之间的延迟（微秒）
    total_duration_sec: u64, // 运行多长时间
) -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = MatchingEngine::new(PoolConfig::default())?;

    // 现实的采样间隔
    let config = MarketDataConfig::new(
        100_000_000,   // 100ms BBO采样
        enable_increments,
        500_000_000,   // 500ms Depth50采样
        1_000_000_000, // 1s Level2采样
    );
    engine.set_market_data_config(config);

    let (depth_tx, mut depth_rx) = RingBuffer::<DepthSnapshotEvent>::new(1000);
    engine.set_depth_snapshot_sender(depth_tx);

    let mut latencies = Histogram::<u64>::new(3)?;
    let mut snapshot_count = 0;
    let mut total_orders = 0;

    let base_price = 50000.0;
    let total_start = Instant::now();

    // 运行指定时间
    while total_start.elapsed().as_secs() < total_duration_sec {
        for side_idx in 0..2 {
            for level in 0..5 {
                let price = if side_idx == 0 {
                    base_price - 2.0 + level as f64
                } else {
                    base_price + level as f64
                };

                let start = Instant::now();
                let order = Order::new(
                    (total_orders as u64) * 10000 + side_idx * 1000 + level,
                    if side_idx == 0 { Side::Buy } else { Side::Sell },
                    price,
                    10.0,
                    TimeInForce::GTC,
                    0,
                );
                let _ = engine.place_order(order);
                latencies.record(start.elapsed().as_nanos() as u64)?;

                total_orders += 1;

                // 消费采样
                while let Ok(_) = depth_rx.pop() {
                    snapshot_count += 1;
                }

                // 模拟网络/处理延迟
                if order_delay_us > 0 {
                    thread::sleep(Duration::from_micros(order_delay_us));
                }

                if total_start.elapsed().as_secs() >= total_duration_sec {
                    break;
                }
            }
            if total_start.elapsed().as_secs() >= total_duration_sec {
                break;
            }
        }
    }

    // 消费剩余快照
    while let Ok(_) = depth_rx.pop() {
        snapshot_count += 1;
    }

    let total_elapsed = total_start.elapsed();
    let tps = total_orders as f64 / total_elapsed.as_secs_f64();
    let p50_ns = latencies.value_at_quantile(0.5);
    let p99_ns = latencies.value_at_quantile(0.99);

    println!(
        "{:<55} | TPS: {:>7.2}M | P50: {:>5} ns | P99: {:>6} ns | Snapshots: {:>4}",
        name,
        tps / 1_000_000.0,
        p50_ns,
        p99_ns,
        snapshot_count
    );

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 带实际延迟的深度采样性能测试 ===\n");

    println!("预热中...");
    let mut engine = MatchingEngine::new(PoolConfig::default())?;
    for i in 0..100 {
        let order = Order::new(i as u64, Side::Buy, 50000.0, 10.0, TimeInForce::GTC, 0);
        let _ = engine.place_order(order);
    }
    println!("预热完成\n");

    println!("【采样间隔】");
    println!("  BBO采样: 100ms");
    println!("  Depth50采样: 500ms");
    println!("  Level2采样: 1s\n");

    println!("{}", "─".repeat(115));
    println!("场景（每订单延迟）                                  | TPS        | P50延迟  | P99延迟 | 采样数");
    println!("{}", "─".repeat(115));

    // 场景1: 无延迟（已知问题：采样会失败）
    benchmark_with_realistic_timing(
        "【1】无延迟（CPU最大速度）",
        false,
        0,
        3,
    )?;

    benchmark_with_realistic_timing(
        "【2】无延迟 + increments",
        true,
        0,
        3,
    )?;

    // 场景2: 10微秒延迟（模拟实际交易）
    benchmark_with_realistic_timing(
        "【3】10us延迟（实际交易模拟）",
        false,
        10,
        3,
    )?;

    benchmark_with_realistic_timing(
        "【4】10us延迟 + increments",
        true,
        10,
        3,
    )?;

    // 场景3: 100微秒延迟（较重的处理）
    benchmark_with_realistic_timing(
        "【5】100us延迟（较重处理）",
        false,
        100,
        3,
    )?;

    benchmark_with_realistic_timing(
        "【6】100us延迟 + increments",
        true,
        100,
        3,
    )?;

    println!("{}", "─".repeat(115));

    println!("\n【观察】");
    println!("- 无延迟：采样失效（SystemTime分辨率不足）");
    println!("- 10us/100us延迟：采样应该正常工作");
    println!("- Snapshots数应该随运行时间线性增长");

    Ok(())
}
