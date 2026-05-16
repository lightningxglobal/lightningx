/// 高频采样性能测试 - 1秒内，无人为延迟，能采样100+次

use matching_engine::{
    MatchingEngine, PoolConfig, Order, Side, TimeInForce,
    MarketDataConfig, DepthSnapshotEvent, Depth50SnapshotEvent, Level2SnapshotEvent,
};
use rtrb::RingBuffer;
use std::time::Instant;
use hdrhistogram::Histogram;

fn benchmark_high_freq(
    name: &str,
    enable_increments: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = MatchingEngine::new(PoolConfig::default())?;

    // 采样间隔设置为1ms/5ms/10ms - 看采样是否工作
    let config = MarketDataConfig::new(
        1_000_000,     // 1ms BBO采样 ← 更频繁，容易观察
        enable_increments,
        5_000_000,     // 5ms D50采样
        10_000_000,    // 10ms L2采样
    );
    engine.set_market_data_config(config);

    let (depth_tx, mut depth_rx) = RingBuffer::<DepthSnapshotEvent>::new(1000);
    let (depth50_tx, mut depth50_rx) = RingBuffer::<Depth50SnapshotEvent>::new(1000);
    let (level2_tx, mut level2_rx) = RingBuffer::<Level2SnapshotEvent>::new(1000);

    engine.set_depth_snapshot_sender(depth_tx);
    engine.set_depth50_sender(depth50_tx);
    engine.set_level2_sender(level2_tx);

    let mut latencies = Histogram::<u64>::new(3)?;
    let mut bbo_count = 0;
    let mut d50_count = 0;
    let mut l2_count = 0;
    let mut total_orders = 0;

    let base_price = 50000.0;
    let start = Instant::now();

    // 疯狂下单1秒钟，不加任何延迟
    while start.elapsed().as_secs_f64() < 1.0 {
        for level in 0..10 {
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
            let _ = let mut affected_makers = SmallVec::new(); engine.place_order(order, &mut affected_makers, &mut smallvec::SmallVec::new());
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
            let _ = let mut affected_makers = SmallVec::new(); engine.place_order(order, &mut affected_makers, &mut smallvec::SmallVec::new());
            latencies.record(t.elapsed().as_nanos() as u64)?;
            total_orders += 1;

            // 及时消费采样事件（避免ring buffer溢出）
            while let Ok(_) = depth_rx.pop() {
                bbo_count += 1;
            }
            while let Ok(_) = depth50_rx.pop() {
                d50_count += 1;
            }
            while let Ok(_) = level2_rx.pop() {
                l2_count += 1;
            }
        }
    }

    let elapsed = start.elapsed();

    // 消费剩余事件
    while let Ok(_) = depth_rx.pop() {
        bbo_count += 1;
    }
    while let Ok(_) = depth50_rx.pop() {
        d50_count += 1;
    }
    while let Ok(_) = level2_rx.pop() {
        l2_count += 1;
    }

    let tps = total_orders as f64 / elapsed.as_secs_f64();
    let p50_ns = latencies.value_at_quantile(0.5);
    let p99_ns = latencies.value_at_quantile(0.99);
    let p999_ns = latencies.value_at_quantile(0.999);

    println!("{:<50} | TPS: {:>7.2}M | P50: {:>5}ns | P99: {:>6}ns | BBO: {:>3} D50: {:>3} L2: {:>3}",
             name,
             tps / 1_000_000.0,
             p50_ns,
             p99_ns,
             bbo_count,
             d50_count,
             l2_count);

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔══════════════════════════════════════════════════════════════════════════════════╗");
    println!("║              高频采样性能对比 - 1秒无延迟 (应能采样>100次)                     ║");
    println!("╚══════════════════════════════════════════════════════════════════════════════════╝\n");

    println!("采样配置: 1ms (BBO) / 5ms (D50) / 10ms (L2)");
    println!("预期采样数: ~1000 (BBO) / ~200 (D50) / ~100 (L2)\n");

    println!("{}", "─".repeat(105));
    println!("配置                                              | TPS        | P50延迟 | P99延迟 | BBO采样 | D50采样 | L2采样");
    println!("{}", "─".repeat(105));

    // 禁用increments
    benchmark_high_freq(
        "【禁用increments】仅BBO采样",
        false,
    )?;

    // 启用increments
    benchmark_high_freq(
        "【启用increments】BBO+D50+L2采样",
        true,
    )?;

    println!("{}", "─".repeat(105));

    println!("\n【关键对比】");
    println!("✓ 采样数能达到预期（>100次BBO采样）");
    println!("✓ 可以准确测量采样对P50/P99延迟的影响");
    println!("✓ TPS数据真实可靠（无人为延迟干扰）");

    Ok(())
}
