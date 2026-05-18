/// CLOCK_REALTIME采样验证 - 10秒高频无延迟

use lightning_exchange::{
    MatchingEngine, PoolConfig, Order, Side, TimeInForce,
    MarketDataConfig, DepthSnapshotEvent, Depth50SnapshotEvent, Level2SnapshotEvent,
};
use rtrb::RingBuffer;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = MatchingEngine::new(PoolConfig::default())?;

    // 使用现实的采样间隔
    let config = MarketDataConfig::new(
        100_000_000,   // 100ms BBO采样
        true,
        500_000_000,   // 500ms D50采样
        1_000_000_000, // 1s L2采样
    );
    engine.set_market_data_config(config);

    let (depth_tx, mut depth_rx) = RingBuffer::<DepthSnapshotEvent>::new(10000);
    let (depth50_tx, mut depth50_rx) = RingBuffer::<Depth50SnapshotEvent>::new(1000);
    let (level2_tx, mut level2_rx) = RingBuffer::<Level2SnapshotEvent>::new(100);

    engine.set_depth_snapshot_sender(depth_tx);
    engine.set_depth50_sender(depth50_tx);
    engine.set_level2_sender(level2_tx);

    println!("╔═══════════════════════════════════════════════════════════════════════════╗");
    println!("║    CLOCK_REALTIME采样验证 - 10秒无延迟高频下单                          ║");
    println!("╚═══════════════════════════════════════════════════════════════════════════╝\n");

    println!("采样间隔: 100ms (BBO) / 500ms (D50) / 1s (L2)");
    println!("预期采样数: ~100 (BBO) / ~20 (D50) / ~10 (L2)\n");

    let mut bbo_count = 0;
    let mut d50_count = 0;
    let mut l2_count = 0;
    let mut total_orders = 0;

    let base_price = 50000.0;
    let start = Instant::now();

    // 10秒无延迟高频下单
    while start.elapsed().as_secs_f64() < 10.0 {
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
            total_orders += 1;

            // 消费采样事件
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

    println!("【10秒运行结果】");
    println!("运行时间: {:.2} 秒", elapsed.as_secs_f64());
    println!("总订单数: {} 个", total_orders);
    println!("TPS: {:.2}M", tps / 1_000_000.0);
    println!();
    println!("【采样结果】");
    println!("BBO采样:     {:>4} 个 (预期 ~100)", bbo_count);
    println!("D50采样:     {:>4} 个 (预期 ~20)", d50_count);
    println!("L2采样:      {:>4} 个 (预期 ~10)", l2_count);
    println!();
    println!("【分析】");

    if bbo_count >= 50 {
        println!("✓ BBO采样工作正常 - CLOCK_REALTIME精度足够");
    } else if bbo_count >= 10 {
        println!("⚠ BBO采样频率偏低 - 可能时间精度不足");
    } else {
        println!("❌ BBO采样频率极低 - 采样条件检查有问题");
    }

    if d50_count >= 5 {
        println!("✓ D50采样工作正常");
    } else {
        println!("⚠ D50采样频率偏低");
    }

    if l2_count >= 3 {
        println!("✓ L2采样工作正常");
    } else {
        println!("⚠ L2采样频率偏低");
    }

    Ok(())
}
