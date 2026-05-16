/// 长时间运行的采样测试 - 验证CLOCK_MONOTONIC是否有效工作

use matching_engine::{
    MatchingEngine, PoolConfig, Order, Side, TimeInForce,
    MarketDataConfig, DepthSnapshotEvent, Depth50SnapshotEvent, Level2SnapshotEvent,
};
use rtrb::RingBuffer;
use std::time::Instant;
use std::thread;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = MatchingEngine::new(PoolConfig::default())?;

    // 采样间隔必须足够大以匹配系统时钟精度（约40-100ns）
    // 1ms = 1,000,000ns，应该可以工作
    let config = MarketDataConfig::new(
        100_000_000,   // 100ms BBO采样 ← 系统时钟精度需要毫秒级
        true,
        500_000_000,   // 500ms D50采样
        1_000_000_000, // 1s L2采样
    );
    engine.set_market_data_config(config);

    let (depth_tx, mut depth_rx) = RingBuffer::<DepthSnapshotEvent>::new(100000);
    let (depth50_tx, mut depth50_rx) = RingBuffer::<Depth50SnapshotEvent>::new(10000);
    let (level2_tx, mut level2_rx) = RingBuffer::<Level2SnapshotEvent>::new(1000);

    engine.set_depth_snapshot_sender(depth_tx);
    engine.set_depth50_sender(depth50_tx);
    engine.set_level2_sender(level2_tx);

    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║   长时间采样测试 - 验证Instant精度 (10秒，1us延迟)          ║");
    println!("╚════════════════════════════════════════════════════════════════╝\n");

    println!("采样间隔: 100ms (BBO) / 500ms (D50) / 1s (L2)");
    println!("吞吐: ~0.21M TPS (with 1us order delay)");
    println!("预期采样数: ~100 (BBO) / ~20 (D50) / ~10 (L2)\n");

    let start = Instant::now();
    let mut order_id: u64 = 0;
    let target_duration = Duration::from_secs(10);

    // 用微小延迟模拟持续的订单流
    while start.elapsed() < target_duration {
        let price = 50000.0 + (order_id as f64 % 100.0) * 0.01;
        let order = Order::new(
            order_id,
            if order_id % 2 == 0 { Side::Buy } else { Side::Sell },
            price,
            10.0,
            TimeInForce::GTC,
            0,
        );
        let _ = let mut affected_makers = SmallVec::new(); engine.place_order(order, &mut affected_makers, &mut smallvec::SmallVec::new());
        order_id += 1;

        // 1微秒延迟（模拟网络/处理）
        thread::sleep(Duration::from_micros(1));
    }

    let elapsed = start.elapsed();
    println!("【测试完成】");
    println!("运行时间: {:.1} 秒", elapsed.as_secs_f64());
    println!("总订单数: {}", order_id);
    println!("订单吞吐: {:.2}M TPS\n", order_id as f64 / elapsed.as_secs_f64() / 1_000_000.0);

    // 消费所有剩余事件
    let mut bbo_count = 0;
    let mut d50_count = 0;
    let mut l2_count = 0;

    while let Ok(_) = depth_rx.pop() {
        bbo_count += 1;
    }
    while let Ok(_) = depth50_rx.pop() {
        d50_count += 1;
    }
    while let Ok(_) = level2_rx.pop() {
        l2_count += 1;
    }

    println!("【采样结果】");
    println!("BBO快照:     {:>6} (预期 ~10000)", bbo_count);
    println!("Depth-50:    {:>6} (预期 ~2000)", d50_count);
    println!("Level2-400:  {:>6} (预期 ~1000)", l2_count);

    println!("\n【分析】");
    if bbo_count > 5000 {
        println!("✓ BBO采样工作正常 (CLOCK_MONOTONIC精度足够)");
    } else {
        println!("❌ BBO采样不足 - 时钟可能还是有问题");
    }

    if d50_count > 1000 {
        println!("✓ Depth-50采样工作正常");
    } else {
        println!("❌ Depth-50采样不足");
    }

    if l2_count > 500 {
        println!("✓ Level2采样工作正常");
    } else {
        println!("❌ Level2采样不足");
    }

    Ok(())
}
