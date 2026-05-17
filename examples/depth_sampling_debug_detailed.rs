use matching_engine::{
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

    println!("采样间隔配置检查:");
    println!("  shallow_sample_interval_ns = {} (预期 100,000,000)", config.shallow_sample_interval_ns);
    println!("  enable_depth_increments = {}", config.enable_depth_increments);
    println!("  depth50_interval_ns = {} (预期 500,000,000)", config.depth50_interval_ns);
    println!("  level2_interval_ns = {} (预期 1,000,000,000)", config.level2_interval_ns);
    println!();

    let mut bbo_count = 0;
    let mut d50_count = 0;
    let mut l2_count = 0;
    let mut total_orders = 0;

    let base_price = 50000.0;
    let start = Instant::now();

    // 只运行1秒，更容易调试
    while start.elapsed().as_secs_f64() < 1.0 {
        for level in 0..5 {
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
            while let Ok(evt) = depth_rx.pop() {
                bbo_count += 1;
                if bbo_count <= 3 {
                    println!("[BBO样本 #{}] ts={}", bbo_count, evt.timestamp);
                }
            }
            while let Ok(evt) = depth50_rx.pop() {
                d50_count += 1;
                if d50_count <= 3 {
                    println!("[D50样本 #{}] ts={}", d50_count, evt.timestamp);
                }
            }
            while let Ok(evt) = level2_rx.pop() {
                l2_count += 1;
                if l2_count <= 3 {
                    println!("[L2样本 #{}] ts={}", l2_count, evt.timestamp);
                }
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

    println!("\n【1秒运行结果】");
    println!("运行时间: {:.3} 秒", elapsed.as_secs_f64());
    println!("总订单数: {} 个", total_orders);
    println!("TPS: {:.2}M", tps / 1_000_000.0);
    println!();
    println!("【采样结果】");
    println!("BBO采样:  {} 个 (预期 ~10)", bbo_count);
    println!("D50采样:  {} 个 (预期 ~2)", d50_count);
    println!("L2采样:   {} 个 (预期 ~1)", l2_count);

    Ok(())
}
