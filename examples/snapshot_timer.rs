//! 1ms定时器快照生成示例
//!
//! 演示市场数据引擎的定时器功能：
//! 1. 在单独线程中以1ms间隔生成快照
//! 2. 收集所有快照类型：BBO、Level2、聚合成交、24h统计
//! 3. 测量快照的生成和发送延迟
//! 4. 验证1ms的定时器精度

use matching_engine::{
    MatchingEngine, MarketDataEngine, PublishedSnapshot, SnapshotTimer, PoolConfig,
    Order, Side, TimeInForce, TradeEvent,
};
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

fn main() {
    println!("=== 1ms定时器快照生成示例 ===\n");

    // 1. 创建TradeEvent通道
    let (sender, receiver) = crossbeam::channel::bounded::<TradeEvent>(1_000_000);
    println!("✓ 创建TradeEvent通道 (capacity: 1,000,000)");

    // 2. 创建市场数据引擎并包装在Arc<Mutex<>>中
    let engine = Arc::new(Mutex::new(MarketDataEngine::new(receiver)));
    println!("✓ 创建市场数据引擎");

    // 3. 创建快照发布通道
    let (snapshot_tx, snapshot_rx) = crossbeam::channel::unbounded::<PublishedSnapshot>();
    println!("✓ 创建快照发布通道（无界）");

    // 4. 启动1ms定时器
    let engine_clone = engine.clone();
    let timer = SnapshotTimer::spawn(engine_clone, snapshot_tx);
    println!("✓ 启动1ms定时器\n");

    // 5. 在单独线程中生成交易事件
    let sender_clone = sender.clone();
    let trade_thread = thread::spawn(move || {
        // 生成50笔交易
        for i in 1..=50 {
            // 买单
            let buy_order = Order::new(
                i * 2 - 1,
                Side::Buy,
                50000.0 + (i as f64 * 10.0),
                10.0,
                TimeInForce::GTC,
                0,
            );

            let mut matching_engine = MatchingEngine::new(PoolConfig::default())
                .expect("创建匹配引擎失败");
            matching_engine.set_trade_event_sender(sender_clone.clone());
            let _ = matching_engine.place_order(buy_order);

            // 卖单 - 触发成交
            let sell_order = Order::new(
                i * 2,
                Side::Sell,
                50000.0 + (i as f64 * 10.0),
                10.0,
                TimeInForce::IOC,
                0,
            );

            let _ = matching_engine.place_order(sell_order);

            // 稍微延迟以允许引擎处理
            thread::sleep(Duration::from_micros(100));
        }
    });

    // 6. 收集快照并测量间隔
    let snapshot_count = Arc::new(AtomicUsize::new(0));
    let snapshot_count_clone = snapshot_count.clone();

    let snapshot_thread = thread::spawn(move || {
        let start = Instant::now();
        let mut last_timestamp = 0u64;
        let mut interval_samples = Vec::new();

        for _ in 0..100 {  // 收集100个快照
            match snapshot_rx.recv_timeout(Duration::from_secs(5)) {
                Ok(snapshot) => {
                    let count = snapshot_count_clone.fetch_add(1, Ordering::Relaxed) + 1;

                    // 计算快照间隔
                    if last_timestamp > 0 && snapshot.timestamp > last_timestamp {
                        let interval_ns = snapshot.timestamp - last_timestamp;
                        interval_samples.push(interval_ns);

                        if count <= 10 || count % 20 == 0 {
                            println!(
                                "快照 #{}: seq={}, interval={:.3}ms, BBO bid={}, ask={}",
                                count,
                                snapshot.sequence,
                                interval_ns as f64 / 1_000_000.0,
                                snapshot.bbo.best_bid_price,
                                snapshot.bbo.best_ask_price
                            );
                        }
                    }

                    last_timestamp = snapshot.timestamp;

                    // 验证快照完整性
                    assert!(snapshot.timestamp > 0, "快照时间戳不能为0");
                    assert!(snapshot.sequence > 0, "快照序列号不能为0");
                }
                Err(_) => {
                    println!("等待快照超时");
                    break;
                }
            }
        }

        (interval_samples, start.elapsed())
    });

    // 7. 等待两个线程完成
    let _ = trade_thread.join();
    let (interval_samples, total_duration) = snapshot_thread.join().unwrap();

    // 8. 停止定时器
    timer.stop();
    thread::sleep(Duration::from_millis(10));
    println!("\n✓ 停止定时器");

    // 9. 分析快照间隔
    println!("\n=== 快照间隔分析 ===\n");

    let count = snapshot_count.load(Ordering::Relaxed);
    println!("收集的快照数: {}", count);

    if !interval_samples.is_empty() {
        let mut samples = interval_samples.clone();
        samples.sort();

        let min = samples.first().unwrap();
        let max = samples.last().unwrap();
        let avg = samples.iter().sum::<u64>() as f64 / samples.len() as f64;

        // 计算百分位数（毫秒）
        let p50_idx = samples.len() / 2;
        let p99_idx = (samples.len() * 99) / 100;

        let p50 = samples[p50_idx];
        let p99 = if p99_idx < samples.len() {
            samples[p99_idx]
        } else {
            samples[samples.len() - 1]
        };

        println!("时间间隔（纳秒）:");
        println!("  最小: {:.2} ns ({:.3} ms)", min, *min as f64 / 1_000_000.0);
        println!("  平均: {:.2} ns ({:.3} ms)", avg, avg / 1_000_000.0);
        println!("  P50:  {:.2} ns ({:.3} ms)", p50, p50 as f64 / 1_000_000.0);
        println!("  P99:  {:.2} ns ({:.3} ms)", p99, p99 as f64 / 1_000_000.0);
        println!("  最大: {:.2} ns ({:.3} ms)", max, *max as f64 / 1_000_000.0);

        // 检查是否接近1ms目标 (±0.5ms容限)
        let target_ns = 1_000_000u64;  // 1ms in nanoseconds
        let tolerance_ns = 500_000u64; // ±0.5ms tolerance
        let within_target = samples
            .iter()
            .filter(|&&s| s >= target_ns - tolerance_ns && s <= target_ns + tolerance_ns)
            .count();

        println!("\n目标间隔: 1ms");
        println!("容限: ±0.5ms");
        println!("满足目标: {}/{} ({:.1}%)\n",
            within_target,
            samples.len(),
            (within_target * 100) as f64 / samples.len() as f64
        );

        let jitter_ns = max - min;
        let jitter_ms = jitter_ns as f64 / 1_000_000.0;
        println!("抖动 (max - min): {:.3} ms", jitter_ms);

        println!("\n总耗时: {:.3}s", total_duration.as_secs_f64());

        if avg >= (target_ns as f64 - 100_000.0) && avg <= (target_ns as f64 + 100_000.0) {
            println!("\n✓ 定时器精度良好！平均间隔接近1ms目标");
        } else {
            println!("\n⚠ 定时器精度可能需要调整");
        }
    }
}
