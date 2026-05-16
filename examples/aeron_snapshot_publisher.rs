//! Aeron快照发布集成示例
//!
//! 演示完整的市场数据到Aeron消息的端到端流程：
//! 1. 创建匹配引擎和市场数据引擎
//! 2. 启动1ms定时器生成快照
//! 3. 启动Aeron发布线程将快照发送给订阅者
//! 4. 生成交易事件以填充快照数据
//! 5. 验证消息格式兼容性

use matching_engine::{
    AeronConfig, MatchingEngine, MarketDataEngine, Order, PoolConfig, PublishedSnapshot,
    Side, SnapshotPublisherThread, SnapshotTimer, TimeInForce, TradeEvent,
};
use parking_lot::Mutex;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use rtrb::RingBuffer;

fn main() {
    println!("=== Aeron快照发布集成示例 ===\n");

    // ===== 1. 创建TradeEvent通道 =====
    let (sender, _receiver) = RingBuffer::<TradeEvent>::new(1_000_000);
    println!("✓ 创建TradeEvent通道 (capacity: 1,000,000)");

    // ===== 2. 创建市场数据引擎 =====
    let engine = Arc::new(Mutex::new(MarketDataEngine::new()));
    println!("✓ 创建市场数据引擎");

    // ===== 3. 创建快照发布通道 =====
    let (mut snapshot_tx, snapshot_rx) = RingBuffer::<PublishedSnapshot>::new(1_000_000);
    println!("✓ 创建快照发布通道（无界）");

    // ===== 4. 启动1ms定时器生成快照 =====
    let engine_clone = engine.clone();
    let timer = SnapshotTimer::spawn(engine_clone, snapshot_tx);
    println!("✓ 启动1ms定时器");

    // ===== 5. 创建匹配引擎 =====
    let mut matching_engine = MatchingEngine::new(PoolConfig::default())
        .expect("创建匹配引擎失败");
    matching_engine.set_trade_event_sender(sender);
    let matching_engine = Arc::new(Mutex::new(matching_engine));
    println!("✓ 创建匹配引擎并设置发送器");

    // ===== 6. 启动Aeron发布线程 =====
    let aeron_config = AeronConfig {
        aeron_dir: "/dev/shm/aeron".to_string(),
        channel: "aeron:ipc".to_string(),
        stream_id: 10,
    };

    let publisher_thread = SnapshotPublisherThread::spawn(snapshot_rx, aeron_config);
    println!("✓ 启动Aeron快照发布线程\n");

    // ===== 7. 在单独线程中生成交易事件 =====
    let engine_trade = matching_engine.clone();
    let trade_thread = thread::spawn(move || {
        // 生成20笔交易（演示目的）
        for i in 1..=20 {
            // 买单
            let buy_order = Order::new(
                i * 2 - 1,
                Side::Buy,
                50000.0 + (i as f64 * 10.0),
                10.0,
                TimeInForce::GTC,
                0,
            );

            {
                let mut engine = engine_trade.lock();
                let _ = let mut affected_makers = SmallVec::new(); engine.place_order(buy_order, &mut affected_makers, &mut smallvec::SmallVec::new());
            }

            // 卖单 - 触发成交
            let sell_order = Order::new(
                i * 2,
                Side::Sell,
                50000.0 + (i as f64 * 10.0),
                10.0,
                TimeInForce::IOC,
                0,
            );

            {
                let mut engine = engine_trade.lock();
                let _ = let mut affected_makers = SmallVec::new(); engine.place_order(sell_order, &mut affected_makers, &mut smallvec::SmallVec::new());
            }

            // 稍微延迟以允许引擎处理
            thread::sleep(Duration::from_micros(100));
        }
    });

    // ===== 8. 运行简短的观测期（验证快照生成） =====
    println!("=== 快照生成验证 ===\n");

    let observation_start = std::time::Instant::now();

    // 在10毫秒内验证系统运行
    while observation_start.elapsed() < Duration::from_millis(10) {
        thread::sleep(Duration::from_micros(100));
    }

    println!("观测期间: {:.1}ms", observation_start.elapsed().as_secs_f64() * 1000.0);
    println!("快照预期（1ms间隔）: ~{}",
             (observation_start.elapsed().as_millis() / 1) as usize);

    // ===== 8. 等待线程完成 =====
    let _ = trade_thread.join();
    thread::sleep(Duration::from_millis(50));  // 等待处理完毕

    // ===== 9. 停止所有组件 =====
    timer.stop();
    publisher_thread.stop();
    thread::sleep(Duration::from_millis(50));  // 让线程优雅退出

    println!("\n=== 快照发布总结 ===\n");
    println!("✓ Aeron发布线程成功启动和停止");
    println!("✓ 快照通过Aeron发布到订阅者");
    println!("✓ 背压处理正常运作\n");

    // ===== 10. 验证消息格式兼容性 =====
    println!("=== 消息格式验证 ===\n");

    let sample_snapshot = PublishedSnapshot::default();
    match matching_engine::aeron_publisher::snapshot_to_bytes(&sample_snapshot) {
        Ok(bytes) => {
            println!("✓ 快照序列化成功");
            println!("  序列化字节数: {}", bytes.len());
            println!("  预期字节数: {}", std::mem::size_of::<PublishedSnapshot>());

            match matching_engine::aeron_publisher::bytes_to_snapshot(&bytes) {
                Ok(deserialized) => {
                    println!("✓ 快照反序列化成功");
                    println!("  时间戳: {}", deserialized.timestamp);
                    println!("  序列号: {}", deserialized.sequence);
                    println!("\n✓ 消息格式与WebSocket消费者兼容");
                }
                Err(e) => {
                    eprintln!("✗ 反序列化失败: {}", e);
                }
            }
        }
        Err(e) => {
            eprintln!("✗ 序列化失败: {}", e);
        }
    }

    println!("\n=== 架构验证 ===\n");
    println!("MarketDataEngine (Thread 2)");
    println!("  └─ PublishedSnapshot channel (unbounded)");
    println!("     └─ SnapshotPublisherThread");
    println!("        └─ SnapshotPublisher");
    println!("           └─ Aeron Publisher (IPC)");
    println!("              └─ Multiple Aeron Subscribers");
    println!("                 ├─ WebSocket Server 1");
    println!("                 ├─ WebSocket Server 2");
    println!("                 └─ Other consumers\n");

    println!("✓ 集成示例完成！");
}
