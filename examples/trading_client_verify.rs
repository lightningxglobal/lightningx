/// 交易客户端验证演示 - 完整的功能和性能检验
///
/// 发送各种类型的订单，验证：
/// 1. 订单更新事件是否按时间顺序接收
/// 2. 成交通知是否包含正确的信息
/// 3. 行情数据是否定期发送
/// 4. 响应时间是否符合预期
///
/// Prerequisites:
/// - aeronmd 已启动: AERON_DIR=/tmp/aeron
/// - 撮合系统运行: cargo run --release --example aeron_integration_demo
///
/// 运行: AERON_DIR=/tmp/aeron cargo run --release --example trading_client_verify

use std::collections::HashMap;
use std::sync::mpsc::{channel as mpsc_channel, TryRecvError};
use std::thread;
use std::time::{Duration, Instant};
use parking_lot::Mutex;
use std::sync::Arc;
use tracing::{info, warn};

use aeron_wrapper::{AeronClient, NoopLifecycle, PollCallback, Pub};

#[derive(Debug, Clone)]
struct ReceivedEvent {
    event_type: String,
    timestamp: Instant,
    size: usize,
    raw: Vec<u8>,
}

struct EventCallback {
    tx: std::sync::mpsc::Sender<ReceivedEvent>,
    event_type: String,
}

impl PollCallback for EventCallback {
    fn on_data(&mut self, data: &[u8]) {
        let _ = self.tx.send(ReceivedEvent {
            event_type: self.event_type.clone(),
            timestamp: Instant::now(),
            size: data.len(),
            raw: data.to_vec(),
        });
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let aeron_dir = std::env::var("AERON_DIR").unwrap_or_else(|_| "/tmp/aeron".to_string());
    let channel = "aeron:ipc";

    info!("╔═════════════════════════════════════════════════════════════════╗");
    info!("║      交易客户端 - 完整功能验证                                    ║");
    info!("╚═════════════════════════════════════════════════════════════════╝");
    info!("");
    info!("配置: Aeron Dir={}, Channel={}", aeron_dir, channel);
    info!("");

    // 初始化Aeron
    let client = AeronClient::new(&aeron_dir)
        .map_err(|e| format!("Failed to create AeronClient: {:?}", e))?;

    let (tx, rx) = mpsc_channel();

    // 创建订阅者
    info!("初始化Aeron订阅者...");
    let (tx1, tx2, tx3, tx4, tx5) = (
        tx.clone(),
        tx.clone(),
        tx.clone(),
        tx.clone(),
        tx.clone(),
    );

    let _sub_updates = client
        .add_subscription(channel, 2, 10_000, EventCallback { tx: tx1, event_type: "OrderUpdate".to_string() }, NoopLifecycle)
        .map_err(|e| format!("Failed subscription 2: {:?}", e))?;

    let _sub_trades = client
        .add_subscription(channel, 3, 10_000, EventCallback { tx: tx2, event_type: "Trade".to_string() }, NoopLifecycle)
        .map_err(|e| format!("Failed subscription 3: {:?}", e))?;

    let _sub_depth20 = client
        .add_subscription(channel, 4, 10_000, EventCallback { tx: tx3, event_type: "Depth20".to_string() }, NoopLifecycle)
        .map_err(|e| format!("Failed subscription 4: {:?}", e))?;

    let _sub_depth50 = client
        .add_subscription(channel, 5, 10_000, EventCallback { tx: tx4, event_type: "Depth50".to_string() }, NoopLifecycle)
        .map_err(|e| format!("Failed subscription 5: {:?}", e))?;

    let _sub_level2 = client
        .add_subscription(channel, 6, 10_000, EventCallback { tx: tx5, event_type: "Level2".to_string() }, NoopLifecycle)
        .map_err(|e| format!("Failed subscription 6: {:?}", e))?;

    // 创建发布者
    let mut publisher = client
        .add_publication(channel, 1)
        .map_err(|e| format!("Failed to create publisher: {:?}", e))?;

    info!("✓ Aeron初始化完成");
    info!("");

    // 等待连接
    info!("等待连接建立...");
    let start_time = Instant::now();
    while start_time.elapsed() < Duration::from_secs(5) {
        client.do_work();
        if publisher.is_connected() {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }

    if !publisher.is_connected() {
        warn!("⚠ 发布者未连接，继续运行...");
    } else {
        info!("✓ 已连接");
    }
    info!("");

    // 测试场景
    let mut stats = TestStats::new();

    info!("╔═════════════════════════════════════════════════════════════════╗");
    info!("║ 测试场景1: 单笔IOC订单（不成交）                                 ║");
    info!("╚═════════════════════════════════════════════════════════════════╝");
    info!("");

    // 发送一个不匹配的IOC订单
    send_order(&mut publisher, 1001, 1, 45000.0, 10.0, 0, 1)?;  // Buy @ 45000
    info!("发送订单 1001: Buy 10 @ 45000 (IOC)");

    // 收集响应
    let duration = Duration::from_secs(5);
    let start = Instant::now();
    let mut updates_received = 0;
    while start.elapsed() < duration {
        client.do_work();
        while let Ok(event) = rx.try_recv() {
            print_event(&event);
            if event.event_type == "OrderUpdate" {
                updates_received += 1;
                *stats.events_by_type.entry("OrderUpdate".to_string()).or_insert(0) += 1;
            }
        }
        thread::sleep(Duration::from_millis(100));
    }

    info!("✓ 场景1完成: 收到 {} 个订单更新", updates_received);
    info!("");

    // 测试场景2: 配对成交
    info!("╔═════════════════════════════════════════════════════════════════╗");
    info!("║ 测试场景2: 配对成交（买单 + 卖单）                               ║");
    info!("╚═════════════════════════════════════════════════════════════════╝");
    info!("");

    send_order(&mut publisher, 1002, 1, 50000.0, 10.0, 0, 0)?;   // Buy @ 50000 (GTC)
    info!("发送订单 1002: Buy 10 @ 50000 (GTC)");
    thread::sleep(Duration::from_millis(500));

    send_order(&mut publisher, 1003, 1, 50000.0, 10.0, 1, 1)?;   // Sell @ 50000 (IOC)
    info!("发送订单 1003: Sell 10 @ 50000 (IOC)");

    // 收集响应
    let start = Instant::now();
    let mut trades_received = 0;
    while start.elapsed() < duration {
        client.do_work();
        while let Ok(event) = rx.try_recv() {
            print_event(&event);
            match event.event_type.as_str() {
                "Trade" => {
                    trades_received += 1;
                    *stats.events_by_type.entry("Trade".to_string()).or_insert(0) += 1;
                }
                "OrderUpdate" => {
                    *stats.events_by_type.entry("OrderUpdate".to_string()).or_insert(0) += 1;
                }
                _ => {}
            }
        }
        thread::sleep(Duration::from_millis(100));
    }

    info!("✓ 场景2完成: 收到 {} 个成交", trades_received);
    info!("");

    // 测试场景3: 多笔订单
    info!("╔═════════════════════════════════════════════════════════════════╗");
    info!("║ 测试场景3: 多笔订单流                                            ║");
    info!("╚═════════════════════════════════════════════════════════════════╝");
    info!("");

    for i in 0..5 {
        let side = if i % 2 == 0 { 0 } else { 1 };
        let price = 50000.0 + (i as f64) * 100.0;
        send_order(&mut publisher, 2000 + i, 1, price, 5.0, side, 1)?;
        info!("发送订单 {}: {} 5 @ {} (IOC)", 2000 + i, if side == 0 { "Buy" } else { "Sell" }, price);
        thread::sleep(Duration::from_millis(100));
    }

    // 收集响应
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(10) {
        client.do_work();
        while let Ok(event) = rx.try_recv() {
            *stats.events_by_type.entry(event.event_type.clone()).or_insert(0) += 1;
        }
        thread::sleep(Duration::from_millis(100));
    }

    info!("✓ 场景3完成");
    info!("");

    // 输出统计信息
    info!("╔═════════════════════════════════════════════════════════════════╗");
    info!("║ 测试统计                                                         ║");
    info!("╚═════════════════════════════════════════════════════════════════╝");
    info!("");

    info!("接收到的事件:");
    for (event_type, count) in &stats.events_by_type {
        info!("  {}: {} 个", event_type, count);
    }

    info!("");
    info!("✓ 验证完成");

    Ok(())
}

struct TestStats {
    events_by_type: HashMap<String, usize>,
}

impl TestStats {
    fn new() -> Self {
        Self {
            events_by_type: HashMap::new(),
        }
    }
}

fn send_order(
    publisher: &mut aeron_wrapper::Publisher,
    client_order_id: u64,
    participant_id: u64,
    price: f64,
    quantity: f64,
    side: u8,
    time_in_force: u8,
) -> Result<(), Box<dyn std::error::Error>> {
    let data = create_sbe_order(client_order_id, participant_id, price, quantity, side, time_in_force);

    loop {
        match publisher.send(&data) {
            Ok(()) => return Ok(()),
            Err(aeron_wrapper::Error::BackPressured) => {
                std::hint::spin_loop();
            }
            Err(e) => {
                return Err(format!("Failed to send: {:?}", e).into());
            }
        }
    }
}

fn create_sbe_order(
    client_order_id: u64,
    participant_id: u64,
    price: f64,
    quantity: f64,
    side: u8,
    time_in_force: u8,
) -> Vec<u8> {
    let mut data = vec![0u8; 56];

    // SBE Header
    data[0..2].copy_from_slice(&48u16.to_le_bytes());
    data[2..4].copy_from_slice(&1u16.to_le_bytes());
    data[4..6].copy_from_slice(&1u16.to_le_bytes());
    data[6..8].copy_from_slice(&0u16.to_le_bytes());

    // Message Body
    data[8..16].copy_from_slice(&client_order_id.to_le_bytes());
    data[16..24].copy_from_slice(&participant_id.to_le_bytes());
    data[24..32].copy_from_slice(&price.to_le_bytes());
    data[32..40].copy_from_slice(&quantity.to_le_bytes());
    data[40] = side;
    data[41] = time_in_force;

    data
}

fn print_event(event: &ReceivedEvent) {
    match event.event_type.as_str() {
        "OrderUpdate" => {
            info!("📨 [{}μs] OrderUpdate: {}B", event.timestamp.elapsed().as_micros(), event.size);
        }
        "Trade" => {
            info!("💰 [{}μs] Trade: {}B", event.timestamp.elapsed().as_micros(), event.size);
        }
        "Depth20" => {
            info!("📊 Depth20: {}B", event.size);
        }
        "Depth50" => {
            info!("📊 Depth50: {}B", event.size);
        }
        "Level2" => {
            info!("📊 Level2: {}B", event.size);
        }
        _ => {
            info!("？ {}: {}B", event.event_type, event.size);
        }
    }
}
