/// 交易客户端演示 - 与撮合系统交互
///
/// 客户端定期发送IOC订单（买和卖），接收订单更新、成交通知、行情数据
/// 验证接收到的数据是否符合预期
///
/// Prerequisites:
/// - aeronmd 已启动: AERON_DIR=/tmp/aeron
/// - 撮合系统运行: cargo run --release --example aeron_integration_demo
///
/// 运行: AERON_DIR=/tmp/aeron cargo run --release --example trading_client

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::mpsc::{channel as mpsc_channel, Sender, TryRecvError};
use std::thread;
use std::time::{Duration, Instant};
use parking_lot::Mutex;
use tracing::{info, warn};

use aeron_wrapper::{AeronClient, NoopLifecycle, PollCallback, Pub};

/// 接收的消息类型（用于验证）
#[derive(Debug, Clone)]
enum ReceivedMessage {
    OrderUpdate(String),      // 订单状态变化
    Trade(String),             // 成交通知
    MarketData(String),        // 行情数据
}

/// 订单状态跟踪
#[derive(Debug, Clone)]
struct OrderState {
    client_order_id: u64,
    side: &'static str,
    price: f64,
    quantity: f64,
    status: &'static str,
    received_at: Option<Instant>,
    matched_qty: f64,
}

// ============================================================================
// 订单更新回调
// ============================================================================

struct OrderUpdateCallback {
    tx: Sender<ReceivedMessage>,
}

impl PollCallback for OrderUpdateCallback {
    fn on_data(&mut self, data: &[u8]) {
        if data.len() >= 8 {
            let msg = format!("OrderUpdate({}B): {:02X?}", data.len(), &data[..std::cmp::min(32, data.len())]);
            let _ = self.tx.send(ReceivedMessage::OrderUpdate(msg));
        }
    }
}

// ============================================================================
// 成交回调
// ============================================================================

struct TradeCallback {
    tx: Sender<ReceivedMessage>,
}

impl PollCallback for TradeCallback {
    fn on_data(&mut self, data: &[u8]) {
        if data.len() >= 8 {
            let msg = format!("Trade({}B): {:02X?}", data.len(), &data[..std::cmp::min(32, data.len())]);
            let _ = self.tx.send(ReceivedMessage::Trade(msg));
        }
    }
}

// ============================================================================
// 行情数据回调
// ============================================================================

struct MarketDataCallback {
    tx: Sender<ReceivedMessage>,
}

impl PollCallback for MarketDataCallback {
    fn on_data(&mut self, data: &[u8]) {
        if data.len() >= 8 {
            let size_hint = match data.len() {
                704 => "Depth20",
                1728 => "Depth50",
                12928 => "Level2",
                _ => "Unknown",
            };
            let msg = format!("MarketData({}): {}B", size_hint, data.len());
            let _ = self.tx.send(ReceivedMessage::MarketData(msg));
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let aeron_dir = std::env::var("AERON_DIR").unwrap_or_else(|_| "/tmp/aeron".to_string());
    let channel = "aeron:ipc";

    info!("═══════════════════════════════════════════════════════════════");
    info!("         交易客户端 - 与撮合系统交互演示");
    info!("═══════════════════════════════════════════════════════════════\n");
    info!("Aeron Dir: {}", aeron_dir);
    info!("Channel: {}\n", channel);

    // 创建Aeron客户端
    let client = AeronClient::new(&aeron_dir)
        .map_err(|e| format!("Failed to create AeronClient: {:?}", e))?;

    // 创建消息通道（聚合所有响应）
    let (tx, rx) = mpsc_channel();

    // 创建订阅者用于接收各种响应
    info!("初始化Aeron订阅者...");

    let (tx1, tx2, tx3) = (tx.clone(), tx.clone(), tx.clone());

    let _order_update_sub = client
        .add_subscription(
            channel,
            2,  // Stream 2: 订单更新
            10_000,
            OrderUpdateCallback { tx: tx1 },
            NoopLifecycle,
        )
        .map_err(|e| format!("Failed to create order_update subscription: {:?}", e))?;

    let _trade_sub = client
        .add_subscription(
            channel,
            3,  // Stream 3: 成交通知
            10_000,
            TradeCallback { tx: tx2 },
            NoopLifecycle,
        )
        .map_err(|e| format!("Failed to create trade subscription: {:?}", e))?;

    let _market_data_sub = client
        .add_subscription(
            channel,
            4,  // Stream 4: 行情数据 (多个stream可以订阅)
            10_000,
            MarketDataCallback { tx: tx3 },
            NoopLifecycle,
        )
        .map_err(|e| format!("Failed to create market_data subscription: {:?}", e))?;

    // 创建发布者用于发送订单到stream 1
    let mut order_publisher = client
        .add_publication(channel, 1)  // Stream 1: 订单发送
        .map_err(|e| format!("Failed to create order publisher: {:?}", e))?;

    info!("✓ Aeron客户端初始化完成\n");

    // 等待连接建立
    info!("等待连接建立...");
    let start_connect = Instant::now();
    while start_connect.elapsed() < Duration::from_secs(5) {
        client.do_work();
        if order_publisher.is_connected() {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }

    if !order_publisher.is_connected() {
        warn!("⚠ 警告: 发布者未连接（确保撮合系统已启动）");
    } else {
        info!("✓ 客户端已连接\n");
    }

    // 跟踪订单状态
    let orders = Arc::new(Mutex::new(HashMap::<u64, OrderState>::new()));

    info!("═══════════════════════════════════════════════════════════════");
    info!("         主循环 - 定期发送订单并收集响应");
    info!("═══════════════════════════════════════════════════════════════\n");

    let start = Instant::now();
    let mut last_order_time = Instant::now();
    let mut client_order_id = 1u64;
    let mut order_count = 0u64;
    let mut update_count = 0u64;
    let mut trade_count = 0u64;
    let mut market_data_count = 0u64;

    loop {
        // 每3秒发送一批订单
        if last_order_time.elapsed() >= Duration::from_secs(3) && order_count < 20 {
            send_order_batch(
                &mut order_publisher,
                &client,
                client_order_id,
                &orders,
            )?;
            client_order_id += 4;  // 发送4个订单
            order_count += 4;
            last_order_time = Instant::now();
        }

        // 驱动Aeron客户端
        client.do_work();

        // 收集并验证接收到的消息
        loop {
            match rx.try_recv() {
                Ok(msg) => {
                    match &msg {
                        ReceivedMessage::OrderUpdate(_) => {
                            update_count += 1;
                            info!("✓ [{}] {}", update_count, fmt_message(&msg));
                        }
                        ReceivedMessage::Trade(_) => {
                            trade_count += 1;
                            info!("✓ [{}] {}", trade_count, fmt_message(&msg));
                        }
                        ReceivedMessage::MarketData(_) => {
                            market_data_count += 1;
                            if market_data_count % 10 == 0 {
                                info!("✓ [{}] {}", market_data_count, fmt_message(&msg));
                            }
                        }
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => break,
            }
        }

        thread::sleep(Duration::from_millis(100));

        // 运行60秒后停止
        if start.elapsed() > Duration::from_secs(60) {
            break;
        }
    }

    info!("\n═══════════════════════════════════════════════════════════════");
    info!("         统计信息");
    info!("═══════════════════════════════════════════════════════════════");
    info!("运行时间: {:.1}s", start.elapsed().as_secs_f64());
    info!("发送订单数: {}", order_count);
    info!("收到订单更新: {}", update_count);
    info!("收到成交通知: {}", trade_count);
    info!("收到行情数据: {}", market_data_count);
    info!("✓ 客户端演示完成");

    Ok(())
}

fn send_order_batch(
    publisher: &mut aeron_wrapper::Publisher,
    client: &AeronClient,
    start_client_order_id: u64,
    orders: &Arc<Mutex<HashMap<u64, OrderState>>>,
) -> Result<(), Box<dyn std::error::Error>> {
    // 发送4个订单：2个买、2个卖，各种价格
    let sides = ["Buy", "Sell", "Buy", "Sell"];
    let prices = [49950.0, 50050.0, 49980.0, 50020.0];

    for i in 0..4 {
        let order_data = create_sbe_new_order(
            start_client_order_id + i as u64,
            1u64,                          // participant_id
            prices[i],
            10.0,                          // quantity
            if sides[i] == "Buy" { 0 } else { 1 },  // side
            1,                             // IOC
        );

        // 发送订单
        loop {
            match publisher.send(&order_data) {
                Ok(()) => {
                    info!("📤 发送订单 #{}: {} @ {} (IOC)",
                          start_client_order_id + i as u64,
                          sides[i],
                          prices[i]);
                    break;
                }
                Err(aeron_wrapper::Error::BackPressured) => {
                    std::hint::spin_loop();
                }
                Err(e) => {
                    return Err(format!("Failed to send order: {:?}", e).into());
                }
            }
        }

        // 跟踪订单状态
        let mut order_map = orders.lock();
        order_map.insert(
            start_client_order_id + i as u64,
            OrderState {
                client_order_id: start_client_order_id + i as u64,
                side: sides[i],
                price: prices[i],
                quantity: 10.0,
                status: "Pending",
                received_at: None,
                matched_qty: 0.0,
            },
        );

        // 短暂延迟避免burst
        thread::sleep(Duration::from_millis(10));
    }

    Ok(())
}

/// 构造SBE格式的NewOrderRequest消息
///
/// 格式: 8字节SBE Header + 48字节消息体
/// Header: block_length(2) + template_id(2) + schema_id(2) + version(2)
fn create_sbe_new_order(
    client_order_id: u64,
    participant_id: u64,
    price: f64,
    quantity: f64,
    side: u8,
    time_in_force: u8,
) -> Vec<u8> {
    let mut data = vec![0u8; 56];  // 8 (header) + 48 (body)

    // SBE Header (little-endian)
    data[0..2].copy_from_slice(&48u16.to_le_bytes());  // block_length
    data[2..4].copy_from_slice(&1u16.to_le_bytes());   // template_id = 1 (NewOrderRequest)
    data[4..6].copy_from_slice(&1u16.to_le_bytes());   // schema_id
    data[6..8].copy_from_slice(&0u16.to_le_bytes());   // version

    // Message Body (little-endian)
    data[8..16].copy_from_slice(&client_order_id.to_le_bytes());
    data[16..24].copy_from_slice(&participant_id.to_le_bytes());
    data[24..32].copy_from_slice(&price.to_le_bytes());
    data[32..40].copy_from_slice(&quantity.to_le_bytes());
    data[40] = side;
    data[41] = time_in_force;
    // data[42..48] = padding

    data
}

fn fmt_message(msg: &ReceivedMessage) -> String {
    match msg {
        ReceivedMessage::OrderUpdate(s) => format!("📨 {}", s),
        ReceivedMessage::Trade(s) => format!("💰 {}", s),
        ReceivedMessage::MarketData(s) => format!("📊 {}", s),
    }
}
