/// 交易客户端演示 - 精心构造订单生成成交
///
/// 客户端策略：
/// 1. 发送配对的订单（买单 + 卖单）使其相互成交
/// 2. 解析SBE消息格式，提取实际数据
/// 3. 打印有意义的信息：价格、数量、成交ID等
/// 4. 显示行情数据（BBO、Depth、Level2）
/// 5. 显示成交和订单更新
///
/// Prerequisites:
/// - aeronmd: AERON_DIR=/tmp/aeron aeronmd &
/// - 撮合系统: cargo run --release --example aeron_integration_demo
///
/// 运行: AERON_DIR=/tmp/aeron cargo run --release --example trading_client

use std::sync::mpsc::{channel as mpsc_channel, Sender, TryRecvError};
use std::thread;
use std::time::{Duration, Instant};
use tracing::info;

use aeron_wrapper::{AeronClient, NoopLifecycle, PollCallback, Pub};

#[derive(Debug, Clone)]
struct RawMessage {
    stream_id: i32,
    template_id: u16,
    data: Vec<u8>,
}

struct EventCallback {
    tx: Sender<RawMessage>,
    stream_id: i32,
}

impl PollCallback for EventCallback {
    fn on_data(&mut self, data: &[u8]) {
        if data.len() >= 4 {
            // 读取SBE Header获取template_id
            let template_id = u16::from_le_bytes([data[2], data[3]]);

            let _ = self.tx.send(RawMessage {
                stream_id: self.stream_id,
                template_id,
                data: data.to_vec(),
            });
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let aeron_dir = std::env::var("AERON_DIR").unwrap_or_else(|_| "/tmp/aeron".to_string());
    let channel = "aeron:ipc";

    info!("╔═════════════════════════════════════════════════════════════════╗");
    info!("║         交易客户端 - 精心构造订单生成成交                         ║");
    info!("╚═════════════════════════════════════════════════════════════════╝");
    info!("");
    info!("Aeron Dir: {}", aeron_dir);
    info!("Channel: {}", channel);
    info!("");

    let client = AeronClient::new(&aeron_dir)
        .map_err(|e| format!("Failed to create AeronClient: {:?}", e))?;

    let (tx, rx) = mpsc_channel();

    // 创建订阅者（Stream 2-6）
    info!("初始化Aeron订阅者...");

    let (tx2, tx3, tx4, tx5, tx6) = (
        tx.clone(),
        tx.clone(),
        tx.clone(),
        tx.clone(),
        tx.clone(),
    );

    let _sub2 = client.add_subscription(
        channel, 2, 10_000,
        EventCallback { tx: tx2, stream_id: 2 },
        NoopLifecycle,
    )?;

    let _sub3 = client.add_subscription(
        channel, 3, 10_000,
        EventCallback { tx: tx3, stream_id: 3 },
        NoopLifecycle,
    )?;

    let _sub4 = client.add_subscription(
        channel, 4, 10_000,
        EventCallback { tx: tx4, stream_id: 4 },
        NoopLifecycle,
    )?;

    let _sub5 = client.add_subscription(
        channel, 5, 10_000,
        EventCallback { tx: tx5, stream_id: 5 },
        NoopLifecycle,
    )?;

    let _sub6 = client.add_subscription(
        channel, 6, 10_000,
        EventCallback { tx: tx6, stream_id: 6 },
        NoopLifecycle,
    )?;

    // 创建发布者
    let mut publisher = client.add_publication(channel, 1)?;

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

    info!("✓ 已连接");
    info!("");

    // 测试场景：定期发送配对订单以生成成交
    info!("╔═════════════════════════════════════════════════════════════════╗");
    info!("║ 发送配对订单以生成成交                                           ║");
    info!("╚═════════════════════════════════════════════════════════════════╝");
    info!("");

    let start = Instant::now();
    let mut round = 0;

    while start.elapsed() < Duration::from_secs(30) {
        // 每5秒发送一轮配对订单
        if round % 5 == 0 {
            round += 1;

            let base_price = 50000.0;
            let order_id = 1000 + (round as u64);

            info!("═══ 第 {} 轮成交 ═══", round / 5);

            // 发送买单 (GTC)
            send_order(
                &mut publisher,
                order_id,          // client_order_id
                1,                 // participant_id
                base_price + 10.0, // 买10个单位 @ 50010
                10.0,              // quantity
                0,                 // side = Buy
                0,                 // time_in_force = GTC
            )?;
            info!("📤 发送 GTC 买单: {} @ {}", 10.0, base_price + 10.0);

            thread::sleep(Duration::from_millis(100));

            // 发送卖单 (IOC) 与买单匹配
            send_order(
                &mut publisher,
                order_id + 1,      // client_order_id
                1,                 // participant_id
                base_price + 10.0, // 卖10个单位 @ 50010
                10.0,              // quantity
                1,                 // side = Sell
                1,                 // time_in_force = IOC
            )?;
            info!("📤 发送 IOC 卖单: {} @ {}", 10.0, base_price + 10.0);
        }

        // 驱动Aeron
        client.do_work();

        // 接收并解析消息
        while let Ok(msg) = rx.try_recv() {
            parse_and_print_message(&msg);
        }

        thread::sleep(Duration::from_millis(100));
        round += 1;
    }

    info!("");
    info!("═══════════════════════════════════════════════════════════════════");
    info!("客户端演示完成");
    info!("═══════════════════════════════════════════════════════════════════");

    Ok(())
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
    let data = create_sbe_order(
        client_order_id,
        participant_id,
        price,
        quantity,
        side,
        time_in_force,
    );

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
    data[0..2].copy_from_slice(&48u16.to_le_bytes());  // block_length
    data[2..4].copy_from_slice(&1u16.to_le_bytes());   // template_id = 1
    data[4..6].copy_from_slice(&1u16.to_le_bytes());   // schema_id
    data[6..8].copy_from_slice(&0u16.to_le_bytes());   // version

    // Message Body
    data[8..16].copy_from_slice(&client_order_id.to_le_bytes());
    data[16..24].copy_from_slice(&participant_id.to_le_bytes());
    data[24..32].copy_from_slice(&price.to_le_bytes());
    data[32..40].copy_from_slice(&quantity.to_le_bytes());
    data[40] = side;
    data[41] = time_in_force;

    data
}

fn parse_and_print_message(msg: &RawMessage) {
    match msg.stream_id {
        2 => parse_order_update(msg),
        3 => parse_trade(msg),
        4 => parse_depth_snapshot(msg),
        5 => parse_depth50_snapshot(msg),
        6 => parse_level2_snapshot(msg),
        _ => {}
    }
}

fn parse_order_update(msg: &RawMessage) {
    // OrderUpdateEvent: 64 bytes packed
    if msg.data.len() < 64 {
        return;
    }

    let data = &msg.data;

    // Parse fields (assuming specific layout from order_update.rs)
    let kind = data[0];
    let order_id = u64::from_le_bytes(data[8..16].try_into().unwrap_or([0; 8]));
    let client_order_id = u64::from_le_bytes(data[16..24].try_into().unwrap_or([0; 8]));
    let fill_price = f64::from_le_bytes(data[32..40].try_into().unwrap_or([0; 8]));
    let fill_qty = f64::from_le_bytes(data[40..48].try_into().unwrap_or([0; 8]));
    let remaining_qty = f64::from_le_bytes(data[48..56].try_into().unwrap_or([0; 8]));

    let status = match kind {
        1 => "✓ ACCEPTED",
        2 => "✓ FILLED",
        3 => "⟳ PARTIAL_FILL",
        4 => "✗ CANCELLED",
        5 => "✗ REJECTED",
        _ => "? UNKNOWN",
    };

    match kind {
        1 => info!("📨 [OrderUpdate] {} Order#{} (Client#{})", status, order_id, client_order_id),
        2 => info!("💯 [OrderUpdate] {} Order#{} @ {} qty={} (remaining: {})",
                   status, order_id, fill_price, fill_qty, remaining_qty),
        3 => info!("⟳ [OrderUpdate] {} Order#{} @ {} qty={} (remaining: {})",
                   status, order_id, fill_price, fill_qty, remaining_qty),
        _ => info!("📨 [OrderUpdate] {} Order#{}", status, order_id),
    }
}

fn parse_trade(msg: &RawMessage) {
    // TradeNotification: 56 bytes (8 header + 48 body)
    if msg.data.len() < 56 {
        return;
    }

    let data = &msg.data;

    // Parse Trade message body (offset 8 for SBE header)
    let sequence = u64::from_le_bytes(data[8..16].try_into().unwrap_or([0; 8]));
    let taker_order_id = u64::from_le_bytes(data[16..24].try_into().unwrap_or([0; 8]));
    let maker_order_id = u64::from_le_bytes(data[24..32].try_into().unwrap_or([0; 8]));
    let price = f64::from_le_bytes(data[32..40].try_into().unwrap_or([0; 8]));
    let quantity = f64::from_le_bytes(data[40..48].try_into().unwrap_or([0; 8]));
    let side = data[48];

    let side_str = if side == 0 { "Buy" } else { "Sell" };

    info!("💰 [Trade] Seq#{} {} Order#{} ↔ Order#{} @ {} qty={}",
          sequence, side_str, taker_order_id, maker_order_id, price, quantity);
}

fn parse_depth_snapshot(msg: &RawMessage) {
    // DepthSnapshot: 704 bytes
    if msg.data.len() < 704 {
        return;
    }

    let data = &msg.data;

    // Parse DepthSnapshot (from snapshot.rs)
    // timestamp (u64), sequence (u64), num_bids, num_asks, then price levels
    let timestamp = u64::from_le_bytes(data[0..8].try_into().unwrap_or([0; 8]));
    let sequence = u64::from_le_bytes(data[8..16].try_into().unwrap_or([0; 8]));
    let num_bids = u32::from_le_bytes(data[16..20].try_into().unwrap_or([0; 4])) as usize;
    let num_asks = u32::from_le_bytes(data[20..24].try_into().unwrap_or([0; 4])) as usize;

    if num_bids > 0 && num_asks > 0 {
        // 解析最优买卖价 (BBO)
        let bid_price_offset = 24;
        let bid_price = f64::from_le_bytes(
            data[bid_price_offset..bid_price_offset + 8].try_into().unwrap_or([0; 8])
        );
        let bid_qty_offset = bid_price_offset + 8;
        let bid_qty = f64::from_le_bytes(
            data[bid_qty_offset..bid_qty_offset + 8].try_into().unwrap_or([0; 8])
        );

        let ask_price_offset = 24 + (num_bids * 16);
        let ask_price = f64::from_le_bytes(
            data[ask_price_offset..ask_price_offset + 8].try_into().unwrap_or([0; 8])
        );
        let ask_qty_offset = ask_price_offset + 8;
        let ask_qty = f64::from_le_bytes(
            data[ask_qty_offset..ask_qty_offset + 8].try_into().unwrap_or([0; 8])
        );

        info!("📊 [Depth20] Seq#{} BBO: Bid {} @ {:.0} | Ask {} @ {:.0} | {} bids {} asks",
              sequence, bid_qty, bid_price, ask_qty, ask_price, num_bids, num_asks);
    }
}

fn parse_depth50_snapshot(msg: &RawMessage) {
    // Depth50Snapshot: 1728 bytes
    if msg.data.len() < 1728 {
        return;
    }

    let data = &msg.data;
    let sequence = u64::from_le_bytes(data[8..16].try_into().unwrap_or([0; 8]));
    let num_bids = u32::from_le_bytes(data[16..20].try_into().unwrap_or([0; 4])) as usize;
    let num_asks = u32::from_le_bytes(data[20..24].try_into().unwrap_or([0; 4])) as usize;

    info!("📊 [Depth50] Seq#{} {} bids {} asks", sequence, num_bids, num_asks);
}

fn parse_level2_snapshot(msg: &RawMessage) {
    // Level2Snapshot: 12928 bytes
    if msg.data.len() < 12928 {
        return;
    }

    let data = &msg.data;
    let sequence = u64::from_le_bytes(data[8..16].try_into().unwrap_or([0; 8]));
    let num_bids = u32::from_le_bytes(data[16..20].try_into().unwrap_or([0; 4])) as usize;
    let num_asks = u32::from_le_bytes(data[20..24].try_into().unwrap_or([0; 4])) as usize;

    info!("📊 [Level2] Seq#{} {} bids {} asks", sequence, num_bids, num_asks);
}
