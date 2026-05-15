/// 交易客户端演示 - 全面的订单操作和行情接收
///
/// 支持的订单类型：
/// 1. IOC (Immediate-or-Cancel): 立即成交或取消，不进入簿
/// 2. GTC (Good-Till-Cancel): 进入簿，持续等待成交，直到成交或手工撤单
/// 3. FOK (Fill-or-Kill): 全部成交或全部取消
/// 4. PostOnly: 只做市商，不能主动成交
///
/// 测试场景：
/// - 可成交的单: 买单 + 卖单配对产生成交
/// - GTC单: 进入簿，等待成交
/// - IOC单: 不进簿，立即返回
/// - 撤单: 从簿中移除GTC订单
/// - 新单: 各种价格、数量的订单
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
    info!("║         交易客户端 - 全面的订单类型和行情接收                     ║");
    info!("╚═════════════════════════════════════════════════════════════════╝");
    info!("");
    info!("支持的订单类型:");
    info!("  • IOC (Immediate-or-Cancel): 立即成交或取消");
    info!("  • GTC (Good-Till-Cancel): 进入簿等待成交");
    info!("  • FOK (Fill-or-Kill): 全部成交或全部取消");
    info!("  • PostOnly: 只做市商，不主动成交");
    info!("");
    info!("操作:");
    info!("  • 发送新订单 (NewOrder)");
    info!("  • 撤销订单 (CancelOrder)");
    info!("  • 配对成交 (买单 + 卖单)");
    info!("");

    let client = AeronClient::new(&aeron_dir)
        .map_err(|e| format!("Failed to create AeronClient: {:?}", e))?;

    let (tx, rx) = mpsc_channel();

    // 创建订阅者 (Stream 2-6)
    info!("初始化Aeron...");

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

    let mut publisher = client.add_publication(channel, 1)?;

    info!("✓ 已初始化");
    info!("");

    // 等待连接
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

    let start = Instant::now();
    let mut next_order_id = 1000u64;

    // 场景1: IOC单（立即返回，无成交）
    info!("═══════════════════════════════════════════════════════════════════");
    info!("场景1: IOC单 (立即成交或取消，不进入簿)");
    info!("═══════════════════════════════════════════════════════════════════");
    send_order(&mut publisher, next_order_id, 1, 45000.0, 10.0, 0, 1)?; // Buy IOC
    info!("📤 发送 IOC 买单: 10 @ 45000 (无匹配方，应立即返回)");
    next_order_id += 1;
    thread::sleep(Duration::from_millis(200));
    info!("");

    // 场景2: GTC单进簿
    info!("═══════════════════════════════════════════════════════════════════");
    info!("场景2: GTC单 (进入簿，等待成交)");
    info!("═══════════════════════════════════════════════════════════════════");
    send_order(&mut publisher, next_order_id, 1, 50000.0, 20.0, 0, 0)?; // Buy GTC
    info!("📤 发送 GTC 买单: 20 @ 50000 (进入簿)");
    let gtc_buy_id = next_order_id;
    next_order_id += 1;
    thread::sleep(Duration::from_millis(500));
    info!("");

    // 场景3: IOC卖单与GTC买单匹配 -> 产生成交
    info!("═══════════════════════════════════════════════════════════════════");
    info!("场景3: 配对成交 (IOC卖单 ↔ GTC买单)");
    info!("═══════════════════════════════════════════════════════════════════");
    send_order(&mut publisher, next_order_id, 1, 50000.0, 15.0, 1, 1)?; // Sell IOC
    info!("📤 发送 IOC 卖单: 15 @ 50000 (应与GTC买单配对)");
    info!("   期望成交: 15 @ 50000 (买单剩余: 5)");
    next_order_id += 1;
    thread::sleep(Duration::from_millis(500));
    info!("");

    // 场景4: 新的GTC卖单进簿
    info!("═══════════════════════════════════════════════════════════════════");
    info!("场景4: 新的GTC卖单 (进入簿)");
    info!("═══════════════════════════════════════════════════════════════════");
    send_order(&mut publisher, next_order_id, 1, 50100.0, 30.0, 1, 0)?; // Sell GTC
    info!("📤 发送 GTC 卖单: 30 @ 50100 (进入簿)");
    let gtc_sell_id = next_order_id;
    next_order_id += 1;
    thread::sleep(Duration::from_millis(300));
    info!("");

    // 场景5: FOK单（全部或取消）
    info!("═══════════════════════════════════════════════════════════════════");
    info!("场景5: FOK单 (Fill-or-Kill, 全部或取消)");
    info!("═══════════════════════════════════════════════════════════════════");
    send_order(&mut publisher, next_order_id, 1, 50100.0, 100.0, 0, 2)?; // Buy FOK
    info!("📤 发送 FOK 买单: 100 @ 50100 (簿中只有30，应全部取消)");
    next_order_id += 1;
    thread::sleep(Duration::from_millis(300));
    info!("");

    // 场景6: 撤单 (Cancel GTC订单)
    info!("═══════════════════════════════════════════════════════════════════");
    info!("场景6: 撤单 (取消之前的GTC卖单)");
    info!("═══════════════════════════════════════════════════════════════════");
    send_cancel_order(&mut publisher, gtc_sell_id)?;
    info!("📤 发送撤单请求: Order#{} (取消GTC卖单)", gtc_sell_id);
    thread::sleep(Duration::from_millis(300));
    info!("");

    // 场景7: 继续IOC成交
    info!("═══════════════════════════════════════════════════════════════════");
    info!("场景7: IOC卖单与剩余GTC买单成交");
    info!("═══════════════════════════════════════════════════════════════════");
    send_order(&mut publisher, next_order_id, 1, 50000.0, 5.0, 1, 1)?; // Sell IOC
    info!("📤 发送 IOC 卖单: 5 @ 50000 (与剩余GTC买单成交)");
    next_order_id += 1;
    thread::sleep(Duration::from_millis(300));
    info!("");

    // 场景8: PostOnly单（只做市商）
    info!("═══════════════════════════════════════════════════════════════════");
    info!("场景8: PostOnly单 (只做市商，不主动成交)");
    info!("═══════════════════════════════════════════════════════════════════");
    send_order(&mut publisher, next_order_id, 1, 49950.0, 25.0, 0, 3)?; // Buy PostOnly
    info!("📤 发送 PostOnly 买单: 25 @ 49950 (不与卖单成交，进入簿)");
    next_order_id += 1;
    thread::sleep(Duration::from_millis(300));
    info!("");

    // 场景9: 各种新订单
    info!("═══════════════════════════════════════════════════════════════════");
    info!("场景9: 各种新订单 (不同价格和数量)");
    info!("═══════════════════════════════════════════════════════════════════");

    let prices = [50050.0, 49900.0, 50200.0, 49800.0];
    let quantities = [5.0, 15.0, 8.0, 12.0];
    let sides = [1u8, 0, 1, 0];  // 卖、买、卖、买

    for i in 0..4 {
        let side_str = if sides[i] == 0 { "买" } else { "卖" };
        send_order(&mut publisher, next_order_id, 1, prices[i], quantities[i], sides[i], 0)?;
        info!("📤 发送新 GTC 订单#{}: {} {} @ {}", next_order_id, quantities[i], side_str, prices[i]);
        next_order_id += 1;
        thread::sleep(Duration::from_millis(100));
    }
    info!("");

    // 收集并显示所有响应
    info!("═══════════════════════════════════════════════════════════════════");
    info!("接收订单更新、成交通知和行情数据...");
    info!("═══════════════════════════════════════════════════════════════════");
    info!("");

    let mut stats = Stats::new();

    while start.elapsed() < Duration::from_secs(45) {
        client.do_work();

        while let Ok(msg) = rx.try_recv() {
            parse_and_print_message(&msg, &mut stats);
        }

        thread::sleep(Duration::from_millis(100));
    }

    info!("");
    info!("═══════════════════════════════════════════════════════════════════");
    info!("统计信息");
    info!("═══════════════════════════════════════════════════════════════════");
    info!("收到的OrderUpdate: {}", stats.order_updates);
    info!("收到的Trade: {}", stats.trades);
    info!("收到的Depth20: {}", stats.depth20);
    info!("收到的Depth50: {}", stats.depth50);
    info!("收到的Level2: {}", stats.level2);
    info!("");
    info!("✓ 客户端演示完成");

    Ok(())
}

struct Stats {
    order_updates: u64,
    trades: u64,
    depth20: u64,
    depth50: u64,
    level2: u64,
}

impl Stats {
    fn new() -> Self {
        Self {
            order_updates: 0,
            trades: 0,
            depth20: 0,
            depth50: 0,
            level2: 0,
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

fn send_cancel_order(
    publisher: &mut aeron_wrapper::Publisher,
    order_id: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut data = vec![0u8; 16];

    // SBE Header (8 bytes)
    data[0..2].copy_from_slice(&8u16.to_le_bytes());    // block_length
    data[2..4].copy_from_slice(&2u16.to_le_bytes());    // template_id = 2 (CancelOrder)
    data[4..6].copy_from_slice(&1u16.to_le_bytes());    // schema_id
    data[6..8].copy_from_slice(&0u16.to_le_bytes());    // version

    // Message Body (8 bytes)
    data[8..16].copy_from_slice(&order_id.to_le_bytes());

    loop {
        match publisher.send(&data) {
            Ok(()) => return Ok(()),
            Err(aeron_wrapper::Error::BackPressured) => {
                std::hint::spin_loop();
            }
            Err(e) => {
                return Err(format!("Failed to send cancel: {:?}", e).into());
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

    // SBE Header (8 bytes)
    data[0..2].copy_from_slice(&48u16.to_le_bytes());  // block_length
    data[2..4].copy_from_slice(&1u16.to_le_bytes());   // template_id = 1
    data[4..6].copy_from_slice(&1u16.to_le_bytes());   // schema_id
    data[6..8].copy_from_slice(&0u16.to_le_bytes());   // version

    // Message Body (48 bytes)
    data[8..16].copy_from_slice(&client_order_id.to_le_bytes());
    data[16..24].copy_from_slice(&participant_id.to_le_bytes());
    data[24..32].copy_from_slice(&price.to_le_bytes());
    data[32..40].copy_from_slice(&quantity.to_le_bytes());
    data[40] = side;
    data[41] = time_in_force;

    data
}

fn parse_and_print_message(msg: &RawMessage, stats: &mut Stats) {
    match msg.stream_id {
        2 => {
            parse_order_update(msg);
            stats.order_updates += 1;
        }
        3 => {
            parse_trade(msg);
            stats.trades += 1;
        }
        4 => {
            parse_depth_snapshot(msg);
            stats.depth20 += 1;
        }
        5 => {
            parse_depth50_snapshot(msg);
            stats.depth50 += 1;
        }
        6 => {
            parse_level2_snapshot(msg);
            stats.level2 += 1;
        }
        _ => {}
    }
}

fn parse_order_update(msg: &RawMessage) {
    if msg.data.len() < 64 {
        return;
    }

    let data = &msg.data;
    let kind = data[0];
    let order_id = u64::from_le_bytes(data[8..16].try_into().unwrap_or([0; 8]));
    let client_order_id = u64::from_le_bytes(data[16..24].try_into().unwrap_or([0; 8]));
    let fill_price = f64::from_le_bytes(data[32..40].try_into().unwrap_or([0; 8]));
    let fill_qty = f64::from_le_bytes(data[40..48].try_into().unwrap_or([0; 8]));
    let remaining_qty = f64::from_le_bytes(data[48..56].try_into().unwrap_or([0; 8]));

    match kind {
        1 => info!("📨 [ACCEPTED] Order#{} Client#{}", order_id, client_order_id),
        2 => info!("💯 [FILLED] Order#{} @ {} qty={}", order_id, fill_price, fill_qty),
        3 => info!("⟳ [PARTIAL_FILL] Order#{} @ {} qty={} remaining={}",
                   order_id, fill_price, fill_qty, remaining_qty),
        4 => info!("✗ [CANCELLED] Order#{} cancelled_qty={}", order_id, fill_qty),
        5 => info!("✗ [REJECTED] Client#{} rejected", client_order_id),
        _ => info!("📨 [UNKNOWN] Order#{}", order_id),
    }
}

fn parse_trade(msg: &RawMessage) {
    if msg.data.len() < 56 {
        return;
    }

    let data = &msg.data;
    let sequence = u64::from_le_bytes(data[8..16].try_into().unwrap_or([0; 8]));
    let taker_order_id = u64::from_le_bytes(data[16..24].try_into().unwrap_or([0; 8]));
    let maker_order_id = u64::from_le_bytes(data[24..32].try_into().unwrap_or([0; 8]));
    let price = f64::from_le_bytes(data[32..40].try_into().unwrap_or([0; 8]));
    let quantity = f64::from_le_bytes(data[40..48].try_into().unwrap_or([0; 8]));
    let side = data[48];

    let side_str = if side == 0 { "Buy" } else { "Sell" };
    info!("💰 [TRADE] Seq#{} {} Taker#{} × Maker#{} @ {} qty={}",
          sequence, side_str, taker_order_id, maker_order_id, price, quantity);
}

fn parse_depth_snapshot(msg: &RawMessage) {
    if msg.data.len() < 704 {
        return;
    }

    let data = &msg.data;
    let sequence = u64::from_le_bytes(data[8..16].try_into().unwrap_or([0; 8]));
    let num_bids = u32::from_le_bytes(data[16..20].try_into().unwrap_or([0; 4])) as usize;
    let num_asks = u32::from_le_bytes(data[20..24].try_into().unwrap_or([0; 4])) as usize;

    if num_bids > 0 && num_asks > 0 {
        let bid_price = f64::from_le_bytes(data[24..32].try_into().unwrap_or([0; 8]));
        let bid_qty = f64::from_le_bytes(data[32..40].try_into().unwrap_or([0; 8]));
        let ask_price = f64::from_le_bytes(data[24 + (num_bids * 16)..32 + (num_bids * 16)].try_into().unwrap_or([0; 8]));
        let ask_qty = f64::from_le_bytes(data[32 + (num_bids * 16)..40 + (num_bids * 16)].try_into().unwrap_or([0; 8]));

        info!("📊 [BBO] Bid {} @ {:.0} | Ask {} @ {:.0} | {} bids {} asks",
              bid_qty, bid_price, ask_qty, ask_price, num_bids, num_asks);
    }
}

fn parse_depth50_snapshot(msg: &RawMessage) {
    if msg.data.len() < 1728 {
        return;
    }

    let data = &msg.data;
    let sequence = u64::from_le_bytes(data[8..16].try_into().unwrap_or([0; 8]));
    let num_bids = u32::from_le_bytes(data[16..20].try_into().unwrap_or([0; 4])) as usize;
    let num_asks = u32::from_le_bytes(data[20..24].try_into().unwrap_or([0; 4])) as usize;

    info!("📊 [Depth50] Seq#{} {} levels on each side", sequence, std::cmp::min(num_bids, num_asks));
}

fn parse_level2_snapshot(msg: &RawMessage) {
    if msg.data.len() < 12928 {
        return;
    }

    let data = &msg.data;
    let sequence = u64::from_le_bytes(data[8..16].try_into().unwrap_or([0; 8]));
    let num_bids = u32::from_le_bytes(data[16..20].try_into().unwrap_or([0; 4])) as usize;
    let num_asks = u32::from_le_bytes(data[20..24].try_into().unwrap_or([0; 4])) as usize;

    info!("📊 [Level2] Seq#{} {} bids {} asks", sequence, num_bids, num_asks);
}
