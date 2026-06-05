use crate::market_data::{Depth50SnapshotEvent, DepthSnapshotEvent, Level2SnapshotEvent};
/// 真实的Aeron Transport实现
///
/// 通过aeron-wrapper与Aeron C client集成
/// 支持四个独立的流：
/// - Stream 1: 入站委托 (NewOrder/CancelOrder)
/// - Stream 2: 出站订单更新 (OrderAccepted/Filled/etc)
/// - Stream 3: 出站成交 (TradeNotification)
/// - Stream 4: 出站行情 (DepthSnapshot/Depth50/Level2)
use crate::sbe::{
    self, CancelOrderRequest, NewOrderRequest, TradeNotification, TEMPLATE_ORDER_UPDATE,
    TEMPLATE_TRADE_NOTIFICATION,
};
use crate::transport::{
    InboundMsg, MarketDataPublisher, OrderSubscriber, OrderUpdateMsg, OrderUpdatePublisher,
    TradePublisher, TransportError,
};

use aeron_wrapper::{AeronClient, Error as AeronError, NoopLifecycle, PollCallback, Pub};
use parking_lot::Mutex;
use rtrb::{Consumer, Producer, RingBuffer};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const MAX_BACKPRESSURE_SPINS: u32 = 100_000;
// Bumped to 5 M each so even sustained 100 K msg/s × 50 s pile-ups don't
// drop. Memory cost: ORDER_INBOUND × 3 engines ≈ 1.2 GB, plus others ≈ 2.3 GB
// total — fine on a modern host, prevents the silent-drop failure mode
// where the engine inbound ring full triggers a recv-spin stall → engine
// ou_pub spin → conductor timeout → entire Aeron client death.
const ORDER_INBOUND_RING: usize = 5_000_000;
const ORDER_UPDATE_RING: usize = 5_000_000;
const TRADE_RING: usize = 5_000_000;
// Depth events are periodic (every 10-50 ms from the engine), not burst-driven.
// DeskDepthMsg enum is ~12.9 KB (dominated by Level2SnapshotEvent with 400 levels).
// 5M entries × 12.9 KB × 3 rings ≈ 193 GB — was causing 10 GB RSS.
// 1024 is plenty: at 100 snapshots/s the public recv drains faster than they arrive.
const DEPTH_RING: usize = 1_024;

/// Zero-copy publish: retry `try_claim` on back-pressure, then run the
/// writer directly into Aeron's term-log buffer (saves the staging-array
/// memcpy + the encoder→buffer memcpy that `publisher.send(&bytes)` does).
fn publish_with_retry_claim<F: FnOnce(&mut [u8])>(
    publisher: &mut aeron_wrapper::Publisher,
    length: usize,
    writer: F,
) -> Result<(), TransportError> {
    let mut spins = 0u32;
    let claim_result = loop {
        match publisher.try_claim(length) {
            Ok(claim) => break Ok(claim),
            Err(AeronError::BackPressured) => {
                spins += 1;
                if spins >= MAX_BACKPRESSURE_SPINS {
                    break Err(TransportError::BackPressured);
                }
                if spins % 1024 == 0 {
                    std::thread::yield_now();
                } else {
                    std::hint::spin_loop();
                }
            }
            Err(AeronError::NotConnected) => break Err(TransportError::Disconnected),
            Err(AeronError::Closed) => break Err(TransportError::Closed),
            Err(_) => break Err(TransportError::BackPressured),
        }
    };
    let mut claim = claim_result?;
    writer(claim.as_mut_slice());
    claim.commit().map_err(|_| TransportError::BackPressured)
}

// ============================================================================
// 入站订阅者 - 接收委托
// ============================================================================

struct OrderInboundCallback {
    tx: Producer<InboundMsg>,
    dropped: Arc<AtomicU64>,
}

impl PollCallback for OrderInboundCallback {
    fn on_data(&mut self, data: &[u8]) {
        // 解析SBE消息：首8字节是header，后续是消息体
        if data.len() < 8 {
            return;
        }

        // 读取template_id (offset 2, 2字节, little-endian)
        let template_id = u16::from_le_bytes([data[2], data[3]]);

        match template_id {
            1 => {
                // NewOrderRequest: header(8) + body(64) = 72字节
                if data.len() >= 72 {
                    unsafe {
                        let req = std::ptr::read_unaligned(
                            &data[8] as *const u8 as *const NewOrderRequest,
                        );
                        if let Err(rtrb::PushError::Full(_)) =
                            self.tx.push(InboundMsg::NewOrder(req))
                        {
                            self.dropped.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
            2 => {
                // CancelOrderRequest: header(8) + body(24) = 32 bytes
                if data.len() >= 32 {
                    unsafe {
                        let req = std::ptr::read_unaligned(
                            &data[8] as *const u8 as *const CancelOrderRequest,
                        );
                        if let Err(rtrb::PushError::Full(_)) =
                            self.tx.push(InboundMsg::CancelOrder(req))
                        {
                            self.dropped.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
            _ => {}
        }
    }
}

pub struct AeronOrderSubscriber {
    subscriber: Arc<Mutex<Box<aeron_wrapper::Subscriber<OrderInboundCallback>>>>,
    rx: Consumer<InboundMsg>,
    dropped: Arc<AtomicU64>,
    last_reported_dropped: u64,
    client: Arc<AeronClient>,
}

impl AeronOrderSubscriber {
    pub fn new(client: Arc<AeronClient>, channel: &str, stream_id: i32) -> Result<Self, String> {
        let (tx, rx) = RingBuffer::<InboundMsg>::new(ORDER_INBOUND_RING);
        let dropped = Arc::new(AtomicU64::new(0));

        let subscriber = client
            .add_subscription(
                channel,
                stream_id,
                10_000,
                OrderInboundCallback {
                    tx,
                    dropped: dropped.clone(),
                },
                NoopLifecycle,
            )
            .map_err(|e| format!("Failed to add subscription: {:?}", e))?;

        // 立即返回，不等待连接（官方pattern）
        // 连接管理在matching_thread中处理
        use tracing::info;
        info!(
            "✓ Order subscriber created on stream {} (will connect when client connects)",
            stream_id
        );

        Ok(Self {
            subscriber: Arc::new(Mutex::new(subscriber)),
            rx,
            dropped,
            last_reported_dropped: 0,
            client,
        })
    }

    pub fn dropped_messages(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

impl OrderSubscriber for AeronOrderSubscriber {
    fn do_work(&mut self) {
        self.client.do_work();
    }

    fn poll(&mut self) -> Option<InboundMsg> {
        // Aeron回调需要显式的poll()调用来触发
        // poll()会调用on_data()回调，回调写入SPSC ring。
        {
            let mut sub = self.subscriber.lock();
            let _ = sub.poll();
        } // 立即释放lock

        let current_dropped = self.dropped.load(Ordering::Relaxed);
        if current_dropped != self.last_reported_dropped {
            tracing::warn!(
                "Aeron order inbound ring full: {} messages dropped (total)",
                current_dropped
            );
            self.last_reported_dropped = current_dropped;
        }

        self.rx.pop().ok()
    }

    fn is_connected(&self) -> bool {
        self.subscriber.lock().is_connected()
    }
}

// ============================================================================
// 出站发布者 - 发送订单更新
// ============================================================================

pub struct AeronOrderUpdatePublisher {
    client: Arc<AeronClient>,
    publisher: aeron_wrapper::Publisher,
}

impl AeronOrderUpdatePublisher {
    pub fn new(client: Arc<AeronClient>, channel: &str, stream_id: i32) -> Result<Self, String> {
        let publisher = client
            .add_publication(channel, stream_id)
            .map_err(|e| format!("Failed to add publication: {:?}", e))?;

        // 立即返回，不等待连接（官方pattern）
        use tracing::info;
        info!("✓ Order update publisher created on stream {}", stream_id);

        Ok(Self { client, publisher })
    }
}

impl OrderUpdatePublisher for AeronOrderUpdatePublisher {
    fn do_work(&mut self) {
        self.client.do_work();
    }

    fn publish(&mut self, msg: &OrderUpdateMsg) -> Result<(), TransportError> {
        use tracing::info;
        let result = publish_with_retry_claim(&mut self.publisher, 72, |buf| {
            // Buffer is exactly 72 B (8-byte header + 64-byte body); the
            // encoder never errors here, so ignore the Result.
            let _ = sbe::encode_order_update(msg, buf);
        });
        if let Err(ref e) = result {
            info!("[AeronOrderUpdatePublisher] publish failed: {:?}", e);
        }
        result
    }
}

// ============================================================================
// 出站发布者 - 发送成交通知
// ============================================================================

pub struct AeronTradePublisher {
    client: Arc<AeronClient>,
    publisher: aeron_wrapper::Publisher,
}

impl AeronTradePublisher {
    pub fn new(client: Arc<AeronClient>, channel: &str, stream_id: i32) -> Result<Self, String> {
        let publisher = client
            .add_publication(channel, stream_id)
            .map_err(|e| format!("Failed to add publication: {:?}", e))?;

        // 立即返回，不等待连接（官方pattern）
        use tracing::info;
        info!("✓ Trade publisher created on stream {}", stream_id);

        Ok(Self { client, publisher })
    }
}

impl TradePublisher for AeronTradePublisher {
    fn do_work(&mut self) {
        self.client.do_work();
    }

    fn publish(&mut self, msg: &TradeNotification) -> Result<(), TransportError> {
        publish_with_retry_claim(&mut self.publisher, 72, |buf| {
            let _ = sbe::encode_trade_notification(msg, buf);
        })
    }
}

// ============================================================================
// 出站发布者 - 发送行情数据
// ============================================================================

pub struct AeronMarketDataPublisher {
    client: Arc<AeronClient>,
    depth_publisher: aeron_wrapper::Publisher,
    depth50_publisher: aeron_wrapper::Publisher,
    level2_publisher: aeron_wrapper::Publisher,
}

impl AeronMarketDataPublisher {
    pub fn new(
        client: Arc<AeronClient>,
        channel: &str,
        depth_stream_id: i32,
        depth50_stream_id: i32,
        level2_stream_id: i32,
    ) -> Result<Self, String> {
        let depth_publisher = client
            .add_publication(channel, depth_stream_id)
            .map_err(|e| format!("Failed to add depth publication: {:?}", e))?;

        let depth50_publisher = client
            .add_publication(channel, depth50_stream_id)
            .map_err(|e| format!("Failed to add depth50 publication: {:?}", e))?;

        let level2_publisher = client
            .add_publication(channel, level2_stream_id)
            .map_err(|e| format!("Failed to add level2 publication: {:?}", e))?;

        // 立即返回，不等待连接（官方pattern）
        use tracing::info;
        info!(
            "✓ Market data publishers created on streams {}, {}, {}",
            depth_stream_id, depth50_stream_id, level2_stream_id
        );

        Ok(Self {
            client,
            depth_publisher,
            depth50_publisher,
            level2_publisher,
        })
    }
}

impl MarketDataPublisher for AeronMarketDataPublisher {
    fn do_work(&mut self) {
        self.client.do_work();
    }

    fn publish_depth(&mut self, msg: &DepthSnapshotEvent) -> Result<(), TransportError> {
        let len = std::mem::size_of::<DepthSnapshotEvent>();
        publish_with_retry_claim(&mut self.depth_publisher, len, |buf| unsafe {
            std::ptr::copy_nonoverlapping(
                msg as *const DepthSnapshotEvent as *const u8,
                buf.as_mut_ptr(),
                len,
            );
        })
    }

    fn publish_depth50(&mut self, msg: &Depth50SnapshotEvent) -> Result<(), TransportError> {
        let len = std::mem::size_of::<Depth50SnapshotEvent>();
        publish_with_retry_claim(&mut self.depth50_publisher, len, |buf| unsafe {
            std::ptr::copy_nonoverlapping(
                msg as *const Depth50SnapshotEvent as *const u8,
                buf.as_mut_ptr(),
                len,
            );
        })
    }

    fn publish_level2(&mut self, msg: &Level2SnapshotEvent) -> Result<(), TransportError> {
        let len = std::mem::size_of::<Level2SnapshotEvent>();
        publish_with_retry_claim(&mut self.level2_publisher, len, |buf| unsafe {
            std::ptr::copy_nonoverlapping(
                msg as *const Level2SnapshotEvent as *const u8,
                buf.as_mut_ptr(),
                len,
            );
        })
    }
}

// ============================================================================
// 柜台侧 - 发送委托到引擎 (Stream 1)
// ============================================================================

pub struct DeskOrderPublisher {
    client: Arc<AeronClient>,
    publisher: aeron_wrapper::Publisher,
}

impl DeskOrderPublisher {
    pub fn new(client: Arc<AeronClient>, channel: &str, stream_id: i32) -> Result<Self, String> {
        let publisher = client
            .add_publication(channel, stream_id)
            .map_err(|e| format!("Failed to add desk order publication: {:?}", e))?;

        use tracing::info;
        info!("✓ DeskOrderPublisher created on stream {}", stream_id);

        Ok(Self { client, publisher })
    }

    pub fn do_work(&mut self) {
        self.client.do_work();
    }

    pub fn publish_new_order(
        &mut self,
        req: &crate::sbe::NewOrderRequest,
    ) -> Result<(), crate::transport::TransportError> {
        publish_with_retry_claim(&mut self.publisher, 72, |buf| {
            let _ = crate::sbe::encode_new_order(req, buf);
        })
    }

    pub fn publish_cancel(
        &mut self,
        req: &crate::sbe::CancelOrderRequest,
    ) -> Result<(), crate::transport::TransportError> {
        publish_with_retry_claim(&mut self.publisher, 32, |buf| {
            let _ = crate::sbe::encode_cancel_order(req, buf);
        })
    }
}

// ============================================================================
// 柜台侧 - 接收订单更新 (Stream 2)
// ============================================================================

struct OrderUpdateCallback {
    tx: Producer<crate::transport::OrderUpdateMsg>,
    dropped: Arc<AtomicU64>,
}

impl PollCallback for OrderUpdateCallback {
    fn on_data(&mut self, data: &[u8]) {
        // SBE: 8-byte header + 64-byte body = 72 bytes
        if data.len() < 72 {
            return;
        }
        let template_id = u16::from_le_bytes([data[2], data[3]]);
        if template_id == TEMPLATE_ORDER_UPDATE {
            if let Some(msg) = sbe::decode_order_update(data) {
                if let Err(rtrb::PushError::Full(_)) = self.tx.push(msg) {
                    self.dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
}

pub struct DeskOrderUpdateSubscriber {
    subscriber: Arc<Mutex<Box<aeron_wrapper::Subscriber<OrderUpdateCallback>>>>,
    rx: Consumer<crate::transport::OrderUpdateMsg>,
    dropped: Arc<AtomicU64>,
    last_reported_dropped: u64,
    client: Arc<AeronClient>,
}

impl DeskOrderUpdateSubscriber {
    pub fn new(client: Arc<AeronClient>, channel: &str, stream_id: i32) -> Result<Self, String> {
        let (tx, rx) = RingBuffer::<crate::transport::OrderUpdateMsg>::new(ORDER_UPDATE_RING);
        let dropped = Arc::new(AtomicU64::new(0));
        let subscriber = client
            .add_subscription(
                channel,
                stream_id,
                10_000,
                OrderUpdateCallback {
                    tx,
                    dropped: dropped.clone(),
                },
                NoopLifecycle,
            )
            .map_err(|e| format!("Failed to subscribe to order updates: {:?}", e))?;

        use tracing::info;
        info!(
            "✓ DeskOrderUpdateSubscriber created on stream {}",
            stream_id
        );

        Ok(Self {
            subscriber: Arc::new(Mutex::new(subscriber)),
            rx,
            dropped,
            last_reported_dropped: 0,
            client,
        })
    }

    pub fn do_work(&mut self) {
        self.client.do_work();
    }

    pub fn poll(&mut self) -> Option<crate::transport::OrderUpdateMsg> {
        {
            let mut sub = self.subscriber.lock();
            let _ = sub.poll();
        }
        let current_dropped = self.dropped.load(Ordering::Relaxed);
        if current_dropped != self.last_reported_dropped {
            tracing::warn!(
                "Aeron order-update ring full: {} messages dropped (total)",
                current_dropped
            );
            self.last_reported_dropped = current_dropped;
        }
        self.rx.pop().ok()
    }

    pub fn dropped_messages(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

// ============================================================================
// 柜台侧 - 接收成交通知 (Stream 3)
// ============================================================================

struct TradeNotificationCallback {
    tx: Producer<TradeNotification>,
    dropped: Arc<AtomicU64>,
}

impl PollCallback for TradeNotificationCallback {
    fn on_data(&mut self, data: &[u8]) {
        // SBE: 8-byte header + 64-byte body = 72 bytes
        if data.len() < 72 {
            return;
        }
        let template_id = u16::from_le_bytes([data[2], data[3]]);
        if template_id == TEMPLATE_TRADE_NOTIFICATION {
            unsafe {
                let msg =
                    std::ptr::read_unaligned(&data[8] as *const u8 as *const TradeNotification);
                if let Err(rtrb::PushError::Full(_)) = self.tx.push(msg) {
                    self.dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
}

pub struct DeskTradeSubscriber {
    subscriber: Arc<Mutex<Box<aeron_wrapper::Subscriber<TradeNotificationCallback>>>>,
    rx: Consumer<TradeNotification>,
    dropped: Arc<AtomicU64>,
    last_reported_dropped: u64,
    client: Arc<AeronClient>,
}

impl DeskTradeSubscriber {
    pub fn new(client: Arc<AeronClient>, channel: &str, stream_id: i32) -> Result<Self, String> {
        let (tx, rx) = RingBuffer::<TradeNotification>::new(TRADE_RING);
        let dropped = Arc::new(AtomicU64::new(0));
        let subscriber = client
            .add_subscription(
                channel,
                stream_id,
                10_000,
                TradeNotificationCallback {
                    tx,
                    dropped: dropped.clone(),
                },
                NoopLifecycle,
            )
            .map_err(|e| format!("Failed to subscribe to trades: {:?}", e))?;

        use tracing::info;
        info!("✓ DeskTradeSubscriber created on stream {}", stream_id);

        Ok(Self {
            subscriber: Arc::new(Mutex::new(subscriber)),
            rx,
            dropped,
            last_reported_dropped: 0,
            client,
        })
    }

    pub fn do_work(&mut self) {
        self.client.do_work();
    }

    pub fn poll(&mut self) -> Option<TradeNotification> {
        {
            let mut sub = self.subscriber.lock();
            let _ = sub.poll();
        }
        let current_dropped = self.dropped.load(Ordering::Relaxed);
        if current_dropped != self.last_reported_dropped {
            tracing::warn!(
                "Aeron trade ring full: {} messages dropped (total)",
                current_dropped
            );
            self.last_reported_dropped = current_dropped;
        }
        self.rx.pop().ok()
    }

    pub fn dropped_messages(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

// ============================================================================
// 柜台侧 - 接收深度行情 (Streams 4, 5, 6)
// ============================================================================

#[derive(Clone, Copy)]
pub enum DeskDepthMsg {
    Depth(DepthSnapshotEvent),
    Depth50(Depth50SnapshotEvent),
    Level2(Level2SnapshotEvent),
}

struct DepthCallback {
    tx: Producer<DeskDepthMsg>,
    dropped: Arc<AtomicU64>,
}

impl PollCallback for DepthCallback {
    fn on_data(&mut self, data: &[u8]) {
        let depth_size = std::mem::size_of::<DepthSnapshotEvent>();
        let depth50_size = std::mem::size_of::<Depth50SnapshotEvent>();
        let level2_size = std::mem::size_of::<Level2SnapshotEvent>();

        let msg = if data.len() >= depth_size && data.len() < depth50_size {
            unsafe {
                let evt = std::ptr::read_unaligned(data.as_ptr() as *const DepthSnapshotEvent);
                Some(DeskDepthMsg::Depth(evt))
            }
        } else if data.len() >= depth50_size && data.len() < level2_size {
            unsafe {
                let evt = std::ptr::read_unaligned(data.as_ptr() as *const Depth50SnapshotEvent);
                Some(DeskDepthMsg::Depth50(evt))
            }
        } else if data.len() >= level2_size {
            unsafe {
                let evt = std::ptr::read_unaligned(data.as_ptr() as *const Level2SnapshotEvent);
                Some(DeskDepthMsg::Level2(evt))
            }
        } else {
            None
        };

        if let Some(m) = msg {
            if let Err(rtrb::PushError::Full(_)) = self.tx.push(m) {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

pub struct DeskDepthSubscriber {
    depth_sub: Arc<Mutex<Box<aeron_wrapper::Subscriber<DepthCallback>>>>,
    depth50_sub: Arc<Mutex<Box<aeron_wrapper::Subscriber<DepthCallback>>>>,
    level2_sub: Arc<Mutex<Box<aeron_wrapper::Subscriber<DepthCallback>>>>,
    depth_rx: Consumer<DeskDepthMsg>,
    depth50_rx: Consumer<DeskDepthMsg>,
    level2_rx: Consumer<DeskDepthMsg>,
    dropped: Arc<AtomicU64>,
    client: Arc<AeronClient>,
    poll_counter: u64,
}

impl DeskDepthSubscriber {
    pub fn new(
        client: Arc<AeronClient>,
        channel: &str,
        depth_stream: i32,
        depth50_stream: i32,
        level2_stream: i32,
    ) -> Result<Self, String> {
        let (depth_tx, depth_rx) = RingBuffer::<DeskDepthMsg>::new(DEPTH_RING);
        let (depth50_tx, depth50_rx) = RingBuffer::<DeskDepthMsg>::new(DEPTH_RING);
        let (level2_tx, level2_rx) = RingBuffer::<DeskDepthMsg>::new(DEPTH_RING);
        let dropped = Arc::new(AtomicU64::new(0));

        let depth_sub = client
            .add_subscription(
                channel,
                depth_stream,
                10_000,
                DepthCallback {
                    tx: depth_tx,
                    dropped: dropped.clone(),
                },
                NoopLifecycle,
            )
            .map_err(|e| format!("Failed to subscribe to depth: {:?}", e))?;

        let depth50_sub = client
            .add_subscription(
                channel,
                depth50_stream,
                10_000,
                DepthCallback {
                    tx: depth50_tx,
                    dropped: dropped.clone(),
                },
                NoopLifecycle,
            )
            .map_err(|e| format!("Failed to subscribe to depth50: {:?}", e))?;

        let level2_sub = client
            .add_subscription(
                channel,
                level2_stream,
                10_000,
                DepthCallback {
                    tx: level2_tx,
                    dropped: dropped.clone(),
                },
                NoopLifecycle,
            )
            .map_err(|e| format!("Failed to subscribe to level2: {:?}", e))?;

        use tracing::info;
        info!(
            "✓ DeskDepthSubscriber created on streams {}, {}, {}",
            depth_stream, depth50_stream, level2_stream
        );

        Ok(Self {
            depth_sub: Arc::new(Mutex::new(depth_sub)),
            depth50_sub: Arc::new(Mutex::new(depth50_sub)),
            level2_sub: Arc::new(Mutex::new(level2_sub)),
            depth_rx,
            depth50_rx,
            level2_rx,
            dropped,
            client,
            poll_counter: 0,
        })
    }

    pub fn do_work(&mut self) {
        self.client.do_work();
    }

    pub fn poll(&mut self) -> Option<DeskDepthMsg> {
        {
            let mut s = self.depth_sub.lock();
            let _ = s.poll();
        }
        {
            let mut s = self.depth50_sub.lock();
            let _ = s.poll();
        }
        {
            let mut s = self.level2_sub.lock();
            let _ = s.poll();
        }
        // Round-robin across the three rings so depth20 bursts cannot starve depth50/level2.
        let r = self.poll_counter % 3;
        self.poll_counter = self.poll_counter.wrapping_add(1);
        match r {
            0 => self.depth_rx.pop()
                    .or_else(|_| self.depth50_rx.pop())
                    .or_else(|_| self.level2_rx.pop()),
            1 => self.depth50_rx.pop()
                    .or_else(|_| self.level2_rx.pop())
                    .or_else(|_| self.depth_rx.pop()),
            _ => self.level2_rx.pop()
                    .or_else(|_| self.depth_rx.pop())
                    .or_else(|_| self.depth50_rx.pop()),
        }
        .ok()
    }

    pub fn dropped_messages(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

// ============================================================================
// PersistEvent — desk-server → redis-writer / pg-writer
// ============================================================================

use crate::transport::persist_event::{PersistFrame, FRAME_SIZE as PERSIST_FRAME_SIZE};

const PERSIST_RING: usize = 16 * 1024;

pub struct PersistPublisher {
    _client: Arc<AeronClient>,
    publisher: aeron_wrapper::Publisher,
}

impl PersistPublisher {
    pub fn new(client: Arc<AeronClient>, channel: &str, stream_id: i32) -> Result<Self, String> {
        let publisher = client
            .add_publication(channel, stream_id)
            .map_err(|e| format!("Failed to add persist publication: {:?}", e))?;
        tracing::info!(
            "✓ PersistPublisher created on stream {} ({} bytes/frame)",
            stream_id,
            PERSIST_FRAME_SIZE,
        );
        Ok(Self {
            _client: client,
            publisher,
        })
    }

    pub fn publish(&mut self, frame: &PersistFrame) -> Result<(), TransportError> {
        let bytes = frame.as_bytes();
        publish_with_retry_claim(&mut self.publisher, bytes.len(), |buf| {
            buf.copy_from_slice(bytes);
        })
    }
}

struct PersistCallback {
    tx: Producer<PersistFrame>,
    dropped: Arc<AtomicU64>,
}

impl PollCallback for PersistCallback {
    fn on_data(&mut self, data: &[u8]) {
        let Some(frame) = PersistFrame::from_bytes(data) else {
            return;
        };
        if let Err(rtrb::PushError::Full(_)) = self.tx.push(frame) {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub struct PersistSubscriber {
    subscriber: Arc<Mutex<Box<aeron_wrapper::Subscriber<PersistCallback>>>>,
    rx: Consumer<PersistFrame>,
    dropped: Arc<AtomicU64>,
    last_reported_dropped: u64,
    client: Arc<AeronClient>,
}

impl PersistSubscriber {
    pub fn new(client: Arc<AeronClient>, channel: &str, stream_id: i32) -> Result<Self, String> {
        let (tx, rx) = RingBuffer::<PersistFrame>::new(PERSIST_RING);
        let dropped = Arc::new(AtomicU64::new(0));
        let subscriber = client
            .add_subscription(
                channel,
                stream_id,
                10_000,
                PersistCallback {
                    tx,
                    dropped: dropped.clone(),
                },
                NoopLifecycle,
            )
            .map_err(|e| format!("Failed to add persist subscription: {:?}", e))?;
        tracing::info!("✓ PersistSubscriber created on stream {}", stream_id);
        Ok(Self {
            subscriber: Arc::new(Mutex::new(subscriber)),
            rx,
            dropped,
            last_reported_dropped: 0,
            client,
        })
    }

    pub fn do_work(&self) {
        self.client.do_work();
    }

    /// Drive the underlying Aeron subscription and return one parsed frame
    /// if available. Caller is expected to spin-loop until `None`.
    pub fn poll(&mut self) -> Option<PersistFrame> {
        {
            let mut sub = self.subscriber.lock();
            let _ = sub.poll();
        }
        let dropped = self.dropped.load(Ordering::Relaxed);
        if dropped != self.last_reported_dropped {
            tracing::warn!(
                "PersistSubscriber ring full: {} frames dropped (total)",
                dropped
            );
            self.last_reported_dropped = dropped;
        }
        self.rx.pop().ok()
    }

    pub fn dropped_frames(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    pub fn is_connected(&self) -> bool {
        self.subscriber.lock().is_connected()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_inbound_callback_pushes_to_ring() {
        let (tx, mut rx) = RingBuffer::<InboundMsg>::new(4);
        let dropped = Arc::new(AtomicU64::new(0));
        let mut callback = OrderInboundCallback {
            tx,
            dropped: dropped.clone(),
        };

        let req = NewOrderRequest {
            client_order_id: 42,
            participant_id: 7,
            price_ticks: 10_125,  // 101.25 at tick=0.01
            quantity_lots: 3_500_000,  // 3.5 at step=1e-6
            side: 0,
            time_in_force: 0,
            response_stream_id: 200,
            _pad: [0; 10],
            symbol: *b"BTC_USDT\0\0\0\0\0\0\0\0",
        };
        let mut data = [0u8; 72];
        sbe::encode_new_order(&req, &mut data).unwrap();

        callback.on_data(&data);

        match rx.pop().unwrap() {
            InboundMsg::NewOrder(msg) => {
                let client_order_id = msg.client_order_id;
                let participant_id = msg.participant_id;
                assert_eq!(client_order_id, 42);
                assert_eq!(participant_id, 7);
            }
            InboundMsg::CancelOrder(_) => panic!("expected new order"),
        }
        assert_eq!(dropped.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn order_inbound_callback_counts_full_ring() {
        let (tx, mut rx) = RingBuffer::<InboundMsg>::new(1);
        let dropped = Arc::new(AtomicU64::new(0));
        let mut callback = OrderInboundCallback {
            tx,
            dropped: dropped.clone(),
        };

        let req = CancelOrderRequest {
            order_id: 9,
            participant_id: 42,
            response_stream_id: 200,
            _pad: [0; 4],
        };
        let mut data = [0u8; 32];
        sbe::encode_cancel_order(&req, &mut data).unwrap();

        callback.on_data(&data);
        callback.on_data(&data);

        assert!(matches!(rx.pop().unwrap(), InboundMsg::CancelOrder(_)));
        assert_eq!(dropped.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn order_update_callback_pushes_to_ring() {
        let (tx, mut rx) = RingBuffer::<crate::transport::OrderUpdateMsg>::new(4);
        let dropped = Arc::new(AtomicU64::new(0));
        let mut callback = OrderUpdateCallback {
            tx,
            dropped: dropped.clone(),
        };

        let msg = crate::transport::OrderUpdateMsg::accepted(11, 22, 33, 44);
        let mut data = [0u8; 72];
        sbe::encode_order_update(&msg, &mut data).unwrap();

        callback.on_data(&data);

        let got = rx.pop().unwrap();
        let order_id = got.order_id;
        let client_order_id = got.client_order_id;
        assert_eq!(order_id, 11);
        assert_eq!(client_order_id, 22);
        assert_eq!(dropped.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn trade_callback_pushes_to_ring() {
        let (tx, mut rx) = RingBuffer::<TradeNotification>::new(4);
        let dropped = Arc::new(AtomicU64::new(0));
        let mut callback = TradeNotificationCallback {
            tx,
            dropped: dropped.clone(),
        };

        let msg = TradeNotification {
            sequence: 1,
            taker_order_id: 2,
            maker_order_id: 3,
            price: 100.0,
            quantity: 0.5,
            side: 0,
            _pad: [0; 7],
            symbol: *b"BTC_USDT\0\0\0\0\0\0\0\0",
        };
        let mut data = [0u8; 72];
        sbe::encode_trade_notification(&msg, &mut data).unwrap();

        callback.on_data(&data);

        let got = rx.pop().unwrap();
        let sequence = got.sequence;
        let maker_order_id = got.maker_order_id;
        assert_eq!(sequence, 1);
        assert_eq!(maker_order_id, 3);
        assert_eq!(dropped.load(Ordering::Relaxed), 0);
    }
}
