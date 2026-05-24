/// 真实的Aeron Transport实现
///
/// 通过aeron-wrapper与Aeron C client集成
/// 支持四个独立的流：
/// - Stream 1: 入站委托 (NewOrder/CancelOrder)
/// - Stream 2: 出站订单更新 (OrderAccepted/Filled/etc)
/// - Stream 3: 出站成交 (TradeNotification)
/// - Stream 4: 出站行情 (DepthSnapshot/Depth50/Level2)

use crate::sbe::{NewOrderRequest, CancelOrderRequest, TradeNotification};
use crate::transport::{
    InboundMsg, OrderSubscriber, OrderUpdatePublisher, TradePublisher, MarketDataPublisher,
    OrderUpdateMsg, TransportError,
};
use crate::market_data::{DepthSnapshotEvent, Depth50SnapshotEvent, Level2SnapshotEvent};

use aeron_wrapper::{AeronClient, Error as AeronError, NoopLifecycle, Pub, PollCallback};
use std::sync::Arc;
use std::collections::VecDeque;
use parking_lot::Mutex;

// ============================================================================
// 入站订阅者 - 接收委托
// ============================================================================

struct OrderInboundCallback {
    message_queue: Arc<Mutex<VecDeque<InboundMsg>>>,
}

impl PollCallback for OrderInboundCallback {
    fn on_data(&mut self, data: &[u8]) {
        use tracing::info;
        info!("[OrderInboundCallback] on_data() called with {} bytes", data.len());

        // 解析SBE消息：首8字节是header，后续是消息体
        if data.len() < 8 {
            info!("[OrderInboundCallback] ❌ Data too short: {}", data.len());
            return;
        }

        // 读取template_id (offset 2, 2字节, little-endian)
        let template_id = u16::from_le_bytes([data[2], data[3]]);
        info!("[OrderInboundCallback] template_id={}", template_id);

        match template_id {
            1 => {
                // NewOrderRequest: header(8) + body(48) = 56字节
                if data.len() >= 56 {
                    unsafe {
                        let req = std::ptr::read_unaligned(
                            &data[8] as *const u8 as *const NewOrderRequest
                        );
                        // 复制字段到局部变量来规避alignment检查
                        let client_id = req.client_order_id;
                        let participant_id = req.participant_id;
                        let price = req.price;
                        let qty = req.quantity;
                        info!("[OrderInboundCallback] 📊 NewOrder parsed: client_id={}, participant_id={}, price={}, qty={}",
                              client_id, participant_id, price, qty);

                        // 存储到queue而不是覆盖
                        let mut queue = self.message_queue.lock();
                        queue.push_back(InboundMsg::NewOrder(req));
                        info!("[OrderInboundCallback] ✅ NewOrder enqueued (queue size={})", queue.len());
                    }
                } else {
                    info!("[OrderInboundCallback] ❌ NewOrderRequest too short: {} < 56", data.len());
                }
            }
            2 => {
                // CancelOrderRequest: header(8) + body(8) = 16字节
                if data.len() >= 16 {
                    unsafe {
                        let req = std::ptr::read_unaligned(
                            &data[8] as *const u8 as *const CancelOrderRequest
                        );
                        let mut queue = self.message_queue.lock();
                        queue.push_back(InboundMsg::CancelOrder(req));
                        info!("[OrderInboundCallback] ✅ CancelOrder enqueued (queue size={})", queue.len());
                    }
                } else {
                    info!("[OrderInboundCallback] ❌ CancelOrderRequest too short: {} < 16", data.len());
                }
            }
            _ => {
                info!("[OrderInboundCallback] ❌ Unknown template_id: {}", template_id);
            }
        }
    }
}

pub struct AeronOrderSubscriber {
    subscriber: Arc<Mutex<Box<aeron_wrapper::Subscriber<OrderInboundCallback>>>>,
    message_queue: Arc<Mutex<VecDeque<InboundMsg>>>,
    client: Arc<AeronClient>,
}

impl AeronOrderSubscriber {
    pub fn new(client: Arc<AeronClient>, channel: &str, stream_id: i32) -> Result<Self, String> {
        let message_queue = Arc::new(Mutex::new(VecDeque::new()));

        let subscriber = client
            .add_subscription(
                channel,
                stream_id,
                10_000,
                OrderInboundCallback { message_queue: message_queue.clone() },
                NoopLifecycle,
            )
            .map_err(|e| format!("Failed to add subscription: {:?}", e))?;

        // 立即返回，不等待连接（官方pattern）
        // 连接管理在matching_thread中处理
        use tracing::info;
        info!("✓ Order subscriber created on stream {} (will connect when client connects)", stream_id);

        Ok(Self {
            subscriber: Arc::new(Mutex::new(subscriber)),
            message_queue,
            client,
        })
    }
}

impl OrderSubscriber for AeronOrderSubscriber {
    fn do_work(&mut self) {
        self.client.do_work();
    }

    fn poll(&mut self) -> Option<InboundMsg> {
        use tracing::info;
        // Aeron回调需要显式的poll()调用来触发
        // poll()会调用on_data()回调，回调写入消息到Arc<Mutex<>>
        {
            let mut sub = self.subscriber.lock();
            let _ = sub.poll();  // 触发回调，消息进入VecDeque
        }  // 立即释放lock

        // 从队列中pop回调写入的消息
        let mut queue = self.message_queue.lock();
        let result = match queue.pop_front() {
            Some(msg) => {
                info!("[AeronOrderSubscriber] ✅ dequeued message (queue now {})", queue.len());
                Some(msg)
            }
            None => None,
        };

        result
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
        // Create SBE-encoded message: 8-byte header + 64-byte body = 72 bytes
        let mut data = vec![0u8; 72];

        // SBE Header (8 bytes)
        data[0..2].copy_from_slice(&56u16.to_le_bytes());   // block_length = 56 (message body size)
        data[2..4].copy_from_slice(&2u16.to_le_bytes());    // template_id = 2 (OrderUpdate)
        data[4..6].copy_from_slice(&1u16.to_le_bytes());    // schema_id = 1
        data[6..8].copy_from_slice(&0u16.to_le_bytes());    // version = 0

        // Copy OrderUpdateMsg directly into body (64 bytes total for the struct)
        let msg_bytes = unsafe {
            std::slice::from_raw_parts(msg as *const OrderUpdateMsg as *const u8, 64)
        };
        data[8..72].copy_from_slice(msg_bytes);

        let mut attempts = 0;
        loop {
            attempts += 1;
            match self.publisher.send(&data) {
                Ok(()) => return Ok(()),
                Err(AeronError::BackPressured) => {
                    if attempts == 1 {
                        info!("[AeronOrderUpdatePublisher] ⏳ BackPressured, retrying...");
                    }
                    std::hint::spin_loop();  // aeronmd会处理，只需让出CPU
                }
                Err(AeronError::NotConnected) => {
                    info!("[AeronOrderUpdatePublisher] ❌ NotConnected!");
                    return Err(TransportError::Disconnected);
                }
                Err(AeronError::Closed) => {
                    info!("[AeronOrderUpdatePublisher] ❌ Publisher Closed!");
                    return Err(TransportError::Closed);
                }
                Err(e) => {
                    info!("[AeronOrderUpdatePublisher] ❌ Error: {:?}", e);
                    return Err(TransportError::BackPressured);
                }
            }
        }
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
        // Create SBE-encoded message: 8-byte header + 64-byte body = 72 bytes
        let mut data = vec![0u8; 72];

        // SBE Header (8 bytes)
        data[0..2].copy_from_slice(&64u16.to_le_bytes());   // block_length = 64 (message body size)
        data[2..4].copy_from_slice(&20u16.to_le_bytes());   // template_id = 20 (TEMPLATE_TRADE_NOTIFICATION)
        data[4..6].copy_from_slice(&1u16.to_le_bytes());    // schema_id = 1
        data[6..8].copy_from_slice(&0u16.to_le_bytes());    // version = 0

        // Copy TradeNotification directly into body
        let msg_bytes = unsafe {
            std::slice::from_raw_parts(msg as *const TradeNotification as *const u8, 64)
        };
        data[8..72].copy_from_slice(msg_bytes);

        loop {
            match self.publisher.send(&data) {
                Ok(()) => return Ok(()),
                Err(AeronError::BackPressured) => {
                    std::hint::spin_loop();  // aeronmd会处理，只需让出CPU
                }
                Err(AeronError::NotConnected) => return Err(TransportError::Disconnected),
                Err(AeronError::Closed) => return Err(TransportError::Closed),
                Err(_) => return Err(TransportError::BackPressured),
            }
        }
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
        info!("✓ Market data publishers created on streams {}, {}, {}",
              depth_stream_id, depth50_stream_id, level2_stream_id);

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
        let data = unsafe {
            std::slice::from_raw_parts(
                msg as *const DepthSnapshotEvent as *const u8,
                std::mem::size_of::<DepthSnapshotEvent>(),
            )
        };

        loop {
            match self.depth_publisher.send(data) {
                Ok(()) => return Ok(()),
                Err(AeronError::BackPressured) => {
                    std::hint::spin_loop();  // aeronmd会处理，只需让出CPU
                }
                Err(AeronError::NotConnected) => return Err(TransportError::Disconnected),
                Err(AeronError::Closed) => return Err(TransportError::Closed),
                Err(_) => return Err(TransportError::BackPressured),
            }
        }
    }

    fn publish_depth50(&mut self, msg: &Depth50SnapshotEvent) -> Result<(), TransportError> {
        let data = unsafe {
            std::slice::from_raw_parts(
                msg as *const Depth50SnapshotEvent as *const u8,
                std::mem::size_of::<Depth50SnapshotEvent>(),
            )
        };

        loop {
            match self.depth50_publisher.send(data) {
                Ok(()) => return Ok(()),
                Err(AeronError::BackPressured) => {
                    std::hint::spin_loop();  // aeronmd会处理，只需让出CPU
                }
                Err(AeronError::NotConnected) => return Err(TransportError::Disconnected),
                Err(AeronError::Closed) => return Err(TransportError::Closed),
                Err(_) => return Err(TransportError::BackPressured),
            }
        }
    }

    fn publish_level2(&mut self, msg: &Level2SnapshotEvent) -> Result<(), TransportError> {
        let data = unsafe {
            std::slice::from_raw_parts(
                msg as *const Level2SnapshotEvent as *const u8,
                std::mem::size_of::<Level2SnapshotEvent>(),
            )
        };

        loop {
            match self.level2_publisher.send(data) {
                Ok(()) => return Ok(()),
                Err(AeronError::BackPressured) => {
                    std::hint::spin_loop();  // aeronmd会处理，只需让出CPU
                }
                Err(AeronError::NotConnected) => return Err(TransportError::Disconnected),
                Err(AeronError::Closed) => return Err(TransportError::Closed),
                Err(_) => return Err(TransportError::BackPressured),
            }
        }
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

    pub fn publish_new_order(&mut self, req: &crate::sbe::NewOrderRequest) -> Result<(), crate::transport::TransportError> {
        let mut data = [0u8; 56]; // 8 header + 48 body
        crate::sbe::encode_new_order(req, &mut data).map_err(|_| crate::transport::TransportError::BackPressured)?;

        loop {
            match self.publisher.send(&data) {
                Ok(()) => return Ok(()),
                Err(AeronError::BackPressured) => { std::hint::spin_loop(); }
                Err(AeronError::NotConnected) => return Err(crate::transport::TransportError::Disconnected),
                Err(AeronError::Closed) => return Err(crate::transport::TransportError::Closed),
                Err(_) => return Err(crate::transport::TransportError::BackPressured),
            }
        }
    }

    pub fn publish_cancel(&mut self, req: &crate::sbe::CancelOrderRequest) -> Result<(), crate::transport::TransportError> {
        let mut data = [0u8; 16]; // 8 header + 8 body
        crate::sbe::encode_cancel_order(req, &mut data).map_err(|_| crate::transport::TransportError::BackPressured)?;

        loop {
            match self.publisher.send(&data) {
                Ok(()) => return Ok(()),
                Err(AeronError::BackPressured) => { std::hint::spin_loop(); }
                Err(AeronError::NotConnected) => return Err(crate::transport::TransportError::Disconnected),
                Err(AeronError::Closed) => return Err(crate::transport::TransportError::Closed),
                Err(_) => return Err(crate::transport::TransportError::BackPressured),
            }
        }
    }
}

// ============================================================================
// 柜台侧 - 接收订单更新 (Stream 2)
// ============================================================================

struct OrderUpdateCallback {
    queue: Arc<Mutex<VecDeque<crate::transport::OrderUpdateMsg>>>,
}

impl PollCallback for OrderUpdateCallback {
    fn on_data(&mut self, data: &[u8]) {
        // SBE: 8-byte header + 64-byte body = 72 bytes
        if data.len() < 72 {
            return;
        }
        let template_id = u16::from_le_bytes([data[2], data[3]]);
        // template_id = 2 (unified OrderUpdate)
        if template_id == 2 {
            unsafe {
                let msg = std::ptr::read_unaligned(
                    &data[8] as *const u8 as *const crate::transport::OrderUpdateMsg,
                );
                let mut q = self.queue.lock();
                q.push_back(msg);
            }
        }
    }
}

pub struct DeskOrderUpdateSubscriber {
    subscriber: Arc<Mutex<Box<aeron_wrapper::Subscriber<OrderUpdateCallback>>>>,
    queue: Arc<Mutex<VecDeque<crate::transport::OrderUpdateMsg>>>,
    client: Arc<AeronClient>,
}

impl DeskOrderUpdateSubscriber {
    pub fn new(client: Arc<AeronClient>, channel: &str, stream_id: i32) -> Result<Self, String> {
        let queue = Arc::new(Mutex::new(VecDeque::new()));
        let subscriber = client
            .add_subscription(
                channel,
                stream_id,
                10_000,
                OrderUpdateCallback { queue: queue.clone() },
                NoopLifecycle,
            )
            .map_err(|e| format!("Failed to subscribe to order updates: {:?}", e))?;

        use tracing::info;
        info!("✓ DeskOrderUpdateSubscriber created on stream {}", stream_id);

        Ok(Self {
            subscriber: Arc::new(Mutex::new(subscriber)),
            queue,
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
        self.queue.lock().pop_front()
    }
}

// ============================================================================
// 柜台侧 - 接收成交通知 (Stream 3)
// ============================================================================

struct TradeNotificationCallback {
    queue: Arc<Mutex<VecDeque<TradeNotification>>>,
}

impl PollCallback for TradeNotificationCallback {
    fn on_data(&mut self, data: &[u8]) {
        // SBE: 8-byte header + 64-byte body = 72 bytes
        if data.len() < 72 {
            return;
        }
        let template_id = u16::from_le_bytes([data[2], data[3]]);
        if template_id == 20 {
            unsafe {
                let msg = std::ptr::read_unaligned(
                    &data[8] as *const u8 as *const TradeNotification,
                );
                self.queue.lock().push_back(msg);
            }
        }
    }
}

pub struct DeskTradeSubscriber {
    subscriber: Arc<Mutex<Box<aeron_wrapper::Subscriber<TradeNotificationCallback>>>>,
    queue: Arc<Mutex<VecDeque<TradeNotification>>>,
    client: Arc<AeronClient>,
}

impl DeskTradeSubscriber {
    pub fn new(client: Arc<AeronClient>, channel: &str, stream_id: i32) -> Result<Self, String> {
        let queue = Arc::new(Mutex::new(VecDeque::new()));
        let subscriber = client
            .add_subscription(
                channel,
                stream_id,
                10_000,
                TradeNotificationCallback { queue: queue.clone() },
                NoopLifecycle,
            )
            .map_err(|e| format!("Failed to subscribe to trades: {:?}", e))?;

        use tracing::info;
        info!("✓ DeskTradeSubscriber created on stream {}", stream_id);

        Ok(Self {
            subscriber: Arc::new(Mutex::new(subscriber)),
            queue,
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
        self.queue.lock().pop_front()
    }
}

// ============================================================================
// 柜台侧 - 接收深度行情 (Streams 4, 5, 6)
// ============================================================================

pub enum DeskDepthMsg {
    Depth(DepthSnapshotEvent),
    Depth50(Depth50SnapshotEvent),
    Level2(Level2SnapshotEvent),
}

struct DepthCallback {
    queue: Arc<Mutex<VecDeque<DeskDepthMsg>>>,
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
            self.queue.lock().push_back(m);
        }
    }
}

pub struct DeskDepthSubscriber {
    depth_sub: Arc<Mutex<Box<aeron_wrapper::Subscriber<DepthCallback>>>>,
    depth50_sub: Arc<Mutex<Box<aeron_wrapper::Subscriber<DepthCallback>>>>,
    level2_sub: Arc<Mutex<Box<aeron_wrapper::Subscriber<DepthCallback>>>>,
    queue: Arc<Mutex<VecDeque<DeskDepthMsg>>>,
    client: Arc<AeronClient>,
}

impl DeskDepthSubscriber {
    pub fn new(
        client: Arc<AeronClient>,
        channel: &str,
        depth_stream: i32,
        depth50_stream: i32,
        level2_stream: i32,
    ) -> Result<Self, String> {
        let queue = Arc::new(Mutex::new(VecDeque::new()));

        let depth_sub = client
            .add_subscription(channel, depth_stream, 10_000,
                DepthCallback { queue: queue.clone() }, NoopLifecycle)
            .map_err(|e| format!("Failed to subscribe to depth: {:?}", e))?;

        let depth50_sub = client
            .add_subscription(channel, depth50_stream, 10_000,
                DepthCallback { queue: queue.clone() }, NoopLifecycle)
            .map_err(|e| format!("Failed to subscribe to depth50: {:?}", e))?;

        let level2_sub = client
            .add_subscription(channel, level2_stream, 10_000,
                DepthCallback { queue: queue.clone() }, NoopLifecycle)
            .map_err(|e| format!("Failed to subscribe to level2: {:?}", e))?;

        use tracing::info;
        info!("✓ DeskDepthSubscriber created on streams {}, {}, {}",
              depth_stream, depth50_stream, level2_stream);

        Ok(Self {
            depth_sub: Arc::new(Mutex::new(depth_sub)),
            depth50_sub: Arc::new(Mutex::new(depth50_sub)),
            level2_sub: Arc::new(Mutex::new(level2_sub)),
            queue,
            client,
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
        self.queue.lock().pop_front()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_order_inbound_callback() {
        let message_queue = Arc::new(Mutex::new(VecDeque::new()));
        let _callback = OrderInboundCallback { message_queue };
        // Callback created successfully
    }
}
