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
use std::sync::mpsc::{channel as mpsc_channel, Sender, Receiver, TryRecvError};
use parking_lot::Mutex;

// ============================================================================
// 入站订阅者 - 接收委托
// ============================================================================

struct OrderInboundCallback {
    tx: Sender<InboundMsg>,
}

impl PollCallback for OrderInboundCallback {
    fn on_data(&mut self, data: &[u8]) {
        use tracing::info;
        info!("[OrderInboundCallback] on_data() called with {} bytes", data.len());

        // 解析SBE消息：首8字节是header，后续是消息体
        if data.len() < 8 {
            info!("[OrderInboundCallback] Data too short");
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
                        let _ = self.tx.send(InboundMsg::NewOrder(req));
                    }
                }
            }
            2 => {
                // CancelOrderRequest: header(8) + body(8) = 16字节
                if data.len() >= 16 {
                    unsafe {
                        let req = std::ptr::read_unaligned(
                            &data[8] as *const u8 as *const CancelOrderRequest
                        );
                        let _ = self.tx.send(InboundMsg::CancelOrder(req));
                    }
                }
            }
            _ => {
                // 未知消息类型，忽略
            }
        }
    }
}

pub struct AeronOrderSubscriber {
    subscriber: Arc<Mutex<Box<aeron_wrapper::Subscriber<OrderInboundCallback>>>>,
    rx: Receiver<InboundMsg>,
    client: Arc<AeronClient>,
}

impl AeronOrderSubscriber {
    pub fn new(client: Arc<AeronClient>, channel: &str, stream_id: i32) -> Result<Self, String> {
        let (tx, rx) = mpsc_channel();

        let subscriber = client
            .add_subscription(
                channel,
                stream_id,
                10_000,
                OrderInboundCallback { tx },
                NoopLifecycle,
            )
            .map_err(|e| format!("Failed to add subscription: {:?}", e))?;

        // 无限等待直到subscription连接，遵循官方aeron-wrapper例子模式
        use tracing::info;
        info!("⏳ Waiting for order subscriber to connect on stream {}...", stream_id);
        let mut wait_count = 0u32;
        loop {
            client.do_work();
            if subscriber.is_connected() {
                info!("✅ Order subscriber connected (waited {} iterations)", wait_count);
                break;
            }
            wait_count += 1;
            if wait_count % 1000 == 0 {
                info!("⏳ Still waiting for order subscriber... ({} iterations)", wait_count);
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        Ok(Self {
            subscriber: Arc::new(Mutex::new(subscriber)),
            rx,
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
        // poll()会调用on_data()回调，回调发送消息到mpsc channel
        static POLL_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let poll_count = POLL_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        {
            let mut sub = self.subscriber.lock();
            let n = sub.poll();  // 触发回调，消息进入channel
            if poll_count % 100_000 == 0 {
                info!("[AeronOrderSubscriber] poll #{} returned {} fragments (is_connected={})",
                    poll_count, n, self.is_connected());
            }
        }  // 立即释放lock

        // 然后从channel读取回调发送的消息
        match self.rx.try_recv() {
            Ok(msg) => {
                info!("[AeronOrderSubscriber] ✓ received message from channel");
                Some(msg)
            }
            Err(TryRecvError::Empty) => None,
            Err(TryRecvError::Disconnected) => None,
        }
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

        // Wait for publisher to be connected
        use tracing::info;
        info!("⏳ Waiting for order update publisher to connect on stream {}...", stream_id);
        let mut wait_count = 0u32;
        loop {
            client.do_work();
            if publisher.is_connected() {
                info!("✅ Order update publisher connected (waited {} iterations)", wait_count);
                break;
            }
            wait_count += 1;
            if wait_count % 1000 == 0 {
                info!("⏳ Still waiting for order update publisher... ({} iterations)", wait_count);
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }

        Ok(Self { client, publisher })
    }
}

impl OrderUpdatePublisher for AeronOrderUpdatePublisher {
    fn do_work(&mut self) {
        self.client.do_work();
    }

    fn publish(&mut self, msg: &OrderUpdateMsg) -> Result<(), TransportError> {
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

        // Wait for publisher to be connected
        use tracing::info;
        info!("⏳ Waiting for trade publisher to connect on stream {}...", stream_id);
        let mut wait_count = 0u32;
        loop {
            client.do_work();
            if publisher.is_connected() {
                info!("✅ Trade publisher connected (waited {} iterations)", wait_count);
                break;
            }
            wait_count += 1;
            if wait_count % 1000 == 0 {
                info!("⏳ Still waiting for trade publisher... ({} iterations)", wait_count);
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }

        Ok(Self { client, publisher })
    }
}

impl TradePublisher for AeronTradePublisher {
    fn do_work(&mut self) {
        self.client.do_work();
    }

    fn publish(&mut self, msg: &TradeNotification) -> Result<(), TransportError> {
        // Create SBE-encoded message: 8-byte header + 48-byte body = 56 bytes
        let mut data = vec![0u8; 56];

        // SBE Header (8 bytes)
        data[0..2].copy_from_slice(&48u16.to_le_bytes());   // block_length = 48 (message body size)
        data[2..4].copy_from_slice(&3u16.to_le_bytes());    // template_id = 3 (Trade)
        data[4..6].copy_from_slice(&1u16.to_le_bytes());    // schema_id = 1
        data[6..8].copy_from_slice(&0u16.to_le_bytes());    // version = 0

        // Copy TradeNotification directly into body
        let msg_bytes = unsafe {
            std::slice::from_raw_parts(msg as *const TradeNotification as *const u8, 48)
        };
        data[8..56].copy_from_slice(msg_bytes);

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

        // Wait for all publishers to be connected
        use tracing::info;
        info!("⏳ Waiting for market data publishers to connect (streams {}, {}, {})...",
              depth_stream_id, depth50_stream_id, level2_stream_id);
        let mut wait_count = 0u32;
        loop {
            client.do_work();
            if depth_publisher.is_connected() && depth50_publisher.is_connected() && level2_publisher.is_connected() {
                info!("✅ All market data publishers connected (waited {} iterations)", wait_count);
                break;
            }
            wait_count += 1;
            if wait_count % 1000 == 0 {
                info!("⏳ Still waiting for market data publishers... ({} iterations, depth={}, depth50={}, level2={})",
                      wait_count,
                      depth_publisher.is_connected(),
                      depth50_publisher.is_connected(),
                      level2_publisher.is_connected());
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_order_inbound_callback() {
        let (tx, _rx) = mpsc_channel();
        let _callback = OrderInboundCallback { tx };
        // Callback created successfully
    }
}
