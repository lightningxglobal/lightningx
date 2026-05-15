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
        // 解析SBE消息：首8字节是header，后续是消息体
        if data.len() < 8 {
            return;
        }

        // 读取template_id (offset 2, 2字节, little-endian)
        let template_id = u16::from_le_bytes([data[2], data[3]]);

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
    subscriber: Arc<Mutex<Option<Box<aeron_wrapper::Subscriber<OrderInboundCallback>>>>>,
    rx: Receiver<InboundMsg>,
    client: AeronClient,
}

impl AeronOrderSubscriber {
    pub fn new(aeron_dir: &str, channel: &str, stream_id: i32) -> Result<Self, String> {
        let client = AeronClient::new(aeron_dir)
            .map_err(|e| format!("Failed to create AeronClient: {:?}", e))?;

        let (tx, rx) = mpsc_channel();

        let mut subscriber = client
            .add_subscription(
                channel,
                stream_id,
                10_000,
                OrderInboundCallback { tx },
                NoopLifecycle,
            )
            .map_err(|e| format!("Failed to add subscription: {:?}", e))?;

        // 等待连接
        for _ in 0..100 {
            client.do_work();
            if subscriber.is_connected() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        Ok(Self {
            subscriber: Arc::new(Mutex::new(Some(subscriber))),
            rx,
            client,
        })
    }
}

impl OrderSubscriber for AeronOrderSubscriber {
    fn poll(&mut self) -> Option<InboundMsg> {
        // 驱动Aeron client
        self.client.do_work();

        // 轮询订阅者
        if let Some(ref mut sub) = *self.subscriber.lock() {
            let _n = sub.poll();
        }

        // 尝试接收消息
        match self.rx.try_recv() {
            Ok(msg) => Some(msg),
            Err(TryRecvError::Empty) => None,
            Err(TryRecvError::Disconnected) => None,
        }
    }

    fn is_connected(&self) -> bool {
        if let Some(ref sub) = *self.subscriber.lock() {
            sub.is_connected()
        } else {
            false
        }
    }
}

// ============================================================================
// 出站发布者 - 发送订单更新
// ============================================================================

pub struct AeronOrderUpdatePublisher {
    publisher: aeron_wrapper::Publisher,
}

impl AeronOrderUpdatePublisher {
    pub fn new(aeron_dir: &str, channel: &str, stream_id: i32) -> Result<Self, String> {
        let client = AeronClient::new(aeron_dir)
            .map_err(|e| format!("Failed to create AeronClient: {:?}", e))?;

        let publisher = client
            .add_publication(channel, stream_id)
            .map_err(|e| format!("Failed to add publication: {:?}", e))?;

        Ok(Self { publisher })
    }
}

impl OrderUpdatePublisher for AeronOrderUpdatePublisher {
    fn publish(&mut self, msg: &OrderUpdateMsg) -> Result<(), TransportError> {
        let data = unsafe {
            std::slice::from_raw_parts(msg as *const OrderUpdateMsg as *const u8, 64)
        };

        loop {
            match self.publisher.send(data) {
                Ok(()) => return Ok(()),
                Err(AeronError::BackPressured) => std::hint::spin_loop(),
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
    publisher: aeron_wrapper::Publisher,
}

impl AeronTradePublisher {
    pub fn new(aeron_dir: &str, channel: &str, stream_id: i32) -> Result<Self, String> {
        let client = AeronClient::new(aeron_dir)
            .map_err(|e| format!("Failed to create AeronClient: {:?}", e))?;

        let publisher = client
            .add_publication(channel, stream_id)
            .map_err(|e| format!("Failed to add publication: {:?}", e))?;

        Ok(Self { publisher })
    }
}

impl TradePublisher for AeronTradePublisher {
    fn publish(&mut self, msg: &TradeNotification) -> Result<(), TransportError> {
        let data = unsafe {
            std::slice::from_raw_parts(msg as *const TradeNotification as *const u8, 56)
        };

        loop {
            match self.publisher.send(data) {
                Ok(()) => return Ok(()),
                Err(AeronError::BackPressured) => std::hint::spin_loop(),
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
    depth_publisher: aeron_wrapper::Publisher,
    depth50_publisher: aeron_wrapper::Publisher,
    level2_publisher: aeron_wrapper::Publisher,
}

impl AeronMarketDataPublisher {
    pub fn new(
        aeron_dir: &str,
        channel: &str,
        depth_stream_id: i32,
        depth50_stream_id: i32,
        level2_stream_id: i32,
    ) -> Result<Self, String> {
        let client = AeronClient::new(aeron_dir)
            .map_err(|e| format!("Failed to create AeronClient: {:?}", e))?;

        let depth_publisher = client
            .add_publication(channel, depth_stream_id)
            .map_err(|e| format!("Failed to add depth publication: {:?}", e))?;

        let depth50_publisher = client
            .add_publication(channel, depth50_stream_id)
            .map_err(|e| format!("Failed to add depth50 publication: {:?}", e))?;

        let level2_publisher = client
            .add_publication(channel, level2_stream_id)
            .map_err(|e| format!("Failed to add level2 publication: {:?}", e))?;

        Ok(Self {
            depth_publisher,
            depth50_publisher,
            level2_publisher,
        })
    }
}

impl MarketDataPublisher for AeronMarketDataPublisher {
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
                Err(AeronError::BackPressured) => std::hint::spin_loop(),
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
                Err(AeronError::BackPressured) => std::hint::spin_loop(),
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
                Err(AeronError::BackPressured) => std::hint::spin_loop(),
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
