pub mod aeron_channels;
pub mod aeron_transport;
pub mod order_update;
pub mod persist_event;
pub mod sbe;
pub mod tracer;
pub mod ws_sbe;

/// Transport层抽象 - Aeron的trait定义 + Mock实现
///
/// 用于屏蔽实际Aeron实现，通过trait定义来实现解耦和测试隔离
pub use self::sbe::{CancelOrderRequest, NewOrderRequest, TradeNotification};
use crate::market_data::{Depth50SnapshotEvent, DepthSnapshotEvent, Level2SnapshotEvent};
use std::sync::mpsc::{channel, Receiver, Sender};

// ============================================================================
// WS fast-path: Aeron command sent from async WS handler to Aeron spin thread
// ============================================================================

/// A command queued by the WS/REST handler for the Aeron spin thread to publish.
pub enum AeronCmd {
    NewOrder(NewOrderRequest),
    Cancel(CancelOrderRequest),
    /// Batch-cancel: all requests published in one tight loop inside the spin thread.
    BatchCancel(smallvec::SmallVec<[CancelOrderRequest; 64]>),
    /// Batch-new-order: all requests published in one tight loop so the engine
    /// processes the entire batch before the next depth snapshot fires (10ms).
    BatchNewOrder(smallvec::SmallVec<[NewOrderRequest; 32]>),
}

/// Order metadata stored by the WS handler so the Aeron event loop can
/// create the DB row and freeze funds after the engine confirms the order.
pub struct OrderMeta {
    pub user_id: i64,
    pub symbol: [u8; 16],
    pub side: u8,
    pub order_type: [u8; 16],
    pub price: Option<f64>,
    pub qty: f64,
    pub client_order_id: String,
    pub freeze_price: f64,
    pub initial_margin_cents: i64,
}

#[inline]
pub fn pack_str16(value: &str) -> [u8; 16] {
    let mut out = [0u8; 16];
    let bytes = value.as_bytes();
    let n = bytes.len().min(16);
    out[..n].copy_from_slice(&bytes[..n]);
    out
}

#[inline]
pub fn unpack_str16(value: &[u8; 16]) -> Option<&str> {
    let end = value.iter().position(|&b| b == 0).unwrap_or(16);
    std::str::from_utf8(&value[..end]).ok()
}

#[derive(Debug)]
pub enum TransportError {
    Disconnected,
    BackPressured,
    Closed,
}

#[derive(Debug)]
pub enum InboundMsg {
    NewOrder(NewOrderRequest),
    CancelOrder(CancelOrderRequest),
}

pub enum MarketDataMsg {
    DepthSnapshot(DepthSnapshotEvent),
    Depth50Snapshot(Depth50SnapshotEvent),
    Level2Snapshot(Level2SnapshotEvent),
}

// ============================================================================
// Outbound order update message
// ============================================================================

#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct OrderUpdateMsg {
    pub kind: u8, // ACCEPTED=1, FILLED=2, PARTIAL=3, CANCELLED=4, REJECTED=5
    pub reject_reason: u8,
    pub _pad1: [u8; 6],
    pub order_id: u64,
    pub client_order_id: u64,
    pub participant_id: u64,
    pub fill_price: f64,
    pub fill_qty: f64,
    pub remaining_qty: f64,
    pub timestamp: u64,
}

static_assertions::const_assert_eq!(std::mem::size_of::<OrderUpdateMsg>(), 64);

pub mod order_update_kind {
    pub const ACCEPTED: u8 = 1;
    pub const FILLED: u8 = 2;
    pub const PARTIAL_FILL: u8 = 3;
    pub const CANCELLED: u8 = 4;
    pub const REJECTED: u8 = 5;
}

impl OrderUpdateMsg {
    pub fn accepted(order_id: u64, client_order_id: u64, participant_id: u64, ts: u64) -> Self {
        Self {
            kind: order_update_kind::ACCEPTED,
            reject_reason: 0,
            _pad1: [0; 6],
            order_id,
            client_order_id,
            participant_id,
            fill_price: 0.0,
            fill_qty: 0.0,
            remaining_qty: 0.0,
            timestamp: ts,
        }
    }

    pub fn filled(
        order_id: u64,
        client_order_id: u64,
        participant_id: u64,
        price: f64,
        qty: f64,
        ts: u64,
    ) -> Self {
        Self {
            kind: order_update_kind::FILLED,
            reject_reason: 0,
            _pad1: [0; 6],
            order_id,
            client_order_id,
            participant_id,
            fill_price: price,
            fill_qty: qty,
            remaining_qty: 0.0,
            timestamp: ts,
        }
    }

    pub fn partial_fill(
        order_id: u64,
        client_order_id: u64,
        participant_id: u64,
        price: f64,
        filled: f64,
        remaining: f64,
        ts: u64,
    ) -> Self {
        Self {
            kind: order_update_kind::PARTIAL_FILL,
            reject_reason: 0,
            _pad1: [0; 6],
            order_id,
            client_order_id,
            participant_id,
            fill_price: price,
            fill_qty: filled,
            remaining_qty: remaining,
            timestamp: ts,
        }
    }

    pub fn cancelled(
        order_id: u64,
        client_order_id: u64,
        participant_id: u64,
        cancelled_qty: f64,
        ts: u64,
    ) -> Self {
        Self {
            kind: order_update_kind::CANCELLED,
            reject_reason: 0,
            _pad1: [0; 6],
            order_id,
            client_order_id,
            participant_id,
            fill_price: 0.0,
            fill_qty: cancelled_qty,
            remaining_qty: 0.0,
            timestamp: ts,
        }
    }

    pub fn rejected(client_order_id: u64, participant_id: u64, reason: u8, ts: u64) -> Self {
        Self {
            kind: order_update_kind::REJECTED,
            reject_reason: reason,
            _pad1: [0; 6],
            order_id: 0,
            client_order_id,
            participant_id,
            fill_price: 0.0,
            fill_qty: 0.0,
            remaining_qty: 0.0,
            timestamp: ts,
        }
    }
}

// ============================================================================
// Publisher & Subscriber Traits
// ============================================================================

pub trait OrderSubscriber: Send {
    fn poll(&mut self) -> Option<InboundMsg>;
    fn is_connected(&self) -> bool;
    /// Drive the Aeron conductor (heartbeat, connection state, flow control).
    /// Must be called before poll() in invoker mode.
    fn do_work(&mut self);
}

pub trait OrderUpdatePublisher: Send {
    fn publish(&mut self, msg: &OrderUpdateMsg) -> Result<(), TransportError>;
    fn do_work(&mut self);
}

pub trait TradePublisher: Send {
    fn publish(&mut self, msg: &TradeNotification) -> Result<(), TransportError>;
    fn do_work(&mut self);
}

pub trait MarketDataPublisher: Send {
    fn publish_depth(&mut self, msg: &DepthSnapshotEvent) -> Result<(), TransportError>;
    fn publish_depth50(&mut self, msg: &Depth50SnapshotEvent) -> Result<(), TransportError>;
    fn publish_level2(&mut self, msg: &Level2SnapshotEvent) -> Result<(), TransportError>;
    fn do_work(&mut self);
}

// ============================================================================
// Mock Implementation (for testing)
// ============================================================================

pub struct MockOrderSubscriber {
    rx: Receiver<InboundMsg>,
    pub tx: Sender<InboundMsg>,
}

impl MockOrderSubscriber {
    pub fn new() -> Self {
        let (tx, rx) = channel();
        Self { rx, tx }
    }
}

impl OrderSubscriber for MockOrderSubscriber {
    fn poll(&mut self) -> Option<InboundMsg> {
        self.rx.try_recv().ok()
    }

    fn is_connected(&self) -> bool {
        true
    }

    fn do_work(&mut self) {
        // Mock: no-op
    }
}

pub struct MockOrderUpdatePublisher {
    pub rx: Receiver<OrderUpdateMsg>,
    tx: Sender<OrderUpdateMsg>,
}

impl MockOrderUpdatePublisher {
    pub fn new() -> Self {
        let (tx, rx) = channel();
        Self { rx, tx }
    }
}

impl OrderUpdatePublisher for MockOrderUpdatePublisher {
    fn publish(&mut self, msg: &OrderUpdateMsg) -> Result<(), TransportError> {
        self.tx.send(*msg).map_err(|_| TransportError::Closed)
    }

    fn do_work(&mut self) {
        // Mock: no-op
    }
}

pub struct MockTradePublisher {
    pub rx: Receiver<TradeNotification>,
    tx: Sender<TradeNotification>,
}

impl MockTradePublisher {
    pub fn new() -> Self {
        let (tx, rx) = channel();
        Self { rx, tx }
    }
}

impl TradePublisher for MockTradePublisher {
    fn publish(&mut self, msg: &TradeNotification) -> Result<(), TransportError> {
        self.tx.send(*msg).map_err(|_| TransportError::Closed)
    }

    fn do_work(&mut self) {
        // Mock: no-op
    }
}

pub struct MockMarketDataPublisher {
    pub depth_rx: Receiver<DepthSnapshotEvent>,
    pub depth50_rx: Receiver<Depth50SnapshotEvent>,
    pub level2_rx: Receiver<Level2SnapshotEvent>,
    depth_tx: Sender<DepthSnapshotEvent>,
    depth50_tx: Sender<Depth50SnapshotEvent>,
    level2_tx: Sender<Level2SnapshotEvent>,
}

impl MockMarketDataPublisher {
    pub fn new() -> Self {
        let (depth_tx, depth_rx) = channel();
        let (depth50_tx, depth50_rx) = channel();
        let (level2_tx, level2_rx) = channel();
        Self {
            depth_rx,
            depth50_rx,
            level2_rx,
            depth_tx,
            depth50_tx,
            level2_tx,
        }
    }
}

impl MarketDataPublisher for MockMarketDataPublisher {
    fn publish_depth(&mut self, msg: &DepthSnapshotEvent) -> Result<(), TransportError> {
        self.depth_tx.send(*msg).map_err(|_| TransportError::Closed)
    }

    fn publish_depth50(&mut self, msg: &Depth50SnapshotEvent) -> Result<(), TransportError> {
        self.depth50_tx
            .send(*msg)
            .map_err(|_| TransportError::Closed)
    }

    fn publish_level2(&mut self, msg: &Level2SnapshotEvent) -> Result<(), TransportError> {
        self.level2_tx
            .send(*msg)
            .map_err(|_| TransportError::Closed)
    }

    fn do_work(&mut self) {
        // Mock: no-op
    }
}

// ============================================================================
// Factory Functions
// ============================================================================

pub fn mock_transport() -> (
    Box<dyn OrderSubscriber>,
    Box<dyn OrderUpdatePublisher>,
    Box<dyn TradePublisher>,
    Box<dyn MarketDataPublisher>,
) {
    let subscriber = MockOrderSubscriber::new();
    let order_update_pub = MockOrderUpdatePublisher::new();
    let trade_pub = MockTradePublisher::new();
    let market_data_pub = MockMarketDataPublisher::new();

    (
        Box::new(subscriber),
        Box::new(order_update_pub),
        Box::new(trade_pub),
        Box::new(market_data_pub),
    )
}

// For convenience in tests
pub fn mock_transport_with_parts() -> (
    MockOrderSubscriber,
    MockOrderUpdatePublisher,
    MockTradePublisher,
    MockMarketDataPublisher,
) {
    (
        MockOrderSubscriber::new(),
        MockOrderUpdatePublisher::new(),
        MockTradePublisher::new(),
        MockMarketDataPublisher::new(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_order_subscriber() {
        let mut subscriber = MockOrderSubscriber::new();
        assert!(subscriber.is_connected());

        let req = NewOrderRequest {
            client_order_id: 1,
            participant_id: 456,
            price_ticks: 10_050,    // 100.5 at tick=0.01
            quantity_lots: 10_000_000, // 10.0 at step=1e-6
            side: 0,
            time_in_force: 0,
            _pad: [0; 14],
            symbol: [0; 16],
        };

        subscriber.tx.send(InboundMsg::NewOrder(req)).unwrap();
        let msg = subscriber.poll().expect("should receive message");

        match msg {
            InboundMsg::NewOrder(_r) => {
                // Verified message was received successfully
                assert!(true);
            }
            _ => panic!("expected NewOrder"),
        }
    }

    #[test]
    fn test_mock_order_update_publisher() {
        let mut publisher = MockOrderUpdatePublisher::new();
        let msg = OrderUpdateMsg::accepted(1, 2, 3, 1000);
        publisher.publish(&msg).expect("publish failed");

        let received = publisher.rx.recv().expect("receive failed");
        assert_eq!(received.kind, order_update_kind::ACCEPTED);
    }

    #[test]
    fn test_mock_trade_publisher() {
        let mut publisher = MockTradePublisher::new();
        let msg = TradeNotification {
            sequence: 1,
            taker_order_id: 10,
            maker_order_id: 20,
            price: 100.5,
            quantity: 5.0,
            side: 0,
            _pad: [0; 7],
            symbol: [0; 16],
        };

        publisher.publish(&msg).expect("publish failed");
        let _received = publisher.rx.recv().expect("receive failed");
        // Verified message was received successfully
        assert!(true);
    }

    #[test]
    fn test_mock_market_data_publisher() {
        let mut publisher = MockMarketDataPublisher::new();

        let snapshot = DepthSnapshotEvent::new(1000, 1);

        publisher.publish_depth(&snapshot).expect("publish failed");
        let _received = publisher.depth_rx.recv().expect("receive failed");
        // Verified message was received successfully
        assert!(true);
    }
}
