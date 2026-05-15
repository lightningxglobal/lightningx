/// Trading Engine - 撮合系统主体
///
/// 双线程架构：
/// Thread 1: 接收委托 + 撮合 + 生成事件 → rtrb
/// Thread 2: 消费事件 + SBE编码 → 通过Publisher发布

use crate::engine::{MatchingEngine, PoolConfig};
use crate::market_data::MarketDataConfig;
use crate::order_update::OrderUpdateEvent;
use crate::order::{Order, Side, TimeInForce};
use crate::market_data::{TradeEvent, DepthSnapshotEvent, Depth50SnapshotEvent, Level2SnapshotEvent};
use crate::transport::{
    OrderSubscriber, OrderUpdatePublisher, TradePublisher, MarketDataPublisher,
    InboundMsg, OrderUpdateMsg,
};
use crate::time_provider::wall_clock_nanos;
use rtrb::{RingBuffer, Producer, Consumer};
use std::thread::{JoinHandle, spawn};
use std::collections::HashMap;

pub struct TradingConfig {
    pub pool_config: PoolConfig,
    pub market_data_config: MarketDataConfig,
    pub order_update_ring: usize,
    pub trade_ring: usize,
    pub depth_ring: usize,
    pub depth50_ring: usize,
    pub level2_ring: usize,
}

impl Default for TradingConfig {
    fn default() -> Self {
        Self {
            pool_config: PoolConfig {
                order_capacity: 1_000_000,
                orderbook_type: crate::orderbook_impl::OrderBookType::SkipList,
                queue_capacity: 10_000,
            },
            market_data_config: MarketDataConfig::new(
                10_000_000,    // BBO
                false,
                50_000_000,    // D50
                100_000_000,   // Level2
            ),
            order_update_ring: 65536,
            trade_ring: 65536,
            depth_ring: 1024,
            depth50_ring: 256,
            level2_ring: 64,
        }
    }
}

pub struct TradingEngine {
    config: TradingConfig,
}

impl TradingEngine {
    pub fn new(config: TradingConfig) -> Self {
        Self { config }
    }

    pub fn run(
        self,
        subscriber: Box<dyn OrderSubscriber>,
        order_update_pub: Box<dyn OrderUpdatePublisher>,
        trade_pub: Box<dyn TradePublisher>,
        market_data_pub: Box<dyn MarketDataPublisher>,
    ) -> (JoinHandle<()>, JoinHandle<()>) {
        // 创建rtrb ring buffers
        let (order_update_tx, order_update_rx) = RingBuffer::new(self.config.order_update_ring);
        let (trade_tx, trade_rx) = RingBuffer::new(self.config.trade_ring);
        let (depth_tx, depth_rx) = RingBuffer::new(self.config.depth_ring);
        let (depth50_tx, depth50_rx) = RingBuffer::new(self.config.depth50_ring);
        let (level2_tx, level2_rx) = RingBuffer::new(self.config.level2_ring);

        let pool_config = self.config.pool_config;
        let market_data_config = self.config.market_data_config;

        // 启动撮合线程
        let matching_thread = spawn(move || {
            run_matching_thread(
                subscriber,
                pool_config,
                market_data_config,
                order_update_tx,
                trade_tx,
                depth_tx,
                depth50_tx,
                level2_tx,
            )
        });

        // 启动发布线程
        let publishing_thread = spawn(move || {
            run_publishing_thread(
                trade_rx,
                order_update_rx,
                depth_rx,
                depth50_rx,
                level2_rx,
                order_update_pub,
                trade_pub,
                market_data_pub,
            )
        });

        (matching_thread, publishing_thread)
    }
}

// ============================================================================
// Matching Thread
// ============================================================================

fn run_matching_thread(
    mut subscriber: Box<dyn OrderSubscriber>,
    pool_config: PoolConfig,
    market_data_config: MarketDataConfig,
    mut order_update_tx: Producer<OrderUpdateEvent>,
    mut trade_tx: Producer<TradeEvent>,
    mut depth_tx: Producer<DepthSnapshotEvent>,
    mut depth50_tx: Producer<Depth50SnapshotEvent>,
    mut level2_tx: Producer<Level2SnapshotEvent>,
) {
    let mut engine = MatchingEngine::new(pool_config).expect("failed to create engine");
    engine.set_market_data_config(market_data_config);

    // 创建rtrb senders
    let (trade_tx2, mut trade_rx) = RingBuffer::new(1024);
    let (depth_tx2, mut depth_rx) = RingBuffer::new(256);
    let (depth50_tx2, mut depth50_rx) = RingBuffer::new(64);
    let (level2_tx2, mut level2_rx) = RingBuffer::new(16);

    engine.set_trade_event_sender(trade_tx2);
    engine.set_depth_snapshot_sender(depth_tx2);
    engine.set_depth50_sender(depth50_tx2);
    engine.set_level2_sender(level2_tx2);

    // 维护order_id -> (client_order_id, participant_id, quantity, price)的映射
    let mut order_info: HashMap<u64, (u64, u64, f64, f64)> = HashMap::new();

    loop {
        // 轮询订单
        if let Some(msg) = subscriber.poll() {
            let now = wall_clock_nanos();
            match msg {
                InboundMsg::NewOrder(req) => {
                    // Generate order ID using engine's counter
                    let next_id = engine.stats().total_orders as u64;
                    let order = Order::new(
                        next_id,
                        if req.side == 0 { Side::Buy } else { Side::Sell },
                        req.price,
                        req.quantity,
                        match req.time_in_force {
                            1 => TimeInForce::IOC,
                            2 => TimeInForce::FOK,
                            3 => TimeInForce::PostOnly,
                            _ => TimeInForce::GTC,
                        },
                        now,
                    );

                    let order_id = order.id;
                    order_info.insert(order_id, (req.client_order_id, req.participant_id, req.quantity, req.price));

                    match engine.place_order(order) {
                        Ok(result) => {
                            let evt = match result.status {
                                crate::engine::OrderStatus::Accepted => {
                                    OrderUpdateEvent::accepted(
                                        order_id,
                                        req.client_order_id,
                                        req.participant_id,
                                        now,
                                    )
                                }
                                crate::engine::OrderStatus::Filled => {
                                    OrderUpdateEvent::filled(
                                        order_id,
                                        req.client_order_id,
                                        req.participant_id,
                                        req.price,
                                        result.filled,
                                        now,
                                    )
                                }
                                crate::engine::OrderStatus::PartiallyFilled => {
                                    OrderUpdateEvent::partial_fill(
                                        order_id,
                                        req.client_order_id,
                                        req.participant_id,
                                        req.price,
                                        result.filled,
                                        req.quantity - result.filled,
                                        now,
                                    )
                                }
                                _ => continue,
                            };
                            let _ = order_update_tx.push(evt);
                        }
                        Err(_e) => {
                            let evt = OrderUpdateEvent::rejected(
                                req.client_order_id,
                                req.participant_id,
                                1,
                                now,
                            );
                            let _ = order_update_tx.push(evt);
                        }
                    }
                }
                InboundMsg::CancelOrder(req) => {
                    // Copy request to avoid alignment issues with packed struct
                    let req_copy = req;
                    let order_id = unsafe { std::ptr::read_unaligned(&req_copy as *const _ as *const u64) };
                    match engine.cancel_order(order_id) {
                        Ok(result) => {
                            if let Some((client_order_id, participant_id, _qty, _price)) = order_info.get(&order_id) {
                                let evt = OrderUpdateEvent::cancelled(
                                    order_id,
                                    *client_order_id,
                                    *participant_id,
                                    result.cancelled_quantity,
                                    now,
                                );
                                let _ = order_update_tx.push(evt);
                            }
                        }
                        Err(_) => {
                            // Cancel failed, don't send update
                        }
                    }
                }
            }
        }

        // 转发engine生成的事件到跨线程ring buffers
        while let Ok(evt) = trade_rx.pop() {
            let _ = trade_tx.push(evt);
        }
        while let Ok(evt) = depth_rx.pop() {
            let _ = depth_tx.push(evt);
        }
        while let Ok(evt) = depth50_rx.pop() {
            let _ = depth50_tx.push(evt);
        }
        while let Ok(evt) = level2_rx.pop() {
            let _ = level2_tx.push(evt);
        }

        std::hint::spin_loop();
    }
}

// ============================================================================
// Publishing Thread
// ============================================================================

fn run_publishing_thread(
    mut trade_rx: Consumer<TradeEvent>,
    mut order_update_rx: Consumer<OrderUpdateEvent>,
    mut depth_rx: Consumer<DepthSnapshotEvent>,
    mut depth50_rx: Consumer<Depth50SnapshotEvent>,
    mut level2_rx: Consumer<Level2SnapshotEvent>,
    mut order_update_pub: Box<dyn OrderUpdatePublisher>,
    mut trade_pub: Box<dyn TradePublisher>,
    mut market_data_pub: Box<dyn MarketDataPublisher>,
) {
    loop {
        // 消费order updates
        while let Ok(evt) = order_update_rx.pop() {
            let msg = OrderUpdateMsg {
                kind: evt.kind,
                reject_reason: evt.reject_reason,
                _pad1: [0; 6],
                order_id: evt.order_id,
                client_order_id: evt.client_order_id,
                participant_id: evt.participant_id,
                fill_price: evt.fill_price,
                fill_qty: evt.fill_qty,
                remaining_qty: evt.remaining_qty,
                timestamp: evt.timestamp,
            };
            let _ = order_update_pub.publish(&msg);
        }

        // 消费trades
        while let Ok(evt) = trade_rx.pop() {
            let msg = crate::sbe::TradeNotification {
                sequence: evt.sequence,
                taker_order_id: evt.taker_id,
                maker_order_id: evt.maker_id,
                price: evt.price,
                quantity: evt.quantity,
                side: if evt.side == Side::Buy { 0 } else { 1 },
                _pad: [0; 7],
            };
            let _ = trade_pub.publish(&msg);
        }

        // 消费depth snapshots
        while let Ok(evt) = depth_rx.pop() {
            let _ = market_data_pub.publish_depth(&evt);
        }
        while let Ok(evt) = depth50_rx.pop() {
            let _ = market_data_pub.publish_depth50(&evt);
        }
        while let Ok(evt) = level2_rx.pop() {
            let _ = market_data_pub.publish_level2(&evt);
        }

        std::hint::spin_loop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trading_config_default() {
        let config = TradingConfig::default();
        assert_eq!(config.order_update_ring, 65536);
        assert_eq!(config.trade_ring, 65536);
    }

    #[test]
    fn test_trading_engine_creation() {
        let config = TradingConfig::default();
        let _engine = TradingEngine::new(config);
    }
}
