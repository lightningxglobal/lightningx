//! LightningX matching engine — lock-free SkipList order book, 6–9M TPS, <100ns latency.

// ── 撮合子系统 (matching engine + market data) ────────────────────────────────
pub mod matching;

// ── 传输子系统 (Aeron / SBE / tracer) ────────────────────────────────────────
pub mod transport;

// ── 柜台子系统 (desk server + API) ───────────────────────────────────────────
pub mod desk;
pub mod util;

// ── Flat re-exports: preserve all existing `crate::module` paths ──────────────
pub use matching::engine;
pub use matching::error;
pub use matching::event;
pub use matching::list_pool;
pub use matching::market_data;
pub use matching::order;
pub use matching::orderbook;
pub use matching::orderbook_impl;
pub use matching::pools;
pub use matching::skiplist;
pub use matching::snapshot;
pub use matching::time_provider;
pub use matching::trade;

pub use transport::aeron_channels;
pub use transport::aeron_transport;
pub use transport::order_update;
pub use transport::sbe;
pub use transport::tracer;
pub use transport::ws_sbe;

pub use desk::account;
pub use desk::account_repository;
pub use desk::api;
pub use desk::counter_shard;
pub use desk::db;
pub use desk::leader;
pub use desk::metrics;
pub use desk::models;
pub use desk::money;
pub use desk::order_state;
pub use desk::positions;
pub use desk::rate_limit;
pub use desk::read_actor;
pub use desk::reconcile;
pub use desk::snowflake;
pub use desk::symbol_rules;
pub use desk::user_service;
pub use desk::write_actor;
pub use desk::ws_handler;

#[cfg(test)]
mod boundary_tests;
#[cfg(test)]
mod sbe_tests;

// ── Public re-exports ─────────────────────────────────────────────────────────
pub use account::{Account, AccountId, AccountManager, Position};
pub use engine::{
    CancelOrderResult, EngineStats, MatchingEngine, OrderStatus, PlaceOrderResult, PoolConfig,
};
pub use error::{MatchingEngineError, OrderResult};
pub use event::MatchingEvent;
pub use market_data::{
    AggregateTrade, BBOSnapshot, Depth50SnapshotEvent, DepthSnapshotEvent, Level2Snapshot,
    Level2SnapshotEvent, MarketDataConfig, MarketDataEngine, PublishedSnapshot, SnapshotTimer,
    Statistics24h, TradeEvent,
};
pub use order::{Order, Side, TimeInForce};
pub use order_state::{
    DbOrderStatus, HISTORY_DB_STATUSES, OPEN_DB_STATUSES, WsOrderStatus, db_status_from_engine,
    db_status_from_update_kind, maker_ws_status_from_db_status, ws_status_from_engine,
    ws_status_from_update_kind,
};
pub use rate_limit::{RateLimitPolicy, RateLimiter};
pub use snapshot::{DepthSnapshot, PriceLevel};
pub use snowflake::SnowflakeIdGenerator;
pub use symbol_rules::{
    FixedOrderInput, FixedOrderShape, SymbolRules, normalize_order_shape, validate_order_shape,
};
pub use trade::Trade;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_order_creation() {
        let order = Order::new(1, Side::Buy, 50000, 10, TimeInForce::GTC, 0);
        assert_eq!(order.id, 1);
        assert_eq!(order.side, Side::Buy);
        assert_eq!(order.price_ticks, 50000);
        assert_eq!(order.quantity_lots, 10);
        assert_eq!(order.filled_lots, 0);
    }

    #[test]
    fn test_order_remaining() {
        let mut order = Order::new(1, Side::Buy, 50000, 10, TimeInForce::GTC, 0);
        assert_eq!(order.remaining_lots(), 10);
        order.filled_lots = 5;
        assert_eq!(order.remaining_lots(), 5);
    }

    #[test]
    fn test_order_is_filled() {
        let mut order = Order::new(1, Side::Buy, 50000, 10, TimeInForce::GTC, 0);
        assert!(!order.is_filled());
        order.filled_lots = 10;
        assert!(order.is_filled());
    }

    #[test]
    fn test_depth_snapshot_creation() {
        let snapshot = DepthSnapshot::new(1000000, 1);
        assert_eq!(snapshot.timestamp, 1000000);
        assert_eq!(snapshot.sequence, 1);
        assert_eq!(snapshot.num_bids, 0);
        assert_eq!(snapshot.num_asks, 0);
    }

    #[test]
    fn test_depth_snapshot_add_levels() {
        let mut snapshot = DepthSnapshot::new(1000000, 1);

        // Add bids
        for i in 0..3 {
            let _ = snapshot.add_bid(50000.0 - i as f64, 10.0 + i as f64);
        }
        assert_eq!(snapshot.num_bids, 3);

        // Add asks
        for i in 0..3 {
            let _ = snapshot.add_ask(50000.0 + i as f64, 10.0 + i as f64);
        }
        assert_eq!(snapshot.num_asks, 3);
    }
}
