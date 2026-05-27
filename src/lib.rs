//! LightningX matching engine — lock-free SkipList order book, 6–9M TPS, <100ns latency.

// ── 撮合子系统 (matching engine + market data) ────────────────────────────────
pub mod matching;

// ── 传输子系统 (Aeron / SBE / tracer) ────────────────────────────────────────
pub mod transport;

// ── 柜台子系统 (desk server + API) ───────────────────────────────────────────
pub mod desk;

// ── Flat re-exports: preserve all existing `crate::module` paths ──────────────
pub use matching::engine;
pub use matching::error;
pub use matching::event;
pub use matching::float_ext;
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

pub use desk::account;
pub use desk::account_repository;
pub use desk::api;
pub use desk::db;
pub use desk::models;
pub use desk::positions;
pub use desk::rate_limit;
pub use desk::snowflake;
pub use desk::symbol_rules;
pub use desk::user_service;
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
pub use rate_limit::{RateLimitPolicy, RateLimiter};
pub use snapshot::{DepthSnapshot, PriceLevel};
pub use snowflake::SnowflakeIdGenerator;
pub use symbol_rules::{validate_order_shape, SymbolRules};
pub use trade::Trade;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_order_creation() {
        let order = Order::new(1, Side::Buy, 50000.0, 10.0, TimeInForce::GTC, 0);
        assert_eq!(order.id, 1);
        assert_eq!(order.side, Side::Buy);
        assert_eq!(order.price, 50000.0);
        assert_eq!(order.quantity, 10.0);
        assert_eq!(order.filled, 0.0);
    }

    #[test]
    fn test_order_remaining() {
        let mut order = Order::new(1, Side::Buy, 50000.0, 10.0, TimeInForce::GTC, 0);
        assert_eq!(order.remaining(), 10.0);
        order.filled = 5.0;
        assert_eq!(order.remaining(), 5.0);
    }

    #[test]
    fn test_order_is_filled() {
        let mut order = Order::new(1, Side::Buy, 50000.0, 10.0, TimeInForce::GTC, 0);
        assert!(!order.is_filled());
        order.filled = 10.0;
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
