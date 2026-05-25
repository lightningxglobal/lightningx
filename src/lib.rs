//! 加密货币交易撮合引擎
//!
//! 基于跳表的极高频交易撮合引擎，支持GTC、IOC、FOK、Post-Only四种委托类型。
//! 单线程无锁设计，目标TPS > 6,000,000，延迟 < 3微秒。

pub mod aeron_channels;
pub mod order;
pub mod trade;
pub mod error;
pub mod event;
pub mod pools;
pub mod list_pool;
pub mod orderbook;
pub mod orderbook_impl;
pub mod skiplist;
pub mod array_orderbook;
pub mod btree_orderbook;
pub mod engine;
pub mod snapshot;
pub mod market_data;
pub mod aeron_publisher;
pub mod time_provider;
pub mod sbe;
pub mod transport;
pub mod order_update;
pub mod trading_engine;
pub mod aeron_transport;
pub mod account;
pub mod rate_limit;
pub mod market_data_server;
pub mod snowflake;
pub mod desk_server;
pub mod db;
pub mod models;
pub mod user_service;
pub mod account_repository;
pub mod positions;
pub mod api;
pub mod ws_handler;
pub mod tracer;
#[cfg(test)]
mod sbe_tests;
#[cfg(test)]
mod boundary_tests;

pub use engine::{
    MatchingEngine, PoolConfig, EngineStats, PlaceOrderResult, OrderStatus, CancelOrderResult,
};
pub use trade::Trade;
pub use order::{Order, Side, TimeInForce};
pub use error::{MatchingEngineError, OrderResult};
pub use event::MatchingEvent;
pub use snapshot::{DepthSnapshot, PriceLevel};
pub use market_data::{
    TradeEvent, MarketDataEngine, BBOSnapshot, Level2Snapshot, AggregateTrade, Statistics24h,
    PublishedSnapshot, SnapshotTimer, SnapshotPublisherThread, TradePublisherThread,
    DepthSnapshotEvent, Depth50SnapshotEvent, Level2SnapshotEvent, MarketDataConfig,
};
pub use aeron_publisher::{
    SnapshotPublisher, TradePublisher, AeronConfig, PublisherResult, PublisherError,
    snapshot_to_bytes, bytes_to_snapshot, trade_to_bytes, bytes_to_trade,
};
pub use account::{
    Account, AccountManager, Position, AccountId,
};
pub use rate_limit::{
    RateLimiter, RateLimitPolicy,
};
pub use market_data_server::{
    MarketDataServer, DepthLevel,
    TradeSnapshot, Statistics, BBO, create_router, start_server,
};
pub mod market_data_server_types {
    pub use crate::market_data_server::{DepthSnapshot, DepthLevel, TradeSnapshot, Statistics, BBO};
}
pub use snowflake::SnowflakeIdGenerator;
pub use desk_server::{
    DeskServer, DeskConfig, SessionInfo, SessionId,
    DeskAppState, check_price_deviation, desk_ws_handler, desk_market_data_broadcaster,
};

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
