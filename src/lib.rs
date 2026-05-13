//! 加密货币交易撮合引擎
//!
//! 基于跳表的极高频交易撮合引擎，支持GTC、IOC、FOK、Post-Only四种委托类型。
//! 单线程无锁设计，目标TPS > 6,000,000，延迟 < 3微秒。

pub mod order;
pub mod error;
pub mod event;
pub mod pools;
pub mod skiplist;
pub mod engine;
pub mod snapshot;
pub mod recovery;

pub use engine::{
    MatchingEngine, PoolConfig, EngineStats, PlaceOrderResult, OrderStatus, Trade, CancelOrderResult,
};
pub use order::{Order, Side, TimeInForce};
pub use error::{MatchingEngineError, OrderResult};
pub use event::MatchingEvent;
pub use snapshot::{DepthSnapshot, PriceLevel};
