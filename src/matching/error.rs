use crate::order::{PriceTicks, QuantityLots};
use std::fmt;

/// 撮合引擎错误类型
#[derive(Debug, Clone)]
pub enum MatchingEngineError {
    /// 订单不存在
    OrderNotFound,
    /// 价格无效
    InvalidPrice(PriceTicks),
    /// 数量无效
    InvalidQuantity(QuantityLots),
    /// 订单已成交
    AlreadyFilled,
    /// 订单已取消
    AlreadyCancelled,
    /// 订单ID已存在
    DuplicateOrderId(u64),
    /// 委托类型无效
    InvalidTimeInForce,
    /// 订单池已耗尽
    OrderPoolExhausted,
    /// 节点池已耗尽
    NodePoolExhausted,
    /// 队列池已耗尽
    QueuePoolExhausted,
    /// Aeron未连接
    AeronNotConnected,
    /// Aeron背压
    AeronBackPressured,
    /// Aeron已关闭
    AeronClosed,
}

impl fmt::Display for MatchingEngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OrderNotFound => write!(f, "Order not found"),
            Self::InvalidPrice(p) => write!(f, "Invalid price: {}", p),
            Self::InvalidQuantity(q) => write!(f, "Invalid quantity: {}", q),
            Self::AlreadyFilled => write!(f, "Order already filled"),
            Self::AlreadyCancelled => write!(f, "Order already cancelled"),
            Self::DuplicateOrderId(id) => write!(f, "Duplicate order id: {}", id),
            Self::InvalidTimeInForce => write!(f, "Invalid time in force"),
            Self::OrderPoolExhausted => write!(f, "Order pool exhausted"),
            Self::NodePoolExhausted => write!(f, "Node pool exhausted"),
            Self::QueuePoolExhausted => write!(f, "Queue pool exhausted"),
            Self::AeronNotConnected => write!(f, "Aeron not connected"),
            Self::AeronBackPressured => write!(f, "Aeron back pressured"),
            Self::AeronClosed => write!(f, "Aeron closed"),
        }
    }
}

impl std::error::Error for MatchingEngineError {}

/// 订单操作结果类型
pub type OrderResult<T> = Result<T, MatchingEngineError>;
