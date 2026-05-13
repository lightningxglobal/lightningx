use crate::order::Side;

/// 撮合引擎事件，发布到Aeron
#[repr(C, align(64))]
#[derive(Debug, Clone)]
pub enum MatchingEvent {
    /// 订单已下达
    OrderPlaced {
        order_id: u64,
        side: Side,
        price: f64,
        quantity: f64,
        timestamp: u64,
    },
    /// 订单已取消
    OrderCancelled {
        order_id: u64,
        timestamp: u64,
    },
    /// 成交事件
    Trade {
        taker_order_id: u64,
        maker_order_id: u64,
        price: f64,
        quantity: f64,
        timestamp: u64,
    },
}

impl MatchingEvent {
    /// 获取事件时间戳
    pub fn timestamp(&self) -> u64 {
        match self {
            Self::OrderPlaced { timestamp, .. } => *timestamp,
            Self::OrderCancelled { timestamp, .. } => *timestamp,
            Self::Trade { timestamp, .. } => *timestamp,
        }
    }
}
