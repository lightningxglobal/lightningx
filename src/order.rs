/// 订单方向
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

/// 委托类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeInForce {
    /// Good Till Cancel - 一直有效直到成交或撤销
    GTC,
    /// Immediate Or Cancel - 立即成交，未成交部分取消
    IOC,
    /// Fill Or Kill - 全部成交或完全取消
    FOK,
    /// Post-Only - 只挂单，不吃单
    PostOnly,
}

/// 订单结构，64字节对齐
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy)]
pub struct Order {
    pub id: u64,
    pub side: Side,
    pub price: f64,
    pub quantity: f64,
    pub filled: f64,
    pub time_in_force: TimeInForce,
    pub timestamp: u64,
    _padding: [u8; 18],
}

impl Order {
    /// 创建新订单
    pub fn new(
        id: u64,
        side: Side,
        price: f64,
        quantity: f64,
        time_in_force: TimeInForce,
        timestamp: u64,
    ) -> Self {
        Self {
            id,
            side,
            price,
            quantity,
            filled: 0.0,
            time_in_force,
            timestamp,
            _padding: [0; 18],
        }
    }

    /// 获取剩余数量
    #[inline(always)]
    pub fn remaining(&self) -> f64 {
        self.quantity - self.filled
    }

    /// 检查是否完全成交
    #[inline(always)]
    pub fn is_filled(&self) -> bool {
        self.filled >= self.quantity
    }

    /// 检查订单是否有效
    #[inline(always)]
    pub fn is_valid(&self) -> bool {
        self.price > 0.0 && self.quantity > 0.0 && !self.price.is_nan() && !self.quantity.is_nan()
    }
}

impl Default for Order {
    fn default() -> Self {
        Self {
            id: 0,
            side: Side::Buy,
            price: 0.0,
            quantity: 0.0,
            filled: 0.0,
            time_in_force: TimeInForce::GTC,
            timestamp: 0,
            _padding: [0; 18],
        }
    }
}
