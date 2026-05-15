/// 成交信息
#[derive(Debug, Clone, Copy, Default)]
pub struct Trade {
    pub taker_id: u64,
    pub maker_id: u64,
    pub price: f64,
    pub quantity: f64,
}

impl Trade {
    /// 创建新的成交记录
    pub fn new(
        taker_id: u64,
        maker_id: u64,
        price: f64,
        quantity: f64,
    ) -> Self {
        Self {
            taker_id,
            maker_id,
            price,
            quantity,
        }
    }
}
