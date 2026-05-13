/// 价格档位
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct PriceLevel {
    pub price: f64,
    pub quantity: f64,
}

/// 市场深度快照，64字节对齐
#[repr(C, align(64))]
#[derive(Debug, Clone)]
pub struct DepthSnapshot {
    pub timestamp: u64,
    pub sequence: u64,
    pub num_bids: u16,
    pub num_asks: u16,
    pub bids: [PriceLevel; 20],
    pub asks: [PriceLevel; 20],
}

impl Default for DepthSnapshot {
    fn default() -> Self {
        Self {
            timestamp: 0,
            sequence: 0,
            num_bids: 0,
            num_asks: 0,
            bids: [PriceLevel { price: 0.0, quantity: 0.0 }; 20],
            asks: [PriceLevel { price: 0.0, quantity: 0.0 }; 20],
        }
    }
}

impl DepthSnapshot {
    /// 创建新快照
    pub fn new(timestamp: u64, sequence: u64) -> Self {
        Self {
            timestamp,
            sequence,
            ..Default::default()
        }
    }

    /// 添加买盘价格档位
    pub fn add_bid(&mut self, price: f64, quantity: f64) -> Result<(), String> {
        if self.num_bids >= 20 {
            return Err("Too many bid levels".to_string());
        }

        self.bids[self.num_bids as usize] = PriceLevel { price, quantity };
        self.num_bids += 1;
        Ok(())
    }

    /// 添加卖盘价格档位
    pub fn add_ask(&mut self, price: f64, quantity: f64) -> Result<(), String> {
        if self.num_asks >= 20 {
            return Err("Too many ask levels".to_string());
        }

        self.asks[self.num_asks as usize] = PriceLevel { price, quantity };
        self.num_asks += 1;
        Ok(())
    }
}
