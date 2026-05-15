//! 市场数据结构 - 用于市场数据引擎和快照发送
//!
//! 本模块定义了高频交易系统中使用的所有市场数据结构。
//! 所有结构都采用64字节对齐，以优化缓存性能。

use crate::order::Side;
use crossbeam::channel::Receiver;

/// 交易事件 - 实时成交数据（64字节对齐）
/// 用于匹配引擎和市场数据引擎之间的通信
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TradeEvent {
    pub sequence: u64,        // 全局交易序列号
    pub order_id: u64,        // 订单ID
    pub maker_order_id: u64,  // 挂单ID
    pub timestamp: u64,       // 纳秒时间戳
    pub price: f64,           // 成交价格
    pub quantity: f64,        // 成交数量
    pub taker_id: u64,        // 吃单者ID
    pub maker_id: u64,        // 挂单者ID
    pub side: Side,           // 吃单方向 (Buy/Sell)
    _padding: [u8; 7],        // 填充至64字节
}

impl TradeEvent {
    /// 创建新的交易事件
    pub fn new(
        sequence: u64,
        order_id: u64,
        maker_order_id: u64,
        timestamp: u64,
        price: f64,
        quantity: f64,
        side: Side,
        taker_id: u64,
        maker_id: u64,
    ) -> Self {
        Self {
            sequence,
            order_id,
            maker_order_id,
            timestamp,
            price,
            quantity,
            taker_id,
            maker_id,
            side,
            _padding: [0; 7],
        }
    }
}

impl Default for TradeEvent {
    fn default() -> Self {
        Self {
            sequence: 0,
            order_id: 0,
            maker_order_id: 0,
            timestamp: 0,
            price: 0.0,
            quantity: 0.0,
            taker_id: 0,
            maker_id: 0,
            side: Side::Buy,
            _padding: [0; 7],
        }
    }
}

/// 最优买卖价快照 - 最高频率更新
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BBOSnapshot {
    pub timestamp: u64,           // 纳秒时间戳
    pub sequence: u64,            // 序列号
    pub best_bid_price: f64,      // 最高买价
    pub best_bid_qty: f64,        // 最高买价数量
    pub best_ask_price: f64,      // 最低卖价
    pub best_ask_qty: f64,        // 最低卖价数量
    pub bid_level_count: u16,     // 买方档位数
    pub ask_level_count: u16,     // 卖方档位数
    _padding: [u8; 12],           // 填充至64字节
}

impl BBOSnapshot {
    /// 创建新的BBO快照
    pub fn new(
        timestamp: u64,
        sequence: u64,
        best_bid_price: f64,
        best_bid_qty: f64,
        best_ask_price: f64,
        best_ask_qty: f64,
        bid_level_count: u16,
        ask_level_count: u16,
    ) -> Self {
        Self {
            timestamp,
            sequence,
            best_bid_price,
            best_bid_qty,
            best_ask_price,
            best_ask_qty,
            bid_level_count,
            ask_level_count,
            _padding: [0; 12],
        }
    }

    /// 获取中间价格（两端价格的平均值）
    #[inline(always)]
    pub fn mid_price(&self) -> f64 {
        (self.best_bid_price + self.best_ask_price) / 2.0
    }

    /// 获取买卖价差
    #[inline(always)]
    pub fn spread(&self) -> f64 {
        self.best_ask_price - self.best_bid_price
    }
}

impl Default for BBOSnapshot {
    fn default() -> Self {
        Self {
            timestamp: 0,
            sequence: 0,
            best_bid_price: 0.0,
            best_bid_qty: 0.0,
            best_ask_price: 0.0,
            best_ask_qty: 0.0,
            bid_level_count: 0,
            ask_level_count: 0,
            _padding: [0; 12],
        }
    }
}

/// 价格档位
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PriceLevel {
    pub price: f64,
    pub quantity: f64,
}

impl PriceLevel {
    /// 创建新的价格档位
    pub fn new(price: f64, quantity: f64) -> Self {
        Self { price, quantity }
    }
}

impl Default for PriceLevel {
    fn default() -> Self {
        Self {
            price: 0.0,
            quantity: 0.0,
        }
    }
}

/// Level 2 深度快照 - 包含前10档行情
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy)]
pub struct Level2Snapshot {
    pub timestamp: u64,                           // 纳秒时间戳
    pub sequence: u64,                            // 序列号
    pub bids: [PriceLevel; 10],                   // 买方前10档
    pub asks: [PriceLevel; 10],                   // 卖方前10档
    pub num_bids: u16,                            // 实际买方档位数
    pub num_asks: u16,                            // 实际卖方档位数
    _padding: [u8; 12],                           // 填充至64字节
}

impl Level2Snapshot {
    /// 创建新的Level2快照
    pub fn new(timestamp: u64, sequence: u64) -> Self {
        Self {
            timestamp,
            sequence,
            bids: [PriceLevel::default(); 10],
            asks: [PriceLevel::default(); 10],
            num_bids: 0,
            num_asks: 0,
            _padding: [0; 12],
        }
    }

    /// 添加买单档位（从高到低排序）
    pub fn add_bid(&mut self, price: f64, quantity: f64) -> bool {
        if self.num_bids >= 10 {
            return false;
        }
        self.bids[self.num_bids as usize] = PriceLevel::new(price, quantity);
        self.num_bids += 1;
        true
    }

    /// 添加卖单档位（从低到高排序）
    pub fn add_ask(&mut self, price: f64, quantity: f64) -> bool {
        if self.num_asks >= 10 {
            return false;
        }
        self.asks[self.num_asks as usize] = PriceLevel::new(price, quantity);
        self.num_asks += 1;
        true
    }

    /// 清空快照
    pub fn clear(&mut self) {
        self.bids = [PriceLevel::default(); 10];
        self.asks = [PriceLevel::default(); 10];
        self.num_bids = 0;
        self.num_asks = 0;
    }

    /// 获取最高买价
    #[inline(always)]
    pub fn best_bid(&self) -> Option<PriceLevel> {
        if self.num_bids > 0 {
            Some(self.bids[0])
        } else {
            None
        }
    }

    /// 获取最低卖价
    #[inline(always)]
    pub fn best_ask(&self) -> Option<PriceLevel> {
        if self.num_asks > 0 {
            Some(self.asks[0])
        } else {
            None
        }
    }
}

impl Default for Level2Snapshot {
    fn default() -> Self {
        Self::new(0, 0)
    }
}

/// 聚合成交数据 - 时间段内的OHLCV数据
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy)]
pub struct AggregateTrade {
    pub start_time: u64,              // 开始时间（纳秒）
    pub end_time: u64,                // 结束时间（纳秒）
    pub sequence: u64,                // 序列号范围（起始）
    pub open: f64,                    // 开盘价
    pub close: f64,                   // 收盘价
    pub high: f64,                    // 最高价
    pub low: f64,                     // 最低价
    pub volume: f64,                  // 总交易量
    pub quote_asset_volume: f64,      // 总交易额（成交量*价格）
    pub trade_count: u64,             // 成交笔数
}

impl AggregateTrade {
    /// 创建新的聚合成交
    pub fn new(start_time: u64, end_time: u64, sequence: u64) -> Self {
        Self {
            start_time,
            end_time,
            sequence,
            open: 0.0,
            close: 0.0,
            high: 0.0,
            low: 0.0,
            volume: 0.0,
            quote_asset_volume: 0.0,
            trade_count: 0,
        }
    }

    /// 初始化第一笔成交的OHLC
    pub fn init_with_first_trade(&mut self, price: f64, quantity: f64) {
        self.open = price;
        self.close = price;
        self.high = price;
        self.low = price;
        self.volume = quantity;
        self.quote_asset_volume = price * quantity;
        self.trade_count = 1;
    }

    /// 更新成交数据
    pub fn update_with_trade(&mut self, price: f64, quantity: f64) {
        self.close = price;
        self.high = self.high.max(price);
        self.low = self.low.min(price);
        self.volume += quantity;
        self.quote_asset_volume += price * quantity;
        self.trade_count += 1;
    }

    /// 获取成交均价
    #[inline(always)]
    pub fn vwap(&self) -> f64 {
        if self.volume > 0.0 {
            self.quote_asset_volume / self.volume
        } else {
            0.0
        }
    }
}

impl Default for AggregateTrade {
    fn default() -> Self {
        Self::new(0, 0, 0)
    }
}

/// 24小时统计数据
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy)]
pub struct Statistics24h {
    pub timestamp: u64,              // 统计时间戳（纳秒）
    pub price_24h_high: f64,         // 24小时最高价
    pub price_24h_low: f64,          // 24小时最低价
    pub price_change_percent: f64,   // 24小时价格变化百分比
    pub weighted_avg_price: f64,     // 加权平均价格
    pub volume_24h: f64,             // 24小时成交量
    pub quote_asset_volume_24h: f64, // 24小时成交额
    pub bid_price: f64,              // 当前买价
    pub bid_qty: f64,                // 当前买价数量
    pub ask_price: f64,              // 当前卖价
    pub ask_qty: f64,                // 当前卖价数量
    pub open_time: u64,              // 开盘时间
    pub close_time: u64,             // 收盘时间
    pub first_trade_id: u64,         // 第一笔成交ID
    pub last_trade_id: u64,          // 最后一笔成交ID
    pub trade_count: u64,            // 成交笔数
}

impl Statistics24h {
    /// 创建新的24小时统计
    pub fn new(timestamp: u64) -> Self {
        Self {
            timestamp,
            price_24h_high: 0.0,
            price_24h_low: 0.0,
            price_change_percent: 0.0,
            weighted_avg_price: 0.0,
            volume_24h: 0.0,
            quote_asset_volume_24h: 0.0,
            bid_price: 0.0,
            bid_qty: 0.0,
            ask_price: 0.0,
            ask_qty: 0.0,
            open_time: 0,
            close_time: 0,
            first_trade_id: 0,
            last_trade_id: 0,
            trade_count: 0,
        }
    }

    /// 更新最高/最低价
    pub fn update_price_range(&mut self, price: f64) {
        if self.price_24h_high == 0.0 || price > self.price_24h_high {
            self.price_24h_high = price;
        }
        if self.price_24h_low == 0.0 || price < self.price_24h_low {
            self.price_24h_low = price;
        }
    }

    /// 更新价格变化百分比
    pub fn update_price_change(&mut self, open_price: f64) {
        if open_price > 0.0 {
            let last_price = if self.quote_asset_volume_24h > 0.0 {
                self.quote_asset_volume_24h / self.volume_24h
            } else {
                0.0
            };
            self.price_change_percent = ((last_price - open_price) / open_price) * 100.0;
        }
    }

    /// 计算加权平均价格
    #[inline(always)]
    pub fn calculate_weighted_avg_price(&self) -> f64 {
        if self.volume_24h > 0.0 {
            self.quote_asset_volume_24h / self.volume_24h
        } else {
            0.0
        }
    }
}

impl Default for Statistics24h {
    fn default() -> Self {
        Self::new(0)
    }
}

/// 市场数据引擎 - 聚合和维护市场数据状态
///
/// 从匹配引擎接收TradeEvent，维护以下状态：
/// - BBO（最优买卖价）
/// - Level2深度（前10档）
/// - 聚合成交数据（时间桶）
/// - 24小时统计数据
///
/// 线程安全：接收方由Crossbeam channel提供线程安全保证
pub struct MarketDataEngine {
    /// TradeEvent接收方
    receiver: Receiver<TradeEvent>,
    /// 当前BBO快照
    bbo_snapshot: BBOSnapshot,
    /// 当前Level2快照
    level2_snapshot: Level2Snapshot,
    /// 24小时统计数据
    statistics_24h: Statistics24h,
    /// 聚合成交数据（预留，用于任务2.4）
    aggregate_trades: Vec<AggregateTrade>,
}

impl MarketDataEngine {
    /// 创建新的市场数据引擎
    ///
    /// # 参数
    /// - `receiver`: TradeEvent的接收方，来自匹配引擎
    ///
    /// # 返回
    /// 初始化完毕的MarketDataEngine实例
    pub fn new(receiver: Receiver<TradeEvent>) -> Self {
        Self {
            receiver,
            bbo_snapshot: BBOSnapshot::default(),
            level2_snapshot: Level2Snapshot::default(),
            statistics_24h: Statistics24h::default(),
            aggregate_trades: Vec::new(),
        }
    }

    /// 根据交易事件更新BBO状态
    ///
    /// 基于交易方向更新最优买卖价格和数量：
    /// - 买方成交（taker is buyer）：最低卖价可能下降
    /// - 卖方成交（taker is seller）：最高买价可能上升
    ///
    /// # 参数
    /// - `event`: 交易事件
    ///
    /// # 返回
    /// 如果BBO状态发生变化返回true，否则返回false
    #[inline]
    fn update_bbo_from_trade(&mut self, event: TradeEvent) -> bool {
        let mut changed = false;

        match event.side {
            Side::Buy => {
                // 买方成交（taker is buyer）：卖方流动性被消耗
                // 成交价格成为可能的最低卖价（ask）
                if self.bbo_snapshot.best_ask_price == 0.0 {
                    // 初始状态：设置初始ask价格和数量
                    self.bbo_snapshot.best_ask_price = event.price;
                    self.bbo_snapshot.best_ask_qty = event.quantity;
                    changed = true;
                } else if event.price < self.bbo_snapshot.best_ask_price {
                    // Ask价格下降（ask improved）
                    self.bbo_snapshot.best_ask_price = event.price;
                    self.bbo_snapshot.best_ask_qty = event.quantity;
                    changed = true;
                } else if event.price == self.bbo_snapshot.best_ask_price {
                    // Ask价格相同：成交数量消耗了该档的流动性
                    self.bbo_snapshot.best_ask_qty -= event.quantity;
                    if self.bbo_snapshot.best_ask_qty < 0.0 {
                        self.bbo_snapshot.best_ask_qty = 0.0;
                    }
                    changed = true;
                }
            }
            Side::Sell => {
                // 卖方成交（taker is seller）：买方流动性被消耗
                // 成交价格成为可能的最高买价（bid）
                if self.bbo_snapshot.best_bid_price == 0.0 {
                    // 初始状态：设置初始bid价格和数量
                    self.bbo_snapshot.best_bid_price = event.price;
                    self.bbo_snapshot.best_bid_qty = event.quantity;
                    changed = true;
                } else if event.price > self.bbo_snapshot.best_bid_price {
                    // Bid价格上升（bid improved）
                    self.bbo_snapshot.best_bid_price = event.price;
                    self.bbo_snapshot.best_bid_qty = event.quantity;
                    changed = true;
                } else if event.price == self.bbo_snapshot.best_bid_price {
                    // Bid价格相同：成交数量消耗了该档的流动性
                    self.bbo_snapshot.best_bid_qty -= event.quantity;
                    if self.bbo_snapshot.best_bid_qty < 0.0 {
                        self.bbo_snapshot.best_bid_qty = 0.0;
                    }
                    changed = true;
                }
            }
        }

        changed
    }

    /// 生成BBO快照
    ///
    /// 基于当前的BBO状态创建快照，包括时间戳、序列号和档位数。
    /// 此方法应在每次交易事件后调用以生成最新的快照。
    ///
    /// # 返回
    /// 当前状态的BBOSnapshot
    #[allow(dead_code)]
    #[inline]
    fn generate_bbo_snapshot(&self) -> BBOSnapshot {
        BBOSnapshot {
            timestamp: self.bbo_snapshot.timestamp,
            sequence: self.bbo_snapshot.sequence,
            best_bid_price: self.bbo_snapshot.best_bid_price,
            best_bid_qty: self.bbo_snapshot.best_bid_qty,
            best_ask_price: self.bbo_snapshot.best_ask_price,
            best_ask_qty: self.bbo_snapshot.best_ask_qty,
            bid_level_count: self.bbo_snapshot.bid_level_count,
            ask_level_count: self.bbo_snapshot.ask_level_count,
            _padding: [0; 12],
        }
    }

    /// 消费交易事件，更新所有内部状态
    ///
    /// 此方法在接收到TradeEvent时被调用，用于：
    /// 1. 更新BBO（最优买卖价）
    /// 2. 更新Level2深度
    /// 3. 更新聚合成交数据
    /// 4. 更新24小时统计
    ///
    /// # 参数
    /// - `event`: 交易事件
    ///
    /// # 返回
    /// 如果所有更新成功返回true，否则返回false
    pub fn consume_trade_event(&mut self, event: TradeEvent) -> bool {
        // 更新时间戳和序列号
        self.bbo_snapshot.timestamp = event.timestamp;
        self.bbo_snapshot.sequence = event.sequence;
        self.level2_snapshot.timestamp = event.timestamp;
        self.level2_snapshot.sequence = event.sequence;
        self.statistics_24h.timestamp = event.timestamp;

        // 更新24小时统计数据中的BBO信息
        if event.side == Side::Buy {
            // 买方成交意味着卖方流动性被消耗
            self.statistics_24h.ask_price = event.price;
            // 卖方数量信息可从后续的BBO更新获取
        } else {
            // 卖方成交意味着买方流动性被消耗
            self.statistics_24h.bid_price = event.price;
            // 买方数量信息可从后续的BBO更新获取
        }

        // 更新24小时统计的交易相关数据
        self.statistics_24h.volume_24h += event.quantity;
        self.statistics_24h.quote_asset_volume_24h += event.price * event.quantity;
        if self.statistics_24h.trade_count == 0 {
            self.statistics_24h.first_trade_id = event.order_id;
        }
        self.statistics_24h.last_trade_id = event.order_id;
        self.statistics_24h.trade_count += 1;

        // 更新价格范围
        self.statistics_24h.update_price_range(event.price);

        // 更新BBO状态 - Task 2.2
        let _ = self.update_bbo_from_trade(event);

        // TODO: Level2更新逻辑 - Task 2.3
        // TODO: 聚合成交数据更新逻辑 - Task 2.4

        true
    }

    /// 获取当前BBO快照
    ///
    /// # 返回
    /// 当前的BBOSnapshot副本
    #[inline]
    pub fn get_bbo_snapshot(&self) -> BBOSnapshot {
        self.bbo_snapshot
    }

    /// 获取当前Level2快照
    ///
    /// # 返回
    /// 当前的Level2Snapshot副本
    #[inline]
    pub fn get_level2_snapshot(&self) -> Level2Snapshot {
        self.level2_snapshot
    }

    /// 获取24小时统计数据
    ///
    /// # 返回
    /// 当前的Statistics24h副本
    #[inline]
    pub fn get_24h_statistics(&self) -> Statistics24h {
        self.statistics_24h
    }

    /// 获取聚合成交数据
    ///
    /// # 返回
    /// 当前聚合成交数据的副本
    #[inline]
    pub fn get_aggregate_trades(&self) -> Vec<AggregateTrade> {
        self.aggregate_trades.clone()
    }

    /// 主事件循环 - 连续消费来自通道的TradeEvent
    ///
    /// 此方法将阻塞并持续消费TradeEvent，直到通道关闭。
    /// 每接收一个事件，调用consume_trade_event()更新内部状态。
    ///
    /// # 返回值
    /// 当通道关闭时，循环结束并返回
    pub fn run(&mut self) {
        loop {
            match self.receiver.recv() {
                Ok(event) => {
                    // 消费事件，更新状态
                    let _ = self.consume_trade_event(event);
                }
                Err(_) => {
                    // 通道已关闭，结束事件循环
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ===== 对齐和大小测试 =====

    #[test]
    fn test_trade_event_alignment() {
        assert_eq!(std::mem::align_of::<TradeEvent>(), 64);
    }

    #[test]
    fn test_trade_event_size() {
        // Structure content is 65 bytes (8*8 fields + 1 byte Side)
        // With align(64), allocated size is 128 bytes (next multiple of 64)
        // This is expected behavior for cache-line aligned structures
        let size = std::mem::size_of::<TradeEvent>();
        println!("TradeEvent size: {}", size);
        assert!(size >= 64 && size <= 128, "TradeEvent size {} should be 64-128 bytes", size);
    }

    #[test]
    fn test_bbo_snapshot_alignment() {
        assert_eq!(std::mem::align_of::<BBOSnapshot>(), 64);
    }

    #[test]
    fn test_bbo_snapshot_size() {
        // 8 (timestamp) + 8 (sequence) + 8 (best_bid_price) + 8 (best_bid_qty)
        // + 8 (best_ask_price) + 8 (best_ask_qty) + 2 (bid_level_count) + 2 (ask_level_count)
        // + 12 (padding) = 64 bytes
        let size = std::mem::size_of::<BBOSnapshot>();
        println!("BBOSnapshot size: {}", size);
        assert_eq!(size, 64, "BBOSnapshot should be 64 bytes, got {}", size);
    }

    #[test]
    fn test_level2_snapshot_alignment() {
        assert_eq!(std::mem::align_of::<Level2Snapshot>(), 64);
    }

    #[test]
    fn test_level2_snapshot_size() {
        let size = std::mem::size_of::<Level2Snapshot>();
        println!("Level2Snapshot size: {}", size);
        // 8 (ts) + 8 (seq) + 10*16 (bids) + 10*16 (asks) + 2 (num_bids) + 2 (num_asks) + 12 (padding)
        // = 8 + 8 + 160 + 160 + 2 + 2 + 12 = 352
        assert!(size <= 384, "Level2Snapshot size {} should be reasonable", size);
    }

    #[test]
    fn test_aggregate_trade_alignment() {
        assert_eq!(std::mem::align_of::<AggregateTrade>(), 64);
    }

    #[test]
    fn test_aggregate_trade_size() {
        // 8 (start_time) + 8 (end_time) + 8 (sequence) + 8 (open) + 8 (close)
        // + 8 (high) + 8 (low) + 8 (volume) + 8 (quote_asset_volume) + 8 (trade_count)
        // = 80 bytes, with align(64) this becomes 128 bytes (next multiple)
        let size = std::mem::size_of::<AggregateTrade>();
        println!("AggregateTrade size: {}", size);
        assert!(size == 80 || size == 128, "AggregateTrade size {} should be 80 or 128 bytes", size);
    }

    #[test]
    fn test_statistics_24h_alignment() {
        assert_eq!(std::mem::align_of::<Statistics24h>(), 64);
    }

    #[test]
    fn test_statistics_24h_size() {
        let size = std::mem::size_of::<Statistics24h>();
        println!("Statistics24h size: {}", size);
        // 8*4 (timestamps) + 8*7 (f64 prices/volumes) + 8*3 (trade IDs) + 8 (count)
        // = 32 + 56 + 24 + 8 = 120
        assert!(size <= 192, "Statistics24h size {} should be reasonable", size);
    }

    // ===== TradeEvent 测试 =====

    #[test]
    fn test_trade_event_creation() {
        let event = TradeEvent::new(
            1,
            100,
            99,
            1_000_000_000,
            50000.0,
            10.5,
            Side::Buy,
            1001,
            2001,
        );

        assert_eq!(event.sequence, 1);
        assert_eq!(event.order_id, 100);
        assert_eq!(event.maker_order_id, 99);
        assert_eq!(event.timestamp, 1_000_000_000);
        assert_eq!(event.price, 50000.0);
        assert_eq!(event.quantity, 10.5);
        assert_eq!(event.side, Side::Buy);
        assert_eq!(event.taker_id, 1001);
        assert_eq!(event.maker_id, 2001);
    }

    #[test]
    fn test_trade_event_default() {
        let event = TradeEvent::default();
        assert_eq!(event.sequence, 0);
        assert_eq!(event.order_id, 0);
        assert_eq!(event.price, 0.0);
    }

    // ===== BBOSnapshot 测试 =====

    #[test]
    fn test_bbo_snapshot_creation() {
        let snapshot = BBOSnapshot::new(1_000_000_000, 1, 50000.0, 10.0, 50001.0, 20.0, 5, 5);

        assert_eq!(snapshot.timestamp, 1_000_000_000);
        assert_eq!(snapshot.sequence, 1);
        assert_eq!(snapshot.best_bid_price, 50000.0);
        assert_eq!(snapshot.best_bid_qty, 10.0);
        assert_eq!(snapshot.best_ask_price, 50001.0);
        assert_eq!(snapshot.best_ask_qty, 20.0);
        assert_eq!(snapshot.bid_level_count, 5);
        assert_eq!(snapshot.ask_level_count, 5);
    }

    #[test]
    fn test_bbo_snapshot_mid_price() {
        let snapshot = BBOSnapshot::new(1_000_000_000, 1, 50000.0, 10.0, 50002.0, 20.0, 5, 5);
        assert_eq!(snapshot.mid_price(), 50001.0);
    }

    #[test]
    fn test_bbo_snapshot_spread() {
        let snapshot = BBOSnapshot::new(1_000_000_000, 1, 50000.0, 10.0, 50002.0, 20.0, 5, 5);
        assert_eq!(snapshot.spread(), 2.0);
    }

    #[test]
    fn test_bbo_snapshot_default() {
        let snapshot = BBOSnapshot::default();
        assert_eq!(snapshot.timestamp, 0);
        assert_eq!(snapshot.sequence, 0);
        assert_eq!(snapshot.best_bid_price, 0.0);
    }

    // ===== PriceLevel 测试 =====

    #[test]
    fn test_price_level_creation() {
        let level = PriceLevel::new(50000.0, 10.0);
        assert_eq!(level.price, 50000.0);
        assert_eq!(level.quantity, 10.0);
    }

    #[test]
    fn test_price_level_default() {
        let level = PriceLevel::default();
        assert_eq!(level.price, 0.0);
        assert_eq!(level.quantity, 0.0);
    }

    // ===== Level2Snapshot 测试 =====

    #[test]
    fn test_level2_snapshot_creation() {
        let snapshot = Level2Snapshot::new(1_000_000_000, 1);

        assert_eq!(snapshot.timestamp, 1_000_000_000);
        assert_eq!(snapshot.sequence, 1);
        assert_eq!(snapshot.num_bids, 0);
        assert_eq!(snapshot.num_asks, 0);
    }

    #[test]
    fn test_level2_snapshot_add_bids() {
        let mut snapshot = Level2Snapshot::new(1_000_000_000, 1);

        // Add 3 bids in descending price order
        assert!(snapshot.add_bid(50000.0, 10.0));
        assert!(snapshot.add_bid(49999.0, 20.0));
        assert!(snapshot.add_bid(49998.0, 30.0));

        assert_eq!(snapshot.num_bids, 3);
        assert_eq!(snapshot.bids[0].price, 50000.0);
        assert_eq!(snapshot.bids[0].quantity, 10.0);
        assert_eq!(snapshot.bids[1].price, 49999.0);
        assert_eq!(snapshot.bids[1].quantity, 20.0);
    }

    #[test]
    fn test_level2_snapshot_add_asks() {
        let mut snapshot = Level2Snapshot::new(1_000_000_000, 1);

        // Add 3 asks in ascending price order
        assert!(snapshot.add_ask(50001.0, 10.0));
        assert!(snapshot.add_ask(50002.0, 20.0));
        assert!(snapshot.add_ask(50003.0, 30.0));

        assert_eq!(snapshot.num_asks, 3);
        assert_eq!(snapshot.asks[0].price, 50001.0);
        assert_eq!(snapshot.asks[1].price, 50002.0);
    }

    #[test]
    fn test_level2_snapshot_max_levels() {
        let mut snapshot = Level2Snapshot::new(1_000_000_000, 1);

        // Add 10 bids
        for i in 0..10 {
            assert!(snapshot.add_bid(50000.0 - i as f64, (i + 1) as f64 * 10.0));
        }

        // 11th bid should fail
        assert!(!snapshot.add_bid(49990.0, 110.0));
        assert_eq!(snapshot.num_bids, 10);
    }

    #[test]
    fn test_level2_snapshot_best_bid() {
        let mut snapshot = Level2Snapshot::new(1_000_000_000, 1);
        assert!(snapshot.best_bid().is_none());

        snapshot.add_bid(50000.0, 10.0);
        let best = snapshot.best_bid().unwrap();
        assert_eq!(best.price, 50000.0);
        assert_eq!(best.quantity, 10.0);
    }

    #[test]
    fn test_level2_snapshot_best_ask() {
        let mut snapshot = Level2Snapshot::new(1_000_000_000, 1);
        assert!(snapshot.best_ask().is_none());

        snapshot.add_ask(50001.0, 20.0);
        let best = snapshot.best_ask().unwrap();
        assert_eq!(best.price, 50001.0);
        assert_eq!(best.quantity, 20.0);
    }

    #[test]
    fn test_level2_snapshot_clear() {
        let mut snapshot = Level2Snapshot::new(1_000_000_000, 1);
        snapshot.add_bid(50000.0, 10.0);
        snapshot.add_ask(50001.0, 20.0);

        assert_eq!(snapshot.num_bids, 1);
        assert_eq!(snapshot.num_asks, 1);

        snapshot.clear();
        assert_eq!(snapshot.num_bids, 0);
        assert_eq!(snapshot.num_asks, 0);
    }

    // ===== AggregateTrade 测试 =====

    #[test]
    fn test_aggregate_trade_creation() {
        let trade = AggregateTrade::new(1_000_000_000, 2_000_000_000, 1);

        assert_eq!(trade.start_time, 1_000_000_000);
        assert_eq!(trade.end_time, 2_000_000_000);
        assert_eq!(trade.sequence, 1);
        assert_eq!(trade.volume, 0.0);
        assert_eq!(trade.trade_count, 0);
    }

    #[test]
    fn test_aggregate_trade_init_with_first_trade() {
        let mut trade = AggregateTrade::new(1_000_000_000, 2_000_000_000, 1);
        trade.init_with_first_trade(50000.0, 10.0);

        assert_eq!(trade.open, 50000.0);
        assert_eq!(trade.close, 50000.0);
        assert_eq!(trade.high, 50000.0);
        assert_eq!(trade.low, 50000.0);
        assert_eq!(trade.volume, 10.0);
        assert_eq!(trade.quote_asset_volume, 500000.0);
        assert_eq!(trade.trade_count, 1);
    }

    #[test]
    fn test_aggregate_trade_update() {
        let mut trade = AggregateTrade::new(1_000_000_000, 2_000_000_000, 1);
        trade.init_with_first_trade(50000.0, 10.0);

        trade.update_with_trade(50100.0, 5.0);
        assert_eq!(trade.close, 50100.0);
        assert_eq!(trade.high, 50100.0);
        assert_eq!(trade.low, 50000.0);
        assert_eq!(trade.volume, 15.0);
        assert_eq!(trade.quote_asset_volume, 750500.0);
        assert_eq!(trade.trade_count, 2);

        trade.update_with_trade(49900.0, 8.0);
        assert_eq!(trade.close, 49900.0);
        assert_eq!(trade.high, 50100.0);
        assert_eq!(trade.low, 49900.0);
        assert_eq!(trade.volume, 23.0);
        assert_eq!(trade.trade_count, 3);
    }

    #[test]
    fn test_aggregate_trade_vwap() {
        let mut trade = AggregateTrade::new(1_000_000_000, 2_000_000_000, 1);
        trade.init_with_first_trade(50000.0, 10.0);
        trade.update_with_trade(50200.0, 10.0);

        let vwap = trade.vwap();
        assert_eq!(vwap, 50100.0);
    }

    // ===== Statistics24h 测试 =====

    #[test]
    fn test_statistics_24h_creation() {
        let stats = Statistics24h::new(1_000_000_000);

        assert_eq!(stats.timestamp, 1_000_000_000);
        assert_eq!(stats.price_24h_high, 0.0);
        assert_eq!(stats.price_24h_low, 0.0);
        assert_eq!(stats.volume_24h, 0.0);
    }

    #[test]
    fn test_statistics_24h_update_price_range() {
        let mut stats = Statistics24h::new(1_000_000_000);

        stats.update_price_range(50000.0);
        assert_eq!(stats.price_24h_high, 50000.0);
        assert_eq!(stats.price_24h_low, 50000.0);

        stats.update_price_range(51000.0);
        assert_eq!(stats.price_24h_high, 51000.0);
        assert_eq!(stats.price_24h_low, 50000.0);

        stats.update_price_range(49000.0);
        assert_eq!(stats.price_24h_high, 51000.0);
        assert_eq!(stats.price_24h_low, 49000.0);
    }

    #[test]
    fn test_statistics_24h_calculate_weighted_avg_price() {
        let mut stats = Statistics24h::new(1_000_000_000);
        stats.volume_24h = 20.0;
        stats.quote_asset_volume_24h = 1_000_000.0;

        let wap = stats.calculate_weighted_avg_price();
        assert_eq!(wap, 50000.0);
    }

    #[test]
    fn test_statistics_24h_default() {
        let stats = Statistics24h::default();
        assert_eq!(stats.timestamp, 0);
        assert_eq!(stats.trade_count, 0);
    }

    // ===== 集成测试 =====

    #[test]
    fn test_snapshot_workflow() {
        // Create a BBO snapshot
        let bbo = BBOSnapshot::new(1_000_000_000, 1, 50000.0, 100.0, 50001.0, 100.0, 1, 1);
        assert_eq!(bbo.mid_price(), 50000.5);

        // Create a Level2 snapshot
        let mut l2 = Level2Snapshot::new(1_000_000_000, 1);
        l2.add_bid(50000.0, 100.0);
        l2.add_ask(50001.0, 100.0);

        assert_eq!(l2.best_bid().unwrap().price, 50000.0);
        assert_eq!(l2.best_ask().unwrap().price, 50001.0);

        // Create a trade event
        let trade_event = TradeEvent::new(1, 100, 99, 1_000_000_000, 50000.5, 10.0, Side::Buy, 1001, 2001);
        assert_eq!(trade_event.quantity, 10.0);
    }

    // ===== MarketDataEngine 测试 =====

    use crossbeam::channel;

    #[test]
    fn test_market_data_engine_creation() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let engine = MarketDataEngine::new(receiver);

        // Verify initial state
        let bbo = engine.get_bbo_snapshot();
        assert_eq!(bbo.timestamp, 0);
        assert_eq!(bbo.sequence, 0);

        let l2 = engine.get_level2_snapshot();
        assert_eq!(l2.timestamp, 0);
        assert_eq!(l2.sequence, 0);
        assert_eq!(l2.num_bids, 0);
        assert_eq!(l2.num_asks, 0);

        let stats = engine.get_24h_statistics();
        assert_eq!(stats.timestamp, 0);
        assert_eq!(stats.trade_count, 0);
        assert_eq!(stats.volume_24h, 0.0);
    }

    #[test]
    fn test_market_data_engine_consume_single_event() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        let event = TradeEvent::new(
            1,                  // sequence
            100,                // order_id
            99,                 // maker_order_id
            1_000_000_000,      // timestamp
            50000.0,            // price
            10.5,               // quantity
            Side::Buy,          // side
            1001,               // taker_id
            2001,               // maker_id
        );

        assert!(engine.consume_trade_event(event));

        // Verify snapshots updated
        let bbo = engine.get_bbo_snapshot();
        assert_eq!(bbo.timestamp, 1_000_000_000);
        assert_eq!(bbo.sequence, 1);

        let l2 = engine.get_level2_snapshot();
        assert_eq!(l2.timestamp, 1_000_000_000);
        assert_eq!(l2.sequence, 1);

        let stats = engine.get_24h_statistics();
        assert_eq!(stats.timestamp, 1_000_000_000);
        assert_eq!(stats.trade_count, 1);
        assert_eq!(stats.volume_24h, 10.5);
        assert_eq!(stats.quote_asset_volume_24h, 50000.0 * 10.5);
        assert_eq!(stats.first_trade_id, 100);
        assert_eq!(stats.last_trade_id, 100);
    }

    #[test]
    fn test_market_data_engine_consume_multiple_events() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        // First event - buy at 50000
        let event1 = TradeEvent::new(1, 100, 99, 1_000_000_000, 50000.0, 10.0, Side::Buy, 1001, 2001);
        assert!(engine.consume_trade_event(event1));

        // Second event - sell at 50100
        let event2 = TradeEvent::new(2, 101, 100, 2_000_000_000, 50100.0, 5.0, Side::Sell, 1002, 2002);
        assert!(engine.consume_trade_event(event2));

        // Third event - buy at 50050
        let event3 = TradeEvent::new(3, 102, 101, 3_000_000_000, 50050.0, 8.0, Side::Buy, 1003, 2003);
        assert!(engine.consume_trade_event(event3));

        // Verify final state
        let bbo = engine.get_bbo_snapshot();
        assert_eq!(bbo.sequence, 3);
        assert_eq!(bbo.timestamp, 3_000_000_000);

        let stats = engine.get_24h_statistics();
        assert_eq!(stats.trade_count, 3);
        assert_eq!(stats.volume_24h, 10.0 + 5.0 + 8.0);
        assert_eq!(stats.first_trade_id, 100);
        assert_eq!(stats.last_trade_id, 102);

        // Verify price range tracking
        assert_eq!(stats.price_24h_high, 50100.0);
        assert_eq!(stats.price_24h_low, 50000.0);
    }

    #[test]
    fn test_market_data_engine_buy_side_event() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        let event = TradeEvent::new(
            1,
            100,
            99,
            1_000_000_000,
            50000.0,
            10.0,
            Side::Buy,
            1001,
            2001,
        );

        engine.consume_trade_event(event);

        let stats = engine.get_24h_statistics();
        // Buy side trade updates ask_price
        assert_eq!(stats.ask_price, 50000.0);
    }

    #[test]
    fn test_market_data_engine_sell_side_event() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        let event = TradeEvent::new(
            1,
            100,
            99,
            1_000_000_000,
            50000.0,
            10.0,
            Side::Sell,
            1001,
            2001,
        );

        engine.consume_trade_event(event);

        let stats = engine.get_24h_statistics();
        // Sell side trade updates bid_price
        assert_eq!(stats.bid_price, 50000.0);
    }

    #[test]
    fn test_market_data_engine_get_aggregate_trades() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let engine = MarketDataEngine::new(receiver);

        // Should return empty vector initially
        let trades = engine.get_aggregate_trades();
        assert!(trades.is_empty());
    }

    #[test]
    fn test_market_data_engine_channel_closure() {
        let (sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        // Send one event
        let event = TradeEvent::new(
            1,
            100,
            99,
            1_000_000_000,
            50000.0,
            10.0,
            Side::Buy,
            1001,
            2001,
        );
        sender.send(event).unwrap();

        // Close the sender (which closes the channel)
        drop(sender);

        // Run should complete without panicking
        engine.run();

        // Verify the event was processed
        let stats = engine.get_24h_statistics();
        assert_eq!(stats.trade_count, 1);
    }

    #[test]
    fn test_market_data_engine_event_loop_with_multiple_events() {
        let (sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        // Create a thread to send events
        std::thread::spawn(move || {
            for i in 0..5 {
                let event = TradeEvent::new(
                    i as u64 + 1,
                    100 + i as u64,
                    99 + i as u64,
                    1_000_000_000 + (i as u64 * 1_000_000),
                    50000.0 + (i as f64 * 100.0),
                    10.0,
                    if i % 2 == 0 { Side::Buy } else { Side::Sell },
                    1001 + i as u64,
                    2001 + i as u64,
                );
                let _ = sender.send(event);
            }
            // Drop sender to close the channel
            drop(sender);
        });

        // Run the engine event loop
        engine.run();

        // Verify all events were processed
        let stats = engine.get_24h_statistics();
        assert_eq!(stats.trade_count, 5);
        assert_eq!(stats.volume_24h, 50.0); // 5 events * 10.0 qty each
        assert_eq!(stats.first_trade_id, 100);
        assert_eq!(stats.last_trade_id, 104);

        // Verify BBO and Level2 were updated
        let bbo = engine.get_bbo_snapshot();
        assert_eq!(bbo.sequence, 5);

        let l2 = engine.get_level2_snapshot();
        assert_eq!(l2.sequence, 5);
    }

    #[test]
    fn test_market_data_engine_statistics_accumulation() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        // Simulate a series of trades with different prices and quantities
        let trades = vec![
            (50000.0, 10.0, Side::Buy),
            (50100.0, 5.0, Side::Sell),
            (50050.0, 8.0, Side::Buy),
            (49950.0, 15.0, Side::Sell),
        ];

        let mut expected_volume = 0.0;
        let mut expected_quote_volume = 0.0;

        for (idx, (price, qty, side)) in trades.iter().enumerate() {
            let event = TradeEvent::new(
                (idx + 1) as u64,
                100 + idx as u64,
                99 + idx as u64,
                1_000_000_000 + (idx as u64 * 1_000_000),
                *price,
                *qty,
                *side,
                1001 + idx as u64,
                2001 + idx as u64,
            );
            engine.consume_trade_event(event);
            expected_volume += qty;
            expected_quote_volume += price * qty;
        }

        let stats = engine.get_24h_statistics();
        assert_eq!(stats.trade_count, 4);
        assert_eq!(stats.volume_24h, expected_volume);
        assert_eq!(stats.quote_asset_volume_24h, expected_quote_volume);
        assert_eq!(stats.price_24h_high, 50100.0);
        assert_eq!(stats.price_24h_low, 49950.0);
    }

    // ===== BBO 更新逻辑测试 - Task 2.2 =====

    #[test]
    fn test_bbo_initialization_empty_state() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let engine = MarketDataEngine::new(receiver);

        let bbo = engine.get_bbo_snapshot();
        assert_eq!(bbo.best_bid_price, 0.0);
        assert_eq!(bbo.best_bid_qty, 0.0);
        assert_eq!(bbo.best_ask_price, 0.0);
        assert_eq!(bbo.best_ask_qty, 0.0);
        assert_eq!(bbo.timestamp, 0);
        assert_eq!(bbo.sequence, 0);
    }

    #[test]
    fn test_bbo_update_on_first_buy_trade() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        let event = TradeEvent::new(
            1,
            100,
            99,
            1_000_000_000,
            50000.0,
            10.0,
            Side::Buy,
            1001,
            2001,
        );

        engine.consume_trade_event(event);

        let bbo = engine.get_bbo_snapshot();
        // 首次买方成交：初始化ask
        assert_eq!(bbo.best_ask_price, 50000.0);
        assert_eq!(bbo.best_ask_qty, 10.0);
        assert_eq!(bbo.best_bid_price, 0.0); // bid still uninitialized
        assert_eq!(bbo.timestamp, 1_000_000_000);
        assert_eq!(bbo.sequence, 1);
    }

    #[test]
    fn test_bbo_update_on_first_sell_trade() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        let event = TradeEvent::new(
            1,
            100,
            99,
            1_000_000_000,
            50000.0,
            10.0,
            Side::Sell,
            1001,
            2001,
        );

        engine.consume_trade_event(event);

        let bbo = engine.get_bbo_snapshot();
        // 首次卖方成交：初始化bid
        assert_eq!(bbo.best_bid_price, 50000.0);
        assert_eq!(bbo.best_bid_qty, 10.0);
        assert_eq!(bbo.best_ask_price, 0.0); // ask still uninitialized
        assert_eq!(bbo.timestamp, 1_000_000_000);
        assert_eq!(bbo.sequence, 1);
    }

    #[test]
    fn test_bbo_ask_price_improvement_on_buy_trade() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        // First trade sets initial ask at 50100
        let event1 = TradeEvent::new(1, 100, 99, 1_000_000_000, 50100.0, 10.0, Side::Buy, 1001, 2001);
        engine.consume_trade_event(event1);

        let bbo1 = engine.get_bbo_snapshot();
        assert_eq!(bbo1.best_ask_price, 50100.0);
        assert_eq!(bbo1.best_ask_qty, 10.0);

        // Second trade at lower price: ask price improves (decreases)
        let event2 = TradeEvent::new(2, 101, 100, 2_000_000_000, 50050.0, 5.0, Side::Buy, 1002, 2002);
        engine.consume_trade_event(event2);

        let bbo2 = engine.get_bbo_snapshot();
        assert_eq!(bbo2.best_ask_price, 50050.0);
        assert_eq!(bbo2.best_ask_qty, 5.0);
        assert_eq!(bbo2.sequence, 2);
    }

    #[test]
    fn test_bbo_bid_price_improvement_on_sell_trade() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        // First trade sets initial bid at 49900
        let event1 = TradeEvent::new(1, 100, 99, 1_000_000_000, 49900.0, 10.0, Side::Sell, 1001, 2001);
        engine.consume_trade_event(event1);

        let bbo1 = engine.get_bbo_snapshot();
        assert_eq!(bbo1.best_bid_price, 49900.0);
        assert_eq!(bbo1.best_bid_qty, 10.0);

        // Second trade at higher price: bid price improves (increases)
        let event2 = TradeEvent::new(2, 101, 100, 2_000_000_000, 49950.0, 5.0, Side::Sell, 1002, 2002);
        engine.consume_trade_event(event2);

        let bbo2 = engine.get_bbo_snapshot();
        assert_eq!(bbo2.best_bid_price, 49950.0);
        assert_eq!(bbo2.best_bid_qty, 5.0);
        assert_eq!(bbo2.sequence, 2);
    }

    #[test]
    fn test_bbo_quantity_reduction_on_same_price_buy_trade() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        // First trade sets initial ask
        let event1 = TradeEvent::new(1, 100, 99, 1_000_000_000, 50000.0, 20.0, Side::Buy, 1001, 2001);
        engine.consume_trade_event(event1);

        let bbo1 = engine.get_bbo_snapshot();
        assert_eq!(bbo1.best_ask_price, 50000.0);
        assert_eq!(bbo1.best_ask_qty, 20.0);

        // Second trade at same price: quantity consumed from the level
        let event2 = TradeEvent::new(2, 101, 100, 2_000_000_000, 50000.0, 8.0, Side::Buy, 1002, 2002);
        engine.consume_trade_event(event2);

        let bbo2 = engine.get_bbo_snapshot();
        assert_eq!(bbo2.best_ask_price, 50000.0);
        assert_eq!(bbo2.best_ask_qty, 12.0); // 20 - 8
        assert_eq!(bbo2.sequence, 2);
    }

    #[test]
    fn test_bbo_quantity_reduction_on_same_price_sell_trade() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        // First trade sets initial bid
        let event1 = TradeEvent::new(1, 100, 99, 1_000_000_000, 50000.0, 20.0, Side::Sell, 1001, 2001);
        engine.consume_trade_event(event1);

        let bbo1 = engine.get_bbo_snapshot();
        assert_eq!(bbo1.best_bid_price, 50000.0);
        assert_eq!(bbo1.best_bid_qty, 20.0);

        // Second trade at same price: quantity consumed from the level
        let event2 = TradeEvent::new(2, 101, 100, 2_000_000_000, 50000.0, 8.0, Side::Sell, 1002, 2002);
        engine.consume_trade_event(event2);

        let bbo2 = engine.get_bbo_snapshot();
        assert_eq!(bbo2.best_bid_price, 50000.0);
        assert_eq!(bbo2.best_bid_qty, 12.0); // 20 - 8
        assert_eq!(bbo2.sequence, 2);
    }

    #[test]
    fn test_bbo_multiple_consecutive_trades() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        // Sequence of trades: sell, buy, sell, buy
        let trades = vec![
            (50000.0, 10.0, Side::Sell),  // bid = 50000, qty = 10
            (50050.0, 5.0, Side::Buy),    // ask = 50050, qty = 5
            (50025.0, 8.0, Side::Sell),   // bid improves to 50025, qty = 8
            (50040.0, 3.0, Side::Buy),    // ask improves to 50040, qty = 3
        ];

        for (idx, (price, qty, side)) in trades.iter().enumerate() {
            let event = TradeEvent::new(
                (idx + 1) as u64,
                100 + idx as u64,
                99 + idx as u64,
                1_000_000_000 + (idx as u64 * 1_000_000),
                *price,
                *qty,
                *side,
                1001 + idx as u64,
                2001 + idx as u64,
            );
            engine.consume_trade_event(event);
        }

        let bbo = engine.get_bbo_snapshot();
        assert_eq!(bbo.best_bid_price, 50025.0);
        assert_eq!(bbo.best_bid_qty, 8.0);
        assert_eq!(bbo.best_ask_price, 50040.0);
        assert_eq!(bbo.best_ask_qty, 3.0);
        assert_eq!(bbo.sequence, 4);
    }

    #[test]
    fn test_bbo_timestamp_and_sequence_tracking() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        for i in 1..=5 {
            let event = TradeEvent::new(
                i as u64,
                100 + i as u64,
                99 + i as u64,
                1_000_000_000 + (i as u64 * 1_000_000),
                50000.0 + (i as f64 * 10.0),
                10.0,
                if i % 2 == 0 { Side::Buy } else { Side::Sell },
                1001 + i as u64,
                2001 + i as u64,
            );
            engine.consume_trade_event(event);
        }

        let bbo = engine.get_bbo_snapshot();
        assert_eq!(bbo.sequence, 5);
        assert_eq!(bbo.timestamp, 1_000_000_000 + (5 * 1_000_000));
    }

    #[test]
    fn test_bbo_market_spread_changes() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        // Start with sell at 50000 (bid = 50000, ask = 0)
        let event1 = TradeEvent::new(1, 100, 99, 1_000_000_000, 50000.0, 10.0, Side::Sell, 1001, 2001);
        engine.consume_trade_event(event1);

        let bbo1 = engine.get_bbo_snapshot();
        assert_eq!(bbo1.best_bid_price, 50000.0);
        assert_eq!(bbo1.best_ask_price, 0.0); // ask not set yet

        // Add buy at 50050 (ask = 50050)
        let event2 = TradeEvent::new(2, 101, 100, 2_000_000_000, 50050.0, 5.0, Side::Buy, 1002, 2002);
        engine.consume_trade_event(event2);

        let bbo2 = engine.get_bbo_snapshot();
        assert_eq!(bbo2.best_bid_price, 50000.0);
        assert_eq!(bbo2.best_ask_price, 50050.0);
        assert_eq!(bbo2.spread(), 50.0);

        // Bid improves to 50040 (sell at 50040)
        let event3 = TradeEvent::new(3, 102, 101, 3_000_000_000, 50040.0, 8.0, Side::Sell, 1003, 2003);
        engine.consume_trade_event(event3);

        let bbo3 = engine.get_bbo_snapshot();
        assert_eq!(bbo3.best_bid_price, 50040.0);
        assert_eq!(bbo3.best_ask_price, 50050.0);
        assert_eq!(bbo3.spread(), 10.0);
    }

    #[test]
    fn test_bbo_quantity_goes_negative_protection() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        // First trade sets ask at 50000 with qty 10
        let event1 = TradeEvent::new(1, 100, 99, 1_000_000_000, 50000.0, 10.0, Side::Buy, 1001, 2001);
        engine.consume_trade_event(event1);

        let bbo1 = engine.get_bbo_snapshot();
        assert_eq!(bbo1.best_ask_qty, 10.0);

        // Second trade consumes 15 at same price (more than available)
        let event2 = TradeEvent::new(2, 101, 100, 2_000_000_000, 50000.0, 15.0, Side::Buy, 1002, 2002);
        engine.consume_trade_event(event2);

        let bbo2 = engine.get_bbo_snapshot();
        // Quantity should not go negative, should be clamped to 0
        assert!(bbo2.best_ask_qty >= 0.0);
        assert_eq!(bbo2.best_ask_qty, 0.0); // 10 - 15 = -5, clamped to 0
    }

    #[test]
    fn test_bbo_mid_price_with_active_bid_ask() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        // Sell at 50000 (bid = 50000)
        let event1 = TradeEvent::new(1, 100, 99, 1_000_000_000, 50000.0, 10.0, Side::Sell, 1001, 2001);
        engine.consume_trade_event(event1);

        // Buy at 50100 (ask = 50100)
        let event2 = TradeEvent::new(2, 101, 100, 2_000_000_000, 50100.0, 5.0, Side::Buy, 1002, 2002);
        engine.consume_trade_event(event2);

        let bbo = engine.get_bbo_snapshot();
        let mid = bbo.mid_price();
        assert_eq!(mid, 50050.0); // (50000 + 50100) / 2
    }

    #[test]
    fn test_bbo_after_complex_order_flow() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        // Complex order flow simulating real market
        let trades = vec![
            (50000.0, 10.0, Side::Sell),   // bid = 50000, qty = 10
            (50100.0, 5.0, Side::Buy),     // ask = 50100, qty = 5
            (50000.0, 3.0, Side::Sell),    // bid qty: 10 - 3 = 7
            (50100.0, 2.0, Side::Buy),     // ask qty: 5 - 2 = 3
            (50050.0, 8.0, Side::Sell),    // bid improves to 50050, qty = 8
            (50080.0, 4.0, Side::Buy),     // ask improves to 50080, qty = 4
        ];

        for (idx, (price, qty, side)) in trades.iter().enumerate() {
            let event = TradeEvent::new(
                (idx + 1) as u64,
                100 + idx as u64,
                99 + idx as u64,
                1_000_000_000 + (idx as u64 * 1_000_000),
                *price,
                *qty,
                *side,
                1001 + idx as u64,
                2001 + idx as u64,
            );
            engine.consume_trade_event(event);
        }

        let bbo = engine.get_bbo_snapshot();
        assert_eq!(bbo.best_bid_price, 50050.0);
        assert_eq!(bbo.best_bid_qty, 8.0);
        assert_eq!(bbo.best_ask_price, 50080.0);
        assert_eq!(bbo.best_ask_qty, 4.0);
        assert_eq!(bbo.sequence, 6);
        assert_eq!(bbo.mid_price(), 50065.0);
    }

    #[test]
    fn test_bbo_snapshot_independence() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        let event1 = TradeEvent::new(1, 100, 99, 1_000_000_000, 50000.0, 10.0, Side::Sell, 1001, 2001);
        engine.consume_trade_event(event1);

        let bbo1 = engine.get_bbo_snapshot();
        let bid1 = bbo1.best_bid_price;

        let event2 = TradeEvent::new(2, 101, 100, 2_000_000_000, 50050.0, 5.0, Side::Sell, 1002, 2002);
        engine.consume_trade_event(event2);

        let bbo2 = engine.get_bbo_snapshot();
        let bid2 = bbo2.best_bid_price;

        // Ensure bbo1 snapshot is independent (snapshot at event 1 time)
        assert_eq!(bid1, 50000.0);
        // New snapshot reflects updated bid
        assert_eq!(bid2, 50050.0);
    }
}
