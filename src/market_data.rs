//! 市场数据结构 - 用于市场数据引擎和快照发送
//!
//! 本模块定义了高频交易系统中使用的所有市场数据结构。
//! 所有结构都采用64字节对齐，以优化缓存性能。

use crate::order::Side;
use rtrb::{Consumer, Producer};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

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

/// 时间桶类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketType {
    OneSecond,      // 1秒桶
    FiveSeconds,    // 5秒桶
    OneMinute,      // 1分钟桶
}

impl BucketType {
    /// 获取桶的时间窗口（纳秒）
    pub fn window_nanos(&self) -> u64 {
        match self {
            BucketType::OneSecond => 1_000_000_000,      // 1秒 = 1e9纳秒
            BucketType::FiveSeconds => 5_000_000_000,    // 5秒 = 5e9纳秒
            BucketType::OneMinute => 60_000_000_000,     // 1分钟 = 6e10纳秒
        }
    }
}

/// 时间桶窗口 - 跟踪活跃的时间窗口
#[derive(Debug, Clone, Copy)]
pub struct AggregateTradeWindow {
    pub bucket_type: BucketType,
    pub start_time: u64,
    pub end_time: u64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub quote_asset_volume: f64,
    pub trade_count: u32,
    pub sequence: u64,
    pub has_trades: bool,  // 是否有成交
}

impl AggregateTradeWindow {
    /// 创建新的时间桶窗口
    pub fn new(bucket_type: BucketType, start_time: u64, sequence: u64) -> Self {
        let window_size = bucket_type.window_nanos();
        Self {
            bucket_type,
            start_time,
            end_time: start_time + window_size,
            open: 0.0,
            high: 0.0,
            low: 0.0,
            close: 0.0,
            volume: 0.0,
            quote_asset_volume: 0.0,
            trade_count: 0,
            sequence,
            has_trades: false,
        }
    }

    /// 检查时间是否在这个桶的窗口内
    #[inline]
    pub fn contains_time(&self, timestamp: u64) -> bool {
        timestamp < self.end_time
    }

    /// 用第一笔成交初始化OHLC
    #[inline]
    pub fn init_with_first_trade(&mut self, price: f64, quantity: f64) {
        self.open = price;
        self.close = price;
        self.high = price;
        self.low = price;
        self.volume = quantity;
        self.quote_asset_volume = price * quantity;
        self.trade_count = 1;
        self.has_trades = true;
    }

    /// 用成交更新此窗口
    #[inline]
    pub fn update_with_trade(&mut self, price: f64, quantity: f64) {
        self.close = price;
        self.high = self.high.max(price);
        self.low = self.low.min(price);
        self.volume += quantity;
        self.quote_asset_volume += price * quantity;
        self.trade_count += 1;
    }

    /// 将此窗口转换为AggregateTrade
    pub fn to_aggregate_trade(&self) -> AggregateTrade {
        AggregateTrade {
            start_time: self.start_time,
            end_time: self.end_time,
            sequence: self.sequence,
            open: self.open,
            close: self.close,
            high: self.high,
            low: self.low,
            volume: self.volume,
            quote_asset_volume: self.quote_asset_volume,
            trade_count: self.trade_count as u64,
        }
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

/// 已发布快照 - 定时器生成的市场数据快照
///
/// 包含在1ms定时器间隔处生成的所有市场数据类型：
/// - BBO（最优买卖价）
/// - Level2深度（前10档）
/// - 聚合成交数据（当前活跃的时间桶）
/// - 24小时统计数据
///
/// 用于定期向Aeron通道发布市场数据（Phase 3.2）
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy)]
pub struct PublishedSnapshot {
    pub timestamp: u64,              // 快照时间戳（纳秒）
    pub sequence: u64,               // 快照序列号
    pub bbo: BBOSnapshot,            // 最优买卖价快照
    pub level2: Level2Snapshot,      // Level2深度快照
    pub current_agg_1s: AggregateTrade,   // 当前活跃的1秒聚合成交
    pub current_agg_5s: AggregateTrade,   // 当前活跃的5秒聚合成交
    pub current_agg_1m: AggregateTrade,   // 当前活跃的1分钟聚合成交
    pub stats_24h: Statistics24h,    // 24小时统计数据
}

impl PublishedSnapshot {
    /// 创建新的已发布快照
    pub fn new(
        timestamp: u64,
        sequence: u64,
        bbo: BBOSnapshot,
        level2: Level2Snapshot,
        current_agg_1s: AggregateTrade,
        current_agg_5s: AggregateTrade,
        current_agg_1m: AggregateTrade,
        stats_24h: Statistics24h,
    ) -> Self {
        Self {
            timestamp,
            sequence,
            bbo,
            level2,
            current_agg_1s,
            current_agg_5s,
            current_agg_1m,
            stats_24h,
        }
    }
}

impl Default for PublishedSnapshot {
    fn default() -> Self {
        Self {
            timestamp: 0,
            sequence: 0,
            bbo: BBOSnapshot::default(),
            level2: Level2Snapshot::default(),
            current_agg_1s: AggregateTrade::default(),
            current_agg_5s: AggregateTrade::default(),
            current_agg_1m: AggregateTrade::default(),
            stats_24h: Statistics24h::default(),
        }
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
    /// 当前BBO快照
    bbo_snapshot: BBOSnapshot,
    /// 当前Level2快照
    level2_snapshot: Level2Snapshot,
    /// 24小时统计数据
    statistics_24h: Statistics24h,
    /// 活跃的1秒时间桶
    active_1s_bucket: AggregateTradeWindow,
    /// 活跃的5秒时间桶
    active_5s_bucket: AggregateTradeWindow,
    /// 活跃的1分钟时间桶
    active_1m_bucket: AggregateTradeWindow,
    /// 完成的聚合成交历史（滚动窗口，保留最新10个）
    completed_1s_trades: Vec<AggregateTrade>,
    completed_5s_trades: Vec<AggregateTrade>,
    completed_1m_trades: Vec<AggregateTrade>,
    /// 最大历史记录数
    max_history: usize,
}

impl MarketDataEngine {
    /// 创建新的市场数据引擎
    ///
    /// # 返回
    /// 初始化完毕的MarketDataEngine实例
    pub fn new() -> Self {
        let initial_time = 0u64;
        Self {
            bbo_snapshot: BBOSnapshot::default(),
            level2_snapshot: Level2Snapshot::default(),
            statistics_24h: Statistics24h::default(),
            active_1s_bucket: AggregateTradeWindow::new(BucketType::OneSecond, initial_time, 0),
            active_5s_bucket: AggregateTradeWindow::new(BucketType::FiveSeconds, initial_time, 0),
            active_1m_bucket: AggregateTradeWindow::new(BucketType::OneMinute, initial_time, 0),
            completed_1s_trades: Vec::new(),
            completed_5s_trades: Vec::new(),
            completed_1m_trades: Vec::new(),
            max_history: 10,
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

    /// 从交易事件更新Level2深度档位
    ///
    /// 基于交易事件的价格和数量，更新维护的Level2档位列表。
    /// 对于每笔交易：
    /// - 如果是买方成交：找到对应卖价档位并减少数量
    /// - 如果是卖方成交：找到对应买价档位并减少数量
    /// - 如果该档位数量降至0，则从列表中移除
    /// - 保持买档（降序）和卖档（升序）的排序
    ///
    /// # 参数
    /// - `event`: 交易事件
    ///
    /// # 返回
    /// 如果Level2状态发生变化返回true，否则返回false
    #[inline]
    fn update_level2_from_trade(&mut self, event: TradeEvent) -> bool {
        let mut changed = false;

        match event.side {
            Side::Buy => {
                // 买方成交：更新卖方档位
                // 在asks中查找event.price的档位
                for i in 0..self.level2_snapshot.num_asks as usize {
                    if (self.level2_snapshot.asks[i].price - event.price).abs() < 1e-10 {
                        // 找到相同价格的档位，减少数量
                        self.level2_snapshot.asks[i].quantity -= event.quantity;
                        if self.level2_snapshot.asks[i].quantity < 1e-10 {
                            // 数量降至0，从列表中移除
                            for j in i..self.level2_snapshot.num_asks as usize - 1 {
                                self.level2_snapshot.asks[j] = self.level2_snapshot.asks[j + 1];
                            }
                            // 清除最后一个元素
                            self.level2_snapshot.asks[self.level2_snapshot.num_asks as usize - 1] =
                                PriceLevel::default();
                            self.level2_snapshot.num_asks -= 1;
                        }
                        changed = true;
                        break;
                    }
                }

                // 如果在现有档位中没有找到该价格，则添加为新档位
                if !changed && self.level2_snapshot.num_asks < 10 {
                    // 在保持升序的情况下，找到插入位置
                    let mut insert_pos = self.level2_snapshot.num_asks as usize;
                    for i in 0..self.level2_snapshot.num_asks as usize {
                        if event.price < self.level2_snapshot.asks[i].price {
                            insert_pos = i;
                            break;
                        }
                    }

                    // 向后移动元素
                    for i in (insert_pos..self.level2_snapshot.num_asks as usize).rev() {
                        self.level2_snapshot.asks[i + 1] = self.level2_snapshot.asks[i];
                    }

                    // 插入新档位
                    self.level2_snapshot.asks[insert_pos] = PriceLevel::new(event.price, event.quantity);
                    self.level2_snapshot.num_asks += 1;
                    changed = true;
                }
            }
            Side::Sell => {
                // 卖方成交：更新买方档位
                // 在bids中查找event.price的档位
                for i in 0..self.level2_snapshot.num_bids as usize {
                    if (self.level2_snapshot.bids[i].price - event.price).abs() < 1e-10 {
                        // 找到相同价格的档位，减少数量
                        self.level2_snapshot.bids[i].quantity -= event.quantity;
                        if self.level2_snapshot.bids[i].quantity < 1e-10 {
                            // 数量降至0，从列表中移除
                            for j in i..self.level2_snapshot.num_bids as usize - 1 {
                                self.level2_snapshot.bids[j] = self.level2_snapshot.bids[j + 1];
                            }
                            // 清除最后一个元素
                            self.level2_snapshot.bids[self.level2_snapshot.num_bids as usize - 1] =
                                PriceLevel::default();
                            self.level2_snapshot.num_bids -= 1;
                        }
                        changed = true;
                        break;
                    }
                }

                // 如果在现有档位中没有找到该价格，则添加为新档位
                if !changed && self.level2_snapshot.num_bids < 10 {
                    // 在保持降序的情况下，找到插入位置
                    let mut insert_pos = self.level2_snapshot.num_bids as usize;
                    for i in 0..self.level2_snapshot.num_bids as usize {
                        if event.price > self.level2_snapshot.bids[i].price {
                            insert_pos = i;
                            break;
                        }
                    }

                    // 向后移动元素
                    for i in (insert_pos..self.level2_snapshot.num_bids as usize).rev() {
                        self.level2_snapshot.bids[i + 1] = self.level2_snapshot.bids[i];
                    }

                    // 插入新档位
                    self.level2_snapshot.bids[insert_pos] = PriceLevel::new(event.price, event.quantity);
                    self.level2_snapshot.num_bids += 1;
                    changed = true;
                }
            }
        }

        changed
    }

    /// 生成Level2快照
    ///
    /// 基于当前的Level2状态创建快照副本。
    /// 此方法应在每次Level2更新后调用以生成最新的快照。
    ///
    /// # 返回
    /// 当前状态的Level2Snapshot副本
    #[allow(dead_code)]
    #[inline]
    fn generate_level2_snapshot(&self) -> Level2Snapshot {
        Level2Snapshot {
            timestamp: self.level2_snapshot.timestamp,
            sequence: self.level2_snapshot.sequence,
            bids: self.level2_snapshot.bids,
            asks: self.level2_snapshot.asks,
            num_bids: self.level2_snapshot.num_bids,
            num_asks: self.level2_snapshot.num_asks,
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

        // 更新Level2状态 - Task 2.3
        let _ = self.update_level2_from_trade(event);

        // 更新聚合成交数据 - Task 2.4
        self.update_aggregate_trades_from_event(event);

        true
    }

    /// 从交易事件更新聚合成交数据
    ///
    /// 更新所有活跃的时间桶（1s, 5s, 1m）。如果成交时间超过桶的结束时间，
    /// 则完成该桶并创建新桶。
    #[inline]
    fn update_aggregate_trades_from_event(&mut self, event: TradeEvent) {
        // 更新1秒桶
        if self.active_1s_bucket.contains_time(event.timestamp) {
            if !self.active_1s_bucket.has_trades {
                self.active_1s_bucket.init_with_first_trade(event.price, event.quantity);
            } else {
                self.active_1s_bucket.update_with_trade(event.price, event.quantity);
            }
        } else {
            if self.active_1s_bucket.has_trades {
                self.completed_1s_trades.push(self.active_1s_bucket.to_aggregate_trade());
                if self.completed_1s_trades.len() > self.max_history {
                    self.completed_1s_trades.remove(0);
                }
            }
            self.active_1s_bucket = AggregateTradeWindow::new(BucketType::OneSecond, event.timestamp, event.sequence);
            self.active_1s_bucket.init_with_first_trade(event.price, event.quantity);
        }

        // 更新5秒桶
        if self.active_5s_bucket.contains_time(event.timestamp) {
            if !self.active_5s_bucket.has_trades {
                self.active_5s_bucket.init_with_first_trade(event.price, event.quantity);
            } else {
                self.active_5s_bucket.update_with_trade(event.price, event.quantity);
            }
        } else {
            if self.active_5s_bucket.has_trades {
                self.completed_5s_trades.push(self.active_5s_bucket.to_aggregate_trade());
                if self.completed_5s_trades.len() > self.max_history {
                    self.completed_5s_trades.remove(0);
                }
            }
            self.active_5s_bucket = AggregateTradeWindow::new(BucketType::FiveSeconds, event.timestamp, event.sequence);
            self.active_5s_bucket.init_with_first_trade(event.price, event.quantity);
        }

        // 更新1分钟桶
        if self.active_1m_bucket.contains_time(event.timestamp) {
            if !self.active_1m_bucket.has_trades {
                self.active_1m_bucket.init_with_first_trade(event.price, event.quantity);
            } else {
                self.active_1m_bucket.update_with_trade(event.price, event.quantity);
            }
        } else {
            if self.active_1m_bucket.has_trades {
                self.completed_1m_trades.push(self.active_1m_bucket.to_aggregate_trade());
                if self.completed_1m_trades.len() > self.max_history {
                    self.completed_1m_trades.remove(0);
                }
            }
            self.active_1m_bucket = AggregateTradeWindow::new(BucketType::OneMinute, event.timestamp, event.sequence);
            self.active_1m_bucket.init_with_first_trade(event.price, event.quantity);
        }
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

    /// 重置24小时统计数据
    ///
    /// 将所有24小时统计数据重置为初始状态。在正式环境中，
    /// 应该在UTC午夜自动调用此方法（在Phase 3中通过定时器实现）。
    /// 对于MVP版本，此方法可由外部代码在适当的时间点调用。
    ///
    /// # 参数
    /// - `timestamp`: 新的开始时间戳（纳秒），通常是重置时刻的时间
    ///
    /// # 示例
    /// ```ignore
    /// engine.reset_24h_statistics(current_timestamp);
    /// ```
    pub fn reset_24h_statistics(&mut self, timestamp: u64) {
        self.statistics_24h = Statistics24h {
            timestamp,
            open_time: timestamp,
            close_time: timestamp,
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
            first_trade_id: 0,
            last_trade_id: 0,
            trade_count: 0,
        };
    }

    /// 获取当前活跃的1秒聚合成交
    ///
    /// # 返回
    /// 当前活跃的1秒时间桶转换为AggregateTrade
    #[inline]
    pub fn get_current_1s_aggregate(&self) -> AggregateTrade {
        self.active_1s_bucket.to_aggregate_trade()
    }

    /// 获取当前活跃的5秒聚合成交
    ///
    /// # 返回
    /// 当前活跃的5秒时间桶转换为AggregateTrade
    #[inline]
    pub fn get_current_5s_aggregate(&self) -> AggregateTrade {
        self.active_5s_bucket.to_aggregate_trade()
    }

    /// 获取当前活跃的1分钟聚合成交
    ///
    /// # 返回
    /// 当前活跃的1分钟时间桶转换为AggregateTrade
    #[inline]
    pub fn get_current_1m_aggregate(&self) -> AggregateTrade {
        self.active_1m_bucket.to_aggregate_trade()
    }

    /// 获取1秒聚合成交历史
    ///
    /// # 返回
    /// 最近完成的1秒聚合成交列表（滚动窗口）
    #[inline]
    pub fn get_1s_aggregate_history(&self) -> Vec<AggregateTrade> {
        self.completed_1s_trades.clone()
    }

    /// 获取5秒聚合成交历史
    ///
    /// # 返回
    /// 最近完成的5秒聚合成交列表（滚动窗口）
    #[inline]
    pub fn get_5s_aggregate_history(&self) -> Vec<AggregateTrade> {
        self.completed_5s_trades.clone()
    }

    /// 获取1分钟聚合成交历史
    ///
    /// # 返回
    /// 最近完成的1分钟聚合成交列表（滚动窗口）
    #[inline]
    pub fn get_1m_aggregate_history(&self) -> Vec<AggregateTrade> {
        self.completed_1m_trades.clone()
    }

    /// 生成已发布快照
    ///
    /// 根据当前的引擎状态生成一个PublishedSnapshot，包含：
    /// - BBO快照
    /// - Level2快照
    /// - 所有活跃时间桶的聚合成交数据
    /// - 24小时统计数据
    ///
    /// # 参数
    /// - `timestamp`: 快照时间戳（纳秒），通常由定时器提供
    /// - `sequence`: 快照序列号
    ///
    /// # 返回
    /// 新生成的PublishedSnapshot
    #[inline]
    pub fn generate_published_snapshot(&self, timestamp: u64, sequence: u64) -> PublishedSnapshot {
        PublishedSnapshot::new(
            timestamp,
            sequence,
            self.bbo_snapshot,
            self.level2_snapshot,
            self.active_1s_bucket.to_aggregate_trade(),
            self.active_5s_bucket.to_aggregate_trade(),
            self.active_1m_bucket.to_aggregate_trade(),
            self.statistics_24h,
        )
    }

}

/// 1ms定时器和快照发布器
///
/// 负责定期（每1ms）从MarketDataEngine收集快照并发送到发布通道。
/// 运行在独立线程中，不阻塞主事件循环。
///
/// # 示例
///
/// ```ignore
/// let engine = Arc::new(Mutex::new(MarketDataEngine::new(receiver)));
/// let (snapshot_tx, snapshot_rx) = crossbeam::channel::unbounded();
/// let timer = SnapshotTimer::spawn(engine.clone(), snapshot_tx);
///
/// // 处理快照...
///
/// timer.stop();  // 停止定时器
/// ```
pub struct SnapshotTimer {
    should_stop: Arc<AtomicBool>,
}

impl SnapshotTimer {
    /// 启动1ms定时器和快照生成线程
    ///
    /// # 参数
    /// - `engine`: 市场数据引擎的Arc<Mutex<>>包装
    /// - `snapshot_tx`: 快照发送器通道
    ///
    /// # 返回
    /// SnapshotTimer控制句柄，用于停止定时器
    pub fn spawn(
        engine: Arc<parking_lot::Mutex<MarketDataEngine>>,
        mut snapshot_tx: Producer<PublishedSnapshot>,
    ) -> Self {
        let should_stop = Arc::new(AtomicBool::new(false));
        let stop_flag = should_stop.clone();

        thread::spawn(move || {
            let mut sequence = 1u64;
            let interval = Duration::from_millis(1);

            loop {
                // 检查是否应该停止
                if stop_flag.load(Ordering::Relaxed) {
                    break;
                }

                // 获取当前时间（模拟纳秒精度）
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64;

                // 锁定引擎并生成快照
                {
                    let engine = engine.lock();
                    let snapshot = engine.generate_published_snapshot(timestamp, sequence);

                    // 发送快照到ring buffer
                    // 如果缓冲满，忽略错误（接收方可能处理不过来）
                    let _ = snapshot_tx.push(snapshot);
                }

                sequence = sequence.wrapping_add(1);

                // 等待1ms再生成下一个快照
                thread::sleep(interval);
            }
        });

        Self { should_stop }
    }

    /// 停止定时器和快照生成线程
    ///
    /// 此方法是非阻塞的，它设置停止标志但不等待线程退出。
    /// 线程将在下一次迭代时看到标志并退出。
    pub fn stop(&self) {
        self.should_stop.store(true, Ordering::Relaxed);
    }
}

/// Aeron快照发布器线程控制
///
/// SnapshotPublisherThread在单独的线程中运行，消费PublishedSnapshot通道
/// 并通过Aeron发布到WebSocket服务器和其他消费者。
///
/// # 线程安全
///
/// - should_stop标志通过AtomicBool安全共享
/// - 发布线程独占访问Publisher（无并发问题）
pub struct SnapshotPublisherThread {
    /// 停止信号
    should_stop: Arc<AtomicBool>,
}

impl SnapshotPublisherThread {
    /// 启动Aeron快照发布线程
    ///
    /// # 参数
    /// - `snapshot_rx`: PublishedSnapshot接收器，由SnapshotTimer生成
    /// - `aeron_config`: Aeron配置（目录、通道、流ID）
    ///
    /// # 返回
    /// SnapshotPublisherThread控制句柄，用于停止线程
    ///
    /// # 行为
    /// 线程会：
    /// 1. 创建到Aeron媒体驱动的连接
    /// 2. 持续消费snapshot_rx通道
    /// 3. 将每个快照序列化为二进制格式
    /// 4. 通过Aeron发布到订阅者
    /// 5. 优雅地处理背压（丢弃快照而不是阻塞）
    /// 6. 当should_stop标志被设置时退出
    ///
    /// 如果连接初始化失败，线程将记录错误并退出。
    pub fn spawn(
        mut snapshot_rx: Consumer<PublishedSnapshot>,
        aeron_config: crate::aeron_publisher::AeronConfig,
    ) -> Self {
        let should_stop = Arc::new(AtomicBool::new(false));
        let stop_flag = should_stop.clone();

        thread::spawn(move || {
            // 初始化Aeron发布器
            let publisher = match crate::aeron_publisher::SnapshotPublisher::new(aeron_config) {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("Failed to initialize Aeron publisher: {}", e);
                    return;
                }
            };

            let mut dropped_count = 0u64;
            let mut published_count = 0u64;

            loop {
                // 检查是否应该停止
                if stop_flag.load(Ordering::Relaxed) {
                    break;
                }

                // 尝试从ring buffer接收快照（非阻塞）
                match snapshot_rx.pop() {
                    Ok(snapshot) => {
                        // 尝试发布快照
                        match publisher.publish(&snapshot) {
                            Ok(()) => {
                                published_count = published_count.wrapping_add(1);
                            }
                            Err(crate::aeron_publisher::PublisherError::BackPressured) => {
                                // 环形缓冲区已满，丢弃此快照
                                // 这是正常情况，不值得记录每一次
                                dropped_count = dropped_count.wrapping_add(1);
                            }
                            Err(crate::aeron_publisher::PublisherError::NotConnected) => {
                                // 没有连接的订阅者，忽略此快照
                                dropped_count = dropped_count.wrapping_add(1);
                            }
                            Err(e) => {
                                // 其他错误（序列化失败、已关闭等）
                                eprintln!("Error publishing snapshot: {}", e);
                                // 仍然计为丢弃
                                dropped_count = dropped_count.wrapping_add(1);
                            }
                        }
                    }
                    Err(_) => {
                        // Ring buffer暂时为空，让出CPU
                        std::thread::yield_now();
                    }
                }
            }

            // 线程退出时的诊断信息
            tracing::info!(
                "Snapshot publisher thread stopped. Published: {}, Dropped: {}",
                published_count,
                dropped_count
            );
        });

        Self { should_stop }
    }

    /// 停止发布线程
    ///
    /// 此方法是非阻塞的，它设置停止标志但不等待线程退出。
    /// 线程将在下一次迭代时看到标志并退出。
    pub fn stop(&self) {
        self.should_stop.store(true, Ordering::Relaxed);
    }
}

/// TradePublisherThread在单独的线程中运行，消费TradeEvent通道
/// 并通过Aeron立即发布到WebSocket服务器和其他消费者。
///
/// 与SnapshotPublisherThread不同的是，TradePublisherThread发布的是单个成交事件，
/// 不进行任何缓冲或聚合。每个成交都立即发布（逐笔成交）。
///
/// # 线程安全
///
/// - should_stop标志通过AtomicBool安全共享
/// - 发布线程独占访问Publisher（无并发问题）
pub struct TradePublisherThread {
    /// 停止信号
    should_stop: Arc<AtomicBool>,
}

impl TradePublisherThread {
    /// 启动Aeron成交事件发布线程
    ///
    /// # 参数
    /// - `trade_rx`: TradeEvent接收器，由MatchingEngine发送
    /// - `aeron_config`: Aeron配置（目录、通道、流ID）
    ///
    /// # 返回
    /// TradePublisherThread控制句柄，用于停止线程
    ///
    /// # 行为
    /// 线程会：
    /// 1. 创建到Aeron媒体驱动的连接
    /// 2. 持续消费trade_rx通道
    /// 3. 将每个成交序列化为二进制格式
    /// 4. 通过Aeron立即发布到订阅者（没有缓冲）
    /// 5. 优雅地处理背压（丢弃成交而不是阻塞）
    /// 6. 当should_stop标志被设置时退出
    ///
    /// 如果连接初始化失败，线程将记录错误并退出。
    pub fn spawn(
        mut trade_rx: Consumer<TradeEvent>,
        aeron_config: crate::aeron_publisher::AeronConfig,
    ) -> Self {
        let should_stop = Arc::new(AtomicBool::new(false));
        let stop_flag = should_stop.clone();

        thread::spawn(move || {
            // 初始化Aeron发布器
            let publisher = match crate::aeron_publisher::TradePublisher::new(aeron_config) {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("Failed to initialize Aeron trade publisher: {}", e);
                    return;
                }
            };

            let mut dropped_count = 0u64;
            let mut published_count = 0u64;

            loop {
                // 检查是否应该停止
                if stop_flag.load(Ordering::Relaxed) {
                    break;
                }

                // 尝试从ring buffer接收成交（非阻塞）
                match trade_rx.pop() {
                    Ok(trade) => {
                        // 尝试立即发布成交
                        match publisher.publish(&trade) {
                            Ok(()) => {
                                published_count = published_count.wrapping_add(1);
                            }
                            Err(crate::aeron_publisher::PublisherError::BackPressured) => {
                                // 环形缓冲区已满，丢弃此成交
                                // 这很少发生，但可能在订阅者跟不上时发生
                                dropped_count = dropped_count.wrapping_add(1);
                            }
                            Err(crate::aeron_publisher::PublisherError::NotConnected) => {
                                // 没有连接的订阅者，忽略此成交
                                dropped_count = dropped_count.wrapping_add(1);
                            }
                            Err(e) => {
                                // 其他错误（序列化失败、已关闭等）
                                eprintln!("Error publishing trade: {}", e);
                                // 仍然计为丢弃
                                dropped_count = dropped_count.wrapping_add(1);
                            }
                        }
                    }
                    Err(_) => {
                        // Ring buffer暂时为空，让出CPU
                        std::thread::yield_now();
                    }
                }
            }

            // 线程退出时的诊断信息
            tracing::info!(
                "Trade publisher thread stopped. Published: {}, Dropped: {}",
                published_count,
                dropped_count
            );
        });

        Self { should_stop }
    }

    /// 停止发布线程
    ///
    /// 此方法是非阻塞的，它设置停止标志但不等待线程退出。
    /// 线程将在下一次迭代时看到标志并退出。
    pub fn stop(&self) {
        self.should_stop.store(true, Ordering::Relaxed);
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

    #[test]
    fn test_reset_24h_statistics_clears_all_fields() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        // Add trades to accumulate statistics
        let event1 = TradeEvent::new(1, 100, 99, 1_000_000_000, 50000.0, 10.0, Side::Buy, 1001, 2001);
        engine.consume_trade_event(event1);

        let event2 = TradeEvent::new(2, 101, 100, 2_000_000_000, 50100.0, 5.0, Side::Sell, 1002, 2002);
        engine.consume_trade_event(event2);

        // Verify statistics are accumulated
        let stats = engine.get_24h_statistics();
        assert_eq!(stats.trade_count, 2);
        assert_eq!(stats.volume_24h, 15.0);
        assert!(stats.price_24h_high > 0.0);
        assert!(stats.price_24h_low > 0.0);

        // Reset statistics
        let reset_time = 3_000_000_000u64;
        engine.reset_24h_statistics(reset_time);

        // Verify all fields are reset
        let stats = engine.get_24h_statistics();
        assert_eq!(stats.timestamp, reset_time);
        assert_eq!(stats.open_time, reset_time);
        assert_eq!(stats.close_time, reset_time);
        assert_eq!(stats.trade_count, 0);
        assert_eq!(stats.volume_24h, 0.0);
        assert_eq!(stats.quote_asset_volume_24h, 0.0);
        assert_eq!(stats.price_24h_high, 0.0);
        assert_eq!(stats.price_24h_low, 0.0);
        assert_eq!(stats.price_change_percent, 0.0);
        assert_eq!(stats.first_trade_id, 0);
        assert_eq!(stats.last_trade_id, 0);
        assert_eq!(stats.bid_price, 0.0);
        assert_eq!(stats.ask_price, 0.0);
    }

    #[test]
    fn test_reset_24h_statistics_allows_new_trades() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        // Add first trade
        let event1 = TradeEvent::new(1, 100, 99, 1_000_000_000, 50000.0, 10.0, Side::Buy, 1001, 2001);
        engine.consume_trade_event(event1);

        // Verify first trade is recorded
        let mut stats = engine.get_24h_statistics();
        assert_eq!(stats.trade_count, 1);
        assert_eq!(stats.first_trade_id, 100);
        assert_eq!(stats.volume_24h, 10.0);

        // Reset at new time
        let reset_time = 2_000_000_000u64;
        engine.reset_24h_statistics(reset_time);

        // Verify reset
        stats = engine.get_24h_statistics();
        assert_eq!(stats.trade_count, 0);
        assert_eq!(stats.first_trade_id, 0);

        // Add new trade after reset
        let event2 = TradeEvent::new(2, 200, 199, 3_000_000_000, 51000.0, 5.0, Side::Sell, 2001, 3001);
        engine.consume_trade_event(event2);

        // Verify new trade is correctly recorded
        stats = engine.get_24h_statistics();
        assert_eq!(stats.trade_count, 1);
        assert_eq!(stats.first_trade_id, 200);
        assert_eq!(stats.last_trade_id, 200);
        assert_eq!(stats.volume_24h, 5.0);
        assert_eq!(stats.price_24h_high, 51000.0);
        assert_eq!(stats.price_24h_low, 51000.0);
    }

    #[test]
    fn test_reset_24h_statistics_preserves_bbo_and_level2() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        // Add trade
        let event = TradeEvent::new(1, 100, 99, 1_000_000_000, 50000.0, 10.0, Side::Buy, 1001, 2001);
        engine.consume_trade_event(event);

        // Verify BBO was updated
        let bbo_before = engine.get_bbo_snapshot();
        assert_eq!(bbo_before.best_ask_price, 50000.0);

        let reset_time = 2_000_000_000u64;
        engine.reset_24h_statistics(reset_time);

        // Verify BBO and Level2 are NOT reset (only 24h stats)
        let bbo_after = engine.get_bbo_snapshot();
        assert_eq!(bbo_after.best_ask_price, 50000.0);

        let l2 = engine.get_level2_snapshot();
        assert_eq!(l2.num_asks, 1);
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
        let history = engine.get_1s_aggregate_history();
        assert!(history.is_empty());
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

    // ===== 聚合成交 (Aggregate Trade) 测试 - Task 2.4 =====

    #[test]
    fn test_aggregate_trade_window_contains_time() {
        let bucket = AggregateTradeWindow::new(BucketType::OneSecond, 1_000_000_000, 1);

        // 窗口: 1_000_000_000 to 2_000_000_000
        assert!(bucket.contains_time(1_000_000_000));
        assert!(bucket.contains_time(1_500_000_000));
        assert!(!bucket.contains_time(2_000_000_000));
        assert!(!bucket.contains_time(3_000_000_000));
    }

    #[test]
    fn test_aggregate_trade_window_bucket_types() {
        let bucket_1s = AggregateTradeWindow::new(BucketType::OneSecond, 1_000_000_000, 1);
        assert_eq!(bucket_1s.end_time - bucket_1s.start_time, 1_000_000_000);

        let bucket_5s = AggregateTradeWindow::new(BucketType::FiveSeconds, 1_000_000_000, 1);
        assert_eq!(bucket_5s.end_time - bucket_5s.start_time, 5_000_000_000);

        let bucket_1m = AggregateTradeWindow::new(BucketType::OneMinute, 1_000_000_000, 1);
        assert_eq!(bucket_1m.end_time - bucket_1m.start_time, 60_000_000_000);
    }

    #[test]
    fn test_aggregate_trade_window_init_and_update() {
        let mut bucket = AggregateTradeWindow::new(BucketType::OneSecond, 1_000_000_000, 1);
        assert!(!bucket.has_trades);

        // 初始化第一笔成交
        bucket.init_with_first_trade(100.0, 10.0);
        assert!(bucket.has_trades);
        assert_eq!(bucket.open, 100.0);
        assert_eq!(bucket.close, 100.0);
        assert_eq!(bucket.high, 100.0);
        assert_eq!(bucket.low, 100.0);
        assert_eq!(bucket.volume, 10.0);
        assert_eq!(bucket.quote_asset_volume, 1000.0);
        assert_eq!(bucket.trade_count, 1);

        // 更新第二笔成交
        bucket.update_with_trade(101.0, 5.0);
        assert_eq!(bucket.open, 100.0);  // 开盘价不变
        assert_eq!(bucket.close, 101.0);  // 收盘价更新
        assert_eq!(bucket.high, 101.0);   // 最高价更新
        assert_eq!(bucket.low, 100.0);    // 最低价不变
        assert_eq!(bucket.volume, 15.0);
        assert_eq!(bucket.quote_asset_volume, 1505.0);
        assert_eq!(bucket.trade_count, 2);

        // 更新第三笔成交
        bucket.update_with_trade(99.0, 8.0);
        assert_eq!(bucket.high, 101.0);
        assert_eq!(bucket.low, 99.0);
        assert_eq!(bucket.close, 99.0);
        assert_eq!(bucket.volume, 23.0);
        assert_eq!(bucket.trade_count, 3);
    }

    #[test]
    fn test_aggregate_trade_window_to_aggregate_trade() {
        let mut bucket = AggregateTradeWindow::new(BucketType::OneSecond, 1_000_000_000, 5);
        bucket.init_with_first_trade(100.0, 10.0);
        bucket.update_with_trade(101.0, 5.0);

        let trade = bucket.to_aggregate_trade();
        assert_eq!(trade.start_time, 1_000_000_000);
        assert_eq!(trade.end_time, 2_000_000_000);
        assert_eq!(trade.sequence, 5);
        assert_eq!(trade.open, 100.0);
        assert_eq!(trade.close, 101.0);
        assert_eq!(trade.high, 101.0);
        assert_eq!(trade.low, 100.0);
        assert_eq!(trade.volume, 15.0);
        assert_eq!(trade.quote_asset_volume, 1505.0);
        assert_eq!(trade.trade_count, 2);
    }

    #[test]
    fn test_market_data_engine_single_trade_updates_all_buckets() {
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

        // 检查所有三个活跃桶都更新了
        let agg_1s = engine.get_current_1s_aggregate();
        let agg_5s = engine.get_current_5s_aggregate();
        let agg_1m = engine.get_current_1m_aggregate();

        // 1秒桶
        assert!(agg_1s.open == 50000.0);
        assert_eq!(agg_1s.volume, 10.0);
        assert_eq!(agg_1s.trade_count, 1);

        // 5秒桶
        assert_eq!(agg_5s.open, 50000.0);
        assert_eq!(agg_5s.volume, 10.0);
        assert_eq!(agg_5s.trade_count, 1);

        // 1分钟桶
        assert_eq!(agg_1m.open, 50000.0);
        assert_eq!(agg_1m.volume, 10.0);
        assert_eq!(agg_1m.trade_count, 1);
    }

    #[test]
    fn test_market_data_engine_multiple_trades_within_bucket() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        // 三笔成交在同一秒内
        let trades = vec![
            (1_000_000_000, 100.0, 10.0),
            (1_300_000_000, 101.0, 5.0),
            (1_800_000_000, 100.5, 8.0),
        ];

        for (idx, (timestamp, price, qty)) in trades.iter().enumerate() {
            let event = TradeEvent::new(
                (idx + 1) as u64,
                100 + idx as u64,
                99 + idx as u64,
                *timestamp,
                *price,
                *qty,
                Side::Buy,
                1001 + idx as u64,
                2001 + idx as u64,
            );
            engine.consume_trade_event(event);
        }

        let agg_1s = engine.get_current_1s_aggregate();
        assert_eq!(agg_1s.open, 100.0);
        assert_eq!(agg_1s.close, 100.5);
        assert_eq!(agg_1s.high, 101.0);
        assert_eq!(agg_1s.low, 100.0);
        assert_eq!(agg_1s.volume, 23.0);
        assert_eq!(agg_1s.quote_asset_volume, 100.0*10.0 + 101.0*5.0 + 100.5*8.0);
        assert_eq!(agg_1s.trade_count, 3);
    }

    #[test]
    fn test_market_data_engine_bucket_expiration_1s() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        // 第一笔成交在1秒时
        let event1 = TradeEvent::new(
            1,
            100,
            99,
            1_000_000_000,
            100.0,
            10.0,
            Side::Buy,
            1001,
            2001,
        );
        engine.consume_trade_event(event1);

        let agg_1s_before = engine.get_current_1s_aggregate();
        assert_eq!(agg_1s_before.volume, 10.0);
        assert_eq!(agg_1s_before.start_time, 1_000_000_000);

        // 第二笔成交超过1秒（在1.5秒）
        let event2 = TradeEvent::new(
            2,
            101,
            100,
            2_100_000_000,  // 超过第一个1秒窗口的结束时间 (2_000_000_000)
            101.0,
            5.0,
            Side::Buy,
            1002,
            2002,
        );
        engine.consume_trade_event(event2);

        // 历史应该包含已完成的第一个桶
        let history = engine.get_1s_aggregate_history();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].volume, 10.0);
        assert_eq!(history[0].open, 100.0);

        // 当前活跃桶应该是新的
        let agg_1s_after = engine.get_current_1s_aggregate();
        assert_eq!(agg_1s_after.volume, 5.0);
        assert_eq!(agg_1s_after.start_time, 2_100_000_000);
        assert_eq!(agg_1s_after.open, 101.0);
    }

    #[test]
    fn test_market_data_engine_rolling_window_history() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        // 创建12个连续的1秒桶（超过max_history=10）
        for i in 0..12 {
            let timestamp = 1_000_000_000 + (i as u64 * 1_000_000_000) + 100;
            let event = TradeEvent::new(
                (i + 1) as u64,
                100 + i as u64,
                99 + i as u64,
                timestamp,
                50000.0 + i as f64,
                10.0 + i as f64,
                Side::Buy,
                1001 + i as u64,
                2001 + i as u64,
            );
            engine.consume_trade_event(event);
        }

        // 历史应该只包含最近的10个（max_history=10）
        // 12个事件中，11个完成了桶（最后一个事件的桶是活跃的），
        // 其中前1个被丢弃，保留最后的10个
        let history = engine.get_1s_aggregate_history();
        assert_eq!(history.len(), 10);

        // 验证最早的桶（来自事件i=1，体积=11）
        assert_eq!(history[0].volume, 11.0);  // 事件i=1
        // 验证最后的桶（来自事件i=10，体积=20）
        assert_eq!(history[9].volume, 20.0);  // 事件i=10（最后一个完成的桶）
    }

    #[test]
    fn test_market_data_engine_bucket_expiration_5s() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        // 第一笔成交在1秒
        let event1 = TradeEvent::new(
            1,
            100,
            99,
            1_000_000_000,
            100.0,
            10.0,
            Side::Buy,
            1001,
            2001,
        );
        engine.consume_trade_event(event1);

        let agg_5s_before = engine.get_current_5s_aggregate();
        // 初始桶始于0，结束于5秒
        assert_eq!(agg_5s_before.end_time, 5_000_000_000);
        assert_eq!(agg_5s_before.volume, 10.0);

        // 第二笔成交在6秒（超过5秒窗口）
        let event2 = TradeEvent::new(
            2,
            101,
            100,
            6_100_000_000,
            101.0,
            5.0,
            Side::Buy,
            1002,
            2002,
        );
        engine.consume_trade_event(event2);

        // 历史应该包含已完成的5秒桶
        let history = engine.get_5s_aggregate_history();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].volume, 10.0);

        // 当前活跃桶应该是新的
        let agg_5s_after = engine.get_current_5s_aggregate();
        assert_eq!(agg_5s_after.start_time, 6_100_000_000);
        assert_eq!(agg_5s_after.end_time, 11_100_000_000);
        assert_eq!(agg_5s_after.volume, 5.0);
    }

    #[test]
    fn test_market_data_engine_bucket_expiration_1m() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        // 第一笔成交在0秒
        let event1 = TradeEvent::new(
            1,
            100,
            99,
            1_000_000_000,
            100.0,
            10.0,
            Side::Buy,
            1001,
            2001,
        );
        engine.consume_trade_event(event1);

        // 第二笔成交在61秒（超过1分钟窗口）
        let event2 = TradeEvent::new(
            2,
            101,
            100,
            61_000_000_000 + 100,  // 61秒 + 100纳秒
            101.0,
            5.0,
            Side::Buy,
            1002,
            2002,
        );
        engine.consume_trade_event(event2);

        // 历史应该包含已完成的1分钟桶
        let history = engine.get_1m_aggregate_history();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].volume, 10.0);

        // 当前活跃桶应该是新的
        let agg_1m_after = engine.get_current_1m_aggregate();
        assert_eq!(agg_1m_after.volume, 5.0);
    }

    #[test]
    fn test_market_data_engine_independent_bucket_types() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        // 在同一时间戳创建多个事件，不同的桶应该独立更新
        for i in 0..3 {
            let timestamp = 1_000_000_000 + (i as u64 * 200_000_000);
            let event = TradeEvent::new(
                (i + 1) as u64,
                100 + i as u64,
                99 + i as u64,
                timestamp,
                50000.0 + i as f64 * 100.0,
                10.0,
                Side::Buy,
                1001 + i as u64,
                2001 + i as u64,
            );
            engine.consume_trade_event(event);
        }

        // 所有成交都在同一1秒窗口内
        let agg_1s = engine.get_current_1s_aggregate();
        assert_eq!(agg_1s.trade_count, 3);

        // 所有成交都在同一5秒窗口内
        let agg_5s = engine.get_current_5s_aggregate();
        assert_eq!(agg_5s.trade_count, 3);

        // 所有成交都在同一1分钟窗口内
        let agg_1m = engine.get_current_1m_aggregate();
        assert_eq!(agg_1m.trade_count, 3);
    }

    // ===== Level2 更新逻辑测试 - Task 2.3 =====

    #[test]
    fn test_level2_initialization_empty_state() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let engine = MarketDataEngine::new(receiver);

        let l2 = engine.get_level2_snapshot();
        assert_eq!(l2.timestamp, 0);
        assert_eq!(l2.sequence, 0);
        assert_eq!(l2.num_bids, 0);
        assert_eq!(l2.num_asks, 0);
    }

    #[test]
    fn test_level2_update_on_first_buy_trade() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        let event = TradeEvent::new(1, 100, 99, 1_000_000_000, 50000.0, 10.0, Side::Buy, 1001, 2001);
        engine.consume_trade_event(event);

        let l2 = engine.get_level2_snapshot();
        // 买方成交：添加卖档
        assert_eq!(l2.num_asks, 1);
        assert_eq!(l2.asks[0].price, 50000.0);
        assert_eq!(l2.asks[0].quantity, 10.0);
        assert_eq!(l2.num_bids, 0);
    }

    #[test]
    fn test_level2_update_on_first_sell_trade() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        let event = TradeEvent::new(1, 100, 99, 1_000_000_000, 50000.0, 10.0, Side::Sell, 1001, 2001);
        engine.consume_trade_event(event);

        let l2 = engine.get_level2_snapshot();
        // 卖方成交：添加买档
        assert_eq!(l2.num_bids, 1);
        assert_eq!(l2.bids[0].price, 50000.0);
        assert_eq!(l2.bids[0].quantity, 10.0);
        assert_eq!(l2.num_asks, 0);
    }

    #[test]
    fn test_level2_ask_quantity_reduction() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        // First trade at 50000 with qty 20
        let event1 = TradeEvent::new(1, 100, 99, 1_000_000_000, 50000.0, 20.0, Side::Buy, 1001, 2001);
        engine.consume_trade_event(event1);

        let l2_1 = engine.get_level2_snapshot();
        assert_eq!(l2_1.num_asks, 1);
        assert_eq!(l2_1.asks[0].quantity, 20.0);

        // Second trade at same price with qty 8
        let event2 = TradeEvent::new(2, 101, 100, 2_000_000_000, 50000.0, 8.0, Side::Buy, 1002, 2002);
        engine.consume_trade_event(event2);

        let l2_2 = engine.get_level2_snapshot();
        assert_eq!(l2_2.num_asks, 1);
        assert_eq!(l2_2.asks[0].price, 50000.0);
        assert_eq!(l2_2.asks[0].quantity, 12.0); // 20 - 8
    }

    #[test]
    fn test_level2_bid_quantity_reduction() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        // First trade at 50000 with qty 20
        let event1 = TradeEvent::new(1, 100, 99, 1_000_000_000, 50000.0, 20.0, Side::Sell, 1001, 2001);
        engine.consume_trade_event(event1);

        let l2_1 = engine.get_level2_snapshot();
        assert_eq!(l2_1.num_bids, 1);
        assert_eq!(l2_1.bids[0].quantity, 20.0);

        // Second trade at same price with qty 8
        let event2 = TradeEvent::new(2, 101, 100, 2_000_000_000, 50000.0, 8.0, Side::Sell, 1002, 2002);
        engine.consume_trade_event(event2);

        let l2_2 = engine.get_level2_snapshot();
        assert_eq!(l2_2.num_bids, 1);
        assert_eq!(l2_2.bids[0].price, 50000.0);
        assert_eq!(l2_2.bids[0].quantity, 12.0); // 20 - 8
    }

    #[test]
    fn test_level2_level_removal_when_quantity_depleted() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        // First trade at 50000 with qty 10
        let event1 = TradeEvent::new(1, 100, 99, 1_000_000_000, 50000.0, 10.0, Side::Buy, 1001, 2001);
        engine.consume_trade_event(event1);

        let l2_1 = engine.get_level2_snapshot();
        assert_eq!(l2_1.num_asks, 1);

        // Second trade at same price consuming all qty
        let event2 = TradeEvent::new(2, 101, 100, 2_000_000_000, 50000.0, 10.0, Side::Buy, 1002, 2002);
        engine.consume_trade_event(event2);

        let l2_2 = engine.get_level2_snapshot();
        // Level should be removed when quantity reaches 0
        assert_eq!(l2_2.num_asks, 0);
    }

    #[test]
    fn test_level2_multiple_ask_levels_ascending_order() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        // Add ask levels at different prices
        let event1 = TradeEvent::new(1, 100, 99, 1_000_000_000, 50001.0, 10.0, Side::Buy, 1001, 2001);
        engine.consume_trade_event(event1);

        let event2 = TradeEvent::new(2, 101, 100, 2_000_000_000, 50000.0, 20.0, Side::Buy, 1002, 2002);
        engine.consume_trade_event(event2);

        let event3 = TradeEvent::new(3, 102, 101, 3_000_000_000, 50002.0, 5.0, Side::Buy, 1003, 2003);
        engine.consume_trade_event(event3);

        let l2 = engine.get_level2_snapshot();
        assert_eq!(l2.num_asks, 3);
        // Ask levels should be in ascending order
        assert_eq!(l2.asks[0].price, 50000.0);
        assert_eq!(l2.asks[0].quantity, 20.0);
        assert_eq!(l2.asks[1].price, 50001.0);
        assert_eq!(l2.asks[1].quantity, 10.0);
        assert_eq!(l2.asks[2].price, 50002.0);
        assert_eq!(l2.asks[2].quantity, 5.0);
    }

    #[test]
    fn test_level2_multiple_bid_levels_descending_order() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        // Add bid levels at different prices
        let event1 = TradeEvent::new(1, 100, 99, 1_000_000_000, 49999.0, 10.0, Side::Sell, 1001, 2001);
        engine.consume_trade_event(event1);

        let event2 = TradeEvent::new(2, 101, 100, 2_000_000_000, 50000.0, 20.0, Side::Sell, 1002, 2002);
        engine.consume_trade_event(event2);

        let event3 = TradeEvent::new(3, 102, 101, 3_000_000_000, 49998.0, 5.0, Side::Sell, 1003, 2003);
        engine.consume_trade_event(event3);

        let l2 = engine.get_level2_snapshot();
        assert_eq!(l2.num_bids, 3);
        // Bid levels should be in descending order
        assert_eq!(l2.bids[0].price, 50000.0);
        assert_eq!(l2.bids[0].quantity, 20.0);
        assert_eq!(l2.bids[1].price, 49999.0);
        assert_eq!(l2.bids[1].quantity, 10.0);
        assert_eq!(l2.bids[2].price, 49998.0);
        assert_eq!(l2.bids[2].quantity, 5.0);
    }

    #[test]
    fn test_level2_max_10_levels() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        // Add 10 ask levels
        for i in 0..10 {
            let event = TradeEvent::new(
                (i + 1) as u64,
                100 + i as u64,
                99 + i as u64,
                1_000_000_000 + (i as u64 * 1_000_000),
                50000.0 + i as f64,
                10.0,
                Side::Buy,
                1001 + i as u64,
                2001 + i as u64,
            );
            engine.consume_trade_event(event);
        }

        let l2 = engine.get_level2_snapshot();
        assert_eq!(l2.num_asks, 10);

        // Try to add 11th level - should not be added
        let event11 = TradeEvent::new(11, 110, 109, 11_000_000_000, 50010.0, 10.0, Side::Buy, 1011, 2011);
        engine.consume_trade_event(event11);

        let l2_after = engine.get_level2_snapshot();
        // Should still be 10 levels
        assert_eq!(l2_after.num_asks, 10);
    }

    #[test]
    fn test_level2_mixed_bid_ask_updates() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        // Sequence of trades: sell, buy, sell, buy
        let trades = vec![
            (50000.0, 10.0, Side::Sell),
            (50050.0, 5.0, Side::Buy),
            (50025.0, 8.0, Side::Sell),
            (50040.0, 3.0, Side::Buy),
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

        let l2 = engine.get_level2_snapshot();
        // Should have 2 bid levels
        assert_eq!(l2.num_bids, 2);
        assert_eq!(l2.bids[0].price, 50025.0);
        assert_eq!(l2.bids[0].quantity, 8.0);
        assert_eq!(l2.bids[1].price, 50000.0);
        assert_eq!(l2.bids[1].quantity, 10.0);

        // Should have 2 ask levels
        assert_eq!(l2.num_asks, 2);
        assert_eq!(l2.asks[0].price, 50040.0);
        assert_eq!(l2.asks[0].quantity, 3.0);
        assert_eq!(l2.asks[1].price, 50050.0);
        assert_eq!(l2.asks[1].quantity, 5.0);
    }

    #[test]
    fn test_level2_timestamp_and_sequence() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        let event = TradeEvent::new(
            5,
            100,
            99,
            2_000_000_000,
            50000.0,
            10.0,
            Side::Buy,
            1001,
            2001,
        );
        engine.consume_trade_event(event);

        let l2 = engine.get_level2_snapshot();
        assert_eq!(l2.timestamp, 2_000_000_000);
        assert_eq!(l2.sequence, 5);
    }

    #[test]
    fn test_level2_complex_scenario() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        // Simulate a complex order flow
        // Sell orders at 50000, 50001, 50002 (bids)
        for i in 0..3 {
            let event = TradeEvent::new(
                (i + 1) as u64,
                100 + i as u64,
                99 + i as u64,
                1_000_000_000 + (i as u64 * 1_000_000),
                50000.0 + i as f64,
                (10.0 - i as f64).max(1.0),
                Side::Sell,
                1001 + i as u64,
                2001 + i as u64,
            );
            engine.consume_trade_event(event);
        }

        // Buy orders at 50050, 50051, 50052 (asks)
        for i in 0..3 {
            let event = TradeEvent::new(
                (4 + i) as u64,
                104 + i as u64,
                103 + i as u64,
                4_000_000_000 + (i as u64 * 1_000_000),
                50050.0 + i as f64,
                (5.0 + i as f64) as f64,
                Side::Buy,
                1004 + i as u64,
                2004 + i as u64,
            );
            engine.consume_trade_event(event);
        }

        let l2 = engine.get_level2_snapshot();

        // Verify bid levels (descending)
        assert_eq!(l2.num_bids, 3);
        assert_eq!(l2.bids[0].price, 50002.0);
        assert_eq!(l2.bids[1].price, 50001.0);
        assert_eq!(l2.bids[2].price, 50000.0);

        // Verify ask levels (ascending)
        assert_eq!(l2.num_asks, 3);
        assert_eq!(l2.asks[0].price, 50050.0);
        assert_eq!(l2.asks[1].price, 50051.0);
        assert_eq!(l2.asks[2].price, 50052.0);
    }

    #[test]
    fn test_level2_level_removal_maintains_order() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        // Add 3 ask levels: 50000, 50001, 50002
        for i in 0..3 {
            let event = TradeEvent::new(
                (i + 1) as u64,
                100 + i as u64,
                99 + i as u64,
                1_000_000_000 + (i as u64 * 1_000_000),
                50000.0 + i as f64,
                10.0,
                Side::Buy,
                1001 + i as u64,
                2001 + i as u64,
            );
            engine.consume_trade_event(event);
        }

        let l2_1 = engine.get_level2_snapshot();
        assert_eq!(l2_1.num_asks, 3);
        assert_eq!(l2_1.asks[0].price, 50000.0);
        assert_eq!(l2_1.asks[1].price, 50001.0);
        assert_eq!(l2_1.asks[2].price, 50002.0);

        // Remove middle level (50001) by consuming all quantity
        let event = TradeEvent::new(4, 104, 103, 4_000_000_000, 50001.0, 10.0, Side::Buy, 1004, 2004);
        engine.consume_trade_event(event);

        let l2_2 = engine.get_level2_snapshot();
        assert_eq!(l2_2.num_asks, 2);
        // Remaining levels should be in order
        assert_eq!(l2_2.asks[0].price, 50000.0);
        assert_eq!(l2_2.asks[1].price, 50002.0);
    }

    #[test]
    fn test_level2_snapshot_independence() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        let event1 = TradeEvent::new(1, 100, 99, 1_000_000_000, 50000.0, 10.0, Side::Buy, 1001, 2001);
        engine.consume_trade_event(event1);

        let l2_1 = engine.get_level2_snapshot();
        assert_eq!(l2_1.num_asks, 1);
        assert_eq!(l2_1.asks[0].quantity, 10.0);

        // Consume some quantity
        let event2 = TradeEvent::new(2, 101, 100, 2_000_000_000, 50000.0, 3.0, Side::Buy, 1002, 2002);
        engine.consume_trade_event(event2);

        let l2_2 = engine.get_level2_snapshot();
        assert_eq!(l2_2.num_asks, 1);
        assert_eq!(l2_2.asks[0].quantity, 7.0);

        // l2_1 should be independent (snap at event1 time)
        // But since we're returning a copy, it should reflect the state at that moment
        // This test verifies that snapshots are indeed independent copies
        assert_eq!(l2_1.asks[0].quantity, 10.0); // Original snapshot value unchanged
    }

    #[test]
    fn test_level2_best_bid_ask() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        // Add bid
        let event1 = TradeEvent::new(1, 100, 99, 1_000_000_000, 50000.0, 10.0, Side::Sell, 1001, 2001);
        engine.consume_trade_event(event1);

        let l2_1 = engine.get_level2_snapshot();
        assert_eq!(l2_1.best_bid(), Some(PriceLevel::new(50000.0, 10.0)));

        // Add ask
        let event2 = TradeEvent::new(2, 101, 100, 2_000_000_000, 50050.0, 20.0, Side::Buy, 1002, 2002);
        engine.consume_trade_event(event2);

        let l2_2 = engine.get_level2_snapshot();
        assert_eq!(l2_2.best_bid(), Some(PriceLevel::new(50000.0, 10.0)));
        assert_eq!(l2_2.best_ask(), Some(PriceLevel::new(50050.0, 20.0)));
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

    // ===== PublishedSnapshot Tests =====

    #[test]
    fn test_published_snapshot_creation() {
        let bbo = BBOSnapshot::default();
        let level2 = Level2Snapshot::default();
        let agg_1s = AggregateTrade::default();
        let agg_5s = AggregateTrade::default();
        let agg_1m = AggregateTrade::default();
        let stats = Statistics24h::default();

        let snapshot = PublishedSnapshot::new(
            1_000_000_000,
            1,
            bbo,
            level2,
            agg_1s,
            agg_5s,
            agg_1m,
            stats,
        );

        assert_eq!(snapshot.timestamp, 1_000_000_000);
        assert_eq!(snapshot.sequence, 1);
    }

    #[test]
    fn test_published_snapshot_default() {
        let snapshot = PublishedSnapshot::default();

        assert_eq!(snapshot.timestamp, 0);
        assert_eq!(snapshot.sequence, 0);
        assert_eq!(snapshot.bbo.best_bid_price, 0.0);
        assert_eq!(snapshot.level2.num_bids, 0);
        assert_eq!(snapshot.stats_24h.volume_24h, 0.0);
    }

    #[test]
    fn test_generate_published_snapshot() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        // Consume some events within the same time bucket (to keep them in active_1s_bucket)
        let event1 = TradeEvent::new(1, 100, 99, 1_000_000_000, 50000.0, 10.0, Side::Sell, 1001, 2001);
        engine.consume_trade_event(event1);

        // Keep the second event within the same 1s bucket (1_500_000_000 is still within the first 1s window)
        let event2 = TradeEvent::new(2, 101, 100, 1_500_000_000, 50100.0, 5.0, Side::Buy, 1002, 2002);
        engine.consume_trade_event(event2);

        // Generate snapshot
        let ts = 1_800_000_000u64;  // Still within the same 1s bucket
        let seq = 42u64;
        let snapshot = engine.generate_published_snapshot(ts, seq);

        // Verify snapshot contains all required data
        assert_eq!(snapshot.timestamp, ts);
        assert_eq!(snapshot.sequence, seq);
        assert_eq!(snapshot.bbo.best_bid_price, 50000.0);
        assert_eq!(snapshot.bbo.best_ask_price, 50100.0);
        assert_eq!(snapshot.stats_24h.volume_24h, 15.0);
        assert_eq!(snapshot.current_agg_1s.volume, 15.0);
    }

    #[test]
    fn test_published_snapshot_reflects_engine_state() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        // Add multiple trades
        for i in 0..5 {
            let side = if i % 2 == 0 { Side::Buy } else { Side::Sell };
            let event = TradeEvent::new(
                i,
                100 + i,
                99 + i,
                1_000_000_000 + (i as u64 * 1000),
                50000.0 + (i as f64 * 10.0),
                10.0,
                side,
                1000 + i,
                2000 + i,
            );
            engine.consume_trade_event(event);
        }

        let snapshot = engine.generate_published_snapshot(10_000_000_000, 100);

        // All snapshot types should be populated
        assert!(snapshot.bbo.best_bid_price > 0.0 || snapshot.bbo.best_ask_price > 0.0);
        assert_eq!(snapshot.stats_24h.trade_count, 5);
        assert_eq!(snapshot.current_agg_1s.volume, 50.0);
    }

    #[test]
    fn test_published_snapshot_sequence_increment() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let engine = MarketDataEngine::new(receiver);

        let snap1 = engine.generate_published_snapshot(1_000_000_000, 1);
        let snap2 = engine.generate_published_snapshot(2_000_000_000, 2);
        let snap3 = engine.generate_published_snapshot(3_000_000_000, 3);

        assert_eq!(snap1.sequence, 1);
        assert_eq!(snap2.sequence, 2);
        assert_eq!(snap3.sequence, 3);
        assert!(snap2.timestamp > snap1.timestamp);
        assert!(snap3.timestamp > snap2.timestamp);
    }

    #[test]
    fn test_published_snapshot_contains_all_aggregate_types() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let mut engine = MarketDataEngine::new(receiver);

        // Create a trade at timestamp 500ms
        let trade_ts = 500_000_000u64;
        let event = TradeEvent::new(1, 100, 99, trade_ts, 50000.0, 10.0, Side::Buy, 1001, 2001);
        engine.consume_trade_event(event);

        let snapshot = engine.generate_published_snapshot(700_000_000, 1);

        // Verify all aggregate types are included in the published snapshot
        // and have been updated with trade data
        assert_eq!(snapshot.current_agg_1s.volume, 10.0);
        assert_eq!(snapshot.current_agg_5s.volume, 10.0);
        assert_eq!(snapshot.current_agg_1m.volume, 10.0);

        // Verify that all buckets show the same price
        assert_eq!(snapshot.current_agg_1s.close, 50000.0);
        assert_eq!(snapshot.current_agg_5s.close, 50000.0);
        assert_eq!(snapshot.current_agg_1m.close, 50000.0);
    }

    // ===== SnapshotTimer Tests =====

    #[test]
    fn test_snapshot_timer_sequence_starts_at_one() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let engine = Arc::new(parking_lot::Mutex::new(MarketDataEngine::new(receiver)));

        let (snapshot_tx, snapshot_rx) = channel::unbounded::<PublishedSnapshot>();

        // Spawn timer
        let timer = SnapshotTimer::spawn(engine, snapshot_tx);

        // Collect first snapshot
        match snapshot_rx.recv_timeout(std::time::Duration::from_millis(100)) {
            Ok(snapshot) => {
                // Verify sequence starts at 1, not 0
                assert!(snapshot.sequence > 0, "First snapshot sequence must be > 0");
                assert_eq!(snapshot.sequence, 1, "First snapshot sequence should be 1");
            }
            Err(_) => {
                panic!("Failed to receive first snapshot within 100ms");
            }
        }

        timer.stop();
    }

    #[test]
    fn test_snapshot_timer_sequence_increments() {
        let (_sender, receiver) = channel::unbounded::<TradeEvent>();
        let engine = Arc::new(parking_lot::Mutex::new(MarketDataEngine::new(receiver)));

        let (snapshot_tx, snapshot_rx) = channel::unbounded::<PublishedSnapshot>();

        // Spawn timer
        let timer = SnapshotTimer::spawn(engine, snapshot_tx);

        // Collect multiple snapshots
        let mut sequences = Vec::new();
        for _ in 0..3 {
            match snapshot_rx.recv_timeout(std::time::Duration::from_millis(100)) {
                Ok(snapshot) => {
                    sequences.push(snapshot.sequence);
                }
                Err(_) => break,
            }
        }

        timer.stop();

        // Verify we got at least 2 snapshots with correct sequencing
        assert!(sequences.len() >= 2, "Should collect at least 2 snapshots");
        assert_eq!(sequences[0], 1, "First snapshot should have sequence 1");

        // Verify sequences increment (allowing for wrapping, but should be sequential)
        for i in 1..sequences.len() {
            let expected = sequences[i - 1] + 1;
            assert_eq!(sequences[i], expected, "Sequences should increment by 1");
        }
    }

    // ===== TradePublisherThread 测试 =====

    #[test]
    fn test_trade_publisher_thread_creation() {
        let (trade_tx, trade_rx) = channel::bounded::<TradeEvent>(1000);
        drop(trade_tx);  // Close sender to prevent blocking

        let aeron_config = crate::aeron_publisher::AeronConfig {
            aeron_dir: "/dev/shm/aeron".to_string(),
            channel: "aeron:ipc".to_string(),
            stream_id: 11,
        };

        // This should not panic - thread is spawned successfully even if Aeron is not available
        let publisher = TradePublisherThread::spawn(trade_rx, aeron_config);
        publisher.stop();
    }

    #[test]
    fn test_trade_event_serialization() {
        use crate::aeron_publisher::{trade_to_bytes, bytes_to_trade};

        let trade = TradeEvent::new(
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

        let bytes = trade_to_bytes(&trade).expect("Should serialize");
        let deserialized = bytes_to_trade(&bytes).expect("Should deserialize");

        assert_eq!(deserialized.sequence, trade.sequence);
        assert_eq!(deserialized.order_id, trade.order_id);
        assert_eq!(deserialized.maker_order_id, trade.maker_order_id);
        assert_eq!(deserialized.timestamp, trade.timestamp);
        assert_eq!(deserialized.price, trade.price);
        assert_eq!(deserialized.quantity, trade.quantity);
        assert_eq!(deserialized.taker_id, trade.taker_id);
        assert_eq!(deserialized.maker_id, trade.maker_id);
        assert_eq!(deserialized.side, trade.side);
    }

    #[test]
    fn test_trade_event_ordering() {
        let (trade_tx, trade_rx) = channel::bounded::<TradeEvent>(1000);

        // Create multiple trade events with increasing sequences
        let trades = vec![
            TradeEvent::new(1, 100, 99, 1_000_000_000, 50000.0, 10.0, Side::Buy, 1001, 2001),
            TradeEvent::new(2, 101, 100, 2_000_000_000, 50100.0, 5.0, Side::Sell, 1002, 2002),
            TradeEvent::new(3, 102, 101, 3_000_000_000, 50200.0, 20.0, Side::Buy, 1003, 2003),
        ];

        // Send all trades
        for trade in &trades {
            trade_tx.send(*trade).expect("Should send trade");
        }
        drop(trade_tx);

        // Receive and verify order
        let mut received = Vec::new();
        for _ in 0..3 {
            match trade_rx.recv_timeout(std::time::Duration::from_millis(100)) {
                Ok(trade) => received.push(trade),
                Err(_) => break,
            }
        }

        // Verify we got all trades in order
        assert_eq!(received.len(), 3, "Should receive all 3 trades");
        for (i, trade) in received.iter().enumerate() {
            assert_eq!(trade.sequence, (i + 1) as u64, "Trades should be in order");
        }
    }
}
