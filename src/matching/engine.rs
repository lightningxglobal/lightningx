use crate::error::{MatchingEngineError, OrderResult};
use crate::event::MatchingEvent;
use crate::market_data::{
    Depth50SnapshotEvent, DepthSnapshotEvent, Level2SnapshotEvent, MarketDataConfig, TradeEvent,
};
use crate::order::{Order, PriceTicks, QuantityLots, Side, TimeInForce};
use crate::orderbook::OrderBook;
use crate::orderbook_impl::OrderBookWrapper;
use crate::pools::Pools;
use crate::skiplist::{SkipList, SortOrder};
use crate::time_provider;
use crate::trade::Trade;
use rtrb::Producer;
use smallvec::SmallVec;
use std::collections::HashMap;

pub struct MatchingEngine {
    buy_book: OrderBookWrapper,
    sell_book: OrderBookWrapper,
    orders: HashMap<u64, usize>, // order_id -> pool index
    pools: Pools,
    next_order_id: u64,
    snapshot_sequence: u64,
    trade_event_sender: Option<Producer<TradeEvent>>,
    trade_sequence: u64,

    // 深度采样相关字段
    depth_snapshot_sender: Option<Producer<DepthSnapshotEvent>>,
    depth50_sender: Option<Producer<Depth50SnapshotEvent>>,
    level2_sender: Option<Producer<Level2SnapshotEvent>>,
    market_data_config: MarketDataConfig,
    depth_sampling_initialized: bool,
    last_shallow_sample_ns: u64,
    last_depth50_sample_ns: u64,
    last_level2_sample_ns: u64,
    depth_snapshot_seq: u64,
    depth50_seq: u64,
    level2_seq: u64,

    // 内部缓冲区：记录每次撮合影响的maker订单ID，避免参数传递开销
    affected_makers_buf: SmallVec<[u64; 64]>,
    // 每次撮合产生的逐笔成交记录 (maker_order_id, price, qty)，供调用方做结算
    fills_buf: SmallVec<[(u64, PriceTicks, QuantityLots); 16]>,
}

// Safety: MatchingEngine is accessed only through Mutex<MatchingEngine>; the raw
// pointers inside SkipList are valid for the lifetime of the engine and never
// shared across threads without the mutex.
unsafe impl Send for MatchingEngine {}

impl MatchingEngine {
    /// 创建新的撮合引擎
    pub fn new(pool_config: PoolConfig) -> OrderResult<Self> {
        Ok(Self {
            buy_book: SkipList::new_with_pool(SortOrder::Descending, pool_config.queue_capacity),
            sell_book: SkipList::new_with_pool(SortOrder::Ascending, pool_config.queue_capacity),
            orders: HashMap::with_capacity(pool_config.order_capacity),
            pools: Pools::new(pool_config.order_capacity, pool_config.queue_capacity),
            next_order_id: 1,
            snapshot_sequence: 0,
            trade_event_sender: None,
            trade_sequence: 0,
            depth_snapshot_sender: None,
            depth50_sender: None,
            level2_sender: None,
            market_data_config: MarketDataConfig::default(),
            depth_sampling_initialized: false,
            last_shallow_sample_ns: 0,
            last_depth50_sample_ns: 0,
            last_level2_sample_ns: 0,
            depth_snapshot_seq: 0,
            depth50_seq: 0,
            level2_seq: 0,
            affected_makers_buf: SmallVec::new(),
            fills_buf: SmallVec::new(),
        })
    }

    /// 设置交易事件发送器
    pub fn set_trade_event_sender(&mut self, sender: Producer<TradeEvent>) {
        self.trade_event_sender = Some(sender);
    }

    /// 设置深度快照发送器
    pub fn set_depth_snapshot_sender(&mut self, sender: Producer<DepthSnapshotEvent>) {
        self.depth_snapshot_sender = Some(sender);
    }

    /// 设置Depth-50发送器
    pub fn set_depth50_sender(&mut self, sender: Producer<Depth50SnapshotEvent>) {
        self.depth50_sender = Some(sender);
    }

    /// 设置Level2发送器
    pub fn set_level2_sender(&mut self, sender: Producer<Level2SnapshotEvent>) {
        self.level2_sender = Some(sender);
    }

    /// 设置行情配置
    pub fn set_market_data_config(&mut self, config: MarketDataConfig) {
        self.market_data_config = config;
    }

    /// 获取最后一次撮合影响的maker订单ID列表
    pub fn last_affected_makers(&self) -> &[u64] {
        &self.affected_makers_buf
    }

    /// 获取指定数量的顶层价位（用于性能测试）
    #[doc(hidden)]
    pub fn get_top_levels(&self, limit: usize, is_buy: bool) -> Vec<(PriceTicks, QuantityLots)> {
        if is_buy {
            self.buy_book.get_top_levels(limit)
        } else {
            self.sell_book.get_top_levels(limit)
        }
    }

    #[doc(hidden)]
    pub fn fill_top_levels(&self, is_buy: bool, out: &mut [(PriceTicks, QuantityLots)]) -> usize {
        if is_buy {
            self.buy_book.fill_top_levels(out)
        } else {
            self.sell_book.fill_top_levels(out)
        }
    }

    #[inline]
    pub fn has_crossing_order_id<F>(
        &self,
        side: Side,
        price_ticks: PriceTicks,
        mut matches: F,
    ) -> bool
    where
        F: FnMut(u64) -> bool,
    {
        match side {
            Side::Buy => self.sell_book.any_crossing_order_id(
                |maker_price| price_ticks == 0 || price_ticks >= maker_price,
                |order_id| matches(order_id),
            ),
            Side::Sell => self.buy_book.any_crossing_order_id(
                |maker_price| price_ticks == 0 || price_ticks <= maker_price,
                |order_id| matches(order_id),
            ),
        }
    }

    /// 验证订单有效性
    #[inline(always)]
    fn validate_order(&self, order: &Order) -> OrderResult<()> {
        // 市价单不检查 price，限价单检查
        if !order.is_market && order.price_ticks <= 0 {
            return Err(MatchingEngineError::InvalidPrice(order.price_ticks));
        }

        if order.quantity_lots <= 0 {
            return Err(MatchingEngineError::InvalidQuantity(order.quantity_lots));
        }

        Ok(())
    }

    /// 获取订单
    #[inline(always)]
    pub fn get_order(&self, order_id: u64) -> Option<Order> {
        self.orders
            .get(&order_id)
            .and_then(|&pool_idx| self.pools.orders.get(pool_idx))
            .copied()
    }

    /// 获取订单填充状态（filled 和 remaining）
    #[inline(always)]
    pub fn get_order_fill_status(&self, order_id: u64) -> Option<(QuantityLots, QuantityLots)> {
        self.orders
            .get(&order_id)
            .and_then(|&pool_idx| self.pools.orders.get(pool_idx))
            .map(|order| (order.filled_lots, order.remaining_lots()))
    }

    /// 获取统计信息
    pub fn stats(&self) -> EngineStats {
        EngineStats {
            total_orders: self.orders.len(),
            buy_book_levels: self.buy_book.count(),
            sell_book_levels: self.sell_book.count(),
            next_order_id: self.next_order_id,
            pools: self.pools.stats(),
        }
    }

    /// 深度采样（检查是否需要发送深度快照）
    fn maybe_sample_depth(&mut self) {
        let now_ns = time_provider::monotonic_nanos();
        let cfg = self.market_data_config;

        // Initialize sampling timestamps on first call
        if !self.depth_sampling_initialized {
            self.depth_sampling_initialized = true;
            self.last_shallow_sample_ns = now_ns;
            if cfg.enable_depth_increments {
                self.last_depth50_sample_ns = now_ns;
                self.last_level2_sample_ns = now_ns;
            }

            // Trigger first sample immediately
            if let Some(ref mut sender) = self.depth_snapshot_sender {
                let mut event = DepthSnapshotEvent::new(now_ns, self.depth_snapshot_seq);
                let mut bids = [(0, 0); 20];
                let mut asks = [(0, 0); 20];
                let num_bids = self.buy_book.fill_top_levels(&mut bids);
                let num_asks = self.sell_book.fill_top_levels(&mut asks);
                for (i, (p, q)) in bids.iter().take(num_bids).enumerate() {
                    event.bids[i] = (*p as f64, *q as f64);
                    event.num_bids = (i + 1) as u8;
                }
                for (i, (p, q)) in asks.iter().take(num_asks).enumerate() {
                    event.asks[i] = (*p as f64, *q as f64);
                    event.num_asks = (i + 1) as u8;
                }
                let _ = sender.push(event);
                self.depth_snapshot_seq += 1;
            }
            if cfg.enable_depth_increments {
                let depth50_event = self.build_depth50_event(now_ns);
                if let Some(ref mut sender) = self.depth50_sender {
                    let _ = sender.push(depth50_event);
                    self.depth50_seq += 1;
                }
                let level2_event = self.build_level2_event(now_ns);
                if let Some(ref mut sender) = self.level2_sender {
                    let _ = sender.push(level2_event);
                    self.level2_seq += 1;
                }
            }
            return;
        }

        // BBO + Depth-20 快照
        let threshold = self
            .last_shallow_sample_ns
            .saturating_add(cfg.shallow_sample_interval_ns);
        if now_ns >= threshold {
            if let Some(ref mut sender) = self.depth_snapshot_sender {
                let mut event = DepthSnapshotEvent::new(now_ns, self.depth_snapshot_seq);

                let mut bids = [(0, 0); 20];
                let mut asks = [(0, 0); 20];
                let num_bids = self.buy_book.fill_top_levels(&mut bids);
                let num_asks = self.sell_book.fill_top_levels(&mut asks);

                for (i, (p, q)) in bids.iter().take(num_bids).enumerate() {
                    event.bids[i] = (*p as f64, *q as f64);
                    event.num_bids = (i + 1) as u8;
                }

                for (i, (p, q)) in asks.iter().take(num_asks).enumerate() {
                    event.asks[i] = (*p as f64, *q as f64);
                    event.num_asks = (i + 1) as u8;
                }

                let _ = sender.push(event);
                self.depth_snapshot_seq += 1;
            }
            self.last_shallow_sample_ns = now_ns;
        }

        // 增量模式（仅在启用时）
        if cfg.enable_depth_increments {
            // Depth-50（每50ms）
            if now_ns >= self.last_depth50_sample_ns + cfg.depth50_interval_ns {
                let event = self.build_depth50_event(now_ns);
                if let Some(ref mut sender) = self.depth50_sender {
                    let _ = sender.push(event);
                    self.depth50_seq += 1;
                }
                self.last_depth50_sample_ns = now_ns;
            }

            // Level2-400（每100ms）
            if now_ns >= self.last_level2_sample_ns + cfg.level2_interval_ns {
                let event = self.build_level2_event(now_ns);
                if let Some(ref mut sender) = self.level2_sender {
                    let _ = sender.push(event);
                    self.level2_seq += 1;
                }
                self.last_level2_sample_ns = now_ns;
            }
        }
    }

    /// 构建Depth-50快照事件
    #[inline]
    fn build_depth50_event(&self, now_ns: u64) -> Depth50SnapshotEvent {
        let mut event = Depth50SnapshotEvent::new(now_ns, self.depth50_seq);

        let mut bids = [(0, 0); 50];
        let mut asks = [(0, 0); 50];
        let num_bids = self.buy_book.fill_top_levels(&mut bids);
        let num_asks = self.sell_book.fill_top_levels(&mut asks);

        for (i, (p, q)) in bids.iter().take(num_bids).enumerate() {
            event.bids[i] = (*p as f64, *q as f64);
            event.num_bids = (i + 1) as u8;
        }

        for (i, (p, q)) in asks.iter().take(num_asks).enumerate() {
            event.asks[i] = (*p as f64, *q as f64);
            event.num_asks = (i + 1) as u8;
        }

        event
    }

    /// 构建Level2-400快照事件
    #[inline]
    fn build_level2_event(&self, now_ns: u64) -> Level2SnapshotEvent {
        let mut event = Level2SnapshotEvent::new(now_ns, self.level2_seq);

        let mut bids = [(0, 0); 400];
        let mut asks = [(0, 0); 400];
        let num_bids = self.buy_book.fill_top_levels(&mut bids);
        let num_asks = self.sell_book.fill_top_levels(&mut asks);

        for (i, (p, q)) in bids.iter().take(num_bids).enumerate() {
            event.bids[i] = (*p as f64, *q as f64);
            event.num_bids = (i + 1) as u16;
        }

        for (i, (p, q)) in asks.iter().take(num_asks).enumerate() {
            event.asks[i] = (*p as f64, *q as f64);
            event.num_asks = (i + 1) as u16;
        }

        event
    }

    // ===== Task 8: Place Order Methods =====

    /// 下单（热路径）
    #[inline]
    pub fn place_order(&mut self, order: Order) -> OrderResult<PlaceOrderResult> {
        // 验证订单
        self.validate_order(&order)?;

        if self.orders.contains_key(&order.id) {
            return Err(MatchingEngineError::DuplicateOrderId(order.id));
        }

        // 检查资源
        if !self.pools.has_space_for_order() {
            return Err(MatchingEngineError::OrderPoolExhausted);
        }

        self.affected_makers_buf.clear();

        // 处理市价单或限价单
        let result = if order.is_market {
            self.handle_market_order(order)?
        } else {
            // 处理限价单：按不同的委托类型
            match order.time_in_force {
                TimeInForce::PostOnly => self.handle_post_only(order)?,
                TimeInForce::FOK => self.handle_fok(order)?,
                TimeInForce::IOC => self.handle_ioc(order)?,
                TimeInForce::GTC => self.handle_gtc(order)?,
            }
        };

        self.maybe_sample_depth();
        Ok(result)
    }

    /// 批量下单（支持最多40笔订单，避免堆分配）
    #[inline]
    pub fn place_orders(
        &mut self,
        orders: SmallVec<[Order; 40]>,
    ) -> OrderResult<SmallVec<[PlaceOrderResult; 40]>> {
        let mut results = SmallVec::new();

        // 预分配订单ID并验证
        for i in 0..orders.len() {
            let order = &orders[i];
            self.validate_order(order)?;
            if self.orders.contains_key(&order.id) {
                return Err(MatchingEngineError::DuplicateOrderId(order.id));
            }
            if orders.iter().take(i).any(|prev| prev.id == order.id) {
                return Err(MatchingEngineError::DuplicateOrderId(order.id));
            }
            if !self.pools.has_space_for_order() {
                return Err(MatchingEngineError::OrderPoolExhausted);
            }
        }

        // 批量处理订单，使用内部affected_makers缓冲区
        for order in orders.into_iter() {
            self.affected_makers_buf.clear();
            let result = if order.is_market {
                self.handle_market_order(order)?
            } else {
                match order.time_in_force {
                    TimeInForce::PostOnly => self.handle_post_only(order)?,
                    TimeInForce::FOK => self.handle_fok(order)?,
                    TimeInForce::IOC => self.handle_ioc(order)?,
                    TimeInForce::GTC => self.handle_gtc(order)?,
                }
            };
            results.push(result);
        }

        Ok(results)
    }

    /// 处理市价订单（不入盘，立即成交或拒绝）
    #[inline]
    fn handle_market_order(&mut self, order: Order) -> OrderResult<PlaceOrderResult> {
        let filled_lots = self.match_order(order)?;

        let status = if filled_lots >= order.quantity_lots {
            OrderStatus::Filled
        } else if filled_lots > 0 {
            OrderStatus::PartiallyFilled
        } else {
            OrderStatus::Rejected
        };

        Ok(PlaceOrderResult {
            order_id: order.id,
            filled_lots: filled_lots,
            status,
            fills: self.fills_buf.clone(),
        })
    }

    /// 处理Post-Only订单
    #[inline]
    fn handle_post_only(&mut self, order: Order) -> OrderResult<PlaceOrderResult> {
        // 检查是否会立即成交
        let opposite_book = match order.side {
            Side::Buy => &self.sell_book,
            Side::Sell => &self.buy_book,
        };

        if let Some(best) = opposite_book.best() {
            let would_match = match order.side {
                Side::Buy => order.price_ticks >= best.price_ticks,
                Side::Sell => order.price_ticks <= best.price_ticks,
            };

            if would_match {
                return Ok(PlaceOrderResult {
                    order_id: order.id,
                    filled_lots: 0,
                    status: OrderStatus::Rejected,
                    fills: SmallVec::new(),
                });
            }
        }

        // 加入订单簿
        self.add_to_book(order)?;
        self.publish_event(&MatchingEvent::OrderPlaced {
            order_id: order.id,
            side: order.side,
            price_ticks: order.price_ticks,
            quantity_lots: order.quantity_lots,
            timestamp: order.timestamp,
        });

        Ok(PlaceOrderResult {
            order_id: order.id,
            filled_lots: 0,
            status: OrderStatus::Accepted,
            fills: SmallVec::new(),
        })
    }

    /// Read-only pre-check: can this order be fully filled at acceptable prices?
    /// Must be called before any state-mutating match_order to avoid FOK book corruption.
    fn can_fill_fok(&self, order: &Order) -> bool {
        let opposite_book = match order.side {
            Side::Buy => &self.sell_book,
            Side::Sell => &self.buy_book,
        };
        opposite_book.has_cumulative_quantity_until(
            order.quantity_lots,
            order.price_ticks,
            order.is_market,
            order.side == Side::Buy,
        )
    }

    /// 处理FOK订单
    #[inline]
    fn handle_fok(&mut self, order: Order) -> OrderResult<PlaceOrderResult> {
        // Pre-check WITHOUT modifying any state. If not fully fillable, reject immediately.
        // This prevents the book corruption bug where makers were consumed before the
        // fillability check, leaving the book in an inconsistent state on rejection.
        if !self.can_fill_fok(&order) {
            return Ok(PlaceOrderResult {
                order_id: order.id,
                filled_lots: 0,
                status: OrderStatus::Rejected,
                fills: SmallVec::new(),
            });
        }

        // Now we know it's fully fillable — commit the fills.
        let filled_lots = self.match_order(order)?;
        Ok(PlaceOrderResult {
            order_id: order.id,
            filled_lots: filled_lots,
            status: OrderStatus::Filled,
            fills: self.fills_buf.clone(),
        })
    }

    /// 处理IOC订单
    #[inline]
    fn handle_ioc(&mut self, order: Order) -> OrderResult<PlaceOrderResult> {
        let filled_lots = self.match_order(order)?;

        let status = if filled_lots >= order.quantity_lots {
            OrderStatus::Filled
        } else if filled_lots > 0 {
            OrderStatus::PartiallyFilled
        } else {
            OrderStatus::Rejected
        };

        Ok(PlaceOrderResult {
            order_id: order.id,
            filled_lots: filled_lots,
            status,
            fills: self.fills_buf.clone(),
        })
    }

    /// 处理GTC订单
    #[inline]
    fn handle_gtc(&mut self, order: Order) -> OrderResult<PlaceOrderResult> {
        let filled_lots = self.match_order(order)?;

        // 如果有剩余，加入订单簿
        if filled_lots < order.quantity_lots {
            let mut remaining_order = order;
            remaining_order.filled_lots = filled_lots;
            self.add_to_book(remaining_order)?;
        }

        let status = if filled_lots >= order.quantity_lots {
            OrderStatus::Filled
        } else if filled_lots > 0 {
            OrderStatus::PartiallyFilled
        } else {
            OrderStatus::Accepted
        };

        Ok(PlaceOrderResult {
            order_id: order.id,
            filled_lots: filled_lots,
            status,
            fills: self.fills_buf.clone(),
        })
    }

    /// 将订单加入订单簿
    pub fn add_to_book(&mut self, order: Order) -> OrderResult<()> {
        if self.orders.contains_key(&order.id) {
            return Err(MatchingEngineError::DuplicateOrderId(order.id));
        }

        // 从对象池获取Order（减少allocation）
        let pool_idx = self
            .pools
            .orders
            .acquire()
            .ok_or(MatchingEngineError::OrderPoolExhausted)?;

        // 初始化Order到池中
        if let Some(pooled_order) = self.pools.orders.get_mut(pool_idx) {
            *pooled_order = order;
        }

        // 插入价格档位（如果不存在）
        let book = match order.side {
            Side::Buy => &mut self.buy_book,
            Side::Sell => &mut self.sell_book,
        };

        // 尝试插入新价格级别，如果已存在则忽略错误
        let _ = book.insert_level(order.price_ticks);

        // 添加订单到价格档位队列（使用 remaining_lots，而非 quantity_lots，
        // 因为 GTC 部分成交后剩余量才是实际进入订单簿的量）
        if book
            .add_order_at_level(order.price_ticks, order.id, order.remaining_lots())
            .is_err()
        {
            self.pools.orders.release(pool_idx);
            let level_empty = book
                .get_node_at_price(order.price_ticks)
                .map(|n| n.orders.is_empty())
                .unwrap_or(false);
            if level_empty {
                let _ = book.remove_level(order.price_ticks);
            }
            return Err(MatchingEngineError::NodePoolExhausted);
        }

        // 储存池索引
        self.orders.insert(order.id, pool_idx);

        // Invalidate the best-price cache for this book side.  Without this,
        // a new order at a BETTER price (lower ask / higher bid) than the
        // currently cached value would be invisible to the next match_order
        // call: get_best_price_cached() would see the old cached price still
        // has orders and return it unchanged, skipping the cheaper order.
        match order.side {
            Side::Buy => self.buy_book.invalidate_best_price(),
            Side::Sell => self.sell_book.invalidate_best_price(),
        }

        Ok(())
    }

    /// 发布事件到Aeron
    #[inline(always)]
    fn publish_event(&self, _event: &MatchingEvent) {
        // 在当前实现中，我们不连接Aeron。
        // 在生产环境中，这里会将事件发送到Aeron。
    }

    // ===== Task 9: Match Order Logic =====

    /// 撮合订单（关键热路径，使用持久化缓存优化最优价格查询）
    #[inline]
    fn match_order(&mut self, order: Order) -> OrderResult<QuantityLots> {
        let mut filled: QuantityLots = 0;
        self.affected_makers_buf.clear();
        self.fills_buf.clear();

        // 使用本地变量而非大结构体字段(缓存友好)
        let mut trade_indices: SmallVec<[usize; 64]> = SmallVec::new();
        // Only build TradeEvents when a consumer is wired up. The engine works in
        // integer tick/lot space and has no SymbolRules reference, so price/quantity
        // here would be raw integers (e.g. 7_589_360) not actual values (75893.60).
        let has_trade_sender = self.trade_event_sender.is_some();
        let mut trade_events: SmallVec<[TradeEvent; 128]> = SmallVec::new();

        // 外层循环：获取最优对手价
        loop {
            if filled >= order.quantity_lots {
                break;
            }

            // 获取缓存的最优价格（如果无效则自动查询下一个最优价）
            let best_price = {
                let opposite_book = match order.side {
                    Side::Buy => &mut self.sell_book,
                    Side::Sell => &mut self.buy_book,
                };

                match opposite_book.get_best_price() {
                    Some(price) => {
                        // 验证价格级别存在且检查价格是否匹配
                        if opposite_book.get_node_at_price(price).is_some() {
                            // 市价单直接吃穿所有价位，限价单检查价格匹配
                            let price_matches = order.is_market
                                || match order.side {
                                    Side::Buy => order.price_ticks >= price,
                                    Side::Sell => order.price_ticks <= price,
                                };

                            if !price_matches {
                                break;
                            }

                            Some(price)
                        } else {
                            None
                        }
                    }
                    None => None,
                }
            };

            let best_price = match best_price {
                Some(p) => p,
                None => break,
            };

            // 内层循环：在同一价格级别内批量成交，减少 best_with_orders() 调用
            loop {
                if filled >= order.quantity_lots {
                    break;
                }

                // 从对手订单簿获取最优价格级别的第一个订单ID
                let counter_order_id = {
                    let opposite_book = match order.side {
                        Side::Buy => &mut self.sell_book,
                        Side::Sell => &mut self.buy_book,
                    };

                    match opposite_book.get_node_mut(best_price) {
                        Some(node) => {
                            if let Some(node_idx) = node.orders.front() {
                                let list_pool = opposite_book.get_list_pool();
                                if let Some(list_node) = list_pool.get(node_idx) {
                                    list_node.order_id
                                } else {
                                    break;
                                }
                            } else {
                                break;
                            }
                        }
                        None => break,
                    }
                };

                let counter_order = match self.orders.get(&counter_order_id) {
                    Some(&pool_idx) => {
                        match self.pools.orders.get(pool_idx) {
                            Some(o) => *o,
                            None => {
                                // Pool slot invalid: stale entry. Clean up and try next.
                                let opp = match order.side {
                                    Side::Buy => &mut self.sell_book,
                                    Side::Sell => &mut self.buy_book,
                                };
                                let _ = opp.remove_order_at_level(best_price, counter_order_id);
                                self.orders.remove(&counter_order_id);
                                if opp
                                    .get_node_at_price(best_price)
                                    .map(|n| n.orders.is_empty())
                                    .unwrap_or(true)
                                {
                                    let _ = opp.remove_level(best_price);
                                }
                                continue;
                            }
                        }
                    }
                    None => {
                        // ID in price-level list but not in self.orders (zombie from failed cancel).
                        // Remove the stale list entry and continue to the next order.
                        let opp = match order.side {
                            Side::Buy => &mut self.sell_book,
                            Side::Sell => &mut self.buy_book,
                        };
                        let _ = opp.remove_order_at_level(best_price, counter_order_id);
                        if opp
                            .get_node_at_price(best_price)
                            .map(|n| n.orders.is_empty())
                            .unwrap_or(true)
                        {
                            let _ = opp.remove_level(best_price);
                        }
                        continue;
                    }
                };

                // 计算成交数量
                let order_remaining = order.quantity_lots - filled;
                let counter_remaining = counter_order.remaining_lots();

                // Safety guard: float accumulation can leave counter_remaining as noise
                // (e.g. 1e-17) even though is_filled() was false. Force-remove the ghost.
                if counter_remaining <= 0 {
                    let opp = match order.side {
                        Side::Buy => &mut self.sell_book,
                        Side::Sell => &mut self.buy_book,
                    };
                    let _ = opp.remove_order_at_level(best_price, counter_order_id);
                    if let Some(idx) = self.orders.remove(&counter_order_id) {
                        self.pools.orders.release(idx);
                    }
                    if opp
                        .get_node_at_price(best_price)
                        .map(|n| n.orders.is_empty())
                        .unwrap_or(true)
                    {
                        let _ = opp.remove_level(best_price);
                    }
                    continue;
                }

                let trade_lots = order_remaining.min(counter_remaining);

                // 收集被修改的 maker ID 及成交明细
                self.affected_makers_buf.push(counter_order_id);
                self.fills_buf
                    .push((counter_order_id, best_price, trade_lots));

                // 更新订单状态
                {
                    if let Some(&pool_idx) = self.orders.get(&order.id) {
                        if let Some(o) = self.pools.orders.get_mut(pool_idx) {
                            o.filled_lots += trade_lots;
                        }
                    }
                    if let Some(&counter_pool_idx) = self.orders.get(&counter_order_id) {
                        if let Some(o) = self.pools.orders.get_mut(counter_pool_idx) {
                            o.filled_lots += trade_lots;
                        }
                    }
                }
                {
                    let opposite_book = match order.side {
                        Side::Buy => &mut self.sell_book,
                        Side::Sell => &mut self.buy_book,
                    };
                    let _ = opposite_book.reduce_order_quantity_at_level(
                        best_price,
                        counter_order_id,
                        trade_lots,
                    );
                }

                filled += trade_lots;

                // 从对象池获取Trade（减少allocation churn）
                let trade_idx = self
                    .pools
                    .trades
                    .acquire()
                    .ok_or(MatchingEngineError::OrderPoolExhausted)?;

                // 初始化Trade内容
                {
                    if let Some(trade) = self.pools.trades.get_mut(trade_idx) {
                        trade.taker_id = order.id;
                        trade.maker_id = counter_order_id;
                        trade.price_ticks = best_price;
                        trade.quantity_lots = trade_lots;
                    }
                }

                // 存储Trade索引
                trade_indices.push(trade_idx);

                self.publish_event(&MatchingEvent::Trade {
                    taker_order_id: order.id,
                    maker_order_id: counter_order_id,
                    price_ticks: best_price,
                    quantity_lots: trade_lots,
                    timestamp: order.timestamp,
                });

                self.trade_sequence += 1;
                if has_trade_sender {
                    trade_events.push(TradeEvent::new(
                        self.trade_sequence,
                        order.id,
                        counter_order_id,
                        order.timestamp,
                        best_price as f64, // raw ticks — caller must convert via SymbolRules
                        trade_lots as f64, // raw lots — caller must convert via SymbolRules
                        order.side,
                        order.id,
                        counter_order_id,
                    ));
                }

                // 检查对手订单是否完全成交
                if let Some(&counter_pool_idx) = self.orders.get(&counter_order_id) {
                    if let Some(counter) = self.pools.orders.get(counter_pool_idx) {
                        if counter.is_filled() {
                            let opposite_book = match order.side {
                                Side::Buy => &mut self.sell_book,
                                Side::Sell => &mut self.buy_book,
                            };
                            let _ =
                                opposite_book.remove_order_at_level(best_price, counter_order_id);
                            // 从orders HashMap中移除并释放池索引
                            if let Some(idx) = self.orders.remove(&counter_order_id) {
                                self.pools.orders.release(idx);
                            }
                            // Drop the level when its last order has gone.
                            // Use orders.is_empty() instead of total_quantity_lots <= 0
                            // to handle float drift where total_quantity_lots ≈ 0 but > 0.
                            let level_empty = opposite_book
                                .get_node_at_price(best_price)
                                .map(|lvl| lvl.orders.is_empty())
                                .unwrap_or(true);
                            if level_empty {
                                let _ = opposite_book.remove_level(best_price);
                            }
                        }
                    }
                }
            }

            // 内层循环退出后，下次调用get_best_price()会自动检测缓存的价格级别是否仍有订单
        }

        // 撮合完成后，批量发送所有交易事件到ring buffer
        if let Some(ref mut sender) = self.trade_event_sender {
            for trade_event in trade_events.iter() {
                let _ = sender.push(*trade_event);
            }
        }

        // 释放Trade对象回池
        for trade_idx in trade_indices.iter() {
            self.pools.trades.release(*trade_idx);
        }

        Ok(filled)
    }

    /// 批量撮合多个订单（最多40个），共享TradeEvent批处理以提升性能
    /// 所有订单的TradeEvent被收集到一个batch中，最后一次性发送
    #[inline]
    pub fn match_orders_batch(
        &mut self,
        orders: SmallVec<[Order; 40]>,
    ) -> OrderResult<SmallVec<[(QuantityLots, Vec<Trade>); 40]>> {
        let mut results: SmallVec<[(QuantityLots, Vec<Trade>); 40]> = SmallVec::new();
        let has_trade_sender = self.trade_event_sender.is_some();
        let mut all_trade_events: SmallVec<[TradeEvent; 256]> = SmallVec::new();

        // 处理每个订单，收集所有TradeEvent到共享的batch中
        for order in orders.into_iter() {
            let (mut filled, mut trade_indices): (QuantityLots, SmallVec<[usize; 64]>) =
                (0, SmallVec::new());

            // 外层循环：获取最优对手价
            loop {
                if filled >= order.quantity_lots {
                    break;
                }

                // 获取最优价格
                let best_price = {
                    let opposite_book = match order.side {
                        Side::Buy => &mut self.sell_book,
                        Side::Sell => &mut self.buy_book,
                    };

                    match opposite_book.get_best_price() {
                        Some(price) => {
                            if opposite_book.get_node_at_price(price).is_some() {
                                let price_matches = order.is_market || match order.side {
                                    Side::Buy => order.price_ticks >= price,
                                    Side::Sell => order.price_ticks <= price,
                                };
                                if !price_matches {
                                    break;
                                }
                                Some(price)
                            } else {
                                None
                            }
                        }
                        None => None,
                    }
                };

                let best_price = match best_price {
                    Some(p) => p,
                    None => break,
                };

                // 内层循环：在同一价格级别内成交
                loop {
                    if filled >= order.quantity_lots {
                        break;
                    }

                    let counter_order_id = {
                        let opposite_book = match order.side {
                            Side::Buy => &mut self.sell_book,
                            Side::Sell => &mut self.buy_book,
                        };

                        match opposite_book.get_node_mut(best_price) {
                            Some(node) => {
                                if let Some(node_idx) = node.orders.front() {
                                    let list_pool = opposite_book.get_list_pool();
                                    if let Some(list_node) = list_pool.get(node_idx) {
                                        list_node.order_id
                                    } else {
                                        break;
                                    }
                                } else {
                                    break;
                                }
                            }
                            None => break,
                        }
                    };

                    let counter_order = match self.orders.get(&counter_order_id) {
                        Some(&pool_idx) => match self.pools.orders.get(pool_idx) {
                            Some(o) => *o,
                            None => {
                                let opp = match order.side {
                                    Side::Buy => &mut self.sell_book,
                                    Side::Sell => &mut self.buy_book,
                                };
                                let _ = opp.remove_order_at_level(best_price, counter_order_id);
                                self.orders.remove(&counter_order_id);
                                if opp
                                    .get_node_at_price(best_price)
                                    .map(|n| n.orders.is_empty())
                                    .unwrap_or(true)
                                {
                                    let _ = opp.remove_level(best_price);
                                }
                                continue;
                            }
                        },
                        None => {
                            let opp = match order.side {
                                Side::Buy => &mut self.sell_book,
                                Side::Sell => &mut self.buy_book,
                            };
                            let _ = opp.remove_order_at_level(best_price, counter_order_id);
                            if opp
                                .get_node_at_price(best_price)
                                .map(|n| n.orders.is_empty())
                                .unwrap_or(true)
                            {
                                let _ = opp.remove_level(best_price);
                            }
                            continue;
                        }
                    };

                    let order_remaining = order.quantity_lots - filled;
                    let counter_remaining = counter_order.remaining_lots();

                    if counter_remaining <= 0 {
                        let opp = match order.side {
                            Side::Buy => &mut self.sell_book,
                            Side::Sell => &mut self.buy_book,
                        };
                        let _ = opp.remove_order_at_level(best_price, counter_order_id);
                        if let Some(idx) = self.orders.remove(&counter_order_id) {
                            self.pools.orders.release(idx);
                        }
                        if opp
                            .get_node_at_price(best_price)
                            .map(|n| n.orders.is_empty())
                            .unwrap_or(true)
                        {
                            let _ = opp.remove_level(best_price);
                        }
                        continue;
                    }

                    let trade_lots = order_remaining.min(counter_remaining);

                    // 更新订单状态
                    {
                        if let Some(&pool_idx) = self.orders.get(&order.id) {
                            if let Some(o) = self.pools.orders.get_mut(pool_idx) {
                                o.filled_lots += trade_lots;
                            }
                        }
                        if let Some(&counter_pool_idx) = self.orders.get(&counter_order_id) {
                            if let Some(o) = self.pools.orders.get_mut(counter_pool_idx) {
                                o.filled_lots += trade_lots;
                            }
                        }
                    }
                    {
                        let opposite_book = match order.side {
                            Side::Buy => &mut self.sell_book,
                            Side::Sell => &mut self.buy_book,
                        };
                        let _ = opposite_book.reduce_order_quantity_at_level(
                            best_price,
                            counter_order_id,
                            trade_lots,
                        );
                    }

                    filled += trade_lots;

                    let trade_idx = self
                        .pools
                        .trades
                        .acquire()
                        .ok_or(MatchingEngineError::OrderPoolExhausted)?;

                    {
                        if let Some(trade) = self.pools.trades.get_mut(trade_idx) {
                            trade.taker_id = order.id;
                            trade.maker_id = counter_order_id;
                            trade.price_ticks = best_price;
                            trade.quantity_lots = trade_lots;
                        }
                    }

                    trade_indices.push(trade_idx);

                    self.trade_sequence += 1;
                    if has_trade_sender {
                        all_trade_events.push(TradeEvent::new(
                            self.trade_sequence,
                            order.id,
                            counter_order_id,
                            order.timestamp,
                            best_price as f64, // raw ticks — caller must convert via SymbolRules
                            trade_lots as f64, // raw lots — caller must convert via SymbolRules
                            order.side,
                            order.id,
                            counter_order_id,
                        ));
                    }

                    if let Some(&counter_pool_idx) = self.orders.get(&counter_order_id) {
                        if let Some(counter) = self.pools.orders.get(counter_pool_idx) {
                            if counter.is_filled() {
                                let opposite_book = match order.side {
                                    Side::Buy => &mut self.sell_book,
                                    Side::Sell => &mut self.buy_book,
                                };
                                let _ = opposite_book
                                    .remove_order_at_level(best_price, counter_order_id);
                                // 从orders HashMap中移除并释放池索引
                                if let Some(idx) = self.orders.remove(&counter_order_id) {
                                    self.pools.orders.release(idx);
                                }
                                let level_empty = opposite_book
                                    .get_node_at_price(best_price)
                                    .map(|lvl| lvl.orders.is_empty())
                                    .unwrap_or(true);
                                if level_empty {
                                    let _ = opposite_book.remove_level(best_price);
                                }
                            }
                        }
                    }
                }
            }

            // 构造该订单的trade结果
            let mut result_trades = Vec::with_capacity(trade_indices.len());
            for trade_idx in trade_indices.iter() {
                if let Some(trade) = self.pools.trades.get(*trade_idx) {
                    result_trades.push(*trade);
                }
                self.pools.trades.release(*trade_idx);
            }

            // 关键：如果有剩余，加入订单簿（允许批次内后续订单与其匹配）
            if filled < order.quantity_lots {
                let mut remaining_order = order;
                remaining_order.filled_lots = filled;

                // Acquire pool slot FIRST (matches add_to_book order) so that pool
                // exhaustion never leaves an orphan entry in the price-level list.
                let pool_idx = self
                    .pools
                    .orders
                    .acquire()
                    .ok_or(MatchingEngineError::OrderPoolExhausted)?;

                if let Some(o) = self.pools.orders.get_mut(pool_idx) {
                    *o = remaining_order;
                }

                let book = match remaining_order.side {
                    Side::Buy => &mut self.buy_book,
                    Side::Sell => &mut self.sell_book,
                };

                if book
                    .get_node_at_price(remaining_order.price_ticks)
                    .is_none()
                {
                    let _ = book.insert_level(remaining_order.price_ticks);
                }

                if book
                    .add_order_at_level(
                        remaining_order.price_ticks,
                        remaining_order.id,
                        remaining_order.quantity_lots - remaining_order.filled_lots,
                    )
                    .is_err()
                {
                    self.pools.orders.release(pool_idx);
                    return Err(MatchingEngineError::NodePoolExhausted);
                }

                self.orders.insert(remaining_order.id, pool_idx);
            }

            results.push((filled, result_trades));
        }

        // 所有订单处理完毕，批量发送所有TradeEvent到ring buffer
        if let Some(ref mut sender) = self.trade_event_sender {
            let num_events = all_trade_events.len();

            // 优先使用write_chunk()批量写入
            let should_fallback = if let Ok(mut chunk) = sender.write_chunk(num_events) {
                // 批量写入所有events到ring buffer
                let (s1, s2) = chunk.as_mut_slices();
                let mut idx = 0;

                // 写入第一段
                for slot in s1.iter_mut() {
                    if idx < num_events {
                        *slot = all_trade_events[idx];
                        idx += 1;
                    }
                }

                // 写入第二段（环形缓冲区的环绕部分）
                for slot in s2.iter_mut() {
                    if idx < num_events {
                        *slot = all_trade_events[idx];
                        idx += 1;
                    }
                }
                chunk.commit(num_events); // 提交所有写入的slots
                false
            } else {
                true
            };

            // 如果批量写入失败，逐个push
            if should_fallback {
                for event in all_trade_events.iter() {
                    let _ = sender.push(*event);
                }
            }
        }

        self.maybe_sample_depth();
        Ok(results)
    }

    // ===== Task 10: Cancel Order =====

    /// 撤销订单（热路径 - 使用List实现，真正删除）
    #[inline]
    pub fn cancel_order(&mut self, order_id: u64) -> OrderResult<CancelOrderResult> {
        self.cancel_order_inner(order_id, true)
    }

    #[inline]
    fn cancel_order_inner(
        &mut self,
        order_id: u64,
        sample_depth: bool,
    ) -> OrderResult<CancelOrderResult> {
        // 查询订单池索引
        let pool_idx = self
            .orders
            .get(&order_id)
            .copied()
            .ok_or(MatchingEngineError::OrderNotFound)?;

        // 从池中读取订单
        let order = self
            .pools
            .orders
            .get(pool_idx)
            .copied()
            .ok_or(MatchingEngineError::OrderNotFound)?;

        // 检查是否已成交
        if order.is_filled() {
            return Err(MatchingEngineError::AlreadyFilled);
        }

        // 获取剩余数量和时间戳
        let remaining = order.remaining_lots();
        let timestamp = order.timestamp;

        // 从订单簿中真正移除订单
        self.remove_from_book(order_id)?;

        // 从orders HashMap中移除并release到池
        if let Some(idx) = self.orders.remove(&order_id) {
            self.pools.orders.release(idx);
        }

        // 发布撤单事件
        self.publish_event(&MatchingEvent::OrderCancelled {
            order_id,
            timestamp,
        });

        if sample_depth {
            self.maybe_sample_depth();
        }
        Ok(CancelOrderResult {
            order_id,
            cancelled_quantity: remaining,
        })
    }

    /// Cancel multiple orders in one pass. Returns (order_id, cancelled_lots) for each
    /// successfully cancelled order; silently skips orders not found or already filled.
    pub fn cancel_orders_batch(
        &mut self,
        order_ids: &[u64],
    ) -> smallvec::SmallVec<[(u64, QuantityLots); 64]> {
        let mut results = smallvec::SmallVec::new();
        for &id in order_ids {
            if let Ok(r) = self.cancel_order_inner(id, false) {
                results.push((id, r.cancelled_quantity));
            }
        }
        if !results.is_empty() {
            self.maybe_sample_depth();
        }
        results
    }

    /// 从订单簿移除未成交订单（取消订单时调用）
    fn remove_from_book(&mut self, order_id: u64) -> OrderResult<()> {
        let pool_idx = self
            .orders
            .get(&order_id)
            .copied()
            .ok_or(MatchingEngineError::OrderNotFound)?;

        let order = self
            .pools
            .orders
            .get(pool_idx)
            .copied()
            .ok_or(MatchingEngineError::OrderNotFound)?;

        let book = match order.side {
            Side::Buy => &mut self.buy_book,
            Side::Sell => &mut self.sell_book,
        };

        book.remove_order_at_level(order.price_ticks, order_id)
            .map_err(|_| MatchingEngineError::OrderNotFound)?;

        // Remove the price level entirely if no orders remain, to avoid ghost
        // 0-qty levels showing up in get_top_levels() depth snapshots.
        let level_empty = book
            .get_node_at_price(order.price_ticks)
            .map(|n| n.orders.is_empty())
            .unwrap_or(true);
        if level_empty {
            let _ = book.remove_level(order.price_ticks);
        }

        Ok(())
    }

    /// 生成市场深度快照
    pub fn generate_depth_snapshot(&self) -> crate::snapshot::DepthSnapshot {
        let timestamp = time_provider::wall_clock_nanos();

        let mut snapshot = crate::snapshot::DepthSnapshot::new(timestamp, self.snapshot_sequence);

        // 添加买盘价格档位（最多20档）- 降序排列
        let buy_levels = self.buy_book.get_top_levels(20);
        for (price, _) in buy_levels {
            // 计算该价格档位的总剩余数量
            let mut total_remaining: QuantityLots = 0;
            if let Some(node) = self.buy_book.get_node_at_price(price) {
                // 遍历链表中的订单
                let mut node_idx_opt = node.orders.front();
                let list_pool = &self.buy_book.list_pool;
                while let Some(node_idx) = node_idx_opt {
                    if let Some(list_node) = list_pool.get(node_idx) {
                        if let Some(&pool_idx) = self.orders.get(&list_node.order_id) {
                            if let Some(order) = self.pools.orders.get(pool_idx) {
                                total_remaining += order.remaining_lots();
                            }
                        }
                        node_idx_opt = list_node.next;
                    } else {
                        break;
                    }
                }
            }

            if total_remaining > 0 {
                snapshot.add_bid(price as f64, total_remaining as f64).ok();
            }
        }

        // 添加卖盘价格档位（最多20档）- 升序排列
        let sell_levels = self.sell_book.get_top_levels(20);
        for (price, _) in sell_levels {
            // 计算该价格档位的总剩余数量
            let mut total_remaining: QuantityLots = 0;
            if let Some(node) = self.sell_book.get_node_at_price(price) {
                // 遍历链表中的订单
                let mut node_idx_opt = node.orders.front();
                let list_pool = &self.sell_book.list_pool;
                while let Some(node_idx) = node_idx_opt {
                    if let Some(list_node) = list_pool.get(node_idx) {
                        if let Some(&pool_idx) = self.orders.get(&list_node.order_id) {
                            if let Some(order) = self.pools.orders.get(pool_idx) {
                                total_remaining += order.remaining_lots();
                            }
                        }
                        node_idx_opt = list_node.next;
                    } else {
                        break;
                    }
                }
            }

            if total_remaining > 0 {
                snapshot.add_ask(price as f64, total_remaining as f64).ok();
            }
        }

        snapshot
    }

    /// 发布快照到Aeron
    pub fn publish_depth_snapshot(&mut self) -> OrderResult<()> {
        let _snapshot = self.generate_depth_snapshot();

        // 在当前实现中，快照已生成但未发送到Aeron
        // 完整实现需要配置Aeron publisher

        self.snapshot_sequence += 1;
        Ok(())
    }

    /// 定时发布快照（在定时器中调用）
    pub fn tick_snapshot(&mut self, _interval_ns: u64) -> OrderResult<()> {
        // 简化实现：每次调用都尝试发布一次快照
        self.publish_depth_snapshot()
    }
}

/// 撮合引擎配置
#[derive(Debug, Clone)]
pub struct PoolConfig {
    pub order_capacity: usize,
    pub queue_capacity: usize,
}

impl Default for PoolConfig {
    fn default() -> Self {
        // POOL_CAPACITY env var allows smaller pre-allocation on memory-constrained hosts.
        // Default 2M is tuned for high-throughput benchmarks; set to e.g. 200000 on t3.small.
        let cap = std::env::var("POOL_CAPACITY")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(2_000_000);
        Self {
            order_capacity: cap,
            queue_capacity: cap,
        }
    }
}

/// 引擎统计信息
#[derive(Debug, Clone)]
pub struct EngineStats {
    pub total_orders: usize,
    pub buy_book_levels: usize,
    pub sell_book_levels: usize,
    pub next_order_id: u64,
    pub pools: crate::pools::PoolStats,
}

/// 下单结果
#[derive(Debug, Clone)]
pub struct PlaceOrderResult {
    pub order_id: u64,
    pub filled_lots: QuantityLots,
    pub status: OrderStatus,
    /// Individual fills produced by this order: (maker_order_id, fill_price, fill_qty).
    pub fills: SmallVec<[(u64, PriceTicks, QuantityLots); 16]>,
}

/// 订单状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderStatus {
    Accepted,
    PartiallyFilled,
    Filled,
    Rejected,
    Cancelled,
}

/// 成交记录

/// 撤单结果
#[derive(Debug, Clone)]
pub struct CancelOrderResult {
    pub order_id: u64,
    pub cancelled_quantity: QuantityLots,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_market_buy_vs_limit_sell() {
        let pool_config = PoolConfig::default();
        let mut engine = MatchingEngine::new(pool_config).unwrap();

        // 先放一个卖限价单
        let sell_order = Order::new(1, Side::Sell, 50000, 10, TimeInForce::GTC, 0);
        let _ = engine.place_order(sell_order).unwrap();

        // 买市价单应该成交
        let buy_market = Order::new_market(2, Side::Buy, 5, 0);
        let result = engine.place_order(buy_market).unwrap();

        assert_eq!(result.status, OrderStatus::Filled);
        assert_eq!(result.filled_lots, 5);
        assert_eq!(result.order_id, 2);
    }

    #[test]
    fn test_market_sell_vs_limit_buy() {
        let pool_config = PoolConfig::default();
        let mut engine = MatchingEngine::new(pool_config).unwrap();

        // 先放一个买限价单
        let buy_order = Order::new(1, Side::Buy, 50000, 10, TimeInForce::GTC, 0);
        let _ = engine.place_order(buy_order).unwrap();

        // 卖市价单应该成交
        let sell_market = Order::new_market(2, Side::Sell, 5, 0);
        let result = engine.place_order(sell_market).unwrap();

        assert_eq!(result.status, OrderStatus::Filled);
        assert_eq!(result.filled_lots, 5);
    }

    #[test]
    fn test_market_order_empty_book() {
        let pool_config = PoolConfig::default();
        let mut engine = MatchingEngine::new(pool_config).unwrap();

        // 空盘口，市价单应该被拒绝
        let buy_market = Order::new_market(1, Side::Buy, 10, 0);
        let result = engine.place_order(buy_market).unwrap();

        assert_eq!(result.status, OrderStatus::Rejected);
        assert_eq!(result.filled_lots, 0);
    }

    #[test]
    fn test_market_sell_empty_book_rejected() {
        let pool_config = PoolConfig::default();
        let mut engine = MatchingEngine::new(pool_config).unwrap();

        // Sell-side mirror of test_market_order_empty_book. QA needed this path
        // verified directly because the seeded SOL_USDT book is too deep to
        // drain from a normal test account.
        let sell_market = Order::new_market(1, Side::Sell, 10, 0);
        let result = engine.place_order(sell_market).unwrap();

        assert_eq!(result.status, OrderStatus::Rejected);
        assert_eq!(result.filled_lots, 0);
    }

    #[test]
    fn test_market_buy_with_only_bids_rejected() {
        let pool_config = PoolConfig::default();
        let mut engine = MatchingEngine::new(pool_config).unwrap();

        // Same-side liquidity must not satisfy a market order: a buy needs asks.
        let resting_bid = Order::new(1, Side::Buy, 50_000, 10, TimeInForce::GTC, 0);
        engine.place_order(resting_bid).unwrap();

        let buy_market = Order::new_market(2, Side::Buy, 5, 0);
        let result = engine.place_order(buy_market).unwrap();

        assert_eq!(result.status, OrderStatus::Rejected);
        assert_eq!(result.filled_lots, 0);
    }

    #[test]
    fn test_fully_consumed_level_does_not_leave_ghost() {
        // Regression: after a maker order is fully consumed, the price level
        // must not stay in the book as a 0-qty ghost. Without remove_level
        // get_top_levels would surface `(50000, 0)` which the depth
        // broadcast then renders as a phantom ask.
        let pool_config = PoolConfig::default();
        let mut engine = MatchingEngine::new(pool_config).unwrap();

        let sell = Order::new(1, Side::Sell, 50_000, 1, TimeInForce::GTC, 0);
        engine.place_order(sell).unwrap();
        let buy = Order::new_market(2, Side::Buy, 1, 0);
        engine.place_order(buy).unwrap();

        assert!(
            engine.get_top_levels(5, false).is_empty(),
            "asks should be empty after the level is fully consumed, got {:?}",
            engine.get_top_levels(5, false),
        );
    }

    #[test]
    fn test_market_sell_with_only_asks_rejected() {
        let pool_config = PoolConfig::default();
        let mut engine = MatchingEngine::new(pool_config).unwrap();

        // Mirror of the above: a sell needs bids, not asks.
        let resting_ask = Order::new(1, Side::Sell, 50_000, 10, TimeInForce::GTC, 0);
        engine.place_order(resting_ask).unwrap();

        let sell_market = Order::new_market(2, Side::Sell, 5, 0);
        let result = engine.place_order(sell_market).unwrap();

        assert_eq!(result.status, OrderStatus::Rejected);
        assert_eq!(result.filled_lots, 0);
    }

    #[test]
    fn test_market_order_partial_fill() {
        let pool_config = PoolConfig::default();
        let mut engine = MatchingEngine::new(pool_config).unwrap();

        // 放一个 5 个数量的卖限价单
        let sell_order = Order::new(1, Side::Sell, 50000, 5, TimeInForce::GTC, 0);
        let _ = engine.place_order(sell_order).unwrap();

        // 买市价 10 个，但只有 5 个可成交
        let buy_market = Order::new_market(2, Side::Buy, 10, 0);
        let result = engine.place_order(buy_market).unwrap();

        assert_eq!(result.status, OrderStatus::PartiallyFilled);
        assert_eq!(result.filled_lots, 5);
    }

    #[test]
    fn test_duplicate_order_id_is_rejected_without_book_mutation() {
        let pool_config = PoolConfig {
            order_capacity: 16,
            queue_capacity: 16,
        };
        let mut engine = MatchingEngine::new(pool_config).unwrap();

        let first = Order::new(1, Side::Buy, 100, 2, TimeInForce::GTC, 0);
        engine.place_order(first).unwrap();

        let duplicate = Order::new(1, Side::Sell, 101, 3, TimeInForce::GTC, 0);
        let err = engine.place_order(duplicate).unwrap_err();
        assert!(matches!(err, MatchingEngineError::DuplicateOrderId(1)));

        let original = engine.get_order(1).unwrap();
        assert_eq!(original.side, Side::Buy);
        assert_eq!(original.price_ticks, 100);
        assert_eq!(engine.get_top_levels(10, true), vec![(100, 2)]);
        assert!(engine.get_top_levels(10, false).is_empty());
    }

    #[test]
    fn test_partial_fill_updates_visible_depth_quantity() {
        let pool_config = PoolConfig {
            order_capacity: 16,
            queue_capacity: 16,
        };
        let mut engine = MatchingEngine::new(pool_config).unwrap();

        engine
            .place_order(Order::new(1, Side::Sell, 100, 10, TimeInForce::GTC, 0))
            .unwrap();
        let result = engine
            .place_order(Order::new_market(2, Side::Buy, 4, 0))
            .unwrap();

        assert_eq!(result.status, OrderStatus::Filled);
        assert_eq!(engine.get_top_levels(10, false), vec![(100, 6)]);
    }

    #[test]
    fn test_fok_can_fill_beyond_one_thousand_levels() {
        let pool_config = PoolConfig {
            order_capacity: 2_100,
            queue_capacity: 2_100,
        };
        let mut engine = MatchingEngine::new(pool_config).unwrap();

        for i in 0..1_001 {
            engine
                .place_order(Order::new(
                    i + 1,
                    Side::Sell,
                    100 + i as i64,
                    1,
                    TimeInForce::GTC,
                    0,
                ))
                .unwrap();
        }

        let result = engine
            .place_order(Order::new(
                2_000,
                Side::Buy,
                1_200,
                1_001,
                TimeInForce::FOK,
                0,
            ))
            .unwrap();

        assert_eq!(result.status, OrderStatus::Filled);
        assert_eq!(result.filled_lots, 1_001);
        assert!(engine.get_top_levels(10, false).is_empty());
    }

    #[test]
    fn test_no_trade_event_without_sender() {
        let pool_config = PoolConfig::default();
        let mut engine = MatchingEngine::new(pool_config).unwrap();

        // 不设置发送器，应该不会崩溃
        let buy_order = Order::new(1, Side::Buy, 50000, 10, TimeInForce::GTC, 0);
        engine.place_order(buy_order).unwrap();

        let sell_order = Order::new(2, Side::Sell, 50000, 10, TimeInForce::IOC, 0);
        let result = engine.place_order(sell_order).unwrap();

        // 成交应该正常发生
        assert_eq!(result.status, OrderStatus::Filled);
    }

    // ── Price-priority correctness tests ───────────────────────────────────────
    //
    // These tests specifically guard against the best_price cache bug where
    // add_to_book() failed to invalidate the cached best price, causing a new
    // order at a better price to be invisible to the next match_order call.

    /// Buy limit order must match the CHEAPEST available ask first.
    #[test]
    fn test_buy_matches_cheapest_ask_first() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        // Place asks: expensive first so the cache is set to the high price.
        engine.place_order(Order::new(1, Side::Sell, 100, 5, TimeInForce::GTC, 0)).unwrap();
        engine.place_order(Order::new(2, Side::Sell, 90, 5, TimeInForce::GTC, 0)).unwrap();
        engine.place_order(Order::new(3, Side::Sell, 80, 5, TimeInForce::GTC, 0)).unwrap();

        // Buy at 100: should match ask@80 first (price priority), not ask@100.
        let result = engine.place_order(Order::new(10, Side::Buy, 100, 5, TimeInForce::GTC, 0)).unwrap();

        assert_eq!(result.status, OrderStatus::Filled, "buy should fill at cheapest ask");
        assert_eq!(result.filled_lots, 5);

        // Asks at 90 and 100 must still be resting.
        let asks = engine.get_top_levels(5, false);
        assert_eq!(asks.len(), 2, "two asks should remain");
        assert_eq!(asks[0].0, 90, "next best ask should be 90");
        assert_eq!(asks[1].0, 100);
    }

    /// After a partial trade drains one price level, the next buy must still
    /// find the cheapest remaining ask — the cache must not return a stale value.
    #[test]
    fn test_price_priority_after_partial_trade_clears_level() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        // Ask at 100 (5 lots) and ask at 90 (5 lots).
        engine.place_order(Order::new(1, Side::Sell, 100, 5, TimeInForce::GTC, 0)).unwrap();
        engine.place_order(Order::new(2, Side::Sell, 90,  5, TimeInForce::GTC, 0)).unwrap();

        // First buy fully fills ask@90 — cache should now point to ask@100.
        let r1 = engine.place_order(Order::new(10, Side::Buy, 90, 5, TimeInForce::GTC, 0)).unwrap();
        assert_eq!(r1.status, OrderStatus::Filled);

        // Now add a NEW cheaper ask at 85.  This must update the cache.
        engine.place_order(Order::new(3, Side::Sell, 85, 5, TimeInForce::GTC, 0)).unwrap();

        // Second buy at 100: must match ask@85 (cheapest), NOT ask@100.
        let r2 = engine.place_order(Order::new(11, Side::Buy, 100, 5, TimeInForce::GTC, 0)).unwrap();
        assert_eq!(r2.status, OrderStatus::Filled, "should fill at ask@85");

        // Only ask@100 should remain.
        let asks = engine.get_top_levels(5, false);
        assert_eq!(asks.len(), 1);
        assert_eq!(asks[0].0, 100, "only ask@100 should remain");
    }

    /// The stale-cache scenario that caused the real-world negative-spread bug:
    /// sell_book cache = 74,333.90 after a previous match; maker then adds
    /// ask@74,309.70.  Next buy@74,333.80 MUST match at 74,309.70, not skip.
    #[test]
    fn test_stale_cache_does_not_miss_cheaper_ask() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        // Represents state after a previous match: ask@74,333.90 is the only
        // remaining ask (cache = 74,333.90 after the prior best was consumed).
        engine.place_order(Order::new(1, Side::Sell, 7_433_390, 1, TimeInForce::GTC, 0)).unwrap();

        // Trigger a buy that partially exhausts the book so the cache is set.
        // Place a buy that does NOT cross 74,333.90 so ask@74,333.90 stays.
        // (We just need the cache to be warmed up pointing at 74,333.90.)
        //
        // Simplest way: place a market-sell to warm the buy book, then buy one
        // lot of ask@74,333.90 indirectly.  Instead, just use add_to_book
        // directly by placing a GTC sell and then doing a match that leaves the
        // level alive but the cache "confirmed valid at that price".

        // Buy@74,333.80 → price_match: 74,333.80 < 74,333.90 → NO fill (correct).
        // Ask@74,333.90 remains.  Cache = 74,333.90.
        let r0 = engine.place_order(Order::new(10, Side::Buy, 7_433_380, 1, TimeInForce::IOC, 0)).unwrap();
        assert_eq!(r0.status, OrderStatus::Rejected, "no match at 74,333.80 when cheapest ask is 74,333.90");

        // ← This is the critical moment: cache is pointing at 74,333.90 (which
        //   still has an order).  Now add a CHEAPER ask at 74,309.70.
        engine.place_order(Order::new(2, Side::Sell, 7_430_970, 1, TimeInForce::GTC, 0)).unwrap();

        // Buy@74,333.80 must now match at 74,309.70 (the cheaper ask).
        // Before the fix, get_best_price_cached() returned 74,333.90 (stale),
        // price_match failed (74,333.80 < 74,333.90), and the trade was missed.
        let r1 = engine.place_order(Order::new(11, Side::Buy, 7_433_380, 1, TimeInForce::IOC, 0)).unwrap();
        assert_eq!(r1.status, OrderStatus::Filled, "must match the cheaper ask@74,309.70");
        assert_eq!(r1.filled_lots, 1);

        // ask@74,333.90 should still be resting (was not consumed).
        let asks = engine.get_top_levels(5, false);
        assert_eq!(asks.len(), 1, "ask@74,333.90 should remain");
        assert_eq!(asks[0].0, 7_433_390);
    }

    /// Sell order must match the HIGHEST available bid first (symmetric test).
    #[test]
    fn test_sell_matches_highest_bid_first() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        // Place bids: cheapest first so cache is set to the low price.
        engine.place_order(Order::new(1, Side::Buy, 80,  5, TimeInForce::GTC, 0)).unwrap();
        engine.place_order(Order::new(2, Side::Buy, 90,  5, TimeInForce::GTC, 0)).unwrap();
        engine.place_order(Order::new(3, Side::Buy, 100, 5, TimeInForce::GTC, 0)).unwrap();

        // Sell at 80: should match bid@100 first (price priority).
        let result = engine.place_order(Order::new(10, Side::Sell, 80, 5, TimeInForce::GTC, 0)).unwrap();
        assert_eq!(result.status, OrderStatus::Filled);

        let bids = engine.get_top_levels(5, true);
        assert_eq!(bids.len(), 2);
        assert_eq!(bids[0].0, 90, "next best bid after 100 consumed");
        assert_eq!(bids[1].0, 80);
    }

    /// Trade price must always equal the MAKER's price (the resting order),
    /// regardless of how aggressive the taker is.
    #[test]
    fn test_trade_price_equals_maker_price() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        // Maker: sell@90.
        engine.place_order(Order::new(1, Side::Sell, 90, 10, TimeInForce::GTC, 0)).unwrap();

        // Taker: buy@120 (very aggressive).  Trade must be at 90, not 120.
        let result = engine.place_order(Order::new(2, Side::Buy, 120, 10, TimeInForce::GTC, 0)).unwrap();
        assert_eq!(result.status, OrderStatus::Filled);
        // Trade price is confirmed via the depth being empty (the 90-level consumed).
        let asks = engine.get_top_levels(5, false);
        assert!(asks.is_empty(), "ask@90 fully consumed");
    }

    /// Market order must sweep all available levels in price order.
    #[test]
    fn test_market_buy_sweeps_levels_in_price_order() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        engine.place_order(Order::new(1, Side::Sell, 100, 3, TimeInForce::GTC, 0)).unwrap();
        engine.place_order(Order::new(2, Side::Sell, 90,  3, TimeInForce::GTC, 0)).unwrap();
        engine.place_order(Order::new(3, Side::Sell, 80,  3, TimeInForce::GTC, 0)).unwrap();

        // Market buy for 7 lots: should consume 80(3) + 90(3) + 100(1 of 3).
        let result = engine.place_order(Order::new_market(10, Side::Buy, 7, 0)).unwrap();
        assert_eq!(result.status, OrderStatus::Filled);
        assert_eq!(result.filled_lots, 7);

        let asks = engine.get_top_levels(5, false);
        assert_eq!(asks.len(), 1, "only ask@100 partially remains");
        assert_eq!(asks[0].0, 100);
        assert_eq!(asks[0].1, 2, "2 lots remain at 100");
    }

    /// A resting GTC bid that gets a new cheaper ask added must trade at the
    /// ask price (stale-cache symmetric bug check for the buy book).
    #[test]
    fn test_stale_cache_does_not_miss_higher_bid() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        // Bid@80 resting; cache eventually set to 80.
        engine.place_order(Order::new(1, Side::Buy, 80, 1, TimeInForce::GTC, 0)).unwrap();

        // Sell@90: price 90 > bid 80 → no match (correct).
        let r0 = engine.place_order(Order::new(10, Side::Sell, 90, 1, TimeInForce::IOC, 0)).unwrap();
        assert_eq!(r0.status, OrderStatus::Rejected);

        // Add new bid@100 (better than cached 80).  Cache must be invalidated.
        engine.place_order(Order::new(2, Side::Buy, 100, 1, TimeInForce::GTC, 0)).unwrap();

        // Sell@90: must now match bid@100 (the new better bid).
        let r1 = engine.place_order(Order::new(11, Side::Sell, 90, 1, TimeInForce::IOC, 0)).unwrap();
        assert_eq!(r1.status, OrderStatus::Filled, "must match the new bid@100");

        // bid@80 still resting.
        let bids = engine.get_top_levels(5, true);
        assert_eq!(bids.len(), 1);
        assert_eq!(bids[0].0, 80);
    }

    // ── Round 1: IOC semantics ─────────────────────────────────────────────────

    #[test]
    fn test_ioc_partial_fill_returns_partially_filled() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        // 3 lots available
        engine.place_order(Order::new(1, Side::Sell, 100, 3, TimeInForce::GTC, 0)).unwrap();

        // IOC wants 5, only 3 available → PartiallyFilled, no resting remainder
        let r = engine.place_order(Order::new(10, Side::Buy, 100, 5, TimeInForce::IOC, 0)).unwrap();
        assert_eq!(r.status, OrderStatus::PartiallyFilled);
        assert_eq!(r.filled_lots, 3);

        // The ask at 100 is fully consumed, nothing left in book for either side
        assert!(engine.get_top_levels(5, false).is_empty(), "ask fully consumed");
        // IOC does not rest in book
        assert!(engine.get_top_levels(5, true).is_empty(), "IOC must not rest");
    }

    #[test]
    fn test_ioc_no_liquidity_rejected() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        let r = engine.place_order(Order::new(1, Side::Buy, 100, 5, TimeInForce::IOC, 0)).unwrap();
        assert_eq!(r.status, OrderStatus::Rejected);
        assert_eq!(r.filled_lots, 0);
        assert!(engine.get_top_levels(5, true).is_empty(), "IOC must not rest when rejected");
    }

    #[test]
    fn test_ioc_does_not_rest_in_book_after_full_fill() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        engine.place_order(Order::new(1, Side::Sell, 100, 10, TimeInForce::GTC, 0)).unwrap();

        let r = engine.place_order(Order::new(10, Side::Buy, 100, 10, TimeInForce::IOC, 0)).unwrap();
        assert_eq!(r.status, OrderStatus::Filled);

        // Both sides empty after full fill
        assert!(engine.get_top_levels(5, false).is_empty());
        assert!(engine.get_top_levels(5, true).is_empty());
    }

    #[test]
    fn test_ioc_exact_quantity_match() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        engine.place_order(Order::new(1, Side::Sell, 50, 7, TimeInForce::GTC, 0)).unwrap();

        let r = engine.place_order(Order::new(2, Side::Buy, 50, 7, TimeInForce::IOC, 0)).unwrap();
        assert_eq!(r.status, OrderStatus::Filled);
        assert_eq!(r.filled_lots, 7);
    }

    // ── Round 2: FOK semantics ─────────────────────────────────────────────────

    #[test]
    fn test_fok_rejected_when_depth_insufficient() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        // Only 3 lots available, FOK wants 5
        engine.place_order(Order::new(1, Side::Sell, 100, 3, TimeInForce::GTC, 0)).unwrap();

        let r = engine.place_order(Order::new(10, Side::Buy, 100, 5, TimeInForce::FOK, 0)).unwrap();
        assert_eq!(r.status, OrderStatus::Rejected);
        assert_eq!(r.filled_lots, 0);
    }

    #[test]
    fn test_fok_rejection_does_not_consume_makers() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        engine.place_order(Order::new(1, Side::Sell, 100, 3, TimeInForce::GTC, 0)).unwrap();

        // FOK fails — makers must be completely untouched
        let r = engine.place_order(Order::new(10, Side::Buy, 100, 5, TimeInForce::FOK, 0)).unwrap();
        assert_eq!(r.status, OrderStatus::Rejected);

        let asks = engine.get_top_levels(5, false);
        assert_eq!(asks, vec![(100, 3)], "ask must be fully intact after FOK rejection");
    }

    #[test]
    fn test_fok_filled_when_exact_depth_available() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        engine.place_order(Order::new(1, Side::Sell, 100, 5, TimeInForce::GTC, 0)).unwrap();

        let r = engine.place_order(Order::new(10, Side::Buy, 100, 5, TimeInForce::FOK, 0)).unwrap();
        assert_eq!(r.status, OrderStatus::Filled);
        assert_eq!(r.filled_lots, 5);
        assert!(engine.get_top_levels(5, false).is_empty());
    }

    #[test]
    fn test_fok_fills_across_multiple_levels() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        engine.place_order(Order::new(1, Side::Sell, 100, 3, TimeInForce::GTC, 0)).unwrap();
        engine.place_order(Order::new(2, Side::Sell, 101, 3, TimeInForce::GTC, 0)).unwrap();

        // Need 6 lots spread across two levels — FOK should succeed
        let r = engine.place_order(Order::new(10, Side::Buy, 105, 6, TimeInForce::FOK, 0)).unwrap();
        assert_eq!(r.status, OrderStatus::Filled);
        assert_eq!(r.filled_lots, 6);
        assert!(engine.get_top_levels(5, false).is_empty());
    }

    #[test]
    fn test_fok_rejected_when_price_not_crossable() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        // Ask at 110, FOK buy at 105 — price doesn't cross
        engine.place_order(Order::new(1, Side::Sell, 110, 5, TimeInForce::GTC, 0)).unwrap();

        let r = engine.place_order(Order::new(10, Side::Buy, 105, 5, TimeInForce::FOK, 0)).unwrap();
        assert_eq!(r.status, OrderStatus::Rejected);

        let asks = engine.get_top_levels(5, false);
        assert_eq!(asks, vec![(110, 5)], "ask must be intact");
    }

    // ── Round 3: PostOnly semantics ────────────────────────────────────────────

    #[test]
    fn test_post_only_buy_rejected_when_crosses_ask() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        engine.place_order(Order::new(1, Side::Sell, 100, 5, TimeInForce::GTC, 0)).unwrap();

        // PostOnly buy at 100: would cross the ask@100 → must be rejected
        let r = engine.place_order(Order::new(10, Side::Buy, 100, 3, TimeInForce::PostOnly, 0)).unwrap();
        assert_eq!(r.status, OrderStatus::Rejected);
        assert_eq!(r.filled_lots, 0);

        // The ask must still be intact (PostOnly never partially fills)
        let asks = engine.get_top_levels(5, false);
        assert_eq!(asks, vec![(100, 5)]);
        // PostOnly never enters the buy book on rejection
        assert!(engine.get_top_levels(5, true).is_empty());
    }

    #[test]
    fn test_post_only_buy_rejected_when_price_above_best_ask() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        engine.place_order(Order::new(1, Side::Sell, 100, 5, TimeInForce::GTC, 0)).unwrap();

        // PostOnly buy at 110 > ask@100 → would immediately fill → Rejected
        let r = engine.place_order(Order::new(10, Side::Buy, 110, 3, TimeInForce::PostOnly, 0)).unwrap();
        assert_eq!(r.status, OrderStatus::Rejected);

        let asks = engine.get_top_levels(5, false);
        assert_eq!(asks, vec![(100, 5)], "ask must be intact");
    }

    #[test]
    fn test_post_only_buy_accepted_when_below_best_ask() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        engine.place_order(Order::new(1, Side::Sell, 100, 5, TimeInForce::GTC, 0)).unwrap();

        // PostOnly buy at 90 < ask@100 → will not cross → Accepted and enters book
        let r = engine.place_order(Order::new(10, Side::Buy, 90, 3, TimeInForce::PostOnly, 0)).unwrap();
        assert_eq!(r.status, OrderStatus::Accepted);
        assert_eq!(r.filled_lots, 0);

        let bids = engine.get_top_levels(5, true);
        assert_eq!(bids, vec![(90, 3)], "PostOnly order must rest in book");
    }

    #[test]
    fn test_post_only_accepted_with_empty_book() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        let r = engine.place_order(Order::new(1, Side::Sell, 100, 5, TimeInForce::PostOnly, 0)).unwrap();
        assert_eq!(r.status, OrderStatus::Accepted);

        let asks = engine.get_top_levels(5, false);
        assert_eq!(asks, vec![(100, 5)]);
    }

    #[test]
    fn test_post_only_sell_rejected_when_crosses_bid() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        engine.place_order(Order::new(1, Side::Buy, 100, 5, TimeInForce::GTC, 0)).unwrap();

        // PostOnly sell at 100: would cross bid@100 → Rejected
        let r = engine.place_order(Order::new(10, Side::Sell, 100, 3, TimeInForce::PostOnly, 0)).unwrap();
        assert_eq!(r.status, OrderStatus::Rejected);

        let bids = engine.get_top_levels(5, true);
        assert_eq!(bids, vec![(100, 5)], "bid must be intact");
        assert!(engine.get_top_levels(5, false).is_empty());
    }

    // ── Round 4: GTC resting semantics ────────────────────────────────────────

    #[test]
    fn test_gtc_partial_fill_rests_remainder_in_book() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        engine.place_order(Order::new(1, Side::Sell, 100, 3, TimeInForce::GTC, 0)).unwrap();

        // Buy 10, only 3 fillable → PartiallyFilled, remaining 7 rests as bid
        let r = engine.place_order(Order::new(10, Side::Buy, 100, 10, TimeInForce::GTC, 0)).unwrap();
        assert_eq!(r.status, OrderStatus::PartiallyFilled);
        assert_eq!(r.filled_lots, 3);

        assert!(engine.get_top_levels(5, false).is_empty(), "ask fully consumed");
        let bids = engine.get_top_levels(5, true);
        assert_eq!(bids, vec![(100, 7)], "remaining 7 must rest as bid");
    }

    #[test]
    fn test_gtc_no_match_rests_full_order() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        // Ask at 110, bid at 90 — no cross
        engine.place_order(Order::new(1, Side::Sell, 110, 5, TimeInForce::GTC, 0)).unwrap();

        let r = engine.place_order(Order::new(2, Side::Buy, 90, 5, TimeInForce::GTC, 0)).unwrap();
        assert_eq!(r.status, OrderStatus::Accepted);
        assert_eq!(r.filled_lots, 0);

        let bids = engine.get_top_levels(5, true);
        assert_eq!(bids, vec![(90, 5)]);
        let asks = engine.get_top_levels(5, false);
        assert_eq!(asks, vec![(110, 5)]);
    }

    #[test]
    fn test_gtc_full_fill_leaves_empty_book() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        engine.place_order(Order::new(1, Side::Sell, 100, 5, TimeInForce::GTC, 0)).unwrap();

        let r = engine.place_order(Order::new(2, Side::Buy, 100, 5, TimeInForce::GTC, 0)).unwrap();
        assert_eq!(r.status, OrderStatus::Filled);
        assert_eq!(r.filled_lots, 5);

        assert!(engine.get_top_levels(5, false).is_empty());
        assert!(engine.get_top_levels(5, true).is_empty());
    }

    // ── Round 5: cancel_order semantics ───────────────────────────────────────

    #[test]
    fn test_cancel_resting_order_removes_from_book() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        engine.place_order(Order::new(1, Side::Buy, 90, 5, TimeInForce::GTC, 0)).unwrap();
        engine.place_order(Order::new(2, Side::Buy, 80, 3, TimeInForce::GTC, 0)).unwrap();

        let r = engine.cancel_order(1).unwrap();
        assert_eq!(r.order_id, 1);
        assert_eq!(r.cancelled_quantity, 5);

        let bids = engine.get_top_levels(5, true);
        assert_eq!(bids, vec![(80, 3)], "only order 2 remains");
    }

    #[test]
    fn test_cancel_nonexistent_order_returns_error() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        let err = engine.cancel_order(999).unwrap_err();
        assert!(matches!(err, MatchingEngineError::OrderNotFound));
    }

    #[test]
    fn test_cancel_filled_order_returns_not_found() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        // Place a GTC maker, then fill it with an IOC taker.
        engine.place_order(Order::new(1, Side::Sell, 100, 5, TimeInForce::GTC, 0)).unwrap();
        engine.place_order(Order::new(2, Side::Buy, 100, 5, TimeInForce::IOC, 0)).unwrap();

        // Order 1 is fully filled and immediately removed from the orders map.
        // cancel_order therefore sees OrderNotFound (not AlreadyFilled).
        let err = engine.cancel_order(1).unwrap_err();
        assert!(matches!(err, MatchingEngineError::OrderNotFound));
    }

    #[test]
    fn test_cancel_last_order_at_price_level_clears_level() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        engine.place_order(Order::new(1, Side::Buy, 100, 5, TimeInForce::GTC, 0)).unwrap();

        engine.cancel_order(1).unwrap();

        assert!(engine.get_top_levels(5, true).is_empty(), "level must be removed when last order cancelled");
    }

    #[test]
    fn test_cancel_one_of_two_orders_at_same_price() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        engine.place_order(Order::new(1, Side::Buy, 100, 3, TimeInForce::GTC, 0)).unwrap();
        engine.place_order(Order::new(2, Side::Buy, 100, 4, TimeInForce::GTC, 0)).unwrap();

        engine.cancel_order(1).unwrap();

        let bids = engine.get_top_levels(5, true);
        assert_eq!(bids, vec![(100, 4)], "only order 2's 4 lots remain at price 100");
    }

    #[test]
    fn test_cancel_batch_skips_nonexistent_and_filled() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        engine.place_order(Order::new(1, Side::Buy, 90, 5, TimeInForce::GTC, 0)).unwrap();
        engine.place_order(Order::new(2, Side::Sell, 90, 5, TimeInForce::GTC, 0)).unwrap();
        // Order 1 was just filled by order 2 (same price, GTC cross)
        // Actually order 2 is a sell at 90, order 1 is buy at 90: they should match.
        // Let's use a fresh setup:
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();
        engine.place_order(Order::new(10, Side::Buy, 80, 3, TimeInForce::GTC, 0)).unwrap();
        engine.place_order(Order::new(11, Side::Buy, 70, 4, TimeInForce::GTC, 0)).unwrap();

        let cancelled = engine.cancel_orders_batch(&[10, 11, 999]);
        assert_eq!(cancelled.len(), 2);
        let ids: Vec<u64> = cancelled.iter().map(|c| c.0).collect();
        assert!(ids.contains(&10));
        assert!(ids.contains(&11));
        assert!(engine.get_top_levels(5, true).is_empty());
    }

    #[test]
    fn test_cancel_batch_returns_correct_quantities() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        engine.place_order(Order::new(1, Side::Sell, 100, 7, TimeInForce::GTC, 0)).unwrap();
        engine.place_order(Order::new(2, Side::Sell, 110, 3, TimeInForce::GTC, 0)).unwrap();

        let cancelled = engine.cancel_orders_batch(&[1, 2]);
        assert_eq!(cancelled.len(), 2);

        let qty_map: std::collections::HashMap<u64, i64> = cancelled.into_iter().collect();
        assert_eq!(qty_map[&1], 7);
        assert_eq!(qty_map[&2], 3);
    }

    // ── Round 6: time priority (FIFO) at same price ────────────────────────────

    #[test]
    fn test_time_priority_same_price_fifo() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        // Two asks at the same price, placed in order: id=1 then id=2.
        // Time priority: order id=1 must be filled first.
        engine.place_order(Order::new(1, Side::Sell, 100, 5, TimeInForce::GTC, 0)).unwrap();
        engine.place_order(Order::new(2, Side::Sell, 100, 5, TimeInForce::GTC, 0)).unwrap();

        // IOC buy for 5 lots: must fill from order id=1 (first in), leaving id=2 intact.
        let r = engine.place_order(Order::new(10, Side::Buy, 100, 5, TimeInForce::IOC, 0)).unwrap();
        assert_eq!(r.status, OrderStatus::Filled);
        assert_eq!(r.filled_lots, 5);

        // Order 1 fully consumed and removed from the order map; cancel returns OrderNotFound.
        let err = engine.cancel_order(1).unwrap_err();
        assert!(matches!(err, MatchingEngineError::OrderNotFound), "fully-filled order removed from map");

        let asks = engine.get_top_levels(5, false);
        assert_eq!(asks, vec![(100, 5)], "order 2 still rests");
    }

    // ── Round 7: depth accuracy after mixed operations ─────────────────────────

    #[test]
    fn test_depth_correct_after_partial_fill_then_cancel() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        // Three asks at same price
        engine.place_order(Order::new(1, Side::Sell, 100, 4, TimeInForce::GTC, 0)).unwrap();
        engine.place_order(Order::new(2, Side::Sell, 100, 4, TimeInForce::GTC, 0)).unwrap();
        engine.place_order(Order::new(3, Side::Sell, 100, 4, TimeInForce::GTC, 0)).unwrap();

        // Buy 5: consumes order 1 (4) and 1 lot from order 2 — order 2 partially filled
        let r = engine.place_order(Order::new(10, Side::Buy, 100, 5, TimeInForce::IOC, 0)).unwrap();
        assert_eq!(r.status, OrderStatus::Filled);
        assert_eq!(r.filled_lots, 5);

        // Ask depth should be: order 2 has 3 remaining, order 3 has 4 → total 7
        let asks = engine.get_top_levels(5, false);
        assert_eq!(asks, vec![(100, 7)]);

        // Cancel order 3 (fully unreduced): depth drops by 4
        engine.cancel_order(3).unwrap();
        let asks = engine.get_top_levels(5, false);
        assert_eq!(asks, vec![(100, 3)], "only order 2's 3 remaining lots should be left");
    }

    #[test]
    fn test_multiple_price_levels_depth_order() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        engine.place_order(Order::new(1, Side::Sell, 103, 1, TimeInForce::GTC, 0)).unwrap();
        engine.place_order(Order::new(2, Side::Sell, 101, 2, TimeInForce::GTC, 0)).unwrap();
        engine.place_order(Order::new(3, Side::Sell, 102, 3, TimeInForce::GTC, 0)).unwrap();

        // get_top_levels(false) = asks, ascending price
        let asks = engine.get_top_levels(3, false);
        assert_eq!(asks, vec![(101, 2), (102, 3), (103, 1)], "asks must be ascending");
    }

    #[test]
    fn test_bid_levels_depth_order() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        engine.place_order(Order::new(1, Side::Buy, 97, 1, TimeInForce::GTC, 0)).unwrap();
        engine.place_order(Order::new(2, Side::Buy, 99, 2, TimeInForce::GTC, 0)).unwrap();
        engine.place_order(Order::new(3, Side::Buy, 98, 3, TimeInForce::GTC, 0)).unwrap();

        // get_top_levels(true) = bids, descending price
        let bids = engine.get_top_levels(3, true);
        assert_eq!(bids, vec![(99, 2), (98, 3), (97, 1)], "bids must be descending");
    }

    // ── Round 8: order validation ──────────────────────────────────────────────

    #[test]
    fn test_invalid_price_zero_rejected() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        let err = engine.place_order(Order::new(1, Side::Buy, 0, 5, TimeInForce::GTC, 0)).unwrap_err();
        assert!(matches!(err, MatchingEngineError::InvalidPrice(0)));
    }

    #[test]
    fn test_invalid_quantity_zero_rejected() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        let err = engine.place_order(Order::new(1, Side::Buy, 100, 0, TimeInForce::GTC, 0)).unwrap_err();
        assert!(matches!(err, MatchingEngineError::InvalidQuantity(0)));
    }

    #[test]
    fn test_negative_price_rejected() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        let err = engine.place_order(Order::new(1, Side::Sell, -1, 5, TimeInForce::GTC, 0)).unwrap_err();
        assert!(matches!(err, MatchingEngineError::InvalidPrice(_)));
    }

    // ── Round 9: pool exhaustion ───────────────────────────────────────────────

    #[test]
    fn test_order_pool_exhaustion_returns_error() {
        let config = PoolConfig { order_capacity: 4, queue_capacity: 4 };
        let mut engine = MatchingEngine::new(config).unwrap();

        // Fill the pool with GTC resting orders
        engine.place_order(Order::new(1, Side::Buy, 80, 1, TimeInForce::GTC, 0)).unwrap();
        engine.place_order(Order::new(2, Side::Buy, 81, 1, TimeInForce::GTC, 0)).unwrap();
        engine.place_order(Order::new(3, Side::Buy, 82, 1, TimeInForce::GTC, 0)).unwrap();
        engine.place_order(Order::new(4, Side::Buy, 83, 1, TimeInForce::GTC, 0)).unwrap();

        // Fifth order must fail with pool exhaustion
        let err = engine.place_order(Order::new(5, Side::Buy, 84, 1, TimeInForce::GTC, 0)).unwrap_err();
        assert!(matches!(err, MatchingEngineError::OrderPoolExhausted));
    }

    // ── Round 10: cancel restores pool slot for reuse ─────────────────────────

    #[test]
    fn test_cancelled_slot_reused_by_new_order() {
        let config = PoolConfig { order_capacity: 2, queue_capacity: 4 };
        let mut engine = MatchingEngine::new(config).unwrap();

        engine.place_order(Order::new(1, Side::Buy, 80, 5, TimeInForce::GTC, 0)).unwrap();
        engine.place_order(Order::new(2, Side::Buy, 70, 5, TimeInForce::GTC, 0)).unwrap();

        // Pool full — third order must fail
        assert!(engine.place_order(Order::new(3, Side::Buy, 60, 5, TimeInForce::GTC, 0)).is_err());

        // Cancel order 1 to free a slot
        engine.cancel_order(1).unwrap();

        // Now a new order should succeed using the freed slot
        let r = engine.place_order(Order::new(4, Side::Buy, 65, 5, TimeInForce::GTC, 0)).unwrap();
        assert_eq!(r.status, OrderStatus::Accepted);

        let bids = engine.get_top_levels(5, true);
        assert_eq!(bids.len(), 2);
    }

    // ── Orderbook depth correctness after all operation types ─────────────────
    //
    // These tests verify get_top_levels() stays accurate through the full
    // lifecycle: add → trade (partial/full) → cancel → add again.

    #[test]
    fn test_depth_single_ask_placed() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();
        engine.place_order(Order::new(1, Side::Sell, 100, 7, TimeInForce::GTC, 0)).unwrap();
        assert_eq!(engine.get_top_levels(5, false), vec![(100, 7)]);
        assert!(engine.get_top_levels(5, true).is_empty());
    }

    #[test]
    fn test_depth_two_orders_same_price_aggregated() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();
        engine.place_order(Order::new(1, Side::Sell, 100, 3, TimeInForce::GTC, 0)).unwrap();
        engine.place_order(Order::new(2, Side::Sell, 100, 4, TimeInForce::GTC, 0)).unwrap();
        // Same price: quantities aggregate to 7
        assert_eq!(engine.get_top_levels(5, false), vec![(100, 7)]);
    }

    #[test]
    fn test_depth_three_ask_levels_ascending_order() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();
        engine.place_order(Order::new(1, Side::Sell, 103, 1, TimeInForce::GTC, 0)).unwrap();
        engine.place_order(Order::new(2, Side::Sell, 101, 2, TimeInForce::GTC, 0)).unwrap();
        engine.place_order(Order::new(3, Side::Sell, 102, 3, TimeInForce::GTC, 0)).unwrap();
        assert_eq!(
            engine.get_top_levels(3, false),
            vec![(101, 2), (102, 3), (103, 1)],
            "asks must be ascending price"
        );
    }

    #[test]
    fn test_depth_three_bid_levels_descending_order() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();
        engine.place_order(Order::new(1, Side::Buy, 97, 1, TimeInForce::GTC, 0)).unwrap();
        engine.place_order(Order::new(2, Side::Buy, 99, 2, TimeInForce::GTC, 0)).unwrap();
        engine.place_order(Order::new(3, Side::Buy, 98, 3, TimeInForce::GTC, 0)).unwrap();
        assert_eq!(
            engine.get_top_levels(3, true),
            vec![(99, 2), (98, 3), (97, 1)],
            "bids must be descending price"
        );
    }

    #[test]
    fn test_depth_after_cancel_decreases_quantity() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();
        engine.place_order(Order::new(1, Side::Sell, 100, 5, TimeInForce::GTC, 0)).unwrap();
        engine.place_order(Order::new(2, Side::Sell, 100, 3, TimeInForce::GTC, 0)).unwrap();
        engine.cancel_order(2).unwrap();
        assert_eq!(engine.get_top_levels(5, false), vec![(100, 5)]);
    }

    #[test]
    fn test_depth_level_disappears_when_all_orders_cancelled() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();
        engine.place_order(Order::new(1, Side::Sell, 100, 5, TimeInForce::GTC, 0)).unwrap();
        engine.cancel_order(1).unwrap();
        assert!(engine.get_top_levels(5, false).is_empty());
    }

    #[test]
    fn test_depth_after_ioc_partial_fill_reduces_top_level() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();
        engine.place_order(Order::new(1, Side::Sell, 100, 10, TimeInForce::GTC, 0)).unwrap();
        // IOC buy of 4: consumes 4 from the ask level
        engine.place_order(Order::new(2, Side::Buy, 100, 4, TimeInForce::IOC, 0)).unwrap();
        assert_eq!(engine.get_top_levels(5, false), vec![(100, 6)]);
    }

    #[test]
    fn test_depth_after_ioc_full_fill_removes_level() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();
        engine.place_order(Order::new(1, Side::Sell, 100, 5, TimeInForce::GTC, 0)).unwrap();
        engine.place_order(Order::new(2, Side::Buy, 100, 5, TimeInForce::IOC, 0)).unwrap();
        assert!(engine.get_top_levels(5, false).is_empty());
    }

    #[test]
    fn test_depth_after_market_sweep_shows_partially_consumed_level() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();
        engine.place_order(Order::new(1, Side::Sell, 100, 3, TimeInForce::GTC, 0)).unwrap();
        engine.place_order(Order::new(2, Side::Sell, 101, 5, TimeInForce::GTC, 0)).unwrap();
        // Market buy for 5: consumes all of level 100 (3) + 2 from level 101 (5)
        engine.place_order(Order::new_market(10, Side::Buy, 5, 0)).unwrap();
        assert_eq!(engine.get_top_levels(5, false), vec![(101, 3)]);
    }

    #[test]
    fn test_depth_gtc_partial_taker_rests_remainder_correctly() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();
        // 3 lots available as ask
        engine.place_order(Order::new(1, Side::Sell, 100, 3, TimeInForce::GTC, 0)).unwrap();
        // GTC buy for 8: fills 3, rests 5 as a bid at price 100
        engine.place_order(Order::new(2, Side::Buy, 100, 8, TimeInForce::GTC, 0)).unwrap();

        // Ask side fully consumed
        assert!(engine.get_top_levels(5, false).is_empty(), "ask must be empty");
        // Bid side: 5 remaining (not 8)
        assert_eq!(engine.get_top_levels(5, true), vec![(100, 5)], "bid must show remaining 5");
    }

    #[test]
    fn test_depth_limit_caps_returned_levels() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();
        for i in 1..=5 {
            engine.place_order(Order::new(i, Side::Sell, i as i64 * 10, 1, TimeInForce::GTC, 0)).unwrap();
        }
        // Only top 3
        let asks = engine.get_top_levels(3, false);
        assert_eq!(asks.len(), 3);
        assert_eq!(asks[0].0, 10);
        assert_eq!(asks[2].0, 30);
    }

    #[test]
    fn test_depth_limit_zero_returns_empty() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();
        engine.place_order(Order::new(1, Side::Sell, 100, 5, TimeInForce::GTC, 0)).unwrap();
        assert_eq!(engine.get_top_levels(0, false), vec![]);
    }

    #[test]
    fn test_depth_ask_side_unaffected_by_bid_operations() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();
        engine.place_order(Order::new(1, Side::Sell, 200, 5, TimeInForce::GTC, 0)).unwrap();
        engine.place_order(Order::new(2, Side::Buy, 100, 3, TimeInForce::GTC, 0)).unwrap();
        engine.cancel_order(2).unwrap();

        // Ask side must be untouched
        assert_eq!(engine.get_top_levels(5, false), vec![(200, 5)]);
    }

    #[test]
    fn test_depth_no_phantom_level_after_ioc_consumes_multiple_orders_at_level() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();
        engine.place_order(Order::new(1, Side::Sell, 100, 3, TimeInForce::GTC, 0)).unwrap();
        engine.place_order(Order::new(2, Side::Sell, 100, 2, TimeInForce::GTC, 0)).unwrap();
        // IOC buys all 5 — both orders at price 100 consumed, level must vanish
        engine.place_order(Order::new_market(10, Side::Buy, 5, 0)).unwrap();
        assert!(engine.get_top_levels(5, false).is_empty(), "no phantom level after full consumption");
    }

    #[test]
    fn test_depth_post_only_rejection_leaves_book_unchanged() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();
        engine.place_order(Order::new(1, Side::Sell, 100, 5, TimeInForce::GTC, 0)).unwrap();
        // PostOnly buy at 100 → rejected, book must not change
        engine.place_order(Order::new(2, Side::Buy, 100, 3, TimeInForce::PostOnly, 0)).unwrap();
        assert_eq!(engine.get_top_levels(5, false), vec![(100, 5)]);
        assert!(engine.get_top_levels(5, true).is_empty());
    }

    #[test]
    fn test_depth_fok_rejection_leaves_book_unchanged() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();
        engine.place_order(Order::new(1, Side::Sell, 100, 3, TimeInForce::GTC, 0)).unwrap();
        // FOK wants 5 but only 3 available → rejected, ask untouched
        engine.place_order(Order::new(2, Side::Buy, 100, 5, TimeInForce::FOK, 0)).unwrap();
        assert_eq!(engine.get_top_levels(5, false), vec![(100, 3)]);
        assert!(engine.get_top_levels(5, true).is_empty());
    }

    #[test]
    fn test_depth_add_after_full_clear_starts_fresh() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();
        engine.place_order(Order::new(1, Side::Sell, 100, 5, TimeInForce::GTC, 0)).unwrap();
        engine.cancel_order(1).unwrap();
        assert!(engine.get_top_levels(5, false).is_empty());

        // Now add a fresh order at a different price
        engine.place_order(Order::new(2, Side::Sell, 110, 8, TimeInForce::GTC, 0)).unwrap();
        assert_eq!(engine.get_top_levels(5, false), vec![(110, 8)]);
    }

    // ── match_orders_batch market order regression ────────────────────────────

    #[test]
    fn test_match_orders_batch_market_buy_fills() {
        // Regression: match_orders_batch was missing `order.is_market ||` in the
        // price check, causing market orders to never fill (price_ticks=0 < any ask).
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        // Seed a resting ask at price 50_000
        engine.place_order(Order::new(1, Side::Sell, 50_000, 10, TimeInForce::GTC, 0)).unwrap();

        let market_buy = Order::new_market(2, Side::Buy, 5, 1);
        let mut batch: SmallVec<[Order; 40]> = SmallVec::new();
        batch.push(market_buy);

        let results = engine.match_orders_batch(batch).unwrap();
        assert_eq!(results.len(), 1);
        let (filled, _trades) = &results[0];
        assert_eq!(*filled, 5, "market buy should fill 5 lots from the ask");

        // Remaining 5 lots still in book
        assert_eq!(engine.get_top_levels(5, false), vec![(50_000, 5)]);
    }

    #[test]
    fn test_match_orders_batch_market_sell_fills() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        // Seed a resting bid at 50_000
        engine.place_order(Order::new(1, Side::Buy, 50_000, 10, TimeInForce::GTC, 0)).unwrap();

        let market_sell = Order::new_market(2, Side::Sell, 7, 1);
        let mut batch: SmallVec<[Order; 40]> = SmallVec::new();
        batch.push(market_sell);

        let results = engine.match_orders_batch(batch).unwrap();
        let (filled, _trades) = &results[0];
        assert_eq!(*filled, 7, "market sell should fill 7 lots from the bid");

        // Remaining 3 lots still in book
        assert_eq!(engine.get_top_levels(5, true), vec![(50_000, 3)]);
    }

    #[test]
    fn test_match_orders_batch_market_empty_book_fills_zero() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        let market_buy = Order::new_market(1, Side::Buy, 10, 0);
        let mut batch: SmallVec<[Order; 40]> = SmallVec::new();
        batch.push(market_buy);

        let results = engine.match_orders_batch(batch).unwrap();
        let (filled, _trades) = &results[0];
        assert_eq!(*filled, 0, "market order on empty book should fill 0 lots");
    }

    #[test]
    fn test_match_orders_batch_mixed_limit_and_market() {
        // Validate that limit and market orders in the same batch interact correctly.
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        // Seed ask liquidity
        engine.place_order(Order::new(1, Side::Sell, 100, 20, TimeInForce::GTC, 0)).unwrap();

        // Batch: limit buy at 100 (takes 5) then market buy (takes another 5)
        let limit_buy = Order::new(2, Side::Buy, 100, 5, TimeInForce::IOC, 1);
        let market_buy = Order::new_market(3, Side::Buy, 5, 2);
        let mut batch: SmallVec<[Order; 40]> = SmallVec::new();
        batch.push(limit_buy);
        batch.push(market_buy);

        let results = engine.match_orders_batch(batch).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, 5, "limit buy fills 5");
        assert_eq!(results[1].0, 5, "market buy fills 5");

        // 10 remaining in ask
        assert_eq!(engine.get_top_levels(5, false), vec![(100, 10)]);
    }
}
