use crate::error::{MatchingEngineError, OrderResult};
use crate::event::MatchingEvent;
use crate::float_ext::FloatExt;
use crate::market_data::{
    Depth50SnapshotEvent, DepthSnapshotEvent, Level2SnapshotEvent, MarketDataConfig, TradeEvent,
};
use crate::order::{Order, Side, TimeInForce};
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
    fills_buf: SmallVec<[(u64, f64, f64); 16]>,
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
    pub fn get_top_levels(&self, limit: usize, is_buy: bool) -> Vec<(f64, f64)> {
        if is_buy {
            self.buy_book.get_top_levels(limit)
        } else {
            self.sell_book.get_top_levels(limit)
        }
    }

    #[doc(hidden)]
    pub fn fill_top_levels(&self, is_buy: bool, out: &mut [(f64, f64)]) -> usize {
        if is_buy {
            self.buy_book.fill_top_levels(out)
        } else {
            self.sell_book.fill_top_levels(out)
        }
    }

    /// 验证订单有效性
    #[inline(always)]
    fn validate_order(&self, order: &Order) -> OrderResult<()> {
        // 市价单不检查 price，限价单检查
        if !order.is_market && (!order.price.positive() || order.price.is_nan()) {
            return Err(MatchingEngineError::InvalidPrice(order.price));
        }

        if !order.quantity.positive() || order.quantity.is_nan() {
            return Err(MatchingEngineError::InvalidQuantity(order.quantity));
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
    pub fn get_order_fill_status(&self, order_id: u64) -> Option<(f64, f64)> {
        self.orders
            .get(&order_id)
            .and_then(|&pool_idx| self.pools.orders.get(pool_idx))
            .map(|order| (order.filled, order.remaining()))
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
                let mut bids = [(0.0, 0.0); 20];
                let mut asks = [(0.0, 0.0); 20];
                let num_bids = self.buy_book.fill_top_levels(&mut bids);
                let num_asks = self.sell_book.fill_top_levels(&mut asks);
                for (i, (p, q)) in bids.iter().take(num_bids).enumerate() {
                    event.bids[i] = (*p, *q);
                    event.num_bids = (i + 1) as u8;
                }
                for (i, (p, q)) in asks.iter().take(num_asks).enumerate() {
                    event.asks[i] = (*p, *q);
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

                let mut bids = [(0.0, 0.0); 20];
                let mut asks = [(0.0, 0.0); 20];
                let num_bids = self.buy_book.fill_top_levels(&mut bids);
                let num_asks = self.sell_book.fill_top_levels(&mut asks);

                for (i, (p, q)) in bids.iter().take(num_bids).enumerate() {
                    event.bids[i] = (*p, *q);
                    event.num_bids = (i + 1) as u8;
                }

                for (i, (p, q)) in asks.iter().take(num_asks).enumerate() {
                    event.asks[i] = (*p, *q);
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

        let mut bids = [(0.0, 0.0); 50];
        let mut asks = [(0.0, 0.0); 50];
        let num_bids = self.buy_book.fill_top_levels(&mut bids);
        let num_asks = self.sell_book.fill_top_levels(&mut asks);

        for (i, (p, q)) in bids.iter().take(num_bids).enumerate() {
            event.bids[i] = (*p, *q);
            event.num_bids = (i + 1) as u8;
        }

        for (i, (p, q)) in asks.iter().take(num_asks).enumerate() {
            event.asks[i] = (*p, *q);
            event.num_asks = (i + 1) as u8;
        }

        event
    }

    /// 构建Level2-400快照事件
    #[inline]
    fn build_level2_event(&self, now_ns: u64) -> Level2SnapshotEvent {
        let mut event = Level2SnapshotEvent::new(now_ns, self.level2_seq);

        let mut bids = [(0.0, 0.0); 400];
        let mut asks = [(0.0, 0.0); 400];
        let num_bids = self.buy_book.fill_top_levels(&mut bids);
        let num_asks = self.sell_book.fill_top_levels(&mut asks);

        for (i, (p, q)) in bids.iter().take(num_bids).enumerate() {
            event.bids[i] = (*p, *q);
            event.num_bids = (i + 1) as u16;
        }

        for (i, (p, q)) in asks.iter().take(num_asks).enumerate() {
            event.asks[i] = (*p, *q);
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

    /// 批量下单（支持最多20笔订单，避免堆分配）
    #[inline]
    pub fn place_orders(
        &mut self,
        orders: SmallVec<[Order; 20]>,
    ) -> OrderResult<SmallVec<[PlaceOrderResult; 20]>> {
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
        let filled_qty = self.match_order(order)?;

        let status = if filled_qty.ge_eps(order.quantity) {
            OrderStatus::Filled
        } else if filled_qty.positive() {
            OrderStatus::PartiallyFilled
        } else {
            OrderStatus::Rejected
        };

        Ok(PlaceOrderResult {
            order_id: order.id,
            filled: filled_qty,
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
                Side::Buy => order.price >= best.price,
                Side::Sell => order.price <= best.price,
            };

            if would_match {
                return Ok(PlaceOrderResult {
                    order_id: order.id,
                    filled: 0.0,
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
            price: order.price,
            quantity: order.quantity,
            timestamp: order.timestamp,
        });

        Ok(PlaceOrderResult {
            order_id: order.id,
            filled: 0.0,
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
            order.quantity,
            order.price,
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
                filled: 0.0,
                status: OrderStatus::Rejected,
                fills: SmallVec::new(),
            });
        }

        // Now we know it's fully fillable — commit the fills.
        let filled_qty = self.match_order(order)?;
        Ok(PlaceOrderResult {
            order_id: order.id,
            filled: filled_qty,
            status: OrderStatus::Filled,
            fills: self.fills_buf.clone(),
        })
    }

    /// 处理IOC订单
    #[inline]
    fn handle_ioc(&mut self, order: Order) -> OrderResult<PlaceOrderResult> {
        let filled_qty = self.match_order(order)?;

        let status = if filled_qty.ge_eps(order.quantity) {
            OrderStatus::Filled
        } else if filled_qty.positive() {
            OrderStatus::PartiallyFilled
        } else {
            OrderStatus::Rejected
        };

        Ok(PlaceOrderResult {
            order_id: order.id,
            filled: filled_qty,
            status,
            fills: self.fills_buf.clone(),
        })
    }

    /// 处理GTC订单
    #[inline]
    fn handle_gtc(&mut self, order: Order) -> OrderResult<PlaceOrderResult> {
        let filled_qty = self.match_order(order)?;

        // 如果有剩余，加入订单簿
        if filled_qty.lt_eps(order.quantity) {
            let mut remaining_order = order;
            remaining_order.filled = filled_qty;
            self.add_to_book(remaining_order)?;
        }

        let status = if filled_qty.ge_eps(order.quantity) {
            OrderStatus::Filled
        } else if filled_qty.positive() {
            OrderStatus::PartiallyFilled
        } else {
            OrderStatus::Accepted
        };

        Ok(PlaceOrderResult {
            order_id: order.id,
            filled: filled_qty,
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
        let _ = book.insert_level(order.price);

        // 添加订单到价格档位队列
        if book
            .add_order_at_level(order.price, order.id, order.quantity)
            .is_err()
        {
            self.pools.orders.release(pool_idx);
            let level_empty = book
                .get_node_at_price(order.price)
                .map(|n| n.orders.is_empty())
                .unwrap_or(false);
            if level_empty {
                let _ = book.remove_level(order.price);
            }
            return Err(MatchingEngineError::NodePoolExhausted);
        }

        // 储存池索引
        self.orders.insert(order.id, pool_idx);

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
    fn match_order(&mut self, order: Order) -> OrderResult<f64> {
        let mut filled = 0.0;
        self.affected_makers_buf.clear();
        self.fills_buf.clear();

        // 使用本地变量而非大结构体字段(缓存友好)
        let mut trade_indices: SmallVec<[usize; 64]> = SmallVec::new();
        let mut trade_events: SmallVec<[TradeEvent; 128]> = SmallVec::new();

        // 外层循环：获取最优对手价
        loop {
            if filled.ge_eps(order.quantity) {
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
                                    Side::Buy => order.price.ge_eps(price),
                                    Side::Sell => order.price.le_eps(price),
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
                if filled.ge_eps(order.quantity) {
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
                let order_remaining = order.quantity - filled;
                let counter_remaining = counter_order.remaining();

                // Safety guard: float accumulation can leave counter_remaining as noise
                // (e.g. 1e-17) even though is_filled() was false. Force-remove the ghost.
                if !counter_remaining.positive() {
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

                let trade_qty = order_remaining.min(counter_remaining);

                // 收集被修改的 maker ID 及成交明细
                self.affected_makers_buf.push(counter_order_id);
                self.fills_buf
                    .push((counter_order_id, best_price, trade_qty));

                // 更新订单状态
                {
                    if let Some(&pool_idx) = self.orders.get(&order.id) {
                        if let Some(o) = self.pools.orders.get_mut(pool_idx) {
                            o.filled += trade_qty;
                        }
                    }
                    if let Some(&counter_pool_idx) = self.orders.get(&counter_order_id) {
                        if let Some(o) = self.pools.orders.get_mut(counter_pool_idx) {
                            o.filled += trade_qty;
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
                        trade_qty,
                    );
                }

                filled += trade_qty;

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
                        trade.price = best_price;
                        trade.quantity = trade_qty;
                    }
                }

                // 存储Trade索引
                trade_indices.push(trade_idx);

                self.publish_event(&MatchingEvent::Trade {
                    taker_order_id: order.id,
                    maker_order_id: counter_order_id,
                    price: best_price,
                    quantity: trade_qty,
                    timestamp: order.timestamp,
                });

                // 收集交易事件到batch（后续一次性发送到ring buffer）
                let trade_event = TradeEvent::new(
                    self.trade_sequence,
                    order.id,
                    counter_order_id,
                    order.timestamp,
                    best_price,
                    trade_qty,
                    order.side,
                    order.id,
                    counter_order_id,
                );
                self.trade_sequence += 1;
                trade_events.push(trade_event);

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
                            // Use orders.is_empty() instead of total_quantity <= 0
                            // to handle float drift where total_quantity ≈ 0 but > 0.
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

    /// 批量撮合多个订单（最多20个），共享TradeEvent批处理以提升性能
    /// 所有订单的TradeEvent被收集到一个batch中，最后一次性发送
    #[inline]
    pub fn match_orders_batch(
        &mut self,
        orders: SmallVec<[Order; 20]>,
    ) -> OrderResult<SmallVec<[(f64, Vec<Trade>); 20]>> {
        let mut results: SmallVec<[(f64, Vec<Trade>); 20]> = SmallVec::new();
        let mut all_trade_events: SmallVec<[TradeEvent; 256]> = SmallVec::new();

        // 处理每个订单，收集所有TradeEvent到共享的batch中
        for order in orders.into_iter() {
            let (mut filled, mut trade_indices): (f64, SmallVec<[usize; 64]>) =
                (0.0, SmallVec::new());

            // 外层循环：获取最优对手价
            loop {
                if filled.ge_eps(order.quantity) {
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
                                let price_matches = match order.side {
                                    Side::Buy => order.price.ge_eps(price),
                                    Side::Sell => order.price.le_eps(price),
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
                    if filled.ge_eps(order.quantity) {
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

                    let order_remaining = order.quantity - filled;
                    let counter_remaining = counter_order.remaining();

                    if !counter_remaining.positive() {
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

                    let trade_qty = order_remaining.min(counter_remaining);

                    // 更新订单状态
                    {
                        if let Some(&pool_idx) = self.orders.get(&order.id) {
                            if let Some(o) = self.pools.orders.get_mut(pool_idx) {
                                o.filled += trade_qty;
                            }
                        }
                        if let Some(&counter_pool_idx) = self.orders.get(&counter_order_id) {
                            if let Some(o) = self.pools.orders.get_mut(counter_pool_idx) {
                                o.filled += trade_qty;
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
                            trade_qty,
                        );
                    }

                    filled += trade_qty;

                    let trade_idx = self
                        .pools
                        .trades
                        .acquire()
                        .ok_or(MatchingEngineError::OrderPoolExhausted)?;

                    {
                        if let Some(trade) = self.pools.trades.get_mut(trade_idx) {
                            trade.taker_id = order.id;
                            trade.maker_id = counter_order_id;
                            trade.price = best_price;
                            trade.quantity = trade_qty;
                        }
                    }

                    trade_indices.push(trade_idx);

                    // 收集TradeEvent到共享batch中（不立即发送）
                    let trade_event = TradeEvent::new(
                        self.trade_sequence,
                        order.id,
                        counter_order_id,
                        order.timestamp,
                        best_price,
                        trade_qty,
                        order.side,
                        order.id,
                        counter_order_id,
                    );
                    self.trade_sequence += 1;
                    all_trade_events.push(trade_event);

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
            if filled.lt_eps(order.quantity) {
                let mut remaining_order = order;
                remaining_order.filled = filled;

                // 优化：先检查价位是否已存在，避免redundant insert_level
                let book = match remaining_order.side {
                    Side::Buy => &mut self.buy_book,
                    Side::Sell => &mut self.sell_book,
                };

                if book.get_node_at_price(remaining_order.price).is_none() {
                    // 价位不存在，需要插入
                    let _ = book.insert_level(remaining_order.price);
                }

                // 添加订单到价位
                book.add_order_at_level(
                    remaining_order.price,
                    remaining_order.id,
                    remaining_order.quantity - remaining_order.filled,
                )
                .map_err(|_| MatchingEngineError::NodePoolExhausted)?;

                // 储存池索引
                let pool_idx = self
                    .pools
                    .orders
                    .acquire()
                    .ok_or(MatchingEngineError::OrderPoolExhausted)?;

                if let Some(o) = self.pools.orders.get_mut(pool_idx) {
                    *o = remaining_order;
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
        let remaining = order.remaining();
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

    /// Cancel multiple orders in one pass. Returns (order_id, cancelled_qty) for each
    /// successfully cancelled order; silently skips orders not found or already filled.
    pub fn cancel_orders_batch(
        &mut self,
        order_ids: &[u64],
    ) -> smallvec::SmallVec<[(u64, f64); 64]> {
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

        book.remove_order_at_level(order.price, order_id)
            .map_err(|_| MatchingEngineError::OrderNotFound)?;

        // Remove the price level entirely if no orders remain, to avoid ghost
        // 0-qty levels showing up in get_top_levels() depth snapshots.
        let level_empty = book
            .get_node_at_price(order.price)
            .map(|n| n.orders.is_empty())
            .unwrap_or(true);
        if level_empty {
            let _ = book.remove_level(order.price);
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
            let mut total_remaining = 0.0;
            if let Some(node) = self.buy_book.get_node_at_price(price) {
                // 遍历链表中的订单
                let mut node_idx_opt = node.orders.front();
                let list_pool = &self.buy_book.list_pool;
                while let Some(node_idx) = node_idx_opt {
                    if let Some(list_node) = list_pool.get(node_idx) {
                        if let Some(&pool_idx) = self.orders.get(&list_node.order_id) {
                            if let Some(order) = self.pools.orders.get(pool_idx) {
                                total_remaining += order.remaining();
                            }
                        }
                        node_idx_opt = list_node.next;
                    } else {
                        break;
                    }
                }
            }

            if total_remaining.positive() {
                snapshot.add_bid(price, total_remaining).ok();
            }
        }

        // 添加卖盘价格档位（最多20档）- 升序排列
        let sell_levels = self.sell_book.get_top_levels(20);
        for (price, _) in sell_levels {
            // 计算该价格档位的总剩余数量
            let mut total_remaining = 0.0;
            if let Some(node) = self.sell_book.get_node_at_price(price) {
                // 遍历链表中的订单
                let mut node_idx_opt = node.orders.front();
                let list_pool = &self.sell_book.list_pool;
                while let Some(node_idx) = node_idx_opt {
                    if let Some(list_node) = list_pool.get(node_idx) {
                        if let Some(&pool_idx) = self.orders.get(&list_node.order_id) {
                            if let Some(order) = self.pools.orders.get(pool_idx) {
                                total_remaining += order.remaining();
                            }
                        }
                        node_idx_opt = list_node.next;
                    } else {
                        break;
                    }
                }
            }

            if total_remaining.positive() {
                snapshot.add_ask(price, total_remaining).ok();
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
    pub filled: f64,
    pub status: OrderStatus,
    /// Individual fills produced by this order: (maker_order_id, fill_price, fill_qty).
    pub fills: SmallVec<[(u64, f64, f64); 16]>,
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
    pub cancelled_quantity: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_market_buy_vs_limit_sell() {
        let pool_config = PoolConfig::default();
        let mut engine = MatchingEngine::new(pool_config).unwrap();

        // 先放一个卖限价单
        let sell_order = Order::new(1, Side::Sell, 50000.0, 10.0, TimeInForce::GTC, 0);
        let _ = engine.place_order(sell_order).unwrap();

        // 买市价单应该成交
        let buy_market = Order::new_market(2, Side::Buy, 5.0, 0);
        let result = engine.place_order(buy_market).unwrap();

        assert_eq!(result.status, OrderStatus::Filled);
        assert_eq!(result.filled, 5.0);
        assert_eq!(result.order_id, 2);
    }

    #[test]
    fn test_market_sell_vs_limit_buy() {
        let pool_config = PoolConfig::default();
        let mut engine = MatchingEngine::new(pool_config).unwrap();

        // 先放一个买限价单
        let buy_order = Order::new(1, Side::Buy, 50000.0, 10.0, TimeInForce::GTC, 0);
        let _ = engine.place_order(buy_order).unwrap();

        // 卖市价单应该成交
        let sell_market = Order::new_market(2, Side::Sell, 5.0, 0);
        let result = engine.place_order(sell_market).unwrap();

        assert_eq!(result.status, OrderStatus::Filled);
        assert_eq!(result.filled, 5.0);
    }

    #[test]
    fn test_market_order_empty_book() {
        let pool_config = PoolConfig::default();
        let mut engine = MatchingEngine::new(pool_config).unwrap();

        // 空盘口，市价单应该被拒绝
        let buy_market = Order::new_market(1, Side::Buy, 10.0, 0);
        let result = engine.place_order(buy_market).unwrap();

        assert_eq!(result.status, OrderStatus::Rejected);
        assert_eq!(result.filled, 0.0);
    }

    #[test]
    fn test_market_sell_empty_book_rejected() {
        let pool_config = PoolConfig::default();
        let mut engine = MatchingEngine::new(pool_config).unwrap();

        // Sell-side mirror of test_market_order_empty_book. QA needed this path
        // verified directly because the seeded SOL_USDT book is too deep to
        // drain from a normal test account.
        let sell_market = Order::new_market(1, Side::Sell, 10.0, 0);
        let result = engine.place_order(sell_market).unwrap();

        assert_eq!(result.status, OrderStatus::Rejected);
        assert_eq!(result.filled, 0.0);
    }

    #[test]
    fn test_market_buy_with_only_bids_rejected() {
        let pool_config = PoolConfig::default();
        let mut engine = MatchingEngine::new(pool_config).unwrap();

        // Same-side liquidity must not satisfy a market order: a buy needs asks.
        let resting_bid = Order::new(1, Side::Buy, 50_000.0, 10.0, TimeInForce::GTC, 0);
        engine.place_order(resting_bid).unwrap();

        let buy_market = Order::new_market(2, Side::Buy, 5.0, 0);
        let result = engine.place_order(buy_market).unwrap();

        assert_eq!(result.status, OrderStatus::Rejected);
        assert_eq!(result.filled, 0.0);
    }

    #[test]
    fn test_fully_consumed_level_does_not_leave_ghost() {
        // Regression: after a maker order is fully consumed, the price level
        // must not stay in the book as a 0-qty ghost. Without remove_level
        // get_top_levels would surface `(50000.0, 0.0)` which the depth
        // broadcast then renders as a phantom ask.
        let pool_config = PoolConfig::default();
        let mut engine = MatchingEngine::new(pool_config).unwrap();

        let sell = Order::new(1, Side::Sell, 50_000.0, 1.0, TimeInForce::GTC, 0);
        engine.place_order(sell).unwrap();
        let buy = Order::new_market(2, Side::Buy, 1.0, 0);
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
        let resting_ask = Order::new(1, Side::Sell, 50_000.0, 10.0, TimeInForce::GTC, 0);
        engine.place_order(resting_ask).unwrap();

        let sell_market = Order::new_market(2, Side::Sell, 5.0, 0);
        let result = engine.place_order(sell_market).unwrap();

        assert_eq!(result.status, OrderStatus::Rejected);
        assert_eq!(result.filled, 0.0);
    }

    #[test]
    fn test_market_order_partial_fill() {
        let pool_config = PoolConfig::default();
        let mut engine = MatchingEngine::new(pool_config).unwrap();

        // 放一个 5 个数量的卖限价单
        let sell_order = Order::new(1, Side::Sell, 50000.0, 5.0, TimeInForce::GTC, 0);
        let _ = engine.place_order(sell_order).unwrap();

        // 买市价 10 个，但只有 5 个可成交
        let buy_market = Order::new_market(2, Side::Buy, 10.0, 0);
        let result = engine.place_order(buy_market).unwrap();

        assert_eq!(result.status, OrderStatus::PartiallyFilled);
        assert_eq!(result.filled, 5.0);
    }

    #[test]
    fn test_duplicate_order_id_is_rejected_without_book_mutation() {
        let pool_config = PoolConfig {
            order_capacity: 16,
            queue_capacity: 16,
        };
        let mut engine = MatchingEngine::new(pool_config).unwrap();

        let first = Order::new(1, Side::Buy, 100.0, 2.0, TimeInForce::GTC, 0);
        engine.place_order(first).unwrap();

        let duplicate = Order::new(1, Side::Sell, 101.0, 3.0, TimeInForce::GTC, 0);
        let err = engine.place_order(duplicate).unwrap_err();
        assert!(matches!(err, MatchingEngineError::DuplicateOrderId(1)));

        let original = engine.get_order(1).unwrap();
        assert_eq!(original.side, Side::Buy);
        assert_eq!(original.price, 100.0);
        assert_eq!(engine.get_top_levels(10, true), vec![(100.0, 2.0)]);
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
            .place_order(Order::new(1, Side::Sell, 100.0, 10.0, TimeInForce::GTC, 0))
            .unwrap();
        let result = engine
            .place_order(Order::new_market(2, Side::Buy, 4.0, 0))
            .unwrap();

        assert_eq!(result.status, OrderStatus::Filled);
        assert_eq!(engine.get_top_levels(10, false), vec![(100.0, 6.0)]);
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
                    100.0 + i as f64,
                    1.0,
                    TimeInForce::GTC,
                    0,
                ))
                .unwrap();
        }

        let result = engine
            .place_order(Order::new(
                2_000,
                Side::Buy,
                1_200.0,
                1_001.0,
                TimeInForce::FOK,
                0,
            ))
            .unwrap();

        assert_eq!(result.status, OrderStatus::Filled);
        assert_eq!(result.filled, 1_001.0);
        assert!(engine.get_top_levels(10, false).is_empty());
    }

    #[test]
    fn test_no_trade_event_without_sender() {
        let pool_config = PoolConfig::default();
        let mut engine = MatchingEngine::new(pool_config).unwrap();

        // 不设置发送器，应该不会崩溃
        let buy_order = Order::new(1, Side::Buy, 50000.0, 10.0, TimeInForce::GTC, 0);
        engine.place_order(buy_order).unwrap();

        let sell_order = Order::new(2, Side::Sell, 50000.0, 10.0, TimeInForce::IOC, 0);
        let result = engine.place_order(sell_order).unwrap();

        // 成交应该正常发生
        assert_eq!(result.status, OrderStatus::Filled);
    }
}
