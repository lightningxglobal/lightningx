use crate::order::{Order, Side, TimeInForce};
use crate::error::{MatchingEngineError, OrderResult};
use crate::event::MatchingEvent;
use crate::pools::Pools;
use crate::skiplist::{SkipList, SortOrder};
use std::collections::HashMap;

pub struct MatchingEngine {
    buy_book: SkipList,
    sell_book: SkipList,
    orders: HashMap<u64, Order>,
    pools: Pools,
    next_order_id: u64,
    _snapshot_sequence: u64,
}

impl MatchingEngine {
    /// 创建新的撮合引擎
    pub fn new(pool_config: PoolConfig) -> OrderResult<Self> {
        Ok(Self {
            buy_book: SkipList::new(SortOrder::Descending),
            sell_book: SkipList::new(SortOrder::Ascending),
            orders: HashMap::with_capacity(pool_config.order_capacity),
            pools: Pools::new(pool_config.order_capacity, pool_config.queue_capacity),
            next_order_id: 1,
            _snapshot_sequence: 0,
        })
    }

    /// 验证订单有效性
    #[inline(always)]
    fn validate_order(&self, order: &Order) -> OrderResult<()> {
        if order.price <= 0.0 || order.price.is_nan() {
            return Err(MatchingEngineError::InvalidPrice(order.price));
        }

        if order.quantity <= 0.0 || order.quantity.is_nan() {
            return Err(MatchingEngineError::InvalidQuantity(order.quantity));
        }

        Ok(())
    }

    /// 获取订单
    #[inline(always)]
    pub fn get_order(&self, order_id: u64) -> Option<Order> {
        self.orders.get(&order_id).copied()
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

    // ===== Task 8: Place Order Methods =====

    /// 下单
    pub fn place_order(&mut self, mut order: Order) -> OrderResult<PlaceOrderResult> {
        // 验证订单
        self.validate_order(&order)?;

        // 检查资源
        if !self.pools.has_space_for_order() {
            return Err(MatchingEngineError::OrderPoolExhausted);
        }

        // 分配订单ID
        order.id = self.next_order_id;
        self.next_order_id += 1;

        // 处理不同的委托类型
        let result = match order.time_in_force {
            TimeInForce::PostOnly => self.handle_post_only(order)?,
            TimeInForce::FOK => self.handle_fok(order)?,
            TimeInForce::IOC => self.handle_ioc(order)?,
            TimeInForce::GTC => self.handle_gtc(order)?,
        };

        Ok(result)
    }

    /// 处理Post-Only订单
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
                return Err(MatchingEngineError::InvalidTimeInForce);
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
        })
    }

    /// 处理FOK订单
    fn handle_fok(&mut self, order: Order) -> OrderResult<PlaceOrderResult> {
        // 尝试撮合
        let (filled_qty, _trades) = self.match_order(order)?;

        if (filled_qty - order.quantity).abs() < 1e-10 {
            // 完全成交
            Ok(PlaceOrderResult {
                order_id: order.id,
                filled: filled_qty,
                status: OrderStatus::Filled,
            })
        } else {
            // 无法完全成交，拒绝
            Err(MatchingEngineError::InvalidTimeInForce)
        }
    }

    /// 处理IOC订单
    fn handle_ioc(&mut self, order: Order) -> OrderResult<PlaceOrderResult> {
        let (filled_qty, _trades) = self.match_order(order)?;

        if filled_qty > 0.0 {
            Ok(PlaceOrderResult {
                order_id: order.id,
                filled: filled_qty,
                status: OrderStatus::Filled,
            })
        } else {
            Ok(PlaceOrderResult {
                order_id: order.id,
                filled: 0.0,
                status: OrderStatus::Rejected,
            })
        }
    }

    /// 处理GTC订单
    fn handle_gtc(&mut self, order: Order) -> OrderResult<PlaceOrderResult> {
        let (filled_qty, _trades) = self.match_order(order)?;

        // 如果有剩余，加入订单簿
        if filled_qty < order.quantity {
            let mut remaining_order = order;
            remaining_order.filled = filled_qty;
            self.add_to_book(remaining_order)?;
        }

        let status = if (filled_qty - order.quantity).abs() < 1e-10 {
            OrderStatus::Filled
        } else if filled_qty > 0.0 {
            OrderStatus::PartiallyFilled
        } else {
            OrderStatus::Accepted
        };

        Ok(PlaceOrderResult {
            order_id: order.id,
            filled: filled_qty,
            status,
        })
    }

    /// 将订单加入订单簿
    fn add_to_book(&mut self, order: Order) -> OrderResult<()> {
        // 插入价格档位
        let book = match order.side {
            Side::Buy => &mut self.buy_book,
            Side::Sell => &mut self.sell_book,
        };

        book.insert_level(order.price)
            .map_err(|_| MatchingEngineError::NodePoolExhausted)?;

        // 储存订单
        self.orders.insert(order.id, order);

        Ok(())
    }

    /// 发布事件到Aeron
    #[inline(always)]
    fn publish_event(&self, _event: &MatchingEvent) {
        // 在当前实现中，我们不连接Aeron。
        // 在生产环境中，这里会将事件发送到Aeron。
    }

    // ===== Task 9: Match Order Logic =====

    /// 撮合订单
    fn match_order(&mut self, order: Order) -> OrderResult<(f64, Vec<Trade>)> {
        let mut filled = 0.0;
        let mut trades = Vec::new();

        // 循环撮合直到不能撮合或订单完全成交
        loop {
            if filled >= order.quantity {
                break;
            }

            // 获取最优对手价
            let (best_price, counter_order_id) = {
                let opposite_book = match order.side {
                    Side::Buy => &self.sell_book,
                    Side::Sell => &self.buy_book,
                };

                let best_node = match opposite_book.best() {
                    Some(n) => n,
                    None => break,
                };

                // 检查价格是否匹配
                let price_matches = match order.side {
                    Side::Buy => order.price >= best_node.price,
                    Side::Sell => order.price <= best_node.price,
                };

                if !price_matches {
                    break;
                }

                // 从队列中取出对手订单
                if best_node.orders.is_empty() {
                    break;
                }

                let counter_id = match best_node.orders.front() {
                    Some(&id) => id,
                    None => break,
                };

                (best_node.price, counter_id)
            };

            let counter_order = match self.orders.get(&counter_order_id) {
                Some(o) => *o,
                None => break,
            };

            // 计算成交数量
            let order_remaining = order.quantity - filled;
            let counter_remaining = counter_order.remaining();
            let trade_qty = order_remaining.min(counter_remaining);

            // 更新订单状态
            {
                if let Some(o) = self.orders.get_mut(&order.id) {
                    o.filled += trade_qty;
                }
                if let Some(o) = self.orders.get_mut(&counter_order_id) {
                    o.filled += trade_qty;
                }
            }

            filled += trade_qty;

            // 发布成交事件
            let trade = Trade {
                taker_id: order.id,
                maker_id: counter_order_id,
                price: best_price,
                quantity: trade_qty,
            };
            trades.push(trade.clone());

            self.publish_event(&MatchingEvent::Trade {
                taker_order_id: order.id,
                maker_order_id: counter_order_id,
                price: best_price,
                quantity: trade_qty,
                timestamp: order.timestamp,
            });

            // 检查对手订单是否完全成交，如果是则从订单簿移除
            if let Some(counter) = self.orders.get(&counter_order_id) {
                if counter.is_filled() {
                    // 从订单簿的价格档位队列中移除此订单
                    let _ = self.remove_order_from_book(counter_order_id, best_price);
                }
            }
        }

        Ok((filled, trades))
    }

    // ===== Task 10: Cancel Order =====

    /// 撤销订单
    pub fn cancel_order(&mut self, order_id: u64) -> OrderResult<CancelOrderResult> {
        // 查询订单
        let order = self.orders.get(&order_id)
            .copied()
            .ok_or(MatchingEngineError::OrderNotFound)?;

        // 检查是否已成交
        if order.is_filled() {
            return Err(MatchingEngineError::AlreadyFilled);
        }

        // 获取剩余数量
        let remaining = order.remaining();

        // 从订单簿移除
        self.remove_from_book(order_id)?;

        // 从订单映射中删除
        self.orders.remove(&order_id);

        // 发布撤单事件
        self.publish_event(&MatchingEvent::OrderCancelled {
            order_id,
            timestamp: order.timestamp,
        });

        Ok(CancelOrderResult {
            order_id,
            cancelled_quantity: remaining,
        })
    }

    /// 从订单簿的价格档位队列中移除已成交订单
    fn remove_order_from_book(&mut self, order_id: u64, price: f64) -> OrderResult<()> {
        let order = self.orders.get(&order_id)
            .copied()
            .ok_or(MatchingEngineError::OrderNotFound)?;

        let book = match order.side {
            Side::Buy => &mut self.buy_book,
            Side::Sell => &mut self.sell_book,
        };

        // 从指定价格的订单队列中移除订单ID
        let _ = book.remove_order_at_level(price, order_id);

        Ok(())
    }

    /// 从订单簿移除未成交订单（取消订单时调用）
    fn remove_from_book(&mut self, order_id: u64) -> OrderResult<()> {
        let order = self.orders.get(&order_id)
            .copied()
            .ok_or(MatchingEngineError::OrderNotFound)?;

        let book = match order.side {
            Side::Buy => &mut self.buy_book,
            Side::Sell => &mut self.sell_book,
        };

        // 从价格档位的订单队列中移除此订单
        let _ = book.remove_order_at_level(order.price, order_id);

        Ok(())
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
        Self {
            order_capacity: 1_000_000,
            queue_capacity: 100_000,
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
#[derive(Debug, Clone)]
pub struct Trade {
    pub taker_id: u64,
    pub maker_id: u64,
    pub price: f64,
    pub quantity: f64,
}

/// 撤单结果
#[derive(Debug, Clone)]
pub struct CancelOrderResult {
    pub order_id: u64,
    pub cancelled_quantity: f64,
}
