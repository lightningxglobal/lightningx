use crate::order::{Order, Side, TimeInForce};
use crate::error::{MatchingEngineError, OrderResult};
use crate::event::MatchingEvent;
use crate::market_data::TradeEvent;
use crate::pools::Pools;
use crate::skiplist::SortOrder;
use crate::orderbook_impl::OrderBookWrapper;
use crate::orderbook::OrderBook;
use std::collections::HashMap;
use rtrb::Producer;

pub struct MatchingEngine {
    buy_book: OrderBookWrapper,
    sell_book: OrderBookWrapper,
    orders: HashMap<u64, Order>,
    pools: Pools,
    next_order_id: u64,
    snapshot_sequence: u64,
    trade_event_sender: Option<Producer<TradeEvent>>,
    trade_sequence: u64,
}

impl MatchingEngine {
    /// 创建新的撮合引擎
    pub fn new(pool_config: PoolConfig) -> OrderResult<Self> {
        Ok(Self {
            buy_book: OrderBookWrapper::new(pool_config.orderbook_type, SortOrder::Descending, pool_config.queue_capacity),
            sell_book: OrderBookWrapper::new(pool_config.orderbook_type, SortOrder::Ascending, pool_config.queue_capacity),
            orders: HashMap::with_capacity(pool_config.order_capacity),
            pools: Pools::new(pool_config.order_capacity, pool_config.queue_capacity),
            next_order_id: 1,
            snapshot_sequence: 0,
            trade_event_sender: None,
            trade_sequence: 0,
        })
    }

    /// 设置交易事件发送器
    pub fn set_trade_event_sender(&mut self, sender: Producer<TradeEvent>) {
        self.trade_event_sender = Some(sender);
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

    /// 下单（热路径）
    #[inline]
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
        // 插入价格档位（如果不存在）
        let book = match order.side {
            Side::Buy => &mut self.buy_book,
            Side::Sell => &mut self.sell_book,
        };

        // 尝试插入新价格级别，如果已存在则忽略错误
        let _ = book.insert_level(order.price);

        // 添加订单到价格档位队列
        book.add_order_at_level(order.price, order.id, order.quantity)
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

    /// 撮合订单（关键热路径）
    #[inline]
    fn match_order(&mut self, order: Order) -> OrderResult<(f64, Vec<Trade>)> {
        let mut filled = 0.0;
        let mut trades = Vec::new();

        // 循环撮合直到不能撮合或订单完全成交
        loop {
            if filled >= order.quantity {
                break;
            }

            // 获取最优对手价（有订单的价格档位）
            let best_price = {
                let opposite_book = match order.side {
                    Side::Buy => &self.sell_book,
                    Side::Sell => &self.buy_book,
                };

                match opposite_book.best_with_orders() {
                    Some(n) => {
                        // 检查价格是否匹配
                        let price_matches = match order.side {
                            Side::Buy => order.price >= n.price,
                            Side::Sell => order.price <= n.price,
                        };

                        if !price_matches {
                            break;
                        }

                        Some(n.price)
                    }
                    None => None,
                }
            };

            let best_price = match best_price {
                Some(p) => p,
                None => break,
            };

            // 从对手订单簿获取最优价格级别的第一个订单ID
            // 使用List实现后，取消的订单已被真正移除，无需检查cancelled标志
            let counter_order_id = {
                let opposite_book = match order.side {
                    Side::Buy => &mut self.sell_book,
                    Side::Sell => &mut self.buy_book,
                };

                match opposite_book.get_node_mut(best_price) {
                    Some(node) => {
                        // 获取链表的第一个节点
                        if let Some(node_idx) = node.orders.front() {
                            // 获取 list_pool 的可变引用来访问订单节点
                            let list_pool = opposite_book.get_list_pool();
                            if let Some(list_node) = list_pool.get(node_idx) {
                                list_node.order_id
                            } else {
                                break;
                            }
                        } else {
                            break;  // 链表为空
                        }
                    }
                    None => break,
                }
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

            // 发布交易事件到ring buffer
            if let Some(ref mut sender) = self.trade_event_sender {
                let trade_event = TradeEvent::new(
                    self.trade_sequence,
                    order.id,
                    counter_order_id,
                    order.timestamp,
                    best_price,
                    trade_qty,
                    order.side,
                    order.id,  // 吃单方用户ID（使用订单ID）
                    counter_order_id,  // 挂单方用户ID（使用订单ID）
                );
                self.trade_sequence += 1;
                // 忽略发送错误（接收方可能已关闭或缓冲满）
                let _ = sender.push(trade_event);
            }

            // 检查对手订单是否完全成交，如果是则从订单簿移除
            if let Some(counter) = self.orders.get(&counter_order_id) {
                if counter.is_filled() {
                    // 从订单簿中移除已成交的订单
                    let opposite_book = match order.side {
                        Side::Buy => &mut self.sell_book,
                        Side::Sell => &mut self.buy_book,
                    };
                    let _ = opposite_book.remove_order_at_level(best_price, counter_order_id);
                }
            }
        }

        Ok((filled, trades))
    }

    // ===== Task 10: Cancel Order =====

    /// 撤销订单（热路径 - 使用List实现，真正删除）
    #[inline]
    pub fn cancel_order(&mut self, order_id: u64) -> OrderResult<CancelOrderResult> {
        // 查询订单
        let order = self.orders.get(&order_id)
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

        // 从orders HashMap中移除
        self.orders.remove(&order_id);

        // 发布撤单事件
        self.publish_event(&MatchingEvent::OrderCancelled {
            order_id,
            timestamp,
        });

        Ok(CancelOrderResult {
            order_id,
            cancelled_quantity: remaining,
        })
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

    /// 生成市场深度快照
    pub fn generate_depth_snapshot(&self) -> crate::snapshot::DepthSnapshot {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);

        let mut snapshot = crate::snapshot::DepthSnapshot::new(timestamp, self.snapshot_sequence);

        // 添加买盘价格档位（最多20档）- 降序排列
        let buy_levels = self.buy_book.get_top_levels(20);
        for (price, _) in buy_levels {
            // 计算该价格档位的总剩余数量
            let mut total_remaining = 0.0;
            if let Some(node) = self.buy_book.get_node_at_price(price) {
                // 遍历链表中的订单
                let mut node_idx_opt = node.orders.front();
                let list_pool = self.buy_book.list_pool();
                while let Some(node_idx) = node_idx_opt {
                    if let Some(list_node) = list_pool.get(node_idx) {
                        if let Some(order) = self.orders.get(&list_node.order_id) {
                            total_remaining += order.remaining();
                        }
                        node_idx_opt = list_node.next;
                    } else {
                        break;
                    }
                }
            }

            if total_remaining > 0.0 {
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
                let list_pool = self.sell_book.list_pool();
                while let Some(node_idx) = node_idx_opt {
                    if let Some(list_node) = list_pool.get(node_idx) {
                        if let Some(order) = self.orders.get(&list_node.order_id) {
                            total_remaining += order.remaining();
                        }
                        node_idx_opt = list_node.next;
                    } else {
                        break;
                    }
                }
            }

            if total_remaining > 0.0 {
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
    pub orderbook_type: crate::orderbook_impl::OrderBookType,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            order_capacity: 1_000_000,
            queue_capacity: 100_000,
            orderbook_type: crate::orderbook_impl::OrderBookType::SkipList,
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

#[cfg(test)]
mod tests {
    use super::*;

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
