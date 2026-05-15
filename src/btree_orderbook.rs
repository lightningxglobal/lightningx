use crate::orderbook::{OrderBook, PriceLevel as OrderBookPriceLevel};
use crate::skiplist::SortOrder;
use crate::list_pool::{ListNodePool, PooledList};
use std::collections::BTreeMap;
use ordered_float::OrderedFloat;

/// BTreeMap 实现的 OrderBook - 缓存局部性更好，常数因子更小
pub struct BTreeOrderBook {
    book: BTreeMap<OrderedFloat<f64>, PriceLevel>,
    order: SortOrder,
    pub list_pool: ListNodePool,
    count: usize,
}

#[repr(C, align(64))]
pub struct PriceLevel {
    pub price: f64,
    pub total_quantity: f64,
    pub orders: PooledList,
}

impl PriceLevel {
    fn new(price: f64) -> Self {
        Self {
            price,
            total_quantity: 0.0,
            orders: PooledList::new(),
        }
    }
}

impl BTreeOrderBook {
    pub fn new_with_pool(order: SortOrder, pool_capacity: usize) -> Self {
        Self {
            book: BTreeMap::new(),
            order,
            list_pool: ListNodePool::new(pool_capacity),
            count: 0,
        }
    }

    fn is_less(&self, a: f64, b: f64) -> bool {
        match self.order {
            SortOrder::Ascending => a < b,
            SortOrder::Descending => a > b,
        }
    }
}

impl OrderBook for BTreeOrderBook {
    fn insert_level(&mut self, price: f64) -> Result<(), String> {
        if self.book.contains_key(&OrderedFloat(price)) {
            return Err("Price level already exists".into());
        }
        self.book.insert(OrderedFloat(price), PriceLevel::new(price));
        self.count += 1;
        Ok(())
    }

    fn find_node(&self, price: f64) -> Result<(), String> {
        if self.book.contains_key(&OrderedFloat(price)) {
            Ok(())
        } else {
            Err("Price not found".into())
        }
    }

    fn get_node_at_price(&self, price: f64) -> Option<&OrderBookPriceLevel> {
        self.book.get(&OrderedFloat(price)).map(|node| unsafe {
            &*(node as *const PriceLevel as *const OrderBookPriceLevel)
        })
    }

    fn get_node_mut(&mut self, price: f64) -> Option<&mut OrderBookPriceLevel> {
        self.book.get_mut(&OrderedFloat(price)).map(|node| unsafe {
            &mut *(node as *mut PriceLevel as *mut OrderBookPriceLevel)
        })
    }

    fn best(&self) -> Option<&OrderBookPriceLevel> {
        match self.order {
            SortOrder::Ascending => {
                self.book.iter().next().map(|(_, node)| unsafe {
                    &*(node as *const PriceLevel as *const OrderBookPriceLevel)
                })
            }
            SortOrder::Descending => {
                self.book.iter().next_back().map(|(_, node)| unsafe {
                    &*(node as *const PriceLevel as *const OrderBookPriceLevel)
                })
            }
        }
    }

    fn best_with_orders(&self) -> Option<&OrderBookPriceLevel> {
        match self.order {
            SortOrder::Ascending => {
                for (_, node) in self.book.iter() {
                    if !node.orders.is_empty() {
                        return Some(unsafe {
                            &*(node as *const PriceLevel as *const OrderBookPriceLevel)
                        });
                    }
                }
                None
            }
            SortOrder::Descending => {
                for (_, node) in self.book.iter().rev() {
                    if !node.orders.is_empty() {
                        return Some(unsafe {
                            &*(node as *const PriceLevel as *const OrderBookPriceLevel)
                        });
                    }
                }
                None
            }
        }
    }

    fn add_order_at_level(
        &mut self,
        price: f64,
        order_id: u64,
        quantity: f64,
    ) -> Result<(), String> {
        if let Some(node) = self.book.get_mut(&OrderedFloat(price)) {
            let node_idx = self.list_pool
                .acquire(order_id, quantity)
                .ok_or("ListNodePool exhausted")?;
            node.orders.push_back(node_idx, &mut self.list_pool);
            node.total_quantity += quantity;
            Ok(())
        } else {
            Err("Price level not found".into())
        }
    }

    fn remove_order_at_level(&mut self, price: f64, order_id: u64) -> Result<(), String> {
        if let Some(node) = self.book.get_mut(&OrderedFloat(price)) {
            // 首先找到要移除的节点索引和数量
            let mut to_remove: Option<(usize, f64)> = None;

            if let Some(node_idx) = node.orders.front() {
                if let Some(list_node) = self.list_pool.get(node_idx) {
                    if list_node.order_id == order_id {
                        to_remove = Some((node_idx, list_node.quantity));
                    }
                }
            }

            if to_remove.is_none() {
                // 线性搜索其他节点
                let mut current = node.orders.front();
                while let Some(idx) = current {
                    if let Some(list_node) = self.list_pool.get(idx) {
                        let next = list_node.next;
                        if list_node.order_id == order_id {
                            to_remove = Some((idx, list_node.quantity));
                            break;
                        }
                        current = next;
                    } else {
                        break;
                    }
                }
            }

            // 执行移除操作
            if let Some((node_idx, quantity)) = to_remove {
                node.orders.remove(node_idx, &mut self.list_pool);
                node.total_quantity -= quantity;
                self.list_pool.release(node_idx);
                Ok(())
            } else {
                Err("Order not found at price level".into())
            }
        } else {
            Err("Price level not found".into())
        }
    }

    fn remove_level(&mut self, price: f64) -> Result<(), String> {
        if self.book.remove(&OrderedFloat(price)).is_some() {
            self.count -= 1;
            Ok(())
        } else {
            Err("Price level not found".into())
        }
    }

    fn clear(&mut self) {
        self.book.clear();
        self.count = 0;
    }

    fn get_top_levels(&self, limit: usize) -> Vec<(f64, f64)> {
        match self.order {
            SortOrder::Ascending => {
                self.book
                    .iter()
                    .take(limit)
                    .map(|(_, node)| (node.price, node.total_quantity))
                    .collect()
            }
            SortOrder::Descending => {
                self.book
                    .iter()
                    .rev()
                    .take(limit)
                    .map(|(_, node)| (node.price, node.total_quantity))
                    .collect()
            }
        }
    }

    fn count(&self) -> usize {
        self.count
    }

    fn get_list_pool(&mut self) -> &mut ListNodePool {
        &mut self.list_pool
    }
}
