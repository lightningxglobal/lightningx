use crate::list_pool::ListNodePool;
use crate::orderbook::{OrderBook, PriceLevel as OrderBookPriceLevel};
use std::collections::HashMap;

// 重导出 SortOrder 从 skiplist
pub use crate::skiplist::SortOrder;

/// 基于数组的 OrderBook（HashMap + 有序价格列表）
/// 优点：避免 SkipList 的随机数生成，二分查找效率高
pub struct ArrayOrderBook {
    levels: HashMap<u64, OrderBookPriceLevel>,  // price as u64 bits -> level
    sorted_prices: Vec<u64>,                    // 已排序的价格（u64 bits 表示）
    order: SortOrder,
    count: usize,
    pub list_pool: ListNodePool,
    best_price: Option<f64>,
}

impl ArrayOrderBook {
    pub fn new_with_pool(order: SortOrder, pool_capacity: usize) -> Self {
        Self {
            levels: HashMap::new(),
            sorted_prices: Vec::new(),
            order,
            count: 0,
            list_pool: ListNodePool::new(pool_capacity),
            best_price: None,
        }
    }

    #[inline(always)]
    fn price_to_bits(price: f64) -> u64 {
        price.to_bits()
    }

    #[inline(always)]
    fn bits_to_price(bits: u64) -> f64 {
        f64::from_bits(bits)
    }

    #[inline(always)]
    fn should_insert(&self, new_price: f64, existing_price: f64) -> bool {
        match self.order {
            SortOrder::Ascending => new_price < existing_price,
            SortOrder::Descending => new_price > existing_price,
        }
    }

    /// 二分查找价格在排序列表中的位置
    #[inline]
    fn binary_search_position(&self, price: f64) -> Result<usize, usize> {
        let price_bits = Self::price_to_bits(price);
        self.sorted_prices.binary_search(&price_bits)
    }

    /// 插入价格档位
    pub fn insert_level(&mut self, price: f64) -> Result<(), String> {
        let price_bits = Self::price_to_bits(price);

        if self.levels.contains_key(&price_bits) {
            return Err("Price level already exists".to_string());
        }

        let level = OrderBookPriceLevel::new(price);
        self.levels.insert(price_bits, level);

        // 维护排序的价格列表
        match self.binary_search_position(price) {
            Ok(_) => {} // 不应该发生（已检查 exists）
            Err(pos) => {
                self.sorted_prices.insert(pos, price_bits);
            }
        }

        self.count += 1;
        Ok(())
    }

    /// 查找价格是否存在
    pub fn find_node(&self, price: f64) -> Result<(), String> {
        let price_bits = Self::price_to_bits(price);
        if self.levels.contains_key(&price_bits) {
            Ok(())
        } else {
            Err("Price not found".to_string())
        }
    }

    /// 获取指定价格的只读引用
    pub fn get_node_at_price(&self, price: f64) -> Option<&OrderBookPriceLevel> {
        let price_bits = Self::price_to_bits(price);
        self.levels.get(&price_bits)
    }

    /// 获取指定价格的可变引用
    pub fn get_node_mut(&mut self, price: f64) -> Option<&mut OrderBookPriceLevel> {
        let price_bits = Self::price_to_bits(price);
        self.levels.get_mut(&price_bits)
    }

    /// 获取最优价格（买方最低价 或 卖方最高价）
    pub fn best(&self) -> Option<&OrderBookPriceLevel> {
        if let Some(&price_bits) = self.sorted_prices.first() {
            Some(&self.levels[&price_bits])
        } else {
            None
        }
    }

    /// 获取最优且有订单的价格
    pub fn best_with_orders(&self) -> Option<&OrderBookPriceLevel> {
        for &price_bits in &self.sorted_prices {
            let level = &self.levels[&price_bits];
            if !level.orders.is_empty() {
                return Some(level);
            }
        }
        None
    }

    /// 向指定价格添加订单
    pub fn add_order_at_level(
        &mut self,
        price: f64,
        order_id: u64,
        quantity: f64,
    ) -> Result<(), String> {
        let node_idx = self.list_pool.acquire(order_id, quantity)
            .ok_or_else(|| "List pool exhausted".to_string())?;

        let price_bits = Self::price_to_bits(price);
        if let Some(level) = self.levels.get_mut(&price_bits) {
            level.orders.push_back(node_idx, &mut self.list_pool);
            level.total_quantity += quantity;
            Ok(())
        } else {
            self.list_pool.release(node_idx);
            Err(format!("Price level {} not found", price))
        }
    }

    /// 从指定价格移除订单
    pub fn remove_order_at_level(&mut self, price: f64, order_id: u64) -> Result<(), String> {
        let price_bits = Self::price_to_bits(price);

        if let Some(level) = self.levels.get_mut(&price_bits) {
            let mut node_idx_opt = level.orders.front();
            while let Some(node_idx) = node_idx_opt {
                let next_idx = if let Some(node) = self.list_pool.get(node_idx) {
                    if node.order_id == order_id {
                        // 找到了，从链表和池中移除
                        level.orders.remove(node_idx, &mut self.list_pool);
                        self.list_pool.release(node_idx);
                        level.total_quantity -= if let Some(n) = self.list_pool.get(node_idx) {
                            n.quantity
                        } else {
                            0.0
                        };
                        return Ok(());
                    }
                    node.next
                } else {
                    break;
                };
                node_idx_opt = next_idx;
            }
            Err(format!("Order {} not found at price level {}", order_id, price))
        } else {
            Err(format!("Price level {} not found", price))
        }
    }

    /// 移除整个价格档位
    pub fn remove_level(&mut self, price: f64) -> Result<(), String> {
        let price_bits = Self::price_to_bits(price);

        if self.levels.remove(&price_bits).is_some() {
            if let Ok(pos) = self.binary_search_position(price) {
                self.sorted_prices.remove(pos);
            }
            self.count -= 1;
            Ok(())
        } else {
            Err("Price not found".to_string())
        }
    }

    /// 清空 OrderBook
    pub fn clear(&mut self) {
        self.levels.clear();
        self.sorted_prices.clear();
        self.count = 0;
        self.best_price = None;
    }

    /// 获取缓存的最优价格（如果有效）或重新计算
    pub fn get_best_price_cached(&mut self) -> Option<f64> {
        // Check if cached price is still valid
        if let Some(price) = self.best_price {
            if let Some(node) = self.get_node_at_price(price) {
                if node.total_quantity > 0.0 {
                    return Some(price);
                }
            }
            // Cache invalid, find next best
            self.best_price = None;
        }

        // Find best price with orders
        let best = self.best_with_orders()
            .map(|node| node.price);
        self.best_price = best;
        best
    }

    /// 使缓存失效
    pub fn invalidate_best_price_cache(&mut self) {
        self.best_price = None;
    }

    /// 获取前 N 个价格档位
    pub fn get_top_levels(&self, limit: usize) -> Vec<(f64, f64)> {
        self.sorted_prices
            .iter()
            .take(limit)
            .map(|&bits| {
                let price = Self::bits_to_price(bits);
                let qty = self.levels[&bits].total_quantity;
                (price, qty)
            })
            .collect()
    }

    /// 获取节点数量
    #[inline(always)]
    pub fn count(&self) -> usize {
        self.count
    }
}

impl OrderBook for ArrayOrderBook {
    fn insert_level(&mut self, price: f64) -> Result<(), String> {
        ArrayOrderBook::insert_level(self, price)
    }

    fn find_node(&self, price: f64) -> Result<(), String> {
        ArrayOrderBook::find_node(self, price)
    }

    fn get_node_at_price(&self, price: f64) -> Option<&OrderBookPriceLevel> {
        ArrayOrderBook::get_node_at_price(self, price)
    }

    fn get_node_mut(&mut self, price: f64) -> Option<&mut OrderBookPriceLevel> {
        ArrayOrderBook::get_node_mut(self, price)
    }

    fn best(&self) -> Option<&OrderBookPriceLevel> {
        ArrayOrderBook::best(self)
    }

    fn best_with_orders(&self) -> Option<&OrderBookPriceLevel> {
        ArrayOrderBook::best_with_orders(self)
    }

    fn add_order_at_level(
        &mut self,
        price: f64,
        order_id: u64,
        quantity: f64,
    ) -> Result<(), String> {
        ArrayOrderBook::add_order_at_level(self, price, order_id, quantity)
    }

    fn remove_order_at_level(&mut self, price: f64, order_id: u64) -> Result<(), String> {
        ArrayOrderBook::remove_order_at_level(self, price, order_id)
    }

    fn remove_level(&mut self, price: f64) -> Result<(), String> {
        ArrayOrderBook::remove_level(self, price)
    }

    fn clear(&mut self) {
        ArrayOrderBook::clear(self)
    }

    fn get_top_levels(&self, limit: usize) -> Vec<(f64, f64)> {
        ArrayOrderBook::get_top_levels(self, limit)
    }

    fn count(&self) -> usize {
        ArrayOrderBook::count(self)
    }

    fn get_list_pool(&mut self) -> &mut crate::list_pool::ListNodePool {
        &mut self.list_pool
    }

    fn get_best_price(&mut self) -> Option<f64> {
        self.get_best_price_cached()
    }

    fn invalidate_best_price(&mut self) {
        self.invalidate_best_price_cache()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_find() {
        let mut book = ArrayOrderBook::new_with_pool(SortOrder::Ascending, 1000);

        assert!(book.insert_level(100.0).is_ok());
        assert!(book.insert_level(101.0).is_ok());
        assert!(book.insert_level(99.0).is_ok());

        assert!(book.find_node(100.0).is_ok());
        assert!(book.find_node(99.0).is_ok());
        assert!(book.find_node(102.0).is_err());

        assert_eq!(book.count(), 3);
    }

    #[test]
    fn test_sorted_order() {
        let mut book = ArrayOrderBook::new_with_pool(SortOrder::Ascending, 1000);

        for price in [100.5, 99.0, 101.2, 100.0, 102.3].iter() {
            let _ = book.insert_level(*price);
        }

        let top = book.get_top_levels(5);
        assert!(top[0].0 < top[1].0); // ascending order
        assert!(top[1].0 < top[2].0);
    }

    #[test]
    fn test_best_price() {
        let mut book = ArrayOrderBook::new_with_pool(SortOrder::Ascending, 1000);

        book.insert_level(100.0).ok();
        book.insert_level(99.0).ok();
        book.insert_level(101.0).ok();

        assert_eq!(book.best().unwrap().price, 99.0);
    }
}
