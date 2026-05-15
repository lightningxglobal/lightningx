use crate::orderbook::{OrderBook, PriceLevel};
use crate::skiplist::SkipList;
use crate::array_orderbook::ArrayOrderBook;

/// OrderBook 实现选择
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderBookType {
    SkipList,
    Array,
}

/// OrderBook 包装器，支持不同的实现
pub enum OrderBookWrapper {
    SkipList(SkipList),
    Array(ArrayOrderBook),
}

impl OrderBookWrapper {
    pub fn new_skiplist(order: crate::skiplist::SortOrder, pool_capacity: usize) -> Self {
        OrderBookWrapper::SkipList(SkipList::new_with_pool(order, pool_capacity))
    }

    pub fn new_array(order: crate::skiplist::SortOrder, pool_capacity: usize) -> Self {
        OrderBookWrapper::Array(ArrayOrderBook::new_with_pool(order, pool_capacity))
    }

    pub fn new(book_type: OrderBookType, order: crate::skiplist::SortOrder, pool_capacity: usize) -> Self {
        match book_type {
            OrderBookType::SkipList => Self::new_skiplist(order, pool_capacity),
            OrderBookType::Array => Self::new_array(order, pool_capacity),
        }
    }

    /// 获取 list_pool 的不可变引用
    pub fn list_pool(&self) -> &crate::list_pool::ListNodePool {
        match self {
            OrderBookWrapper::SkipList(book) => &book.list_pool,
            OrderBookWrapper::Array(book) => &book.list_pool,
        }
    }
}

impl OrderBook for OrderBookWrapper {
    fn insert_level(&mut self, price: f64) -> Result<(), String> {
        match self {
            OrderBookWrapper::SkipList(book) => book.insert_level(price),
            OrderBookWrapper::Array(book) => book.insert_level(price),
        }
    }

    fn find_node(&self, price: f64) -> Result<(), String> {
        match self {
            OrderBookWrapper::SkipList(book) => book.find_node(price),
            OrderBookWrapper::Array(book) => book.find_node(price),
        }
    }

    fn get_node_at_price(&self, price: f64) -> Option<&PriceLevel> {
        match self {
            // 两个实现都通过 OrderBook trait 返回正确的类型
            OrderBookWrapper::SkipList(book) => {
                // book 实现了 OrderBook trait，所以返回的应该是 &PriceLevel
                <SkipList as OrderBook>::get_node_at_price(book, price)
            }
            OrderBookWrapper::Array(book) => book.get_node_at_price(price),
        }
    }

    fn get_node_mut(&mut self, price: f64) -> Option<&mut PriceLevel> {
        match self {
            OrderBookWrapper::SkipList(book) => {
                <SkipList as OrderBook>::get_node_mut(book, price)
            }
            OrderBookWrapper::Array(book) => book.get_node_mut(price),
        }
    }

    fn best(&self) -> Option<&PriceLevel> {
        match self {
            OrderBookWrapper::SkipList(book) => {
                <SkipList as OrderBook>::best(book)
            }
            OrderBookWrapper::Array(book) => book.best(),
        }
    }

    fn best_with_orders(&self) -> Option<&PriceLevel> {
        match self {
            OrderBookWrapper::SkipList(book) => {
                <SkipList as OrderBook>::best_with_orders(book)
            }
            OrderBookWrapper::Array(book) => book.best_with_orders(),
        }
    }

    fn add_order_at_level(
        &mut self,
        price: f64,
        order_id: u64,
        quantity: f64,
    ) -> Result<(), String> {
        match self {
            OrderBookWrapper::SkipList(book) => book.add_order_at_level(price, order_id, quantity),
            OrderBookWrapper::Array(book) => book.add_order_at_level(price, order_id, quantity),
        }
    }

    fn remove_order_at_level(&mut self, price: f64, order_id: u64) -> Result<(), String> {
        match self {
            OrderBookWrapper::SkipList(book) => book.remove_order_at_level(price, order_id),
            OrderBookWrapper::Array(book) => book.remove_order_at_level(price, order_id),
        }
    }

    fn remove_level(&mut self, price: f64) -> Result<(), String> {
        match self {
            OrderBookWrapper::SkipList(book) => book.remove_level(price),
            OrderBookWrapper::Array(book) => book.remove_level(price),
        }
    }

    fn clear(&mut self) {
        match self {
            OrderBookWrapper::SkipList(book) => book.clear(),
            OrderBookWrapper::Array(book) => book.clear(),
        }
    }

    fn get_top_levels(&self, limit: usize) -> Vec<(f64, f64)> {
        match self {
            OrderBookWrapper::SkipList(book) => book.get_top_levels(limit),
            OrderBookWrapper::Array(book) => book.get_top_levels(limit),
        }
    }

    fn count(&self) -> usize {
        match self {
            OrderBookWrapper::SkipList(book) => book.count(),
            OrderBookWrapper::Array(book) => book.count(),
        }
    }

    fn get_list_pool(&mut self) -> &mut crate::list_pool::ListNodePool {
        match self {
            OrderBookWrapper::SkipList(book) => book.get_list_pool(),
            OrderBookWrapper::Array(book) => book.get_list_pool(),
        }
    }
}
