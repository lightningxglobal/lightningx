use crate::list_pool::PooledList;

/// 价格档位信息 - 与 SkipListNode 的前三个字段布局兼容
#[derive(Debug)]
#[repr(C)]
pub struct PriceLevel {
    pub price: f64,
    pub total_quantity: f64,
    pub orders: PooledList,
}

impl PriceLevel {
    pub fn new(price: f64) -> Self {
        Self {
            price,
            total_quantity: 0.0,
            orders: PooledList::new(),
        }
    }
}

/// OrderBook 公共接口
/// 支持不同的 Order Book 实现（SkipList、ArrayOrderBook 等）
pub trait OrderBook {
    /// 插入新的价格档位
    fn insert_level(&mut self, price: f64) -> Result<(), String>;

    /// 查找价格是否存在
    fn find_node(&self, price: f64) -> Result<(), String>;

    /// 获取指定价格的只读引用
    fn get_node_at_price(&self, price: f64) -> Option<&PriceLevel>;

    /// 获取指定价格的可变引用
    fn get_node_mut(&mut self, price: f64) -> Option<&mut PriceLevel>;

    /// 获取最优价格（最高或最低取决于排序方向）
    fn best(&self) -> Option<&PriceLevel>;

    /// 获取最优且有订单的价格
    fn best_with_orders(&self) -> Option<&PriceLevel>;

    /// 向指定价格添加订单
    fn add_order_at_level(
        &mut self,
        price: f64,
        order_id: u64,
        quantity: f64,
    ) -> Result<(), String>;

    /// 从指定价格移除订单
    fn remove_order_at_level(&mut self, price: f64, order_id: u64) -> Result<(), String>;

    /// 移除整个价格档位
    fn remove_level(&mut self, price: f64) -> Result<(), String>;

    /// 清空 OrderBook
    fn clear(&mut self);

    /// 获取前 N 个价格档位
    fn get_top_levels(&self, limit: usize) -> Vec<(f64, f64)>;

    /// 获取节点数量
    fn count(&self) -> usize;

    /// 获取 list pool 的引用（用于访问订单列表）
    fn get_list_pool(&mut self) -> &mut crate::list_pool::ListNodePool;

    /// 获取指定索引的链表节点
    fn get_list_node(&self, index: usize) -> Option<&crate::list_pool::ListNode> {
        // 默认实现通过获取 list_pool 和获取节点
        // 注：这需要可变 self，所以在调用时需要特殊处理
        None
    }
}
