use crate::list_pool::PooledList;
use crate::order::{PriceTicks, QuantityLots};

/// 价格档位信息 - 与 SkipListNode 的前三个字段布局兼容
#[derive(Debug)]
#[repr(C)]
pub struct PriceLevel {
    pub price_ticks: PriceTicks,
    pub total_quantity_lots: QuantityLots,
    pub orders: PooledList,
}

impl PriceLevel {
    pub fn new(price_ticks: PriceTicks) -> Self {
        Self {
            price_ticks,
            total_quantity_lots: 0,
            orders: PooledList::new(),
        }
    }
}

/// OrderBook 公共接口
/// 支持不同的 Order Book 实现（SkipList、ArrayOrderBook 等）
pub trait OrderBook {
    /// 插入新的价格档位
    fn insert_level(&mut self, price_ticks: PriceTicks) -> Result<(), String>;

    /// 查找价格是否存在
    fn find_node(&self, price_ticks: PriceTicks) -> Result<(), String>;

    /// 获取指定价格的只读引用
    fn get_node_at_price(&self, price_ticks: PriceTicks) -> Option<&PriceLevel>;

    /// 获取指定价格的可变引用
    fn get_node_mut(&mut self, price_ticks: PriceTicks) -> Option<&mut PriceLevel>;

    /// 获取最优价格（最高或最低取决于排序方向）
    fn best(&self) -> Option<&PriceLevel>;

    /// 获取最优且有订单的价格
    fn best_with_orders(&self) -> Option<&PriceLevel>;

    /// 向指定价格添加订单
    fn add_order_at_level(
        &mut self,
        price_ticks: PriceTicks,
        order_id: u64,
        quantity_lots: QuantityLots,
    ) -> Result<(), String>;

    /// 从指定价格移除订单
    fn remove_order_at_level(
        &mut self,
        price_ticks: PriceTicks,
        order_id: u64,
    ) -> Result<(), String>;

    /// 移除整个价格档位
    fn remove_level(&mut self, price_ticks: PriceTicks) -> Result<(), String>;

    /// 清空 OrderBook
    fn clear(&mut self);

    /// 获取前 N 个价格档位
    fn get_top_levels(&self, limit: usize) -> Vec<(PriceTicks, QuantityLots)>;

    /// 获取节点数量
    fn count(&self) -> usize;

    /// 获取 list pool 的引用（用于访问订单列表）
    fn get_list_pool(&mut self) -> &mut crate::list_pool::ListNodePool;

    /// 获取指定索引的链表节点
    fn get_list_node(&self, _index: usize) -> Option<&crate::list_pool::ListNode> {
        // 默认实现通过获取 list_pool 和获取节点
        // 注：这需要可变 self，所以在调用时需要特殊处理
        None
    }

    /// 获取缓存的最优价格（如果有效）或重新计算
    fn get_best_price(&mut self) -> Option<PriceTicks>;

    /// 使缓存失效（调用者在发现价格档位为空时调用）
    fn invalidate_best_price(&mut self);
}
