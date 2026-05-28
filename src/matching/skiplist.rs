use crate::list_pool::{ListNodePool, PooledList};
use crate::order::{PriceTicks, QuantityLots};
use crate::orderbook::{OrderBook, PriceLevel as OrderBookPriceLevel};

const MAX_LEVEL: usize = 12;
const PROMOTION_PROBABILITY: f64 = 0.25;

/// 跳表节点 - 使用原始指针支持多级链接
#[repr(C)]
pub struct SkipListNode {
    pub price_ticks: PriceTicks,
    pub total_quantity_lots: QuantityLots,
    pub orders: PooledList,
    pub level: usize,
    /// 指向各级下一个节点的原始指针数组
    /// 安全性保证：所有指针都指向arena中的节点，在SkipList的生命周期内有效
    pub forward: [*mut SkipListNode; MAX_LEVEL],
}

impl SkipListNode {
    /// 创建新节点 - 用于在arena中初始化
    fn new(price_ticks: PriceTicks, level: usize) -> Self {
        Self {
            price_ticks,
            total_quantity_lots: 0,
            orders: PooledList::new(),
            level,
            forward: [std::ptr::null_mut(); MAX_LEVEL],
        }
    }
}

/// 跳表排序方向
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortOrder {
    Ascending,  // 升序（最小值在头部）
    Descending, // 降序（最大值在头部）
}

/// 跳表实现 - 使用arena存储所有节点，raw pointers实现多级链接
pub struct SkipList {
    /// 指向head sentinel节点的原始指针（在arena中）
    pub head: *mut SkipListNode,
    /// 当前跳表的最大层级
    level: usize,
    /// 节点总数（不含head sentinel）
    count: usize,
    /// 排序方向
    order: SortOrder,
    /// 订单链表节点池
    pub list_pool: ListNodePool,
    /// Arena - 所有SkipListNode的唯一所有者
    arena: Vec<Box<SkipListNode>>,
    /// Unlinked price-level nodes available for reuse.
    free_nodes: Vec<*mut SkipListNode>,
    /// 缓存最优价格
    best_price: Option<PriceTicks>,
}

impl SkipList {
    /// 创建新跳表，指定链表节点池的容量
    pub fn new_with_pool(order: SortOrder, pool_capacity: usize) -> Self {
        let head_price = if order == SortOrder::Ascending {
            PriceTicks::MIN
        } else {
            PriceTicks::MAX
        };

        let mut head_node = Box::new(SkipListNode::new(head_price, 0));
        let head_ptr = head_node.as_mut() as *mut SkipListNode;

        let mut arena = Vec::with_capacity(1000);
        arena.push(head_node);

        Self {
            head: head_ptr,
            level: 0,
            count: 0,
            order,
            list_pool: ListNodePool::new(pool_capacity),
            arena,
            free_nodes: Vec::new(),
            best_price: None,
        }
    }

    /// 生成随机层级
    fn random_level() -> usize {
        let mut lv = 0;
        while lv < MAX_LEVEL - 1 && rand::random::<f64>() < PROMOTION_PROBABILITY {
            lv += 1;
        }
        lv
    }

    /// find_update辅助函数 - 返回所有级别的前驱节点和第0级的目标节点
    ///
    /// 安全性：只在self的生命周期内调用，所有指针都指向arena中的节点
    unsafe fn find_update(
        &self,
        price: PriceTicks,
    ) -> ([*mut SkipListNode; MAX_LEVEL], *mut SkipListNode) {
        let mut update = [std::ptr::null_mut::<SkipListNode>(); MAX_LEVEL];
        let mut current = self.head;

        // 从最高层向下遍历
        for i in (0..=self.level).rev() {
            loop {
                let fwd = (*current).forward[i];
                if fwd.is_null() {
                    break;
                }

                let fwd_price = (*fwd).price_ticks;

                // 如果目标价格已经在下一个节点，停止
                if fwd_price == price {
                    break;
                }

                // 如果应该插入到下一个节点之前，停止
                if self.should_insert(price, fwd_price) {
                    break;
                }

                // 继续向右遍历
                current = fwd;
            }
            update[i] = current;
        }

        // 对于高于当前level的层，直接返回head作为前驱
        for i in (self.level + 1)..MAX_LEVEL {
            update[i] = self.head;
        }

        (update, current)
    }

    /// 获取跳表中的节点数量
    #[inline(always)]
    pub fn count(&self) -> usize {
        self.count
    }

    /// 检查价格是否应该插入到跳表中
    #[inline(always)]
    fn should_insert(&self, new_price: PriceTicks, existing_price: PriceTicks) -> bool {
        match self.order {
            SortOrder::Ascending => new_price < existing_price,
            SortOrder::Descending => new_price > existing_price,
        }
    }

    /// 获取指定价格的只读节点引用
    #[inline]
    pub fn get_node_at_price(&self, price: PriceTicks) -> Option<&SkipListNode> {
        unsafe {
            let mut current = self.head;
            // 从最高层向下遍历
            for i in (0..=self.level).rev() {
                loop {
                    let fwd = (*current).forward[i];
                    if fwd.is_null() {
                        break;
                    }

                    let fwd_price = (*fwd).price_ticks;
                    // 找到目标价格
                    if fwd_price == price {
                        return Some(&*fwd);
                    }

                    // 应该停在这一层继续下一层
                    if self.should_insert(price, fwd_price) {
                        break;
                    }

                    current = fwd;
                }
            }
        }
        None
    }

    /// 获取指定价格的可变节点引用
    #[inline]
    pub fn get_node_mut(&mut self, price: PriceTicks) -> Option<&mut SkipListNode> {
        unsafe {
            let mut current = self.head;
            // 从最高层向下遍历
            for i in (0..=self.level).rev() {
                loop {
                    let fwd = (*current).forward[i];
                    if fwd.is_null() {
                        break;
                    }

                    let fwd_price = (*fwd).price_ticks;
                    // 找到目标价格
                    if fwd_price == price {
                        return Some(&mut *fwd);
                    }

                    // 应该停在这一层继续下一层
                    if self.should_insert(price, fwd_price) {
                        break;
                    }

                    current = fwd;
                }
            }
        }
        None
    }

    /// 插入价格档位节点
    pub fn insert_level(&mut self, price: PriceTicks) -> Result<(), String> {
        // 检查是否已存在
        if self.get_node_at_price(price).is_some() {
            return Err("Price level already exists".to_string());
        }

        let new_level = Self::random_level();

        unsafe {
            // 查找所有级别的前驱节点
            let (update, _) = self.find_update(price);

            let new_ptr = if let Some(ptr) = self.free_nodes.pop() {
                *ptr = SkipListNode::new(price, new_level);
                ptr
            } else {
                let mut new_box = Box::new(SkipListNode::new(price, new_level));
                let ptr = new_box.as_mut() as *mut SkipListNode;
                self.arena.push(new_box);
                ptr
            };

            // 更新跳表级别
            if new_level > self.level {
                self.level = new_level;
            }

            // 在所有级别上插入新节点
            for i in 0..=new_level {
                (*new_ptr).forward[i] = (*update[i]).forward[i];
                (*update[i]).forward[i] = new_ptr;
            }

            self.count += 1;
        }

        Ok(())
    }

    /// 查找价格节点 - 简单包装，调用get_node_at_price
    pub fn find_node(&self, price: PriceTicks) -> Result<(), String> {
        if self.get_node_at_price(price).is_some() {
            Ok(())
        } else {
            Err("Price not found".to_string())
        }
    }

    /// 获取最优价格节点（头部）
    #[inline(always)]
    pub fn best(&self) -> Option<&SkipListNode> {
        unsafe {
            let next = (*self.head).forward[0];
            if next.is_null() {
                None
            } else {
                Some(&*next)
            }
        }
    }

    /// 获取最优且有订单的价格节点（跳过空的价格档位）
    #[inline]
    pub fn best_with_orders(&self) -> Option<&SkipListNode> {
        unsafe {
            let mut current = self.head;
            loop {
                let next = (*current).forward[0];
                if next.is_null() {
                    return None;
                }
                if !(*next).orders.is_empty() {
                    return Some(&*next);
                }
                current = next;
            }
        }
    }

    /// 向指定价格档位添加订单ID
    #[inline]
    pub fn add_order_at_level(
        &mut self,
        price: PriceTicks,
        order_id: u64,
        quantity: QuantityLots,
    ) -> Result<(), String> {
        // 先从pool中获取节点，避免后续的借用冲突
        let node_idx = self
            .list_pool
            .acquire(order_id, quantity)
            .ok_or_else(|| "List pool exhausted".to_string())?;

        // 查找目标价格节点
        unsafe {
            let mut current = self.head;
            for i in (0..=self.level).rev() {
                loop {
                    let fwd = (*current).forward[i];
                    if fwd.is_null() {
                        break;
                    }
                    let fwd_price = (*fwd).price_ticks;
                    if fwd_price == price {
                        break;
                    }
                    if self.should_insert(price, fwd_price) {
                        break;
                    }
                    current = fwd;
                }
            }

            // 检查第0层的目标节点
            let next = (*current).forward[0];
            if !next.is_null() && (*next).price_ticks == price {
                (*next).orders.push_back(node_idx, &mut self.list_pool);
                (*next).total_quantity_lots += quantity;
                return Ok(());
            }
        }

        // 如果没有找到价格节点，需要释放已获取的pool节点
        self.list_pool.release(node_idx);
        Err(format!("Price level {} not found", price))
    }

    /// 从指定价格的订单队列中移除订单
    pub fn remove_order_at_level(
        &mut self,
        price: PriceTicks,
        order_id: u64,
    ) -> Result<(), String> {
        unsafe {
            // 查找目标价格节点
            let mut current = self.head;
            for i in (0..=self.level).rev() {
                loop {
                    let fwd = (*current).forward[i];
                    if fwd.is_null() {
                        break;
                    }
                    let fwd_price = (*fwd).price_ticks;
                    if fwd_price == price {
                        break;
                    }
                    if self.should_insert(price, fwd_price) {
                        break;
                    }
                    current = fwd;
                }
            }

            // 检查第0层的目标节点
            let next = (*current).forward[0];
            if !next.is_null() && (*next).price_ticks == price {
                // 遍历链表找到order_id对应的节点
                let mut node_idx_opt = (*next).orders.front();
                while let Some(node_idx) = node_idx_opt {
                    // 先保存next指针，避免后续借用冲突
                    let next_idx = if let Some(list_node) = self.list_pool.get(node_idx) {
                        if list_node.order_id == order_id {
                            // 找到了，获取数量用于更新total_quantity_lots
                            let quantity = list_node.quantity_lots;
                            // 从链表和池中移除
                            (*next).orders.remove(node_idx, &mut self.list_pool);
                            self.list_pool.release(node_idx);
                            // 更新total_quantity_lots
                            (*next).total_quantity_lots -= quantity;
                            return Ok(());
                        }
                        list_node.next
                    } else {
                        break;
                    };
                    node_idx_opt = next_idx;
                }
                return Err(format!(
                    "Order {} not found at price level {}",
                    order_id, price
                ));
            }
        }

        Err(format!("Price level {} not found", price))
    }

    /// Reduce the visible resting quantity for an order that was partially
    /// filled but remains in the price level queue.
    pub fn reduce_order_quantity_at_level(
        &mut self,
        price: PriceTicks,
        order_id: u64,
        quantity: QuantityLots,
    ) -> Result<(), String> {
        unsafe {
            let mut current = self.head;
            for i in (0..=self.level).rev() {
                loop {
                    let fwd = (*current).forward[i];
                    if fwd.is_null() {
                        break;
                    }
                    let fwd_price = (*fwd).price_ticks;
                    if fwd_price == price {
                        break;
                    }
                    if self.should_insert(price, fwd_price) {
                        break;
                    }
                    current = fwd;
                }
            }

            let next = (*current).forward[0];
            if !next.is_null() && (*next).price_ticks == price {
                let mut node_idx_opt = (*next).orders.front();
                while let Some(node_idx) = node_idx_opt {
                    if let Some(list_node) = self.list_pool.get_mut(node_idx) {
                        if list_node.order_id == order_id {
                            list_node.quantity_lots = (list_node.quantity_lots - quantity).max(0);
                            (*next).total_quantity_lots =
                                ((*next).total_quantity_lots - quantity).max(0);
                            return Ok(());
                        }
                        node_idx_opt = list_node.next;
                    } else {
                        break;
                    }
                }
                return Err(format!(
                    "Order {} not found at price level {}",
                    order_id, price
                ));
            }
        }

        Err(format!("Price level {} not found", price))
    }

    /// 移除整个价格档位（从所有级别移除）
    pub fn remove_level(&mut self, price: PriceTicks) -> Result<(), String> {
        unsafe {
            // 查找所有级别的前驱节点
            let (update, _) = self.find_update(price);
            let target = (*update[0]).forward[0];

            // 检查目标节点是否存在
            if target.is_null() || (*target).price_ticks != price {
                return Err("Price not found".into());
            }

            let node_level = (*target).level;

            // 从所有级别移除节点
            for i in 0..=node_level {
                if (*update[i]).forward[i] == target {
                    (*update[i]).forward[i] = (*target).forward[i];
                }
            }

            // 如果需要，缩小跳表的级别
            while self.level > 0 && (*self.head).forward[self.level].is_null() {
                self.level -= 1;
            }

            self.count -= 1;
            (*target).orders.clear();
            (*target).total_quantity_lots = 0;
            (*target).forward = [std::ptr::null_mut(); MAX_LEVEL];
            self.free_nodes.push(target);
        }

        Ok(())
    }

    /// 清空跳表
    pub fn clear(&mut self) {
        unsafe {
            // 清除head的所有forward指针
            for i in 0..MAX_LEVEL {
                (*self.head).forward[i] = std::ptr::null_mut();
            }
        }
        // 仅保留head sentinel节点
        self.arena.truncate(1);
        self.free_nodes.clear();
        self.level = 0;
        self.count = 0;
        self.best_price = None;
    }

    /// 获取缓存的最优价格（如果有效）或重新计算
    pub fn get_best_price_cached(&mut self) -> Option<PriceTicks> {
        if let Some(price) = self.best_price {
            if let Some(node) = self.get_node_at_price(price) {
                if !node.orders.is_empty() {
                    return Some(price);
                }
            }
            // Cache invalid, find next best
            self.best_price = None;
        }

        // Find best price with orders
        let best = self.best_with_orders().map(|node| node.price_ticks);
        self.best_price = best;
        best
    }

    /// 使缓存失效
    pub fn invalidate_best_price_cache(&mut self) {
        self.best_price = None;
    }

    /// 获取前N个价格档位（用于生成快照）
    pub fn get_top_levels(&self, limit: usize) -> Vec<(PriceTicks, QuantityLots)> {
        let mut result = Vec::new();
        unsafe {
            let mut current = self.head;

            while result.len() < limit {
                let next = (*current).forward[0];
                if next.is_null() {
                    break;
                }
                result.push(((*next).price_ticks, (*next).total_quantity_lots));
                current = next;
            }
        }

        result
    }

    /// Fill a caller-provided buffer with top levels and return the number of
    /// entries written. This avoids heap allocation in hot snapshot paths.
    pub fn fill_top_levels(&self, out: &mut [(PriceTicks, QuantityLots)]) -> usize {
        let mut written = 0;
        unsafe {
            let mut current = self.head;
            while written < out.len() {
                let next = (*current).forward[0];
                if next.is_null() {
                    break;
                }
                out[written] = ((*next).price_ticks, (*next).total_quantity_lots);
                written += 1;
                current = next;
            }
        }
        written
    }

    /// Return true once cumulative quantity reaches `target_qty` before an
    /// unacceptable price is encountered. Used by FOK pre-checks so they do not
    /// need to allocate or stop at an arbitrary depth.
    pub fn has_cumulative_quantity_until(
        &self,
        target_qty: QuantityLots,
        limit_price: PriceTicks,
        is_market: bool,
        buy_taker: bool,
    ) -> bool {
        let mut available: QuantityLots = 0;
        unsafe {
            let mut current = self.head;
            loop {
                let next = (*current).forward[0];
                if next.is_null() {
                    return false;
                }

                let price = (*next).price_ticks;
                let acceptable = is_market
                    || if buy_taker {
                        limit_price >= price
                    } else {
                        limit_price <= price
                    };
                if !acceptable {
                    return false;
                }

                available += (*next).total_quantity_lots;
                if available >= target_qty {
                    return true;
                }
                current = next;
            }
        }
    }
}

impl Drop for SkipList {
    fn drop(&mut self) {
        // arena会自动释放所有Box<SkipListNode>
        // raw pointers会变成无效，但没有人会再访问它们
    }
}

impl OrderBook for SkipList {
    fn insert_level(&mut self, price: PriceTicks) -> Result<(), String> {
        SkipList::insert_level(self, price)
    }

    fn find_node(&self, price: PriceTicks) -> Result<(), String> {
        SkipList::find_node(self, price)
    }

    fn get_node_at_price(&self, price: PriceTicks) -> Option<&OrderBookPriceLevel> {
        // 安全的转换：SkipListNode 用 #[repr(C)]，前三个字段与 PriceLevel 完全相同
        SkipList::get_node_at_price(self, price)
            .map(|node| unsafe { &*(node as *const SkipListNode as *const OrderBookPriceLevel) })
    }

    fn get_node_mut(&mut self, price: PriceTicks) -> Option<&mut OrderBookPriceLevel> {
        SkipList::get_node_mut(self, price)
            .map(|node| unsafe { &mut *(node as *mut SkipListNode as *mut OrderBookPriceLevel) })
    }

    fn best(&self) -> Option<&OrderBookPriceLevel> {
        SkipList::best(self)
            .map(|node| unsafe { &*(node as *const SkipListNode as *const OrderBookPriceLevel) })
    }

    fn best_with_orders(&self) -> Option<&OrderBookPriceLevel> {
        SkipList::best_with_orders(self)
            .map(|node| unsafe { &*(node as *const SkipListNode as *const OrderBookPriceLevel) })
    }

    fn add_order_at_level(
        &mut self,
        price: PriceTicks,
        order_id: u64,
        quantity: QuantityLots,
    ) -> Result<(), String> {
        SkipList::add_order_at_level(self, price, order_id, quantity)
    }

    fn remove_order_at_level(&mut self, price: PriceTicks, order_id: u64) -> Result<(), String> {
        SkipList::remove_order_at_level(self, price, order_id)
    }

    fn remove_level(&mut self, price: PriceTicks) -> Result<(), String> {
        SkipList::remove_level(self, price)
    }

    fn clear(&mut self) {
        SkipList::clear(self)
    }

    fn get_top_levels(&self, limit: usize) -> Vec<(PriceTicks, QuantityLots)> {
        SkipList::get_top_levels(self, limit)
    }

    fn count(&self) -> usize {
        SkipList::count(self)
    }

    fn get_list_pool(&mut self) -> &mut crate::list_pool::ListNodePool {
        &mut self.list_pool
    }

    fn get_best_price(&mut self) -> Option<PriceTicks> {
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
        let mut sl = SkipList::new_with_pool(SortOrder::Ascending, 100);

        // 插入10个价格
        for i in 0..10 {
            let price = 100 + i;
            assert!(sl.insert_level(price).is_ok());
        }

        // 验证所有价格都能找到
        for i in 0..10 {
            let price = 100 + i;
            assert!(sl.find_node(price).is_ok(), "Should find price {}", price);
            assert!(sl.get_node_at_price(price).is_some());
        }

        // 验证不存在的价格找不到
        assert!(sl.find_node(99).is_err());
        assert!(sl.get_node_at_price(99).is_none());

        // 验证计数
        assert_eq!(sl.count(), 10);
    }

    #[test]
    fn test_insert_duplicate_rejected() {
        let mut sl = SkipList::new_with_pool(SortOrder::Ascending, 100);

        assert!(sl.insert_level(100).is_ok());
        // 尝试插入相同价格应该失败
        assert!(sl.insert_level(100).is_err());
        assert_eq!(sl.count(), 1);
    }

    #[test]
    fn test_level_distribution() {
        let mut sl = SkipList::new_with_pool(SortOrder::Ascending, 1000);

        // 插入100个节点
        for i in 0..100 {
            let price = 1000 + i;
            sl.insert_level(price).ok();
        }

        // 验证计数
        assert_eq!(sl.count(), 100);

        // 验证不是所有节点都在level 0（概率很小会全在level 0）
        // 这个测试可能偶尔失败，但概率极低
        let mut has_higher_levels = false;
        unsafe {
            let mut current = sl.head;
            for _ in 0..100 {
                let next = (*current).forward[0];
                if next.is_null() {
                    break;
                }
                if (*next).level > 0 {
                    has_higher_levels = true;
                    break;
                }
                current = next;
            }
        }
        assert!(has_higher_levels, "Should have some nodes at higher levels");
    }

    #[test]
    fn test_add_remove_orders() {
        let mut sl = SkipList::new_with_pool(SortOrder::Ascending, 100);

        // 插入价格
        assert!(sl.insert_level(100).is_ok());

        // 添加订单
        assert!(sl.add_order_at_level(100, 1, 10).is_ok());
        assert!(sl.add_order_at_level(100, 2, 20).is_ok());

        // 验证总数量
        let node = sl.get_node_at_price(100).unwrap();
        assert_eq!(node.total_quantity_lots, 30);
        assert!(!node.orders.is_empty());

        // 移除订单
        assert!(sl.remove_order_at_level(100, 1).is_ok());
        let node = sl.get_node_at_price(100).unwrap();
        assert_eq!(node.total_quantity_lots, 20);

        // 再移除一个
        assert!(sl.remove_order_at_level(100, 2).is_ok());
        let node = sl.get_node_at_price(100).unwrap();
        assert_eq!(node.total_quantity_lots, 0);
        assert!(node.orders.is_empty());

        // 尝试移除不存在的订单
        assert!(sl.remove_order_at_level(100, 999).is_err());
    }

    #[test]
    fn test_best_with_orders() {
        let mut sl = SkipList::new_with_pool(SortOrder::Ascending, 100);

        // 插入多个价格
        sl.insert_level(100).ok();
        sl.insert_level(101).ok();
        sl.insert_level(102).ok();

        // 仅在第二个价格添加订单
        sl.add_order_at_level(101, 1, 10).ok();

        // best()应该返回最低价格（100）
        assert_eq!(sl.best().unwrap().price_ticks, 100);

        // best_with_orders()应该跳过100和102，返回101
        assert_eq!(sl.best_with_orders().unwrap().price_ticks, 101);
    }

    #[test]
    fn test_clear() {
        let mut sl = SkipList::new_with_pool(SortOrder::Ascending, 100);

        // 插入数据
        for i in 0..10 {
            sl.insert_level(100 + i).ok();
        }
        assert_eq!(sl.count(), 10);

        // 清空
        sl.clear();
        assert_eq!(sl.count(), 0);
        assert!(sl.best().is_none());

        // 应该能再次插入数据
        sl.insert_level(200).ok();
        assert_eq!(sl.count(), 1);
        assert_eq!(sl.best().unwrap().price_ticks, 200);
    }

    #[test]
    fn test_remove_level() {
        let mut sl = SkipList::new_with_pool(SortOrder::Ascending, 100);

        // 插入多个价格
        sl.insert_level(100).ok();
        sl.insert_level(101).ok();
        sl.insert_level(102).ok();
        assert_eq!(sl.count(), 3);

        // 移除中间的价格
        assert!(sl.remove_level(101).is_ok());
        assert_eq!(sl.count(), 2);
        assert!(sl.find_node(101).is_err());
        assert!(sl.find_node(100).is_ok());
        assert!(sl.find_node(102).is_ok());

        // 尝试移除已删除的价格
        assert!(sl.remove_level(101).is_err());
    }

    #[test]
    fn test_removed_level_node_is_reused() {
        let mut sl = SkipList::new_with_pool(SortOrder::Ascending, 100);

        sl.insert_level(100).unwrap();
        let arena_len_after_insert = sl.arena.len();
        sl.remove_level(100).unwrap();
        assert_eq!(sl.free_nodes.len(), 1);

        sl.insert_level(101).unwrap();
        assert_eq!(sl.arena.len(), arena_len_after_insert);
        assert_eq!(sl.free_nodes.len(), 0);
        assert!(sl.get_node_at_price(101).is_some());
    }

    #[test]
    fn test_sorted_level_0_ascending() {
        let mut sl = SkipList::new_with_pool(SortOrder::Ascending, 100);

        // 以随机顺序插入价格
        let prices = vec![105, 100, 103, 102, 104, 101];
        for &price in &prices {
            sl.insert_level(price).ok();
        }

        // 验证level 0是升序的
        let mut prev = i64::MIN;
        unsafe {
            let mut current = sl.head;
            for _ in 0..prices.len() {
                let next = (*current).forward[0];
                if next.is_null() {
                    break;
                }
                let price = (*next).price_ticks;
                assert!(price > prev, "Level 0 should be sorted ascending");
                prev = price;
                current = next;
            }
        }
    }

    #[test]
    fn test_sorted_level_0_descending() {
        let mut sl = SkipList::new_with_pool(SortOrder::Descending, 100);

        // 以随机顺序插入价格
        let prices = vec![105, 100, 103, 102, 104, 101];
        for &price in &prices {
            sl.insert_level(price).ok();
        }

        // 验证level 0是降序的
        let mut prev = i64::MAX;
        unsafe {
            let mut current = sl.head;
            for _ in 0..prices.len() {
                let next = (*current).forward[0];
                if next.is_null() {
                    break;
                }
                let price = (*next).price_ticks;
                assert!(price < prev, "Level 0 should be sorted descending");
                prev = price;
                current = next;
            }
        }
    }

    #[test]
    fn test_get_top_levels() {
        let mut sl = SkipList::new_with_pool(SortOrder::Ascending, 100);

        // 插入5个价格，各有不同数量
        for i in 0..5 {
            let price = 100 + i;
            sl.insert_level(price).ok();
            for j in 0..=i {
                sl.add_order_at_level(price, j as u64, 1).ok();
            }
        }

        // 获取top 3
        let top = sl.get_top_levels(3);
        assert_eq!(top.len(), 3);
        assert_eq!(top[0].0, 100);
        assert_eq!(top[1].0, 101);
        assert_eq!(top[2].0, 102);

        // 验证数量
        assert_eq!(top[0].1, 1);
        assert_eq!(top[1].1, 2);
        assert_eq!(top[2].1, 3);
    }

    #[test]
    fn test_empty_skiplist() {
        let sl = SkipList::new_with_pool(SortOrder::Ascending, 100);

        assert!(sl.best().is_none());
        assert!(sl.best_with_orders().is_none());
        assert_eq!(sl.count(), 0);
        assert!(sl.find_node(100).is_err());
        assert!(sl.get_node_at_price(100).is_none());
        assert_eq!(sl.get_top_levels(10).len(), 0);
    }

    // ── has_cumulative_quantity_until ──────────────────────────────────────────

    fn make_ask_book(levels: &[(i64, i64)]) -> SkipList {
        let capacity = levels.len() * 4 + 10;
        let mut sl = SkipList::new_with_pool(SortOrder::Ascending, capacity);
        for (i, &(price, qty)) in levels.iter().enumerate() {
            sl.insert_level(price).unwrap();
            sl.add_order_at_level(price, (i + 1) as u64, qty).unwrap();
        }
        sl
    }

    fn make_bid_book(levels: &[(i64, i64)]) -> SkipList {
        let capacity = levels.len() * 4 + 10;
        let mut sl = SkipList::new_with_pool(SortOrder::Descending, capacity);
        for (i, &(price, qty)) in levels.iter().enumerate() {
            sl.insert_level(price).unwrap();
            sl.add_order_at_level(price, (i + 1) as u64, qty).unwrap();
        }
        sl
    }

    #[test]
    fn test_hcqu_empty_book_returns_false() {
        let sl = SkipList::new_with_pool(SortOrder::Ascending, 10);
        assert!(!sl.has_cumulative_quantity_until(1, 100, false, true));
    }

    #[test]
    fn test_hcqu_exact_quantity_returns_true() {
        // Ask book: 5 lots at price 100. Buyer wants exactly 5.
        let sl = make_ask_book(&[(100, 5)]);
        assert!(sl.has_cumulative_quantity_until(5, 100, false, true));
    }

    #[test]
    fn test_hcqu_more_than_available_returns_false() {
        let sl = make_ask_book(&[(100, 5)]);
        assert!(!sl.has_cumulative_quantity_until(6, 100, false, true));
    }

    #[test]
    fn test_hcqu_less_than_available_returns_true() {
        let sl = make_ask_book(&[(100, 10)]);
        assert!(sl.has_cumulative_quantity_until(3, 100, false, true));
    }

    #[test]
    fn test_hcqu_price_limit_excludes_expensive_levels() {
        // Asks at 100(5) and 110(5). Buyer limit=105: only 100 is acceptable.
        let sl = make_ask_book(&[(100, 5), (110, 5)]);
        // Want 8, but only 5 acceptable (100<=105, 110>105)
        assert!(!sl.has_cumulative_quantity_until(8, 105, false, true));
        // Want 5 — exactly available within price limit
        assert!(sl.has_cumulative_quantity_until(5, 105, false, true));
    }

    #[test]
    fn test_hcqu_spans_multiple_levels() {
        // Asks at 100(3), 101(3), 102(3). Buyer limit=102, wants 9.
        let sl = make_ask_book(&[(100, 3), (101, 3), (102, 3)]);
        assert!(sl.has_cumulative_quantity_until(9, 102, false, true));
        assert!(!sl.has_cumulative_quantity_until(10, 102, false, true));
    }

    #[test]
    fn test_hcqu_market_order_ignores_price_limit() {
        // Asks at 100(3), 200(3). Market buy ignores price limit.
        let sl = make_ask_book(&[(100, 3), (200, 3)]);
        // Limit of 150 would exclude 200, but is_market=true bypasses check.
        assert!(sl.has_cumulative_quantity_until(6, 150, true, true));
    }

    #[test]
    fn test_hcqu_bid_book_sell_taker() {
        // Bid book: bids at 100(5), 90(5). Sell taker, limit=95: only 100 is acceptable.
        let sl = make_bid_book(&[(100, 5), (90, 5)]);
        // Descending: head→100→90→null
        // Sell taker (buy_taker=false): acceptable when price >= limit_price=95
        // Only 100 passes (90 < 95)
        assert!(sl.has_cumulative_quantity_until(5, 95, false, false));
        assert!(!sl.has_cumulative_quantity_until(6, 95, false, false));
    }

    #[test]
    fn test_hcqu_bid_book_sell_taker_spans_levels() {
        let sl = make_bid_book(&[(100, 4), (90, 4), (80, 4)]);
        // limit=80: all three levels acceptable for a sell taker
        assert!(sl.has_cumulative_quantity_until(12, 80, false, false));
        assert!(!sl.has_cumulative_quantity_until(13, 80, false, false));
    }

    // ── reduce_order_quantity_at_level ─────────────────────────────────────────

    #[test]
    fn test_reduce_order_quantity_updates_level_total() {
        let mut sl = make_ask_book(&[(100, 10)]);
        sl.reduce_order_quantity_at_level(100, 1, 4).unwrap();

        let node = sl.get_node_at_price(100).unwrap();
        assert_eq!(node.total_quantity_lots, 6);
    }

    #[test]
    fn test_reduce_order_quantity_to_zero() {
        let mut sl = make_ask_book(&[(100, 5)]);
        sl.reduce_order_quantity_at_level(100, 1, 5).unwrap();

        let node = sl.get_node_at_price(100).unwrap();
        assert_eq!(node.total_quantity_lots, 0);
    }

    #[test]
    fn test_reduce_order_quantity_wrong_order_id_returns_err() {
        let mut sl = make_ask_book(&[(100, 5)]);
        let result = sl.reduce_order_quantity_at_level(100, 999, 3);
        assert!(result.is_err(), "unknown order id must return Err");
    }

    #[test]
    fn test_reduce_order_quantity_wrong_price_returns_err() {
        let mut sl = make_ask_book(&[(100, 5)]);
        let result = sl.reduce_order_quantity_at_level(200, 1, 3);
        assert!(result.is_err(), "wrong price level must return Err");
    }

    #[test]
    fn test_reduce_order_only_affects_target_order() {
        // Two orders at same price level: order 1 (qty=6), order 2 (qty=4)
        let capacity = 20;
        let mut sl = SkipList::new_with_pool(SortOrder::Ascending, capacity);
        sl.insert_level(100).unwrap();
        sl.add_order_at_level(100, 1, 6).unwrap();
        sl.add_order_at_level(100, 2, 4).unwrap();

        sl.reduce_order_quantity_at_level(100, 1, 3).unwrap();

        let node = sl.get_node_at_price(100).unwrap();
        // Total: (6-3) + 4 = 7
        assert_eq!(node.total_quantity_lots, 7);
    }

    // ── add_order_at_level: price not found ────────────────────────────────────

    #[test]
    fn test_add_order_at_nonexistent_level_returns_err() {
        let mut sl = SkipList::new_with_pool(SortOrder::Ascending, 20);
        let result = sl.add_order_at_level(100, 1, 5);
        assert!(result.is_err(), "adding to non-existent level must fail");
    }

    // ── best_price cache invalidation ─────────────────────────────────────────

    #[test]
    fn test_best_price_cache_returns_none_after_clear() {
        let mut sl = make_ask_book(&[(100, 5), (110, 5)]);
        assert_eq!(sl.get_best_price_cached(), Some(100));
        sl.clear();
        assert_eq!(sl.get_best_price_cached(), None);
    }

    #[test]
    fn test_best_price_cache_updated_after_remove_level() {
        let mut sl = make_ask_book(&[(100, 5), (110, 5)]);
        // Warm cache to 100
        assert_eq!(sl.get_best_price_cached(), Some(100));

        // Remove level 100; cache should invalidate and next call must return 110
        sl.remove_level(100).unwrap();
        assert_eq!(sl.get_best_price_cached(), Some(110));
    }

    #[test]
    fn test_invalidate_best_price_forces_rescan() {
        let mut sl = make_ask_book(&[(100, 5), (90, 5)]);
        // Force cache to 100 (the ascending-first might give 90 first actually)
        // In ascending order: 90 < 100, so best is 90
        let _ = sl.get_best_price_cached();
        sl.invalidate_best_price_cache();
        // After invalidation, next call rescans and finds real best
        let best = sl.get_best_price_cached();
        assert_eq!(best, Some(90), "ascending book best is the lowest price");
    }

    // ── remove_order_at_level: quantities ─────────────────────────────────────

    #[test]
    fn test_remove_order_updates_total_quantity() {
        let mut sl = make_ask_book(&[(100, 7)]);
        sl.remove_order_at_level(100, 1).unwrap();
        let node = sl.get_node_at_price(100).unwrap();
        assert_eq!(node.total_quantity_lots, 0);
    }

    #[test]
    fn test_remove_one_of_two_orders_updates_total() {
        let capacity = 20;
        let mut sl = SkipList::new_with_pool(SortOrder::Ascending, capacity);
        sl.insert_level(100).unwrap();
        sl.add_order_at_level(100, 1, 7).unwrap();
        sl.add_order_at_level(100, 2, 3).unwrap();

        sl.remove_order_at_level(100, 1).unwrap();
        let node = sl.get_node_at_price(100).unwrap();
        assert_eq!(node.total_quantity_lots, 3);
    }
}
