use crate::list_pool::{ListNodePool, PooledList};
use crate::orderbook::{OrderBook, PriceLevel as OrderBookPriceLevel};

const MAX_LEVEL: usize = 12;
const PROMOTION_PROBABILITY: f64 = 0.25;

/// 跳表节点 - 使用原始指针支持多级链接
#[repr(C)]
pub struct SkipListNode {
    pub price: f64,
    pub total_quantity: f64,
    pub orders: PooledList,
    pub level: usize,
    /// 指向各级下一个节点的原始指针数组
    /// 安全性保证：所有指针都指向arena中的节点，在SkipList的生命周期内有效
    forward: [*mut SkipListNode; MAX_LEVEL],
}

impl SkipListNode {
    /// 创建新节点 - 用于在arena中初始化
    fn new(price: f64, level: usize) -> Self {
        Self {
            price,
            total_quantity: 0.0,
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
    head: *mut SkipListNode,
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
    /// 缓存最优价格
    best_price: Option<f64>,
}

impl SkipList {
    /// 创建新跳表，指定链表节点池的容量
    pub fn new_with_pool(order: SortOrder, pool_capacity: usize) -> Self {
        let head_price = if order == SortOrder::Ascending {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
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
    unsafe fn find_update(&self, price: f64) -> ([*mut SkipListNode; MAX_LEVEL], *mut SkipListNode) {
        let mut update = [std::ptr::null_mut::<SkipListNode>(); MAX_LEVEL];
        let mut current = self.head;

        // 从最高层向下遍历
        for i in (0..=self.level).rev() {
            loop {
                let fwd = (*current).forward[i];
                if fwd.is_null() { break; }

                let fwd_price = (*fwd).price;

                // 如果目标价格已经在下一个节点，停止
                if (fwd_price - price).abs() < 1e-10 { break; }

                // 如果应该插入到下一个节点之前，停止
                if self.should_insert(price, fwd_price) { break; }

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
    fn should_insert(&self, new_price: f64, existing_price: f64) -> bool {
        match self.order {
            SortOrder::Ascending => new_price < existing_price,
            SortOrder::Descending => new_price > existing_price,
        }
    }

    /// 获取指定价格的只读节点引用
    #[inline]
    pub fn get_node_at_price(&self, price: f64) -> Option<&SkipListNode> {
        unsafe {
            let mut current = self.head;
            // 从最高层向下遍历
            for i in (0..=self.level).rev() {
                loop {
                    let fwd = (*current).forward[i];
                    if fwd.is_null() { break; }

                    let fwd_price = (*fwd).price;
                    // 找到目标价格
                    if (fwd_price - price).abs() < 1e-10 {
                        return Some(&*fwd);
                    }

                    // 应该停在这一层继续下一层
                    if self.should_insert(price, fwd_price) { break; }

                    current = fwd;
                }
            }
        }
        None
    }

    /// 获取指定价格的可变节点引用
    #[inline]
    pub fn get_node_mut(&mut self, price: f64) -> Option<&mut SkipListNode> {
        unsafe {
            let mut current = self.head;
            // 从最高层向下遍历
            for i in (0..=self.level).rev() {
                loop {
                    let fwd = (*current).forward[i];
                    if fwd.is_null() { break; }

                    let fwd_price = (*fwd).price;
                    // 找到目标价格
                    if (fwd_price - price).abs() < 1e-10 {
                        return Some(&mut *fwd);
                    }

                    // 应该停在这一层继续下一层
                    if self.should_insert(price, fwd_price) { break; }

                    current = fwd;
                }
            }
        }
        None
    }

    /// 插入价格档位节点
    pub fn insert_level(&mut self, price: f64) -> Result<(), String> {
        // 检查是否已存在
        if self.get_node_at_price(price).is_some() {
            return Err("Price level already exists".to_string());
        }

        let new_level = Self::random_level();

        unsafe {
            // 查找所有级别的前驱节点
            let (update, _) = self.find_update(price);

            // 在arena中创建新节点
            let mut new_box = Box::new(SkipListNode::new(price, new_level));
            let new_ptr = new_box.as_mut() as *mut SkipListNode;
            self.arena.push(new_box);

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
    pub fn find_node(&self, price: f64) -> Result<(), String> {
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
            if next.is_null() { None } else { Some(&*next) }
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
    pub fn add_order_at_level(&mut self, price: f64, order_id: u64, quantity: f64) -> Result<(), String> {
        // 先从pool中获取节点，避免后续的借用冲突
        let node_idx = self.list_pool.acquire(order_id, quantity)
            .ok_or_else(|| "List pool exhausted".to_string())?;

        // 查找目标价格节点
        unsafe {
            let mut current = self.head;
            for i in (0..=self.level).rev() {
                loop {
                    let fwd = (*current).forward[i];
                    if fwd.is_null() { break; }
                    let fwd_price = (*fwd).price;
                    if (fwd_price - price).abs() < 1e-10 { break; }
                    if self.should_insert(price, fwd_price) { break; }
                    current = fwd;
                }
            }

            // 检查第0层的目标节点
            let next = (*current).forward[0];
            if !next.is_null() && ((*next).price - price).abs() < 1e-10 {
                (*next).orders.push_back(node_idx, &mut self.list_pool);
                (*next).total_quantity += quantity;
                return Ok(());
            }
        }

        // 如果没有找到价格节点，需要释放已获取的pool节点
        self.list_pool.release(node_idx);
        Err(format!("Price level {} not found", price))
    }

    /// 从指定价格的订单队列中移除订单
    pub fn remove_order_at_level(&mut self, price: f64, order_id: u64) -> Result<(), String> {
        unsafe {
            // 查找目标价格节点
            let mut current = self.head;
            for i in (0..=self.level).rev() {
                loop {
                    let fwd = (*current).forward[i];
                    if fwd.is_null() { break; }
                    let fwd_price = (*fwd).price;
                    if (fwd_price - price).abs() < 1e-10 { break; }
                    if self.should_insert(price, fwd_price) { break; }
                    current = fwd;
                }
            }

            // 检查第0层的目标节点
            let next = (*current).forward[0];
            if !next.is_null() && ((*next).price - price).abs() < 1e-10 {
                // 遍历链表找到order_id对应的节点
                let mut node_idx_opt = (*next).orders.front();
                while let Some(node_idx) = node_idx_opt {
                    // 先保存next指针，避免后续借用冲突
                    let next_idx = if let Some(list_node) = self.list_pool.get(node_idx) {
                        if list_node.order_id == order_id {
                            // 找到了，获取数量用于更新total_quantity
                            let quantity = list_node.quantity;
                            // 从链表和池中移除
                            (*next).orders.remove(node_idx, &mut self.list_pool);
                            self.list_pool.release(node_idx);
                            // 更新total_quantity
                            (*next).total_quantity -= quantity;
                            return Ok(());
                        }
                        list_node.next
                    } else {
                        break;
                    };
                    node_idx_opt = next_idx;
                }
                return Err(format!("Order {} not found at price level {}", order_id, price));
            }
        }

        Err(format!("Price level {} not found", price))
    }

    /// 移除整个价格档位（从所有级别移除）
    pub fn remove_level(&mut self, price: f64) -> Result<(), String> {
        unsafe {
            // 查找所有级别的前驱节点
            let (update, _) = self.find_update(price);
            let target = (*update[0]).forward[0];

            // 检查目标节点是否存在
            if target.is_null() || ((*target).price - price).abs() >= 1e-10 {
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
            // 节点保留在arena中，当SkipList被drop时随之释放
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
        self.level = 0;
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

    /// 获取前N个价格档位（用于生成快照）
    pub fn get_top_levels(&self, limit: usize) -> Vec<(f64, f64)> {
        let mut result = Vec::new();
        unsafe {
            let mut current = self.head;

            while result.len() < limit {
                let next = (*current).forward[0];
                if next.is_null() { break; }
                result.push(((*next).price, (*next).total_quantity));
                current = next;
            }
        }

        result
    }
}

impl Drop for SkipList {
    fn drop(&mut self) {
        // arena会自动释放所有Box<SkipListNode>
        // raw pointers会变成无效，但没有人会再访问它们
    }
}

impl OrderBook for SkipList {
    fn insert_level(&mut self, price: f64) -> Result<(), String> {
        SkipList::insert_level(self, price)
    }

    fn find_node(&self, price: f64) -> Result<(), String> {
        SkipList::find_node(self, price)
    }

    fn get_node_at_price(&self, price: f64) -> Option<&OrderBookPriceLevel> {
        // 安全的转换：SkipListNode 用 #[repr(C)]，前三个字段与 PriceLevel 完全相同
        SkipList::get_node_at_price(self, price)
            .map(|node| unsafe {
                &*(node as *const SkipListNode as *const OrderBookPriceLevel)
            })
    }

    fn get_node_mut(&mut self, price: f64) -> Option<&mut OrderBookPriceLevel> {
        SkipList::get_node_mut(self, price)
            .map(|node| unsafe {
                &mut *(node as *mut SkipListNode as *mut OrderBookPriceLevel)
            })
    }

    fn best(&self) -> Option<&OrderBookPriceLevel> {
        SkipList::best(self)
            .map(|node| unsafe {
                &*(node as *const SkipListNode as *const OrderBookPriceLevel)
            })
    }

    fn best_with_orders(&self) -> Option<&OrderBookPriceLevel> {
        SkipList::best_with_orders(self)
            .map(|node| unsafe {
                &*(node as *const SkipListNode as *const OrderBookPriceLevel)
            })
    }

    fn add_order_at_level(
        &mut self,
        price: f64,
        order_id: u64,
        quantity: f64,
    ) -> Result<(), String> {
        SkipList::add_order_at_level(self, price, order_id, quantity)
    }

    fn remove_order_at_level(&mut self, price: f64, order_id: u64) -> Result<(), String> {
        SkipList::remove_order_at_level(self, price, order_id)
    }

    fn remove_level(&mut self, price: f64) -> Result<(), String> {
        SkipList::remove_level(self, price)
    }

    fn clear(&mut self) {
        SkipList::clear(self)
    }

    fn get_top_levels(&self, limit: usize) -> Vec<(f64, f64)> {
        SkipList::get_top_levels(self, limit)
    }

    fn count(&self) -> usize {
        SkipList::count(self)
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
        let mut sl = SkipList::new_with_pool(SortOrder::Ascending, 100);

        // 插入10个价格
        for i in 0..10 {
            let price = 100.0 + i as f64;
            assert!(sl.insert_level(price).is_ok());
        }

        // 验证所有价格都能找到
        for i in 0..10 {
            let price = 100.0 + i as f64;
            assert!(sl.find_node(price).is_ok(), "Should find price {}", price);
            assert!(sl.get_node_at_price(price).is_some());
        }

        // 验证不存在的价格找不到
        assert!(sl.find_node(99.0).is_err());
        assert!(sl.get_node_at_price(99.0).is_none());

        // 验证计数
        assert_eq!(sl.count(), 10);
    }

    #[test]
    fn test_insert_duplicate_rejected() {
        let mut sl = SkipList::new_with_pool(SortOrder::Ascending, 100);

        assert!(sl.insert_level(100.0).is_ok());
        // 尝试插入相同价格应该失败
        assert!(sl.insert_level(100.0).is_err());
        assert_eq!(sl.count(), 1);
    }

    #[test]
    fn test_level_distribution() {
        let mut sl = SkipList::new_with_pool(SortOrder::Ascending, 1000);

        // 插入100个节点
        for i in 0..100 {
            let price = 1000.0 + i as f64;
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
                if next.is_null() { break; }
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
        assert!(sl.insert_level(100.0).is_ok());

        // 添加订单
        assert!(sl.add_order_at_level(100.0, 1, 10.0).is_ok());
        assert!(sl.add_order_at_level(100.0, 2, 20.0).is_ok());

        // 验证总数量
        let node = sl.get_node_at_price(100.0).unwrap();
        assert!((node.total_quantity - 30.0).abs() < 1e-10);
        assert!(!node.orders.is_empty());

        // 移除订单
        assert!(sl.remove_order_at_level(100.0, 1).is_ok());
        let node = sl.get_node_at_price(100.0).unwrap();
        assert!((node.total_quantity - 20.0).abs() < 1e-10);

        // 再移除一个
        assert!(sl.remove_order_at_level(100.0, 2).is_ok());
        let node = sl.get_node_at_price(100.0).unwrap();
        assert!((node.total_quantity - 0.0).abs() < 1e-10);
        assert!(node.orders.is_empty());

        // 尝试移除不存在的订单
        assert!(sl.remove_order_at_level(100.0, 999).is_err());
    }

    #[test]
    fn test_best_with_orders() {
        let mut sl = SkipList::new_with_pool(SortOrder::Ascending, 100);

        // 插入多个价格
        sl.insert_level(100.0).ok();
        sl.insert_level(101.0).ok();
        sl.insert_level(102.0).ok();

        // 仅在第二个价格添加订单
        sl.add_order_at_level(101.0, 1, 10.0).ok();

        // best()应该返回最低价格（100.0）
        assert_eq!(sl.best().unwrap().price, 100.0);

        // best_with_orders()应该跳过100.0和102.0，返回101.0
        assert_eq!(sl.best_with_orders().unwrap().price, 101.0);
    }

    #[test]
    fn test_clear() {
        let mut sl = SkipList::new_with_pool(SortOrder::Ascending, 100);

        // 插入数据
        for i in 0..10 {
            sl.insert_level(100.0 + i as f64).ok();
        }
        assert_eq!(sl.count(), 10);

        // 清空
        sl.clear();
        assert_eq!(sl.count(), 0);
        assert!(sl.best().is_none());

        // 应该能再次插入数据
        sl.insert_level(200.0).ok();
        assert_eq!(sl.count(), 1);
        assert_eq!(sl.best().unwrap().price, 200.0);
    }

    #[test]
    fn test_remove_level() {
        let mut sl = SkipList::new_with_pool(SortOrder::Ascending, 100);

        // 插入多个价格
        sl.insert_level(100.0).ok();
        sl.insert_level(101.0).ok();
        sl.insert_level(102.0).ok();
        assert_eq!(sl.count(), 3);

        // 移除中间的价格
        assert!(sl.remove_level(101.0).is_ok());
        assert_eq!(sl.count(), 2);
        assert!(sl.find_node(101.0).is_err());
        assert!(sl.find_node(100.0).is_ok());
        assert!(sl.find_node(102.0).is_ok());

        // 尝试移除已删除的价格
        assert!(sl.remove_level(101.0).is_err());
    }

    #[test]
    fn test_sorted_level_0_ascending() {
        let mut sl = SkipList::new_with_pool(SortOrder::Ascending, 100);

        // 以随机顺序插入价格
        let prices = vec![105.5, 100.2, 103.7, 102.1, 104.9, 101.3];
        for &price in &prices {
            sl.insert_level(price).ok();
        }

        // 验证level 0是升序的
        let mut prev = f64::NEG_INFINITY;
        unsafe {
            let mut current = sl.head;
            for _ in 0..prices.len() {
                let next = (*current).forward[0];
                if next.is_null() { break; }
                let price = (*next).price;
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
        let prices = vec![105.5, 100.2, 103.7, 102.1, 104.9, 101.3];
        for &price in &prices {
            sl.insert_level(price).ok();
        }

        // 验证level 0是降序的
        let mut prev = f64::INFINITY;
        unsafe {
            let mut current = sl.head;
            for _ in 0..prices.len() {
                let next = (*current).forward[0];
                if next.is_null() { break; }
                let price = (*next).price;
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
            let price = 100.0 + i as f64;
            sl.insert_level(price).ok();
            for j in 0..=i {
                sl.add_order_at_level(price, j as u64, 1.0).ok();
            }
        }

        // 获取top 3
        let top = sl.get_top_levels(3);
        assert_eq!(top.len(), 3);
        assert_eq!(top[0].0, 100.0);
        assert_eq!(top[1].0, 101.0);
        assert_eq!(top[2].0, 102.0);

        // 验证数量
        assert_eq!(top[0].1, 1.0);
        assert_eq!(top[1].1, 2.0);
        assert_eq!(top[2].1, 3.0);
    }

    #[test]
    fn test_empty_skiplist() {
        let sl = SkipList::new_with_pool(SortOrder::Ascending, 100);

        assert!(sl.best().is_none());
        assert!(sl.best_with_orders().is_none());
        assert_eq!(sl.count(), 0);
        assert!(sl.find_node(100.0).is_err());
        assert!(sl.get_node_at_price(100.0).is_none());
        assert_eq!(sl.get_top_levels(10).len(), 0);
    }
}
