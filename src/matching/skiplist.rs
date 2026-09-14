use crate::list_pool::{ListNodePool, PooledList};
use crate::order::{PriceTicks, QuantityLots};
use crate::orderbook::{OrderBook, PriceLevel as OrderBookPriceLevel};

const MAX_LEVEL: usize = 12;
type NodeIndex = u32;
const NONE: NodeIndex = NodeIndex::MAX;
// Level promotion probability is 1/4, implemented as a 2-bit draw in
// random_level (deterministic xorshift64*).

/// 跳表节点 - 使用稳定的 arena 索引支持多级链接
#[repr(C, align(64))]
pub struct SkipListNode {
    pub price_ticks: PriceTicks,
    pub total_quantity_lots: QuantityLots,
    pub orders: PooledList,
    pub level: usize,
    /// 各级下一个节点在 arena 中的索引
    pub forward: [NodeIndex; MAX_LEVEL],
}

impl SkipListNode {
    /// 创建新节点 - 用于在arena中初始化
    fn new(price_ticks: PriceTicks, level: usize) -> Self {
        Self {
            price_ticks,
            total_quantity_lots: 0,
            orders: PooledList::new(),
            level,
            forward: [NONE; MAX_LEVEL],
        }
    }
}

/// 跳表排序方向
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortOrder {
    Ascending,  // 升序（最小值在头部）
    Descending, // 降序（最大值在头部）
}

/// 跳表实现 - 节点和链接都使用稳定的 arena 索引
pub struct SkipList {
    /// head sentinel 在 arena 中的索引
    pub head: NodeIndex,
    /// 当前跳表的最大层级
    level: usize,
    /// 节点总数（不含head sentinel）
    count: usize,
    /// 排序方向
    order: SortOrder,
    /// 订单链表节点池
    pub list_pool: ListNodePool,
    /// Arena - 所有 SkipListNode 的唯一所有者
    arena: Vec<SkipListNode>,
    /// Unlinked price-level nodes available for reuse.
    free_nodes: Vec<NodeIndex>,
    /// Deterministic xorshift64* state for level promotion. Seeded with a
    /// fixed constant so replaying the same insert sequence reproduces the
    /// identical internal structure — a prerequisite for bit-exact
    /// primary/standby replay verification. Promotion quality only affects
    /// performance, never matching results.
    rng_state: u64,
    /// 缓存最优价格
    best_price: Option<PriceTicks>,
}

impl SkipList {
    #[inline(always)]
    fn node(&self, index: NodeIndex) -> &SkipListNode {
        debug_assert!((index as usize) < self.arena.len());
        // SAFETY: head, forward links, and free-list entries are created only
        // from valid arena indices. Nodes are never removed from the middle.
        unsafe { self.arena.get_unchecked(index as usize) }
    }

    #[inline(always)]
    fn node_mut(&mut self, index: NodeIndex) -> &mut SkipListNode {
        debug_assert!((index as usize) < self.arena.len());
        // SAFETY: same index invariant as node(); mutable access is scoped to
        // one node at a time and no arena growth occurs while it is borrowed.
        unsafe { self.arena.get_unchecked_mut(index as usize) }
    }

    /// 创建新跳表，指定链表节点池的容量
    pub fn new_with_pool(order: SortOrder, pool_capacity: usize) -> Self {
        let head_price = if order == SortOrder::Ascending {
            PriceTicks::MIN
        } else {
            PriceTicks::MAX
        };

        let arena_capacity = pool_capacity.clamp(1_000, 16_384).saturating_add(1);
        let mut arena = Vec::with_capacity(arena_capacity);
        arena.push(SkipListNode::new(head_price, MAX_LEVEL - 1));

        Self {
            head: 0,
            level: 0,
            count: 0,
            order,
            rng_state: 0x9E37_79B9_7F4A_7C15,
            list_pool: ListNodePool::new(pool_capacity),
            arena,
            free_nodes: Vec::new(),
            best_price: None,
        }
    }

    /// 生成随机层级
    /// Deterministic level draw (replaces rand::random): xorshift64*,
    /// consuming 2 bits per level for the p = 0.25 promotion probability.
    fn random_level(&mut self) -> usize {
        // xorshift64* — fast, never zero-locked with a non-zero seed.
        let mut x = self.rng_state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.rng_state = x;
        let mut bits = x.wrapping_mul(0x2545_F491_4F6C_DD1D);

        let mut lv = 0;
        // promote while the next two bits are 00 (probability 1/4).
        while lv < MAX_LEVEL - 1 && bits & 0b11 == 0 {
            lv += 1;
            bits >>= 2;
        }
        lv
    }

    /// find_update辅助函数 - 返回所有级别的前驱节点索引
    fn find_update(&self, price: PriceTicks) -> [NodeIndex; MAX_LEVEL] {
        let mut update = [0; MAX_LEVEL];
        let mut current = self.head;

        // 从最高层向下遍历
        for i in (0..=self.level).rev() {
            loop {
                let fwd = self.node(current).forward[i];
                if fwd == NONE {
                    break;
                }

                let fwd_price = self.node(fwd).price_ticks;

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

        update
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
        let update = self.find_update(price);
        let index = self.node(update[0]).forward[0];
        (index != NONE && self.node(index).price_ticks == price).then(|| self.node(index))
    }

    /// 获取指定价格的可变节点引用
    #[inline]
    pub fn get_node_mut(&mut self, price: PriceTicks) -> Option<&mut SkipListNode> {
        let update = self.find_update(price);
        let index = self.node(update[0]).forward[0];
        (index != NONE && self.node(index).price_ticks == price).then(|| self.node_mut(index))
    }

    /// 插入价格档位节点
    pub fn insert_level(&mut self, price: PriceTicks) -> Result<(), String> {
        let (_, inserted) = self.get_or_insert_level(price);
        if !inserted {
            return Err("Price level already exists".to_string());
        }

        Ok(())
    }

    /// 一次查找返回已有档位，或在同一次查找结果上插入新档位。
    fn get_or_insert_level(&mut self, price: PriceTicks) -> (NodeIndex, bool) {
        let update = self.find_update(price);
        let existing = self.node(update[0]).forward[0];
        if existing != NONE && self.node(existing).price_ticks == price {
            return (existing, false);
        }

        let new_level = self.random_level();
        let new_index = if let Some(index) = self.free_nodes.pop() {
            self.arena[index as usize] = SkipListNode::new(price, new_level);
            index
        } else {
            let index = NodeIndex::try_from(self.arena.len())
                .expect("SkipList price-level arena exceeds u32 index space");
            self.arena.push(SkipListNode::new(price, new_level));
            index
        };

        for (level, predecessor) in update.iter().copied().enumerate().take(new_level + 1) {
            let successor = self.node(predecessor).forward[level];
            self.node_mut(new_index).forward[level] = successor;
            self.node_mut(predecessor).forward[level] = new_index;
        }

        self.level = self.level.max(new_level);
        self.count += 1;
        (new_index, true)
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
        let next = self.node(self.head).forward[0];
        (next != NONE).then(|| self.node(next))
    }

    /// 获取最优且有订单的价格节点（跳过空的价格档位）
    #[inline]
    pub fn best_with_orders(&self) -> Option<&SkipListNode> {
        let mut next = self.node(self.head).forward[0];
        while next != NONE {
            if !self.node(next).orders.is_empty() {
                return Some(self.node(next));
            }
            next = self.node(next).forward[0];
        }
        None
    }

    /// 向指定价格档位添加订单ID
    #[inline]
    pub fn add_order_at_level(
        &mut self,
        price: PriceTicks,
        order_id: u64,
        quantity: QuantityLots,
    ) -> Result<usize, String> {
        let node_idx = self.list_pool.acquire(order_id, quantity);

        let (level_index, _) = self.get_or_insert_level(price);
        debug_assert!((level_index as usize) < self.arena.len());
        // SAFETY: get_or_insert_level returns a valid arena index.
        let level = unsafe { self.arena.get_unchecked_mut(level_index as usize) };
        level.orders.push_back(node_idx, &mut self.list_pool);
        level.total_quantity_lots += quantity;
        Ok(node_idx)
    }

    /// 从指定价格的订单队列中移除订单
    pub fn remove_order_at_level(
        &mut self,
        price: PriceTicks,
        order_id: u64,
        node_idx: usize,
    ) -> Result<(), String> {
        self.remove_order(price, order_id, node_idx, false)
    }

    pub(crate) fn remove_order_and_empty_level(
        &mut self,
        price: PriceTicks,
        order_id: u64,
        node_idx: usize,
    ) -> Result<(), String> {
        self.remove_order(price, order_id, node_idx, true)
    }

    fn remove_order(
        &mut self,
        price: PriceTicks,
        order_id: u64,
        node_idx: usize,
        remove_empty_level: bool,
    ) -> Result<(), String> {
        let quantity = self
            .list_pool
            .get(node_idx)
            .filter(|node| node.order_id == order_id)
            .map(|node| node.quantity_lots)
            .ok_or_else(|| format!("Order {} not found at price level {}", order_id, price))?;
        let update = self.find_update(price);
        let level_index = self.node(update[0]).forward[0];
        if level_index == NONE || self.node(level_index).price_ticks != price {
            return Err(format!("Price level {} not found", price));
        }
        debug_assert!((level_index as usize) < self.arena.len());
        // SAFETY: find_update returned a valid, matching arena index.
        let level = unsafe { self.arena.get_unchecked_mut(level_index as usize) };
        level.orders.remove(node_idx, &mut self.list_pool);
        level.total_quantity_lots -= quantity;
        let level_empty = level.orders.is_empty();
        self.list_pool.release(node_idx);
        if remove_empty_level && level_empty {
            self.unlink_level(&update, level_index);
        }
        Ok(())
    }

    fn unlink_level(&mut self, update: &[NodeIndex; MAX_LEVEL], target: NodeIndex) {
        let node_level = self.node(target).level;
        for (level, predecessor) in update.iter().copied().enumerate().take(node_level + 1) {
            if self.node(predecessor).forward[level] == target {
                let successor = self.node(target).forward[level];
                self.node_mut(predecessor).forward[level] = successor;
            }
        }

        while self.level > 0 && self.node(self.head).forward[self.level] == NONE {
            self.level -= 1;
        }

        self.count -= 1;
        self.node_mut(target).orders.clear();
        self.node_mut(target).total_quantity_lots = 0;
        self.node_mut(target).forward = [NONE; MAX_LEVEL];
        self.free_nodes.push(target);
        self.best_price = None;
    }

    /// Reduce the visible resting quantity for an order that was partially
    /// filled but remains in the price level queue.
    pub fn reduce_order_quantity_at_level(
        &mut self,
        price: PriceTicks,
        order_id: u64,
        node_idx: usize,
        quantity: QuantityLots,
    ) -> Result<(), String> {
        let list_node = self
            .list_pool
            .get_mut(node_idx)
            .filter(|node| node.order_id == order_id)
            .ok_or_else(|| format!("Order {} not found at price level {}", order_id, price))?;
        list_node.quantity_lots = (list_node.quantity_lots - quantity).max(0);
        let level = self
            .get_node_mut(price)
            .ok_or_else(|| format!("Price level {} not found", price))?;
        level.total_quantity_lots = (level.total_quantity_lots - quantity).max(0);
        Ok(())
    }

    /// 移除整个价格档位（从所有级别移除）
    pub fn remove_level(&mut self, price: PriceTicks) -> Result<(), String> {
        let update = self.find_update(price);
        let target = self.node(update[0]).forward[0];
        if target == NONE || self.node(target).price_ticks != price {
            return Err("Price not found".into());
        }

        let mut order_idx = self.node(target).orders.front();
        while let Some(idx) = order_idx {
            order_idx = self.list_pool.get(idx).and_then(|node| node.next);
            self.list_pool.release(idx);
        }
        self.unlink_level(&update, target);

        Ok(())
    }

    /// 清空跳表
    pub fn clear(&mut self) {
        self.node_mut(self.head).forward = [NONE; MAX_LEVEL];
        self.arena.truncate(1);
        self.free_nodes.clear();
        self.list_pool.clear();
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
        let mut next = self.node(self.head).forward[0];
        while result.len() < limit && next != NONE {
            let node = self.node(next);
            result.push((node.price_ticks, node.total_quantity_lots));
            next = node.forward[0];
        }

        result
    }

    /// Fill a caller-provided buffer with top levels and return the number of
    /// entries written. This avoids heap allocation in hot snapshot paths.
    pub fn fill_top_levels(&self, out: &mut [(PriceTicks, QuantityLots)]) -> usize {
        let mut written = 0;
        let mut next = self.node(self.head).forward[0];
        while written < out.len() && next != NONE {
            let node = self.node(next);
            out[written] = (node.price_ticks, node.total_quantity_lots);
            written += 1;
            next = node.forward[0];
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
        let mut next = self.node(self.head).forward[0];
        while next != NONE {
            let node = self.node(next);
            let acceptable = is_market
                || if buy_taker {
                    limit_price >= node.price_ticks
                } else {
                    limit_price <= node.price_ticks
                };
            if !acceptable {
                return false;
            }

            available += node.total_quantity_lots;
            if available >= target_qty {
                return true;
            }
            next = node.forward[0];
        }
        false
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
    ) -> Result<usize, String> {
        SkipList::add_order_at_level(self, price, order_id, quantity)
    }

    fn remove_order_at_level(
        &mut self,
        price: PriceTicks,
        order_id: u64,
        list_node_idx: usize,
    ) -> Result<(), String> {
        SkipList::remove_order_at_level(self, price, order_id, list_node_idx)
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
    fn identical_insert_sequences_build_identical_structure() {
        // Replay determinism: same inputs → bit-identical internal shape
        // (node levels included), across two independent instances.
        let mut a = SkipList::new_with_pool(SortOrder::Ascending, 4096);
        let mut b = SkipList::new_with_pool(SortOrder::Ascending, 4096);
        // Insert/remove pattern with enough churn to exercise node reuse.
        for i in 0..500i64 {
            let price = (i * 37) % 251 + 1;
            let _ = a.insert_level(price);
            let _ = b.insert_level(price);
            if i % 7 == 0 {
                let victim = (i * 11) % 251 + 1;
                let _ = a.remove_level(victim);
                let _ = b.remove_level(victim);
            }
        }
        assert_eq!(a.count(), b.count());
        for price in 1..=251i64 {
            match (a.get_node_at_price(price), b.get_node_at_price(price)) {
                (Some(na), Some(nb)) => {
                    assert_eq!(na.level, nb.level, "node level diverged at {price}");
                }
                (None, None) => {}
                _ => panic!("presence diverged at {price}"),
            }
        }
    }

    #[test]
    fn random_level_distribution_is_sane() {
        // The deterministic draw must still look like p=0.25 promotion:
        // roughly 3/4 of draws are level 0 and levels stay within bounds.
        let mut sl = SkipList::new_with_pool(SortOrder::Ascending, 16);
        let mut histogram = [0usize; MAX_LEVEL];
        for _ in 0..40_000 {
            let lv = sl.random_level();
            assert!(lv < MAX_LEVEL);
            histogram[lv] += 1;
        }
        let level0_share = histogram[0] as f64 / 40_000.0;
        assert!(
            (0.72..=0.78).contains(&level0_share),
            "level-0 share {level0_share} out of expected band"
        );
        assert!(histogram[1] > histogram[2], "monotone decay expected");
    }

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
        let mut next = sl.node(sl.head).forward[0];
        for _ in 0..100 {
            if next == NONE {
                break;
            }
            if sl.node(next).level > 0 {
                has_higher_levels = true;
                break;
            }
            next = sl.node(next).forward[0];
        }
        assert!(has_higher_levels, "Should have some nodes at higher levels");
    }

    #[test]
    fn test_add_remove_orders() {
        let mut sl = SkipList::new_with_pool(SortOrder::Ascending, 100);

        // 插入价格
        assert!(sl.insert_level(100).is_ok());

        // 添加订单
        let first_idx = sl.add_order_at_level(100, 1, 10).unwrap();
        let second_idx = sl.add_order_at_level(100, 2, 20).unwrap();

        // 验证总数量
        let node = sl.get_node_at_price(100).unwrap();
        assert_eq!(node.total_quantity_lots, 30);
        assert!(!node.orders.is_empty());

        // 移除订单
        assert!(sl.remove_order_at_level(100, 1, first_idx).is_ok());
        let node = sl.get_node_at_price(100).unwrap();
        assert_eq!(node.total_quantity_lots, 20);

        // 再移除一个
        assert!(sl.remove_order_at_level(100, 2, second_idx).is_ok());
        let node = sl.get_node_at_price(100).unwrap();
        assert_eq!(node.total_quantity_lots, 0);
        assert!(node.orders.is_empty());

        // 尝试移除不存在的订单
        assert!(sl.remove_order_at_level(100, 999, first_idx).is_err());
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
    fn clear_releases_all_list_nodes() {
        let mut sl = SkipList::new_with_pool(SortOrder::Ascending, 10);
        sl.add_order_at_level(100, 1, 5).unwrap();
        sl.add_order_at_level(101, 2, 7).unwrap();
        assert_eq!(sl.list_pool.allocated_count(), 2);

        sl.clear();

        assert_eq!(sl.list_pool.allocated_count(), 0);
        assert_eq!(sl.list_pool.available_count(), 10);
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
    fn remove_nonempty_level_releases_its_list_nodes() {
        let mut sl = SkipList::new_with_pool(SortOrder::Ascending, 10);
        sl.add_order_at_level(100, 1, 5).unwrap();
        sl.add_order_at_level(100, 2, 7).unwrap();
        assert_eq!(sl.list_pool.allocated_count(), 2);

        sl.remove_level(100).unwrap();

        assert_eq!(sl.list_pool.allocated_count(), 0);
        assert_eq!(sl.list_pool.available_count(), 10);
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
    fn arena_growth_keeps_index_links_valid() {
        let mut sl = SkipList::new_with_pool(SortOrder::Ascending, 10_000);
        for price in (0..10_000).rev() {
            sl.add_order_at_level(price, price as u64 + 1, 1).unwrap();
        }

        assert_eq!(sl.count(), 10_000);
        assert_eq!(sl.best_with_orders().unwrap().price_ticks, 0);
        assert_eq!(sl.get_node_at_price(9_999).unwrap().total_quantity_lots, 1);
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
        let mut next = sl.node(sl.head).forward[0];
        for _ in 0..prices.len() {
            if next == NONE {
                break;
            }
            let price = sl.node(next).price_ticks;
            assert!(price > prev, "Level 0 should be sorted ascending");
            prev = price;
            next = sl.node(next).forward[0];
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
        let mut next = sl.node(sl.head).forward[0];
        for _ in 0..prices.len() {
            if next == NONE {
                break;
            }
            let price = sl.node(next).price_ticks;
            assert!(price < prev, "Level 0 should be sorted descending");
            prev = price;
            next = sl.node(next).forward[0];
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

    fn first_order_idx(sl: &SkipList, price: PriceTicks) -> usize {
        sl.get_node_at_price(price)
            .and_then(|level| level.orders.front())
            .unwrap()
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
        let node_idx = first_order_idx(&sl, 100);
        sl.reduce_order_quantity_at_level(100, 1, node_idx, 4)
            .unwrap();

        let node = sl.get_node_at_price(100).unwrap();
        assert_eq!(node.total_quantity_lots, 6);
    }

    #[test]
    fn test_reduce_order_quantity_to_zero() {
        let mut sl = make_ask_book(&[(100, 5)]);
        let node_idx = first_order_idx(&sl, 100);
        sl.reduce_order_quantity_at_level(100, 1, node_idx, 5)
            .unwrap();

        let node = sl.get_node_at_price(100).unwrap();
        assert_eq!(node.total_quantity_lots, 0);
    }

    #[test]
    fn test_reduce_order_quantity_wrong_order_id_returns_err() {
        let mut sl = make_ask_book(&[(100, 5)]);
        let node_idx = first_order_idx(&sl, 100);
        let result = sl.reduce_order_quantity_at_level(100, 999, node_idx, 3);
        assert!(result.is_err(), "unknown order id must return Err");
    }

    #[test]
    fn test_reduce_order_quantity_wrong_price_returns_err() {
        let mut sl = make_ask_book(&[(100, 5)]);
        let node_idx = first_order_idx(&sl, 100);
        let result = sl.reduce_order_quantity_at_level(200, 1, node_idx, 3);
        assert!(result.is_err(), "wrong price level must return Err");
    }

    #[test]
    fn test_reduce_order_only_affects_target_order() {
        // Two orders at same price level: order 1 (qty=6), order 2 (qty=4)
        let capacity = 20;
        let mut sl = SkipList::new_with_pool(SortOrder::Ascending, capacity);
        sl.insert_level(100).unwrap();
        let first_idx = sl.add_order_at_level(100, 1, 6).unwrap();
        sl.add_order_at_level(100, 2, 4).unwrap();

        sl.reduce_order_quantity_at_level(100, 1, first_idx, 3)
            .unwrap();

        let node = sl.get_node_at_price(100).unwrap();
        // Total: (6-3) + 4 = 7
        assert_eq!(node.total_quantity_lots, 7);
    }

    // ── add_order_at_level: price not found ────────────────────────────────────

    #[test]
    fn test_add_order_at_nonexistent_level_creates_level() {
        let mut sl = SkipList::new_with_pool(SortOrder::Ascending, 20);
        let result = sl.add_order_at_level(100, 1, 5);
        assert!(result.is_ok());
        assert_eq!(sl.count(), 1);
        assert_eq!(sl.get_node_at_price(100).unwrap().total_quantity_lots, 5);
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
        let node_idx = first_order_idx(&sl, 100);
        sl.remove_order_at_level(100, 1, node_idx).unwrap();
        let node = sl.get_node_at_price(100).unwrap();
        assert_eq!(node.total_quantity_lots, 0);
    }

    #[test]
    fn test_remove_one_of_two_orders_updates_total() {
        let capacity = 20;
        let mut sl = SkipList::new_with_pool(SortOrder::Ascending, capacity);
        sl.insert_level(100).unwrap();
        let first_idx = sl.add_order_at_level(100, 1, 7).unwrap();
        sl.add_order_at_level(100, 2, 3).unwrap();

        sl.remove_order_at_level(100, 1, first_idx).unwrap();
        let node = sl.get_node_at_price(100).unwrap();
        assert_eq!(node.total_quantity_lots, 3);
    }
}
