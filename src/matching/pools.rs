use crate::order::Order;
use crate::trade::Trade;
use std::collections::VecDeque;

/// 通用对象池
pub struct ObjectPool<T: Default> {
    objects: Vec<T>,
    free_indices: Vec<usize>,
    capacity: usize,
    allocated: usize,
}

impl<T: Default> ObjectPool<T> {
    /// 创建新对象池
    pub fn new(capacity: usize) -> Self {
        let mut objects = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            objects.push(T::default());
        }

        let free_indices = (0..capacity).rev().collect();

        Self {
            objects,
            free_indices,
            capacity,
            allocated: 0,
        }
    }

    /// 从池中获取对象，返回索引
    #[inline(always)]
    pub fn acquire(&mut self) -> usize {
        if self.free_indices.is_empty() {
            self.grow();
        }
        let idx = self.free_indices.pop().unwrap();
        self.allocated += 1;
        idx
    }

    #[cold]
    fn grow(&mut self) {
        let old_capacity = self.capacity;
        let new_capacity = old_capacity.max(1).saturating_mul(2);
        assert!(new_capacity > old_capacity, "ObjectPool capacity overflow");
        self.objects.resize_with(new_capacity, T::default);
        self.free_indices.extend((old_capacity..new_capacity).rev());
        self.capacity = new_capacity;
    }

    /// 将对象返还到池中
    #[inline(always)]
    pub fn release(&mut self, index: usize) {
        self.free_indices.push(index);
        self.allocated -= 1;
    }

    /// 获取对象引用
    #[inline(always)]
    pub fn get(&self, index: usize) -> Option<&T> {
        self.objects.get(index)
    }

    /// 获取对象可变引用
    #[inline(always)]
    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        self.objects.get_mut(index)
    }

    /// 获取当前分配数量
    #[inline(always)]
    pub fn allocated_count(&self) -> usize {
        self.allocated
    }

    /// 获取可用数量
    #[inline(always)]
    pub fn available_count(&self) -> usize {
        self.capacity - self.allocated
    }

    /// 清空池（所有对象返还）
    pub fn clear(&mut self) {
        self.free_indices.clear();
        self.free_indices = (0..self.capacity).rev().collect();
        self.allocated = 0;
    }
}

/// 所有对象池的容器
pub struct Pools {
    pub orders: ObjectPool<Order>,
    pub trades: ObjectPool<Trade>,
    pub queues: ObjectPool<VecDeque<u64>>,
}

impl Pools {
    /// 创建新的对象池容器
    pub fn new(order_capacity: usize, queue_capacity: usize) -> Self {
        Self {
            orders: ObjectPool::new(order_capacity),
            trades: ObjectPool::new(order_capacity), // Trade容量 = Order容量（保守估计）
            queues: ObjectPool::new(queue_capacity),
        }
    }

    /// 获取统计信息
    pub fn stats(&self) -> PoolStats {
        PoolStats {
            orders_allocated: self.orders.allocated_count(),
            orders_capacity: self.orders.capacity,
            trades_allocated: self.trades.allocated_count(),
            trades_capacity: self.trades.capacity,
            queues_allocated: self.queues.allocated_count(),
            queues_capacity: self.queues.capacity,
        }
    }
}

/// 对象池统计信息
#[derive(Debug, Clone)]
pub struct PoolStats {
    pub orders_allocated: usize,
    pub orders_capacity: usize,
    pub trades_allocated: usize,
    pub trades_capacity: usize,
    pub queues_allocated: usize,
    pub queues_capacity: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grows_instead_of_returning_pool_exhaustion() {
        let mut pool = ObjectPool::<u64>::new(1);
        let first = pool.acquire();
        let second = pool.acquire();

        assert_ne!(first, second);
        assert_eq!(pool.capacity, 2);
        assert_eq!(pool.allocated_count(), 2);
    }

    #[test]
    fn zero_capacity_pool_grows_and_preserves_indices() {
        let mut pool = ObjectPool::<u64>::new(0);
        let first = pool.acquire();
        *pool.get_mut(first).unwrap() = 42;
        let second = pool.acquire();

        assert_eq!(pool.get(first), Some(&42));
        assert_ne!(first, second);
        assert!(pool.capacity >= 2);
    }
}
