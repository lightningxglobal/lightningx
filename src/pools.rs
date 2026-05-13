use crate::order::Order;
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
    pub fn acquire(&mut self) -> Option<usize> {
        self.free_indices.pop().map(|idx| {
            self.allocated += 1;
            idx
        })
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
    pub queues: ObjectPool<VecDeque<u64>>,
}

impl Pools {
    /// 创建新的对象池容器
    pub fn new(order_capacity: usize, queue_capacity: usize) -> Self {
        Self {
            orders: ObjectPool::new(order_capacity),
            queues: ObjectPool::new(queue_capacity),
        }
    }

    /// 检查是否有足够的资源
    pub fn has_space_for_order(&self) -> bool {
        self.orders.available_count() > 0 && self.queues.available_count() > 0
    }

    /// 获取统计信息
    pub fn stats(&self) -> PoolStats {
        PoolStats {
            orders_allocated: self.orders.allocated_count(),
            orders_capacity: self.orders.capacity,
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
    pub queues_allocated: usize,
    pub queues_capacity: usize,
}
