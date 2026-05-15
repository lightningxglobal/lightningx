/// 链表节点对象池实现
/// 用于存储同价位的订单队列，相比VecDeque支持O(1)的真正删除

/// 链表节点
#[derive(Debug, Clone, Copy)]
pub struct ListNode {
    pub order_id: u64,
    pub quantity: f64,
    pub next: Option<usize>,
    pub prev: Option<usize>,
}

impl Default for ListNode {
    fn default() -> Self {
        Self {
            order_id: 0,
            quantity: 0.0,
            next: None,
            prev: None,
        }
    }
}

/// 链表节点对象池
pub struct ListNodePool {
    nodes: Vec<ListNode>,
    free_indices: Vec<usize>,
    allocated: usize,
    capacity: usize,
}

impl ListNodePool {
    /// 创建新的节点池
    pub fn new(capacity: usize) -> Self {
        let mut nodes = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            nodes.push(ListNode::default());
        }

        let free_indices = (0..capacity).collect();

        Self {
            nodes,
            free_indices,
            allocated: 0,
            capacity,
        }
    }

    /// 从池中获取一个节点
    #[inline]
    pub fn acquire(&mut self, order_id: u64, quantity: f64) -> Option<usize> {
        self.free_indices.pop().map(|idx| {
            self.nodes[idx] = ListNode {
                order_id,
                quantity,
                next: None,
                prev: None,
            };
            self.allocated += 1;
            idx
        })
    }

    /// 将节点返还到池中
    #[inline]
    pub fn release(&mut self, index: usize) {
        self.free_indices.push(index);
        self.allocated -= 1;
    }

    /// 获取节点的引用
    #[inline]
    pub fn get(&self, index: usize) -> Option<&ListNode> {
        self.nodes.get(index)
    }

    /// 获取节点的可变引用
    #[inline]
    pub fn get_mut(&mut self, index: usize) -> Option<&mut ListNode> {
        self.nodes.get_mut(index)
    }

    /// 获取已分配节点数
    #[inline]
    pub fn allocated_count(&self) -> usize {
        self.allocated
    }

    /// 获取可用节点数
    #[inline]
    pub fn available_count(&self) -> usize {
        self.capacity - self.allocated
    }
}

/// 使用节点池的链表
#[derive(Debug)]
pub struct PooledList {
    head: Option<usize>,
    tail: Option<usize>,
    count: usize,
}

impl PooledList {
    /// 创建新的链表
    pub fn new() -> Self {
        Self {
            head: None,
            tail: None,
            count: 0,
        }
    }

    /// 获取链表长度
    #[inline]
    pub fn len(&self) -> usize {
        self.count
    }

    /// 检查链表是否为空
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// 在链表尾部添加节点
    pub fn push_back(&mut self, node_idx: usize, pool: &mut ListNodePool) {
        if let Some(idx) = self.tail {
            pool.get_mut(idx).unwrap().next = Some(node_idx);
            pool.get_mut(node_idx).unwrap().prev = Some(idx);
        } else {
            self.head = Some(node_idx);
        }
        self.tail = Some(node_idx);
        self.count += 1;
    }

    /// 从链表中移除指定节点
    pub fn remove(&mut self, node_idx: usize, pool: &mut ListNodePool) {
        let node = pool.get(node_idx).unwrap();
        let prev = node.prev;
        let next = node.next;

        match (prev, next) {
            (None, None) => {
                // 链表中只有这一个节点
                self.head = None;
                self.tail = None;
            }
            (None, Some(next_idx)) => {
                // 这是头节点
                self.head = Some(next_idx);
                pool.get_mut(next_idx).unwrap().prev = None;
            }
            (Some(prev_idx), None) => {
                // 这是尾节点
                self.tail = Some(prev_idx);
                pool.get_mut(prev_idx).unwrap().next = None;
            }
            (Some(prev_idx), Some(next_idx)) => {
                // 中间节点
                pool.get_mut(prev_idx).unwrap().next = Some(next_idx);
                pool.get_mut(next_idx).unwrap().prev = Some(prev_idx);
            }
        }
        self.count -= 1;
    }

    /// 获取链表头
    #[inline]
    pub fn front(&self) -> Option<usize> {
        self.head
    }

    /// 清空链表
    pub fn clear(&mut self) {
        self.head = None;
        self.tail = None;
        self.count = 0;
    }
}

impl Default for PooledList {
    fn default() -> Self {
        Self::new()
    }
}
