use std::collections::VecDeque;

const MAX_LEVEL: usize = 12;
const PROMOTION_PROBABILITY: f64 = 0.25;

/// 跳表节点
pub struct SkipListNode {
    pub price: f64,
    pub total_quantity: f64,
    pub orders: VecDeque<u64>,
    pub forward: Vec<Option<Box<SkipListNode>>>,
    pub level: usize,
}

impl SkipListNode {
    /// 创建新节点
    fn new(price: f64, level: usize) -> Self {
        let mut forward = Vec::with_capacity(level + 1);
        for _ in 0..=level {
            forward.push(None);
        }

        Self {
            price,
            total_quantity: 0.0,
            orders: VecDeque::new(),
            forward,
            level,
        }
    }

    /// 随机生成节点层数
    fn random_level() -> usize {
        let mut level = 0;
        while level < MAX_LEVEL - 1 && rand::random::<f64>() < PROMOTION_PROBABILITY {
            level += 1;
        }
        level
    }
}

/// 跳表排序方向
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortOrder {
    Ascending,  // 升序（最小值在头部）
    Descending, // 降序（最大值在头部）
}

/// 跳表实现
pub struct SkipList {
    head: Box<SkipListNode>,
    order: SortOrder,
    count: usize,
}

impl SkipList {
    /// 创建新跳表
    pub fn new(order: SortOrder) -> Self {
        let mut forward = Vec::with_capacity(MAX_LEVEL);
        for _ in 0..MAX_LEVEL {
            forward.push(None);
        }

        let head = Box::new(SkipListNode {
            price: if order == SortOrder::Ascending { f64::NEG_INFINITY } else { f64::INFINITY },
            total_quantity: 0.0,
            orders: VecDeque::new(),
            forward,
            level: MAX_LEVEL - 1,
        });

        Self {
            head,
            order,
            count: 0,
        }
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

    /// 插入价格档位节点 - 完整实现
    pub fn insert_level(&mut self, price: f64) -> Result<(), String> {
        // 检查是否已存在
        if self.find_node(price).is_ok() {
            return Err("Price level already exists".to_string());
        }

        let level = SkipListNode::random_level();

        // 使用栈记录每层的插入点 - 存储原始指针
        let mut update: Vec<*mut SkipListNode> = vec![std::ptr::null_mut(); MAX_LEVEL];

        unsafe {
            let head_ptr = &mut *self.head as *mut SkipListNode;
            let mut current = head_ptr;

            // 从最高层向下遍历，找到每层的插入位置
            for i in (0..MAX_LEVEL).rev() {
                // 在第i层向右遍历
                loop {
                    // 显式创建引用来安全访问Vec
                    let forward_ref = &(&(*current).forward);
                    if let Some(Some(ref next_box)) = forward_ref.get(i) {
                        if self.should_insert(price, next_box.price) {
                            break;
                        }
                        // 从不可变引用获取原始指针来继续遍历
                        current = next_box.as_ref() as *const SkipListNode as *mut SkipListNode;
                    } else {
                        break;
                    }
                }

                // 记录该层的插入点
                update[i] = current;
            }

            // 创建新节点
            let new_node = Box::new(SkipListNode::new(price, level));
            let new_node_ptr = Box::into_raw(new_node);

            // 在每一层进行插入 - 从低层到高层
            for i in 0..=level {
                let prev = update[i];

                // 显式创建可变引用来安全访问Vec
                let prev_forward_mut = &mut (&mut (*prev).forward);
                let new_node_forward_mut = &mut (&mut (*new_node_ptr).forward);

                // 新节点的forward[i]指向prev的forward[i]
                new_node_forward_mut[i] = prev_forward_mut[i].take();

                // prev的forward[i]指向新节点
                prev_forward_mut[i] = Some(Box::from_raw(new_node_ptr));
            }
        }

        self.count += 1;
        Ok(())
    }

    /// 查找价格节点 - 改进的实现
    pub fn find_node(&self, price: f64) -> Result<(), String> {
        let mut current = &self.head;

        for i in (0..MAX_LEVEL).rev() {
            loop {
                if let Some(ref next) = current.forward.get(i).and_then(|opt| opt.as_ref()) {
                    // 检查是否找到了目标价格
                    if (next.price - price).abs() < 1e-10 {
                        return Ok(());
                    }

                    // 检查是否应该继续在这一层遍历
                    if self.should_insert(price, next.price) {
                        break;
                    }
                    current = next;
                } else {
                    break;
                }
            }
        }

        Err("Price not found".to_string())
    }

    /// 获取最优价格节点（头部）
    #[inline(always)]
    pub fn best(&self) -> Option<&SkipListNode> {
        self.head.forward.get(0)
            .and_then(|opt| opt.as_ref())
            .map(|b| &**b)
    }

    /// 获取最优且有订单的价格节点（跳过空的价格档位）
    #[inline]
    pub fn best_with_orders(&self) -> Option<&SkipListNode> {
        let mut current = &self.head;
        loop {
            match current.forward.get(0).and_then(|opt| opt.as_ref()) {
                Some(node) if !node.orders.is_empty() => return Some(node),
                Some(node) => {
                    // 跳过空的价格档位，继续查找
                    current = node;
                }
                None => return None,
            }
        }
    }

    /// 获取指定价格的可变节点引用（用于修改队列）
    #[inline]
    pub fn get_node_mut(&mut self, price: f64) -> Option<&mut SkipListNode> {
        unsafe {
            let head_ptr = &mut *self.head as *mut SkipListNode;
            let mut current = head_ptr;

            // 从最高层向下遍历找到目标节点
            for i in (0..MAX_LEVEL).rev() {
                loop {
                    let forward_ref = &(&(*current).forward);
                    if let Some(Some(ref next_box)) = forward_ref.get(i) {
                        let price_match = (next_box.price - price).abs() < 1e-10;

                        if price_match {
                            break;
                        }

                        if self.should_insert(price, next_box.price) {
                            break;
                        }
                        current = next_box.as_ref() as *const SkipListNode as *mut SkipListNode;
                    } else {
                        break;
                    }
                }
            }

            // 在第0层找到精确节点
            if let Some(Some(ref mut next_box)) = (&mut (*current).forward).get_mut(0) {
                if (next_box.price - price).abs() < 1e-10 {
                    return Some(next_box);
                }
            }
        }

        None
    }

    /// 向指定价格档位添加订单ID
    #[inline]
    pub fn add_order_at_level(&mut self, price: f64, order_id: u64, quantity: f64) -> Result<(), String> {
        unsafe {
            let head_ptr = &mut *self.head as *mut SkipListNode;
            let mut current = head_ptr;

            // 从最高层向下遍历找到目标节点
            for i in (0..MAX_LEVEL).rev() {
                loop {
                    let forward_ref = &(&(*current).forward);
                    if let Some(Some(ref next_box)) = forward_ref.get(i) {
                        let price_match = (next_box.price - price).abs() < 1e-10;

                        if price_match {
                            break;
                        }

                        if self.should_insert(price, next_box.price) {
                            break;
                        }
                        current = next_box.as_ref() as *const SkipListNode as *mut SkipListNode;
                    } else {
                        break;
                    }
                }
            }

            // 在第0层找到精确节点
            if let Some(Some(ref mut next_box)) = (&mut (*current).forward).get_mut(0) {
                if (next_box.price - price).abs() < 1e-10 {
                    next_box.orders.push_back(order_id);
                    next_box.total_quantity += quantity;
                    return Ok(());
                }
            }
        }

        Err(format!("Price level {} not found", price))
    }

    /// 从指定价格的订单队列中移除订单
    pub fn remove_order_at_level(&mut self, price: f64, order_id: u64) -> Result<(), String> {
        unsafe {
            let head_ptr = &mut *self.head as *mut SkipListNode;
            let mut current = head_ptr;

            // 从最高层向下遍历找到目标节点
            for i in (0..MAX_LEVEL).rev() {
                loop {
                    let forward_ref = &(&(*current).forward);
                    if let Some(Some(ref next_box)) = forward_ref.get(i) {
                        let price_match = (next_box.price - price).abs() < 1e-10;

                        if price_match {
                            // 找到目标价格，下一层继续查找
                            break;
                        }

                        if self.should_insert(price, next_box.price) {
                            break;
                        }
                        current = next_box.as_ref() as *const SkipListNode as *mut SkipListNode;
                    } else {
                        break;
                    }
                }
            }

            // 在第0层找到精确节点
            if let Some(Some(ref mut next_box)) = (&mut (*current).forward).get_mut(0) {
                if (next_box.price - price).abs() < 1e-10 {
                    // 从VecDeque中移除订单
                    if let Some(pos) = next_box.orders.iter().position(|&id| id == order_id) {
                        next_box.orders.remove(pos);
                        return Ok(());
                    }
                    return Err(format!("Order {} not found at price level {}", order_id, price));
                }
            }
        }

        Err(format!("Price level {} not found", price))
    }

    /// 清空跳表
    pub fn clear(&mut self) {
        self.head.forward.clear();
        for _ in 0..MAX_LEVEL {
            self.head.forward.push(None);
        }
        self.count = 0;
    }
}
