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
        let mut forward = Vec::with_capacity(MAX_LEVEL);
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

    /// 插入价格档位节点
    pub fn insert_level(&mut self, price: f64) -> Result<(), String> {
        if self.find_node(price).is_ok() {
            return Err("Price level already exists".to_string());
        }

        let level = SkipListNode::random_level();
        let new_node = Box::new(SkipListNode::new(price, level));

        // 简化的插入实现：直接追加到最低层
        // 在生产环境中应该实现完整的跳表插入算法
        if level == 0 {
            self.head.forward.get_mut(0).map(|slot| {
                *slot = Some(new_node);
            });
        }

        self.count += 1;
        Ok(())
    }

    /// 查找价格节点
    pub fn find_node(&self, price: f64) -> Result<(), String> {
        let mut current = &self.head;

        for i in (0..MAX_LEVEL).rev() {
            loop {
                match &current.forward.get(i).and_then(|opt| opt.as_ref()) {
                    Some(next) => {
                        if (next.price - price).abs() < 1e-10 {
                            return Ok(());
                        }
                        if self.should_insert(price, next.price) {
                            break;
                        }
                        current = next;
                    }
                    None => break,
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

    /// 清空跳表
    pub fn clear(&mut self) {
        self.head.forward.clear();
        for _ in 0..MAX_LEVEL {
            self.head.forward.push(None);
        }
        self.count = 0;
    }
}
