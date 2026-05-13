# 加密货币交易撮合引擎设计文档

**日期**: 2026-05-13  
**作者**: Claude Code  
**版本**: 1.0  
**状态**: 设计审查

---

## 1. 概述

### 1.1 项目目标

开发一个基于跳表的极高频交易撮合引擎，支持多种委托类型，并通过benchmark验证性能。

**性能目标**:
- **TPS**: > 6,000,000 (每秒订单处理数)
- **延迟**: < 3微秒 (P99)
- **交易对**: 单币对（如BTC/USDT）

### 1.2 支持的功能

**委托类型**:
- GTC (Good Till Cancel): 一直有效直到成交或撤销
- IOC (Immediate Or Cancel): 立即成交，未成交部分取消
- FOK (Fill Or Kill): 全部成交或完全取消
- Post-Only: 只挂单，不吃单（不与现有订单撮合）

**核心操作**:
- 下单 (Place Order)
- 撤单 (Cancel Order)
- 订单查询 (Get Order by ID)
- 行情快照 (Depth Snapshot)

---

## 2. 架构设计

### 2.1 系统架构

```
┌─────────────────────────────────────────────────────────┐
│                    撮合引擎核心 (单线程)                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │   买盘跳表    │  │   卖盘跳表    │  │  订单HashMap  │  │
│  │  (价格降序)   │  │  (价格升序)   │  │  (ID->Order) │  │
│  └──────────────┘  └──────────────┘  └──────────────┘  │
│                                                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │  订单对象池   │  │  节点对象池   │  │  队列对象池   │  │
│  │(1,000,000)   │  │(100,000)     │  │(100,000)     │  │
│  └──────────────┘  └──────────────┘  └──────────────┘  │
└─────────────────────────────────────────────────────────┘
                          ↓ (实时事件)
                    Aeron Publisher
                   (< 100纳秒延迟)
                          ↓
        ┌─────────────────┼─────────────────┐
        ↓                 ↓                 ↓
  增量事件订阅     行情快照定时器    持久化服务
  (Trade/Order)   (1秒间隔)        (磁盘存储)
```

### 2.2 核心特性

1. **单线程模型**: 所有操作在单线程中顺序执行，无锁设计
2. **对象池预分配**: 消除运行时内存分配，避免malloc/free开销
3. **双跳表架构**: 买卖盘分离，O(1)撮合，O(log n)插入
4. **缓存对齐**: 64字节对齐避免false sharing
5. **异步持久化**: 通过Aeron发布事件，不阻塞撮合
6. **价格-时间优先**: 同价格订单按FIFO顺序撮合

---

## 3. 数据结构设计

### 3.1 订单结构 (Order)

```rust
#[repr(align(64))]  // 缓存行对齐
pub struct Order {
    pub id: u64,                      // 订单ID（自增）
    pub side: Side,                   // 买/卖
    pub price: f64,                   // 价格
    pub quantity: f64,                // 数量
    pub filled: f64,                  // 已成交数量
    pub time_in_force: TimeInForce,   // 委托类型
    pub timestamp: u64,               // 纳秒时间戳
    _padding: [u8; 18],               // 填充到64字节
}

pub enum Side { Buy, Sell }
pub enum TimeInForce { GTC, IOC, FOK, PostOnly }
```

**设计要点**:
- 64字节对齐，占用整个缓存行，避免false sharing
- 热数据（price, quantity, filled）紧密排列
- 使用枚举替代bool，提高代码清晰性

### 3.2 跳表节点结构

```rust
#[repr(align(64))]
pub struct SkipListNode {
    pub price: f64,                              // 价格档位
    pub total_quantity: f64,                     // 该价格总数量
    pub orders: *mut VecDeque<u64>,             // 订单ID队列（FIFO）
    pub forward: [Option<*mut SkipListNode>; 12],  // 前向指针（12层）
}
```

**设计要点**:
- 最大层数12（足够支持100万订单，防止缓存行溢出）
- 晋升概率0.25（减少高层节点，提高缓存局部性）
- 指针级别而非引用，便于内存池管理

### 3.3 对象池设计

```rust
pub struct ObjectPool<T> {
    objects: Vec<T>,              // 预分配对象数组
    free_indices: Vec<usize>,     // 空闲索引栈
    capacity: usize,
}

pub struct Pools {
    orders: ObjectPool<Order>,              // 订单池（1,000,000）
    nodes: ObjectPool<SkipListNode>,        // 节点池（100,000）
    queues: ObjectPool<VecDeque<u64>>,      // 队列池（100,000）
}
```

**设计要点**:
- 启动时完整预分配，运行时零malloc/free
- 使用栈管理空闲对象，O(1)获取/释放
- 固定容量，避免动态扩展

### 3.4 撮合引擎主结构

```rust
pub struct MatchingEngine {
    buy_book: SkipList,               // 买盘跳表（价格降序）
    sell_book: SkipList,              // 卖盘跳表（价格升序）
    orders: HashMap<u64, Order>,      // 订单ID映射
    pools: Pools,                      // 对象池
    next_order_id: u64,               // 下一个订单ID
    aeron_publisher: Publisher,        // Aeron发布器
    snapshot_sequence: u64,           // 快照序列号
}
```

---

## 4. 撮合逻辑

### 4.1 下单流程

```
Input: Order
  │
  ├─→ 验证(价格/数量)
  │   │
  │   ├─→ 分配订单ID
  │   └─→ 绑定时间戳
  │
  ├─→ 根据委托类型处理
  │   │
  │   ├─→ PostOnly: 检查是否立即成交
  │   │     ├─→ 是 → 拒绝
  │   │     └─→ 否 → 加入订单簿
  │   │
  │   ├─→ FOK: 检查能否完全成交
  │   │     ├─→ 是 → 撮合
  │   │     └─→ 否 → 拒绝
  │   │
  │   ├─→ IOC: 立即撮合，剩余取消
  │   │
  │   └─→ GTC: 先撮合，剩余加入订单簿
  │
  └─→ Output: OrderResult
```

### 4.2 撮合核心逻辑

**关键优化**:

1. **O(1)撮合**: 只需访问跳表头部获取最优对手价
2. **FIFO队列**: 同价格订单按时间优先级排列
3. **最小分支**: 热路径上减少条件判断
4. **内联函数**: 关键函数标记`#[inline(always)]`

**伪代码**:
```
function match_order(order):
    while order.filled < order.quantity:
        opposite_book = get_opposite_book(order.side)
        best_node = opposite_book.head()  // O(1)
        
        if !price_matches(order, best_node):
            break
        
        counter_order_id = best_node.orders.pop_front()  // O(1)
        trade_qty = min(order.remaining, counter_order.remaining)
        
        update_orders(order, counter_order, trade_qty)
        publish_trade_event()  // Aeron
        
        if counter_order.is_filled():
            remove_from_book(counter_order)
```

### 4.3 撤单流程

1. **查询订单**: HashMap查询O(1)
2. **验证状态**: 检查是否已成交
3. **移除订单**: 从跳表移除O(log n)
4. **清理节点**: 价格档位为空时删除节点
5. **发布事件**: Aeron发布撤单事件

---

## 5. Aeron集成

### 5.1 事件类型

```rust
pub enum MatchingEvent {
    OrderPlaced { ... },      // 下单事件
    OrderCancelled { ... },   // 撤单事件
    Trade { ... },            // 成交事件
}
```

### 5.2 发布策略

- **实时事件**: 每笔成交、下单、撤单立即发布（增量）
- **定时快照**: 每1秒发布一次完整的市场深度快照
- **零拷贝**: 使用Aeron的`claim_and_write` API避免内存拷贝
- **背压处理**: 如果Aeron缓冲区满，不阻塞撮合（可配置）

### 5.3 市场深度快照

```rust
#[repr(C, align(64))]
pub struct DepthSnapshot {
    pub timestamp: u64,           // 纳秒时间戳
    pub sequence: u64,            // 序列号
    pub num_bids: u16,            // 买盘档位数
    pub num_asks: u16,            // 卖盘档位数
    pub bids: [PriceLevel; 20],   // 买盘20档
    pub asks: [PriceLevel; 20],   // 卖盘20档
}

pub struct PriceLevel {
    pub price: f64,
    pub quantity: f64,
}
```

**快照特性**:
- **增量 + 快照模型**: 实时事件 + 定时快照
- **原子性**: 快照在单个时刻生成，反映完整状态
- **新订阅者同步**: 可从快照快速同步历史状态

---

## 6. 性能优化

### 6.1 缓存优化

1. **对齐优化**: 64字节对齐避免false sharing
2. **局部性**: 热数据集中在少量缓存行
3. **预热**: 启动时预热缓存和TLB

### 6.2 编译优化

```toml
[profile.release]
opt-level = 3           # 最高优化
lto = "fat"             # 链接时优化
codegen-units = 1       # 单线程编译
```

### 6.3 运行时优化

- 热路径内联: `#[inline(always)]`
- 分支预测: 使用match替代if-else
- 零分配: 对象池设计，运行时无malloc/free
- 无锁: 单线程设计，无同步开销

---

## 7. Benchmark设计

### 7.1 测试场景

1. **纯下单** (PlaceOrderOnly):
   - 测试订单簿插入性能
   - 测试对象池获取/释放速度
   
2. **纯撮合** (MatchingOnly):
   - 预填充订单簿
   - 测试成交匹配性能

3. **混合场景** (Mixed):
   - 模拟真实交易
   - 包含下单、撤单、撮合的混合操作

4. **极限压测** (MaxThroughput):
   - 10秒内以最快速度执行操作
   - 目标验证TPS > 6,000,000

### 7.2 性能指标

```
延迟 (纳秒):
  - P50, P99, P99.9, Max

吞吐量:
  - TPS (订单/秒)
  - Trades/sec (成交/秒)
  - 持续时间

内存:
  - 峰值内存使用
  - 平均内存使用
```

### 7.3 时间测量

使用Aeron提供的纳秒级时钟（`aeron_nano_clock`），精度 < 100纳秒。

---

## 8. 错误处理

### 8.1 错误类型

```rust
pub enum MatchingEngineError {
    OrderNotFound,
    InvalidPrice,
    InvalidQuantity,
    AlreadyFilled,
    OrderPoolExhausted,
    NodePoolExhausted,
    AeronNotConnected,
    AeronBackPressured,
}
```

### 8.2 处理策略

- **快速失败**: 无效订单立即返回错误
- **资源检查**: 分配前检查对象池可用性
- **背压忽略**: Aeron背压不阻塞撮合

### 8.3 崩溃恢复

```
快照 + 事件重放模型:
  1. 加载最后一个快照（重建订单簿）
  2. 从Aeron读取快照后的事件
  3. 重放事件（place_order / cancel_order）
  4. 恢复完整状态
```

---

## 9. 项目结构

```
matching-engine/
├── Cargo.toml
├── src/
│   ├── lib.rs           # 库入口
│   ├── engine.rs        # 撮合引擎主体
│   ├── skiplist.rs      # 跳表实现
│   ├── order.rs         # 订单数据结构
│   ├── pools.rs         # 对象池实现
│   ├── error.rs         # 错误定义
│   ├── aeron.rs         # Aeron集成
│   ├── snapshot.rs      # 快照生成
│   ├── event.rs         # 事件定义
│   └── recovery.rs      # 崩溃恢复
├── benches/
│   └── matching_bench.rs    # Benchmark
└── docs/
    └── DESIGN.md            # 本设计文档
```

---

## 10. 依赖关系

- **aeron-wrapper**: 自定义的Aeron Rust wrapper
- **criterion**: Benchmark框架
- **hdrhistogram**: 性能指标统计
- **tracing**: 日志和追踪

---

## 11. 实现计划

### Phase 1: 核心引擎 (Week 1)
- [ ] 跳表实现
- [ ] 订单数据结构
- [ ] 对象池实现
- [ ] 基础撮合逻辑

### Phase 2: Aeron集成 (Week 2)
- [ ] 事件发布
- [ ] 快照生成
- [ ] 异步持久化

### Phase 3: Benchmark & 优化 (Week 3)
- [ ] Benchmark实现
- [ ] 性能测试和优化
- [ ] 目标验证 (TPS > 6M, 延迟 < 3μs)

---

## 12. 关键假设和限制

### 假设
- 单币对交易，价格和数量均用f64表示
- 单线程模型，不支持多线程并发
- Aeron已正确配置和启动
- 足够的内存容纳对象池（~512MB）

### 限制
- 最多支持1,000,000个并发订单
- 价格档位最多100,000个
- 快照深度固定为20档

---

## 13. 审批和变更

| 日期 | 版本 | 状态 | 备注 |
|------|------|------|------|
| 2026-05-13 | 1.0 | 设计审查中 | 初始设计 |

---

## 附录

### A. 性能对标

| 指标 | 目标 | 说明 |
|------|------|------|
| TPS | > 6,000,000 | 每秒订单处理数 |
| 延迟 P99 | < 3μs | 99分位延迟 |
| 延迟 P999 | < 10μs | 99.9分位延迟 |
| 内存 | < 1GB | 峰值内存使用 |

### B. 参考资源

- Aeron: https://github.com/real-logic/aeron
- Skip Lists: https://en.wikipedia.org/wiki/Skip_list
- LMAX Disruptor: https://lmax-exchange.github.io/disruptor/

