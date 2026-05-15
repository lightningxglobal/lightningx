# 撮合引擎市场数据设计方案

## 📋 三市场数据汇总

### 市场概览
```
市场1: Binance Spot (现货)
  - K线: 1s推送
  - Depth: 灵活配置 (100ms/1s)
  - Trade: 100ms聚合 + 实时
  - Ticker: 1s周期
  - 特点: 消息速率限制5/s

市场2: Binance Futures (衍生品)
  - K线: 250ms推送 ⭐
  - Depth: 100/250/500ms可选 + 快照250ms
  - Trade: 100ms聚合 + 实时
  - Ticker: 250-500ms
  - Mark Price: 1-3s (含资金费率)
  - Liquidation: 1s汇总
  - 特点: 消息速率限制10/s

市场3: OKX (现货+衍生品通用)
  - K线: TBD (估计200ms左右)
  - Depth: 100ms固定 + 实时BBO 10ms ⭐
  - Trade: 实时
  - Ticker: 实时
  - Open Interest: 定期 (衍生品)
  - Funding Rate: 定期 (衍生品)
  - 特点: 3req/s限制
```

---

## 🔍 数据类型分析

### 1. 成交流 (Trade Data)

| 市场 | Binance Spot | Binance期货 | OKX |
|------|-------------|-----------|-----|
| 原始成交 | 实时 | 实时 | 实时 |
| 聚合成交 | 100ms | 100ms | (无?) |
| 聚合方式 | 同价同向 | 同价同向 | - |

**分析**: 
- 100ms聚合减少消息量，但保留了核心信息
- OKX采用逐笔推送，实时性更强但消息量更大
- **建议**: 两种都提供，让用户选择

---

### 2. 深度流 (Order Book Data)

| 维度 | Binance Spot | Binance期货 | OKX |
|------|-------------|-----------|-----|
| **增量方式** | 配置型 | 100/250/500ms | 100ms固定 |
| **快照方式** | REST only | 250ms | 按需 |
| **档位选项** | 5/10/20 | 5/10/20 | TBD |
| **序列验证** | U/u/pu | U/u/pu | (需验证) |
| **BBO专门推送** | ❌ | ❌ | ✅ (10ms) |

**分析**:
- Binance: 自由选择频率，有快照支撑
- OKX: 固定100ms，BBO极快10ms
- **问题**: 频率差异大，难以统一
- **建议**: 统一为100ms增量 + 实时BBO

---

### 3. K线流 (Kline Data)

| 市场 | 推送频率 | 时间间隔支持 | 特点 |
|------|---------|-----------|------|
| Binance Spot | 1s | 1s-1M | 逐秒推送 |
| Binance期货 | **250ms** | 1s-1M | 高频更新 |
| OKX | TBD | 1m-1M | 可能200ms |

**分析**:
- Binance期货最高频 (250ms)
- Spot最低频 (1s)
- OKX可能在中间 (200ms估计)
- **关键问题**: 为什么Spot和期货频率差这么大？
  - 期货市场波动更快
  - 高频交易者需要更快更新
- **建议**: 
  - 实时模式: 每成交就推送当前K线变化
  - 周期模式: 200ms/500ms/1s三档选择

---

### 4. Ticker流 (价格统计)

| 市场 | 频率 | 数据内容 | 方式 |
|------|------|--------|------|
| Binance Spot | 1s | 24h统计 | 周期推送 |
| Binance期货 | 250-500ms | 24h统计 | 周期推送 |
| OKX | 实时 | 最新价等 | 变化推送 |

**分析**:
- Binance: 固定周期推送，但不够快
- OKX: 实时推送，反应快但可能冗余
- **建议**: 实时推送（有变化就推），避免冗余消息

---

### 5. 衍生品特有数据

| 数据类型 | Binance期货 | OKX |
|---------|-----------|-----|
| **标记价格** | 1-3s | TBD |
| **资金费率** | 1-3s | 定期 |
| **下次资金时间** | ✅ | ✅ |
| **未平仓量** | ❌ | ✅ |
| **清算单** | ✅ 1s汇总 | ❌ |
| **结算价格** | ✅ (settlement前) | ❌ |

**分析**:
- Binance: 清算数据完整，但资金费率频率低
- OKX: OI直接推送，省去REST查询
- **建议**: 
  - 标记价格: 1s周期 (足够)
  - 资金费率: 1s推送
  - OI: 按需更新 (可能1-5分钟)
  - 清算: 可选 (非必需的高频数据)

---

## 🎯 我们的设计决策

### 核心原则

```
1. 简洁性: 减少数据类型，避免冗余
2. 高效性: 选择合理的推送频率
3. 实用性: 满足交易和风控需求
4. 扩展性: 便于后续添加新数据类型
5. 统一性: 不同市场用相同接口
```

---

## 📊 推荐的市场数据方案

### A. 核心实时数据流 (必需)

#### 1. **成交流 (Trade Stream)**
```
频率: 实时 (逐笔)
数据: {
  trade_id: u64,
  timestamp: u64,        // nanosecond
  price: f64,
  qty: f64,
  side: "BUY" | "SELL",  // taker方向
  maker_id: u64,
  taker_id: u64,
}

为什么逐笔而不聚合?
- 完整性: 不丢失交易信息
- 风控: 需要追踪每笔成交
- 行情: 成交是实时消息的核心
- 简化: 让订阅端自己选择聚合

消息量: ~1-10条/秒 (取决于流动性)
```

#### 2. **深度流 (Order Book Stream)**
```
推送方式: 增量 + 实时BBO

增量深度:
频率: 100ms
数据: {
  timestamp: u64,
  sequence: u64,        // 用于去重/排序
  bids: [(f64, f64)],   // (price, qty)
  asks: [(f64, f64)],
}

实时BBO (可选, 如果需要极低延迟):
频率: 每当最优买卖价变化时推送
数据: {
  best_bid: (f64, f64),
  best_ask: (f64, f64),
  timestamp: u64,
}

为什么分离BBO?
- 高频交易需要<10ms BBO延迟
- 普通交易只需要100ms深度
- 分离可以让订阅端灵活选择
```

#### 3. **K线流 (Kline Stream)**
```
推送时机: 
- 每个K线周期关闭时推送一次
- (可选) 周期内开放K线实时更新

支持周期: 1m, 5m, 15m, 1h, 1d
(先这5个, 后续可扩展)

数据: {
  period: "1m" | "5m" | ...,
  timestamp: u64,
  open: f64,
  high: f64,
  low: f64,
  close: f64,
  volume: f64,
  quote_volume: f64,
  trade_count: u32,
  is_closed: bool,
}

频率:
- 1m: 关闭时推送1次 (每分钟最多60条消息)
- 5m: 关闭时推送1次 (每5分钟)
- ...
- 实时更新: (可选) 每500ms推送当前K线

为什么这样?
- K线关闭时推送: 避免冗余消息
- 支持多周期: 满足不同交易策略
- 实时更新可选: 高级用户需要，普通用户不需要
```

---

### B. 衍生品特有数据 (可选)

#### 4. **标记价格 (Mark Price)**
```
频率: 1s推送 (衍生品市场特有)
数据: {
  timestamp: u64,
  mark_price: f64,
  index_price: Option<f64>,      // 指数价格 (可选)
  funding_rate: f64,             // 年化利率 (%)
  next_funding_time: u64,        // 下次资金费时间
}

为什么1s?
- 资金费率不需要太频繁
- 标记价格跟现货价格关联不大
- 减少消息量
```

#### 5. **未平仓量 & 清算 (衍生品)**
```
未平仓量 (Optional):
频率: 5分钟或按需
数据: {
  timestamp: u64,
  open_interest: f64,
  
  为什么低频?
  - 用处: 衡量市场热度
  - 变化: 不是高频数据
  - 成本: 可以REST查询替代
}

清算单 (Optional):
频率: 实时 (清算发生时)
数据: {
  order_id: u64,
  side: "BUY" | "SELL",
  qty: f64,
  price: f64,
  timestamp: u64,
}

为什么可选?
- 用处: 风险监控 (可选)
- 频率: 不是核心交易数据
- 成本: 消息量不可控
```

---

### C. 聚合/统计数据 (衍生品)

#### 6. **聚合成交 (Aggregate Trade)** - 可选
```
频率: 100ms (可选)
聚合方式: 同价同向 (Binance方式)
数据: {
  timestamp: u64,
  price: f64,
  total_qty: f64,
  trade_count: u32,
  side: "BUY" | "SELL",  // taker方向
}

为什么可选?
- 作用: 减少消息量
- 成本: 复杂度增加
- 替代: 客户端自己聚合
建议: 优先推送原始成交，不提供聚合
```

---

## 📈 最终推荐方案

### **优先级 1: 必须实现** (MVP)
```
1. Trade Stream (实时，逐笔)
2. Depth Stream (100ms增量)
3. Kline Stream (1m/5m/15m/1h/1d，周期关闭时推送)
4. Mark Price (1s，衍生品)
```

### **优先级 2: 应该实现** (Phase 2)
```
1. Real-time BBO (当最优价变化时)
2. Ticker (实时推送，when价格/量变化)
3. 实时K线更新 (可选，500ms或1m内更新)
```

### **优先级 3: 可选实现** (Phase 3)
```
1. Aggregate Trade (100ms聚合)
2. Open Interest (5分钟级别)
3. Liquidation Orders (清算单)
4. Order Book Snapshot (快照API)
```

---

## 🔧 推送频率决策矩阵

| 数据类型 | 频率 | 为什么 | 取决于 |
|---------|------|------|--------|
| **Trade** | 实时 | 交易的核心事件 | 成交频率 |
| **Depth增量** | 100ms | Binance期货+OKX的标准，足够快 | 撮合频率 |
| **Depth快照** | 5分钟 | 风险管理，不需要太频繁 | 需求 |
| **BBO** | 实时 | 高频交易需要 | 最优价变化 |
| **Kline** | 周期闭合 | K线只在闭合时有意义 | 周期 |
| **Ticker** | 实时 | 减少冗余，有变化才推 | 价格/量变化 |
| **Mark Price** | 1s | 衍生品特有，不需要太快 | 市场要求 |
| **OI** | 5分钟 | 参考指标，不是交易驱动 | 主动查询 |
| **Liquidation** | 实时 | 风控关键信息 | 清算发生频率 |

---

## 💡 架构设计建议

### 事件驱动模型
```rust
pub enum MarketDataEvent {
    // 核心事件
    Trade(TradeEvent),
    DepthUpdate(DepthDelta),
    KlineClosed(KlineEvent),
    
    // 衍生品事件
    MarkPrice(MarkPriceEvent),
    
    // 可选事件
    BestBidAsk(BboEvent),
    Liquidation(LiquidationEvent),
    OpenInterest(OiEvent),
}

pub trait MarketDataSubscriber {
    fn on_trade(&mut self, event: &TradeEvent);
    fn on_depth_update(&mut self, event: &DepthDelta);
    fn on_kline(&mut self, event: &KlineEvent);
    fn on_mark_price(&mut self, event: &MarkPriceEvent);
    // ... 其他事件
}
```

### 多市场统一接口
```rust
pub struct UnifiedMarketData {
    market_type: MarketType,  // SPOT | FUTURES | SWAP
    symbol: String,           // BTCUSDT
    exchange: Exchange,       // BINANCE | OKX
    
    // 统一的数据字段
    latest_trade: Option<TradeEvent>,
    depth: OrderBookDepth,
    klines: HashMap<String, Kline>,
    mark_price: Option<MarkPriceEvent>,
}
```

---

## 🎯 设计优势

### vs Binance现货
- ✅ K线频率选项更多 (不只1s)
- ✅ Depth固定100ms (而不是配置化)
- ✅ 简化接口 (不提供过多Ticker变体)
- ✅ 衍生品数据完整

### vs Binance期货
- ✅ 统一现货和衍生品接口
- ✅ Depth标准化 (固定100ms)
- ✅ 移除冗余流 (多个Ticker流合并)
- ✅ OI数据支持 (OKX优势)

### vs OKX
- ✅ Mark Price频率明确 (而不是TBD)
- ✅ K线更新频率清晰
- ✅ 清算数据支持 (Binance优势)
- ✅ 增量Depth有序列号验证

---

## 📝 实施路线图

### **Week 1: MVP (Trade + Depth + Kline)**
```
Day 1-2:
  - Trade Stream框架
  - Depth增量维护引擎
  - 序列验证机制

Day 3-4:
  - Kline生成器 (1m/5m/1h/1d)
  - 周期闭合检测

Day 5:
  - 基础WebSocket推送
  - 单市场(Binance)集成测试
```

### **Week 2: Phase 2 (BBO + Mark Price)**
```
Day 1-2:
  - BBO实时推送
  - Mark Price生成器

Day 3-4:
  - 衍生品数据集成
  - 多市场适配层

Day 5:
  - 完整集成测试
  - 性能基准测试
```

### **Week 3+: Phase 3 (Aggregates + OI + Liquidation)**
```
- Aggregate Trade生成
- Open Interest查询
- Liquidation监听
```

---

## 🔗 下一步行动

### 1. 确认方案 (15分钟)
- ✅ 是否同意这个频率和数据类型选择?
- ✅ 是否需要调整任何优先级?

### 2. 数据结构设计 (1小时)
- 定义所有MarketDataEvent的Rust结构体
- 设计WebSocket消息格式

### 3. 实现计划 (2-3小时)
- 编写Trade Stream实现
- 编写Depth Manager
- 编写Kline Generator

---

## ✅ 方案总结

我们的设计：
- **简洁**: 4个核心数据类型 (Trade + Depth + Kline + MarkPrice)
- **高效**: 最大延迟100ms (除BBO实时)
- **统一**: 同一接口支持现货+期货+衍生品
- **可扩展**: 优先级清晰，易于添加新类型

这样既吸收了Binance的完整性和OKX的极速性，又避免了他们各自的冗余和复杂性。

你觉得这个方案如何? 需要调整什么吗?
