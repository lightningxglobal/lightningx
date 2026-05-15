# Binance vs OKX 市场数据对比研究

## 📊 总览

根据官方API文档，两大交易所提供的市场数据流和频率如下：

---

## 🔄 Binance 市场数据

### WebSocket 市场数据流

| 数据类型 | 频率/特征 | 数据内容 |
|---------|---------|--------|
| **Aggregate Trade (aggTrade)** | 每100ms推送一次 | 同价同方向的成交被聚合，包含: trade_id, price, qty, buyer/seller taker |
| **Trade Streams** | 实时推送 | 每笔成交实时推送，保证100%的交易数据 |
| **Kline/Candlestick** | 每1秒推送 (Spot) 或 每250ms (Futures) | OHLCV数据 + 成交量 |
| **Depth (Order Book)** | 实时 (可选100ms/1s更新) | 支持5/10/20档深度，可配置更新频率 |
| **24hr Ticker** | 每秒更新 | 24h价格统计数据 |
| **Rolling Window Stats** | 实时更新 | 不同时间窗口的价格变动统计 |

### REST API 市场数据端点

| 端点 | 用途 | 限制 |
|------|------|------|
| Order Book | 获取深度快照 | Weight: 5-250 (取决于深度) |
| Recent Trades | 最近成交 | Weight: 2 |
| Aggregate Trades | 历史成交聚合 | Weight: 2 |
| Klines | K线数据 | Weight: 2 |
| 24hr Ticker | 24小时统计 | Weight: 2 |

### Kline 支持的时间间隔
```
1s, 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M
```

### WebSocket 连接限制

- 单连接最多订阅: **1024个stream**
- 连接消息速率限制:
  - **Spot**: 5条/秒
  - **Futures**: 10条/秒
- 单个账户连接尝试限制: 300次/5分钟

### SBE Market Data Streams (新)

- 2025年3月18日推出的二进制格式
- **优势**: 更小的payload + 更低的延迟
- **适用于**: 高频交易、低延迟场景

**来源**: [Binance WebSocket Streams](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams)

---

## 🔄 OKX 市场数据

### WebSocket 市场数据通道

| 通道类型 | 频率/特征 | 数据内容 |
|---------|---------|--------|
| **Tickers** | 实时更新 | 最新价格、成交量、涨跌幅等 |
| **Trades** | 实时推送 | 单笔成交详情，包含: id, price, size, taker_side, timestamp |
| **All Trades** | 实时推送 | 全市场交易流 |
| **Depth (Order Book)** | 每100ms推送增量数据 | 支持多档深度(5/10/15/20档等)，推送bids/asks变化 |
| **Candlesticks/Klines** | 推送间隔不定 | 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 1w, 1M |
| **Option Trades** | 实时推送 | 期权交易数据 |
| **Funding Rates** | 定期推送 | 永续合约资金费率 |
| **Open Interest** | 定期推送 | 合约未平仓量 |

### OKX 特色机制

**价格录制**:
- OKX每200ms记录一个价格点
- K线最终价格是该周期内所有价格点的平均值
- 确保K线数据的平滑性

### REST API 市场数据端点

| 端点 | 用途 |
|------|------|
| Get Tickers | 获取交易对报价 |
| Get Ticker | 单个交易对信息 |
| Get Order Book | 深度快照 |
| Get Market Trades | 最近成交 |
| Get Candlesticks | K线数据 |
| Get 24H Trade Volume | 24h成交统计 |

### WebSocket 连接限制

- 连接限制: **3请求/秒** (基于IP)
- Subscribe/Unsubscribe限制: **480次/小时**
- 心跳检测: 20秒内未收到消息需主动ping
- 身份验证: 需要API Key进行私有频道订阅

### 支持的交易类型 (instType)

```
SPOT (现货)
MARGIN (保证金)
SWAP (永续合约)
FUTURES (期货)
OPTION (期权)
ANY (所有)
```

**来源**: [OKX API 文档](https://www.okx.com/docs-v5/en/)

---

## 📈 关键对比表

### 数据频率对比

| 指标 | Binance | OKX |
|------|---------|-----|
| **成交流** | 实时推送 | 实时推送 |
| **聚合成交** | 100ms汇总 | (需check) |
| **K线推送** | 1s (Spot) / 250ms (Futures) | 200ms (平均) |
| **Depth更新** | 配置型 (100ms/1s可选) | **固定100ms** |
| **Ticker更新** | 1s周期 | 实时 |
| **价格精度** | 逐笔记录 | 每200ms采样 |

### 产品覆盖对比

| 功能 | Binance | OKX |
|------|---------|-----|
| 现货市场数据 | ✅ 完整 | ✅ 完整 |
| 期货/永续 | ✅ USDS-M, COIN-M | ✅ SWAP, FUTURES |
| 期权数据 | ❌ 有限 | ✅ 专用通道 |
| 资金费率 | ✅ (Futures) | ✅ (实时推送) |
| 未平仓量 | ✅ (Futures) | ✅ (实时推送) |
| 历史数据 | ✅ REST API | ✅ REST API |

### 连接性对比

| 指标 | Binance | OKX |
|------|---------|-----|
| 单连接streams | 1024个 | (未限制说明) |
| 消息速率 | 5-10/s | 3/s (请求) |
| 身份验证 | API Key (Ed25519) | API Key + Secret + Passphrase |
| WebSocket格式 | JSON (默认) / SBE二进制 | JSON |
| 订阅频率 | 无限制 | 480次/小时 |

---

## 🎯 市场数据生成建议

### 现阶段设计方向

根据研究结果，建议撮合引擎的市场数据生成包括以下内容：

#### 1️⃣ **成交事件 (Trade Events)** - 高频
- 来源: 现有的match_orders_batch() 中的TradeEvent
- 频率: **实时** (完成一笔成交立即生成)
- 内容: trade_id, taker_id, maker_id, price, qty, timestamp, side

#### 2️⃣ **聚合成交 (Aggregate Trades)** - 100ms周期
- 收集100ms内同价、同方向的成交
- 按Binance模式聚合: 相同price + taker_side合并
- 发送: 聚合成交流给订阅者

#### 3️⃣ **K线/Candlestick** - 支持多时间窗口
- 维度: 1m, 5m, 15m, 1h, 1d (最小化, 可扩展)
- 推送: 
  - 每秒推送当前开放K线
  - K线关闭时推送确认K线
- 内容: OHLCV + 成交量 + 成交笔数

#### 4️⃣ **Ticker 数据** - 1秒周期
- 内容: last_price, 24h_high, 24h_low, volume_24h, quote_volume_24h
- 推送: 每秒更新一次

#### 5️⃣ **Order Book Snapshot** - 按需
- 当前最优买卖价 (BBO)
- 20档深度快照 (Binance level)
- 用途: 客户端渲染深度图

#### 6️⃣ **深度增量 (Depth Deltas)** - 100ms周期
- 推送100ms内的价位变化
- 内容: price, qty_change, side
- 用途: 客户端维护实时深度

---

## 💡 实施优先级

### Phase 1 (MVP - 现在)
- ✅ TradeEvent 实时推送 (已实现)
- ✅ 批量成交事件写入ring buffer (已实现)
- **下一步**: Aggregate Trades 100ms汇总

### Phase 2 (8h内)
- Ticker 数据 (1s周期)
- K线数据 (1m, 5m, 1h)
- Order Book快照

### Phase 3 (可选)
- 深度增量 (100ms推送)
- 24h统计数据
- 历史数据查询API

---

## 🔗 官方文档链接

**Binance:**
- [WebSocket Streams](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams)
- [Market Data Endpoints](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints)
- [SBE Market Data Streams](https://developers.binance.com/docs/binance-spot-api-docs/sbe-market-data-streams)

**OKX:**
- [OKX API 文档](https://www.okx.com/docs-v5/en/)
- [WebSocket API Guide](https://www.okx.com/en-us/okx-api)

---

## 📝 建议的数据结构

基于Binance/OKX的标准，建议实现以下Rust数据结构:

```rust
// 已实现
pub struct TradeEvent {
    pub sequence: u64,
    pub taker_id: u64,
    pub maker_id: u64,
    pub timestamp: u64,
    pub price: f64,
    pub quantity: f64,
    pub taker_side: Side,
}

// Phase 2 - Aggregate Trade
pub struct AggregateTradeEvent {
    pub aggregate_id: u64,
    pub timestamp: u64,
    pub price: f64,
    pub total_quantity: f64,
    pub first_trade_id: u64,
    pub last_trade_id: u64,
    pub is_buyer_maker: bool,  // true=卖单成交, false=买单成交
}

// Phase 2 - Kline
pub struct KlineEvent {
    pub timestamp: u64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub quote_volume: f64,
    pub trade_count: u32,
    pub is_closed: bool,
}

// Phase 2 - Ticker  
pub struct TickerEvent {
    pub timestamp: u64,
    pub last_price: f64,
    pub high_24h: f64,
    pub low_24h: f64,
    pub volume_24h: f64,
    pub quote_volume_24h: f64,
}
```

---

## ❓ 后续决策点

1. **Aggregate Trade 实现**: 100ms汇总还是逐笔推送?
2. **K线时间窗口**: 支持哪些时间间隔? (1m/5m/1h/1d)
3. **深度推送**: 是否需要增量更新 (100ms) 还是仅快照?
4. **数据持久化**: 是否需要存储历史行情数据?
5. **订阅模式**: 是否需要支持客户端动态订阅/取消?

