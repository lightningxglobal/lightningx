# Binance & OKX 衍生品市场数据详细对比

## 📊 总览

研究范围: **Binance 期货 (USDS-M)** vs **OKX 永续/期货 (SWAP/FUTURES)**

---

## 🔴 Binance 期货 (Futures - USDS-Margined)

### WebSocket 市场数据流完整列表

#### 1. **成交流 (Trade Streams)**
| 类型 | 频率 | 数据内容 |
|-----|------|--------|
| Aggregate Trade | 100ms汇总 | 价格、数量、买卖方向、成交id |
| Trade | 实时 | 每笔成交实时推送 |

#### 2. **价格流 (Price Streams)**
| 类型 | 频率 | 数据内容 |
|-----|------|--------|
| Mark Price | 1s或3s | 标记价格、资金费率、下次资金时间、结算价格 |
| Mark Price All | 1s或3s | 全市场标记价格 |
| Index Price | 实时 | 指数价格 |

#### 3. **K线流 (Kline Streams)**
| 类型 | 频率 | 说明 |
|-----|------|------|
| Kline | **250ms推送** | 支持1s-1M多种时间窗口 |
| Continuous Contract Kline | **250ms推送** | 连续合约K线 |

#### 4. **深度流 (Depth Streams)**
| 类型 | 频率 | 档位 | 说明 |
|-----|------|------|------|
| Partial Book Depth | **250ms** | 5/10/20 | 快照方式，直接推送指定档位 |
| Diff Book Depth | 100ms/250ms/500ms | 全量 | 增量更新，需要本地维护完整深度 |

#### 5. **Ticker流**
| 类型 | 频率 | 数据内容 |
|-----|------|--------|
| Individual Mini Ticker | **250ms** | 最新价、成交量、价格变动 |
| All Mini Tickers | **250ms** | 全市场迷你ticker |
| Individual Ticker | **500ms** | 完整24h统计 |
| All Tickers | **500ms** | 全市场完整ticker |
| Book Ticker | **实时** | BBO (最优买卖价) |

#### 6. **清算流 (Liquidation Streams)**
| 类型 | 频率 | 说明 |
|-----|------|------|
| Liquidation Order | 1000ms汇总 | 强制平仓订单，1s内最大的一笔推送 |
| All Market Liquidation | 1000ms汇总 | 全市场清算 |

#### 7. **特殊数据流**
| 类型 | 频率 | 说明 |
|-----|------|------|
| Contract Info | 实时 | 合约信息更新 (如手续费、保证金率) |
| Trading Session | 定期 | 交易时段信息 |

### 连接限制

- 单连接最多: **1024个stream**
- 消息速率: **10条/秒** (Futures)
- 心跳: 3分钟发送ping，10分钟内无pong则断连

### 深度流详细说明

**Diff Book Depth 增量更新** (推荐用于完整深度维护):
```
可选频率: 100ms, 250ms, 500ms
数据格式: {
  "U": first_update_id,
  "u": final_update_id,
  "pu": previous_final_update_id,  // 用于序列验证
  "b": [[price, qty], ...],         // bid变化
  "a": [[price, qty], ...]          // ask变化
}
```

**Partial Book Depth 快照**:
```
频率: 250ms
档位: 5, 10, 20
返回指定档位的完整快照
```

---

## 🟦 OKX 衍生品 (SWAP/FUTURES)

### WebSocket 市场数据通道

#### 1. **成交流**
| 通道 | 频率 | 数据内容 |
|-----|------|--------|
| Trades | 实时 | 单笔成交详情 |

#### 2. **价格流**
| 通道 | 频率 | 说明 |
|-----|------|------|
| Tickers | 实时 | 最新价格、成交量、涨跌幅 |
| Mark Price (期货特有) | (需查证) | 标记价格、资金费率 |

#### 3. **K线流**
| 通道 | 频率 | 说明 |
|-----|------|------|
| Candlesticks | (需查证) | 支持多时间窗口: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 1w, 1M |

#### 4. **深度流**
| 通道 | 频率 | 说明 |
|-----|------|------|
| Books (增量) | **100ms** | 订单簿增量更新 |
| Books Full Snapshot | 定期 | 全量快照 |
| Best Bid/Ask (BBO) | **10ms** | 最优买卖价 |

#### 5. **特殊数据流**
| 通道 | 内容 | 说明 |
|-----|------|------|
| Open Interest | 定期 | 未平仓量 (衍生品专属) |
| Funding Rate | 定期 | 资金费率 (SWAP专属) |

### 连接限制

- 请求速率: **3请求/秒** (IP限制)
- 订阅限制: **480次/小时** (subscribe/unsubscribe/login)
- 心跳: 20秒内无消息则需主动ping，30秒无消息断连
- 支持: SPOT, MARGIN, **SWAP**, **FUTURES**, OPTION, ANY

---

## 🔄 关键对比表

### 更新频率对比

| 数据类型 | Binance期货 | OKX (SWAP/期货) |
|---------|-----------|-----------------|
| **K线** | **250ms** | TBD |
| **Depth增量** | 100ms/250ms/500ms | **100ms** |
| **Depth快照** | 250ms | 定期 |
| **BBO** | 实时 | **10ms** ⭐ |
| **Ticker (完整)** | 500ms | 实时 |
| **Mark Price** | 1s/3s | TBD |
| **成交流** | 100ms聚合 + 实时 | 实时 |
| **清算单** | 1s汇总 | (需查) |
| **资金费率** | 3s (Mark Price中) | 定期 |
| **未平仓量** | ❌ | ✅ 定期推送 |

### 数据流数量对比

| 维度 | Binance期货 | OKX衍生品 |
|------|-----------|---------|
| **成交相关** | 2个流 | 1个流 |
| **价格相关** | 4-6个流 | 3-4个流 |
| **深度相关** | 2个流 (快照+增量) | 2-3个流 |
| **Ticker相关** | 4个流 | 1个流 |
| **衍生品特有** | 清算、资金费率 | OI、资金费率 |

---

## 💡 实施优先级建议

### Binance期货实现优先级

#### **Phase 1** (高优先级 - 高频交易必须)
1. ✅ **Aggregate Trade** (100ms) - 成交汇总
2. ✅ **Kline** (250ms) - K线更新
3. ✅ **Diff Book Depth** (100ms/250ms) - 完整深度维护
4. ⏳ **Mark Price** (1s/3s) - 标记价格、资金费率

#### **Phase 2** (中等优先级)
1. **Book Ticker** (实时) - BBO推送
2. **Individual Mini Ticker** (250ms) - 快速ticker
3. **Liquidation Order** (1s) - 清算数据
4. **Partial Book Depth** (250ms) - 快照深度

#### **Phase 3** (可选)
1. **Contract Info** - 合约参数变化
2. **Index Price** - 指数价格
3. 所有市场聚合流

---

### OKX衍生品实现优先级

#### **Phase 1** (需补充具体频率后实施)
1. **Books** (增量100ms) - 深度更新
2. **Tickers** (实时) - 价格ticker
3. **Candlesticks** - K线数据
4. **Trades** (实时) - 成交流

#### **Phase 2**
1. **Best Bid/Ask** (10ms) - 最优价
2. **Open Interest** - 未平仓量
3. **Funding Rate** - 资金费率

---

## 🎯 核心发现

### Binance期货优势
- ✅ **清晰的多档位深度支持** (Partial: 5/10/20档)
- ✅ **灵活的Depth更新频率** (100ms/250ms/500ms可选)
- ✅ **丰富的Ticker流** (迷你/完整/全市场)
- ✅ **明确的清算数据** (1s汇总)

### OKX衍生品优势
- ✅ **极快的BBO更新** (10ms) ⭐
- ✅ **固定100ms Depth增量** (一致性好)
- ✅ **实时Ticker更新** (不是周期性)
- ✅ **未平仓量直接推送** (衍生品关键指标)
- ✅ **实时资金费率** (对永续合约重要)

### 劣势
**Binance**:
- ❌ Depth快照才250ms (较慢)
- ❌ Mark Price/资金费率只有1-3s (偏慢)

**OKX**:
- ❌ 官方文档不够详细 (需补充验证)
- ❌ 未平仓量/资金费率频率未知

---

## 📝 推荐的数据模型

### 衍生品特有结构

```rust
// Binance期货
pub struct BinanceFuturesSnapshot {
    // Mark Price相关
    pub mark_price: f64,
    pub mark_price_ma: f64,         // 标记价格移动平均
    pub index_price: f64,
    pub funding_rate: f64,
    pub next_funding_time: u64,
    pub estimated_settle_price: Option<f64>,  // 结算前1h有效
    
    // 清算数据
    pub liquidation_order: Option<LiquidationOrder>,
}

// OKX衍生品
pub struct OKXDerivativesSnapshot {
    // 永续合约特有
    pub open_interest: f64,
    pub funding_rate: f64,
    pub next_funding_time: u64,
    
    // 深度相关
    pub best_bid_price: f64,
    pub best_ask_price: f64,
    pub best_bid_qty: f64,
    pub best_ask_qty: f64,
}

pub struct DepthDelta {
    pub event_time: u64,
    pub first_update_id: u64,
    pub final_update_id: u64,
    pub bids: Vec<(f64, f64)>,      // price, qty
    pub asks: Vec<(f64, f64)>,
}
```

---

## ⚠️ 实施注意事项

### Binance期货需要处理:
1. **Depth序列验证** - 使用U/u/pu确保无gap
2. **Mark Price vs Liquidation** - 两个信息源可能冲突
3. **K线关闭** - 需要跟踪K线是否已关闭
4. **清算数据去重** - 1s内多笔清算只推送最大的

### OKX需要补充的信息:
1. ⏳ K线推送频率 (需官方确认)
2. ⏳ Mark Price频率 (需官方确认)
3. ⏳ Open Interest更新频率
4. ⏳ 资金费率更新频率

---

## 🔗 官方文档

**Binance期货**:
- [WebSocket Market Streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams)
- [Diff Book Depth](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Diff-Book-Depth-Streams)
- [Mark Price Stream](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Mark-Price-Stream)
- [Liquidation Streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Liquidation-Order-Streams)

**OKX**:
- [OKX API官方文档](https://www.okx.com/docs-v5/en/) ⏳ (部分信息需补充)
- [GitHub - OKX WebSocket API Docs](https://github.com/lhzk377/okx-websocket-api-docs)

---

## 📋 下一步建议

1. **补充OKX官方信息** - 完善各通道具体更新频率
2. **选择实现重点**:
   - 优先Binance期货 (更成熟，数据完善)
   - 后续补充OKX (需官方文档确认)
3. **设计架构**:
   - 分离Spot/Futures/SWAP的市场数据流
   - 统一的DepthManager处理增量更新
   - 事件驱动架构推送数据

4. **性能目标**:
   - Depth增量: <10ms处理延迟 (追赶OKX BBO 10ms)
   - K线更新: <50ms (Binance 250ms内)
   - Ticker: 实时推送 (Binance 250-500ms)
