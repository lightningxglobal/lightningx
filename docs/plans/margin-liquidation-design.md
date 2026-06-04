# 保证金与强平系统设计

> 版本：v1.1 · 日期：2026-06-04  
> 范围：逐仓保证金（Isolated Margin）+ 标记价格 + 强平状态机  
> 前提：仅做永续合约（swap/futures），不做现货

---

## 一、核心原则

1. **撮合线程只撮合**——不查 DB、不算保证金、不等风控结果
2. **下单前必须经过内存 risk pre-check**——O(1)，不跨线程 await
3. **成交后通过事件流增量更新**——position/margin 状态以内存为准，DB 异步备份
4. **强平由独立 liquidation engine 驱动**——走同一套 order gateway + matching 流程，有专属标记
5. **所有状态可从事件日志重放恢复**——内存是缓存，事件日志是 source of truth
6. **全部用 fixed-point integer**——保证金、PnL、强平判断禁止使用 f64
7. **同一用户风险状态必须单写者**——多 desk-server 下按 `user_id` 固定路由到 risk owner shard，避免保证金双花

---

## 二、模块架构

```
客户端 (WebSocket)
    │
    ▼
desk-server
    ├── 解析 SBE 请求
    ├── user_id → risk owner shard 校验
    ├── 查 owner 本地内存 RiskState (O(1))
    ├── reserve margin / order notional（单写者）
    ├── 发 Aeron command → exchange-engine
    └── 返回 order_submitted (不等 accepted)
         │
         ▼
exchange-engine (spin thread per symbol)
    └── 撮合，发出 trade/order events via Aeron
         │
         ├──► position-margin engine
         │        ├── 增量更新 PositionRiskState
         │        ├── 更新 unrealized_pnl
         │        └── 发 PositionChanged / MarginChanged 事件
         │
         ├──► liquidation engine
         │        ├── 消费 mark_price + position + balance events
         │        ├── 检测 maintenance margin 穿越
         │        └── 生成 reduce-only liquidation market order
         │
         └──► persist writer (pg-writer)
                 └── 异步持久化所有事件，不在 live path
```

### 多 desk-server 风险一致性

生产环境会同时运行多个 `desk-server`。如果同一个 `user_id` 的订单可以落到不同 desk，本地 pre-check 会出现保证金双花：

```
desk-0: 看到 available_margin = 100，reserve 80
desk-1: 同时看到 available_margin = 100，reserve 80
结果：实际风险暴露 160，超过账户保证金
```

因此必须满足：

```
risk_owner = hash(user_id) % RISK_SHARDS
```

规则：
- 同一 `user_id` 的所有 order pre-check / reserve / release 由同一个 risk owner shard 串行处理。
- 用户连接可以在任意 desk，但下单必须路由到 owner shard；若当前 desk 不是 owner，可以内部转发，或通过 LB sticky routing 保证用户直接落到 owner desk。
- owner shard 对该用户的 `AccountRiskState` / `PositionRiskState` 是唯一写者；其他 desk 只能读同步过来的 snapshot，不能本地 reserve。
- 下单命令带 `risk_version` / `reserve_id`，便于成交、拒单、撤单事件幂等释放或确认。

**热路径（下单 → accepted）目标：< 500 µs**

热路径允许：
- 解析 SBE 请求
- 查本地内存 AccountRiskState / PositionRiskState
- O(1) risk pre-check（检查 available_margin >= required_margin）
- reserve margin（risk owner 单写者更新）
- 发 Aeron command

热路径禁止：
- DB 查询
- Redis 查询
- 跨线程 await risk 结果
- 全账户/全仓位扫描
- 动态风控脚本

---

## 三、数据结构（Fixed-Point Integer）

所有价格单位：`price_ticks: i64`（与现有撮合引擎一致）  
所有数量单位：`quantity_lots: i64`  
所有金额单位：`margin_units: i64`（1 unit = 最小计价货币单位，如 0.01 USDT）

```rust
/// 账户级风险状态（per user）
struct AccountRiskState {
    user_id: i64,
    version: u64,              // 单调递增，用于 desk snapshot 与 reserve 幂等校验
    equity: i64,              // 权益 = available + used_margin + unrealized_pnl
    available_margin: i64,    // 可用保证金
    order_margin: i64,        // 挂单预留保证金
    used_margin: i64,         // 已用保证金（所有仓位之和）
    maintenance_margin: i64,  // 当前账户维持保证金要求
    unrealized_pnl: i64,      // 浮动盈亏（由 mark price 驱动）
    status: RiskStatus,
}

/// 单仓位风险状态（per user-symbol）
struct PositionRiskState {
    user_id: i64,
    symbol_id: u32,
    side: PositionSide,          // Long / Short
    qty_lots: i64,               // 持仓数量（lots）
    entry_price_ticks: i64,      // 均价（ticks），VWAP
    mark_price_ticks: i64,       // 最新标记价（ticks）
    initial_margin: i64,         // 开仓时计算，逐仓
    maintenance_margin: i64,     // = qty × mark_price × maintenance_rate
    liquidation_price_ticks: i64, // 预计算，mark price 穿越时触发
    bankruptcy_price_ticks: i64,  // 预计算，用于保险基金盈亏判断
    leverage: u8,
    margin_mode: MarginMode,      // v1 只启用 Isolated，结构预留 Cross
}

enum MarginMode {
    Isolated,
    Cross,
}

/// 账户状态机
enum RiskStatus {
    Normal,
    MarginCall,          // 接近强平，限制加仓
    LiquidationPending,  // 禁止用户下单，撤销风险挂单
    Liquidating,         // 仅 liquidation engine 可下单
    Liquidated,          // 仓位清零，等待结算
    Bankruptcy,          // 进入保险基金/ADL 流程
}
```

### 逐仓保证金计算公式

```
多头强平价 = entry_price × (1 - 1/leverage + maintenance_rate)
多头破产价 = entry_price × (1 - 1/leverage)
空头强平价 = entry_price × (1 + 1/leverage - maintenance_rate)
空头破产价 = entry_price × (1 + 1/leverage)

当前 margin_ratio = (initial_margin + unrealized_pnl) / position_value
触发条件: margin_ratio < maintenance_rate (通常 0.5%)
```

以上公式是 v1 简化公式。生产版本还必须纳入：
- taker fee / liquidation fee
- maintenance margin tier
- funding payment
- 部分强平阶梯
- price band / fair price clamp

---

## 四、标记价格（Mark Price）

**生产默认：外部 index price + fair basis + clamp**

生产环境不能依赖本地 orderbook mid-price 作为强平依据，否则薄盘口可被操纵触发强平。生产 mark price 来源：

```
mark_price = clamp(index_price + fair_basis, lower_bound, upper_bound)
```

来源：
- 外部 index price：Binance/Bybit/OKX 等多源聚合
- fair basis：本地/外部永续价格与 index 的基差
- clamp：限制异常价格和瞬时操纵

**开发/测试模式：内部 EWMA Mid-Price**（仅用于跑通机制，不作为生产默认）

```
mark_price = EWMA(mid_price, α=0.1)
mid_price = (best_bid_ticks + best_ask_ticks) / 2
```

来源：exchange-engine 每次撮合后更新 best bid/ask → 通过 Aeron 行情频道推给 risk 相关消费者

**推送给哪些消费者**：
- position-margin engine：更新 unrealized_pnl
- liquidation engine：检查维持保证金

**更新频率**：mark price 高频变化时做 coalescing，risk tick 最高 **10ms 一次**，不直接全量扫描。

---

## 五、账户状态机转换规则

```
Normal ──────────────────────────────────────────────────────────┐
  │ margin_ratio < margin_call_threshold (通常 1.5×maintenance)  │
  ▼                                                               │
MarginCall ─── 限制加仓，只允许减仓 ──────────────────────────────┤
  │ margin_ratio < maintenance_rate                               │
  ▼                                                               │
LiquidationPending ─── 撤销风险挂单，禁止用户下单 ───────────────┤
  │ 撤单完成                                                       │
  ▼                                                               │
Liquidating ─── 仅 liquidation engine 发 reduce-only 单 ─────────┤
  │ 仓位清零 OR 风险恢复                                           │
  ▼                                                               │
Liquidated ─── 结算完成 ─────────────────────────────────────────┤
  │ fill_price < bankruptcy_price（保险基金不足）                  │
  ▼                                                               │
Bankruptcy ─── ADL 候选 ────────────────────────────────────────►┘
```

---

## 六、强平完整流程

```
liquidation engine 监测到 margin_ratio < maintenance_rate
    │
    ├─ 1. 状态机：Normal/MarginCall → LiquidationPending
    │
    ├─ 2. 发 Aeron 批量撤单指令（撤该用户该 symbol 所有挂单）
    │      → 等待撤单确认（超时 50ms 后记录 CancelTimeout，并继续监听后续成交/撤单事件）
    │
    ├─ 3. 状态机：LiquidationPending → Liquidating
    │
    ├─ 4. 通过 order gateway 生成 reduce-only liquidation market order
    │      source = Liquidation
    │      flags = REDUCE_ONLY | LIQUIDATION | RISK_BYPASS
    │      liquidation_id = 全局唯一 ID
    │      time_in_force = IOC
    │      side = 反向（多头 → Sell，空头 → Buy）
    │      qty = 全部持仓
    │
    ├─ 5. 监听成交回报
    │      ├─ fill_price > bankruptcy_price → 盈余转入保险基金
    │      └─ fill_price < bankruptcy_price → 保险基金补亏损
    │
    ├─ 6. 仓位清零 → 状态机：Liquidating → Liquidated
    │
    └─ 7. 发 LiquidationFinished 事件 → persist writer 落库
```

**强平订单的特权**：
- order gateway 识别 `source=Liquidation` + `flags=LIQUIDATION|REDUCE_ONLY` → 跳过 available_margin 检查
- 必须强校验 reduce-only：成交后仓位只能减少，不能反向开仓
- 可以在盘口流动性薄时仍然执行（不受 price limit 约束）
- 优先级：在 exchange-engine 中与普通订单同等处理（IOC 市价单语义）

禁止 liquidation engine 绕过 order gateway 直接向 exchange-engine 发普通 NewOrder。原因：
- order_id / liquidation_id / audit trail 必须统一
- desk/risk/persist 必须能看到强平订单生命周期
- reduce-only 校验必须在进入撮合前完成

---

## 七、保险基金

```rust
struct InsuranceFund {
    symbol: Symbol,
    balance: i64,   // fixed-point, USDT units
}
```

- 初始注资：系统配置
- 流入：强平成交价 > 破产价的盈余
- 流出：强平成交价 < 破产价的亏损
- 余额为零时：触发 ADL（Auto-Deleveraging，强制减仓盈利方）

ADL 是 Phase 4 实现，Phase 1–3 只要能检测到保险基金不足、记录 Bankruptcy 状态即可。

---

## 八、增量化 Mark Price 扫描（关键性能点）

**禁止做**：每次 mark price 更新时全量扫描所有用户

**正确做法**：
```
symbol_position_index: HashMap<SymbolId, Vec<UserId>>

当 BTC_USDT mark price 更新时：
  1. 只遍历 symbol_position_index[BTC_USDT]
  2. 对每个 user_id，读取 PositionRiskState（DashMap O(1)）
  3. 检查 margin_ratio < maintenance_rate
  4. 触发强平状态机

10ms coalescing：
  - mark price 在 10ms 内多次更新 → 只触发一次 risk tick
  - 用 AtomicI64 存最新值，risk tick 定时器读取
```

---

## 九、事件流定义

risk-engine、position-margin engine、liquidation engine 都从同一事件流推进状态；persist writer 异步订阅落库。

Aeron 是 live transport，不是持久 source of truth。可恢复的 source of truth 必须是 append-only risk journal：

```
Aeron live event → risk journal append → in-memory projection update → async DB snapshot
```

恢复路径：

```
latest DB snapshot + risk journal replay(from snapshot_sequence + 1) -> rebuild RiskState
```

```rust
enum RiskEvent {
    // 所有事件公共字段
    // event_id: u128
    // sequence: u64
    // producer_id: u32
    // ts_ns: u64

    RiskReserveCreated { reserve_id, user_id, symbol, margin, risk_version, ts },
    RiskReserveReleased { reserve_id, user_id, symbol, reason, ts },
    RiskReserveCommitted { reserve_id, user_id, symbol, order_id, ts },

    // 来自 exchange-engine
    TradeExecuted { order_id, symbol, side, price_ticks, qty_lots, maker_id, taker_id, ts },
    OrderAccepted { order_id, user_id, symbol, ts },
    OrderCanceled { order_id, user_id, symbol, ts },
    OrderRejected { order_id, user_id, reason, ts },

    // 来自 position-margin engine
    PositionChanged { user_id, symbol, new_qty_lots, new_entry_price_ticks, ts },
    MarginChanged { user_id, available_margin, used_margin, unrealized_pnl, ts },
    MarkPriceUpdated { symbol, mark_price_ticks, ts },

    // 来自 liquidation engine
    LiquidationStarted { user_id, symbol, margin_ratio, ts },
    CancelTimeout { user_id, symbol, liquidation_id, pending_order_count, ts },
    LiquidationOrderSubmitted { user_id, symbol, order_id, ts },
    LiquidationFinished { user_id, symbol, fill_price_ticks, pnl_to_insurance, ts },
    BankruptcyDetected { user_id, symbol, shortfall, ts },
}
```

事件流传输：
- live path：Aeron 新增 `risk_event` 频道，或复用现有 persist_event 频道加 tag 区分。
- recovery path：append-only risk journal 按 `sequence` 持久化，支持幂等 replay。
- 所有 consumer 必须按 `(producer_id, sequence)` 去重，避免重启/重放重复应用事件。

---

## 十、落地顺序

| 阶段 | 内容 | 验证方式 |
|---|---|---|
| **P1** | AccountRiskState + PositionRiskState 数据结构；desk-server 下单 pre-check（available_margin >= initial_margin）；冻结/释放逻辑 | 压测：下单 p50 不回归 |
| **P2** | `user_id -> risk owner shard`；reserve_id / risk_version；多 desk 幂等 reserve/release | 并发测试：同用户跨 desk 不能双花 |
| **P3** | 成交后增量更新 position（VWAP、qty）；TradeExecuted → PositionChanged 事件流 | 手动验证 position 与成交记录一致 |
| **P4** | risk journal + snapshot replay；事件 sequence / idempotency | 重启恢复测试：状态完全一致 |
| **P5** | mark price（dev EWMA + prod index 接口）；unrealized_pnl 增量更新；symbol_position_index；margin_ratio 计算 | 单元测试：多个 mark price 更新场景 |
| **P6** | RiskStatus 状态机；MarginCall + LiquidationPending 判断；批量撤单 | 集成测试：模拟强平场景 |
| **P7** | Liquidating → 生成 reduce-only liquidation IOC；监听成交；Liquidated 状态 | 压测：强平不影响普通用户延迟 |
| **P8** | 保险基金核算；Bankruptcy 检测 | 验证保险基金正确流入/流出 |
| **P9** | ADL（Auto-Deleveraging） | 后续版本 |

---

## 十一、现有代码的改动点

| 文件 | 改动 |
|---|---|
| `src/bin/desk_server.rs` / `src/desk/ws_handler.rs` | 下单前执行 owner risk shard pre-check；携带 `reserve_id` / `risk_version` |
| `src/desk/risk_state.rs`（新增） | AccountRiskState + PositionRiskState + per-user 单写者 reserve/release |
| `src/bin/risk-engine.rs`（新增） | risk projection、risk journal replay、mark price、liquidation detector |
| `src/risk/liquidation.rs`（新增） | 状态机 + 强平订单生成 |
| `src/transport/sbe.rs` / `src/transport/mod.rs` | NewOrderRequest 增加 `source` / `flags` / `reserve_id` / `liquidation_id`，或新增 liquidation template |
| `src/bin/pg_writer.rs` / `src/desk/pg_store.rs` | 订阅 RiskEvent，持久化 position/margin/liquidation 记录和 snapshots |
| `src/bin/exchange_engine.rs` | 校验 reduce-only/liquidation flags 不反向开仓；撮合线程不计算保证金 |
