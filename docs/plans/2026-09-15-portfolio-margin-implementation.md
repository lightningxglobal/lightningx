# 组合保证金（Portfolio Margin）实现方案 v2

状态：设计，未实现。日期：2026-09-15。
前置文档：`docs/portfolio_margin_design.md`（quota lease 协议草案，本文将其定位为独立的延迟优化轨道）。

---

## 0. 对"GRC 方案"分析的核对结论

外部分析（撮合引擎扣保证金、desk 彼此不可见、需新建 Global Risk Coordinator）与当前代码有三处不符：

| 分析中的说法 | 代码现状 | 依据 |
|---|---|---|
| exchange-engine 负责保证金扣减与清算，回推 BALANCE_UPDATE | 引擎是纯撮合，不知道余额。保证金冻结（`check_and_reserve_margin`）和成交结算（`on_fill`）都在 desk-server 的 `RiskEngine` | `src/desk/risk/engine.rs`；`src/matching/` 无任何 margin/balance 概念 |
| 用户按 `user_id % 4` 固定路由到一个 desk，不同币的引擎彼此不知道 | 连接可进任意产品柜台，但每笔私有请求经 Aeron counter-forward 转到 owner shard（`user_id % 4`）。owner 上的 `RiskEngine` 持有该用户**全部 symbol** 的仓位、权益、MMR，并订阅全量 depth 流更新所有 symbol 的 mark price | `src/desk/counter_shard.rs`；`desk_server.rs` cf_owner_new 分支；`DEPTH_STREAM` 单流全 symbol |
| 需要新建 GRC 汇聚 POSITION_UPDATE / BALANCE_UPDATE | 汇聚已经存在，就是 owner shard 的 `RiskEngine`。而且现有 tick 已是**账户级全仓**：`equity ≤ Σ 仓位 MM → LiquidationPending`，强平对该用户全部仓位发 IOC | `run_risk_tick`，`AccountRiskState.maintenance_margin` |

因此："GRC + 信用额度下发"在本代码库里等价于 `docs/portfolio_margin_design.md` 的 quota lease，且外部版本缺 epoch / TTL / haircut，会在价格下跌和脑裂时超发。它解决的是**转发一跳的延迟**（40K 压测：forward 比 no-forward p50 慢 20-25µs，p99 1.5ms → 21-27ms），不是组合保证金本身。

**结论：组合保证金 = 改 owner shard 内 `RiskEngine` 的保证金模型。不需要新协议、新服务、新线程。** quota lease 是可选的第二轨道。

---

## 1. 现状差距（要改的就这四点）

| 维度 | 现状 | 组合保证金目标 |
|---|---|---|
| 维持保证金 MM | 账户 `maintenance_margin = Σ 各仓位 notional × rate` 线性相加 | 风险单元内多空净额 + 跨单元相关性折扣 → `portfolio_mm` |
| 前置初始保证金 IM | `notional / leverage`，与已有仓位无关 | 边际 IM：对冲单少冻或不冻 |
| 强平 | 全部仓位一起 IOC（分批 `LIQ_TRANCHE_BPS`） | 按风险贡献排序逐仓减仓，对冲对成对处理 |
| 数据 | `symbol → users` 索引，无 `user → symbols` | 需要按用户遍历其全部仓位 |

抵押品仍是单一 USDT（多币种抵押是另一个模块，本文不涉及）。

---

## 2. 保证金模型 v1：净额 + 相关性折扣

全部定点数，单位 atoms（1e-8 USDT），比率用 bps，中间量 i128。与 `calc.rs` 现有风格一致。

### 2.1 风险单元（Risk Unit, RU）

RU = base asset（`BTC_USDT` → `BTC`）。当前一个 RU 只有一个 symbol，但模型按 RU 聚合，未来同 RU 多合约（如 `BTC_USDC`）自动净额。`counter_shard::symbol_route_desk_id` 已有 base 解析，复用。

### 2.2 单 RU 维持保证金

```
long_notional  = Σ 多头 |notional|（mark 价）
short_notional = Σ 空头 |notional|
net   = |long − short|
gross = long + short
hedged = gross − net
mm_ru = net × mm_rate_bps/1e4 + hedged × mm_rate_bps × hedge_rate_bps / 1e8
```

`hedge_rate_bps` 是对冲部分保留的比例（如 2000 = 保留 20%，覆盖基差/执行风险）。同 RU 单合约时 `hedged = 0`，退化为现有公式。

### 2.3 跨 RU 折扣（只做配对表，不做协方差矩阵）

对配置表中每对 `(A, B, corr_bps)`：

```
offset(A,B) = min(mm_A 的净多方向敞口对应 MM, mm_B 的净空方向敞口对应 MM) × corr_bps / 1e4
portfolio_mm = Σ mm_ru − Σ offset
```

只有 A 净多 / B 净空（或反之）才产生 offset；同向不折扣。配置示例：`PM_CORR=BTC:ETH:5000,ETH:SOL:3000`，解析函数仿 `parse_risk_tiers`。v1 不估波动率，corr 是人工参数。

### 2.4 初始保证金

`portfolio_im = portfolio_mm × im_multiplier_bps / 1e4`（如 20000 = 2×）。IM 与 MM 同源，避免两套模型漂移；杠杆档位（`check_leverage_tier`）继续限制单仓名义额上限，不再决定 IM。

### 2.5 v2（后置）：情景矩阵

每 RU 价格 ±p% × 波动 ±v% 网格，组合最坏损失为 MM。接口不变（`portfolio_mm(positions, marks, params) -> i64`），只换实现。

---

## 3. 热路径：边际 IM 的 O(1) 实现

**问题**：边际 IM = `pim(portfolio ⊕ order) − pim(portfolio)`，要读用户全部仓位，且 `check_and_reserve_margin` 现在是 `accounts.get()` 共享读锁 + 原子 CAS，无法容忍在里面遍历 `positions`。

**方案**：把"边际"拆成"独立 IM × 对冲折扣"，折扣在慢路径预计算：

```rust
// AccountRiskState 新增（只在 get_mut 下写，与 used_margin 同规则）
pub hedge_credit: SmallVec<[HedgeCredit; 4]>,   // 每个有仓位的 RU 一条

#[derive(Clone, Copy)]
pub struct HedgeCredit {
    pub ru: [u8; 8],
    pub side: u8,              // 哪个方向的新单能享受折扣（与净敞口相反）
    pub notional_atoms: i64,   // 可折扣的名义额上限（= 当前净敞口）
    pub discount_bps: i64,     // 1e4 − 保留比例
}
```

热路径（owner 侧 `cf_owner_new` 分支与本地 `ws_handler` 分支共用）：

1. 算独立 IM（现有 `calc_initial_margin_atoms`）。
2. `credit = hedge_credit.iter().find(ru == order.ru && side == order.side)`；命中则 `im -= min(order_notional, credit.notional) × discount_bps / 1e4`。
3. 原子 CAS 冻结 `im`（现有逻辑不变）。

复杂度：≤4 条 SmallVec 线性查找 + 几次乘除，百纳秒级，不加锁。

**折扣快照的时效**：`hedge_credit` 在 `on_fill`（仓位变化）和 `update_mark_price`（名义额变化）里重算，两处都已持有 `accounts.get_mut`。两笔对冲单并发时第二笔可能重复享受折扣——成交后 `on_fill` 立即按真实 `portfolio_im` 校正 `used_margin`，超冻/少冻的差额通过 `available_margin` 一次性回冲，与现有 `runtime_consume_fill_margin` 的差额回冲路径一致。风险敞口窗口 ≤ 一次成交回执。

跨 RU 折扣不进热路径（v1 只在 MM 侧生效，IM 侧不给跨 RU 折扣）。理由：跨 RU 折扣对单笔订单的边际影响小，而且方向判断依赖两个 RU 的净敞口，预计算表膨胀。

---

## 4. 慢路径：MM 重算与风控 tick

### 4.1 需要 `user → positions` 遍历

`RiskEngine` 现只有 `symbol_position_index: symbol → HashSet<user>`。新增：

```rust
// AccountRiskState 内，get_mut 下维护
pub position_symbols: SmallVec<[[u8; 16]; 4]>,
```

`on_fill` 开仓/平仓时增删。`update_mark_price` 对某 symbol 的每个用户：`accounts.get_mut(uid)` → 对 `position_symbols` 逐个 `positions.get(&(uid, sym))` → 重算 `portfolio_mm` 与 `hedge_credit`。

⚠️ 这是 `get_mut` + 多次键控 `get`，不是 `iter_mut` 内套 `iter`，不触发 DashMap 6.x 的嵌套迭代死锁（见 `margin_liquidation_impl` 记录）。测试里要保留 40K 用户 × 2 仓位的 tick 时长断言。

### 4.2 tick 不改结构

`run_risk_tick` 三段式不动。`AccountRiskState.maintenance_margin` 的**语义**改为 `portfolio_mm`（PM 账户）或 `Σ mm`（非 PM 账户），由写入方（`on_fill` / `update_mark_price`）决定。tick 的阈值判断、`MarginCall` 判定、B2 hydration 保护全部复用。

### 4.3 账户模式

`AccountRiskState.margin_mode: u8`（0 = cross-sum 现状，1 = portfolio）。opt-in，默认 0。来源：`risk_accounts` 表新列 + `RiskAccountSetPayload` 新增 1 字节（占用 `_pad`），hydration 读回。同一 tick 内两种模式并存，只是 MM 数值来源不同。

---

## 5. 组合强平：有序减仓

现状：`LiquidationPending` → 对该用户**每个仓位**发 `LiquidationEvent` → 各自分批 IOC。组合保证金下这是错的：先平掉对冲对的一条腿，组合风险**上升**。

新规则（在 `run_risk_tick` Pass 2 替换 `positions.iter().filter()`）：

1. 取该用户全部仓位，按 RU 分组。
2. 对每个候选减仓动作算 `Δmm = portfolio_mm(P) − portfolio_mm(P \ action)`：
   - 单腿仓位：action = 平该仓一个 tranche。
   - 对冲对（同 RU 多空，或配置表中有 offset 的两 RU）：action = **两腿按净敞口比例同时减**，永远不单独平一条腿。
3. 按 `Δmm / 释放名义额` 降序，先执行第一条 action（一个 tick 只发一组 IOC）。
4. 成交回来 `on_fill` 重算 `portfolio_mm`，下个 tick 重新评估：`equity > portfolio_mm × (1 + buffer_bps)` 则停止，状态回 `Normal`；否则继续下一条。

入口不变：仍是 `AeronCmd::NewOrder` 市价 IOC，`liq_price_ticks != 0` 标记，`fill_margin = 0`。`LIQ_TRANCHE_BPS` 复用。保险基金结算逻辑不动。

---

## 6. 持久化与恢复

- **不持久化派生量**。`portfolio_mm`、`hedge_credit` 都是 `(positions, marks, params)` 的纯函数，崩溃恢复 = hydration 读回仓位后重算。
- 持久化的只有 `margin_mode`（`RiskAccountSetPayload` 加 1 字节，pg-writer 加列）和模型参数版本号（审计用，落 `PersistEvent` 一条 `PmParamsSet`，可后置）。
- 对账工具（`reconcile.rs`）加一项：重算 `portfolio_mm` 与内存值一致。

---

## 7. 落地顺序（每步可独立验证与提交）

| 步 | 改动 | 验证 |
|---|---|---|
| 1 | `calc.rs`：`parse_pm_corr`、`ru_of_symbol`、`ru_mm`、`portfolio_mm`、`portfolio_im`、`hedge_credit_for`。纯函数 | 单测：手算数字。BTC 多 1 + BTC 空 1 → mm = 0.2 × 单腿；BTC 多 / ETH 空 corr 5000 → 折扣 = min 腿 × 50%；同向无折扣；溢出用 i128 |
| 2 | `AccountRiskState` 加 `margin_mode` / `position_symbols` / `hedge_credit`；`on_fill` 与 `update_mark_price` 重算 `maintenance_margin` | `engine.rs` 测试：对冲组合在 cross-sum 下会 `LiquidationPending`、PM 模式下 `Normal`；40K × 2 tick 耗时不回归 |
| 3 | `check_and_reserve_margin(user, im, ru, side)` 应用 `hedge_credit`；两个调用点（`ws_handler.rs` 本地分支、`desk_server.rs` cf_owner_new 分支）与 batch 分支同步 | 测试：已有 BTC 多，下 BTC 空单冻结 = 20%；超出净敞口部分全额；并发两笔对冲单后 `on_fill` 校正 `used_margin` |
| 4 | 强平排序 + 对冲对成对减仓（§5） | 测试：对冲对只发成对 IOC；单腿仓位先平 `Δmm` 最大者；组合回安全后停止 |
| 5 | `margin_mode` 持久化 / hydration / reconcile | e2e：重启后 PM 账户 MM 与重启前一致 |
| 6 | （可选）v2 情景矩阵替换 `portfolio_mm` 实现 | 同步骤 1 的接口测试 |

步骤 1-3 完成即可上线"对冲少冻 + 组合 MM"，是用户可感知的资金效率提升；4-5 是安全闭环，上线前必须完成。

---

## 8. 第二轨道：quota lease 与 PM 的衔接

`docs/portfolio_margin_design.md` 的租约协议目标是去掉 ingress → owner 的转发跳。它与 PM 正交，但 PM 改变了它的一个前提：**边际 IM 不可加**，接入柜台没有组合视图，算不出对冲折扣。

衔接规则：柜台本地闸口按**独立 IM（无折扣）**扣租约，保守放行；owner（home）收到成交流后按真实 `portfolio_im` 重算，差额在下个 tick 通过 `LeaseGrant` 增量返还。对冲收益从"下单时少冻"变成"成交后 ≤10ms 回补"，不变式 `Σ 租约 ≤ F × (1 − haircut)` 仍然成立。

是否做第二轨道的判据：在 Linux 物理机上复测 forward 路径的 p99。若 p99 在目标内，租约协议不做。

---

## 9. 假设（未与需求方确认，按此实现）

1. 单一 USDT 抵押品。
2. RU = base asset；跨 RU 折扣只做人工配对表，不估相关性。
3. PM 是账户级 opt-in 模式，非 PM 账户行为完全不变。
4. IM 侧只给同 RU 对冲折扣，跨 RU 折扣只在 MM 侧生效。
5. 强平中对冲对永远成对减仓。
6. 不引入新二进制、新 Aeron 流、新线程；所有改动在 owner shard 的 `RiskEngine` 及其两个调用点内。
