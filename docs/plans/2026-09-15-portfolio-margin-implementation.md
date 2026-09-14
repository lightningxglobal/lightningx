# 组合保证金（Portfolio Margin）实现方案 v3

状态：设计，未实现。日期：2026-09-15（v3 同日修订：保证金模型改为情景重估内核）。
前置文档：`docs/portfolio_margin_design.md`（quota lease 协议草案，本文将其定位为独立的延迟优化轨道）。
参考：一份面向订单簿型 perp DEX 的风控引擎设计文档（情景法组合保证金 + 强平/ADL/保险基金全链路）。本文 §2、§3、§5 吸收了其中与本仓库相关的部分，§10 列出取舍。

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
| 维持保证金 MM | 账户 `maintenance_margin = Σ 各仓位 notional × rate` 线性相加 | 组合在**同一情景**下重估的最坏损失 → 对冲收益自然涌现 |
| 前置初始保证金 IM | `notional / leverage`，与已有仓位无关；按 side 标签判断 reduce-only | 边际 IM = 加单后与加单前的组合 IM 之差；风险方向按需求量变化判断，不按标签 |
| 强平 | 全部仓位一起 IOC（分批 `LIQ_TRANCHE_BPS`） | 按"每单位释放名义额降低多少需求量"排序逐仓减仓，对冲对成对处理，含收敛检查 |
| 数据 | `symbol → users` 索引，无 `user → symbols` | 账户级缓存一个情景损失向量，多数路径不再遍历仓位 |

抵押品仍是单一 USDT（多币种抵押是另一个模块，本文不涉及）。

---

## 2. 保证金模型：情景重估内核

### 2.1 为什么不用"净额 + 配对折扣表"（v2 的做法）

v2 用同风险单元净额 + 跨单元 corr 配对表算折扣。问题：
- 折扣是**成对**定义的，三个以上单元时"谁和谁抵"需要额外的分配规则；
- IM 与 MM、前置检查与强平排序各自实现一套折扣逻辑，容易漂移；
- 无法表达"相关性失效"这类尾部情形，只能靠调低 corr 参数。

情景法：定义少量确定性情景，把整个组合在每个情景下重估一次，取最坏损失。对冲收益不用算，它是"多空腿在同一情景下同时重估"的自然结果；三个以上单元没有特殊处理；"相关性失效"就是多加一个情景。IM、MM、前置边际检查、强平排序、ADL 保护共用同一个内核函数。

### 2.2 因子与情景集

- **因子** = 风险单元 RU = base asset（`BTC_USDT` → `BTC`），复用 `counter_shard::symbol_route_desk_id` 的 base 解析。当前一个 RU 一个 symbol；同 RU 多合约自动净额。
- 线性 perp 的单腿损失是闭式：`loss_i(s) = signed_notional_i × shock_i(s)`，`signed_notional = ±qty × mark / notional_scale`（多为正）。不需要 `value(state)` trait，`Instrument` 抽象在只有线性 perp 的仓库里是多余的。
- **情景生成**（k = 用户持仓 RU 数，≤ 4 时 S ≤ 32）：
  - 主方向 D ∈ {−, +}；
  - 背离子集 X ⊆ RUs：X 中的 RU 反向移动、幅度为背离冲击 `d_i`，其余 RU 沿主方向移动、幅度为全额冲击 `p_i`；
  - 情景数 = 2 × 2^k。k=1 时退化为 ±p（加两个无害的 ∓d）。
- **两套冲击网格，同一内核**：
  - MM 网格：`p_i = maintenance_rate_bps`（现值 50）。单仓位账户的组合 MM = `notional × 50bps`，与今天**完全一致**；
  - IM 网格：`p_i = 1e4 / default_leverage`（现值 1000）。单仓位账户的组合 IM = `notional / 10`，与今天一致。
  - 这样非 PM 账户和 PM 单仓账户数值不变，只有多腿账户出现差异，便于灰度对比。
- **背离冲击** `d_i = p_i × div_bps / 1e4`，`div_bps` 是每 RU 的参数（BTC/ETH 可设 4000 = 40%）。含义：
  - 多 BTC / 空 ETH 对冲对，最坏情景是"BTC 跌 p、ETH 涨 d"，两腿都亏但 ETH 腿只亏 d，对冲折扣 = `1 − d/p`；
  - 同向持仓（多 BTC + 多 ETH），最坏情景是"全部同跌 p"，**没有分散收益**。这是有意的保守：危机中相关性趋于 1，分散收益不可靠；
  - 相关性失效压力测试 = 把 `div_bps` 调到 1e4，折扣归零。参数化即可，不需要改代码。
- **聚合**：v1 取最坏情景（`max`）。ES 分位数在 S ≤ 32 时意义不大，后置。

### 2.3 内核接口

```rust
// calc.rs — 纯函数，全部定点数（atoms / bps / i128 中间量）
pub const MAX_SCENARIOS: usize = 32;
pub type LossVec = SmallVec<[i64; MAX_SCENARIOS]>;   // 每情景组合损失，正数 = 亏

pub struct RuShock { pub ru: [u8; 8], pub p_bps: i64, pub d_bps: i64 }

/// 单腿在每个情景下的损失向量。losses 可加：多腿向量逐元素相加即组合向量。
pub fn leg_loss_vec(signed_notional_atoms: i64, ru_index: usize, shocks: &[RuShock]) -> LossVec;
/// 最坏情景损失 = 需求量（MM 或 IM，取决于传入的网格）。
pub fn worst(losses: &LossVec) -> i64;   // max(0, max_s losses[s])
```

**关键性质：损失向量对腿线性可加**。加一张单、减一个 tranche、撤一张挂单，都是向量加减，O(S)，不需要重新遍历仓位。这就是热路径 O(1)（常数 S ≤ 32）的来源，替代了 v2 的 `hedge_credit` 预计算表。

情景集的 RU 顺序对每个账户固定（按首次持仓顺序追加，最多 4 个）；账户持仓超过 4 个 RU 时退回 Σ 模式（配置上限，压测后再放）。

---

## 3. 热路径：边际 IM

### 3.1 账户缓存

```rust
// AccountRiskState 新增（只在 get_mut 下写，与 used_margin 同规则）
pub margin_mode: u8,                       // 0 = cross-sum（现状），1 = portfolio
pub ru_order:   SmallVec<[[u8; 8]; 4]>,    // 该账户情景集的 RU 顺序
pub pos_losses_im: LossVec,                // 仓位在 IM 网格下的损失向量
pub pos_losses_mm: LossVec,                // 仓位在 MM 网格下的损失向量
pub pending_losses_im: LossVec,            // 挂单（未成交部分）在 IM 网格下的损失向量
pub position_symbols: SmallVec<[[u8; 16]; 4]>,
```

`maintenance_margin` 字段保留，PM 账户下由写入方写入 `worst(pos_losses_mm)`；`run_risk_tick` 不感知模式。

### 3.2 下单检查（owner 侧 `cf_owner_new` 与本地 `ws_handler` 分支共用）

```
v_order   = leg_loss_vec(±order_notional, ru, im_grid)
req_before = worst(pos_losses_im + pending_losses_im)
req_after  = worst(pos_losses_im + pending_losses_im + v_order)
marginal   = req_after − req_before
```

- `marginal ≤ 0`（风险下降：平仓、减仓、对冲）→ **不设 IM 闸门**，冻结 0，`MarginCall` 状态也放行。判断依据是需求量的变化，不是 buy/sell 或 reduce-only 标签（多头的卖是平仓，空头的卖是加仓，标签会骗人）。现有 `check_reduce_only` 保留给显式 reduce-only 订单的语义检查，不再用于风控放行。
- `marginal > 0` → 走现有 `available_margin` 原子 CAS 冻结 `marginal`。
- 通过后 `pending_losses_im += v_order`（get_mut 写；成交/撤单时减去对应部分）。

**挂单为什么进向量**：两笔并发对冲单若都只对"已有仓位"算边际，会重复享受同一份对冲折扣。把挂单的未成交部分计入向量后，第二笔看到的是"仓位 + 第一笔挂单"的组合，折扣只发一次。v2 承认了这个窗口，v3 把它关掉。

**成本**：三次 O(S) 向量运算 + 一次 `accounts.get()`，S ≤ 32 → 百纳秒级。但比今天多了一次 `get_mut`（写 `pending_losses_im`）。若压测显示 get_mut 在 owner recv-spin 路径上有争用，退化方案：`pending_losses_im` 改为 `[AtomicI64; 32]` 用 `fetch_add` 写，与 `available_margin` 同款。

**边际的时效**：下单时冻结的 `marginal` 基于当时的 mark 和仓位；成交时 `on_fill` 按真实向量重算 `im_total = worst(pos + pending)`，并把 `available_margin` 校正为 `equity − im_total − …`，吸收冻结值与真实值的漂移。这一步与现有 `runtime_consume_fill_margin` 的差额回冲路径合并。

### 3.3 与现有字段的关系

| 字段 | 现状语义 | PM 语义 |
|---|---|---|
| `order_margin` | Σ 挂单冻结 | Σ 挂单边际冻结（可能小于独立 IM 之和） |
| `used_margin` | Σ 仓位独立 IM | `worst(pos_losses_im)` |
| `maintenance_margin` | Σ 仓位 MM | `worst(pos_losses_mm)` |
| `available_margin` | equity − order − used | 不变，slow path 重算时用 PM 值 |

`equity = available + order_margin + used_margin + unrealized` 的守恒关系保持，S2.4 守恒测试继续有效。

---

## 4. 慢路径：向量维护与风控 tick

### 4.1 `on_fill`

已持有 `accounts.get_mut`。追加：
1. `pending_losses_im −= v_order × fill_qty / order_qty`；
2. 仓位变化 → 重算该 RU 腿的向量（旧腿减、新腿加，两套网格）；
3. `used_margin = worst(pos_losses_im)`，`maintenance_margin = worst(pos_losses_mm)`；
4. `available_margin` 校正（§3.2）。

新 RU 首次持仓且 `ru_order.len() == 4` → 该账户降级为 Σ 模式（`margin_mode = 0`）并记 metrics，不拒单。

### 4.2 `update_mark_price`

已按 `symbol_position_index` 遍历该 symbol 的用户并 `get_mut` 账户。追加：该 RU 腿的向量按 `notional_new − notional_old` 缩放后加回两套向量，O(S)。40K 用户 × 32 次乘加远小于已有的 DashMap 开销。如果后续压测证明不够，加价格带触发（累计 mark 变动 < 10bps 不重算）。

⚠️ 这里是 `get_mut(acct)` + 键控 `positions.get`，不是 `iter_mut` 内套 `iter`，不触发 DashMap 6.x 嵌套迭代死锁（见 `margin_liquidation_impl` 记录）。测试里保留 40K 用户 × 2 仓位的 tick 时长断言。

### 4.3 `run_risk_tick` 不改结构

三段式不动。阈值判断、`MarginCall` 判定、B2 hydration 保护全部复用；PM 账户只是 `maintenance_margin` 数值来源不同。`symbol_position_index` 本身就是"因子 → 组合"倒排索引，只唤醒有仓位的用户；账户级 `equity vs maintenance_margin` 的比较就是线性预筛。40K 规模不需要 SIMD 预筛/自适应频率那一层。

### 4.4 账户模式

`margin_mode` opt-in，默认 0。来源：`risk_accounts` 表新列 + `RiskAccountSetPayload` 占用 1 字节 `_pad`，hydration 读回。两种模式在同一 tick 内并存。

---

## 5. 组合强平：有序减仓 + 收敛检查

现状：`LiquidationPending` → 对该用户**每个仓位**发 `LiquidationEvent` → 各自分批 IOC。组合保证金下这是错的：先平掉对冲对的一条腿，组合需求量**上升**（对冲腿被拆，剩下裸头寸）。

新规则（在 `run_risk_tick` Pass 2 替换 `positions.iter().filter()`）：

1. 取该用户全部仓位腿，候选动作：
   - 单腿减一个 tranche（`liquidation_tranche_lots`）；
   - 对冲对（在当前最坏情景下损失符号相反的两腿）按净敞口比例**同时**减一个 tranche。
2. 对每个候选算 `Δmm = worst(V_mm) − worst(V_mm − v_action)`，O(S)，≤ 6 个候选。
3. 按 `Δmm / 释放名义额` 降序，执行第一条（一个 tick 只发一组 IOC）。
4. **收敛检查**：所有候选 `Δmm ≤ 0`（越平越糟）→ 自级联信号 → 跳过单腿，改为全部腿按比例整体减仓（保持净敞口中性）。
5. 成交后 `on_fill` 重算，下个 tick 重评：`equity > maintenance_margin × (1 + buffer_bps)` → 状态回 `Normal`；否则继续。

入口不变：市价 IOC 走 `AeronCmd::NewOrder`，`liq_price_ticks != 0` 标记，`fill_margin = 0`，绕过前置闸门（强平单本来就是降风险单，走闸门会因账户已穿仓被拒，死锁）。保险基金结算逻辑不动。

**ADL 保护**（`run_adl` 小改）：对手方候选在被削仓前算 `worst(V_cp − v_cut)`，若削仓会让对手方自己 `equity ≤ mm`（削掉的是它的对冲腿），降权或跳过。今天的 ADL 评分（uPnL 占比 × 杠杆）保留。

---

## 6. 持久化与恢复

- **不持久化派生量**。损失向量、`used_margin`、`maintenance_margin` 都是 `(positions, marks, params)` 的纯函数，hydration 读回仓位后重算。
- 持久化的只有 `margin_mode`（`RiskAccountSetPayload` 加 1 字节，pg-writer 加列）和冲击参数版本号（审计用，`PersistEvent` 一条 `PmParamsSet`，可后置）。
- 对账（`reconcile.rs`）加一项：从仓位重算向量与内存值一致。

---

## 7. 参数

| 参数 | 来源 | 现值 / 建议 |
|---|---|---|
| MM 网格 `p_i` | `SymbolRules.maintenance_rate_bps` | 50（不变） |
| IM 网格 `p_i` | `1e4 / SymbolRules.default_leverage` | 1000（不变） |
| 背离比例 `div_bps` | 新增 env `PM_DIV_BPS=BTC:4000,ETH:4000,SOL:6000` | 待标定；1e4 = 无对冲折扣 |
| 单账户 RU 上限 | 常量 `MAX_PM_RUS = 4` | 超出降级 Σ 模式 |
| 强平停止缓冲 | 新增 env `LIQ_STOP_BUFFER_BPS` | 建议 1000 |
| 账户模式 | `risk_accounts.margin_mode` | 默认 0 |

`div_bps` 的标定方向：取历史 1 秒/10 秒窗口内两 RU 收益率之差的高分位，除以 `p_i`。这需要行情数据，属于上线前的标定工作，不是设计问题。

---

## 8. 落地顺序（每步可独立验证与提交）

| 步 | 改动 | 验证 |
|---|---|---|
| 1 | `calc.rs`：`RuShock`、`gen_scenarios`、`leg_loss_vec`、`worst`、`parse_pm_div` 纯函数 | 单测：单仓 MM/IM 与现公式逐 atom 相等；多 BTC 空 ETH 折扣 = `1 − d/p`；多 BTC 多 ETH 无折扣；`div=1e4` 折扣归零；向量可加性；i128 无溢出 |
| 2 | `AccountRiskState` 新字段；`on_fill` / `update_mark_price` 维护向量并写 `used_margin` / `maintenance_margin` | `engine.rs` 测试：对冲组合在 Σ 模式下 `LiquidationPending`、PM 模式下 `Normal`；mark 变动后向量与从仓位重算一致；40K × 2 tick 耗时不回归 |
| 3 | 前置检查改边际 IM（三个调用点：`ws_handler.rs` 本地、`desk_server.rs` cf_owner_new、batch 分支）；`pending_losses_im` 维护 | 测试：对冲单冻结 = 边际；平仓单在 `MarginCall` 下放行；两笔并发对冲单只有第一笔拿折扣；撤单后向量归零 |
| 4 | 强平排序 + 对冲对成对减仓 + 收敛检查（§5）；ADL 保护 | 测试：对冲对只发成对 IOC；`Δmm` 最大者先平；越平越糟时整体减仓；组合回安全后停止 |
| 5 | `margin_mode` 持久化 / hydration / reconcile | e2e：重启后 PM 账户 MM 与重启前一致 |

步骤 1-3 完成即可上线"对冲少冻 + 组合 MM"；4-5 是安全闭环，上线前必须完成。

---

## 9. 第二轨道：quota lease 与 PM 的衔接

`docs/portfolio_margin_design.md` 的租约协议目标是去掉 ingress → owner 的转发跳。它与 PM 正交，但 PM 改变了它的一个前提：**边际 IM 不可加**，接入柜台没有组合视图，算不出对冲折扣。

衔接规则：柜台本地闸口按**独立 IM（无折扣）**扣租约，保守放行；owner（home）收到成交流后按真实向量重算，差额在下个 tick 通过 `LeaseGrant` 增量返还。对冲收益从"下单时少冻"变成"成交后 ≤10ms 回补"，不变式 `Σ 租约 ≤ F × (1 − haircut)` 仍然成立。

是否做第二轨道的判据：在 Linux 物理机上复测 forward 路径的 p99。若 p99 在目标内，租约协议不做。

---

## 10. 参考文档的取舍

采纳（已写入上文）：

| 参考文档要点 | 落到本仓库 |
|---|---|
| 一个情景内核，对冲收益涌现而非配对表 | §2 情景集 + 可加损失向量 |
| 前置检查用 `req_after vs req_before` 判风险方向，降风险单不设 IM 闸门 | §3.2 |
| 相关性失效 = 情景参数，不是特殊代码 | `div_bps → 1e4` |
| 强平按边际贡献排序、尊重对冲、成对解除、收敛检查 | §5 |
| ADL 不能把对手方自己的对冲腿削掉 | §5 ADL 保护 |
| 因子倒排索引 + 只唤醒受影响组合 | 已有 `symbol_position_index`，无需新做 |
| 强平单绕过闸门 | 已有（`fill_margin = 0`） |
| IM 用更保守的网格而非 MM × 系数 | §2.2 两套网格，数值与现状对齐 |

不采纳（与本仓库规模或范围不符）：

- `Instrument` trait / `value(state)` / `Axis` 枚举：只有线性 perp，闭式公式足够，抽象是负担。
- 500K 组合、SoA 列存、大页、SIMD 预筛、自适应重估频率：40K 用户 + DashMap + 10ms tick 已够；`update_mark_price` 的价格带触发作为备选。
- 多币种抵押、抵押品瀑布、RWA、预测市场跳变轴：抵押品单一 USDT，范围外。
- L2 / 排序器故障、预言机聚合：本仓库已有 index 冻结 + mark clamp（S4.2），与 PM 无关。
- 强平结算价政策（破产价接管 vs 按实际成交退还）：现状是接管制（`realized_pnl − bkrpt_pnl` 进保险基金），是独立的产品决策，不在 PM 范围内，但值得单独讨论。
- ES 分位数聚合：S ≤ 32 时取 max 即可。

---

## 11. 假设（未与需求方确认，按此实现）

1. 单一 USDT 抵押品。
2. RU = base asset；情景集为"主方向 × 背离子集"，背离幅度按 RU 配置，不估相关矩阵。
3. PM 是账户级 opt-in 模式，非 PM 账户行为完全不变；单仓 PM 账户数值与现状逐 atom 一致。
4. 单账户最多 4 个 RU 参与情景集，超出降级为 Σ 模式。
5. 挂单未成交部分计入 IM 向量；降风险单不设闸门但 `Liquidating` 及之后状态仍拒绝用户订单。
6. 不引入新二进制、新 Aeron 流、新线程；所有改动在 owner shard 的 `RiskEngine` 及其三个调用点内。
