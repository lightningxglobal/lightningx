# 组合保证金（Portfolio Margin）实现方案 v4

状态：设计，未实现。日期：2026-09-15（v4：以多 desk / 多引擎 / 跨服务器拓扑为主线重写）。
相关：`docs/portfolio_margin_design.md`（quota lease 草案，本文 §A6 给出结论）、`docs/plans/counter-sharding-and-route-generation.md`（owner shard 与 route 轮换）、`docs/plans/2026-06-06-ha-replay-model.md`（HA）。
参考：一份面向订单簿型 perp DEX 的风控引擎设计文档（情景法组合保证金）。§B 的模型吸收了它的内核思想；§A 的架构是按本系统的拓扑独立推导的，§D 列出取舍。

---

## 一句话

**账户的组合视图只有一个写者：owner desk（`user_id % 4`）。组合保证金的全部决策（前置边际检查、风控 tick、有序强平、ADL 保护）都在 owner desk 的 `RiskEngine` 内完成；撮合引擎保持纯撮合；desk 之间不复制账户状态。跨服务器只有四条数据流，每条都是已有的或一条定长小消息。**

---

# A. 架构

## A1. 拓扑与角色

```
 引擎主机 E1                    引擎主机 E2
 ┌──────────────────┐          ┌──────────────────┐
 │ exchange-engine  │          │ exchange-engine  │
 │  BTC_USDT spin   │          │  ETH_USDT spin   │      每 symbol 一个自旋线程，纯撮合
 │  (+ 其他 symbol) │          │  SOL_USDT spin   │      不知道余额、仓位、保证金
 └───▲──────────┬───┘          └───▲──────────┬───┘
     │orders    │fills/updates     │orders    │fills/updates
     │(Aeron UDP, 每 symbol 流)    │          │
 ════╪══════════╪══════════════════╪══════════╪════════ 跨主机边界
     │          ▼                  │          ▼
 desk 主机 D0 … D3（每台一个 desk-server，三个正交角色）
 ┌─────────────────────────────────────────────────────────────┐
 │ ① edge   : 终端 WS 会话（任意用户可连）                          │
 │ ② owner  : user_id % 4 == desk_id 的账户账本 + PM 风控（单写者）  │
 │ ③ product: 与本机引擎共处的产品线，负责该产品的 mark price 发布     │
 └─────────────────────────────────────────────────────────────┘
```

三个角色正交：edge 决定"谁接会话"，owner 决定"谁算风控"，product 决定"谁发 mark"。组合保证金只涉及 owner 角色。

**为什么 owner 是唯一正确的位置**：
- 组合跨 symbol、跨引擎，引擎里没有组合视图，也不能有（撮合线程百纳秒预算，且一个引擎只看一个 symbol）；
- owner 已经是账户余额、仓位、挂单冻结的单写者（counter-sharding 设计的核心不变式），PM 只是让它算的"需求量"从 Σ 变成组合最坏损失；
- 任何"独立风控服务"都会成为账户状态的第二个写者，或者在下单路径上多一跳。pg-writer 的教训（双写死锁）同样适用。

## A2. owner 需要的四条跨主机数据流

| # | 流 | 方向 | 现状 | PM 需要的改动 |
|---|---|---|---|---|
| 1 | 订单 / 强平单 | owner → 引擎(symbol) | `orders_stream_for_symbol`，每 symbol 一个流 | 无。但见 A3 端点表 |
| 2 | 成交 / 订单状态 | 引擎(symbol) → owner | `order_update_stream_for_desk(owner)`，引擎按提交方 desk 回流 | 无 |
| 3 | mark price（全 symbol） | product desk → 所有 desk | 每个 desk 自己订阅全量 `DEPTH_STREAM`，用 depth 中价 + index 钳制算 mark | **新增 `MarkPrice` 小流**（A4） |
| 4 | 持久化事件 | owner → pg-writer | `PERSIST_STREAM` | `RiskAccountSet` 加 1 字节 `margin_mode` |

没有第五条。desk 之间不同步仓位、不同步余额、不同步向量。

## A3. 前置条件：多引擎主机的端点表

`aeron_channels.rs` 现在所有通道都指向单一 `ENGINE_HOST`。引擎分布在多台主机时，owner 发 ETH 单要到 E2、发 BTC 单要到 E1，需要：

```
ENGINE_ENDPOINTS=BTC_USDT=10.0.0.1:20121,ETH_USDT=10.0.0.2:20121,SOL_USDT=10.0.0.2:20121
```

`orders_channel_for_symbol(symbol)` 按表解析；找不到的 symbol 走现有 fallback（`valid_symbols` 早拒）。反向流（引擎 → desk）在 UDP 模式下端点是 desk 主机，引擎侧同样需要 `DESK_ENDPOINTS=0=10.0.1.0:20122,…`。

这是**拓扑前置条件，与 PM 无关但 PM 依赖它**：没有它，"引擎在不同服务器"根本跑不起来，更谈不上跨引擎组合。建议作为第 0 步独立提交。

## A4. mark price：一个 symbol 一个发布者

现状每个 desk 各自从 depth 算 mark，四个 desk 对同一 symbol 的 mark 可能有微小差异，且跨主机全量 depth 流量大（每 symbol 每 10ms 一帧 50 档）。

改为：**product desk（与该 symbol 引擎同机）** 算 mark（现有逻辑：depth 中价 + `INDEX_SOURCES` 钳制），发布定长帧：

```
MarkPrice { symbol: [u8;16], mark_ticks: i64, index_ticks: i64, ts_ns: u64, seq: u32 }   // 48B
```

流：`MARK_STREAM = 7`，通道走引擎主机端点（product desk 与引擎同机，其他 desk 已在订阅该主机）。频率 10ms 或变动 > 1 tick 时。

收益：
- 一个 symbol 只有一个 mark 写者 → 所有 owner 对同一账户用相同的价格，强平可审计；
- 跨主机流量从 depth 帧降到 48B/symbol/10ms；
- 外部 index 源只需 product desk 接，其余 desk 不需要外部行情连接。

owner 侧 `update_mark_price` 的输入从 depth 回调换成 `MarkPrice` 帧；B5 的 `last_mark_update` 冻结检测保留，输入源换成 `MarkPrice.ts_ns`。

## A5. 下单路径（跨主机）

```
用户 ──WS──► edge desk ──(若非 owner: cf_cmd, UDP)──► owner desk ──orders(UDP)──► 引擎
                                                        ▲  边际 IM 检查（B3）           │
                                                        │                              │
用户 ◄──WS── edge desk ◄──(cf_resp, UDP)────────────── owner desk ◄──order_update(UDP)──┘
                                                           on_fill 重算向量（B4）
```

- **只有 owner 能算边际 IM**（需要账户向量）。转发帧 `CounterForwardOrderMeta` 已带 symbol / side / price / qty，owner 自算 `v_order`；帧里的 `initial_margin_atoms` 退化为 Σ 模式账户的独立 IM。
- 边际检查在 owner 的两个入口共用一个函数：本地 WS（tokio）与转发（`d{n}-send` 自旋线程）。
- 引擎不参与风控：前置是"闸门在前、一次单向发送"，不存在参考文档里"撮合 → 风控 → 撮合"的往返，所以**风控与撮合不需要同机**。desk 与引擎分机的代价只是订单路径上本来就有的那一跳 UDP。

## A6. 去掉 edge → owner 那一跳：owner 亲和连接

40K 压测：转发路径比直连 p50 慢 20-25µs，p99 从 1.5ms 涨到 21-27ms。这一跳是当前系统里唯一"可以不存在"的跨主机跳。

**方案：登录时把会话引导到 owner desk。** 登录响应携带 `owner_desk_url`（`api_key → user_id → user_id % 4`，一次解析），客户端重连；或网关按认证后的 `user_id % 4` 头路由。之后 `ingress == owner`，转发路径只剩兜底（客户端不配合重连时仍能用）。

为什么可行：私有数据（成交、余额、仓位推送）本来就由 owner 产生并经 edge 中转；公开行情走 market-data-gateway，与连接在哪个 desk 无关。产品线亲和的私有连接没有任何收益。

**quota lease 的结论**：它是为了在不能做亲和路由时去掉这一跳。有了亲和路由，它不再需要。保留草案作为 plan B（例如异地 LB 无法按用户路由时），且 PM 下柜台本地只能按独立 IM 扣租约、owner 在下个 tick 返还对冲差额（细节见旧草案 + 本文 v3 §9，不再展开）。

## A7. 强平跨引擎

owner 的风控 tick 一个用户每 tick 只发一组动作（B5）。对冲对成对减仓 = 两张 IOC 发往两个引擎（可能两台主机），**天然非原子**。处理：

1. 两张 IOC 在同一 tick 内连续发出（同一自旋线程，间隔纳秒级）；
2. 账户置 `Liquidating` 并记 `liq_inflight_until = now + LIQ_INFLIGHT_MS`（建议 50ms），到期或两腿回执齐前不再发下一组；
3. 回执到齐后 `on_fill` 重算向量，下个 tick 重评：一腿成交一腿未成交时，剩下的单腿 Δmm 最大，自然成为下一组动作；
4. 某 symbol mark 冻结（B5 现有守卫）→ 该 RU 的腿不参与候选，其他 RU 继续。

强平单绕过前置闸门（现有 `fill_margin = 0`、`liq_price_ticks != 0`），走与用户订单相同的 `orders_stream_for_symbol`，所以 A3 的端点表对它同样生效。

## A8. 故障与降级（按部件）

| 故障 | 表现 | 处理 | PM 特有的部分 |
|---|---|---|---|
| 引擎主机 E2 宕机 / 链路分区 | ETH/SOL 的 mark 停更、订单发布失败 | 现有：B5 冻结该 symbol 强平；发布失败拒单 | **冻结该 RU**：该 RU 上的风险上升单拒绝；持仓按最后 mark 估值且冲击放大（`p × PM_STALE_MULT`，建议 2×），组合需求量保守上浮；其他 RU 正常 |
| owner desk D1 宕机 | 该 shard 用户不能交易（现状） | 重启后 hydration：仓位 + 挂单 → 重算向量（纯函数，不持久化派生量） | hydration 未完成前拒绝所有单（S1.4 守卫现有）；完成后先跑一次全量 tick 再开闸 |
| product desk 宕机（mark 发布者） | 该产品 mark 停更 | 同引擎宕机的 RU 冻结路径 | 可选：owner 退回 depth 自算 mark 作为兜底（保留旧代码路径一个版本） |
| edge desk 宕机 | 连在它上面的会话断线 | 客户端重连到 owner（A6） | 无 |
| owner 主备切换（HA） | 新 owner 接管 | 需要 epoch 防护栏，避免旧 owner 继续向引擎发单 | 与 HA 重放模型同一课题，不在 PM 范围内；PM 不引入新的状态副本，不加重这个问题 |

原则与参考文档一致：信息可疑时冻结、加厚、拒绝风险上升，不做不可逆动作；但本系统没有"检测快、执行慢"的不对称（执行是 µs 级 Aeron），不需要待强平队列。

## A9. 否决的方案

| 方案 | 为什么不 |
|---|---|
| PM 放进撮合引擎 | 引擎只看一个 symbol，没有组合；自旋线程预算百纳秒 |
| 独立 Global Risk Coordinator 进程 | 下单路径多一跳；成为账户状态第二写者或需要状态复制；owner 已经是这个角色 |
| 把仓位/余额复制到所有 desk | 解决"看见"解决不了"互斥"，双花仍需仲裁点；写放大 ×4 |
| 风控与撮合同机（参考文档的要求） | 它的前提是撮合→风控→撮合往返；本系统是闸门在前的单向发送，不存在往返 |
| quota lease 作为主路径 | 只解决 edge→owner 一跳；owner 亲和连接更便宜地解决同一问题，且不引入租约状态 |
| 每 desk 自算 mark（现状） | 同一账户可能在不同 desk 上看到不同 mark；跨主机 depth 流量大 |

---

# B. 保证金模型：情景重估内核

## B1. 为什么不用"净额 + 配对折扣表"

配对折扣在三个以上风险单元时需要额外分配规则；IM、MM、前置检查、强平排序各自维护一套折扣逻辑，容易漂移；"相关性失效"只能靠调低参数。情景法：定义少量确定性情景，把整个组合在每个情景下重估一次取最坏损失。对冲收益是"多空腿在同一情景下同时重估"的自然结果；三个以上单元没有特殊处理；相关性失效就是一个情景参数。IM、MM、前置边际、强平排序、ADL 保护共用同一个内核函数。

## B2. 因子与情景集

- **因子** = 风险单元 RU = base asset（`BTC_USDT` → `BTC`），复用 `counter_shard::symbol_route_desk_id` 的 base 解析。同 RU 多合约自动净额。
- 线性 perp 单腿损失是闭式：`loss_i(s) = signed_notional_i × shock_i(s)`，`signed_notional = ±qty × mark / notional_scale`。不需要 `value(state)` trait。
- **情景生成**（k = 持仓 RU 数 ≤ 4，S = 2 × 2^k ≤ 32）：主方向 D ∈ {−,+}；背离子集 X ⊆ RUs，X 内的 RU 反向移动、幅度为背离冲击 `d_i`，其余沿主方向移动、幅度为全额冲击 `p_i`。
- **两套冲击网格，同一内核**：
  - MM 网格 `p_i = maintenance_rate_bps`（现值 50）；
  - IM 网格 `p_i = 1e4 / default_leverage`（现值 1000）。
  - 单仓位账户的组合 MM/IM 与今天**逐 atom 一致**，只有多腿账户出现差异，便于灰度对比。
- **背离冲击** `d_i = p_i × div_bps / 1e4`：多 BTC / 空 ETH 的最坏情景是"BTC 跌 p、ETH 涨 d"，对冲折扣 = `1 − d/p`；同向持仓的最坏情景是"全部同跌 p"，**没有分散收益**（危机中相关性趋于 1，分散不可靠）。相关性失效压力测试 = `div_bps → 1e4`。
- **聚合**：取最坏情景（`max`）。

## B3. 内核接口与可加性

```rust
// calc.rs — 纯函数，全部定点数（atoms / bps / i128 中间量）
pub const MAX_SCENARIOS: usize = 32;
pub type LossVec = SmallVec<[i64; MAX_SCENARIOS]>;   // 每情景组合损失，正数 = 亏
pub struct RuShock { pub ru: [u8; 8], pub p_bps: i64, pub d_bps: i64 }
pub fn leg_loss_vec(signed_notional_atoms: i64, ru_index: usize, shocks: &[RuShock]) -> LossVec;
pub fn worst(losses: &LossVec) -> i64;   // max(0, max_s losses[s])
```

**损失向量对腿线性可加**：加一张单、减一个 tranche、撤一张挂单都是向量加减，O(S)。这是热路径常数时间的来源。

## B4. 账户缓存与热路径

```rust
// AccountRiskState 新增（只在 get_mut 下写，与 used_margin 同规则）
pub margin_mode: u8,                       // 0 = cross-sum（现状），1 = portfolio
pub ru_order:   SmallVec<[[u8; 8]; 4]>,
pub pos_losses_im: LossVec,  pub pos_losses_mm: LossVec,
pub pending_losses_im: LossVec,            // 挂单未成交部分
pub position_symbols: SmallVec<[[u8; 16]; 4]>,
```

下单检查（owner 两个入口共用）：

```
v_order    = leg_loss_vec(±order_notional, ru, im_grid)
req_before = worst(pos_im + pending_im)
req_after  = worst(pos_im + pending_im + v_order)
marginal   = req_after − req_before
```

- `marginal ≤ 0`（平仓、减仓、对冲）→ 不设 IM 闸门，冻结 0，`MarginCall` 也放行。判断依据是需求量变化，不是 buy/sell 或 reduce-only 标签。
- `marginal > 0` → 现有 `available_margin` 原子 CAS 冻结 `marginal`。
- 通过后 `pending_losses_im += v_order`；成交/撤单时减去对应部分。**挂单进向量**是为了让两笔并发对冲单只有第一笔拿到折扣。
- 成本：三次 O(S) 向量运算，百纳秒级。多出的 `get_mut` 若在自旋路径上有争用，`pending_losses_im` 改为 `[AtomicI64; 32]` 用 `fetch_add`。
- RU 冻结（A8）：`v_order` 所在 RU 冻结且 `marginal > 0` → 拒单 `reason=ru_frozen`。

字段语义映射：`used_margin = worst(pos_im)`，`maintenance_margin = worst(pos_mm)`，`order_margin = Σ 边际冻结`；`equity = available + order + used + unrealized` 守恒保持，S2.4 守恒测试继续有效。

## B5. 慢路径

- **`on_fill`**（已持 `get_mut`）：`pending_im −= v_order × fill/qty`；重算该 RU 腿（旧减新加，两套网格）；写 `used_margin` / `maintenance_margin`；`available_margin` 校正为真实值，吸收冻结值与真实值的漂移（与 `runtime_consume_fill_margin` 差额回冲合并）。新 RU 且 `ru_order` 已满 → 账户降级 Σ 模式并计 metrics，不拒单。
- **`update_mark_price`**（输入换成 `MarkPrice` 帧）：该 RU 腿按 `notional_new − notional_old` 缩放加回，O(S)。是 `get_mut(acct)` + 键控 `positions.get`，不触发 DashMap 嵌套迭代死锁。若压测需要，加价格带触发（累计变动 < 10bps 不重算）。
- **`run_risk_tick`** 三段式不动；PM 账户只是 `maintenance_margin` 来源不同。`symbol_position_index` 就是"因子 → 组合"倒排索引。

## B6. 有序强平 + 收敛检查（替换 Pass 2）

1. 候选动作：单腿减一个 tranche；对冲对（当前最坏情景下损失符号相反的两腿）按净敞口比例同时减一个 tranche。冻结 RU 的腿不参与。
2. 每个候选算 `Δmm = worst(V_mm) − worst(V_mm − v_action)`，O(S)，≤ 6 个候选。
3. 按 `Δmm / 释放名义额` 降序执行第一条；跨引擎发送与 in-flight 守卫见 A7。
4. 收敛检查：所有候选 `Δmm ≤ 0` → 自级联信号 → 全部腿按比例整体减仓。
5. `equity > maintenance_margin × (1 + LIQ_STOP_BUFFER_BPS)` → 回 `Normal`；否则下个 tick 继续。

**ADL 保护**：`run_adl` 削对手方前算 `worst(V_cp − v_cut)`，若会让对手方自己 `equity ≤ mm`（削的是它的对冲腿）→ 降权或跳过。现有评分保留。

## B7. 持久化与恢复

不持久化派生量（向量、`used_margin`、`maintenance_margin` 都是 `(positions, open_orders, marks, params)` 的纯函数）。持久化 `margin_mode`（`RiskAccountSetPayload` 占 1 字节 `_pad`）与冲击参数版本号（可后置）。hydration 后从仓位 + 挂单重算；`reconcile.rs` 加一项一致性校验。

## B8. 参数

| 参数 | 来源 | 现值 / 建议 |
|---|---|---|
| MM 网格 `p_i` | `SymbolRules.maintenance_rate_bps` | 50 |
| IM 网格 `p_i` | `1e4 / SymbolRules.default_leverage` | 1000 |
| 背离比例 | env `PM_DIV_BPS=BTC:4000,ETH:4000,SOL:6000` | 待标定；1e4 = 无折扣 |
| RU 冻结冲击放大 | env `PM_STALE_MULT_BPS` | 20000 |
| 单账户 RU 上限 | 常量 `MAX_PM_RUS = 4` | 超出降级 Σ |
| 强平 in-flight | env `LIQ_INFLIGHT_MS` | 50 |
| 强平停止缓冲 | env `LIQ_STOP_BUFFER_BPS` | 1000 |
| 账户模式 | `risk_accounts.margin_mode` | 默认 0 |

`div_bps` 标定：历史 1s/10s 窗口内两 RU 收益率之差的高分位除以 `p_i`，需要行情数据，属上线前标定。

---

# C. 落地顺序（每步独立验证、独立提交）

| 步 | 改动 | 验证 |
|---|---|---|
| 0a | 多引擎端点表 `ENGINE_ENDPOINTS` / `DESK_ENDPOINTS`（A3） | UDP 模式两台引擎主机各跑一个 symbol，desk 双向收发 |
| 0b | `MarkPrice` 流（A4）：product desk 发布，所有 desk 订阅；owner 的 `update_mark_price` 换输入 | 四个 desk 对同一 symbol 的 mark 逐 tick 相等；B5 冻结检测仍生效 |
| 0c | owner 亲和连接（A6）：登录响应带 `owner_desk_url`，客户端重连 | 40K 压测 forward 占比 → ~0；p50/p99 与 no-forward 基线一致 |
| 1 | `calc.rs` 内核纯函数（B3） | 单仓 MM/IM 与现公式逐 atom 相等；对冲折扣 = `1 − d/p`；同向无折扣；`div=1e4` 归零；可加性；i128 无溢出 |
| 2 | 账户新字段；`on_fill` / `update_mark_price` 维护向量（B4/B5） | 对冲组合 Σ 模式 `LiquidationPending`、PM 模式 `Normal`；mark 变动后向量与重算一致；40K×2 tick 不回归 |
| 3 | 前置边际 IM（三个调用点）+ `pending_losses_im` + RU 冻结拒单 | 对冲单冻结 = 边际；平仓单 `MarginCall` 放行；并发对冲单只有第一笔拿折扣；冻结 RU 拒风险上升单、放行减仓单 |
| 4 | 有序强平 + 成对减仓 + 收敛检查 + in-flight 守卫 + ADL 保护（B6/A7） | 对冲对成对 IOC 到两个引擎；一腿未成交时下一组动作正确；越平越糟时整体减仓；回安全后停止 |
| 5 | `margin_mode` 持久化 / hydration / reconcile（B7） | 重启后 PM 账户 MM 与重启前一致 |

0a/0b/0c 三步彼此独立、与 PM 独立，可以并行或提前做；1-3 完成即可灰度"对冲少冻 + 组合 MM"；4-5 是安全闭环，上线前必须完成。

---

# D. 参考文档的取舍

采纳：一个情景内核、对冲收益涌现（B1-B3）；前置按 `req_after vs req_before` 判方向、降风险单不设闸门（B4）；相关性失效 = 参数（B2）；强平按边际贡献排序、成对解除、收敛检查（B6）；ADL 不削对手方对冲腿（B6）；因子冻结而非猜价（A8）；IM 用更保守网格而非 MM × 系数（B2）。

不采纳及原因：`Instrument` trait / `Axis` 枚举（只有线性 perp）；500K 组合的 SoA / SIMD 预筛 / 自适应频率（40K + DashMap + 10ms tick 已够，倒排索引已有）；多币种抵押、抵押品瀑布、RWA、预测市场（范围外）；风控与撮合同机（本系统无往返，A5/A9）；"检测快执行慢"的待强平队列（执行是 µs 级）；L2 / 排序器 / 预言机聚合章节（已有 index 冻结 + mark 钳制）；ES 分位数（S ≤ 32 取 max）。强平结算价政策（破产价接管 vs 按实际成交退还）是它提出的好问题，但属独立产品决策，现状为接管制，不在 PM 范围内。

---

# E. 假设（未与需求方确认，按此实现）

1. 单一 USDT 抵押品。
2. 引擎按 symbol 分布在若干主机，一个 symbol 只在一个引擎；desk 与引擎可不同机。
3. RU = base asset；情景集为"主方向 × 背离子集"，背离幅度按 RU 配置。
4. PM 为账户级 opt-in；单仓 PM 账户与现状逐 atom 一致。
5. 单账户最多 4 个 RU 参与情景集，超出降级 Σ 模式。
6. 挂单未成交部分计入 IM 向量；降风险单不设闸门，但 `Liquidating` 及之后状态仍拒绝用户订单。
7. 一个 symbol 的 mark 只有一个发布者（product desk）。
8. 不引入新二进制、新线程；新增一条 Aeron 小流（MarkPrice）与两张端点表；所有风控改动在 owner 的 `RiskEngine` 及其三个调用点内。
