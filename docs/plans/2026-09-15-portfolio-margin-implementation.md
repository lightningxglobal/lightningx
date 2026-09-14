# 组合保证金（Portfolio Margin）实现方案 v5

状态：设计，未实现。日期：2026-09-15。
v5 变化：确定 PM 账户归属（专属 PM owner desk，而非普通 desk 或独立 risk-engine 服务）；内核升级为产品无关（为现货 / RWA / 预测市场铺路）；增加账户目录、在线迁移、多产品路线。
已确认的前提：一个 symbol 只在一个引擎，一个引擎可有多个 symbol；mark price 只由 product desk 发布。
相关：`docs/portfolio_margin_design.md`（quota lease，plan B）、`docs/plans/counter-sharding-and-route-generation.md`、`docs/plans/2026-06-06-ha-replay-model.md`、`src/desk/leader.rs`（fencing epoch）。
参考：一份面向订单簿型 perp DEX 的风控引擎设计文档。内核思想（情景法、product-blind、按 Δ需求量判风险方向、强平自级联）来自它；账户分层、目录路由、在线迁移、相关块分解、拓扑与故障处理是按本系统推导的。§F 列取舍。

---

## 一句话

**PM 账户归专属的 PM owner desk（同一 desk-server 二进制、`DESK_ROLE=pm`、目录路由），普通账户仍走 `user_id % 4` 的普通 owner desk。两类 desk 共用一个产品无关的风控内核库；撮合引擎保持纯撮合；desk 之间不复制状态。账户可以在线从普通 desk 迁到 PM desk（模式切换），持仓保留、挂单撤销、秒级完成。**

---

# A. PM 账户归属

## A1. 三个候选方案

| 方案 | 下单热路径 | 写者 | 隔离 | 多产品演进 | 结论 |
|---|---|---|---|---|---|
| ① 普通 owner desk 加 `margin_mode` 标志（v4） | 本地，零跳 | 单写者 ✓ | ✗ PM 的重计算、代码变更、故障与 1/4 零售用户同进程 | ✗ RWA / 预测 / 多币种抵押的代码都要进零售 desk | 只适合 PM 只有 perp 且用户极少的阶段 |
| ② **专属 PM owner desk**（同一二进制、角色开关、目录路由） | 本地，零跳 | 单写者 ✓ | ✓ 独立进程 / 主机 / 发布节奏；零售 desk 不含 PM 代码路径 | ✓ 新产品的风控只进 PM 角色 | **推荐** |
| ③ 独立 risk-engine 服务，desk 每单去问它 | +1 跨机往返（50-200µs） | ✗ 账户状态出现第二写者，或 desk 与 risk-engine 之间要同步余额/冻结 | ✓ | ✓ | 热路径与一致性都输给 ②；它能做的 ② 都能做 |

③ 只有一种形态不输：risk-engine 自己持有账本、自己发单到引擎、自己回执给用户。那它就是 ② 换了个名字。所以问题不是"要不要独立 risk-engine"，而是"PM owner 用不用独立二进制"。答案：**先不用**。PM owner 必须做 desk 的全部事（WS 会话、账本、Aeron 发单、回执、持久化），独立二进制会复制 desk-server 80% 的代码；角色开关 + 把风控抽成库（`risk-core`）能得到同样的隔离。等 PM 侧代码量和发布节奏真的分化了，再拆二进制，届时库边界已经在。

## A2. 分层 owner

```
                       account directory（登录时解析一次，desk 内存缓存）
                       user_id ──► { home_kind: Normal | PM, desk_id, epoch }
                                    │
          ┌─────────────────────────┴──────────────────────────┐
          ▼                                                    ▼
  普通 owner desk 0..3                                  PM owner desk 8..（编号不进 modulo 环）
  home = user_id % 4                                    home = 目录指定
  margin_mode 固定 Σ（现状风控，不含 PM 代码路径）           risk-core 情景内核（C 部分）
  零售用户，10K+/desk                                    PM 用户，100-10K/desk，10-50 腿/户
  同时兼 edge + product 角色                             只做 owner（+ 自己用户的 edge）；不发 mark
```

- 两类 desk 从同一条 `MarkPrice` 流、同一组引擎 order_update 流取输入，向同一组引擎 orders 流发单，向同一 pg-writer 发 PersistEvent。引擎和 pg-writer 不知道 PM desk 的存在，只多了几个 `desk_id`。
- `counter_shard_for_desk_id(desk_id) = desk_id % 4` 这个环绕必须去掉：PM desk 的 `desk_id` 不属于 modulo 环。`is_user_owned_by_desk` 改为查目录。
- 订单 id slab 按 `DESK_ID` 预留（现有 `base + DESK_ID × 1e9`），PM desk 直接沿用。

## A3. 账户目录与路由

```rust
pub struct AccountHome { pub kind: HomeKind, pub desk_id: u16, pub epoch: u32 }
pub fn resolve_owner(user_id: i64, dir: &HomeDirectory) -> AccountHome
// dir miss → Normal, user_id % 4（零售默认路径不查表，不加延迟）
```

- 真相在 PG 表 `account_home(user_id, kind, desk_id, epoch)`，Redis L1 镜像。desk 启动时全量加载 PM 条目（量小），登录时按用户查一次，之后 `HomeSet` PersistEvent 广播增量（复用 `PERSIST_STREAM`，pg-writer 落库，desk 订阅更新缓存）。
- edge 转发用 `resolve_owner`；登录响应携带 `owner_desk_url`，客户端重连到 owner（v4 §A6 的亲和连接），转发只剩兜底。
- 引擎回执按提交 desk 回流（`publisher_id = desk_id`），与目录无关。

## A4. 模式切换 = 在线迁移（普通 ↔ PM）

对比：Binance PM 是独立账户类型，用户要把资产划转进去；OKX 统一账户切换保证金模式时要求无挂单且部分情形无持仓。本方案：**同一账户 id，持仓保留，只撤挂单，秒级完成**。

```
① 旧 owner：目录写 Migrating(epoch+1) → 该账户拒绝新单 → cancel-all → 等引擎回执 drain（≤ LIQ_INFLIGHT_MS 同款超时）
② 旧 owner：最终快照（余额、仓位、风控状态）同步写 Redis L1；发 HomeSet{user, new_desk, epoch} PersistEvent
③ 新 owner：收到 HomeSet → 从 Redis 读快照 hydrate 该账户 → 从仓位重算向量（纯函数）→ 跑一次该账户 tick → 目录置 Active
④ 客户端：旧 owner 回 `rehome{owner_desk_url}`，客户端重连；期间到旧 owner 的请求被拒 `reason=migrating`
```

- **fencing**：`epoch` 随目录变化递增，转发帧和 PersistEvent 带 epoch，接收方丢弃低 epoch（与 `leader.rs` 同一思路）。旧 owner 在 ③ 之后即使残留状态也无法发单。
- 为什么撤挂单：挂单的回执按提交 desk 回流，不撤就要旧 owner 代转发回执，引入第二条路径。模式切换是低频操作，撤单代价可接受；持仓是纯数据，直接搬。
- 同一协议也用于 PM desk 之间的负载再平衡和 PM desk 的计划内下线。

## A5. 与 Binance / OKX 的比较，以及能做得更好的点

| 维度 | Binance PM | OKX 统一账户 | 本方案 |
|---|---|---|---|
| 账户形态 | 独立账户类型，需划转 | 同一账户，模式切换有前置条件 | 同一账户，在线迁移，持仓保留 |
| 前置检查 | 中心风控，延迟不透明 | 中心风控 | owner 本地向量运算，~100ns，零跨机 |
| 保证金方法 | uniMMR（按币种 MM 表 + 折扣） | 情景压力测试（价格 × 波动网格） | 情景内核 + **相关块分解**（C3）：独立块需求量相加，无情景爆炸 |
| 可解释性 | 给 uniMMR 数值 | 公开方法论 | API 返回"哪个情景在约束、每腿 Δ需求量"（C6），用户能算出加一腿后的变化 |
| 部分故障 | 通常整账户/整市场限制 | 同 | **按风险单元冻结**：一个引擎主机宕机只冻结它的 RU，其他 RU 照常（B6） |
| 强平 | 按 uniMMR 排序减仓 | 同 | 边际贡献排序 + 对冲对成对解除 + 收敛检查（C5） |

"更好"主要体现在热路径延迟、在线切换、可解释性、局部故障。保证金方法论本身与 OKX 同路（情景法），差异在块分解与参数策略，不宣称模型更准。

---

# B. 拓扑与数据流

## B1. 角色

```
 引擎主机 E1: exchange-engine {BTC_USDT, …}     引擎主机 E2: exchange-engine {ETH_USDT, SOL_USDT, …}
     ▲ orders(symbol 流)  │ order_update(按 desk 流)      ▲                  │
 ════╪════════════════════╪═══════════════════════════════╪══════════════════╪═════ 跨主机
 desk 主机：
   普通 desk 0..3 = edge + owner(modulo) + product(与同机引擎的 symbol 发 MarkPrice)
   PM desk 8..    = owner(目录) + 自己用户的 edge
 pg-writer / market-data-gateway 不变
```

## B2. owner 需要的跨主机数据流

| # | 流 | 方向 | 现状 | 改动 |
|---|---|---|---|---|
| 1 | 订单 / 强平单 | owner → 引擎(symbol) | 每 symbol 一个流 | 端点表（B3） |
| 2 | 成交 / 状态 | 引擎 → owner(desk) | 按提交 desk 回流 | PM desk 只是新增 desk_id |
| 3 | mark price | product desk → 所有 desk | 各 desk 从 depth 自算 | **`MarkPrice` 小流**（B4） |
| 4 | 持久化 | owner → pg-writer | `PERSIST_STREAM` | `RiskAccountSet` +1 字节 `margin_mode`；新增 `HomeSet` |

## B3. 前置条件：多引擎主机端点表

`aeron_channels.rs` 现在所有通道指向单一 `ENGINE_HOST`。需要 `ENGINE_ENDPOINTS=BTC_USDT=10.0.0.1:20121,ETH_USDT=10.0.0.2:20121,…`（一个引擎多个 symbol 时多条映射到同一端点）与反向 `DESK_ENDPOINTS=0=…,8=…`。与 PM 无关，但 PM 依赖它；第 0 步独立提交。

## B4. MarkPrice 流

product desk 用现有逻辑（depth 中价 + `INDEX_SOURCES` 钳制）算 mark，发布 `MarkPrice{symbol, mark_ticks, index_ticks, ts_ns, seq}`（48B，`MARK_STREAM=7`，10ms 或变动 > 1 tick）。一个 symbol 一个写者 → 所有 owner 同价、可审计；跨主机流量从 depth 帧降到 48B；外部 index 源只需 product desk 接。owner 侧 `update_mark_price` 输入换成此帧，B5 `last_mark_update` 冻结检测保留。

## B5. 下单路径与跨引擎强平

- 只有 owner 能算边际 IM（需要账户向量）；转发帧已带 symbol/side/price/qty，owner 自算。
- 前置是闸门在前的单向发送，无"撮合 → 风控 → 撮合"往返，**风控与撮合不需要同机**。
- 强平：每用户每 tick 一组动作；对冲对成对减仓 = 两张 IOC 发往两个引擎，非原子。同 tick 连续发出；账户记 `liq_inflight_until`（`LIQ_INFLIGHT_MS`=50），回执齐或超时前不发下一组；一腿成交一腿未成交时，下个 tick 重评，剩下的单腿自然成为下一组动作。强平单绕过闸门（现有 `fill_margin=0`）。

## B6. 故障与降级

| 故障 | 处理 | PM 特有 |
|---|---|---|
| 引擎主机宕机 / 分区 | 现有：B5 冻结该 symbol 强平；发布失败拒单 | **冻结该 RU**：其上风险上升单拒绝；持仓按最后 mark 估值、冲击 × `PM_STALE_MULT`；其他 RU 照常 |
| product desk 宕机（mark 发布者） | 同上 RU 冻结 | 可选：owner 退回 depth 自算 mark 兜底一个版本 |
| PM desk 宕机 | 其用户不能交易；重启 hydration 后从仓位 + 挂单重算向量；未完成前拒单，完成后先全量 tick 再开闸 | 或用 A4 协议把账户迁到另一台 PM desk（计划内）；非计划宕机的自动接管 = HA 课题，epoch 已就绪 |
| 普通 desk 宕机 | 现状 | 无 |
| 目录不一致（edge 缓存旧值） | 转发到旧 owner → 被 epoch 拒 → edge 重查目录重发一次 | — |

## B7. 否决

PM 进引擎（无组合视图、百纳秒预算）；独立计算型 risk-engine（多一跳 + 第二写者）；状态复制到所有 desk（有可见性无互斥）；风控撮合同机（无往返）；quota lease 作主路径（亲和连接更便宜；保留 plan B）；各 desk 自算 mark。

---

# C. 产品无关的情景内核（risk-core 库）

## C1. 设计边界

- **内核只认三样东西**：因子（factor）、腿（leg）的损益形状（payoff）、情景（scenario）。它不认识"perp / 现货 / RWA / 预测"这些产品名。
- 产品接入 = 把它的持仓映射成若干腿，每腿声明 `(factor, payoff)`。内核零改动。这是参考文档 "enum over axis kinds, not products" 的思想，但用 **闭集枚举 + 纯数据** 实现，不用 `dyn Trait`（热路径无虚调用、可 SoA）。

```rust
pub struct FactorId(u16);                       // BTC, ETH, NVDA, USDC_PEG, EVENT_1234 …
pub enum Payoff {                               // 闭集，很少增加
    Linear  { signed_notional_atoms: i64 },     // perp、现货余额、线性 RWA
    Binary  { qty_lots: i64, payout_atoms: i64, cur_prob_bps: i64 }, // 预测头寸：跳到 0 / 1
    // 后续：Option { … } 等
}
pub struct Leg { pub factor: FactorId, pub payoff: Payoff }

pub enum Axis {                                 // 闭集
    Price { p_bps: i64, d_bps: i64 },           // 全额冲击 + 背离冲击（块内联动）
    Jump  { points_bps: SmallVec<[i64; 4]> },   // 离散跳变：depeg、RWA 缺口、预测结算
}
pub struct Factor { pub id: FactorId, pub block: BlockId, pub axes: SmallVec<[Axis; 2]>, pub frozen: bool }
```

## C2. 情景与损失向量

- 因子按 **相关块**（`BlockId`）分组：加密块 {BTC, ETH, SOL…}、美股块 {NVDA, AAPL…}、每个预测事件自成一块、稳定币锚定自成一块。块内因子联动（主方向 × 背离子集，v3/v4 的生成规则），块间独立。
- 每块生成自己的情景集 `S_b`（Price 轴 2·2^k，Jump 轴逐点枚举，≤ 32）。每腿在其块的情景下算损失向量：Linear 闭式 `notional × shock`；Binary 在 Jump 点上 `qty × (payout × point − 当前价值)`。
- **块内可加**：同块各腿向量逐元素相加；加腿/减腿/撤单是向量加减，O(S_b)。

## C3. 相关块分解（避免情景爆炸的关键）

组合损失 = Σ_块 块内损失，块间情景独立（笛卡尔积），于是

```
max_s Σ_b loss_b(s_b) = Σ_b max_{s_b} loss_b(s_b)
```

**组合需求量 = 各块最坏损失之和。** 永远不需要跨块联合枚举。一个用户持有 4 个加密 RU + 3 个美股 RU + 10 个预测事件，情景总数是 32 + 16 + 10×2，不是 32 × 16 × 2^10。参考文档把这一点表述为"相关矩阵稀疏 → 子空间重估"；这里把它做成结构：块是一等公民，块内枚举、块间求和。代价：块间不给分散收益（BTC 与 NVDA 同跌不抵消）——这正是危机中应有的保守。

账户缓存：`SmallVec<[BlockState; 4]>`，每块一个 `{block, ru_order, pos_im, pos_mm, pending_im}`，`used_margin = Σ worst(pos_im)`，`maintenance_margin = Σ worst(pos_mm)`。

## C4. 网格与参数

- MM 网格 `p = maintenance_rate_bps`（现值 50），IM 网格 `p = 1e4/leverage`（现值 1000）→ 单仓 PM 账户与现状逐 atom 一致，灰度可对比。
- 背离 `d = p × div_bps`（`PM_DIV_BPS=BTC:4000,…`；1e4 = 无对冲折扣 = 相关性失效压力测试）。
- Jump 点按因子配置（`USDC_PEG: [−500, −1000]`，预测事件 `[−cur, +（1e4−cur）]`）。
- RU 冻结冲击放大 `PM_STALE_MULT_BPS=20000`；`MAX_PM_RUS_PER_BLOCK=4`，超出该块降级为块内 Σ。

## C5. 前置、慢路径、强平（与 v4 相同，按块执行）

- 前置：`marginal = worst_b(pos+pending+order) − worst_b(pos+pending)`，只算订单所在块；`≤ 0` 不设闸门（平仓、减仓、对冲），`MarginCall` 也放行；冻结因子上的风险上升单拒绝。通过后 `pending += v_order`（并发对冲单只有第一笔拿折扣）。
- `on_fill` / `update_mark_price`：只重算受影响块，O(S_b)；`get_mut(acct)` + 键控 `positions.get`，无嵌套迭代。
- `run_risk_tick` 三段式不动。
- 强平候选：单腿 tranche、块内对冲对成对 tranche；`Δmm = worst_b(V) − worst_b(V − v_action)`；按 `Δmm / 释放名义额` 排序；全部 `Δmm ≤ 0` → 整体按比例减仓；冻结因子的腿不参与；ADL 削对手方前检查不破坏其对冲。

## C6. 可解释性 API（PM desk 提供）

`GET /api/pm/margin` 返回：每块的约束情景（哪个方向、哪些 RU 背离）、每腿在该情景下的损失、`Σ 独立 IM − 组合 IM` 的对冲节省、以及"再加 X 名义额的 Y 腿"的边际（前置函数同款）。全部来自已缓存的向量，O(腿数 × S_b)。

## C7. 持久化与恢复

不持久化派生量；持久化 `margin_mode`、`account_home`、参数版本。hydration 从仓位 + 挂单 + mark 重算；`reconcile.rs` 校验一致性。

---

# D. 多产品路线（内核不变，每阶段只加"腿的映射"和"块"）

| 阶段 | 产品 | 新增 | 内核改动 |
|---|---|---|---|
| 1 | USDT 本位 perp（现在） | 加密块，Linear 腿，Price 轴 | 无（首个实现） |
| 2 | 现货 + 统一账户 | 多币种余额：每个非 USDT 余额是一条 Linear 腿（`signed_notional = +qty × mark`），参与同块情景 → **现货 BTC + 空 BTC perp 自动对冲**，haircut 由冲击替代；权益 = Σ 余额 × mark + 未实现；强平新增"卖抵押品结清 USDT 亏损"瀑布（只在 USDT 不足时触发） | 无 |
| 3 | RWA（美股等） | 美股块；Price + Jump(缺口) 轴；交易时段外因子置 `frozen`（复用 RU 冻结路径：拒开仓、放宽冲击、不强平）；发行方 / 托管信用限额是 desk 层的仓位上限，不进内核 | 无 |
| 4 | 预测市场 | 每事件一块；Binary 腿；Jump 到 0/1；结算 = 一次 Jump 落地成成交；无可靠结算源的事件不接受为抵押 | 无 |
| 5 | 期权 / 非线性 | `Payoff::Option`，Price + Vol 轴 | 加一个枚举分支 |

阶段 2 是拐点：多币种权益与抵押品瀑布是账本层的改动（`account.rs`、pg-writer 表结构），比风控内核工作量大，应单独立项。

---

# E. 落地顺序

| 步 | 改动 | 验证 |
|---|---|---|
| 0a | `ENGINE_ENDPOINTS` / `DESK_ENDPOINTS` | 两台引擎主机各跑 symbol，desk 双向收发 |
| 0b | `MarkPrice` 流 | 所有 desk 同 symbol 的 mark 逐 tick 相等 |
| 0c | owner 亲和连接（登录返回 `owner_desk_url`） | 40K 压测 forward 占比 ≈ 0 |
| 0d | 账户目录 + `resolve_owner` + 去掉 `desk_id % 4` 环绕 + `DESK_ROLE` 开关 | 目录 miss 走 modulo；PM desk 8 收到目录用户的转发；epoch 低的帧被拒 |
| 1 | `risk-core` 库：Factor / Leg / Payoff / Axis / 块情景生成 / 损失向量 / `worst` 纯函数 | 单仓与现公式逐 atom 相等；对冲折扣 `1 − d/p`；同向无折扣；块分解 = 联合枚举的上界且相等（独立块）；Binary 腿 Jump 损失；i128 无溢出 |
| 2 | PM desk 账户块缓存；`on_fill` / `update_mark_price` 维护 | 对冲组合 Σ 模式 `LiquidationPending`、PM 模式 `Normal`；40K×2 tick 不回归（普通 desk 路径不变） |
| 3 | 前置边际 IM（PM desk 的三个入口）+ pending + 因子冻结拒单 | 边际冻结、平仓放行、并发对冲单单次折扣、冻结因子行为 |
| 4 | 有序强平 + 成对减仓 + 收敛检查 + in-flight + ADL 保护 | 跨引擎成对 IOC；一腿未成交的续动作；整体减仓；回安全停止 |
| 5 | 在线迁移协议 A4 + `HomeSet` PersistEvent + 客户端 `rehome` | 普通 → PM → 普通 往返，持仓不变、挂单撤销、epoch 拒旧 owner |
| 6 | `margin_mode` / `account_home` 持久化、hydration、reconcile、C6 API | 重启后一致；API 输出与内部一致 |

0a-0d 彼此独立、与 PM 独立；1-3 灰度"对冲少冻 + 组合 MM"；4-6 上线前必须。阶段 2 产品（现货）另立计划。

---

# F. 参考文档的取舍

采纳：情景内核、对冲收益涌现；product-blind（闭集轴 + 腿注册）；按 `Δ需求量` 判风险方向、降风险单不设闸门；相关性失效 = 参数；强平边际贡献排序、成对解除、收敛检查；ADL 不削对手方对冲腿；因子冻结而非猜价；IM 用更保守网格；Jump 轴表达 depeg / 缺口 / 预测结算；抵押品与持仓同一内核（阶段 2）。

不采纳：`dyn Instrument` trait（用闭集枚举 + 纯数据）；500K 组合的 SoA / SIMD 预筛 / 自适应频率（PM 用户量级不需要；倒排索引已有）；风控撮合同机（无往返）；待强平队列（执行 µs 级）；L2 / 排序器 / 预言机聚合（已有 index 冻结 + mark 钳制）；ES 分位数。强平结算价政策（接管 vs 退还）是独立产品决策，现状接管制。

---

# G. 假设

1. 阶段 1 单一 USDT 抵押品；多币种在阶段 2。
2. 一个 symbol 只在一个引擎，一个引擎可多个 symbol；desk 与引擎可不同机；mark 只由 product desk 发布。
3. PM desk 与普通 desk 同一二进制、`DESK_ROLE` 区分；PM desk 的 `desk_id` 从 8 起，不进 modulo 环。
4. 账户目录 miss 默认 modulo；目录只存 PM 账户。
5. 模式切换撤销挂单、保留持仓；切换期间该账户拒单。
6. 块内最多 4 个 RU 参与联合枚举，超出该块降级块内 Σ；块间不给分散收益。
7. 挂单未成交部分计入 IM 向量；`Liquidating` 及之后状态拒绝用户订单。
8. 不引入新二进制；新增一条 Aeron 小流、一张目录表、两张端点表、一个 `risk-core` 库 crate。
