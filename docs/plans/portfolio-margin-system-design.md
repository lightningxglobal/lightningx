# LightningX Portfolio Margin 系统设计

> 状态：目标架构与实施基线
> 日期：2026-09-15
> 范围：现货、永续、交割合约、RWA、Prediction Markets；跨 Desk、跨撮合引擎、跨服务器
> 优先级：偿付能力与状态一致性优先，其次是延迟、吞吐和资金效率
> 补充决策：[Portfolio Margin 多方架构评审决策记录](portfolio-margin-brainstorm-decisions.md)（2026-09-29）

## 1. 结论

Portfolio Margin 不能继续作为 `desk-server` 内部由 Product Desk 身份隐式拥有的计算模块存在。必须立即解耦逻辑所有权；进程和物理服务器的拆分按“进程内单写 Risk Shard → 同机 sidecar → 跨机 Risk Cluster”渐进实施。目标系统必须把三种所有权解耦：

- 撮合所有权按 `instrument_id` 分片。
- 风险所有权按 `risk_group_id` 分片。
- 财务账本按 `account_id` 或 `risk_group_id` 分片。

同一个风险组合可以在任意数量的 Spot、Perpetual、Futures、RWA 和 Prediction Matcher 上持仓，但它的抵押物、仓位、活动委托、资金费、应收应付和爆仓状态只能由一个 PM Risk Shard Leader 顺序修改。

```text
Client
  │
  ▼
Gateway / Desk ingress
  │  resolve risk_group_id
  ▼
PM Risk Router
  │  virtual_risk_shard = hash(risk_group_id)
  ▼
PM Risk Shard Leader
  │  reserve portfolio risk and issue RiskPermit
  ├──────────────┬──────────────┬──────────────┬──────────────┐
  ▼              ▼              ▼              ▼              ▼
Spot Matcher  Perp Matcher  Futures Matcher  RWA Matcher  Prediction Matcher
  │              │              │              │              │
  └──────── accepted / fill / cancel / reject / expire ───────┘
                         │
                         ▼
                 PM Risk Shard Leader
                         │
                  Ledger / journal
```

撮合引擎不计算 PM。Matcher 只验证 RiskPermit、执行订单并产生有序事实事件。PM Risk Shard 是新增风险能否进入系统的唯一授权者。

因此，对“PM 账户是否需要特殊 owner desk”的直接答案是：逻辑上需要唯一的风险 owner，物理上不应绑定任何产品 Desk。长期形态是独立的 `PM Risk Owner Shard`；过渡期可以由现有服务器承载该 shard，但其权威性来自 RiskShardDirectory 和 leader epoch，而不是 Desk 身份。Desk 只负责接入，批准后的订单由 Risk Owner 直接发给目标 Matcher。

## 2. 与参考架构的取舍

参考文档中以下原则直接采用：

1. 用确定性 scenario-based portfolio revaluation，而不是在下单热路径运行 Monte Carlo。
2. 产品向统一风险因子投影；同一 shocked state 下重估整个组合，让 offset 自然产生，而不是维护成对抵扣表。
3. 使用 factor inverted index、保守预筛选和完整重估两阶段调度。
4. 采用 SoA、连续数组、固定点整数、固定场景数组和无分配热路径。
5. 将“关闭风险仓位”和“变现抵押物”作为两个不同的 waterfall。
6. 爆仓默认部分去风险，每一步成交后重新计算整个组合；保险基金和 ADL 是最后手段。
7. 爆仓使用可信 mark/index，不使用 last trade。
8. 把 depeg、RWA gap、Prediction outcome 当成显式 Jump scenario。

以下内容需要针对 LightningX 调整：

- 参考文档的单机容量估算可以指导单 shard 内存布局，但不能作为全系统单机部署依据。LightningX 明确需要多 Desk、多 Matcher、多服务器，因此从第一版就使用虚拟 Risk Shard。
- `value(state) + risk_axes()` 是语义接口，不是热路径里的 `Box<dyn Instrument>`。动态注册只允许出现在控制面；热路径按 `ProductType` 静态分派。
- 参考文档提出形式上的 close 即使增加净风险也只告警放行。LightningX 不采用这个规则：只有可以证明不会翻仓的 strict reduce-only 无条件放行；拆掉 hedge 后提高组合 requirement 的普通 close 仍然执行 IM 检查。
- Liquidation/ADL 不经过普通用户 IM gate，但必须经过专用的系统风险许可，验证它确实属于目标 liquidation plan 且不会超过允许数量。不能让任何“特殊订单”不经认证直接进入 Matcher。
- LightningX 当前不是以 L2 作为唯一账本真相。目标实现以顺序风险日志、Matcher journal 和 double-entry ledger 为恢复与对账依据；PostgreSQL/Redis 仍只是查询投影，不进入下单热路径。

## 3. 风险边界：RiskGroup

新增稳定标识：

```rust
pub struct RiskGroupId(pub u64);
pub struct AccountId(pub u64);
pub struct InstrumentId(pub u32);
pub struct FactorId(pub u32);
```

`RiskGroupId` 不能简单等同于 `user_id`。它表达允许共享抵押物和风险抵扣的法律及业务边界：

```text
Legal entity
  └── RiskGroup
       ├── Spot account
       ├── Derivatives account
       ├── Strategy subaccount A
       └── Strategy subaccount B
```

规则：

- 普通用户默认一个账户对应一个 RiskGroup。
- 子账户默认隔离；只有显式配置后才共享 PM。
- 不同法人、清算币种、托管域或破产隔离域不得放进同一个 RiskGroup。
- RiskGroup 关系只能通过控制面修改，修改期间冻结新增风险并执行完整对账。
- Matcher、Desk 和产品代码都不能自行推导组合边界，只能使用账户目录返回的 `risk_group_id`。

## 4. 风险分片与路由

### 4.1 虚拟 shard

废除“物理服务器数量就是风险 shard 数量”的假设。建议预建 4096 个虚拟 shard：

```text
virtual_shard = stable_hash(risk_group_id) % 4096
physical_owner = RiskShardDirectory[virtual_shard]
```

每个目录项包含：

```rust
pub struct RiskShardRoute {
    pub virtual_shard: u16,
    pub leader_node_id: u32,
    pub leader_epoch: u64,
    pub command_endpoint: EndpointId,
    pub standby_node_id: u32,
}
```

优势：

- 扩容只迁移部分虚拟 shard。
- 大型做市商可以迁移到独占 CPU 或独占节点。
- Matcher 和 Gateway 不需要知道 Risk Engine 的物理部署。
- 不再受当前固定 `COUNTER_SHARD_COUNT = 4` 限制。

路由变更必须带递增 `leader_epoch`。Matcher 拒绝旧 epoch 签发的新 permit，避免网络分区后双主。

### 4.2 单写者

一个 RiskGroup 在任意时刻只允许一个 Risk Shard Leader 写入：

- 下单预留、成交、撤单、费用、资金费、入金、提现、转账和爆仓事件在该 leader 上排序。
- 读副本可以服务资产和风险查询，但不能批准新增风险。
- 不使用跨节点共享锁，也不让多个 Desk 对同一 RiskGroup 分别 reserve。
- 分片内采用固定线程 event loop；一个 RiskGroup 的事件永远落到同一线程。

### 4.3 PM 账户是否需要特殊 Owner Desk

PM 账户确实需要唯一 owner，但这个 owner 应定义为 `PM Risk Owner Shard`，而不是某个产品 Desk：

```text
错误：PM user -> BTC owner desk -> BTC/ETH/SOL/RWA/Prediction
正确：PM RiskGroup -> PM Risk Shard -> target product Matcher
```

原因：

- 产品 Desk 按 symbol 扩容，Risk Shard 按账户组合扩容，两者热点分布完全不同。
- 把 PM 绑定到特殊 Desk 会让该 Desk 同时承担连接、风控、撮合转发和状态恢复，成为性能及故障域热点。
- 一个 PM 账户可能主要交易 ETH、RWA 或 Prediction，固定经过 BTC/某个产品 Desk 没有业务意义。
- Matcher 迁移、产品新增或 Desk 扩容不应迁移账户风险状态。

推荐部署有两类独立角色：

```text
Ingress Desk / Gateway
  - 可接入任何用户
  - 不拥有权威风险状态

PM Risk Owner
  - 拥有某组 virtual risk shards
  - 单写组合状态并签发 permit
  - 直接把已批准命令发给目标 Matcher
```

响应通过 `ingress_id + connection_id + request_id` 返回原接入节点。客户端不需要知道 PM owner，也不要求 WebSocket sticky 到风险节点。

过渡期可以把一部分 `desk-server` 实例配置成 PM owner，并复用当前 counter-forward 通道，但必须遵守两个限制：

1. PM owner 实例不因为自己也运行某个 symbol 就拥有额外风险权限；权威性只来自 virtual shard leader epoch。
2. 迁移完成后把 Risk Engine 从 desk-server 提取出来，避免临时部署形态固化成长期架构。

非 PM 账户也建议逐步使用同一个 RiskPermit 协议，只是选择更简单的 isolated/cross risk model。不要长期保留“普通账户 Desk 本地批准、PM 账户远程批准”两套资金生命周期，否则撤单、成交、提现和恢复逻辑会再次分叉。可以为 PM 和非 PM 建立不同的物理 Risk Node pool，但它们共享协议、journal、HA 和 reconciliation。

### 4.4 最短热路径

目标链路是：

```text
Ingress -> PM Risk Owner -> Matcher -> PM Risk Owner / Ingress
```

Risk Owner 在批准后直接投递 Matcher command，而不是把 permit 返回 Ingress 后再由 Ingress 转发，从而减少一个网络 hop 和一次队列切换。Ingress 只等待与用户 SLA 有关的 accepted/rejected response；Fill 等事实事件始终直接进入 Risk Owner。

同一台物理机可同时部署若干 Risk Shard 和若干 Matcher，但这只是调度优化，不改变所有权：

- 同机时走共享内存/Aeron IPC。
- 跨机时走 Aeron UDP 或经验证的 kernel-bypass transport。
- 调度器根据真实流量把常见 `Risk Shard <-> Matcher` 对尽量放在同机或同 rack。
- 不能复制 RiskGroup 写状态到每个 Matcher 来消除网络 hop；那会重新引入保证金双花。

## 5. 服务与部署边界

### 5.1 Gateway / Desk ingress

职责：

- 鉴权、限流、协议解析和基础格式检查。
- 从 Account Directory 解析 `risk_group_id`。
- 通过本地缓存的 RiskShardDirectory 路由到正确 leader。
- 把 RiskCheck 请求发送给 Risk Owner；Risk Owner 批准后直接把 permit 和订单发送给目标 Matcher。
- 向客户端聚合异步订单状态。

它不拥有可用于批准交易的风险状态。允许做静态拒绝和缓存预检，但缓存结果永远不能作为放行依据。

### 5.2 PM Risk Cluster

职责：

- 拥有 RiskGroup 的热状态和活动 reservation。
- 执行 pre-trade hypothetical revaluation。
- 签发、确认、消费和释放 RiskPermit。
- 消费所有 Matcher 和 Ledger 事实事件。
- 计算 IM、MM、可用抵押物和 stress loss。
- 检测 breach，创建 liquidation plan。
- 维护 factor inverted index 和风险预筛选缓存。

### 5.3 Matching clusters

Spot、Perpetual、Futures、RWA、Prediction 可以独立部署和扩容。每个 Matcher：

- 单产品或产品组单写者撮合。
- 验证 RiskPermit 的 epoch、instrument、数量、方向和保护价格。
- 对 `command_id` 和 `reservation_id` 幂等。
- 产生严格有序的 Accepted、Fill、Cancel、Reject、Expire 事件。
- 保留可重放的 order journal 和 execution journal。

Matcher 不信任 Gateway 声称的 reduce-only；它根据自己持有的订单/仓位约束执行 permit 中的硬限制。

### 5.4 Market Risk Data Service

独立聚合并发布：

```rust
pub struct RiskFactorUpdate {
    pub factor_id: FactorId,
    pub value: i64,
    pub version: u64,
    pub timestamp_ns: u64,
    pub source_count: u8,
    pub dispersion_bps: u32,
    pub market_status: MarketStatus,
}
```

输入包括 CEX/DEX/index、利率、FX、RWA session/halt、tokenized asset basis、funding、Prediction resolution/oracle 状态。

所有 Risk Shard 在本地维护 factor snapshot。下单热路径不得 RPC 查询某个 Matcher 的最新价格。

### 5.5 Ledger / Clearing

Ledger 是资金归属和财务账的最终权威，采用 double-entry。Risk Shard 维护与其顺序一致的风险投影。

以下事件都必须先进入 RiskGroup 顺序域或持有风险 reservation：

- 入金确认；
- 提现 hold、广播、确认和失败回退；
- 内部转账；
- 借贷与还款；
- 手续费和返佣；
- funding；
- futures settlement；
- RWA corporate action；
- Prediction payout。

提现不能仅查询数据库余额，否则提现服务和订单服务会再次产生保证金双花。

### 5.6 Liquidation Coordinator

Liquidation Coordinator 执行计划，但不单独拥有账户风险状态：

- Risk Shard 创建并持久化 `LiquidationPlan`。
- Coordinator 把计划拆成发往多个 Matcher 的受保护系统订单。
- 每个 fill 返回原 Risk Shard。
- Risk Shard 在每一步后重估并决定继续、换腿或停止。

这是一条带幂等和补偿的 saga，不是跨 Matcher 两阶段提交。

## 6. 顺序、日志和高可用

### 6.1 每 shard 的复制

建议部署：

```text
Risk Shard Leader
  ├── optional synchronous WAL replica in the same AZ
  ├── hot in-memory standby on another host
  └── asynchronous DR replica in another region
```

系统不追求每条传播路径同步完整，而只要求订单、成交、Ledger 等权威事实可恢复。跨地域复制不进入订单热路径；同 AZ reservation 是否同步提交由 RiskGroup 的 durability mode 决定。

`FAST_RECOVERY` 模式按以下顺序执行：

```text
apply reservation in leader memory
  -> append local WAL and replicate to same-AZ standby
  -> standby durable acknowledgement
  -> issue permit and dispatch directly to Matcher
```

它适用于高杠杆、巨鲸、复杂非线性组合、接近 MM 或 Liquidating 的账户。实现上对不同 RiskGroup 请求做流水线并行，不等待一批订单凑齐；standby 与 leader 优先部署在同 rack 的不同故障主机。

`LATENCY_FIRST` 模式只保留一个可靠核心提交点：Risk Leader 先在内存 reserve，Matcher 必须在订单可进入订单簿或成交前，把完整 permit、reservation 和 order state 写入其可靠顺序域；Risk standby 异步追平。这样避免 Risk quorum 与 Matcher commit 两次串行可靠性屏障。

该模式只有在 Matcher journal/replication 已达到上述语义，并且 failover 后冻结整个 Risk Shard、对所有 Matcher 与 Ledger 完成 epoch fence 和 recovery barrier 时才安全。它用更长 RTO 和更低故障期可用性换取正常路径延迟；任何目标 Matcher 不可达时都不能恢复新增风险。

durability mode 只能在 RiskGroup sequence barrier 上切换，切换事件写入 journal，禁止逐单临时选择。目录和 epoch 由独立 quorum 管理，数据面始终保持每个 shard 单 leader，禁止 active-active 写入。

### 6.2 日志记录

风险日志记录事实和确定性配置版本：

```rust
pub enum RiskEvent {
    ReservationCreated,
    ReservationAccepted,
    ReservationRejected,
    FillApplied,
    ReservationReleased,
    CollateralChanged,
    FundingApplied,
    SettlementApplied,
    RiskConfigActivated,
    LiquidationStateChanged,
    InsuranceFundChanged,
    AdlApplied,
}
```

每条事件包含：

- `virtual_shard`；
- `leader_epoch`；
- `shard_sequence`；
- `command_id`；
- 业务幂等键；
- `risk_group_id`；
- `risk_config_version`；
- 必要的 factor/version 输入。

snapshot 只用于加速恢复，journal 才用于重放。PostgreSQL 和 Redis 是异步投影。

### 6.3 不确定订单

如果 Risk Shard 已建立 reservation，但无法确认订单是否到达 Matcher：

```text
Reserved -> Dispatching -> Uncertain
```

不得因为 RPC 超时自动释放。必须查询 Matcher journal 或等待有序 reconciliation，确认订单不存在后才能释放。已经 Accepted 的 GTC reservation 没有普通 TTL；permit 的 TTL 只限制它在被 Matcher 接受前的有效时间。

### 6.4 fail-closed

- Risk leader 不可用：拒绝新增风险。
- 市场数据过期：拒绝新增风险，允许经过证明的 strict reduce-only。
- Matcher 事件有 sequence gap：冻结受影响产品的新增风险并重放补洞。
- leader 切换：提升 epoch，旧 leader 和旧未消费 permit 被 fencing。
- 无法确认账户状态：进入 `Restricted`，不能乐观恢复额度。

### 6.5 权威事实与客户端通知

可靠性按语义分层：

| 层级 | 数据 | 允许的行为 |
|---|---|---|
| 权威事实 | Matcher accepted/fill、Ledger、settlement、funding、insurance/ADL | 不允许最终丢失；必须可重放、去重、查询 |
| 可重建状态 | Risk reservation/scenario/available-margin cache | 允许内存丢失；重建完成前 fail closed |
| 查询投影 | PostgreSQL、Redis、报表和历史索引 | 允许短暂滞后；必须暴露或校验 high watermark |
| 客户端通知 | Private WebSocket、订单/余额/爆仓提醒 | 允许断线、重复和缺失；通过 sequence、snapshot 和 REST reconciliation 恢复 |

订单响应中的 `accepted` 必须表示订单已经进入某个可恢复的权威顺序域。WebSocket 推送、PostgreSQL/Redis 投影和报表不能阻塞撮合，也不能因为发送失败回滚订单。客户端使用幂等 `client_order_id`、事件 sequence 和查询 high watermark 检测缺口；REST 查询若尚未追平目标 watermark，必须明确返回 lag/重试语义，不能把旧投影伪装成最终状态。

## 7. RiskPermit 协议

### 7.1 请求

```rust
#[repr(C)]
pub struct RiskCheckCommand {
    pub command_id: u128,
    pub risk_group_id: u64,
    pub account_id: u64,
    pub instrument_id: u32,
    pub side: u8,
    pub order_type: u8,
    pub time_in_force: u8,
    pub reduce_only: u8,
    pub price_ticks: i64,
    pub quantity_lots: i64,
    pub max_slippage_bps: u32,
    pub product_config_version: u64,
}
```

### 7.2 返回

```rust
#[repr(C)]
pub struct RiskPermit {
    pub reservation_id: u128,
    pub risk_group_id: u64,
    pub risk_version: u64,
    pub risk_epoch: u64,
    pub instrument_id: u32,
    pub side: u8,
    pub flags: u8,
    pub max_quantity_lots: i64,
    pub protection_price_ticks: i64,
    pub valid_until_ns: u64,
    pub risk_config_version: u64,
}
```

permit 必须绑定具体 instrument、方向、最大数量和价格边界，不能成为可以转用于其他订单的通用额度。

跨进程 permit 还必须包含 `leader_epoch`、单调 `permit_sequence` 和认证标签。Matcher 使用当前 epoch 的预共享轮换密钥做常数时间 MAC 校验，并维护已消费的 `reservation_id`/sequence 去重窗口；不能依赖“消息来自内网”判断可信。密钥轮换属于控制面，不在逐单路径做远程验签或密钥查询。

### 7.3 生命周期

```text
Proposed
  -> Reserved
  -> Dispatching
  -> Accepted
  -> PartiallyConsumed
  -> Consumed | Released | Uncertain
```

事件处理：

- Reject：释放全部 reservation。
- Fill：消费成交部分，转成 position/cash exposure。
- Partial fill：保留未成交委托的 scenario risk。
- Cancel/Expire：释放剩余部分。
- IOC terminal partial：先应用 fill，再释放未成交部分。
- 重复事件：按 execution id 和 order sequence 忽略。

RiskPermit 与 Matcher OrderId 的绑定必须在 Accepted 时持久化。Matcher 先成交后回 Accepted 也不会不安全，因为 reservation 在发 permit 前已经生效，但消费端必须能处理跨流乱序。

## 8. PM 组合状态

```rust
pub struct PortfolioState {
    pub risk_group_id: RiskGroupId,
    pub version: u64,
    pub status: PortfolioStatus,

    pub cash_and_collateral: CollateralStore,
    pub positions: PositionStore,
    pub reservations: ReservationStore,

    pub scenario_equity: ScenarioArray,
    pub scenario_requirement: ScenarioArray,
    pub current_im: i64,
    pub current_mm: i64,
    pub available_collateral: i64,
    pub worst_scenario: u16,
}
```

热路径数据不使用 `HashMap<String, ...>`、`Box<dyn ...>` 或每次请求分配的 `Vec`。控制面负责把字符串和产品配置编译成数字 ID 与定长表。

建议的内存层次：

- `PortfolioHeader[]`：账户级 SoA。
- `PositionColumns`：按 product/factor 分桶的 SoA。
- `CollateralColumns`：按 collateral class 分桶。
- `ReservationColumns`：arena index + generation，稳定索引。
- `ScenarioArray`：固定上限，例如 64/128/256 个 `i64`。
- `FactorSubscribers`：factor 到本 shard portfolio index 的倒排表。

大账户允许使用独立 arena 和独立 CPU budget，避免一个有数万腿的机构组合阻塞普通用户。

## 9. 统一估值与风险因子

### 9.1 语义接口

产品在注册/编译阶段提供：

```rust
pub struct CompiledInstrumentRisk {
    pub instrument_id: InstrumentId,
    pub product_type: ProductType,
    pub factor_refs: FixedFactorRefs,
    pub valuation_program: ValuationProgramId,
    pub liquidation_profile: LiquidationProfileId,
}
```

热路径使用静态分派：

```rust
match instrument.product_type {
    ProductType::Spot => value_spot(...),
    ProductType::Perpetual => value_perpetual(...),
    ProductType::Future => value_future(...),
    ProductType::Rwa => value_rwa(...),
    ProductType::Prediction => value_prediction(...),
}
```

统一的是 factor state 和 scenario aggregation，不要求所有产品共享同一个错误的线性定价公式。

### 9.2 风险轴

```rust
pub enum AxisKind {
    Price,
    Volatility,
    Basis,
    Funding,
    InterestRate,
    Fx,
    Peg,
    Jump,
    Liquidity,
    Settlement,
    IssuerCredit,
}
```

产品例子：

| 产品 | 典型风险轴 |
|---|---|
| Spot BTC/USDT | BTC price、USDT peg、venue liquidity |
| BTC perpetual | BTC index、basis、funding、quote peg、liquidity |
| BTC dated future | BTC index、term basis、expiry/settlement、liquidity |
| Tokenized NVDA | NVDA price、USD FX、session/gap、issuer credit、token basis、liquidity |
| Prediction YES | outcome jump、resolution/oracle、time-to-resolution、liquidity |

### 9.3 Collateral 与 position 分离

平台内仓位已经进入 portfolio equity，不能再次被当作 collateral 重复计算。外部可转让资产或 instrument-collateral 必须经过单独 eligibility 配置：

```rust
pub struct CollateralRiskParams {
    pub collateral_id: u32,
    pub base_haircut_bps: u32,
    pub max_group_share_bps: u32,
    pub wrong_way_group: u16,
    pub oracle_id: u32,
    pub staleness_limit_ns: u64,
    pub realization_profile: u16,
    pub issuer_limit_id: u16,
}
```

haircut 不能覆盖 issuer default、法律冻结、oracle 不可验证等风险；这些必须有显式 jump scenario、集中度上限或直接禁止作为 collateral。

## 10. 场景模型

### 10.1 Requirement

```text
PM requirement =
    max(worst deterministic scenario loss, product floors)
  + market impact addon
  + concentration addon
  + wrong-way/self-collateral addon
  + liquidity and realization addon
  + funding/fee/settlement buffer
  + stale-data addon
  - capped diversification credit
```

IM 与 MM 使用不同强度：

- IM：用于新增风险；使用更宽场景或 `MM × IM multiplier`。
- MM：用于持续风险和爆仓触发。
- Bankruptcy：不是固定 margin tier，而是组合在特定市场状态下净权益耗尽的结果。

PM 账户通常不存在唯一的“爆仓价”。UI 展示的单产品 liquidation price 只能是“其他 factor 不变”的条件估算，不能作为系统触发依据。

### 10.2 Scenario blocks

第一版使用确定性场景块，不生成所有 factor 的笛卡尔积：

- Crypto core：方向、相关性、basis、funding 和 stablecoin depeg。
- Futures term：curve/basis/expiry shock。
- RWA：session gap、halt、FX、issuer/token depeg。
- Prediction：每个互斥 outcome 的完整 terminal payoff。
- Cross-block stress：相关性崩坏、所有 risk assets 同跌、quote collateral depeg。

连续因子可以使用 SPAN 风格的有限场景或经验证的降维；Jump 必须显式枚举。正常相关性抵扣之外，必须包含危机相关性场景，例如把同类风险资产相关性推向 1 并缩减 offset。

### 10.3 Open-order risk

不能假设所有挂单都会同时按有利方式成交：

- 已成交仓位始终进入全部场景。
- marketable/IOC 按保护价格和可能成交数量计入。
- 非 marketable GTC 对每个场景计算是否会被触发成交。
- 同方向多笔订单应按可能同时成交的总风险计入。
- 未保证原子执行的两笔 hedge order，不能在两边都成交前给予完整 offset。
- OCO、spread/basket 等只有 Matcher 提供原子语义后才能获得相应 order offset。

这是 PM pre-trade 中最容易低估风险的部分，不能简单使用 `position + candidate order` 后把所有活动委托当成确定性 hedge。

### 10.4 增量计算

每个 Portfolio 缓存固定场景向量：

```text
scenario_equity_before[S]
candidate_delta[S]
scenario_equity_after[S] = before[S] + delta[S]
```

下单只计算受影响 factor block。三个路径：

1. 独立 factor：旧 requirement 加 candidate block，快速路径。
2. 只关联一个 risk block：重估该 block。
3. 大型相关组合或 worst-scenario 发生切换：完整 portfolio revaluation。

同步预检必须使用不会产生 false negative 的保守界限。近似 linear screen 只能证明“安全距离足够大”时跳过 full reval；无法证明时必须进入完整重估。

## 11. 产品规则

### 11.1 Spot

- 计入可用余额、冻结余额、未结算资产、借贷负债和费用。
- 无借贷的 fully-funded spot 委托可以使用简单资产 reservation 快速路径。
- 带 margin borrow 的 spot 按负债和 collateral factor 进入 PM。
- 买入资产在真实 fill 前不能提前作为其他订单的可用 collateral，除非存在受控的原子组合执行。

### 11.2 Perpetual

- 计入 index、mark、basis、funding 和 quote depeg。
- 即将结算的 funding payable 直接降低 effective equity。
- funding squeeze 作为场景风险而不仅是已实现现金流。

### 11.3 Futures

- 计入期限 basis、到期收敛、交割方式和 settlement risk。
- 临近到期提高流动性/交割 addon。
- 到期结算、展期和 delivery event 必须进入 RiskGroup 顺序域。

### 11.4 RWA

- 24/7 token 与非 24/7 underlying 分开建模。
- underlying 闭市不是普通 stale；使用 last-good、显式 gap scenario，并限制新增风险。
- underlying halt、holiday、DST 和 corporate action 来自交易日历/事件服务。
- token 自身流动性决定可变现性，不能使用 underlying 市场深度代替。
- issuer/custody/default、法律冻结和 token-underlying depeg 需要独立限制。

### 11.5 Prediction

- 同一市场互斥 outcome 可以通过最终 payoff 精确净额。
- 不同市场默认不提供历史相关性抵扣。
- settlement oracle、dispute window 和 invalid outcome 是正式风险状态。
- 临近 resolution 时使用离散 terminal scenario，不使用线性 delta 近似。
- 无可靠 resolution source、无法转让或极薄流动性的 position 不作为普通 collateral。

## 12. Pre-trade 决策规则

```text
req_before = cached/current full PM requirement
req_after  = hypothetical PM after candidate and all reservations
risk_delta = req_after - req_before
```

规则：

1. `risk_delta > 0`：必须满足 after-state IM，否则拒绝。
2. `risk_delta <= 0` 且订单是 Matcher 可强制保证的 strict reduce-only：允许，即使账户已经低于 IM。
3. 形式上平仓但会拆掉 hedge、导致 `risk_delta > 0`：按新增风险处理，不能只告警放行。
4. 普通订单不能依靠 `side=close` 声明绕过检查。
5. 账户处于 Liquidating/Default：仅接受目标 liquidation plan 的系统 permit。
6. factor stale、风险配置不匹配或 shard 状态不确定：fail closed。

严格 reduce-only 必须满足：

- 不能翻转仓位；
- 最大数量由当前 Matcher 仓位/订单状态约束；
- 只能减少目标 instrument exposure；
- 若减少该腿会明显破坏 portfolio hedge，Risk Shard仍会评估整体结果，并可要求 paired unwind。

## 13. 爆仓设计

### 13.1 触发

触发条件是可信 factor snapshot 下：

```text
effective_equity < maintenance_requirement
```

必须在 full PM revaluation 后确认，不能用 last trade，也不能只比较某个 position liquidation price。

状态机：

```text
Normal
  -> Restricted
  -> CancelRiskIncreasingOrders
  -> LiquidationPlanning
  -> LiquidatingPositions
  -> SettlingQuoteDeficit
  -> Healthy | Default
  -> InsuranceFund
  -> ADL
  -> Resolved
```

### 13.2 第一步：阻止风险继续增加

- 冻结新增风险、提现、内部转出和提高杠杆。
- 允许可证明安全的 strict reduce-only。
- 撤销尚未成交的 risk-increasing orders。
- 等待撤单事实事件或通过 Matcher reconciliation 确认，不能猜测订单已撤销。
- 重新计算 PM；有些账户撤单后即可恢复。

### 13.3 第二步：生成去风险计划

选择目标不是“亏损最大的腿”，而是单位执行成本带来的组合 MM 改善：

```text
score = marginal_requirement_reduction
        / expected_execution_cost
```

计划器对候选动作做 hypothetical revaluation：

- 关闭某一腿后 requirement 是否真的下降；
- 是否拆掉 hedge 造成 self-cascade；
- 是否需要按比例同时关闭一组 hedge legs；
- 对应 Matcher 深度、价格保护和成交时间；
- 一次关闭多少可以恢复到 `MM + safety_buffer`。

优先部分去风险。每一次或每一批 fill 后都回到 Risk Shard 重新计算；达到健康线立即停止。

### 13.4 多 Matcher liquidation saga

跨产品 PM 不能依赖多撮合引擎原子成交。`LiquidationPlan` 包含：

```rust
pub struct LiquidationPlan {
    pub plan_id: u128,
    pub risk_group_id: u64,
    pub plan_version: u64,
    pub target_mm_buffer_atoms: i64,
    pub max_loss_atoms: i64,
    pub legs: FixedLiquidationLegs,
    pub state: LiquidationPlanState,
}
```

Coordinator 分批执行并保留 residual hedge addon。若 paired legs 不能原子执行：

- 先执行流动性更可靠或能降低尾部风险的一腿；
- 对未成交另一腿立即提高临时 requirement；
- 使用限价保护、节流和最大 participation rate；
- 价格越过保护带时停止冲击盘口，转入 takeover/auction/fund 路径。

### 13.5 第三步：抵押物变现

“关仓去风险”和“卖抵押物结算”不能混在一起。只有已实现的 settlement-currency deficit 需要 collateral waterfall：

```text
settlement cash
  -> liquid stable collateral
  -> liquid blue-chip crypto
  -> long-tail crypto
  -> in-session realizable RWA
  -> instrument collateral / Prediction
  -> fund takeover or delayed auction
```

排序同时考虑流动性、haircut、市场冲击、法律可转让性和它对剩余组合的 hedge 作用。每卖一层后重新计算，防止卖掉 hedge collateral 后再次恶化。

### 13.6 Trigger、fill 和 settlement

必须区分：

- trigger：组合跌破 MM；
- fill：Liquidation order 的真实成交价；
- settlement：向用户和保险基金记账的价格政策。

LightningX 默认建议：

- 用户仓位按真实 liquidation fill 结算；
- 收取显式、可配置、可审计的 liquidation fee；
- 剩余权益返还用户；
- 保险基金靠 liquidation fee、交易费分成和资本注入补充；
- 只有明确采用 bankruptcy takeover 的产品才按破产价接管，不能隐式没收 LM remainder。

该选择资金积累较慢，但账务透明，尤其适合多资产 PM。保险基金规模必须按 tail bad-debt 建模，不能依赖平稳时期没收用户剩余权益。

### 13.7 Oracle 故障

- 可疑或 stale 价格不能触发不可逆爆仓。
- 轻度 stale：扩大场景和 haircut，限制新增风险。
- 严重 stale/divergence：停止新增风险和该 factor 的系统爆仓，允许安全自救。
- 继续记录潜在 breach 和严重度，价格恢复后重新确认，不回放基于错误价格的爆仓决定。

### 13.8 爆仓级联

- 默认 partial liquidation。
- 按市场冲击限制批次大小和 participation rate。
- 使用 mark-relative protection band。
- 按 breach severity 和首次 breach 时间公平排序。
- 达到 cascade threshold 后收紧 IM、限制新增风险或触发市场 circuit breaker。
- 检测仍可高速运行；执行速度必须受可用流动性约束。

## 14. 保险基金与 ADL

### 14.1 两层保险基金

```text
per-risk-pool fund
  -> product-class fund
  -> global backstop
  -> ADL
```

Prediction/RWA 等高跳跃产品不能无上限地消耗主流币永续保险基金。每个 risk pool 有风险预算、watermark 和最大调用 global fund 的额度。

watermark 至少取以下最大值：

- OI 比例 floor；
- 经压力测试的 tail loss；
- sequencer/network outage backlog loss；
- 最近 drain-rate buffer。

### 14.2 Default attribution

PM bad debt 可能来自多个产品。必须按 scenario marginal loss、真实 fills 和 collateral shortfall 归因到 risk pool，不能随意把全部损失记到最后成交的产品。

### 14.3 ADL

ADL 是最后手段。候选必须：

- 对目标 underlying 或 risk factor 持有反向净风险；
- 有可兑现利润；
- hypothetical ADL 后自身不会跌破 MM；
- 关闭该腿不会拆掉关键 hedge 并引起新的爆仓；
- 满足产品/法人/清算域限制。

基础排序可以使用：

```text
profit ratio × effective leverage × systemic contribution
```

但选择前必须运行 candidate portfolio revaluation。执行按所需缺口 take-until-satisfied，支持 lot rounding，结果必须确定性、可重放和可解释。

## 15. 性能设计

### 15.1 热路径目标

目标按路径分层，而不是承诺一个不现实的单值：

- 独立 factor / fully-funded spot / strict reduce：single-digit microseconds 的 Risk Engine 计算。
- 相关 subspace：数十微秒内。
- 大型复杂组合 full reval：数十到数百微秒，超过预算触发专用大账户队列或 reject/retry。
- 网络延迟单独计量；PM 计算与端到端 order-to-ack 不混为一个指标。

### 15.2 数据布局

- 固定点 `i64/i128`，禁止风险计算使用 `f64`。
- SoA，按 factor/product bucket 连续存储。
- arena index + generation，禁止热路径 `Box`。
- 固定容量 SmallVec/数组；容量耗尽时安全扩容，但扩容放在 shard event boundary，记录延迟指标。
- 预编译 scenario 与 product config。
- SIMD scenario accumulation。
- NUMA aware placement、CPU pinning、huge pages。
- snapshot IO 与重估线程分离，避免争抢内存带宽。

### 15.3 调度

Risk Shard 本地维护：

```text
factor -> portfolio indices
```

PriceUpdate 流程：

1. 合并微小 tick，累计越过 band 后唤醒。
2. 仅访问订阅该 factor 及其压力相关邻域的 portfolio。
3. 用保守 exposure bound 判断是否远离 MM。
4. 无法证明安全或接近 MM 的组合执行 full reval。
5. 周期性 full sweep 捕获微小漂移、funding 和 staleness。

高风险组合的有效重估频率自然高于健康组合。

### 15.4 网络

多服务器架构无法让所有 RiskGroup 与所有 Matcher 同机。建议：

- 主 AZ 内 25/100 GbE。
- Aeron UDP 或经实测的 kernel-bypass transport。
- Gateway 直接路由到 Risk Leader，避免无意义的 Desk 转发链。
- Risk Leader 直接发送 Matcher command；Ingress 只接收用户响应。
- Risk Edge 可以与 Matcher 同机做 permit 验证和静态拒绝，但不能本地批准风险。
- 大型流动性提供者可固定到专用 Risk Shard/CPU。

推荐的延迟部署档位：

| 场景 | 路径 | 取舍 |
|---|---|---|
| 高频主流账户 | Ingress、Risk Leader、Matcher 同机或同 rack；standby 跨主机同 rack | 最低延迟，同时保留单机故障保护 |
| 普通 PM 账户 | Ingress 直连 Risk Leader；Risk Leader 直连目标 Matcher | 不增加中间 Desk hop，允许跨机 |
| 超大组合 | 独占 Risk Shard/CPU，必要时独立节点 | 隔离 full reval 尾延迟，不拖慢普通账户 |
| 跨地域灾备 | 异步 DR，不参与 permit quorum | 不把广域网 RTT 加到订单热路径 |

端到端性能预算必须分别记录 `ingress_to_risk`、`risk_compute`、`risk_replication`、`risk_to_matcher` 和 `matcher_ack`。这样才能判断损耗来自 PM 算法、复制还是拓扑；只报告一个 order-to-ack 数字无法指导优化。

## 16. 配置与治理

风险配置采用 versioned immutable bundle：

```rust
pub struct RiskConfigBundle {
    pub version: u64,
    pub instruments: CompiledInstrumentTable,
    pub factors: CompiledFactorTable,
    pub scenarios: CompiledScenarioTable,
    pub correlations: CompiledCorrelationBlocks,
    pub collateral: CompiledCollateralTable,
    pub liquidation: CompiledLiquidationTable,
    pub fund_limits: CompiledFundTable,
}
```

流程：

1. 离线校验和 backtest。
2. 发布 candidate version。
3. 所有 Risk Shard 加载并 checksum。
4. 指定 shard sequence barrier 原子激活。
5. 日志记录每次决策所用版本。
6. 保留上一版用于重放和快速回滚。

配置加载失败不能退回随意默认值。新增产品若缺少 scenario、oracle、staleness、liquidation 和 collateral eligibility 配置，则禁止交易。

## 17. Beacon 监控与告警

### 17.1 路由与 HA

- `risk_shard_leader_epoch`
- `risk_shard_failovers_total`
- `risk_shard_route_mismatch_total`
- `risk_shard_replica_lag_events`
- `risk_shard_replica_lag_ns`
- `risk_journal_append_latency_ns`
- `risk_snapshot_age_seconds`

### 17.2 下单与 reservation

- `risk_check_latency_ns{path=fast|subspace|full}`
- `risk_check_total{decision,reason,product}`
- `risk_reservations_active`
- `risk_reservations_uncertain`
- `risk_reservation_oldest_age_seconds`
- `risk_permit_rejected_total{reason}`
- `risk_order_event_gap_total{matcher}`
- `risk_reconciliation_break_total`

### 17.3 模型与行情

- `risk_factor_age_ms{class}`
- `risk_factor_divergence_bps`
- `risk_scenario_calc_ns`
- `risk_full_reval_total{reason}`
- `risk_worst_scenario{block}`
- `risk_offset_credit_atoms`
- `risk_concentration_addon_atoms`
- `risk_stale_addon_atoms`
- `risk_config_version{shard}`

### 17.4 爆仓与基金

- `risk_groups_below_mm`
- `liquidation_plan_total{state,product}`
- `liquidation_detection_to_first_order_ns`
- `liquidation_slippage_bps`
- `liquidation_self_cascade_avoided_total`
- `liquidation_queue_depth`
- `insurance_fund_balance{pool}`
- `insurance_fund_watermark{pool}`
- `insurance_fund_drain_rate`
- `adl_candidates`
- `adl_notional_atoms`

必须告警：

- 同一 virtual shard 同时出现两个 leader。
- 任一 Matcher event sequence 出现无法自动补齐的 gap。
- Risk 与 Matcher open order/reservation 对账不一致。
- Risk 与 Ledger collateral 对账不一致。
- 主流 factor 超过 staleness threshold。
- `Uncertain` reservation 超过 SLA。
- standby 落后超过允许接管窗口。
- 保险基金低于动态 watermark。

## 18. 对当前仓库的改造映射

当前组件 | 目标改造
---|---
`src/desk/counter_shard.rs` | 从固定 4 shard 改为 `risk_group_id -> virtual shard -> directory route`
`src/desk/risk/engine.rs` | 拆出确定性 `risk-core`，保留无 IO 计算和状态机
`src/bin/desk_server.rs` | 移除权威 RiskEngine，改为 Risk Client、permit 转发和客户端事件聚合
`src/bin/exchange_engine.rs` | 强制验证 RiskPermit，输出有序幂等 order/execution events
`src/desk/risk_persist.rs` | 改为 risk journal + query projection，不再把 PG 当热状态恢复来源
`src/desk/index_price.rs` | 扩展为独立 Market Risk Data Service 输入
`src/transport/counter_forward.rs` | 逐步由通用 Risk Router/response route 替代
当前 liquidation tick | 改为 PM breach detector + versioned LiquidationPlan saga
当前 insurance/ADL | 增加 risk-pool 隔离、watermark 和 PM-aware candidate revaluation

建议新增模块/进程：

```text
src/risk_core/
  portfolio.rs
  reservation.rs
  factor.rs
  scenario.rs
  valuation.rs
  collateral.rs
  liquidation.rs
  insurance.rs
  adl.rs

src/risk_protocol/
  command.rs
  permit.rs
  event.rs
  snapshot.rs

src/bin/pm_risk_node.rs
src/bin/risk_market_data.rs
src/bin/liquidation_coordinator.rs
```

第一阶段可以仍留在一个 Cargo package 中，确认边界和协议稳定后再拆 workspace crate，避免过早制造构建复杂度。

## 19. 实施路线

### Phase 0：不改变保证金结果，先建立边界

- 引入 `RiskGroupId`、`InstrumentId`、`FactorId`。
- 引入 4096 virtual shards 和 RiskShardDirectory。
- 定义 RiskCheck、RiskPermit、MatcherEvent、LedgerEvent。
- 为 reservation 和 fill 建立幂等键及 sequence。
- 保持现有逐仓公式，验证行为不变。

验收：同一 RiskGroup 在任意 Desk 下单都只由一个 Risk Owner reserve；故障注入下无双花。

### Phase 1A：在 desk-server 内建立独立逻辑 Risk Shard

- 将当前共享 `RiskEngine` 改为单写 shard event loop，Desk 只能通过 RiskCheck 协议请求授权。
- 建立 journal、snapshot、reservation、幂等、sequence 和 epoch fencing。
- Matcher 强制要求 permit。

验收：即使物理上仍共进程，任意 Desk 并发下单也只有一个逻辑写者；重放结果一致；不重复 fill、不错误释放 uncertain order。

### Phase 1B：同机 PM Risk sidecar

- 把同一 Risk Shard worker 移入独立 `pm-risk-node` 进程。
- `desk-server -> pm-risk-node` 使用 Aeron IPC；协议和状态机保持不变。
- Risk Node 独立绑核、内存预算、发布与重启；standby/replica 位于另一台故障主机。
- 按 RiskGroup canary 迁移，禁止两个 RiskEngine 同时拥有写权限。

验收：进程 kill -9 后可恢复；Desk 与 Risk Node 可独立升级；同机 sidecar 的 p99 额外成本在实测预算内。

### Phase 1C：按指标迁移到跨机 PM Risk Cluster

- 仅在 PM 计算挤压 Desk、需要 whale 隔离、独立故障域或独立扩容时迁移。
- 只改变 RiskShardDirectory 的物理 placement，不改变 RiskCheck/RiskPermit/MatcherEvent 协议。
- Ingress 直接路由 Risk Leader，Risk Leader 直接投递 Matcher，不增加中心 Router 或 Product Desk 中转。

验收：kill -9 leader 后从 standby 接管；同步模式不丢 reservation；完成 recovery barrier 前 fail closed；端到端仍满足产品 SLO。

### Phase 2：统一仓位和抵押物模型

- 先接 Spot + Perpetual + Futures。
- 所有产品进入统一 RiskGroup 状态。
- 暂时不提供跨产品 offset，PM requirement 等于保守分项之和。
- 建立 Risk/Ledger/Matcher 三方连续对账。

验收：新架构结果不低于旧模型；shadow 模式差异可解释。

### Phase 3：Scenario kernel

- 实现 factor registry、SoA、固定 scenario arrays。
- 实现 open-order scenarios、correlation blocks、jump scenarios 和 add-ons。
- 运行 shadow PM，不影响真实下单。
- 对真实组合分布做 latency、false-negative 和 tail-loss backtest。

验收：任何 fast screen 跳过 full reval 的案例都能由保守数学界限证明安全；压力测试无低估。

### Phase 4：RWA 与 Prediction

- 接入 session/halt/corporate-action 和 issuer risk。
- 接入 outcome/resolution/dispute 状态。
- 默认使用高 haircut、低 offset、严格集中度。
- 建立不可变现 collateral 的 takeover/auction 流程。

验收：闭市、halt、oracle dispute、jump-to-zero 和 invalid market 演练通过。

### Phase 5：PM enforcement

- 员工账户 -> 白名单账户 -> 小额度 -> 主流产品 -> 全量。
- 每一步对比 isolated/cross/PM requirement 和真实 PnL。
- 先提供较少 offset，再根据压力测试逐步放宽。
- 独立开关控制产品、RiskGroup 和 offset block。

### Phase 6：PM liquidation / fund / ADL

- 在 shadow 中运行 liquidation planner。
- 演练跨 Matcher partial fill、单腿失败、oracle stale、sequencer outage。
- 上线 PM-aware liquidation，再上线 fund attribution。
- ADL 最后上线，并保留人工 circuit breaker。

## 20. 上线门槛

PM 进入真实资金前至少满足：

1. 所有新增风险都需要不可伪造、不可复用的 RiskPermit。
2. 同一 RiskGroup 不存在双 leader、双 reservation 或跨 Desk 本地批准。
3. Matcher event 和 Ledger event 可从 journal 完整重放。
4. 对 Accepted/Fill/Cancel/Reject/Expire 的任意重复与乱序测试保持资金守恒。
5. Open-order scenario 不把非原子 hedge 当成已完成 hedge。
6. Stale/divergent oracle 不触发不可逆爆仓。
7. Liquidation 每一步都执行组合重估并检测 self-cascade。
8. Insurance fund、global backstop 和 ADL 的损失归因可以解释和审计。
9. Risk/Ledger/Matcher 自动对账通过持续 chaos test。
10. p99/p99.9 延迟、full-reval 比例、replica lag 和 uncertain reservation 均有 Beacon SLO。

## 21. 待业务与 Quant 决策

以下内容不能由工程实现自行假设：

- 哪些账户/子账户允许归入同一 RiskGroup。
- 合格 collateral 范围及法律/托管隔离边界。
- correlation calibration、危机相关性和 offset cap。
- 各产品 IM/MM scenario 强度。
- Prediction jump/outcome 和 dispute 定价。
- RWA issuer、session、halt 和 corporate-action 策略。
- liquidation settlement 使用真实 fill 还是 bankruptcy takeover。
- liquidation fee、保险基金分池及 global backstop 调用规则。
- ADL 的产品隔离、候选评分和分配方式。

工程侧负责保证这些参数是版本化、可表达、可回放、可灰度和 fail-closed 的；Quant/Risk 负责模型形式与参数，业务和治理负责 eligibility、额度与用户政策。

## 22. 最终设计原则

- Risk ownership 跟用户组合走，不跟 symbol 或 Desk 走。
- 一个 RiskGroup 一个顺序写者；跨 Matcher 使用 reservation saga，不做分布式事务。
- PM 是“相同 shocked state 下的整组合重估”，不是分项保证金相加后打折。
- 统一产品抽象不能以指针追逐和动态分派牺牲热路径性能。
- 任何 offset 都必须有危机场景、上限和可解释来源。
- 只有严格可证明的 risk-down 可以绕过 IM；产品标签不能代替风险变化计算。
- PM 爆仓是账户级决策、多腿执行；不存在可靠的单一 liquidation price。
- 关闭仓位、变现抵押物、保险基金和 ADL 是四个不同阶段。
- 无法确认状态时保留 reservation 并 fail closed，绝不凭超时释放资金。
- 先上线状态一致性和保守 PM，再逐步开放资本效率。
