# LightningX 交易核心与 Portfolio Margin 设计背景

> 日期：2026-09-29
>
> 状态：Accepted，后续架构和实现工作的共同背景
>
> 相关：[Portfolio Margin 系统设计](portfolio-margin-system-design.md)、[多方架构评审决策记录](portfolio-margin-brainstorm-decisions.md)

## 1. 为什么记录这份背景

LightningX 是新系统，没有大型交易所长期积累的协议兼容、消息中间件、账户模型和部署拓扑包袱。我们的目标不是机械复制 Binance、OKX 或传统交易所，而是在吸收其经验后，从第一版建立更小的排序范围、更清晰的状态所有权和更容易水平扩展的架构。

“更好”必须同时满足：

- 更低且更可预测的 p50/p99/p99.9 延迟；
- 更高的跨币种和跨账户吞吐；
- 不低于成熟交易所的业务可用性与用户恢复能力；
- 订单、成交和账务事实可确定性恢复；
- 故障被限制在单个 Matcher/Risk Shard，而不是全平台；
- 能通过增加 Matcher/Risk Node 横向扩展；
- 实现和运维复杂度可以被测试、监控和自动化控制。

## 2. 行业背景信息

以下信息来自团队获得的一手行业交流，可作为内部架构背景，但不是公开材料，不应在对外文档、市场宣传或第三方比较中写成可公开验证的事实：

- Binance 的部分核心内部消息链路使用 Kafka，并存在不按币种分区、多个币种共享串行顺序的设计；这有利于全局排序和恢复，但限制并行度，并带来通用 broker 的延迟与尾延迟成本。
- OKX 早期使用 Kafka，后续将部分关键交易基础设施迁向 Aeron Cluster/Archive，以更低延迟的复制日志、确定性状态机和 replay 替代通用消息中间件。

公开资料只能确认 Binance 广泛使用 Kafka、Aeron 将 OKX 列为采用者，以及 Aeron Cluster/Archive 的能力；不能公开证明上述系统的精确拓扑和迁移范围。后续 Agent 应把这两条视为可信内部输入，而不是可外部引用的证据。

这些信息不意味着 LightningX 必须复制任一模式。它们说明两种共同需求始终存在：

```text
低延迟、高吞吐
        versus
全局顺序、恢复简单、强一致
```

我们的设计目标是在真正需要顺序的最小范围内保持确定性，而不是把所有业务塞进一条全局日志。

## 3. 从两类架构中学习什么

### 3.1 全局 Kafka 串行日志

可能获得：

- 单一全局顺序；
- 跨币种资金和风险处理简单；
- replay、审计和故障定位直接；
- 不需要复杂的跨 Matcher 协议。

主要代价：

- Kafka broker 进入订单热路径；
- partition 成为吞吐上限；
- 热门币种影响无关币种；
- broker、page cache、batch 和 consumer 调度增加尾延迟；
- 增加服务器或 Matcher 不能自然线性扩展。

结论：学习它的确定性顺序和 replay，不复制全局 broker 与全币种串行。

### 3.2 全局或大范围 Aeron Cluster

可能获得：

- 比通用 broker 更低、更稳定的延迟；
- Raft replicated log、leader election 和快速接管；
- 确定性状态机；
- Archive snapshot/replay；
- 较清晰的强一致模型。

主要代价：

- 大范围共享 Raft log 仍可能成为全局排序瓶颈；
- 所有业务共同承担 quorum latency；
- 慢服务或热点可能扩大影响范围；
- Cluster 分片和迁移仍需要业务层设计。

结论：选择性使用 Aeron Cluster，不建立全交易所唯一 Cluster。

## 4. LightningX 的核心选择

### 4.1 局部顺序，不做全局顺序

系统只维护两类必须的严格顺序：

```text
instrument_id -> Matcher 单写顺序
risk_group_id -> PM Risk Owner 单写顺序
```

不同 RiskGroup、不同 Instrument 之间默认没有全局总序。它们通过 `RiskPermit`、`reservation_id`、source sequence 和 journal application order 建立必要的因果关系。

BTC 用户 A 的订单不应阻塞 ETH 用户 B；只有同一订单簿或同一 RiskGroup 的操作需要串行。

### 4.2 正交分片

| 域 | 分片键 | 权威职责 |
|---|---|---|
| Ingress | `connection_id` | 鉴权、连接、用户响应，不拥有交易状态 |
| Risk | `risk_group_id` | reservation、PM、爆仓决策 |
| Matching | `instrument_id` | 订单簿、价格时间优先、成交事实 |
| Ledger | `risk_group_id/account_id` | 资金和财务事实 |

这一设计比全局日志复杂，但允许 Risk 与 Matcher 根据不同热点独立扩展。

### 4.3 热路径不使用通用 Broker

正常订单路径：

```text
Ingress
  -> PM Risk Owner
  -> Target Matcher
```

热路径采用 Aeron IPC/UDP、SBE 固定编码、预分配 buffer 和直接 publication，不经过 Kafka、数据库、Redis、中心 Router 或通用 RPC Gateway。

Kafka 若保留，只用于不要求微秒级延迟的数据平台、CDC、分析、报表、离线风控和外部集成。

## 5. Aeron 组件的职责

### Aeron Transport

用于 Ingress、Risk、Matcher、Ledger 之间的直接低延迟传输。同机使用 IPC，跨机使用经过基准验证的 UDP/网络配置。

### Aeron Archive

用于记录和重放权威源事件：

- Matcher：Accepted、Fill、Cancel、Reject、Expire；
- Risk：Reservation、Portfolio、LiquidationPlan；
- Ledger：Deposit、Withdrawal、Fee、Funding、Settlement、Insurance/ADL。

实时通知可以缺失或重复，Archive/journal 必须支持按 position/sequence 重建。

### Aeron Cluster

选择性用于：

- `FAST_RECOVERY` PM Risk Cluster；
- 需要快速自动接管的 Matcher；
- Ledger 与保险基金等强一致财务状态机。

禁止把所有币种、账户、产品和非关键后台任务放进一个全局 Aeron Cluster log。

## 6. 可用性与性能的取舍

系统不要求所有传播路径百分之百完整，只要求权威事实不丢并可恢复：

```text
Tier 0：订单、成交、Ledger 等权威事实，可重放
Tier 1：Risk cache/reservation 等状态，可从权威事实重建
Tier 2：PostgreSQL/Redis/报表投影，允许短暂滞后
Tier 3：Private WebSocket 通知，允许断线、重复和缺失
```

用户收到 `accepted` 时，订单必须已经进入当前 durability mode 定义的权威顺序域；WebSocket 推送失败不能阻塞或回滚撮合。客户端通过幂等 `client_order_id`、sequence、REST snapshot/query 和 watermark 完成 reconciliation。

PM 支持两种模式：

- `LATENCY_FIRST`：Risk 内存 reserve，Matcher reliable accept 为唯一核心提交点；Risk 故障后冻结 shard，并从 Matcher/Ledger 恢复。
- `FAST_RECOVERY`：Risk reservation 先 quorum commit，再签发 permit；适用于高杠杆、巨鲸、复杂非线性产品、接近 MM 和 Liquidating 账户。

模式只能在 RiskGroup sequence barrier 上切换，禁止逐单临时改变 durability 语义。

## 7. 水平扩展模型

Risk 使用预建虚拟 shard：

```text
virtual_risk_shard = stable_hash(risk_group_id) % 4096
```

物理 Risk Node 承载一组虚拟 shard；扩容和热点治理只迁移目录中的部分 shard。Matcher 按 instrument 或 instrument family 部署；BTC、ETH、SOL、RWA、Prediction 不共享一个全局处理线程。

单个 RiskGroup 内存在不可避免的串行边界。巨鲸账户不通过拆成多个可写 Risk Owner 扩展，而通过：

- 独占 shard/core/arena；
- 增量 scenario 与 SoA；
- matcher-scoped batch permit；
- 必要时独占 Risk Node。

## 8. 爆仓是架构验收的一部分

爆仓不是后续附加功能。PM Risk Owner 必须：

```text
检测 breach
  -> 冻结增险
  -> 持久化 LiquidationPlan
  -> 签发一轮受限 LiquidationPermit
  -> Matcher 执行
  -> 计算实际 Fill + 全部 outstanding 最大暴露
  -> 重估整个 portfolio
  -> 停止、换腿或继续
```

用户断线、Ingress/Product Desk 崩溃或 WebSocket 缺失不能中断爆仓。Executor 不拥有独立风险决策权，多腿清算必须验证任意成交子集。

## 9. Greenfield 优势如何兑现

新系统没有历史包袱，但成熟交易所拥有大量真实事故积累的隐性规则。架构更先进不自动等于系统更可靠。LightningX 必须用自动化测试和运行数据弥补经验差距：

- 每个消息从第一版就有 idempotency key、source sequence 和 epoch；
- 所有状态机可以确定性 replay；
- 旧 epoch permit 被 Matcher fencing；
- Accepted/Fill/Cancel/Reject/Expire 任意重复乱序后资金守恒；
- 对 permit dispatch、Matcher accept、Fill、snapshot 和 failover 边界执行 kill -9；
- 模拟网络分区、迟到 Fill、不可达 Matcher 和 stale oracle；
- 持续执行 Risk/Matcher/Ledger reconciliation；
- shard placement 与迁移从目录驱动，不硬编码物理服务器；
- 每个发布版本都比较 p50、p99、p99.9、吞吐和恢复时间。

## 10. “比它们更好”的验收定义

设计不能在实现前宣称已经优于成熟交易所。只有满足以下内部标准后才能得出结论：

### 性能与吞吐

- 普通账户路径回归不超过 5%；
- PM 常见额外成本保持在产品预算内，目标为几十微秒而不是几百微秒；
- 增加独立 Matcher/Risk Node 后，无关 instrument/RiskGroup 吞吐近似水平扩展；
- 单一热门币种或巨鲸不能显著恶化无关 shard 的 p99.9；
- 热路径没有 Kafka、数据库、Redis 或中心 Router。

### 正确性与恢复

- Accepted 订单、Fill 和 Ledger 事实可确定性恢复；
- 任意故障点不会双重预留、双重释放、重复成交或盲目继续清算；
- `LATENCY_FIRST` failover 在 reconciliation 完成前严格 fail closed；
- `FAST_RECOVERY` 达到定义的 reservation RPO/RTO；
- WebSocket 丢失不影响 REST reconciliation 得到最终状态。

### 可用性与扩展

- 单 Matcher/Risk Shard 故障不导致全平台停摆；
- shard 可以在线迁移，旧 owner 被 epoch fencing；
- PM、Matcher 和 Ledger 可以独立扩容和发布；
- 故障影响范围、RTO 和用户可见行为有明确 Beacon SLO。

## 11. 给后续 Agent 的约束

1. 不把 Binance/OKX 的内部信息当作对外可引用事实。
2. 不因为某公司使用 Kafka/Aeron 就复制其拓扑；先解释所需的顺序范围和故障语义。
3. 不提出全局 Broker、全局锁或全交易所单一 Cluster 作为默认方案。
4. 新增组件必须说明它是否进入订单热路径，以及增加几个网络 hop、队列和 commit barrier。
5. 新方案必须保持 Instrument 与 RiskGroup 两个正交分片边界。
6. 任何降低延迟的方案必须说明权威提交点、failover freeze、replay 和 reconciliation。
7. 任何 HA 方案必须说明故障影响范围，不能只报告全平台平均可用性。
8. 性能目标必须通过真实 benchmark 证明，不得把设计目标写成已实现事实。

最终原则：继承成熟交易所的确定性、replay 和故障意识，但避免其全局串行与历史兼容包袱；只在真正存在业务因果关系的最小范围内同步。
