# Portfolio Margin 多方架构评审决策记录

> 日期：2026-09-29
>
> 状态：Accepted，作为 [LightningX Portfolio Margin 系统设计](portfolio-margin-system-design.md) 的实施补充
>
> 目的：让后续开发与评审 Agent 使用同一组架构结论、约束和验收标准

## 1. 评审问题

LightningX 当前同时按 coin/product 划分 Desk 和 Matching Engine，而 Portfolio Margin 跨 coin、跨产品、跨 Matcher。评审集中回答四个问题：

1. PM Risk Engine 是否必须独立？
2. 如何在不破坏风险一致性的前提下减少正常下单时延？
3. 完整爆仓逻辑对所有权、数据流和部署有什么要求？
4. 应立即跨机部署，还是渐进式拆分？

评审从三个方向独立进行：低延迟数据面、风险/清算安全、现有 Rust/Aeron 系统迁移。最终结论不是多数投票，而是合并三方共同约束，并对复制策略的分歧做交叉评审。

## 2. 最终裁决

### 2.1 必须立即独立的是逻辑所有权

一个 `RiskGroup` 任一时刻只能由一个 `PM Risk Shard Leader` 修改。这个所有权不能由 BTC/ETH/SOL Product Desk 身份推导，也不能由多个 Desk 共享。

逻辑角色固定为：

```text
Ingress / Product Desk
  = 鉴权、连接、静态校验、客户端响应

PM Risk Owner
  = RiskGroup 唯一写者、reservation、permit、PM、爆仓决策

Matcher
  = 订单簿与成交事实权威、O(1) permit 验证

Ledger
  = 资金与财务事实权威

Liquidation Executor
  = 执行受限清算许可，不拥有风险决策权
```

### 2.2 物理部署渐进拆分

长期目标是独立、有状态、可横向分片的 PM Risk Cluster，但不要求第一天跨机器：

```text
阶段 A：desk-server 内的单写 Risk Shard worker
阶段 B：同机独立 pm-risk-node sidecar，Aeron IPC
阶段 C：按指标迁移到跨机 PM Risk Cluster
```

“同机 sidecar”表示 `desk-server` 与 `pm-risk-node` 是同一物理服务器上的两个进程。它提供进程、CPU、内存和发布隔离，但不提供主机故障隔离；同步副本必须位于另一台故障主机。

## 3. 最短安全数据流

### 3.1 新单与增险改单

```text
Client
  -> Ingress（本地 RiskShardDirectory 缓存）
  -> PM Risk Shard Leader
       -> incremental risk calculation
       -> reservation
       -> replication commit（默认 PM 模式）
       -> RiskPermit
  -> Target Matcher
  -> Matcher journal
       -> PM Risk Owner
       -> Client Ingress
```

Risk Owner 批准后直接向目标 Matcher 投递，禁止 permit 返回 Ingress 后再经过 Product Desk 转发。所谓 PM Router 只能是 Ingress 内的目录/client library，不能成为独立网络 hop。

### 3.2 撤单

```text
Ingress -> Matcher
```

撤单请求可以绕过 Risk Owner，但只有 Risk Owner 收到确定性的 `Cancelled/Rejected/Expired` Matcher 事实后才能释放 reservation。RPC 超时、发出 cancel 或 permit 到期都不能证明订单不存在。

### 3.3 Matcher 事件

每个 Matcher 提供自己的严格 source sequence。不同 Matcher 不依赖跨机器时间戳建立全局顺序；Risk Leader 按实际进入单写 mailbox 的顺序写 shard journal，并在重放时复用该顺序。

## 4. 部署决策与迁移门槛

### 4.1 阶段 A：进程内逻辑解耦

不改变保证金公式，先引入：

```text
RiskGroupId
VirtualRiskShardId
LeaderEpoch
ReservationId
RiskCheck / RiskPermit
MatcherEventSequence
RiskJournal / Snapshot
```

现有 `DashMap` 可以保留为查询投影，但不能继续承担多写者权威状态。目标是先证明资金守恒、幂等、重放和 fencing。

### 4.2 阶段 B：同机 sidecar

```text
desk-server --Aeron IPC--> pm-risk-node --Aeron IPC/UDP--> Matcher
```

收益：

- Risk Node 可独立绑核、限资源、升级和重启。
- WebSocket、行情与 Tokio 调度不再直接干扰 Risk event loop。
- Desk 故障后 Risk Node 可以继续消费成交和管理风险。
- 以低 IPC 成本验证真实跨进程协议。

限制：主机断电、内核故障和网卡故障仍同时影响两者。

### 4.3 阶段 C：跨机 Risk Cluster

满足任一条件后迁移：

- PM scenario/factor 更新开始挤压 Desk CPU 或 p99。
- Desk 故障导致爆仓中断已不可接受。
- 巨鲸账户需要独占 shard/core/内存。
- Risk 热点与 Product Desk 热点明显不同。
- 风险模型需要独立发布、审计或故障域。
- sidecar 已通过 replay、fencing、chaos 和延迟验收。

跨机迁移只改变 `RiskShardDirectory` placement，不应改变资金协议。

## 5. 权威事实、通知完整性与复制策略

### 5.1 不追求所有数据流同步完整

客户端 WebSocket 是低延迟通知层，不是订单和账本的权威日志。允许断线、重复和个别事件缺失，客户端通过 `client_order_id`、sequence、REST snapshot/query 和 high watermark 做 reconciliation。WebSocket、PostgreSQL、Redis、报表或审计索引发送失败都不能阻塞或回滚撮合。

可靠性分层如下：

| 层级 | 示例 | 保证 |
|---|---|---|
| Tier 0 权威事实 | Matcher accepted/fill、Ledger、settlement、funding、insurance/ADL | 不允许最终丢失，可重放、去重、查询 |
| Tier 1 可重建状态 | Risk reservation、scenario、available-margin cache | 允许内存丢失，恢复完成前冻结新增风险 |
| Tier 2 查询投影 | PostgreSQL、Redis、历史索引 | 允许短暂滞后，必须追平并暴露 watermark |
| Tier 3 通知 | Private WebSocket、余额和订单推送 | best effort，允许缺失和重复 |

`accepted` 响应必须表示订单已进入可恢复的权威顺序域，而不是只进入临时网络队列。用户没有收到 WebSocket 更新时，可以查询权威订单状态；查询层若尚未追平指定 watermark，必须返回 lag/重试语义。

行业公开接口只能证明客户端通知需要容错，不能据此推断交易所内部允许丢失订单或账务事实。OKX Orders channel 不提供订阅初始快照，连接异常需要重连，其更新日志还要求客户端处理重复订单消息；Binance User Data Stream 明确要求预期定期断线，同时提供订单 REST 查询接口。参考：[OKX API](https://www.okx.com/docs-v5/en/)、[OKX API changelog](https://www.okx.com/docs-v5/log_en/)、[Binance User Data Stream](https://developers.binance.com/en/docs/products/derivatives-trading-coin-futures/user-data-streams)、[Binance Portfolio Margin REST API](https://developers.binance.com/en/docs/catalog/advanced-trading-derivatives-trading-portfolio-margin/api/rest-api/trade)。

### 5.2 只保留一个可靠核心提交点

不能未经评估地把两个同步可靠性屏障串联在每笔订单上：

```text
Risk quorum commit -> Matcher durable/replicated accept
```

系统支持两种明确模式，并记录在 RiskGroup/shard 配置与 journal 中。

#### `LATENCY_FIRST`

```text
Risk Leader memory reserve
  -> issue permit
  -> Matcher reliable accept（唯一核心提交点）
  -> order may enter book/match
  -> Risk standby asynchronous catch-up
```

Matcher 必须在订单可能产生经济效果前可靠记录完整 permit、reservation、约束和 order state。Risk Leader 故障后冻结整个 shard，从 Matcher/Ledger 权威日志重建，再恢复交易。这是用故障期可用性和 RTO 换取正常路径延迟。

适用于健康、低杠杆、主流币 PM 或对延迟敏感且接受 shard 故障后暂停的专业账户，但只有 Matcher journal/replication 通过故障验收后才能启用。

#### `FAST_RECOVERY`

```text
calculate
  -> append reservation delta
  -> leader + fastest follower commit（2/3）
  -> issue permit
  -> Matcher
```

它避免 Leader 在 permit 发出后丢失 reservation，并提供较短 RTO，但可能与 Matcher commit 形成两次串行屏障。适用于高杠杆、巨鲸、复杂期权/RWA/Prediction、接近 MM 或 Liquidating 的 RiskGroup。副本位于同 AZ、优先同 rack 的不同主机；跨地域仅异步 DR。

模式只能在明确的 RiskGroup sequence barrier 上切换，切换前排空或保守计入全部 in-flight permit，并持久化 mode/version；禁止逐笔根据当前延迟临时切换。

### 5.3 `LATENCY_FIRST` 的安全前提

异步 Risk reservation 复制会把安全证明转移给 Matcher、Ledger 和恢复协议，必须同时满足：

1. Matcher 在订单能进入订单簿前可靠记录完整 permit、reservation 和 order state。
2. Fill 有不可丢失、可查询的 execution record。
3. failover 冻结整个 Risk Shard 的新增风险、提现、转账和额度返还。
4. 新 Leader 对所有可能接收旧 permit 的 Matcher 安装新 epoch fence。
5. 对所有 Matcher 与 Ledger 完成 sequence barrier 和 reconciliation。
6. 任一 Matcher 不可达时继续 fail closed，不能推断“没有订单”。
7. 能证明所有旧 permit 要么未被接受且已 fencing，要么已重建为 reservation/position。

因此该模式不是降低数据完整性，而是把权威提交点移到 Matcher，并用更长 RTO、更低故障期可用性和更复杂恢复换取少一次复制 RTT。Matcher journal 不满足可靠接受语义前，必须使用 `FAST_RECOVERY`。

## 6. 爆仓系统不变量

以下规则不得被性能优化绕过：

1. 一个 RiskGroup 同一时刻只有一个风险写者。
2. 任何增险 Matcher command 发出前必须有足额 reservation。
3. reservation 未证明订单不存在前绝不能释放。
4. 已发出但尚未 terminal 的清算订单必须按最大可能成交量进入下一轮风险计算。
5. 多腿清算必须验证任意成交子集，而不是假设各腿同时成交。
6. 每轮 Fill 后由 Risk Owner 重估整个组合，再决定继续、换腿或停止。
7. Liquidation Executor 不能基于旧快照自行连续发单。
8. Fill、funding、settlement、collateral、withdrawal 与 insurance/ADL 进入同一 RiskGroup 顺序域。
9. leader 切换后先完成 recovery barrier，再恢复新增风险。
10. stale oracle 不能直接使用 last trade 做不可逆清算。

每个 LiquidationPlan 至少跟踪：

```text
plan_id / plan_version / risk_epoch
filled_quantity
outstanding_max_quantity
cancel_pending_quantity
remaining_authorized_quantity
factor_snapshot_version
```

Liquidation Executor 只能使用绑定 plan/version/instrument/side/max quantity/protection price/expiry 的 `LiquidationExecutionPermit`。

## 7. 性能结论

不可避免的成本：

- 到唯一 Risk Owner 的一次传输。
- RiskGroup 内串行化与增量 PM 计算。
- 当前 durability mode 定义的一个可靠核心提交点。
- Risk Owner 到 Matcher 的一次传输。

必须消除的成本：

- Product Desk 中转。
- 独立中心 Router。
- permit 返回 Ingress 后再次转发。
- 热路径数据库或远程行情 RPC。
- 全 portfolio 扫描和动态内存分配。
- `DashMap`/共享锁争用和动态分派。
- 跨地域同步复制。
- 普通账户执行完整 PM scenario。
- full revaluation 或爆仓任务阻塞 Fill/Cancel 事实处理。

建议事件优先级：

```text
Fill/Ledger facts
  > liquidation state
  > Cancel terminal facts
  > new order admission
```

大型组合使用独占 shard/core；重估 worker 可以使用带 `portfolio_version` 的不可变快照，但只有版本仍匹配时结果才能提交。

## 8. 高频做市优化顺序

第一版不实现通用跨 Product Risk Lease。PM 风险可能非线性且不可分，BTC Desk 与 ETH Desk 不能重复消费同一 hedge offset。

优化顺序：

1. 先实现直接 Risk Owner 路径并分段测量。
2. 优化固定编码、预分配、SoA、增量 scenario、CPU/NUMA placement。
3. 为高频报价研究 matcher-scoped batch permit。
4. 最后才研究硬切割、不可重叠、可 fencing 的跨 Product lease。

Batch permit 必须限定单个 Matcher/产品、总数量、价格范围、有效期和最大 open-order exposure；Matcher 只能单调消费，不能本地返还并复用额度。

## 9. 明确否决

- 指定 BTC 或其他 Product Desk 作为 PM Owner。
- 多个 Product Desk 保存可写 PM 副本。
- active-active Risk Owner 或分布式锁扣保证金。
- 完整组合复制到 Matcher 并允许其本地批准。
- permit 未达到所选复制提交点就按同步模式发单。
- 超时、cancel request 或 permit expiry 自动释放 reservation。
- 仅凭客户端 `reduce_only` 绕过组合风险检查。
- Coordinator 基于旧快照连续清算。
- 多腿没有原子语义却提前给予完整 offset。
- 使用数据库/Redis 投影做下单或提现权威判断。
- 保险基金与 ADL 最终一致地“稍后补账”。

## 10. 验收门槛

### 正确性

- Accepted/Fill/Cancel/Reject/Expire 任意重复、乱序后资金守恒。
- WebSocket 随机丢失、重复和断线后，客户端能以 sequence + REST snapshot/query 恢复一致状态。
- `accepted` 响应前订单已进入当前模式定义的权威提交点。
- 旧 epoch permit 永远不能进入 Matcher。
- replay 得到完全一致的余额、仓位、reservation 和 liquidation state。
- 任意时点 kill -9 不会双重预留、双重释放或盲目继续爆仓。
- Risk、Matcher、Ledger 持续自动对账。

### 性能

- 普通账户路径回归不超过 5%。
- 同机 sidecar p99 额外成本目标为数微秒级。
- 同 rack PM 常见额外成本目标为几十微秒，而不是几百微秒。
- 总 order-to-ack 继续满足约 200 µs 的产品目标。
- Full revaluation 与 liquidation 不显著恶化普通请求 p99.9。

以上数字是验收目标，不是当前实测结论；必须分别记录：

```text
ingress_to_risk
risk_queue
risk_compute
risk_replication
risk_to_matcher
matching
matcher_to_ack
```

### 可用性

- 对 reservation 创建前后、permit dispatch 前后、Accepted/Fill 前后执行故障注入。
- 同步模式下已签发 permit 的 reservation RPO 为零。
- `LATENCY_FIRST` 故障后必须冻结整个 shard；所有 Matcher/Ledger barrier 完成前不得恢复新增风险。
- 接管期间可以拒绝新增风险，但不能乐观放行。
- Aeron sequence gap 必须补齐或冻结对应 shard。

## 11. 给后续 Agent 的工作规则

1. 先维护逻辑所有权不变量，再讨论部署位置。
2. 不要把 Product Desk、Risk Owner、Matcher 或 Ledger 的权威性混在一起。
3. 新方案必须说明 reservation 在何时达到 commit point。
4. 新方案必须区分权威事实、可重建状态、查询投影和 best-effort 通知。
5. 新增低延迟优化必须说明断线、迟到 Fill、leader crash 和 replay 行为。
6. 涉及爆仓时必须同时计算所有 outstanding liquidation exposure。
7. 性能比较必须与“现有非 owner counter-forward 路径”及“进程内最优路径”分别比较。
8. 未通过真实基准和故障注入前，不把设计目标写成已达到的性能事实。

本记录若与 PM 总设计冲突，以更新日期更晚且经过明确评审的决策为准；修改结论时必须同步更新两份文档。
