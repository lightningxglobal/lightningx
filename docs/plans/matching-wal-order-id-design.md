# LightningX 交易 WAL、提交点与合约内订单 ID 设计

> 日期：2026-09-29
>
> 状态：Accepted，代码迁移实施基线
>
> 相关：[Portfolio Margin 系统设计](portfolio-margin-system-design.md)、[高可用与一致性系统设计](high-availability-system-design.md)、[设计背景](portfolio-margin-design-background.md)、[多方评审决策](portfolio-margin-brainstorm-decisions.md)

## 1. 决策摘要

1. WAL 属于权威状态所有者，不属于每个网络入口或出口。
2. 普通 `LATENCY_FIRST` PM 订单只等待一个可靠核心提交点：Matcher reliable accept。
3. `FAST_RECOVERY` 额外等待 Risk reservation quorum；高风险和爆仓账户使用该模式。
4. WebSocket、PostgreSQL、Redis、报表和普通持久化投影不阻塞下单或撮合。
5. Aeron IPC `offer()` 成功只表示进入本机 term buffer，不等于 Archive recorded、fsync、远端复制或 quorum commit。
6. 订单 ID 不要求跨合约唯一，只要求同一 `instrument_id` 内全所唯一。
7. Matcher 是订单 ID 的唯一生成者；跨域引用使用 `(instrument_id, order_id)`。

## 2. 当前实现与性能报告的真实边界

当前端到端性能路径是：

```text
WS receive
  -> Desk decode/risk
  -> Aeron IPC
  -> Matcher
  -> Aeron IPC order update
  -> Desk WS write
```

PostgreSQL 不在 ACK 前同步提交。Matcher 事实由 `desk-server` 推入 `ArrayQueue<PersistFrame>`，独立 persist thread 发布到 Aeron `PERSIST_STREAM`，再由 pg-writer/redis-writer 异步消费。当前队列满时可以丢投影帧以保护订单响应时延，因此现有约 20 µs 指标不能表述为“包含 PostgreSQL WAL”。

当前 `exchange-engine` 已支持 input orders stream 的 Aeron Archive recording、snapshot 和 replay，但处理订单前没有等待 `recorded_position`、远端 replica position 或 quorum `commit_position`。因此当前能力是异步 journal/replay，不是 WAL-before-apply。

这两个事实同时成立：

- 当前测试证明将 PostgreSQL 投影移出热路径可以获得良好性能。
- 当前测试尚未测量可靠 Matcher commit 加入热路径后的成本。

## 3. WAL 位置与可靠等级

每条日志区分以下位置：

```text
published_position   已写入 Aeron term buffer
recorded_position    Archive 已记录到本机文件/page cache
replicated_position  远端副本已收到连续前缀
durable_position     达到配置定义的持久介质保证
commit_position      当前 durability mode 允许业务外显的位置
applied_position     状态机已顺序应用的位置
```

`offer()` 返回只推进 `published_position`。订单是否可以进入 OrderBook、返回 `ACCEPTED`，由 `commit_position` 决定。

| 等级 | 提交语义 | 抵抗范围 | 是否适合 ACK |
|---|---|---|---|
| L0 | 本机 Aeron term buffer | 无主机故障保证 | 否 |
| L1 | 本机 Archive/page cache | 进程故障，未必抵抗断电 | 仅测试/可重建状态 |
| L2 | 另一主机连续副本确认 | 单主机故障 | 普通 Matcher/Risk 推荐最低线 |
| L3 | 多副本持久介质确认 | 更强断电与节点故障 | Ledger/严格模式 |
| L4 | 跨 AZ/地域确认 | 机房故障 | 不进入订单热路径 |

纳秒级可以描述本机 memory append 或 counter poll，不能描述跨机 quorum 或 fsync。具体延迟必须在目标硬件实测。

## 4. WAL 域与等待规则

| 日志域 | 分片键 | 权威内容 | 正常路径是否等待 |
|---|---|---|---|
| Matcher WAL | `instrument_id` | OrderCommand、permit、订单簿确定性输入 | 是，至少一个可靠核心提交点 |
| Risk WAL | `risk_group_id` | reservation、portfolio、liquidation plan | `LATENCY_FIRST` 否；`FAST_RECOVERY` 是 |
| Ledger WAL | `risk_group_id/account_id` | 资金、费用、funding、settlement、insurance/ADL | 财务 ACK 前是 |
| Control WAL | shard/config | epoch、placement、配置、mode activation | 激活前是，不在逐单路径 |
| Market Factor Archive | `factor_id` | mark/index/factor/version | 否，风险决策引用 version |
| Projection Archive | publisher | PostgreSQL/Redis/报表事件 | 否 |
| Client WebSocket | 无 | 用户通知 | 不建权威 WAL |

### 4.1 Ingress

Ingress 收到请求时不做同步 WAL。进程在确定响应前崩溃时，客户端以同一个 `client_order_id` 查询或重试。

```text
ACCEPTED  已进入当前模式定义的权威顺序域
REJECTED  确定没有进入权威顺序域
UNKNOWN   无法判断，必须幂等查询/重试
```

### 4.2 Risk Owner

`LATENCY_FIRST`：

```text
memory reserve
  -> asynchronous Risk journal
  -> RiskPermit
  -> Matcher
```

`FAST_RECOVERY`：

```text
risk calculation
  -> ReservationCreated + DispatchIntent quorum commit
  -> RiskPermit
  -> Matcher
```

“出 Risk Owner”不增加单独同步 WAL。`DispatchIntent` 是 Risk 状态机的一部分，重启后按相同 `command_id/reservation_id` 幂等重发。

### 4.3 Matcher 入口

```text
OrderCommand + RiskPermit
  -> Matcher sequenced log
  -> selected commit_position
  -> assign contract-local order_id
  -> apply to OrderBook
  -> emit ACCEPTED/FILL
```

命令至少包含：

```text
command_id / client_order_id / reservation_id
risk_group_id / risk_shard_id / risk_epoch
instrument_id
side / type / price / quantity
permit constraints and config versions
```

### 4.4 Matcher 出口

不为每个下游增加同步提交。Matcher 产生带连续 `matcher_sequence` 和 `input_log_position` 的 Accepted/Fill/Cancel/Reject/Expire 事实流，由一份 replayable event recording 服务 Risk、Ingress 和投影消费者。

输入命令、状态变化和输出事件必须通过 `command_id/input_log_position` 确定关联。若只记录输入并在 replay 时重新生成输出，所有影响撮合的配置、时钟语义和随机输入必须确定化；否则记录结果事件。

### 4.5 Risk 消费 Matcher 事实

每个 Matcher 保证自己的 source sequence。Risk Shard 按事件进入单写 mailbox 的顺序应用，并记录该应用顺序。

- `LATENCY_FIRST` 可异步记录；Risk 故障后冻结 shard，从 Matcher/Ledger barrier 重建。
- `FAST_RECOVERY` 在基于新状态继续签发 permit 前，把对应 Risk transition 提交到 replicated log。

### 4.6 Ledger、Control 与爆仓

财务 ACK 前，Deposit、Withdrawal、Fee、Funding、Settlement、CollateralTransfer、Insurance 和 ADL 必须进入 Ledger 权威日志。PostgreSQL表和 Redis 不是财务提交点。

epoch、shard placement、配置和 durability mode 在激活前进入 Control WAL。模式只能在 sequence barrier 切换。

进入 `Liquidating` 前切换到 `FAST_RECOVERY`。LiquidationPlan、授权上限、outstanding quantity、factor/config version 先提交，再发 `LiquidationExecutionPermit`。

## 5. 非阻塞等待与流水线

状态机逻辑上等待 commit，但单写线程不执行阻塞 syscall 或逐单 Future：

```rust
pub struct PendingCommand {
    pub end_position: i64,
    pub command_index: u32,
}
```

Event loop 每轮读取一次 `commit_position` counter，顺序应用所有 `end_position <= commit_position` 的命令。多个请求连续 append，Follower/Archive 只需确认最高连续位置：

```text
append A@100, B@200, C@300
follower ACK through 300
apply A, B, C in order
```

这会摊薄网络包、Archive 写和 quorum ACK 成本，同时保留单合约顺序。

## 6. WAL 基准矩阵

| 基准 | 等待语义 | 目的 |
|---|---|---|
| B0 | 当前异步 PG projection | 现有端到端基线 |
| B1 | 异步 Aeron Archive | 测 recording 旁路开销 |
| B2 | 等待本机 `recorded_position` | 测本机记录提交 |
| B3 | 等待远端 replica/quorum position | 测单主机故障 RPO 0 |
| B4 | 同步 PostgreSQL transaction/WAL | 通用数据库对照组 |

每组报告：

```text
p50 / p90 / p99 / p99.9
orders/sec
published-to-recorded lag
published-to-replicated lag
recovery time
RPO under kill -9 and host-loss simulation
```

预期不是验收事实：B0/B1 热路径应接近；B2 增加本机 recording wait；B3 增加同 rack replica RTT；B4 尾延迟通常最大。以真实硬件测试为准。

## 7. 订单标识作用域

### 7.1 权威订单引用

```rust
#[repr(C)]
#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub struct OrderRef {
    pub instrument_id: u32,
    pub order_id: u64,
}
```

以下同时合法：

```text
(BTC_USDT, 1001)
(ETH_USDT, 1001)
```

同一 `instrument_id` 内 `order_id` 永不重复；不同合约之间不提供唯一性保证，也不需要全局原子计数器。

### 7.2 Matcher 发号

```rust
pub struct MatcherIdentityState {
    pub instrument_id: u32,
    pub next_order_id: u64,
    pub next_match_id: u64,
}
```

OrderCommand 达到 Matcher commit point 后，在确定性状态机中分配 `order_id`。counter 随 snapshot 保存、随 log replay 推进；Leader 切换后继续相同序列。合约下线后重新上市时，不复用旧 `instrument_id`，或者不重置 counter。

### 7.3 ID 职责

| ID | 作用域 | 生成者 | 用途 |
|---|---|---|---|
| `client_order_id` | `(account_id, client_order_id)` | 客户端/Ingress | 客户端幂等和 UNKNOWN 查询 |
| `command_id` | Risk Shard 内唯一，推荐 u128 | Ingress/Risk | 内部重试幂等 |
| `reservation_id` | Risk Shard + epoch 内唯一 | Risk Owner | 风险占用生命周期 |
| `OrderRef` | `(instrument_id, order_id)` | Matcher | 订单权威标识 |
| `MatchRef` | `(instrument_id, match_id)` | Matcher | Fill/Trade 权威标识 |

不得继续把 `client_order_id` 直接当成 Matcher `order_id`。

## 8. OrderRef 迁移顺序

### Phase I：协议双字段

- 引入稳定 `InstrumentId` registry。
- OrderCommand 保留 `client_order_id`，MatcherEvent 携带 `instrument_id`、`order_id` 和 `client_order_id`。
- Matcher 开始生成 contract-local `order_id`。
- Desk/Risk 建立 client/command/reservation 到 `OrderRef` 的映射。

验收：两个合约可以产生相同数字 order ID，路由、撤单和查询仍正确。

### Phase II：内存键迁移

- 跨合约 cache 从 `u64` 改为 `OrderRef`。
- 单 Matcher 内部继续只使用 `u64 order_id`。
- Cancel、amend、liquidation、ADL 和 reconciliation 必须携带 `instrument_id`。

验收：同数字 ID 不会跨合约串单、撤错单或释放错误 reservation。

### Phase III：数据库双写

- `orders` 引入 `instrument_id`，建立 `UNIQUE(instrument_id, order_id)`。
- 可保留数据库内部 surrogate row key，但业务引用一律使用 `OrderRef`。
- `trades` 使用 `(instrument_id, match_id)` 作为业务唯一键，并携带 maker/taker order ID。
- 投影消费者双写，完成历史 backfill 与校验。

### Phase IV：API 切换

- Cancel/query 返回并接受 `instrument_id + order_id`。
- 外部可显示 `BTC_USDT:1001`，wire format 保持两个整数。
- 删除对 DB `MAX(order.id)`、Desk slab 和全局 Snowflake 的订单业务依赖。

### Phase V：移除兼容路径

- 删除旧全局订单 ID 映射与旧唯一约束。
- 全局 trace/correlation 使用 `command_id/request_id`，不重新污染业务 `order_id`。

## 9. 当前代码差距与实施门槛

当前差距：

- `desk-server` 从 PostgreSQL `MAX(id)` 加 Desk slab 生成 ID。
- `exchange-engine` 把 `req.client_order_id` 直接作为 OrderBook order ID。
- `orders.id` 是全局 `BIGINT PRIMARY KEY`。
- trades 的 order pair 唯一索引不含 instrument。
- Persist queue 满时允许丢 frame，不能作为唯一权威 WAL。
- Matcher input Archive 是旁路 recording，处理前不等待 commit position。

进入代码 Phase I 前必须确定：

1. `InstrumentId` 稳定 registry、生命周期和配置发布方式。
2. Matcher reliable commit 的目标故障等级 L1/L2/L3。
3. Aeron Archive/Cluster 如何暴露可轮询的 commit position。
4. Matcher snapshot 如何保存 ID counter 与 log position。
5. REST/WS 的 `ACCEPTED/UNKNOWN` 和 projection watermark 协议。
6. 数据库双写、backfill、回滚和旧客户端兼容窗口。

失败测试必须覆盖：重复 command、Leader 在分配 ID 前后崩溃、两个合约生成相同数字 ID、旧 epoch permit、Archive lag、projection frame 丢失，以及 replay 后 ID counter 一致。
