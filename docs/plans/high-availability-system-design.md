# LightningX 高可用与一致性系统设计

> 日期：2026-09-30
>
> 状态：Proposed，HA v2 实施与验收基线；当前生产代码尚未全部实现
>
> 范围：Matcher、PM Risk Owner、Ledger、Control Plane、Aeron WAL、热备、故障切换和灾备
>
> 配套运维：[HA v2 部署与运维手册](../ops/ha-v2-runbook.md)
>
> 相关：[交易 WAL、提交点与合约内订单 ID](matching-wal-order-id-design.md)、[Portfolio Margin 系统设计](portfolio-margin-system-design.md)

## 1. 决策摘要

1. 每个权威状态所有者组成独立的三副本复制组，正常提交需要 `2/3 quorum`。
2. 三个副本都保存完整 WAL、snapshot 和内存状态，不使用仅投票、不保存数据的 witness。
3. Aeron Cluster/Archive 负责排序、复制、commit position 和日志持久化；Rust 服务保留 Matcher、Risk、Ledger 业务状态机。
4. 状态机只应用 `log_position <= commit_position` 的命令；Aeron `offer()` 或本机 `recorded_position` 都不是业务提交。
5. 只有当前 term 的 Leader 可以产生生产副作用；所有输出携带 producer term，所有消费者拒绝旧 term。
6. Follower 持续静默应用相同 committed log，但不写 PG/Redis、不推 WebSocket、不发送行情、不执行清算委托。
7. PostgreSQL、Redis、WebSocket 和报表是可重建 projection，不参与 Matcher 的同步提交。
8. 同地域三副本保证单节点故障 `RPO=0`；跨地域灾备异步复制，默认不自动切换。
9. 无 quorum 时 fail closed：停止新单、改单和资金操作，允许只读查询；不得降级为单副本继续交易。
10. 现有 PostgreSQL lease + Archive 旁路录制保留为迁移期 HA v1，不是 HA v2 的最终一致性协议。

## 2. 目标与非目标

### 2.1 SLO

| 场景 | RPO | RTO 目标 | 行为 |
|---|---:|---:|---|
| 单进程故障 | 0 | `< 1 s` | 同机 supervisor 拉起或副本接管 |
| 单服务器故障 | 0 | `< 3 s` | 其余两副本形成 quorum 并接管 |
| 单网络链路/单交换机故障 | 0 | `< 5 s` | 多路径网络下自动恢复 |
| 单 AZ 故障 | 0，前提是剩余副本形成 quorum | `< 30 s` | 自动或受控接管 |
| 整个地域故障 | 配置的异步复制窗口 | `< 15 min` | 人工确认后灾备提升 |
| Projection 故障 | 权威状态 RPO 0 | 不阻塞交易 | 重放恢复 PG/Redis/推送 |

延迟目标不是固定承诺。必须在目标硬件上分别报告：

```text
ingress -> replicated commit
commit -> state applied
apply -> client ACK
p50 / p90 / p99 / p99.9
```

### 2.2 非目标

- 不做同一 OrderBook 的 active/active 多写。
- 不追求跨地域同步 quorum；跨地域 RTT 不进入普通订单热路径。
- 不把 PostgreSQL、Redis 或 WebSocket 送达当作撮合提交条件。
- 不承诺客户端只收到一次消息；承诺重复消息具有稳定 ID，可以安全去重和查询。
- 不允许运维人员通过清日志、重置 epoch 或跳过一致性检查来快速恢复交易。

## 3. 术语与不变量

### 3.1 位置

```text
append_position      Leader 已追加到本地 log 的位置
recorded_position    某 Archive 已记录的位置
replicated_position  某 Follower 已连续记录的位置
commit_position      已得到 quorum、允许状态机应用的位置
applied_position     本地状态机已完成应用的位置
snapshot_position    snapshot 完整反映的 applied_position
published_position   权威事件已发布到 projection stream 的位置
```

始终满足：

```text
snapshot_position <= applied_position <= commit_position
commit_position <= quorum(recorded_position[])
```

`recorded_position` 可能领先于 `applied_position`。Snapshot 必须绑定实际消费 fragment 的 end position，不能使用 Archive 的最大录制位置代替。

### 3.2 安全不变量

1. 同一复制组、同一 term 最多一个对外 Leader。
2. 只有 quorum 已持久记录的连续日志前缀可以 commit。
3. 新 Leader 必须包含所有已提交日志。
4. 未提交尾部不得进入权威状态；选主后允许截断。
5. 相同 snapshot 和 committed log 必须生成相同状态、ID 和事件。
6. 相同 `command_id` 最多产生一次业务效果。
7. 相同 `event_id` 在下游最多产生一次 projection 效果。
8. 旧 epoch 的输出在所有副作用边界被拒绝。

## 4. 复制组与分片

### 4.1 复制单位

| 复制组 | 分片键 | 状态 | 推荐提交等级 |
|---|---|---|---|
| Matcher Group | `matcher_shard_id`，包含一组 instrument | OrderBook、订单/成交 ID、命令去重 | 同地域 2/3 |
| PM Risk Group | `risk_group_id` | 组合持仓、保证金、reservation、爆仓状态 | LATENCY_FIRST 异步风险尾；FAST_RECOVERY 2/3 |
| Ledger Group | `ledger_shard_id/account_id` | 余额、费用、funding、settlement、insurance/ADL | 2/3，严格 durability |
| Control Group | 全局或区域 | shard placement、epoch、配置、版本 | 2/3 |
| Market Factor Group | factor shard | mark/index/volatility/factor version | 2/3 或可重建流 |

一个复制组可以承载多个低流量 symbol，但一个 symbol 在任一时刻只能属于一个 Matcher Group。迁移 symbol 必须通过 Control Plane barrier 完成，不能同时在两个 group 接受命令。

### 4.2 推荐节点布局

```text
Failure Domain A          Failure Domain B          Failure Domain C
----------------          ----------------          ----------------
ha-log-node A             ha-log-node B             ha-log-node C
Rust state owner A        Rust state owner B        Rust state owner C
Local Aeron Archive A     Local Aeron Archive B     Local Aeron Archive C
Local NVMe A              Local NVMe B              Local NVMe C
```

三个节点都是完整副本。低流量 shard 可以复用物理节点，但同一复制组的三个副本不得位于同一宿主机、同一电源或同一故障域。

## 5. Aeron 组件边界

### 5.1 选择

生产终态优先采用一个最小化的 `ha-log-node` sidecar：

- 基于 Aeron Cluster/Archive；
- 只负责 ingress sequencing、复制、commit、term/leadership 和 committed command 投递；
- 不包含订单簿、保证金、清算或账本业务；
- 每个副本把 committed command 通过本机 Aeron IPC 交给 Rust 状态机；
- Rust 状态机把 deterministic result 通过本机 IPC 返回 sidecar/出口层。

这是为了复用经过验证的 quorum、选举和 commit 协议。不得仅用 Archive replication counter 拼装一个没有选举安全证明的自研 Raft。

### 5.2 为什么不是 Transport 或 Archive 单独承担 HA

- Aeron Transport 保证活跃 session 的可靠传输，不提供离线 store-and-forward。
- Aeron Archive 提供 recording、replay 和 replication，但单独不决定哪个日志前缀已被 quorum 提交。
- Aeron Cluster 提供 term、选举、append/commit position 和 bounded application。

如果 Aeron Cluster sidecar 的 Rust 集成 spike 未通过，备选方案是成熟的外部共识日志；不得退回“PG lease 决定 Leader、两个 Archive 各自录流”作为最终方案。

## 6. 命令和事件协议

### 6.1 CommandEnvelope

```rust
pub struct CommandEnvelope {
    pub schema_version: u16,
    pub owner_group_id: u32,
    pub instrument_id: u32,
    pub command_id: u128,
    pub client_order_id: u64,
    pub account_id: u64,
    pub risk_group_id: u32,
    pub risk_epoch: u64,
    pub reservation_id: u128,
    pub logical_time_ns: u64,
    pub config_version: u64,
    pub factor_version: u64,
    pub payload_crc32c: u32,
    pub payload: CommandPayload,
}
```

所有影响结果的数据都必须进入日志。Replay 时禁止重新读取当前时间、Redis、数据库当前配置或最新 mark price。

### 6.2 CommittedCommand

```rust
pub struct CommittedCommand {
    pub term: u64,
    pub log_index: u64,
    pub start_position: i64,
    pub end_position: i64,
    pub envelope: CommandEnvelope,
}
```

Rust 状态机只接受：

```text
term >= installed_term
log_index == last_applied_index + 1
end_position <= observed_commit_position
CRC/schema/config version valid
```

遇到 gap、旧 term、未知 schema 或无法获取固定版本配置时立即冻结该 shard，不可跳过。

### 6.3 MatcherEvent

```rust
pub struct BusinessEventId {
    pub owner_group_id: u32,
    pub input_log_index: u64,
    pub event_ordinal: u16,
}

pub struct PublicationStamp {
    pub producer_term: u64,
    pub publication_sequence: u64,
}
```

`BusinessEventId` 是稳定业务 ID：同一输入命令产生的第 N 个事件在重放、重试和 Leader 切换后保持不变。`PublicationStamp` 是发送尝试的栅栏：新 Leader 重发相同业务事件时使用相同 `BusinessEventId`，但使用新 term 和新的 publication sequence。

事件同时携带 `instrument_id`、`OrderRef/MatchRef` 和 `command_id`。下游先用 `producer_term` 拒绝 zombie，再以 `BusinessEventId` 去重，最后用 term-local `publication_sequence` 检测传输 gap。不得用会随选主变化的 term 作为业务幂等 ID 的一部分。

## 7. 正常订单路径

```text
Client
  -> Gateway/Desk：鉴权、格式校验、生成/校验 command_id
  -> PM Risk Owner：计算并预留风险额度
  -> target Matcher Group ingress
  -> Cluster Leader append
  -> 至少一个 Follower Archive 连续记录
  -> quorum 推进 commit_position
  -> 三个 Matcher 副本应用相同 committed command
  -> 三个副本生成相同 deterministic result/outbox
  -> Leader 出口发布带稳定 BusinessEventId 和当前 PublicationStamp 的权威事件
  -> Risk/Ledger/PG/Redis/WS 幂等消费
```

Follower 应用相同状态变化，但抑制以下副作用：

- 外部 Aeron publication；
- WebSocket/REST completion；
- PG/Redis 写；
- 市场数据发布；
- 清算子单发送；
- 邮件、短信和 webhook。

Follower 可以输出本地 metrics、trace、state hash 和健康报告。

## 8. Commit 与响应语义

### 8.1 响应状态

| 状态 | 含义 | 客户端动作 |
|---|---|---|
| `ACCEPTED` | 命令已 commit 且状态机已应用 | 使用返回的 OrderRef |
| `REJECTED` | 确定性拒绝已 commit | 不重试相同业务意图 |
| `UNKNOWN` | 请求结果无法确定 | 使用相同 client/command ID 查询或重试 |
| `UNAVAILABLE` | 未进入权威日志 | 可安全重试 |

Gateway 超时不得擅自把 UNKNOWN 转成 REJECTED。

### 8.2 流水线

Leader 连续 append 多条命令，Follower 可以批量确认最高连续位置。状态机 event loop 每轮读取一次 commit counter，批量应用全部已提交命令，不逐单执行阻塞 syscall 或 Future。

### 8.3 Durability profile

| Profile | Commit 条件 | 适用范围 | 风险 |
|---|---|---|---|
| `LOCAL_RECORDED` | Leader 本机 Archive position | 开发、benchmark | 主机丢失可能丢已确认命令 |
| `REPLICATED_OS` | 至少 2/3 独立主机 Archive 连续写入 OS page cache | 普通 Matcher | 抵抗单主机故障；相关电源故障依赖剩余节点 |
| `REPLICATED_SYNC` | 至少 2/3 独立主机数据和必要 metadata 已同步介质 | Ledger、高风险/爆仓 Risk | 延迟更高，可通过 group commit 摊薄 |

Profile 是 Control WAL 中的版本化配置，只能在 sequence barrier 切换。Aeron Archive `file.sync.level` 和 catalog sync level 必须与 profile 对应；不能把 Cluster append position 自动解释为 fsync 完成。上线验收必须通过断电/拔盘测试确认实际语义。

### 8.4 Deterministic outbox

应用一条 command 时，三个状态机副本同时生成相同的 outbox entry 和 `BusinessEventId`。只有 Leader 尝试发送，但“发送成功”不直接修改 Leader 私有状态：

```text
apply command -> deterministic outbox entry
Leader publish -> downstream idempotent apply
downstream ACK/watermark -> OutboxAck command enters replicated log
commit OutboxAck -> all replicas compact the same outbox prefix
```

Follower 接管后，对所有尚未被 committed `OutboxAck` 覆盖的 entry 重新发送。即使下游已经处理但 ACK 丢失，稳定 BusinessEventId 也会将其去重。

## 9. 热备状态机

### 9.1 正常行为

每个 Follower 持续维护：

```text
local_recorded_position
observed_commit_position
last_applied_position
last_applied_index
installed_term
snapshot_position
state_hash
```

Follower 必须持续应用，而不是仅保存 WAL。这样接管时只需要处理极短尾部。

### 9.2 状态一致性验证

每隔固定 `log_index`，三个副本对 canonical state 计算 hash：

```text
hash(
  owner_group_id,
  term,
  applied_index,
  next_order_id,
  next_match_id,
  canonical order books,
  open-order metadata,
  dedup state and deterministic outbox,
  config/factor versions
)
```

只比较相同 `applied_index` 的 hash。发现不一致时：

1. 标记副本 `DIVERGED`；
2. 禁止其投票和接管；
3. 保留 WAL、snapshot 和 diagnostics；
4. 从健康副本重新 bootstrap；
5. 触发最高级别告警和事故复盘。

## 10. Snapshot 与 Replay

### 10.1 Snapshot 内容

```rust
pub struct OwnerSnapshotHeader {
    pub format_version: u16,
    pub owner_group_id: u32,
    pub term: u64,
    pub last_applied_index: u64,
    pub last_applied_position: i64,
    pub config_version: u64,
    pub state_hash: [u8; 32],
    pub payload_crc32c: u32,
}
```

Matcher payload 必须包含全部 resting order、FIFO 次序、order/match counters、open-order metadata、command dedup table、deterministic outbox 及下游确认 watermark。Risk/Ledger snapshot 包含各自完整权威状态。

### 10.2 生成规则

- 只在一条命令完整应用后截取 snapshot。
- Header 使用该命令 fragment 的实际 end position。
- 先写临时文件，完成 checksum/fsync 后原子 rename。
- Snapshot 生成不阻塞状态机：单写线程生成 immutable/copy-on-write view，由后台序列化。
- 至少保留两个已验证 snapshot；新 snapshot 验证完成后才能回收旧 WAL segment。
- Snapshot 异步上传对象存储，用于区域灾备，不作为同地域热备的唯一来源。

### 10.3 启动与 Replay

```text
1. 读取本地最新 verified snapshot
2. 校验 group/schema/config/checksum
3. 恢复状态和所有 counters/dedup
4. 从 last_applied_position 请求 bounded replay
5. 严格按连续 log_index 应用到 commit_position
6. 与其他副本比较 state hash
7. 进入 FOLLOWER_READY
8. 获得 quorum leadership 后才进入 LEADER_ACTIVE
```

无 snapshot 时从 retained WAL genesis 重放。Snapshot 早于 WAL retention floor 且无法补齐时，副本必须重新 bootstrap，不能加入 quorum。

## 11. 幂等模型

### 11.1 命令去重

权威 key：

```text
(owner_group_id, command_id)
```

去重记录保存：payload hash、首次 log index、结果摘要和生命周期状态。相同 ID、相同 payload 返回原结果；相同 ID、不同 payload 视为协议错误并冻结调用方。

活跃订单和资金命令的去重记录不能按简单 TTL 提前删除。可在业务进入终态、客户端查询窗口结束且 checkpoint 已被灾备确认后压缩成 compact tombstone。

### 11.2 输出去重

Risk、Ledger、Projection 和 Gateway 分别保存：

- 每个 producer 的最高 `producer_term` 和 term-local publication floor，用于 fencing 与 gap detection；
- 已应用的稳定 `BusinessEventId`，并与业务更新在同一事务/状态机 transition 中提交。

新 Leader 可以在新 term 下重发旧 outbox 事件。消费者先通过新 term fencing，再根据不变的 `BusinessEventId` 跳过已经应用的事件，因此不会重复冻结、结算、释放或通知。

### 11.3 exactly-once 的定义

系统不依赖 exactly-once 网络传输。承诺为：

```text
at-least-once delivery
+ deterministic state transition
+ command/event dedup
= exactly-once business effect
```

## 12. Leadership、选举与 Fencing

### 12.1 Term

Term 由复制组共识产生，必须是至少 64 位单调值。现有将 epoch 压入高 16 位的方案只作为兼容 wire format；HA v2 协议应显式携带 `producer_term: u64` 和 `publication_sequence: u64`，消除 65535 次切换上限。稳定 `BusinessEventId` 不包含 term。

### 12.2 Leader 激活门槛

候选节点只有满足以下条件才可对外：

- 赢得当前 term 的 quorum；
- 日志包含全部 committed prefix；
- 已应用到当前 commit position；
- state hash 校验通过；
- Control Plane placement 与本地 group identity 一致；
- 下游已安装该 term 的 fencing watermark；
- 本地时钟、磁盘、网络和 Archive 健康。

### 12.3 副作用栅栏

所有生产副作用 API 必须同时携带稳定 `BusinessEventId` 和 `{owner_group_id, producer_term, publication_sequence}`。消费者保存最高 term：

```text
producer_term < installed_term  -> drop + alert
producer_term > installed_term  -> 安装新 term，重置 term-local publication floor
producer_term == installed_term -> 检查 publication sequence gap，再按 BusinessEventId 去重
```

单靠杀死旧进程、VIP 或 PG lease 不构成 fencing。

## 13. 故障场景

### 13.1 Leader 进程或服务器崩溃

剩余副本选主，新 Leader 确认 committed prefix、补应用、安装新 term，然后恢复输出。客户端在窗口内可能收到 UNKNOWN，但使用相同 command ID 重试不会重复下单。

### 13.2 Leader 与多数派网络隔离

旧 Leader 失去 quorum 后立即停止接收写入和输出。多数派选出新 Leader。旧 Leader 恢复后以 Follower 身份截断未提交尾部并追赶。

### 13.3 少数派 Follower 隔离

多数派继续交易。落后 Follower 不参与选举；恢复后从 Archive replication 或 snapshot + tail 追赶。

### 13.4 失去 quorum

复制组进入 `NO_QUORUM`：

- 拒绝新单、改单、资金转移和新清算计划；
- 允许只读查询并标注 watermark；
- Cancel 是否允许取决于能否被可靠记录，默认同样拒绝，不能产生未复制状态；
- 不允许人工强制单节点 Leader。

### 13.5 Archive/NVMe 损坏

立即将该副本标记为不可投票。保留损坏介质供取证，在新目录从健康副本完整 bootstrap；禁止手工复制部分 segment 后直接加入。

### 13.6 状态 hash 不一致

把不一致副本隔离，不自动选择“多数 state hash”覆盖数据。先以 committed log 为权威重放验证；若多数副本也不一致，全 shard 停写并升级事故。

### 13.7 整地域故障

灾备地域默认是异步 Archive + snapshot，不参加正常 quorum。提升前必须：

1. 确认主地域被网络和入口完全隔离；
2. 计算灾备最后可用 commit watermark 与潜在 RPO；
3. 获得双人审批；
4. 生成新的 region generation 和 term；
5. 冻结 UNKNOWN 命令查询窗口；
6. 重放、对账、开放只读，再逐步开放写入。

## 14. PM Risk、Ledger 与爆仓 HA

### 14.1 PM Risk

同一 PM 账户的所有 spot/perp/future/RWA/prediction 状态由一个 Risk Group 顺序化。Matcher 分散在不同 desk/服务器不改变风险所有权。

`LATENCY_FIRST` 可以先在内存 reservation 后把 permit 发给 Matcher，以 Matcher reliable accept 作为核心提交点；Risk failover 后该 shard 必须冻结并从 Matcher/Ledger barrier 重建。`FAST_RECOVERY` 则在发 permit 前把 reservation 和 dispatch intent 以 2/3 提交。

高风险账户、接近爆仓账户和所有 Liquidating 账户强制 `FAST_RECOVERY`。

### 14.2 Ledger

Ledger 不能使用 LATENCY_FIRST。Deposit、Withdrawal、Fee、Funding、Settlement、CollateralTransfer、Insurance 和 ADL 在财务 ACK 前必须 2/3 commit。PostgreSQL只是查询和审计 projection。

### 14.3 爆仓

进入 `Liquidating`、生成 LiquidationPlan、分配 execution permit 和更新 outstanding quantity 都必须 commit。只有当前 Risk term 的 liquidation executor 可以向 Matcher 发清算子单；Matcher 验证 risk group、term、permit ID、quantity ceiling 和 expiry。

Leader 切换后，新 Risk Leader 从 committed plan 恢复 outstanding quantity，以相同 liquidation action ID 重发；Matcher 去重，因此不会重复平仓。

## 15. Projection 与客户端一致性

PG/Redis/WS 不需要与 Matcher 同步提交，但每个响应应携带：

```text
owner_group_id
producer_term
publication_sequence
business_event_id
projection_watermark（查询接口）
```

客户端收到 ACK 却暂时没有 WebSocket 推送时，可以使用 REST 按 `client_order_id` 或 `OrderRef` 查询。如果 projection watermark 低于 ACK 的 event sequence，接口返回 `PENDING_PROJECTION`，而不是错误地返回“订单不存在”。

## 16. 安全与访问控制

- Cluster ingress、replication、Archive control 使用独立网络/VLAN 和双向认证。
- Archive control port 不向业务网或公网开放。
- 每个服务身份只允许访问自己的 group/stream。
- 运维 break-glass 操作需要双人审批和不可变审计日志。
- Snapshot、WAL 和备份在磁盘及对象存储中加密。
- 所有配置和 binary build 具有内容 hash，写入启动与 snapshot metadata。

## 17. 容量与留存

每组估算：

```text
daily_wal_bytes = peak_commands_per_sec
                * average_frame_bytes
                * 86400
                * burst/headroom factor

node_storage = daily_wal_bytes * online_retention_days
             + snapshot_storage
             + 30% compaction/replay headroom
```

要求：

- NVMe 常态使用率 `< 60%`；`70%` 告警，`80%` 停止非必要写入并扩容，`90%` shard fail closed。
- Online WAL retention 必须覆盖最大允许的副本离线时间、snapshot 周期和运维恢复时间。
- 只有当至少两个同地域 verified snapshot 和一个异地副本覆盖相应位置后，才可 purge 旧 segment。

## 18. Beacon 可观测性

每个复制组暴露以下 Beacon 指标：

```text
ha_role{group,node}                         # leader/follower/candidate/diverged
ha_term{group,node}
ha_commit_position{group,node}
ha_recorded_position{group,node}
ha_applied_position{group,node}
ha_replication_lag_bytes{group,node}
ha_apply_lag_commands{group,node}
ha_state_hash_match{group,node}
ha_quorum_members{group}
ha_leader_changes_total{group}
ha_fenced_events_total{consumer,group}
ha_business_event_duplicates_total{consumer,group}
ha_command_duplicates_total{group}
ha_command_id_payload_conflicts_total{group}
ha_snapshot_age_seconds{group,node}
ha_snapshot_position{group,node}
ha_archive_disk_used_ratio{node}
ha_replay_rate_commands{group,node}
ha_projection_lag_events{consumer,group}
```

所有 latency 使用 HDR Histogram，按 group、command kind 和 durability mode 分维度。不要给 `instrument_id`、account 或 order ID 建高基数 label。

## 19. 发布与迁移

### Phase 0：测量与协议冻结

- 固定 SBE schema、command/event ID、position 和 term 语义。
- 建立 B0–B4 durability benchmark。
- 修复 snapshot 使用 `max_recorded_position` 的问题。

### Phase 1：单节点 committed-log adapter

- `ha-log-node` 单副本运行，但 Rust 状态机已经只消费 committed command。
- 加入 applied position、dedup/outbox snapshot 和确定性 hash。
- 不改变外部流量。

### Phase 2：三副本 shadow

- 三副本复制和静默应用；HA v1 仍负责生产输出。
- 连续 14 天比较状态 hash、结果摘要、ID 和延迟。
- 任何 divergence 阻止进入下一阶段。

### Phase 3：单 symbol canary

- 选择内部/低风险 symbol；启用 quorum commit 和自动 failover。
- 运行 kill、partition、disk-full、slow-follower 和 zombie tests。

### Phase 4：主流币迁移

- BTC、ETH、SOL 分批迁移；一次只迁移一个 Matcher Group。
- 每次迁移后观察完整高峰周期。

### Phase 5：Risk/Ledger/爆仓

- PM Risk 先 shadow 再切 FAST_RECOVERY。
- Ledger 必须在资金业务迁移前通过严格 durability gate。
- 最后启用自动化 liquidation failover。

## 20. 验收 Gate

必须全部通过：

1. 同一 committed log 重放 100 次，state hash 和所有输出逐字节一致。
2. Leader 在 append 前、复制中、commit 后、apply 后、发布中分别 kill -9，不丢已确认订单，不重复业务效果。
3. 三种网络分区中最多一个可写 Leader；少数派 fail closed。
4. Follower 落后、重启、snapshot 损坏和 WAL segment 损坏均能安全处理。
5. 两个副本在相同 position 的状态 hash 连续 14 天一致。
6. 新 Leader 能撤销故障前挂单，并继续相同 order/match counter。
7. Zombie Leader 的所有输出在 Matcher 下游、Risk、Ledger 和 Projection 边界被拒绝；新 Leader 重发旧 outbox 时保持 BusinessEventId 不变。
8. 重复 command、重复 fill、重复 liquidation action 不产生重复效果。
9. 失去 quorum 时所有写路径 fail closed，且恢复后 UNKNOWN 请求可查询。
10. 目标硬件上的 p99/p99.9 和吞吐满足容量预算。

## 21. 当前代码差距

- 当前 Archive 是旁路 recording，Matcher 不等待 quorum commit。
- 当前选主依赖 PostgreSQL lease，不是复制日志的一部分。
- 当前 snapshot 标记 Archive 最大 recorded position，而非实际 applied fragment position。
- 当前 epoch 只有 16 位并嵌入 sequence，需要升级为显式 64 位 term。
- 当前 client order ID 仍直接作为 Matcher order ID。
- 当前 Follower 通过 live/replay handoff 静默应用，但没有 authoritative commit bound。
- 当前 state hash、durable command/event dedup、deterministic outbox 和 term-aware side-effect gateway 不完整。
- 当前 `journal-replicate` 是启动期工具，不是持续 quorum replication protocol。

这些差距意味着当前系统具备 replay/fencing 实验能力，但不能宣称达到本文定义的 HA v2。
