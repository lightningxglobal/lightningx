# LightningX HA v2 部署与运维手册

> 日期：2026-09-30
>
> 状态：Target Runbook；用于 HA v2 实施、验收和未来生产运行
>
> 警告：本文中的 `ha-log-node`、64 位 term、quorum commit 和部分指标尚未全部实现。当前系统仍应按 [HA v1 手册](ha-runbook.md) 操作，直到 HA v2 Gate 全部通过并正式切换。
>
> 架构依据：[LightningX 高可用与一致性系统设计](../plans/high-availability-system-design.md)

## 1. 值班人员必须记住的规则

1. 无 quorum 不交易，禁止强制单节点成为 Leader。
2. `offer_position`、本机 `recorded_position` 和 PostgreSQL commit 都不等于 Matcher quorum commit。
3. 只有 `LEADER_ACTIVE` 可以产生外部副作用；Follower 只能静默应用。
4. 不删除 Archive、snapshot 或 Aeron catalog 来“修复”启动问题。
5. 不在状态 hash 不一致时强行 promote。
6. 旧 Leader 复活后必须以 Follower 重入，不能沿用旧 term。
7. 所有人工灾备提升必须先隔离旧地域，并执行双人审批。
8. 客户端超时是 `UNKNOWN`，不得手工改成 REJECTED 或重建新 command ID。

## 2. 生产拓扑

每个 HA cell 有三个故障域：

```text
Cell / Region
├── Failure Domain A
│   ├── ha-log-node-a
│   ├── state-owner-a (matcher/risk/ledger)
│   ├── Aeron MediaDriver + Archive A
│   └── local NVMe A
├── Failure Domain B
│   ├── ha-log-node-b
│   ├── state-owner-b
│   ├── Aeron MediaDriver + Archive B
│   └── local NVMe B
└── Failure Domain C
    ├── ha-log-node-c
    ├── state-owner-c
    ├── Aeron MediaDriver + Archive C
    └── local NVMe C
```

约束：

- 同一复制组三副本不得位于同一物理宿主机。
- 正常写入需要任意两个完整副本。
- 业务流量只发送到 group ingress，不直接绑定某台 Matcher。
- PG/Redis/WS projection 与权威组解耦，可以独立扩缩容。
- 跨地域 Archive 和 snapshot 是异步 DR，不参加同地域 quorum。

## 3. 服务与职责

| 服务 | 副本 | 职责 | 能否产生生产副作用 |
|---|---:|---|---|
| `ha-log-node` | 3/group cell | 排序、复制、选举、commit、Archive | 仅 Leader 提交命令 |
| `exchange-engine` | 3/group | 确定性 OrderBook 状态机和 outbox | 仅 Leader 输出 MatcherEvent |
| `pm-risk-node` | 3/risk group | PM、reservation、liquidation plan | 仅 Leader 发 permit/action |
| `ledger-node` | 3/ledger group | 资金和结算权威状态 | 仅 Leader产生财务事实 |
| `gateway/desk` | N | 客户端接入、幂等查询、路由 | 不拥有撮合/资金真相 |
| `pg-writer` | N | SQL projection | 消费幂等事件 |
| `redis-writer` | N | 缓存 projection | 消费幂等事件 |
| `beacon` | N | metrics/tracing/alerting | 无 |

## 4. 主机、磁盘与网络要求

### 4.1 主机

- Linux，生产内核和 NIC driver 版本统一。
- Matcher、MediaDriver、Archive 和 Cluster agent 分别绑核。
- 禁用会造成不可控长停顿的省电状态；保留经过测试的 turbo 配置。
- 使用 PTP 或 chrony。业务正确性使用日志顺序，不依赖墙上时间，但事件审计和 lease/证书仍需要可靠时钟。
- 内存预留必须覆盖 OrderBook、dedup、Aeron term buffer、snapshot view 和最坏追赶窗口。

### 4.2 磁盘

建议目录：

```text
/dev/shm/aeron/<cell>/<node>       # MediaDriver 临时目录
/var/lib/lightningx/archive/<group> # 本地持久 NVMe
/var/lib/lightningx/snapshot/<group>
/var/lib/lightningx/diagnostics
```

要求：

- Archive 和 snapshot 使用本地企业级 NVMe；不要放在 `/dev/shm`、NFS 或普通网络盘。
- 文件系统 mount、discard、write cache 和断电保护配置必须经过故障测试。
- 常态磁盘使用率 `< 60%`。
- Archive 目录永不被 systemd `ExecStartPre` 清理；只允许清理 MediaDriver 临时目录。

### 4.3 网络

- replication/cluster、Archive control、client ingress、projection 分离 VLAN 或安全域。
- 三节点间至少两条独立网络路径；关键主流币 cell 推荐 25 GbE。
- Archive control 端口仅管理网可达。
- 防火墙规则按明确 peer allowlist 配置。
- 禁止依赖无监控的多播作为唯一复制路径；复制组链路必须能观测 peer 和 position。

## 5. 配置管理

### 5.1 权威配置

所有节点必须从 Control Plane 获得带版本和 hash 的配置：

```text
cluster_id
group_id
node_id
peer identities/endpoints
instrument placement
archive directory
snapshot directory
durability mode
schema versions
binary build hash
config version/hash
```

禁止三台机器通过手改不同的环境变量运行。同一 group 的 topology/config hash 不一致时，节点只能进入 `CONFIG_MISMATCH`，不得投票。

### 5.2 计划接口

以下是 HA v2 目标配置名，不代表当前二进制已经支持：

```text
HA_CLUSTER_ID
HA_GROUP_ID
HA_NODE_ID
HA_PEERS
HA_QUORUM_SIZE=2
HA_ARCHIVE_DIR
HA_SNAPSHOT_DIR
HA_DURABILITY_MODE=REPLICATED_OS
HA_METRICS_ADDR
HA_STATE_HASH_INTERVAL_COMMANDS
HA_SNAPSHOT_INTERVAL_COMMANDS
HA_MAX_REPLICATION_LAG_BYTES
HA_MAX_APPLY_LAG_COMMANDS
```

实际实现应使用明确值 `LOCAL_RECORDED`、`REPLICATED_OS` 或 `REPLICATED_SYNC`，不得让字符串 `REPLICATED` 隐含 page cache/fsync 语义。Matcher 默认候选值为 `REPLICATED_OS`；Ledger 和 Liquidating Risk 使用 `REPLICATED_SYNC`，最终以硬件断电测试和延迟预算批准结果为准。

实现阶段必须提供配置 schema validator 和 `--check-config`，上线前在三节点分别验证并比较 hash。

## 6. 启动前检查

每次新节点或大版本发布执行：

```text
□ 资产清单中的 node/group/failure-domain 正确
□ binary build hash 和 SBE schema version 已批准
□ 三节点 config hash 相同
□ NTP/PTP 偏移在门限内
□ 两条网络路径连通、无丢包/错误包告警
□ NVMe SMART、延迟、剩余空间正常
□ Archive catalog 和最近 segment checksum 正常
□ 最新两个 snapshot checksum 正常
□ TLS/服务证书未临近过期
□ Beacon scrape、日志和告警路由正常
□ 最近一次恢复演练未过期
```

任一权威数据检查失败，不允许使用 `--force` 进入投票成员。

## 7. 启动顺序

### 7.1 整个 cell 冷启动

```text
1. 启动三台 MediaDriver + Archive
2. 验证每台 Archive catalog 可读、目录可写
3. 启动三个 ha-log-node，先形成 quorum，不开放 ingress
4. 每个节点恢复最新 verified snapshot
5. bounded replay 到当前 commit position
6. 比较相同 applied index 的 state hash
7. 状态所有者进入 FOLLOWER_READY
8. 选出 Leader，但保持入口 maintenance mode
9. 启动 Risk/Ledger/Projection/Gateway 消费者
10. 验证 fencing term 和 projection watermark
11. 先开放只读
12. 运行 canary 下单/撤单/成交/结算
13. 开放写流量并观察一个完整告警窗口
```

### 7.2 单 Follower 重启

```text
1. 节点以 NON_VOTING_BOOTSTRAP 启动
2. 校验本地 snapshot/WAL
3. 从健康 peer 补齐缺失 recording
4. replay 到当前 commit position
5. 与健康副本比较 state hash
6. 进入 FOLLOWER_READY
7. Control Plane 批准后恢复 voting
```

Follower 未追平前不能投票，避免落后日志赢得领导权。

## 8. 健康状态机

每个节点必须只处于下列显式状态之一：

```text
STARTING
NON_VOTING_BOOTSTRAP
REPLAYING
FOLLOWER_READY
CANDIDATE
LEADER_WARMING
LEADER_ACTIVE
NO_QUORUM
CONFIG_MISMATCH
DIVERGED
DISK_UNHEALTHY
FENCED
STOPPED
```

Beacon 和管理接口必须暴露状态与原因。只有 `LEADER_ACTIVE` 能接收写入并产生副作用。

## 9. 日常监控与告警

### 9.1 P0：立即冻结或自动 fail closed

- `ha_quorum_members < 2`；
- 同一 group 观察到两个 `LEADER_ACTIVE`；
- 相同 applied index 的 state hash 不一致；
- command ID 相同但 payload hash 不同；
- Archive/snapshot checksum 失败；
- 当前 Leader 磁盘使用率 `>= 90%`；
- Ledger/Risk 接受旧 term 副作用；
- committed log 出现 gap 或无法解码 schema。

### 9.2 P1：5 分钟内处理

- 任一副本 replication lag 超过配置门限持续 10 秒；
- Follower apply lag 持续扩大；
- `ha_fenced_events_total` 突增；
- snapshot age 超过两个计划周期；
- NVMe p99 写延迟超过基线两倍；
- Leader 在 10 分钟内切换两次以上；
- projection lag 超过客户端查询 SLA。

### 9.3 P2：容量处理

- 磁盘使用率 `>= 70%`；
- WAL 日增长超过容量模型 20%；
- replay ETA 超过 RTO；
- snapshot 时间或大小持续增长；
- 单 group 峰值 CPU 超过 70% 的容量预算。

## 10. Beacon Dashboard

每个 group 页面至少展示：

```text
当前 Leader / term / 最近选主时间
三节点 role 和 health state
commit、recorded、applied、snapshot position
replication lag bytes / time
apply lag commands / time
相同 position state hash
quorum members
订单 commit latency HDR
snapshot age / duration / size
Archive disk usage / write latency
fenced event、duplicate command、duplicate BusinessEventId、gap counters
PG/Redis/WS projection watermark
```

跨 group 总览不能用 account、order 或 instrument 作为 metric label；详情通过 trace/log 按 ID 查询。

## 11. 自动故障切换

### 11.1 Leader 故障预期动作

```text
T0       Leader 停止心跳/复制
T0+Δ1    剩余节点判定 term 失效并选举
T0+Δ2    新 Leader 确认 authoritative committed prefix
T0+Δ3    新 Leader apply 到 commit position
T0+Δ4    下游安装新 term fencing watermark
T0+Δ5    新 Leader 进入 LEADER_ACTIVE
T0+Δ6    Gateway 恢复写入；UNKNOWN 请求按原 command ID 查询/重试
```

运维人员在自动切换期间不要手工重启所有节点，也不要清 Archive。首先确认系统是“正在选举”还是“无 quorum”。

### 11.2 自动切换后检查

```text
□ 只有一个 LEADER_ACTIVE
□ term 增加且三节点最终一致
□ commit/applied position 单调推进
□ 新 Leader state hash 与健康 Follower 相同
□ fenced old-term event 有计数但无业务落地
□ canary 订单可以下单、查询、撤单
□ 故障前 resting order 可以正确撤销
□ order/match/event ID 未回退或重复
□ Risk reservation、Ledger 和 projection watermark 继续推进
□ 新 Leader 重发事件的 BusinessEventId 不变，下游只产生一次业务效果
□ committed OutboxAck 之前的事件会重发，之后的 outbox 可以安全压缩
```

## 12. 计划内 Leader 切换

用于发布、硬件维护或容量迁移：

```text
1. 确认至少两个 Follower READY 且 lag=0
2. 暂停该 group 新写入，保留查询
3. 等待 in-flight command 全部到达 commit/applied barrier
4. 强制生成并验证 snapshot（非必须，但推荐）
5. 发起 leadership transfer 到指定健康节点
6. 等待新 term 安装到所有副作用消费者
7. 运行 canary 下单/撤单
8. 恢复流量
9. 旧 Leader 降为 NON_VOTING，再执行维护
```

任一步骤失败则保持入口冻结，不能同时重启第二个节点。

## 13. 常见故障处置

### 13.1 单 Follower 落后

1. 检查网络、NVMe、Archive backpressure 和 CPU 绑核。
2. 将其标记 non-voting，Leader 继续服务。
3. 若 retained WAL 完整，在线 replay 追赶。
4. 若已越过 retention floor，从健康 snapshot 完整 bootstrap。
5. state hash 对齐后才能恢复 voting。

### 13.2 无 quorum

1. 确认所有写入口已经 fail closed。
2. 不要强制单节点提升。
3. 恢复网络或至少一个完整数据副本。
4. 比较节点 term、last log index、recorded position 和 checksum。
5. 由协议自动选举；人工只恢复资源，不手工选择较旧日志。
6. 恢复后检查 UNKNOWN 命令和 projection gaps。

### 13.3 State hash 不一致

1. 立即把异常副本设为 non-voting，并保存诊断包。
2. 固定比较的 applied index，排除“比较了不同位置”。
3. 从相同 snapshot 对 committed tail 离线 replay。
4. 对比首个 divergence index 的输入、配置版本、ID counter 和输出。
5. 不在事故现场修改状态；从健康副本重新 bootstrap。
6. 如果没有两个一致的健康副本，停止该 group 写入并升级 P0。

### 13.4 Archive 磁盘将满

1. `70%`：扩容或增加 snapshot/purge 执行频率，核实异地备份 watermark。
2. `80%`：停止非必要 diagnostics/长 replay，安排 shard 迁移。
3. `90%`：节点标记 DISK_UNHEALTHY；若 Leader 则转移领导权。
4. Purge 前验证至少两个本地 snapshot 和异地覆盖位置。
5. 绝不能直接 `rm` segment 或 catalog 文件。

### 13.5 Snapshot 损坏

1. 尝试前一个 verified snapshot。
2. 从该 snapshot position replay WAL。
3. 无可用 snapshot 时从 retention floor/genesis 重放。
4. WAL 也不完整时从健康副本完整 bootstrap。
5. 损坏 snapshot 保留取证，不覆盖原文件。

### 13.6 Zombie Leader 仍在发消息

1. 确认下游 `fenced_events_total` 增长且业务无落地。
2. 从负载均衡和复制网络隔离旧节点。
3. 停止进程，保留日志、term 和内存诊断。
4. 重新启动时强制 NON_VOTING_BOOTSTRAP。
5. 查明为何失去 quorum 后未在 deadline 内停止输出。

### 13.7 Projection 丢失或落后

权威 Matcher/Risk/Ledger 不停机：

1. 冻结该 projection consumer 的 checkpoint。
2. 从 event Archive 的 checkpoint 位置 replay。
3. 使用稳定 `BusinessEventId=(owner_group_id,input_log_index,event_ordinal)` 做幂等 apply；producer term 只用于 fencing。
4. 对比 projection watermark 和权威 publication sequence。
5. 追平后恢复查询/推送；客户端期间可得到 `PENDING_PROJECTION`。

## 14. 节点重新 Bootstrap

适用于新机器、Archive 损坏或落后超过 retention：

```text
1. Control Plane 将目标节点移出 voting set
2. 停止目标节点全部状态所有者和 Archive client
3. 移走旧数据到带时间戳的 quarantine 目录，不直接删除
4. 从健康副本复制最新 verified snapshot
5. 复制 snapshot_position 之后的 WAL recordings
6. 校验 catalog、segment、snapshot checksum
7. 以 NON_VOTING_BOOTSTRAP 启动
8. replay 到 commit position
9. 比较 state hash
10. 观察一个稳定窗口
11. 经批准恢复 voting
```

Bootstrap 过程必须是可重入的。已经覆盖到目标 position 的 recording 可以跳过，但不能仅以文件名判断完成，必须检查 recording/session/position/checksum。

## 15. 跨地域灾备

### 15.1 正常状态

- 异步复制 WAL recording 和 verified snapshot。
- Beacon 展示 `dr_replication_lag_bytes/time` 和最后验证 watermark。
- 每日验证对象存储 checksum，每周在隔离环境恢复一次抽样 group。
- 灾备地域不参与主地域 quorum，也不接受生产写入。

### 15.2 提升流程

```text
□ 事故指挥官宣布 region disaster
□ 网络团队隔离旧地域的 ingress 和 egress
□ 两名授权人员批准 break-glass
□ 记录主地域最后已知 commit watermark
□ 记录灾备最后可恢复 watermark 和预计 RPO
□ 恢复 Control、Ledger、Risk、Matcher 的依赖顺序
□ 所有 group replay 并通过 state/checksum 校验
□ 生成新的 region_generation 和 term
□ 先开放只读和查询 UNKNOWN
□ 运行资金、风险、撮合 canary
□ 分批开放写流量
```

旧地域恢复后不得反向自动抢占。它必须作为全新 Follower 从灾备当前权威状态重新 bootstrap。

## 16. 版本发布

### 16.1 兼容规则

- 滚动发布期间，新旧版本必须兼容相同 WAL schema 和 snapshot format。
- schema 采用显式版本；Reader 至少支持当前和前一版本。
- 破坏性状态机变更使用 log barrier：所有副本到达相同位置后统一激活。
- 相同 committed command 在混合版本 shadow 环境中必须产生相同 result hash。

### 16.2 发布顺序

```text
1. 在 replay lab 对生产脱敏 WAL 回放
2. 升级一个 non-voting Follower
3. replay + state hash 对齐
4. 恢复 voting，观察稳定窗口
5. 依次升级第二个 Follower
6. 计划内 transfer leadership
7. 升级旧 Leader
8. 完成三副本 hash 和 latency 对比
```

不得同时升级 quorum 中两个节点。

## 17. Snapshot、WAL 留存和备份

推荐初始策略，最终以容量测试为准：

| 数据 | 在线保留 | 异地/对象存储 | 删除门槛 |
|---|---:|---:|---|
| Matcher WAL | 72 小时 | 30 天或合规要求 | 两个本地 verified snapshot + DR 覆盖 |
| Risk WAL | 7 天 | 90 天或合规要求 | Risk snapshot + Matcher/Ledger barrier 可重建 |
| Ledger WAL | 30 天 | 按财务/合规要求 | 经过审计批准，不自动删除 |
| Snapshot | 最近 4 个 | 每日/每版本 | 至少保留两个已验证版本 |
| Incident diagnostics | 工单生命周期 | 受控归档 | 事故负责人批准 |

Retention job 只能调用 Archive 支持的 detach/purge/truncate 操作，不能直接删文件。

## 18. 演练计划

### 每次发布

- 单 Follower kill -9 和恢复；
- replay 后 state hash；
- duplicate command/event；
- projection consumer 重启补尾。

### 每周

- Leader kill -9 自动切换；
- 计划内 leadership transfer；
- snapshot restore 抽样；
- Beacon 告警链路测试。

### 每月

- 单向/双向网络分区；
- slow follower、Archive backpressure、NVMe latency injection；
- zombie Leader 输出 fencing；
- command UNKNOWN 查询/重试；
- liquidation plan 在 failover 中恢复。

### 每季度

- 整节点丢失后完整 bootstrap；
- snapshot 损坏 + 前一版本恢复；
- region DR 隔离演练；
- Ledger/Risk/Matcher/PG 全链路对账；
- 容量和 retention 模型复核。

所有演练记录：测试版本、随机种子、故障注入时间、commit watermark、RPO/RTO、state hash、客户可见行为和未决问题。

## 19. 上线 Gate

```text
□ HA v2 架构文档和威胁模型评审通过
□ 三节点 quorum 和 fencing 集成测试通过
□ snapshot 使用 applied fragment position
□ 64 位 producer term/publication sequence 与稳定 BusinessEventId 上线
□ command/event dedup 进入 snapshot
□ shadow 14 天 state hash 零 divergence
□ 所有 P0/P1 Beacon 告警经过真实触发验证
□ kill/partition/disk-full/zombie/DR 演练通过
□ p99/p99.9 延迟和吞吐满足批准预算
□ 运维、开发、风险和资金团队完成签字
□ HA v1 回滚路径经过演练
```

Gate 未全部通过前，文档状态不得从 Target Runbook 改为 Active Runbook。

## 20. 事故记录模板

```text
Incident ID:
开始/结束时间:
受影响 group/instrument/account 范围:
发现方式:
Leader/term 变化:
故障前 commit/applied/snapshot position:
故障后 authoritative position:
是否失去 quorum:
是否观察到 zombie 输出:
客户端 ACCEPTED/UNKNOWN/UNAVAILABLE 数量:
RPO/RTO:
state hash/checksum 结果:
Risk/Ledger/Projection 对账结果:
采取的操作及操作者:
根因:
纠正措施与负责人:
```
