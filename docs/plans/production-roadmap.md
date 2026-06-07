# 生产化改造计划（Production Roadmap）

> 基于 2026-06 对全部 ~32k 行代码的审查结论制定。
> 总体判断：**撮合核心可保留复用；资金路径与可靠性层必须重做**。
> 预估总工作量约为现有系统的 40–50%，分六个阶段，每个阶段有明确的验收门槛（Gate），
> 未过 Gate 不进入下一阶段。
>
> 图例：✅ 已完成 ｜ 🟡 部分完成 ｜ ⬜ 未开始

## 进度跟踪（最近评估：2026-06-07 深夜，HEAD `d8feae9`）

| 阶段 | 进度 | 状态 |
|------|------|------|
| P1 资金定点化 | 🟢 100% | **关闭**：orders/trades float8 也已退役（迁移 021，GENERATED 列——读者零改动、写 float 列被 PG 物理拒绝）；freeze_price_atoms 补齐。Gate 1 完全达成：f64 仅存在于派生展示 |
| P2 事件日志+序列号 | 🟢 100% | persist+引擎输入双 journal、exactly-once 位点、事件驱动 replay 完成检测、retention 清理（`5345f04`）。**关闭** |
| P3 资金事务闭环 | 🟢 100% | settle+trade 单事务+全局唯一键、append-only fund_audit 全腿流水、journal-audit 三方对账工具（`87c1c55`）。**关闭** |
| P4 风控补全 | 🟢 100% | 关闭（`3518336`） |
| P5 高可用 | 🟢 ~90% | 选主+fencing+standby（`d8feae9`）+ **单机 failover 演练通过：RTO 实测 3.05s ≤ 要求 180s**（`c30fb25`）。剩：PG 主从/PITR 与跨机演练（部署工作） |
| P6 安全加固 | 🟢 ~95% | 全套密钥/认证/审计/metrics/fuzz 完成（`9c12605`/`f0d7070`）；engine+desk 热路径埋点、文件 secret。剩仅 TODO 三项（双人复核/KYC/渗透测试，见 deferred-todos.md），均非代码 |

**Gate 2/4/5 已实测通过**（`c30fb25`，真实进程 SIGKILL）：
- Gate 2：100 次 kill -9 下 3000 帧零丢失零重复；
- Gate 5：**failover RTO 实测 3.05s**（用户硬性要求 ≤180s，余量 59 倍），
  standby 静默、epoch fencing、接管后可撤 failover 前挂单（无缝）；
- Gate 4：闪崩 -6% 触发熔断 → 仅撤单 → 冷却恢复，全链路过线。
剩余：①多机部署件（PG 主从/PITR、跨机演练——部署工作）；②外部（渗透测试）；
③已延后 TODO（双人复核/KYC，见 deferred-todos.md）。

### 本轮设计决定（用户拍板，已存档）

1. **自成交不做引擎实时检查**：合规有独立的离线稽查系统，交易系统只负责提供
   足够 meta。解析链：`trades.buy/sell_order_id → matching_events(ACCEPTED).participant_id`
   （append-only，撤单物理删除 orders 行也不影响归因）。实时 STP（O(穿越订单数) 扫描）
   已从热路径移除。
2. **HA 用 Aeron Archive，不用 Aeron Cluster**：Cluster 的 service container 仅有
   Java 实现，Rust 引擎无法嵌入；Archive 录制 + replay-merge 正好替代自研 WAL。
   终态推荐 A→B 演进：先 Archive journal + etcd 选主，后半同步双机直复制
   （epoch/term 嵌入输出序列号做 fencing；引擎重放快，初期可不做快照）。
3. **symbol 上限暂定 16 字节**（= 线格式 `[u8;16]`），超长拒绝而非截断；
   拓宽到 20（CompactString 内联）需联动 15+ 个 packed struct 与 risk-engine key，
   列为独立条目。

---

## 0. 指导原则与目标架构

### 0.1 核心原则

1. **撮合引擎是唯一真相源（Source of Truth）**：撮合输出 = 带全局序列号的事件日志，
   先落盘（journal）再对外确认。下游（Redis/PG/行情/通知）全部是日志的消费者，
   任何下游状态都可以从日志重建。
2. **钱永远是整数**：全链路（撮合、冻结、结算、手续费、API）统一定点整数表示，
   浮点只允许出现在展示层的最后一步。
3. **可重放 = 可恢复 = 可审计**：同一份日志重放必须得到比特级相同的状态。
   消除一切非确定性（随机数、系统时钟、HashMap 迭代序进入业务语义）。
4. **每天对得上账**：交易所死于账对不上，不死于撮合慢。对账系统与交易系统同级别投入。

### 0.2 目标架构（改造后）

```
                    ┌──────────────────────────────────────────┐
 Client ──WS/REST──▶│ Gateway (desk-server ×N, 无状态化)        │
                    └───────────────┬──────────────────────────┘
                                    │ 带 client_seq 的指令流 (Aeron)
                                    ▼
                    ┌──────────────────────────────────────────┐
                    │ Sequencer + Matching Engine (主)          │
                    │  - 入站指令编号 → input journal (fsync)   │
                    │  - 撮合（纯函数，确定性）                  │
                    │  - 出站事件编号 → output journal           │
                    └───────┬──────────────────┬───────────────┘
                            │ 事件流(带seq)     │ journal 同步复制
                            ▼                  ▼
              ┌─────────────────────┐   ┌─────────────────┐
              │ 消费者（可重放追赶）  │   │ 热备引擎（从）    │
              │ pg-writer / redis-  │   │ 重放 journal     │
              │ writer / 行情 / 通知 │   │ 随时接管         │
              └─────────────────────┘   └─────────────────┘
```

与现状的差别：在撮合前后各加一层**持久化日志（journal）**，所有下游从"订阅 Aeron
尽力而为"改为"按序消费日志、断点续传"。Aeron 仍做传输，但可靠性不再依赖它。
（注：`docs/plans/2026-06-06-ha-replay-model.md` 已采纳同一模型，细节以该文档为准。）

### 0.3 保留 / 重写清单

| 模块 | 处置 | 理由 |
|------|------|------|
| `src/matching/`（engine/orderbook/skiplist/pools） | ✅ 保留，小改 | 整数撮合、价格时间优先、对象池均为生产级质量 |
| `src/transport/`（Aeron + SBE 骨架） | ⚠️ 保留骨架，消息格式升级 | 架构正确，但需加序列号、删 unsafe 裸读 |
| `src/desk/` 账户/结算（account*, pg_store, redis_store） | ❌ 重新设计 | f64 + 无事务闭环 + 丢数据窗口，修补不可行 |
| `src/desk/risk/` | ⚠️ 保留框架，补齐覆盖 | CAS 保证金框架可用，缺 STP/价格带/头寸上限 |
| `src/desk/` 接入层（ws_handler, api, rate_limit, user_service） | ⚠️ 加固 | 功能完整，需安全加固与精度校验 |
| 部署形态（docker-compose 单机） | ❌ 重做 | 无 HA，见 Phase 5 |

---

## Phase 1：资金正确性（阻塞级，最优先）— 🟢 ~90%

> **目标：系统里不再有任何用 `f64` 表示的钱。**
> 这是改动面最大的一项，但必须最先做，因为后续所有阶段都建立在新数值类型上。
> 越晚做，返工越多。

### 1.1 统一定点数值类型

- ✅ 定点类型已建：`src/desk/money.rs` 的 `AmountAtoms`（i64，scale 1e8），
  带 checked 运算、溢出检查、decimal 字符串解析/格式化，有单元测试。
- ✅ 账户路径核心判断已改整数：`freeze_*`/`release_*`/`settle_trade` 的 SQL
  判断与扣减以 `*_atoms` 列为准（`(balance_atoms - frozen_atoms) >= $N`）。
- ✅ atoms 原生 API（`b8d00f7`）：`freeze_atoms`/`release_frozen_atoms`/
  `settle_trade_atoms` 为正典接口；`cost = price × quantity` 在 i128 整数域
  完成（`checked_mul_scaled`，round-half-up）；f64 旧签名降级为边界薄包装，
  仅入口转换一次；legacy float8 列的写入值从 atoms 反推，两套列不可能分歧。
  资金守恒不变量有单元 + PG 集成测试覆盖。
- ✅ `SymbolRules` 整数换算（`6155075`）：每 symbol 硬编码 `price_tick_atoms`/
  `quantity_step_atoms` 整数孪生（守卫测试钉住与 f64 规则一致）；
  ticks/lots↔atoms 全整数 checked 乘法；atoms→ticks/lots 用**整除对齐**替代
  浮点 epsilon 检查。
- ✅ 清理 `src/matching/float_ext.rs`（死代码，已删除）与 `engine.rs` 的
  误导性"浮点幽灵订单"注释（已改为整数语义的准确描述）。

### 1.2 数据库 Schema 迁移

- ✅ 迁移 012：`accounts` 加 `balance_atoms/frozen_atoms BIGINT` 列 +
  非负/`frozen<=balance` CHECK 约束 + 存量数据换算。
- ✅ 迁移 014（`6155075`）：orders/trades 全套 atoms 列 + 回填 + 非负约束；
  pg-writer 双写（insert/fill/trade 全路径），新 `amount_atoms_invalid` 跳帧计数。
  klines 为行情展示数据，不在资金语义范围内，维持 float8。
- ⬜ **唯一剩余**：legacy f64 列物理下线。下线条件（写在迁移 014 注释里）：
  reconcile 漂移告警连续 30 天为零 且 无任何读方依赖 float8 列。
  到期动作：DROP COLUMN + `filled<=quantity` 升级为 CHECK + 持久化 payload 改携带
  ticks/lots（随 P2 线格式升级一并做）。
- ✅ 对账校验：reconcile 周期任务现覆盖 accounts/orders/trades 三表漂移 + 超额成交。

### 1.3 API/WS 边界

- ✅ 出参（`6155075`）：`GET /api/balances` 返回 `balance_str/frozen_str/available_str`
  精确字符串（旧数值字段保留兼容现有客户端，新客户端解析字符串）。
- 🟡 入参：`from_decimal_str` 已具备（>8 位小数拒绝）；下单 price/qty 的字符串
  入参接线留待客户端协调（破坏性 API 变更，单独排期）。

**Gate 1**：`grep -rn "f64" src/desk/ src/types/` 中不再有任何资金语义字段（待 legacy
列下线）；迁移前后全库资产总额逐资产 diff = 0；✅ property 测试已建并通过
（`tests/settle_conservation.rs`：种子化 200 笔随机结算，base 守恒、quote 差额
精确等于手续费、无残留冻结/负余额，全部 atoms 级断言）。🟡 部分达成

---

## Phase 2：事件日志与序列号（可靠性的地基）— 🟡 ~60%

> **目标：撮合输入/输出全部带全局递增序列号并先落盘，任何下游可断点重放。**
> 解决：消息无序列号、Ring Buffer 满静默丢弃、Aeron 不可重放、崩溃丢成交。

### 2.1 SBE 消息格式升级

- ✅ `OrderUpdateMsg` 已加 `sequence: u64`（64→72 字节，`11f3132`），
  engine 端按 response stream 递增分配，desk_server 做 gap 检测
  （`desk_server.rs:2374` 一带）。
- ✅ persist 流已带 (publisher_id, seq)（`e642cf0`）：desk 单一 drain 点赋值，
  时钟纳秒种子保证跨重启单调；消费端区分真实 gap 与 publisher 重启。
- ✅ 订单解码收敛到统一 `sbe::decode_*` 路径（`7b0e0e1`）；其余 `read_unaligned`
  审计确认全部有 size_of 长度守卫（有界 unsafe，健全）。
- ⬜ **剩余**：order-update 流 gap 仍只能 `warn!`（恢复依赖 journal）；
  depth/counter-forward 流未覆盖；无 `schema_version` 字段。

### 2.2 输入/输出 Journal

- 🟡 已有**审计层**：`matching_events` 追加表（迁移 013，`eccde5c`），
  主键 `(response_stream_id, sequence)` 幂等、可离线查 gap。
- ✅ **persist 流 journal 化**（`8fff81c` + wrapper `aa9718b..7cc7c2e`）：
  desk persist drain 线程录制（专属 AeronClient 遵守 conductor 契约；
  journal 开启而失败 = panic，拒绝静默降级）；pg-writer 启动时先建 live
  订阅（不 poll）→ 按创建序重放全部 recordings（floor 去重、补缺、
  catch-up 不丢帧只阻塞）→ 切 live。`EXCHANGE_ARCHIVE_CONTROL` 开关，
  归档侧要求 `file.sync.level=2` + `catalog.file.sync.level=2`。
  端到端测试：丢 40 帧 → 全部补回、60 重复帧全部丢弃、PG 100/100。
- ✅ **引擎输入流 journal**（`ccb0319`）：每 symbol 线程录制 orders 流；重启时
  先列旧 recordings 再开新录制（不会重放自己的空尾巴），经**正常处理逻辑**静默
  重放（live=false：状态全更新、零发布、响应序列不前进），完成后切 live；
  journal 模式跳过 PG 近似 seed（避免双重建簿）。e2e：300 op 混合负载重建簿
  与崩溃前逐档一致。
  - 设计修正记录：原计划的 per-order ACK 门控对本系统价值有限（订单在 ACK 前
    已同步落 PG），真正的缺口是引擎内存簿的**精确**状态（队列优先级、部分成交），
    输入流重放正中此靶。RecordingPositionCounter 门控保留给未来"成交事件
    落盘才回报"语义（若产品要求）。
- ⬜ **剩余**：recording 段清理（truncate 至消费 floor 以下）；
  replay 结束检测从 2s 静默期改为 stop-position 跟踪。
- ⬜ Ring Buffer 满（`aeron_transport.rs`）改为：入站→拒绝并告知客户端；
  出站→journal 为真相，Aeron 只是推送。

### 2.3 下游消费者改造

- ✅ pg-writer flush 失败**保留批次重试**（`e1d3e00`），有 poison frame 防护计数。
- ✅ flush 全部 6 类 payload 合并**单事务提交**（`783fab4`，归属 P3 但在此实现）。
- ✅ **消费者断点位点**（`e642cf0`）：pg-writer 位点表与数据**同事务**提交
  （GREATEST 防并发回退）= exactly-once；重启/重放去重；毒帧丢弃路径不推进位点；
  redis-writer 位点周期落 Redis（幂等 HSET 容忍 at-least-once）。集成测试覆盖
  重启续传、批内/重放去重、gap/重启区分。
- ⬜ 现有 `hydrate/backfill/reconcile` 降级为监控期对账工具。

### 2.4 撮合确定性收尾

- ✅ skiplist 种子化 xorshift64*（`7b0e0e1`）：结构跨实例可复现（节点层级逐一
  比对测试）；双引擎 2000 单混合负载逐单状态/成交/订单簿一致性测试；性能略升 ~3%。
- ✅ 撮合时间戳已由引擎前端（事实上的 sequencer）在入站时赋值；journal 化随
  Archive 工作落地。

**Gate 2**：kill -9 撮合引擎/任一 writer 进程 100 次（压测流量下），重启重放后：
① 无丢失成交；② 重放状态与 kill 前快照比特级一致；③ PG/Redis 与 journal 对账 diff = 0。
⬜ 未达成

---

## Phase 3：资金事务闭环 — 🟡 ~55%

> **目标：冻结→撮合→结算→记录 全链路无"中间态丢失"窗口。**

### 3.1 结算原子化

- ✅ pg-writer 侧：orders/fills/accounts/trades/matching_events 单事务落库（`783fab4`）。
- ✅ `settle_trade` 四条 SQL 本就在同一 DB 事务内（`account_repository.rs`）。
- ⬜ **剩余**：`settle_trade`（ws_handler 直写路径）与 persist 流（pg-writer 路径）
  双轨并存，两条路径间无原子性——需统一为"消费一条 Trade 事件 = 一个事务内完成
  trades insert + 四个账户腿 + 位点推进"，结算事件带 seq 作幂等键。

### 3.2 冻结闭环

- ⬜ 冻结事件化（gateway 发"冻结请求"，按 seq 处理并 journal 化；拒单/撤单解冻同样走事件）。
- ✅ 不变量校验任务（`bb83e94`）：`desk::reconcile` 周期检查（pg-writer 内，
  `PG_RECONCILE_SECS` 默认 300s，完全离热路径）——①悬挂冻结（有 frozen 无未结
  订单）②legacy/atoms 列漂移（>10 atoms 即告警）。violation 计数精确、样本限量，
  error 级日志。有 PG 集成测试。
- ✅ PostgreSQL 显式 `synchronous_commit = on`（`bb83e94`）：`db::create_pool_sized`
  对每个连接 after_connect 强制 SET 并启动校验，服务器不支持则 fail-fast；
  pg-writer/redis-writer 已切换。

### 3.3 对账系统（与交易系统同级别）

- ✅ PG ↔ Redis 账户对账（`fb6bc14`）：宽限窗口防异步误报、缺失/不匹配分类、
  样本限量；pg-writer 周期执行。journal 腿随 Archive 落地后补齐三方。
- ⬜ 三方对账完整版（+journal 腿）与逐订单状态层级。
- ⬜ 不变量监控：资金守恒、冻结一致性、订单簿总量 = Σ未成交订单，违反即告警+熔断出入金。

**Gate 3**：混沌测试（随机 kill、网络分区、PG 故障切换）一周连续运行，
对账 diff 始终为 0；悬挂冻结数 = 0。⬜ 未达成

---

## Phase 4：风控补全 — 🟢 ~95%

> 相对独立，可与 Phase 2/3 并行。框架沿用 `src/desk/risk/`。
> 硬性约束：热路径新增检查必须 O(1)。

- ✅ **自成交处理 = 离线稽查**（`be64a6c`，设计决定见进度跟踪节）：实时 STP
  （O(穿越订单数) 扫描）已从热路径移除，reject_reason 6 退役保留。
  交易系统的契约是提供足够 meta：成交带双方 order_id，append-only 的
  `matching_events` 永久可解 order_id → participant_id（撤单物理删除 orders
  行也不丢归因——`trades ⋈ orders` 反而有此洞）。
- ✅ **价格带保护 + fallback**（`a69bf07` + `be64a6c`）：双边盘取 mid，
  空盘/单边盘 fallback 最新成交价（开盘、流动性枯竭时段不再裸奔）；
  活跃 mid 存在时陈旧成交价不会撑大带宽；有 4 组单测。
  - ✅ band 已进 `SymbolRules::price_band_bps`（`cc687bb`），env 变为全引擎覆盖项。
- ✅ **单笔限额**（`be64a6c`）：`ENGINE_MAX_ORDER_LOTS`（市价/限价皆限）、
  `ENGINE_MAX_ORDER_NOTIONAL`（ticks×lots，i128 防溢出，仅限价）；O(1)；
  reject_reason 8；有单测含 i64::MAX 溢出用例。
- ✅ **单用户挂单数上限**（`be64a6c`）：`ENGINE_MAX_OPEN_ORDERS_PER_USER`，
  计数与 uid_map 增删严格同步（O(1) 增量维护，零扫描）；reject_reason 9；
  顺带修复 maker 全部成交后 uid_map 永不清理的内存泄漏（O(1) `contains_order` 探针）。
- ⬜ **头寸上限**：单用户头寸上限、单 symbol 全市场敞口上限（risk engine 侧）。
- ⬜ **熔断**：价格波动超阈值自动进入仅撤单模式；恢复用集合竞价或人工确认。
- ⬜ `rate_limit.rs` 限流桶状态落 Redis（现内存态，重连即重置）；加 IP 维度与下单/撤单分桶。

**Gate 4**：风控规则集有完整测试矩阵（限额边界/OI 生命周期/熔断触发与恢复均有
单测）；⬜ 闪崩仿真脚本（端到端）未做——建议作为混沌测试阶段的一部分。

---

## Phase 5：高可用与水平扩展 — 🔴 ~5%

> 依赖 Phase 2 的确定性重放，这是热备的前提。

- ✅ 设计文档已就位：`docs/plans/2026-06-06-ha-replay-model.md`
  （active/passive 单写者模型、WAL 先于发布、明确拒绝 active/active——方向正确，
  照其 Implementation Order 1–6 执行即可）。
- ✅ **技术选型已定（2026-06-06）**：WAL 层用 **Aeron Archive**（recording +
  replay/replay-merge，`fileSyncLevel=2` 保证落盘语义；recording position
  计数器实现"落盘才 ACK"）。**Aeron Cluster 否决**：service container 仅有
  Java 实现，Rust 引擎无法嵌入。演进路径 A→B：
  - A（先做）：Archive journal + etcd/consul 选主 → 单机 RPO=0、RTO 秒级；
  - B（终态）：半同步双机直复制——输入指令流 Aeron UDP 直发备机确定性回放，
    ACK 等备机位点（窗口化摊薄 ~50–100µs 同机房 RTT）；Archive 降级为冷恢复/审计源。
  - 两级通用：fencing 用 epoch/term 嵌入输出序列号（备机接管 epoch+1，下游拒旧）；
    引擎重放快（百万级 ops/s），初期不做快照，全量重放当日 journal 即可。
  - ✅ 源码级核查完成（wrapper `af8556c`）：①fsync 顺序验证——`file.sync.level≥1`
    时 RecordingPos counter 在数据 fsync **之后**才推进，"counter ≥ 落盘位置"
    恒成立 → 零 RPC 的 `RecordingPositionCounter`（counters 文件直读 + 槽位
    复用防护）即为 ACK 门控正解；②conductor 线程契约——invoker 模式下 archive
    控制调用自驱 conductor，必须与 `do_work` 同线程（已写入文档）；
    ③context 所有权语义验证无泄漏。
  - ✅ `aeron-wrapper` Archive 客户端已完成（wrapper `aa9718b`，2026-06-07）：
    connect/start_recording/stop/find/recording_position/start_replay/
    truncate + `ReplayParams::follow_from`（备机追赶原语）；
    真实 Java ArchivingMediaDriver 集成测试全绿。剩余 = exchange 侧接入。
- ⬜ **撮合主备**：备机实时重放 journal，接管时从最后 seq 续跑；
  etcd/consul 选主与 fencing（防双主双发）。
- ⬜ **gateway 无状态化**：desk-server 会话状态外置，前置 LB ×N 实例；
  WS 断线重连带 `last_seen_seq` 续传。
- ⬜ **分片去硬编码**：`counter_shard.rs` 固定 4 desk 改配置驱动 + 一致性哈希预留扩容；
  硬编码 symbol 改 DB/配置加载。
- ⬜ PG 主从（Patroni）+ PITR 备份；Redis 哨兵或集群（仅缓存，可降级）。
- ⬜ 跨机房：Aeron UDP 模式评估，journal 异地复制。

**Gate 5**：演练主撮合机断电，自动切换，RTO 达标、零成交丢失、客户端无感知（仅重连）。
⬜ 未达成

---

## Phase 6：安全与运维加固 — 🟢 ~95%（内部项全部完成）

- ✅ **JWT 密钥**（`8a0d6e7`/`f0d7070`）：环境变量化、≥32 字节、生产无密钥拒启动；
  **文件 secret**（`*_FILE`，Docker/K8s 挂载，不经环境变量）——Vault 的零依赖替代。
- ✅ **JWT 短期 + refresh + 吊销**（`86e0563`/`9c12605`）：`access_ttl` 可配、
  `/api/auth/refresh`；O(1) 内存吊销 + Redis 30s 跨 desk 传播 + admin 端点。
- ✅ **HMAC API key 签名**（`86e0563`）：时间戳 ±30s 防重放、常数时间比较、legacy 弃用期。
- ✅ **symbol 输入加固**（`8a0d6e7`）：超长/非法字符拒绝（消除截断别名）。
- ✅ **解码健壮性**（`eba79b7`）：22 入口敌意输入 CI 测试 + cargo-fuzz 脚手架，
  实跑 683 万次执行零 crash（`9c12605`）。
- ✅ **可观测性**（`9c12605`/`f0d7070`）：Prometheus 文本 `/metrics`——desk(axum 路由)、
  engine(`ENGINE_METRICS_ADDR`)、pg-writer(监听器)；计数器：撮合量/成交量/熔断、
  fenced 丢弃、序列 gap、桥队列深度/丢弃、位点 dup/gap、**对账违规(应恒 0 的告警线)**。
  VictoriaMetrics 直接抓取。
- ✅ **审计**：append-only `audit_log`（登录/注册/token/admin）+ `fund_audit`
  （freeze/release/结算四腿全流水），触发器禁改删。
- ⬜ **双人复核（dual-control）**：TODO，**暂不做**——需策略输入（哪些操作、批准人模型）。
  代码锚点在 `handle_admin_revoke`；底座 audit_log 已就绪。见 `deferred-todos.md §2`。
- ⬜ **KYC/合规**：TODO，**当前不需要**——需供应商+司法辖区决策。占位端点已在。
  见 `deferred-todos.md §1`。
- ⬜ **第三方渗透测试/安全审计**：TODO，**外部机构工作**，上线前 Gate。
  见 `deferred-todos.md §3`。
- ⬜ symbol 16→20 拓宽（CompactString）：可选独立 PR，非安全关键。

**P6 内部可做项已全部完成。** 剩三项全部非代码：两项需业务决策（双人复核策略、
KYC 供应商/辖区），一项纯外部（渗透测试）——均已在 `docs/plans/deferred-todos.md`
明确标注 TODO 且当前不做。


## 里程碑总览

| 阶段 | 内容 | 进度 | 预估剩余工时* | 依赖 |
|------|------|------|--------------|------|
| P1 | 资金定点化 | 🟡 ~65% | 1–1.5 周 | 无（最先做） |
| P2 | 事件日志 + 序列号 + 消费者重造 | 🟡 ~35% | 3.5–5 周 | P1 |
| P3 | 资金事务闭环 + 对账 | 🟡 ~45% | 1.5–2 周 | P2 |
| P4 | 风控补全 | 🟡 ~70% | 0.5–1 周 | 可与 P2/P3 并行 |
| P5 | 高可用 | 🔴 ~5% | 4–6 周 | P2 |
| P6 | 安全运维加固 | 🟡 ~25% | 1.5–2 周 | 贯穿，集中收尾 |
| — | 混沌测试 + 影子运行 + 审计 | ⬜ | 4 周 | 全部 |

\* 按 2–3 名熟悉 Rust 与交易系统的工程师估算。

## 上线前最终检查单（Launch Gates）

1. ⬜ 资金守恒 property 测试 + 每日对账 diff = 0 连续 30 天（影子/灰度环境）
2. ⬜ 100 次随机 kill -9 重放一致性测试通过
3. ⬜ 主备切换演练通过（RPO=0 或明示折中）
4. ⬜ 极端行情仿真（闪崩/插针）无穿仓、熔断正常触发
5. ⬜ 第三方安全审计关键项清零
6. ⬜ 运营手册：故障切换、停机维护、紧急停盘、人工调账（双人复核）流程演练完毕
7. ⬜ 灰度：先小额真实资金 + 白名单用户运行 ≥ 2 周

## 明确不做（Non-Goals，本期）

- 不替换 Aeron/SBE 技术栈（架构正确，只补可靠性语义）
- 不重写撮合算法（保留 `src/matching/`，仅加 STP 与确定性收尾）
- 不做跨链钱包/托管系统（独立项目，与撮合系统通过对账接口交互）
- 不追求撮合延迟进一步优化——当前性能已远超需要，**正确性优先**

## 变更日志

- **2026-06-07 晚（HEAD `86e0563`）**：「可独立闭环项全部清零」批次，5 个提交：
  - P2 `e642cf0`：persist 帧带 (publisher_id, seq)（时钟种子跨重启单调）；pg-writer
    位点与数据同事务=exactly-once、毒帧路径不推进位点；redis-writer 位点周期落 Redis；
    gap 与 publisher 重启区分计数。
  - P2 `7b0e0e1`：skiplist xorshift64* 种子化（结构可复现，性能略升 ~3%）；
    双引擎 2000 单 LCG 工作负载逐单一致性测试；订单解码收敛到统一路径。
  - P3 `fb6bc14`：PG↔Redis 账户对账（宽限窗口防误报、缺失/不匹配分类）。
  - P4 `3518336`：发现原 RateLimiter 完全未接线！SharedRateLimiter 跨连接共享、
    下单/撤单分桶、全部 WS+REST 入口接线、Redis 持久化防重启重置。P4 关闭。
  - P6 `86e0563`：HMAC-SHA256 签名 API key（±30s 防重放、常数时间比较、legacy
    弃用期）；JWT refresh + TTL env；append-only 审计日志（触发器禁改删）；
    cargo-fuzz 脚手架（与 CI 健壮性套件共享入口清单）。
- **2026-06-07（HEAD `eba79b7`）**：P1/P4 收尾 + P6 推进（每 phase 独立提交，
  全部带测试，热路径新增全 O(1)）：
  - P1 `6155075`：orders/trades atoms 列（迁移 014）+ pg-writer 双写；SymbolRules
    整数孪生与整除对齐换算；reconcile 扩展到三表漂移+超额成交；余额 API 字符串
    字段；Gate-1 守恒 property 测试（200 随机结算 atoms 级守恒）通过。
    P1 仅剩 legacy 列物理下线（等 30 天观察窗口）。
  - P4 `cc687bb`：单用户头寸上限 + 全市场 OI 上限（增量 AtomicI64，两条下单路径
    接线）；熔断状态机（仅撤单模式，reason 10）；价格带 per-symbol 化。
    P4 仅剩限流桶 Redis 持久化。
  - P6 `eba79b7`：22 个解码入口敌意输入测试（随机/截断/位翻转/非法 UTF-8）。
    P6 剩余项排期见 P6 节表格。
- **2026-06-06 晚（HEAD `8a0d6e7`）**：四个 phase 落地一批（每 phase 独立提交，
  全部带单元+集成测试，热路径新增逻辑全 O(1)）：
  - P1 `b8d00f7`：atoms 原生 settle/freeze API，cost 计算入 i128 整数域，
    legacy 列从 atoms 反推，float_ext 死代码删除，资金守恒测试。
  - P3 `bb83e94`：synchronous_commit 每连接强制+启动校验；悬挂冻结/列漂移
    周期对账（pg-writer 内离线 SQL）。
  - P4 `be64a6c`：实时 STP 退役（设计决定：合规离线稽查，meta 经
    matching_events 永久可解）；价格带空盘 fallback 最新成交价；单笔
    lots/notional 上限；单用户挂单数上限（增量计数）；uid_map 泄漏修复。
  - P6 `8a0d6e7`：JWT secret 环境变量化（生产无密钥拒启动）；symbol
    超长/非法字符拒绝（消除 pack_str16 静默截断别名风险）。
  - 设计决定 ×3：STP 离线化、HA 选型 Aeron Archive（Cluster 否决）、
    symbol 上限暂 16。
- **2026-06-06（HEAD `1cc3edb`）**：首轮进度评估。已落地：P1 定点列与整数余额判断
  （`5a40210`/`2139f58`）、P2 OrderUpdate 序列号（`11f3132`）与 `matching_events`
  审计表（`eccde5c`）、P3 单事务 flush（`783fab4`）与失败批次保留（`e1d3e00`）、
  P4 STP（`ca805df`）与价格带（`a69bf07`）、P5 HA 设计文档（`5adceed`）。
  原 10 项致命/严重缺陷中已消除 4 项（f64 余额比较、丢批、无 STP、无价格带），
  2 项部分缓解（无序列号、5 个独立 flush）。
- **2026-06（初版）**：基于全量代码审查建立六阶段计划。
