# 生产化改造计划（Production Roadmap）

> 基于 2026-06 对全部 ~32k 行代码的审查结论制定。
> 总体判断：**撮合核心可保留复用；资金路径与可靠性层必须重做**。
> 预估总工作量约为现有系统的 40–50%，分六个阶段，每个阶段有明确的验收门槛（Gate），
> 未过 Gate 不进入下一阶段。
>
> 图例：✅ 已完成 ｜ 🟡 部分完成 ｜ ⬜ 未开始

## 进度跟踪（最近评估：2026-06-07，HEAD `eba79b7`）

| 阶段 | 进度 | 本轮关键落地 |
|------|------|-------------|
| P1 资金定点化 | 🟢 ~90% | orders/trades atoms 列+双写、整数 rules 换算、守恒 property 测试、API 字符串（`6155075`）。仅剩 legacy 列物理下线（等观察窗口） |
| P2 事件日志+序列号 | 🟡 ~35% | （未动）下一优先：消费位点持久化 + Aeron Archive journal |
| P3 资金事务闭环 | 🟡 ~45% | synchronous_commit 强制 + 五类不变量周期对账（`bb83e94`+`6155075`） |
| P4 风控补全 | 🟢 ~95% | 头寸/敞口上限、熔断、per-symbol 价格带（`cc687bb`）。仅剩限流桶 Redis 持久化 |
| P5 高可用 | 🔴 ~5% | 方案确定：Aeron Archive 做 journal/replay（见 P5 备注），Cluster 否决 |
| P6 安全加固 | 🟡 ~40% | JWT 环境变量化、symbol 校验（`8a0d6e7`）、22 个解码器敌意输入测试（`eba79b7`）。补齐计划见 P6 节 |

**当前最关键缺口**：journal/WAL——方案已定为 Aeron Archive（fileSyncLevel=2），
在它完成前，"已对外确认但不可恢复"的窗口始终存在。
**下一个最低阻力项**：pg-writer 消费位点持久化（P2.3），与 Archive 互补。

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

## Phase 2：事件日志与序列号（可靠性的地基）— 🟡 ~35%

> **目标：撮合输入/输出全部带全局递增序列号并先落盘，任何下游可断点重放。**
> 解决：消息无序列号、Ring Buffer 满静默丢弃、Aeron 不可重放、崩溃丢成交。

### 2.1 SBE 消息格式升级

- ✅ `OrderUpdateMsg` 已加 `sequence: u64`（64→72 字节，`11f3132`），
  engine 端按 response stream 递增分配，desk_server 做 gap 检测
  （`desk_server.rs:2374` 一带）。
- ⬜ **剩余**：gap 检测到目前只能 `warn!`，无恢复手段（依赖 2.2 journal）；
  其余流（persist/depth/counter-forward）未覆盖；无 `schema_version` 字段；
  `read_unaligned` 裸读未替换（`aeron_transport.rs:101-110`）。

### 2.2 输入/输出 Journal

- 🟡 已有**审计层**：`matching_events` 追加表（迁移 013，`eccde5c`），
  主键 `(response_stream_id, sequence)` 幂等、可离线查 gap。
- ⬜ **剩余（本阶段大头，也是全系统当前最关键缺口）**：
  `matching_events` 仍经由有损的 Aeron persist 流、由 desk_server 发出——
  是审计表，不是 journal。需按 HA 文档 Implementation Order 执行：
  engine 内分配 seq → **本地 WAL append（fsync 策略待定）→ 才发 Aeron** →
  确认时点后移（ACK 在 journal 落盘后）。
- ⬜ Ring Buffer 满（`aeron_transport.rs`）改为：入站→拒绝并告知客户端；
  出站→journal 为真相，Aeron 只是推送。

### 2.3 下游消费者改造

- ✅ pg-writer flush 失败**保留批次重试**（`e1d3e00`），有 poison frame 防护计数。
- ✅ flush 全部 6 类 payload 合并**单事务提交**（`783fab4`，归属 P3 但在此实现）。
- ⬜ **剩余**：消费者断点位点（记录"已应用到 seq=N"，重启从 N+1 重放追赶）——
  位点应与数据同事务提交，实现 exactly-once 落库。
- ⬜ 现有 `hydrate/backfill/reconcile` 降级为监控期对账工具。

### 2.4 撮合确定性收尾

- ⬜ `skiplist.rs:87` 的 `rand::random()` 换成种子化 RNG（种子入 journal）。
- ⬜ 撮合路径禁止读系统时钟：时间戳由 sequencer 入站时打好、随指令进 journal。

**Gate 2**：kill -9 撮合引擎/任一 writer 进程 100 次（压测流量下），重启重放后：
① 无丢失成交；② 重放状态与 kill 前快照比特级一致；③ PG/Redis 与 journal 对账 diff = 0。
⬜ 未达成

---

## Phase 3：资金事务闭环 — 🟡 ~30%

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

- ⬜ 每日 + 每小时增量三方对账：journal ↔ PG ↔ Redis
  （资产总额、逐用户余额、逐订单状态三个层级）。
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
  - 主要工作量：`aeron-wrapper` 扩展 Archive C 客户端支持。
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

## Phase 6：安全与运维加固 — 🟡 ~25%

- ✅ **JWT 密钥**（`8a0d6e7`）：`EXCHANGE_JWT_SECRET`（≥32 字节）环境变量化；
  配置过短 = 硬错误；`EXCHANGE_ENV=production` 无密钥拒绝启动；dev/CI 回退
  公开 dev 常量并 warn。解析规则为纯函数，密钥矩阵有单测。
  - ⬜ 剩余：JWT 改短期 + refresh；API key 签名补 HMAC 时间戳防重放；Vault 集成。
- ✅ **symbol 输入加固**（`8a0d6e7`）：`MAX_SYMBOL_LEN=16`（=线格式宽度）超长
  **拒绝**而非 pack_str16 静默截断（截断曾使两个不同输入串别名同一线上 symbol /
  风控 key）；字符集限 `[A-Z0-9_]`（无 NUL/小写别名/分隔符）。校验在 REST/WS
  共用入口 `normalize_order_shape` 单点生效；边界 16/17 字节有单测。
  - ⬜ 剩余：上限拓宽到 20（CompactString 内联）需联动全部 `[u8;16]` packed
    struct 与 risk-engine DashMap key，独立 PR。
- ✅ **解码健壮性（CI 层）**（`eba79b7`）：22 个解码入口 × 三类敌意语料
  （随机垃圾 2000 例 / 全长度截断 / 全位翻转 + 非法 UTF-8），每次 cargo test
  必跑，种子化可复现。
  - ⬜ 剩余：cargo-fuzz 覆盖率导向长跑（nightly，复用同一入口清单，离线跑）。
- ⬜ **可观测性**：Prometheus 指标（撮合延迟分位数、journal lag、消费位点滞后、
  对账 diff、ring 丢弃计数）、结构化日志、关键路径 trace（现有 `tracer.rs` 可扩展）。
- ⬜ **审计**：登录/提现/管理操作审计日志（append-only）；管理后台双人复核。
- ⬜ **合规预留**：KYC 钩子、地址筛查接口、监管报送数据导出（按目标司法辖区裁剪）。
- ⬜ 渗透测试 + 第三方安全审计（上线前硬性要求）。

### P6 补齐排期（按依赖与收益排序，预估 2026-06-07 制定）

| # | 项 | 预估 | 要点 |
|---|----|------|------|
| 1 | API key HMAC 签名 | 2–3 天 | `api_keys` 表加 `secret` 列；请求带 `ts + HMAC-SHA256(secret, ts‖method‖path‖body)`；时戳偏差 >30s 拒绝（防重放）；与现有裸 api_key 并行一个弃用期 |
| 2 | JWT 短期化 + refresh | 2 天 | access 15min + refresh 7d（旋转、可吊销，吊销表入 Redis）；WS 长连接在 token 过期时要求 re-auth 帧 |
| 3 | Prometheus /metrics | 3–4 天 | `metrics` crate + exporter；每进程暴露：撮合 burst 分位数（已有统计直接导出）、ring 丢弃、persist 队列深度、flush 时延、reconcile violation 计数（应永远为 0 的告警指标）、WS 连接数 |
| 4 | 审计日志 | 3 天 | append-only `audit_log` 表（actor/action/ip/detail/hash 链）+ 登录/注册/API key 操作埋点；提现埋点等钱包系统接入时加 |
| 5 | cargo-fuzz 长跑 | 1 天搭 + 离线跑 | `fuzz/` crate，target 复用 `tests/sbe_robustness.rs` 的 `exercise_all_decoders`；nightly，CI 每夜 1h |
| 6 | 限流桶 Redis 化（P4 尾巴） | 2 天 | 桶状态 `SETEX` 落 Redis（user 与 IP 双维度、下单/撤单分桶）；热路径仍内存判断，Redis 只做断连恢复源 |
| 7 | 渗透测试 + 第三方审计 | 外部排期 | 上线 Gate，前置条件：1–4 完成 |

合计内部工时约 2–2.5 周（1 人），可与 P2/P5 并行。

---

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
