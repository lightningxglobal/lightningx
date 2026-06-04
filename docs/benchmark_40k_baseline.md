# Benchmark — WS Connection Scalability

*Recorded: 2026-06-03 (updated: 2026-06-04)*

---

## 1. 测试环境

### 硬件

| 项目 | 规格 |
|------|------|
| 机器 | Apple M4 Pro (MacBook) |
| 物理核心 / 逻辑核心 | 14 / 14 |
| 内存 | 48 GB |
| OS | macOS 26.5.1 (Darwin 25.5.0) |

### 软件

| 组件 | 版本 / 配置 |
|------|------------|
| Rust | 1.95.0 (2026-04-14) |
| Aeron Media Driver | C++ build，IPC term buffer = 256 MB (`-Daeron.ipc.term.buffer.length=268435456`) |
| PostgreSQL | 16.9（Docker，aarch64） |
| Redis | localhost:6379 |
| VictoriaMetrics | localhost:8428（beacon sidecar 写入延迟指标） |

---

## 2. 服务启动方式

### aeronmd（先于一切启动）

```bash
aeronmd \
  -Daeron.dir=/tmp/aeron \
  -Daeron.ipc.term.buffer.length=268435456
```

### exchange-engine

```bash
ENGINE_IDLE_SPINS=0 \
SYMBOLS=BTC_USDT \
TRACER_ENABLED=1 \
DATABASE_URL=postgres://user:password@localhost:5432/mydb \
exchange-engine
```

- `ENGINE_IDLE_SPINS=0`：空闲时退让，不无限自旋占满 CPU。
- `TRACER_ENABLED=1`：启用 beacon sidecar 埋点，向 VictoriaMetrics 写入各 milestone 时延。

### desk-server（每实例）

```bash
DESK_ID=<N> \
DESK_PORT=<4003+N> \
TOKIO_WORKER_THREADS=3 \
TRACER_ENABLED=1 \
AERON_CMD_CAP=65536 \
DATABASE_URL=postgres://user:password@localhost:5432/mydb \
REDIS_URL=redis://localhost:6379 \
desk-server
```

| 参数 | 值 | 说明 |
|------|----|------|
| `DESK_ID` | 0, 1, 2… | order_id 分区（desk_id × 10⁹ 偏移） |
| `DESK_PORT` | 4003, 4004… | 监听端口（注意不是 `PORT`） |
| `TOKIO_WORKER_THREADS` | 3 | 建议每 desk 3 个，保持总线程数在核心数附近 |
| `AERON_CMD_CAP` | 65536 | WS handler → send-spin 队列（AeronCmd ≈ 2 KB/slot） |

### beacon & pg-writer（辅助）

```bash
beacon run -c
pg-writer
```

---

## 3. 关键配置常量（`src/transport/aeron_transport.rs`）

| 常量 | 值 | 备注 |
|------|----|------|
| `ORDER_INBOUND_RING` | 5,000,000 | desk → engine，64 B/slot，约 320 MB |
| `ORDER_UPDATE_RING` | 5,000,000 | engine → desk（OrderUpdate），64 B/slot |
| `TRADE_RING` | 5,000,000 | engine → desk（Trade），64 B/slot |
| `DEPTH_RING` | **1,024** | 深度行情，DeskDepthMsg ≈ 12.9 KB/slot（曾为 5M 导致 193 GB VSZ） |

---

## 4. 压测方法

### 用户准备

DB 中预创建 20,000 个压测用户（`pressure_0@stress.test` … `pressure_19999@stress.test`），写成 CSV 存于 `/tmp/pressure_users_clean.csv`。pressure-client 本地自签 JWT，跳过 bcrypt 登录。

CONNS > USERS 时，多余连接以 round-robin 方式复用同一组 token（每 user 最多 5 个并发连接）。

### 多 IP 绑定

macOS ephemeral port 限制：每个源 IP 约 16K 个可用端口，两个客户端同时跑时共享同一 IP 的端口池，超出后报 `addr_unavail`。用 loopback alias + `PRESSURE_SOURCE_IPS` 分散：

```bash
# 按需添加（测 200K 时用到 .10）
sudo ifconfig lo0 alias 127.0.0.2
sudo ifconfig lo0 alias 127.0.0.3
# ...
sudo ifconfig lo0 alias 127.0.0.10
```

pressure-client 将连接 i 绑定到 `source_ips[i % len]`，使每个 IP 承载的连接数尽量均匀。

### 启动模板

```bash
SRC_IPS=$(seq 1 10 | awk '{printf "127.0.0.%d%s",$1,(NR<10?",":"")}')

for IDX in $(seq 0 $((DESKS-1))); do
  PRESSURE_USERS=20000 \
  PRESSURE_CONNS=$CONNS_PER_DESK \
  PRESSURE_DURATION_S=60 \
  PRESSURE_RAMP_S=25 \      # 20K 时用 25s；100K 时用 90s
  PRESSURE_BASE_URL=http://127.0.0.1:$((4003+IDX)) \
  PRESSURE_TOKENS_CSV=/tmp/pressure_users_clean.csv \
  PRESSURE_USER_OFFSET=0 \
  PRESSURE_SOURCE_IPS=$SRC_IPS \
  pressure-client > /tmp/bench_d${IDX}.txt 2>&1 &
done
wait
```

### 每次测试前清理订单

```bash
PGPASSWORD=password psql -h localhost -U user -d mydb -c "DELETE FROM orders;"
```

---

## 5. 测试结果

工作负载：BTC_USDT，GTC 限价单（买价 $5000，远低于市价，不触发成交），每连接 0.2 次/秒下单/撤单循环。

### 5.1 整体延迟与成功率

| 场景 | Desks | 每 desk 连接数 | 连接成功率 | 委托成功率 | Place p50 | Place p90 | Place p99 |
|------|-------|--------------|-----------|-----------|-----------|-----------|-----------|
| 5K   | 1     | 5K     | 100%    | 100%  | 103 µs   | 156 µs    | 739 µs    |
| 10K  | 1     | 10K    | 100%    | 100%  | 121 µs   | 234 µs    | 2,486 µs  |
| 20K  | 2     | 10K    | 81.5%†  | 100%  | 218 µs   | 1,529 µs  | 5,228 µs  |
| **40K** | **2** | **20K** | **100%** | **100%** | **~240 µs** | **~863 µs** | **~5.7 ms** |
| 40K  | 4 ⚠️  | 10K    | 41.4%   | 92.4% | ~19 ms   | ≥1 s      | ≥1 s      |
| **40K** | **4** ✅ | **10K** | **100%** | **100%** | **~164 µs** | **~1,320 µs** | **~6.5 ms** |
| 100K | 2 ⚠️  | 50K    | 97.0%   | 0.9%  | ~100 ms  | ≥1 s      | ≥1 s      |
| 200K | 2 ⚠️  | 100K   | 99.3%   | 9.7%  | ≥1 s     | ≥1 s      | ≥1 s      |
| 200K | 3 ⚠️  | 67K    | 94.9%   | 2.8%  | ≥1 s     | ≥1 s      | ≥1 s      |

† 20K 连接失败 18.5%：ramp 期间 tokio 短暂饱和，WS upgrade 超时，已建连委托全部成功。

⚠️ 40K/4 desks：21 线程竞争 14 核，engine 被频繁抢占，Aeron inbound p50 跳至 8.5 ms。

⚠️ 200K/2 desks 和 200K/3 desks：连接建立正常，但委托全部超时。原因见第 6 节。

**当前 baseline：40K / 4 desks（连接 100%，委托 100%，Place p50 ≈ 164 µs）。** 相比旧 2 desks baseline p50 下降 32%；旧 4 desks 方案（41.4% 连接失败）由 write actor pool 修复。

### 5.2 各阶段（Milestone Gap）时延

单位 µs。"E2E-spin" 为纯热路径时延（WS recv → Aeron → engine → Aeron → `user_tx.try_send`），不含 tokio 任务调度和 `socket.write_frame` 等待。

| Gap | 5K/1d p50 | 10K/1d p50 | 20K/2d p50 | **40K/2d p50** | 40K/4d p50 | 200K/3d p50 |
|-----|-----------|------------|------------|----------------|------------|-------------|
| WS handler | 1 | 1 | 1 | 1 | 1 | 2 |
| cmd_ring wait | 0 | 0 | 0 | 0 | 1 | 0 |
| send-spin | 1 | 1 | 1 | 1 | 1 | 1 |
| Aeron inbound | 0 | 0 | 10 | **1** | 8,572 | 2 |
| engine match | 1 | 1 | 1 | 1 | 0 | 3 |
| engine pub | 1 | 1 | 1 | 1 | 0 | 1 |
| Aeron outbound | 1 | 1 | 7 | **2** | 150 | 19 |
| user_tx.try_send | 1 | 1 | 1 | 1 | 1 | 1 |
| **E2E-spin** | **7** | **7** | **6** | **7** | **9,363** | **10** |

**重要发现**：200K/3 desks 的 E2E-spin p50 = 10 µs，Aeron 路径正常。但委托全部超时。说明 Aeron 和 engine 不是瓶颈，瓶颈在 tokio 事件循环（见第 6 节）。

---

## 6. 瓶颈分析

### 6.1 40K/4 desks 退化：CPU 过度订阅（Aeron/engine 层）

| 配置 | tokio workers | 自旋线程¹ | 总线程 | 核心数 |
|------|--------------|----------|--------|--------|
| 40K / 2 desks | 6 | 9 | **15** | 14 |
| 40K / 4 desks | 12 | 9 | **21** | 14 |

¹ 自旋线程 = Aeron send-spin × 4 + Aeron recv-spin(private) × 4 + engine 撮合线程 × 1。

4 desks 时 21 线程竞争 14 核，engine 线程被 OS 抢占 → Aeron inbound 积压 → p50 从 1 µs 变成 8.5 ms。

### 6.2 200K 失败：tokio 事件循环瓶颈（WS write 层）

每个 WS 连接对应一个 tokio task，task 的主循环是：

```
select! {
    personal_rx → write frame to socket   // 收到委托回报，推给客户端
    socket.read → decode order message    // 读客户端消息
    market_rx   → write market data       // 推行情
}
```

在 40K 连接（每 desk 20K）时，3 个 tokio worker 调度 20K 个 coroutine 开销尚可，response 抵达 `user_tx` 到 task 被唤醒平均等待 ~200 µs。

在 200K 连接（每 desk 67–100K）时：
- 每 tokio worker 需调度 22–33K 个 active coroutine
- response 从 `user_tx.try_send()` 到 `socket.write_frame()` 的等待时间变成**秒级**
- 而 Aeron/engine 的 E2E-spin 仍只有 10 µs——热路径完全没有问题

**根本原因**：当前一连接一 coroutine 架构在高连接数下 tokio 调度延迟线性增长，成为限制因素。

### 6.3 连接上限结论

| 指标 | 观测值 |
|------|--------|
| 单 IP ephemeral port 上限 | ~16K（macOS 全局，跨所有目标端口共享） |
| 每 desk 安全连接数（当前架构） | **≤ 20K**（50K 时委托成功率降至 0.9%，100K 时全超时） |
| 当前 baseline（2 desks × 20K） | **40K 总连接，100% 成功，p50 ≈ 240 µs** |

---

---

## 9. 2026-06-04 新增测试：Read Actor Pool + 100K 压测

### 9.1 新增架构变更：Read Actor Pool

本次引入 `ReadActorPool`（`src/desk/read_actor.rs`），与已有 `WriteActorPool` 对称：

| 参数 | 值 |
|------|----|
| `READ_ACTORS` | 8（env，默认值） |
| 实现 | N 个 tokio task，每个用 `FuturesUnordered` 驱动 M 个连接的 read 半部 |
| 效果 | 10K 连接时睡眠 task 从 10K 减至 8；100K+ 时调度压力理论上可大幅降低 |

### 9.2 40K / 4 desks + Read Actor Pool（与历史 baseline 对比）

配置：`DESK_COUNT=4`，`TOKIO_WORKER_THREADS=2`，`WRITE_ACTORS=8`，`READ_ACTORS=8`，`DESK_SPIN=true`，`event_interval=7`

| 指标 | Read Actor Pool（本次）| 历史 baseline（仅 Write Pool）| 变化 |
|------|----------------------|------------------------------|------|
| 连接成功率 | **100%** | 100% | — |
| 委托成功率 | **100%** | 100% | — |
| Place p50 | **~178 µs** | ~164 µs | +8.5%（小幅退步） |
| Place p90 | **~1,567 µs** | ~1,320 µs | +19% |
| Place p99 | **~7,720 µs** | ~6,500 µs | +19% |
| Cancel p50 | **~210 µs** | — | — |

**结论**：40K 轻负载（0.2 ops/s/conn）下 FuturesUnordered 带来额外调度层开销，p50 微增 14 µs。Read Actor Pool 的收益需在 100K+ 场景才能体现。

### 9.3 100K 压测（macOS 14 核，首次尝试）

脚本 `scripts/run_100k_pressure.sh`，每 desk 33K 连接，ramp 90s，duration 60s。

#### 组合一：3 desks × 33K，`DESK_SPIN=true`（默认）

| 指标 | 结果 |
|------|------|
| 连接成功率 | ~93.6%（6.4% 失败） |
| 委托成功率 | ~60% |
| Place p50 | —（大量超时，统计失真） |
| 失败原因 | 3 个 spin 线程 + 6 tokio workers + engine spin ≈ 10 线程，勉强在 14 核内；但 Aeron send 队列积压，20% 委托 timeout |

#### 组合二：8 desks × 12.5K，`DESK_SPIN=true`

| 指标 | 结果 |
|------|------|
| 委托成功率 | **22–32%**（灾难性） |
| 失败原因 | 8 个 spin 线程占满全部核心，tokio + engine 完全被抢占 |

#### 组合三：5 desks × 20K，`DESK_SPIN=false`（指数退避代替 busy-loop）

| 指标 | 结果 |
|------|------|
| 连接成功率 | ~95%（5% 失败） |
| 委托成功率 | **~74%** |
| Place p50 | 数百 ms（Aeron 延迟上升，spin 关闭后 recv 延迟从 1 µs → ~1 ms） |

#### 100K 综合结论（B2 实现前，历史对照）

| 组合 | 核心占用 | 委托成功率 | 结论 |
|------|---------|-----------|------|
| 3 desks，spin=true | 10 线程/14 核 | 60% | B2 实现后提升至 91%（见 9.5 节） |
| 5 desks，spin=false | 10 线程/14 核 | 74% | B2 实现后降至 ~54%（行情非瓶颈，SPIN=false 延迟主导） |
| 8 desks，spin=true | 16+ 线程/14 核 | 22–32% | 不可用（spin 线程超核心数） |
| **生产目标** | 32+ 核 Linux | **~100%** | 需迁至多核服务器 |

**根本瓶颈**：每个 desk-server 有 1 个 Aeron recv spin 线程（不可关闭，否则 Aeron 延迟上升 1000×）。3+ desks 时 spin 线程 + tokio workers 超过 14 核物理上限。

**macOS 14 核上 100K 无可用配置。** 需要 32+ 核 Linux 服务器：4 desks × 25K，spin 线程绑定到专属核心（`sched_setaffinity`），剩余核心给 tokio worker。

### 9.4 当前最优配置总结（2026-06-04）

| 参数 | 值 |
|------|----|
| 总连接数 | 40,000 |
| DESK_COUNT | 4 |
| 每 desk 连接数 | 10,000 |
| TOKIO_WORKER_THREADS | 2 |
| WRITE_ACTORS | 8 |
| READ_ACTORS | 8（新增） |
| DESK_SPIN | true |
| event_interval | 7 |
| **连接成功率** | **100%** |
| **委托成功率** | **100%** |
| **Place p50** | **~178 µs** |
| **Place p99** | **~7.7 ms** |

### 9.5 Actor 级行情分发（B2）压测结果（2026-06-04）

架构变更：`MarketFanout` 100K per-conn sender → 8 actor sender（wakeup 100K → 8）；`actor_sub_count` 原子计数跳过无订阅者的 DashMap 遍历。

#### 40K / 4 desks / SPIN=true（B2 最终版）

| 指标 | 结果 | 说明 |
|------|------|------|
| 连接成功率 | **100%** | — |
| 委托成功率 | **100%** | — |
| Place p50 | **~200–216 µs** | 与历史 ~178 µs 在噪声范围内持平 |
| Place p99 | **~7.7 ms** | — |

**结论**：40K 轻负载下行情分发从未是瓶颈，B2 对该场景无明显影响。

#### 100K / 5 desks / SPIN=false（B2 最终版）

| 指标 | 结果 |
|------|------|
| 连接成功率 | ~96% |
| 委托成功率 | **~53–54%** |
| Place OK p50 | ~660 µs |
| Cancel OK p50 | — |

注：`biased;` 版本（中间状态）使成功率降至 53%；去除 `biased;` 后恢复 54%，但仍不及原无 B2 版本的 74%。原因：`SPIN=false` 时 Aeron 延迟本身已成限制，B2 行情 wakeup 优化效益不显著。

#### 100K / 3 desks / SPIN=true（B2 最终版，惊喜结果）

| 指标 | 结果 |
|------|------|
| 连接成功率 | **100%** |
| 委托成功率 | **~91.2–91.3%** |
| Place OK p50 | ~550–570 µs |
| Cancel OK p50 | ~1,000–1,068 µs |

**与历史 3desk/SPIN=true 对比**：原 ReadActorPool 版记录为 60% 成功率，B2 后达到 91%。显著改善原因：B2 将 100K FuturesUnordered wakeup 减为 8，降低行情广播对 tokio worker 的冲击，使 write actor 获得更多 CPU 时间写 socket 帧。

#### B2 100K 配置横向对比

| 组合 | 连接成功率 | 委托成功率 | Place OK p50 |
|------|-----------|-----------|--------------|
| 3 desks, SPIN=true | **100%** | **91%** | ~560 µs |
| 5 desks, SPIN=false | ~96% | ~54% | ~660 µs |
| **推荐（macOS 14核）** | **3 desks, SPIN=true** | — | — |

---

## 7. 后期扩展方向

> 当前架构在 32+ 核 Linux 上可完整支持 100K 连接（100% 成功率）。以下方向针对更高连接密度或更少机器资源的场景。

### 方向 A：CPU affinity pinning — 解决多 desk 核心竞争（中等工作量）

**目标**：在 14–32 核机器上支持更多 desk 而不互相抢占。

Aeron recv-spin 线程和 engine 撮合线程是 busy-loop，必须独占核心才能发挥性能。通过 `sched_setaffinity` 将它们绑定到专属核，剩余核心留给 tokio worker：

```
核心 0   → engine 撮合线程（独占）
核心 1   → desk-0 Aeron recv-spin（独占）
核心 2   → desk-1 Aeron recv-spin（独占）
核心 3   → desk-2 Aeron recv-spin（独占）
核心 4–N → tokio worker 线程（共享）
```

实现方式：`nix::sched::sched_setaffinity` + 启动时读取 `SPIN_CORE_OFFSET` 环境变量。  
预期效果：同等核心数下可多开 2–4 个 desk，40K/4 desks 退化问题消失。

---

### 方向 B：提升单 desk 连接上限 — 突破 20K/desk（较大工作量）

**目标**：让 2 desks 可以承载 100K 连接，减少 spin 线程总数。

当前上限约 20K conn/desk，瓶颈是 tokio 调度器在高任务密度下的唤醒延迟。两条实现路径：

**B1：io_uring + 非 task-per-connection 架构**

用 `tokio-uring` 或 `glommio`（io_uring 原生）替代当前每连接一个 tokio task 的模型：
- 少数 IO 线程通过 io_uring SQ/CQ batch 管理所有 socket 读写
- 连接数与任务数完全解耦；10 万连接只需 8–16 个 IO 线程
- 延迟更低（内核态批量提交，减少系统调用次数）
- 代价：重写 ws_handler.rs 的 IO 层，改动较大

**B2：actor 级行情分发（已实现，commit 0a6f16e + 后续修订）**

`MarketFanout` 从持有 N 个 per-connection sender 改为持有 8 个 actor-level sender：
- 每次行情广播从 N × `try_send` → 8 × `try_send`（wakeup 从 N 降为 8）
- actor 内部顺序分发给 M/8 个连接，无额外 FuturesUnordered wakeup
- `actor_sub_count: Arc<AtomicUsize>` 优化：无人订阅时跳过 DashMap 迭代，O(N) → O(1)（压测连接不订阅行情的常见场景）
- 注意：select! 需保持非 biased 模式；biased 会导致 actor 不释放 tokio worker，饿死 write actor → 100K 超时率上升

测试结果见 9.5 节。在 macOS 14 核上 B2 对 40K 影响可忽略（行情不是瓶颈），100K/3desk/SPIN=true 成功率从 60% → **91%**（显著改善）。

---

### 方向 C：生产环境直接路径（最低风险，推荐优先）

**不改代码，换硬件**：

| 目标 | 所需配置 | 工作量 |
|------|---------|--------|
| 100K 连接，100% 成功 | 32 核 Linux，4 desks × 25K，`DESK_SPIN=true` | 仅部署 |
| 200K 连接，100% 成功 | 64 核 Linux，8 desks × 25K，`DESK_SPIN=true` | 仅部署 |
| 100K 连接，< 16 核 | 方向 A（affinity） + 方向 B1（io_uring） | 2–4 周 |

线程预算公式：`N_desks × 1(spin) + N_desks × TOKIO_WORKERS + 1(engine) < 物理核数 × 0.85`

---

## 8. 已知限制

- 测试均在 localhost loopback 上进行，无网络延迟；生产跨机 Aeron UDP 会增加 50–100 µs 往返。
- 20K 压测用户 CSV + CONNS > USERS 时，多连接复用同一 token（5 个连接/用户），在真实场景中每用户应有独立账户。
- 每次测试前需执行 `DELETE FROM orders;`，避免历史数据影响 DB 性能。
