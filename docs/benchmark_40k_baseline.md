# Benchmark — WS Connection Scalability

*Recorded: 2026-06-03*

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

## 7. 突破 40K 的方向

### 方向 A：CPU affinity pinning（解决 40K/4 desks 退化）

将 9 个自旋线程绑定到专属核心，剩余核心给 tokio worker。可支持 4+ desks 而不抢占 engine。实现方式：`nix::sched::sched_setaffinity`。

### 方向 B：减少每连接 tokio 调度开销（突破 20K/desk 限制）

将 WS write 路径从每连接 coroutine 改为少量专用写线程，用 `crossbeam-channel` 或 `mpsc` 投递帧，彻底解耦读（tokio 管）和写（写线程管）。或使用 `io_uring` + `epoll` 直接管理大量 socket，不依赖 tokio task 调度。

---

## 8. 已知限制

- 测试均在 localhost loopback 上进行，无网络延迟；生产跨机 Aeron UDP 会增加 50–100 µs 往返。
- 20K 压测用户 CSV + CONNS > USERS 时，多连接复用同一 token（5 个连接/用户），在真实场景中每用户应有独立账户。
- 每次测试前需执行 `DELETE FROM orders;`，避免历史数据影响 DB 性能。
