# HA 部署与运维手册（双机生产拓扑）

> 版本:2026-06-07,对应代码 HEAD `7ec5556`+。
> 目标读者:按本手册操作即可完成多机上线的运维工程师。
> **SLO:RTO ≤ 180s(单机实测 3.05s);资金数据 RPO = 0(PG 同步复制)。**
>
> 单机已验证的机制(本手册的前提,全部有常驻测试):
> 选主+epoch fencing(`chaos_gate5_failover`,RTO 3.05s)、
> 引擎 journal 重建(`chaos_gate2_engine`)、writer 断点续传与三方对账
> (`chaos_gate2`/`chaos_gate2_writers`)、熔断(`chaos_gate4_flashcrash`)。

---

## 0. 先读:决定整个拓扑的四个架构事实

1. **persist 流是 IPC(共享内存,写死)** → `pg-writer`/`redis-writer` **必须与
   desk-server 同机**。它们是 desk 的"伴随进程"。
2. **orders/order_update/trade/depth 流在 UDP 模式下走网络** → 跨机用**多播**
   (`ENGINE_HOST=224.x.x.x`),让主引擎、备引擎、两边的 Archive **同时收到同一份流**。
3. **每台机器跑自己的 ArchivingMediaDriver + Archive**,各自独立录制 orders 流
   → 备机重启后从**本机** Archive 重放重建订单簿,不依赖主机存活。
   推论:**备机的 Archive 必须从系统开盘前就在录制**(中途加入的空 Archive
   重建不出历史,见 §11.9 重建流程)。
4. **失去 lease 的引擎 `exit(17)`** → systemd `Restart=always` 会把被 fence 的
   旧主自动拉起,经 journal 重放后以 standby 身份回归。**failover 后无需人工干预。**

---

## 1. 拓扑总览

```
                ┌────────────── 客户端 (WS/REST) ──────────────┐
                ▼                                              ▼
        ┌──────────────┐      keepalived VIP / LB      ┌──────────────┐
        │  机器 M1      │◀────────────────────────────▶│  机器 M2      │
        │──────────────│                               │──────────────│
        │ media-driver  │   orders 多播 224.10.9.8:20121│ media-driver  │
        │ + Archive(A1) │◀═════════════════════════════▶│ + Archive(A2) │
        │ exchange-     │   双方 Archive 都在录制        │ exchange-     │
        │  engine (主)  │                               │  engine (备)  │
        │ desk-server   │   order_update/trade/depth    │ desk-server   │
        │  DESK_ID=0    │   多播回流                     │  DESK_ID=1    │
        │ pg-writer     │                               │ pg-writer     │
        │ redis-writer  │                               │ redis-writer  │
        │──────────────│                               │──────────────│
        │ PG 16 (主)    │══ 同步流复制 (sync rep) ══════▶│ PG 16 (备)    │
        │ Redis (主)    │── 可选 replica(L1 可重建)───▶│ Redis (备)    │
        └──────────────┘                               └──────────────┘
```

- **引擎主备**:PG lease 选主,备机实时静默跟单(订单簿与主机比特级一致),
  主机死后 ≤ 2×TTL 内接管(TTL=6s → 实际 ~10s 内)。
- **desk 双活**:两台 desk 同时服务(`DESK_ID` 0/1,订单 id 各占 10 亿槽位,
  响应流按 desk 分片),前面挂 VIP/LB。
- **writers 各自伴随本机 desk**:消费本机 persist IPC 流,位点按
  publisher_id 区分,互不干扰(系统原生多 publisher 设计)。
- **PG 同步复制**:资金 RPO=0 的来源。**Redis 是可重建 L1**,失效用
  `FORCE_REHYDRATE` 从 PG 重灌即可。

---

## 2. 机器与操作系统要求

| 项 | 要求 | 原因 |
|---|---|---|
| OS | Linux x86_64(内核 ≥ 5.x) | `/dev/shm` tmpfs、epoll |
| CPU | ≥ 8 物理核/台 | 撮合线程独占核 + driver + desk |
| 内存 | ≥ 32 GB | media driver term buffers + PG |
| 磁盘 | NVMe SSD;**Archive 目录必须落持久盘**(建议 RAID1) | journal 是恢复的生命线;`file.sync.level=2` 每帧 fsync |
| 网络 | 双机间 ≥ 10 GbE,同一二层(多播可达);**校时 chrony/PTP** | 多播流 + lease 时间判断 |
| Java | OpenJDK 17+ | ArchivingMediaDriver |
| 多播 | 交换机开启 IGMP snooping;或两机直连 | orders 流一对多 |

内核调优(`/etc/sysctl.d/99-exchange.conf`):

```
net.core.rmem_max=16777216
net.core.wmem_max=16777216
kernel.sched_rt_runtime_us=-1
vm.swappiness=1
```

`/dev/shm` 至少 4 GB(driver term buffers);Archive 目录**不要**放 /dev/shm。

```bash
# 每台机器统一目录布局
/opt/exchange/bin/            # exchange-engine desk-server pg-writer redis-writer journal-audit
/opt/exchange/secrets/        # database_url jwt_secret admin_token (0400, root:exchange)
/opt/exchange/aeron-all.jar
/var/lib/exchange/archive/    # Archive 录制目录(持久盘!)
/dev/shm/aeron                # media driver 目录(tmpfs)
/etc/exchange/                # EnvironmentFile per service
```

---

## 3. 网络与频道规划(必须两机一致)

| 用途 | 频道 | 端口/流 |
|---|---|---|
| orders(desk→引擎,**被录制**) | `aeron:udp?endpoint=224.10.9.8:20121\|interface=<本机IP>/24` | 流 10+(按 SYMBOLS 排序索引) |
| order_update(引擎→desk) | 同组 `:20122` | 流 200+DESK_ID |
| trade | 同组 `:20123` | 流 3 |
| depth | 同组 `:20124` | 流 4/5/6 |
| persist(desk→writers) | `aeron:ipc`(固定) | 本机内 |
| Archive 控制(本机) | `aeron:udp?endpoint=<本机IP>:8010` | — |

对应环境变量(**所有进程统一**):

```bash
AERON_TRANSPORT=udp
ENGINE_HOST=224.10.9.8          # 多播组
AERON_UDP_INTERFACE=10.0.0.5/24 # 各机填自己的内网 IP 段(M1=10.0.0.5, M2=10.0.0.6)
AERON_DIR=/dev/shm/aeron
EXCHANGE_ARCHIVE_CONTROL=aeron:udp?endpoint=10.0.0.5:8010  # 各机指向本机 Archive
```

> ⚠️ `EXCHANGE_ARCHIVE_CONTROL` 必须指向**本机**:引擎/desk 录制到本机 Archive,
> 重启从本机重放。两机互指会导致重放走网络且单点。

---

## 4. ArchivingMediaDriver(每台机器一个,最先启动)

`/etc/systemd/system/exchange-driver.service`:

```ini
[Unit]
Description=Aeron ArchivingMediaDriver
After=network-online.target

[Service]
User=exchange
ExecStartPre=/usr/bin/rm -rf /dev/shm/aeron
ExecStart=/usr/bin/java \
  --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
  --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
  -cp /opt/exchange/aeron-all.jar \
  -Daeron.dir=/dev/shm/aeron \
  -Daeron.archive.dir=/var/lib/exchange/archive \
  -Daeron.archive.control.channel=aeron:udp?endpoint=10.0.0.5:8010 \
  -Daeron.archive.replication.channel=aeron:udp?endpoint=10.0.0.5:0 \
  -Daeron.archive.recording.events.enabled=false \
  -Daeron.archive.file.sync.level=2 \
  -Daeron.archive.catalog.file.sync.level=2 \
  -Daeron.threading.mode=DEDICATED \
  -Daeron.archive.threading.mode=SHARED \
  io.aeron.archive.ArchivingMediaDriver
Restart=always
RestartSec=2
LimitMEMLOCK=infinity

[Install]
WantedBy=multi-user.target
```

> ⚠️ 三条铁律:
> 1. `file.sync.level=2` **与** `catalog.file.sync.level=2` 必须同时设
>    (只设前者 driver 直接拒启,实测踩过);
> 2. `archive.dir` 在持久盘;
> 3. `ExecStartPre` 清 `/dev/shm/aeron` 防止脏 driver 目录(Archive 目录**永不**清)。

---

## 5. 各进程环境变量全集

### 5.1 exchange-engine(两台同配,仅 interface/control 不同)

```bash
# /etc/exchange/engine.env
AERON_TRANSPORT=udp
ENGINE_HOST=224.10.9.8
AERON_UDP_INTERFACE=10.0.0.5/24
AERON_DIR=/dev/shm/aeron
EXCHANGE_ARCHIVE_CONTROL=aeron:udp?endpoint=10.0.0.5:8010
DATABASE_URL_FILE=/opt/exchange/secrets/database_url   # 指向 VIP,见 §7
SYMBOLS=BTC_USDT,ETH_USDT,SOL_USDT                     # 两机必须完全一致!
# ── 选主(HA 核心)─────────────────────────────
EXCHANGE_LEADER_ELECT=1
EXCHANGE_LEASE_TTL_SECS=6          # 默认 6;续约间隔 = TTL/3 = 2s
HOSTNAME=m1                        # 进 lease holder 标识,便于排障
# ── 风控 ────────────────────────────────────
ENGINE_PRICE_BAND_BPS=1000         # ±10% 价格带(按品种风险调)
ENGINE_CB_BPS=500                  # 5% 熔断
ENGINE_CB_WINDOW_MS=60000
ENGINE_CB_COOLDOWN_MS=300000       # 5 分钟冷却
ENGINE_MAX_ORDER_LOTS=...          # 按品种定
ENGINE_MAX_ORDER_NOTIONAL=...
ENGINE_MAX_OPEN_ORDERS_PER_USER=200
# ── 运行时 ──────────────────────────────────
ENGINE_MATCH_CORES=4,5,6           # 撮合线程绑核(隔离核,isolcpus 更佳)
ENGINE_METRICS_ADDR=0.0.0.0:9102
RUST_LOG=info
```

systemd 关键项:

```ini
[Service]
EnvironmentFile=/etc/exchange/engine.env
ExecStart=/opt/exchange/bin/exchange-engine
Restart=always          # exit(17)=丢锁自杀 → 自动以 standby 回归
RestartSec=1
LimitMEMLOCK=infinity
```

**RTO 预算**(TTL=6s):主死 → lease 过期 ≤6s → 备机下一次尝试(≤2s)抢到、
epoch+1 → 立即发布(簿已实时跟上,无需追赶)≈ **最坏 ~9s,典型 3-8s**。
不要把 TTL 调到 3s 以下:跨机场景 PG 抖动一次就可能误触发主备来回切。

### 5.2 desk-server

```bash
# /etc/exchange/desk.env  (M1: DESK_ID=0;M2: DESK_ID=1)
AERON_TRANSPORT=udp
ENGINE_HOST=224.10.9.8
AERON_UDP_INTERFACE=10.0.0.5/24
AERON_DIR=/dev/shm/aeron
EXCHANGE_ARCHIVE_CONTROL=aeron:udp?endpoint=10.0.0.5:8010   # persist 流录制到本机
DATABASE_URL_FILE=/opt/exchange/secrets/database_url
REDIS_URL=redis://10.0.0.5:6379/0
DESK_ID=0
SYMBOLS=BTC_USDT,ETH_USDT,SOL_USDT
DESK_PUBLIC_MARKET_DATA=1   # ⚠️ 非可选:trade 流消费线程承担成交结算 + MAKER 保证金更新
# ── 安全(P6)─────────────────────────────────
EXCHANGE_JWT_SECRET_FILE=/opt/exchange/secrets/jwt_secret   # ≥32 字节,两机同一份
EXCHANGE_JWT_ACCESS_TTL_SECS=900
EXCHANGE_ADMIN_TOKEN_FILE=/opt/exchange/secrets/admin_token
WS_RL_PLACE_RPS=20
WS_RL_CANCEL_RPS=40
# ── journal 留存 ─────────────────────────────
EXCHANGE_JOURNAL_RETENTION_HOURS=72   # persist 录制段落保留(位点之下每小时清)
RUST_LOG=info
```

### 5.3 pg-writer / redis-writer(与 desk 同机)

```bash
# /etc/exchange/pg-writer.env
AERON_DIR=/dev/shm/aeron
EXCHANGE_ARCHIVE_CONTROL=aeron:udp?endpoint=10.0.0.5:8010
DATABASE_URL_FILE=/opt/exchange/secrets/database_url
PG_WRITER_FLUSH_MS=20
PG_WRITER_METRICS_ADDR=0.0.0.0:9103
PG_RECONCILE_SECS=300              # 5 分钟一次对账扫描
RUST_LOG=info

# /etc/exchange/redis-writer.env
AERON_DIR=/dev/shm/aeron
EXCHANGE_ARCHIVE_CONTROL=aeron:udp?endpoint=10.0.0.5:8010
DATABASE_URL_FILE=/opt/exchange/secrets/database_url
REDIS_URL=redis://10.0.0.5:6379/0
RUST_LOG=info
```

两者 `Restart=always`:被杀/崩溃后启动时自动 journal 补缺(实测 8 杀轮换零丢失)。

---

## 6. 秘密文件

```bash
install -d -m 0750 -o root -g exchange /opt/exchange/secrets
umask 077
openssl rand -hex 32 > /opt/exchange/secrets/jwt_secret      # 64 hex = 32 字节 ✓
openssl rand -hex 32 > /opt/exchange/secrets/admin_token
echo 'postgres://exchange:<密码>@10.0.0.100:5432/exchange' > /opt/exchange/secrets/database_url
chown root:exchange /opt/exchange/secrets/*; chmod 0440 /opt/exchange/secrets/*
```

规则(代码强制):JWT secret <32 字节生产模式拒启;`*_FILE` 优先于裸 env;
文件尾随换行自动剥离。**两台机器的 jwt_secret 必须是同一份**(token 跨 desk 有效)。

---

## 7. PostgreSQL 高可用(资金 RPO=0 的来源)

### 7.1 主库(M1)`postgresql.conf` 关键项

```
wal_level = replica
synchronous_commit = on                  # 系统连接级也会强制 SET 并校验
synchronous_standby_names = 'ANY 1 (pg_m2)'   # ← 同步复制:备机确认才算提交
max_wal_senders = 5
archive_mode = on
archive_command = 'test ! -f /var/lib/exchange/walarchive/%f && cp %p /var/lib/exchange/walarchive/%f'
```

`pg_hba.conf`:`host replication replicator 10.0.0.6/32 scram-sha-256`

### 7.2 备库(M2)初始化

```bash
systemctl stop postgresql
rm -rf /var/lib/postgresql/16/main
sudo -u postgres pg_basebackup -h 10.0.0.5 -U replicator -D /var/lib/postgresql/16/main \
     -R -X stream -C -S pg_m2_slot --application-name=pg_m2
systemctl start postgresql
# 验证:主库 SELECT sync_state FROM pg_stat_replication;  → 'sync'
```

### 7.3 应用侧连接切换(VIP 方案)

sqlx 不支持多 host URL → 用 **keepalived VIP(10.0.0.100)指向 PG 主**:

- keepalived 健康检查脚本:`psql -h 127.0.0.1 -c "SELECT pg_is_in_recovery()"`
  返回 `f` 才持有 VIP;
- PG failover 时 VIP 漂移,应用连接断开 → sqlx 池自动重连到新主。
  desk/engine/writers 都容忍 PG 短暂断连(lease 失败仅 warn,除非主引擎
  连续失败超过持锁期)。

### 7.4 PG 主库故障切换流程(手动,5 分钟内)

```bash
# 1. 确认主真死(避免脑裂):M1 ping / IPMI / PG 端口
# 2. 提升备库:
sudo -u postgres pg_ctlcluster 16 main promote     # 或 SELECT pg_promote();
# 3. keepalived 自动漂 VIP(或手动 systemctl restart keepalived)
# 4. 验证应用恢复:
curl -s 10.0.0.5:9103/ | grep pg_writer            # writer metrics 在动
psql -h 10.0.0.100 -c "SELECT COUNT(*) FROM leader_lease"
# 5. 旧主复活后必须以备库身份回归(pg_rewind):
sudo -u postgres pg_rewind --target-pgdata=/var/lib/postgresql/16/main \
     --source-server="host=10.0.0.100 user=replicator" && touch standby.signal
```

> 同步复制下 PG failover **不丢已提交事务**(RPO=0)。
> 代价:备库不可达时主库写挂起——监控 `pg_stat_replication`,
> 备库计划内维护时先 `SET synchronous_standby_names=''`(降级为异步,记录工单)。

### 7.5 PITR

- WAL 归档目录每日 rsync 到对象存储/第三台机;每周日 `pg_basebackup` 全量;
- 恢复:全量解包 + `restore_command='cp /walarchive/%f %p'` +
  `recovery_target_time='...'`;**每季度真实演练一次恢复到临时实例并跑
  journal-audit 比对**。

---

## 8. Redis(可重建 L1,运维最简单的一环)

- 正常:每机一个 Redis,desk/writers 用本机实例;
- **任何 Redis 故障的统一答案**:起新实例 → `FORCE_REHYDRATE=1 ONESHOT_HYDRATE=1
  ./redis-writer`(从 PG 全量重灌)→ 起常驻 redis-writer(journal 补缺)→ 完成。
  数据无价值损失(真相在 PG);
- 不需要 Sentinel/Cluster,除非 hydrate 时长(随账户数增长)超过可接受的
  desk 降级窗口——届时再上 replica。

---

## 9. 启动/停机顺序

### 9.1 首次上线(逐条打勾)

```
□ 1. 两机时钟同步(chronyc tracking,offset < 50ms)
□ 2. PG 主从复制建好,sync_state='sync';VIP 在主
□ 3. Redis 两机各一,PING 通
□ 4. M1+M2: systemctl start exchange-driver   (Archive 开录的前提)
□ 5. M1+M2: start exchange-engine             (谁先抢到 lease 谁是主;
     看日志 "LEADERSHIP ACQUIRED (epoch N)")
□ 6. M1+M2: start desk-server pg-writer redis-writer
□ 7. 冒烟:测试账号下单→成交→撤单;curl :9102 看 engine_orders_total 在涨
□ 8. journal-audit 跑一次,exit 0
□ 9. 验收演练(§12)全部通过后才放真实流量
```

### 9.2 计划内停机

逆序:desk(摘 VIP 流量)→ writers(等 persist 位点追平:metrics
`pg_writer_applied` 不再涨)→ 备引擎 → 主引擎 → driver → PG/Redis。
**Archive 目录永远不删。**

---

## 10. 监控与告警(VictoriaMetrics)

抓取点:引擎 `:9102`、pg-writer `:9103`、desk `/metrics`(API 端口)。

| 告警 | 条件 | 含义/动作 |
|---|---|---|
| **EngineLeaderFlap** | epoch 5 分钟内 +2 以上 | lease 抖动:查 PG 延迟/网络;必要时升 TTL |
| **NoLeader** | 两机引擎日志均无 ACQUIRED,或下单无 ACK >10s | P1 级:查 PG 可用性 |
| **FencedDrops** | desk `fenced_drop_count` 增长 | 有僵尸旧主在发布(正常 failover 后短暂出现即停;持续=异常) |
| **SeqGap** | `seq_gap_frames` > 0 | persist 流出现真实缺口:立刻跑 journal-audit |
| **DupSpike** | `duplicate_seq_frames` 突增 | writer 重启重放(正常);持续增长=位点不前进 |
| **CBTrip** | `engine_cb_trips_total` +1 | 熔断触发:通知交易/风控值班 |
| **PGReplLag** | `pg_stat_replication.replay_lag` > 5s | 同步备库追不上:查备机 IO |
| **ArchiveDisk** | /var/lib/exchange/archive 使用 > 70% | 扩容/检查 retention(§13) |
| **ReconcileDrift** | pg-writer 对账日志出现 drift | P1 级:冻结出金,人工核账 |

---

## 11. 故障 Playbook

| # | 故障 | 自动? | 动作 |
|---|------|------|------|
| 11.1 | **主引擎进程死** | ✅ 全自动 | 备机 ~10s 接管(epoch+1);旧主被 systemd 拉起自动变 standby。事后看 FencedDrops 归零即健康 |
| 11.2 | **备引擎进程死** | ✅ | systemd 重启,本机 Archive 重放追平后继续静默跟单。监控重放时长 |
| 11.3 | **desk 死/desk 机器死** | 半自动 | VIP/LB 摘除故障 desk,流量全走另一台(双活,客户端重连重登即可)。该机 persist 尾部未入库帧:该机恢复后 writer 重放自动补 |
| 11.4 | **pg-writer / redis-writer 死** | ✅ | systemd 重启 → journal 补缺(实测零丢失)。无需人工 |
| 11.5 | **PG 主死** | 手动 §7.4 | promote 备库 + VIP 漂移,5 分钟内。RPO=0 |
| 11.6 | **Redis 死** | 手动 §8 | FORCE_REHYDRATE 重灌 |
| 11.7 | **media driver / Archive 死** | 部分 | driver 重启后:本机引擎/desk 的 Aeron 客户端会报错退出 → systemd 连环拉起,引擎以 standby 回归(本机录制有 driver 重启造成的缺口,但另一台机器的录制是完整的——此时**不要**让该机引擎做主,先走 §11.9 重建) |
| 11.8 | **整机失联(M1 全挂)** | 引擎自动 | 引擎:M2 ~10s 接管 ✓。desk:VIP 漂到 M2 ✓。PG:若 M1 带着 PG 主一起挂 → §7.4 提升 M2。**损失界定**:M1 desk 已 ACK 但未入 PG 的 persist 尾帧(≤ 秒级窗口)随 M1 磁盘共存亡——M1 磁盘没坏就能在修复后补回;磁盘坏则丢失该窗口的衍生记录(资金主路径在 PG 同步复制内,不受影响) |
| 11.9 | **备机 Archive 重建**(新机/磁盘更换/11.7 后) | 半自动 | ① 新机 driver+Archive 启动;② 跑引导工具(无需停流量,源录制活跃也可,e2e 实测):`AERON_DIR=/dev/shm/aeron EXCHANGE_ARCHIVE_CONTROL=<本机控制通道> SRC_ARCHIVE_CONTROL=<对端控制通道> SYMBOLS=<与引擎一致> journal-replicate`(exit 0 = 全部录制已拉齐,幂等可重跑);③ `systemctl start exchange-engine` —— 引擎从本机 Archive 重放重建簿,自动转 standby。常驻演练:`tests/chaos_standby_bootstrap.rs` |
| 11.10 | **脑裂疑似**(两机都认为自己是主) | 理论不可能 | lease 是单行 PG 记录,epoch 单调;低 epoch 输出被 desk 全量丢弃(FencedDrops 可见)。若 FencedDrops 持续增长:`SELECT * FROM leader_lease` 看 holder/epoch,kill 低 epoch 进程 |

---

## 12. 上线验收演练(每条都要真实执行并留档)

混沌测试套件就是验收工具(在预生产环境跑,带真实双机配置):

```bash
# 1. writer 杀戮 ×100(预期:零丢失零重复,~130s)
CHAOS_KILLS=100 cargo test --test chaos_gate2 -- --nocapture
# 2. 双 writer 轮换 + 三方对账
CHAOS_KILLS=20 cargo test --test chaos_gate2_writers -- --nocapture
# 3. 引擎杀戮 + 簿一致
CHAOS_KILLS_ENGINE=10 cargo test --test chaos_gate2_engine -- --nocapture
# 4. 跨机 failover(手动,生产拓扑):
#    a. 确认 M1 主(日志 epoch N);压测流量打开
#    b. M1: kill -9 $(pgrep exchange-engine);秒表计时到 M2 首个 ACK
#    c. 记录 RTO;断言 < 180s(预期 < 15s);FencedDrops 短暂出现后归零
#    d. M1 引擎自动回归 standby(日志确认 journal 重放完成)
#    e. 反向再演一次(M2 → M1)
# 5. PG failover 演练(§7.4 全流程,含 pg_rewind 回归)
# 6. 整机断电演练(11.8):拔 M1 电源,验证接管 + 恢复后对账 diff=0
# 7. journal-audit + reconcile 全绿收尾
```

**验收标准:每项的实测值记入表格,跨机 RTO < 180s(预期 <15s),
三方对账 diff=0,资金守恒测试通过。** 此后每季度重演 4-7。

---

## 13. 日常运维

| 周期 | 动作 |
|---|---|
| 每日(cron) | `journal-audit` 跑一次,exit≠0 即告警;检查 PG 复制 lag、Archive 磁盘 |
| 每周 | `pg_basebackup` 全量;WAL 归档外移确认 |
| 每月 | 演练 Redis 重灌(§8)计时;审查 audit_log 异常 |
| 每季 | §12 第 4-7 项重演;PITR 真实恢复演练 |
| 容量 | orders 流录制 ≈ 88B/单 → 1 万单/秒 ≈ 76 GB/天;persist 流 152B/帧。按 §5.2 retention 72h 估算磁盘,**注意 orders 流(引擎重建依赖)当前不能随意截断,见 §14.2** |

---

## 14. 已知限制与改进路线(诚实清单)

1. ~~备机 Archive 中途加入流程繁琐~~ **已解决**:`journal-replicate` 引导工具
   (Archive 间有界复制,wrapper `5e18352` + matching 工具),§11.9 已更新为
   一条命令;新机引导有 e2e 演练(`chaos_standby_bootstrap`)。
2. **orders 流 journal 无限增长**——引擎重建需要从 genesis 重放。根治:定期
   引擎快照(序列化订单簿+uid_map 到 PG/文件)+ 截断快照点之前的录制。
   单 symbol 重放速度实测 ~10 万 op/s 量级,百万订单历史重放仅秒级,
   **量起来之前不急**;监控重启重放耗时,超过 60s 时排期快照功能。
3. **desk 机器整机毁灭的 persist 尾窗**(§11.8)——已 ACK 未入 PG 的衍生记录
   (秒级窗口)依赖该机磁盘。缓解:可用 `journal-replicate` 周期性(如每分钟
   cron)把 persist 流录制增量复制到对端 Archive,把尾窗从"磁盘寿命"缩到
   "复制周期";完全消除需要持续复制(continuous replication,wrapper 已支持
   `stop_position=None`,接线留待需要时)。
4. **PG failover 是手动的**(5 分钟级,远低于资金路径的 RTO 要求,但需要值班)。
   若要全自动:上 Patroni + etcd,把本手册 §7 替换为 Patroni 管理,
   应用侧不变(仍走 VIP)。
5. 多播不可用的网络(部分云环境):改用 Aeron MDC(multi-destination-cast),
   desk 的 orders publication 改 control 模式频道字符串——需要一个小的
   channel 配置扩展,届时排期。

---

## 附录 A:lease 参数速查

| 参数 | 默认 | 说明 |
|---|---|---|
| `EXCHANGE_LEADER_ELECT` | 关 | `=1` 开启选主(不设则单机直跑) |
| `EXCHANGE_LEASE_TTL_SECS` | 6.0 | 锁有效期;续约间隔=TTL/3;**RTO ≈ TTL+TTL/3+亚秒** |
| holder 标识 | `engine-<pid>-<HOSTNAME>` | `SELECT * FROM leader_lease` 排障用 |
| 失锁行为 | `exit(17)` | systemd Restart=always → 自动 standby 回归 |
| epoch | 接管时 +1 | 戳在响应序列号高 16 位;desk 丢弃低 epoch(fenced) |

## 附录 B:验收记录模板

| 项 | 日期 | 实测值 | 标准 | 通过 |
|---|---|---|---|---|
| 跨机引擎 failover RTO | | __s | <180s | □ |
| 反向 failover RTO | | __s | <180s | □ |
| PG promote 总耗时 | | __min | <10min | □ |
| 整机断电→恢复对账 | | diff=__ | =0 | □ |
| journal-audit | | exit __ | 0 | □ |
| 100 杀混沌 | | 丢__重__ | 0/0 | □ |
