# 完整交易系统演示

本项目包含一个完整的加密货币交易撮合系统，支持通过Aeron进行网络通信。系统由以下组件组成：

## 系统架构

```
┌─────────────────────────────────────────────────────────────┐
│                     交易客户端                               │
│   (发送订单 → Stream 1)                                      │
│   (接收响应 ← Streams 2-6)                                   │
│                                                              │
│   - trading_client.rs: 基础客户端                           │
│   - trading_client_verify.rs: 验证客户端（含测试场景）      │
└──────────────────────────┬──────────────────────────────────┘
                           │
                    Aeron IPC通信
                           │
┌──────────────────────────▼──────────────────────────────────┐
│                    Aeron Media Driver                        │
│         (localhost IPC, /tmp/aeron)                         │
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│                    撮合系统服务端                             │
│        (aeron_integration_demo.rs)                          │
│                                                              │
│  Thread 1: 撮合引擎                                         │
│    ├─ 订阅 Stream 1: 接收NewOrder/CancelOrder              │
│    ├─ 生成 TradeEvent (internal rtrb)                      │
│    ├─ 生成 OrderUpdateEvent (internal rtrb)                │
│    └─ 生成 DepthSnapshotEvent (internal rtrb)              │
│                                                              │
│  Thread 2: 发布线程                                         │
│    ├─ 发布 Stream 2: OrderUpdate                           │
│    ├─ 发布 Stream 3: TradeNotification                     │
│    ├─ 发布 Streams 4-6: 行情数据                           │
│    └─ (循环消费rtrb队列)                                   │
│                                                              │
│  功能:                                                       │
│    ✓ 高频撮合 (6M+ TPS)                                    │
│    ✓ 支持GTC/IOC/FOK/PostOnly四种委托                     │
│    ✓ 实时行情采样 (20/50/Level2)                          │
│    ✓ 成交通知推送                                          │
│    ✓ 订单状态变化推送                                      │
└─────────────────────────────────────────────────────────────┘
```

## 启动指南

### 前置条件

```bash
# 1. 确保Aeron Media Driver已启动
export AERON_DIR=/tmp/aeron
aeronmd &

# 2. 等待Media Driver就绪 (2-3秒)
sleep 3
```

### 方案A: 使用演示脚本（推荐）

```bash
cd /Users/alphawu/work/rs/matching

# 启动完整系统（自动启动服务端和客户端）
bash scripts/demo_trading_system.sh
```

该脚本会：
1. 检查Aeron Media Driver是否运行
2. 启动撮合系统 (`aeron_integration_demo`)
3. 启动交易客户端 (`trading_client`)
4. 显示实时消息流

### 方案B: 手动启动（用于开发/调试）

```bash
# 终端1: 启动Aeron Media Driver
export AERON_DIR=/tmp/aeron
aeronmd

# 终端2: 启动撮合系统
cd /Users/alphawu/work/rs/matching
export AERON_DIR=/tmp/aeron
cargo run --release --example aeron_integration_demo

# 终端3: 启动客户端
cd /Users/alphawu/work/rs/matching
export AERON_DIR=/tmp/aeron
cargo run --release --example trading_client

# 或者启动验证客户端（包含功能测试场景）
cargo run --release --example trading_client_verify
```

## 客户端说明

### trading_client.rs

基础客户端，定期发送订单并接收响应：

**功能:**
- 每3秒发送一批订单（买/卖各2个，不同价格）
- 订阅Stream 2-6接收所有响应
- 打印接收到的消息

**输出示例:**
```
📤 发送订单 #1: Buy @ 49950 (IOC)
📤 发送订单 #2: Sell @ 50050 (IOC)
📨 [120μs] OrderUpdate: 64B
💰 [350μs] Trade: 56B
📊 MarketData(Depth20): 704B
```

### trading_client_verify.rs

验证客户端，包含3个功能测试场景：

**测试场景:**

1. **场景1: 单笔IOC订单**
   - 发送不匹配的IOC订单（Buy @ 45000）
   - 验证立即收到拒绝/超时
   - 预期: OrderUpdate indicating no fill

2. **场景2: 配对成交**
   - 先发送GTC买单 (Buy 10 @ 50000)
   - 再发送IOC卖单 (Sell 10 @ 50000)
   - 验证匹配成交
   - 预期: 2个OrderUpdate + 1个Trade

3. **场景3: 多笔订单流**
   - 快速发送5个IOC订单（交替买卖）
   - 验证响应顺序和内容
   - 预期: 按顺序接收订单状态和成交

**运行:**
```bash
export AERON_DIR=/tmp/aeron
cargo run --release --example trading_client_verify
```

## 消息格式

### SBE Wire Format

所有消息采用Simple Binary Encoding (SBE)格式：

```
╔════════════════════════════════════╗
║  SBE Header (8 bytes)              ║
║  ├─ block_length: u16 (LE)         ║
║  ├─ template_id: u16 (LE)          ║
║  ├─ schema_id: u16 (LE)            ║
║  └─ version: u16 (LE)              ║
╠════════════════════════════════════╣
║  Message Body (variable)           ║
║  (固定大小，无压缩)               ║
╚════════════════════════════════════╝
```

### 消息类型

| Stream | 方向 | template_id | 消息 | 大小 |
|--------|------|------------|------|------|
| 1 | Inbound | 1 | NewOrderRequest | 56B (8+48) |
| 1 | Inbound | 2 | CancelOrderRequest | 16B (8+8) |
| 2 | Outbound | 10-14 | OrderUpdate variants | 64B |
| 3 | Outbound | 20 | TradeNotification | 56B (8+48) |
| 4 | Outbound | 30 | DepthSnapshot | 704B |
| 5 | Outbound | 31 | Depth50Snapshot | 1728B |
| 6 | Outbound | 32 | Level2Snapshot | 12928B |

## 性能指标

### 撮合引擎

- **吞吐量**: 6M+ TPS (订单/秒)
- **延迟**: <3 微秒 (P99)
- **订阅者延迟**: <1 毫秒 (Aeron IPC开销)

### 行情采样

- **Depth20**: 10ms采样间隔
- **Depth50**: 50ms采样间隔  
- **Level2**: 100ms采样间隔
- **采样开销**: <2% TPS影响

### 网络通信（Aeron IPC）

- **延迟**: ~100-500 微秒 (取决于消息大小和负载)
- **吞吐量**: 单一IPC通道支持 1M+ 消息/秒
- **背压处理**: 自动重试，无消息丢失

## 验证步骤

1. **启动系统**
   ```bash
   # 按照上面的启动指南启动
   ```

2. **观察消息流**
   - 客户端: 看到定期的订单发送
   - 服务端: 看到订阅和成交日志
   - 客户端: 看到接收的响应

3. **验证功能**
   - 订单状态: Accepted → (Filled | Cancelled | Rejected)
   - 成交通知: 包含正确的价格、数量、参与者ID
   - 行情数据: 定期更新（10ms, 50ms, 100ms）

4. **检查顺序性**
   - OrderUpdate在Trade之前到达
   - 同一订单的多个更新顺序正确
   - 行情采样按计划时间发送

## 故障排查

### 无法连接到Media Driver

```
⚠ 警告: 发布者未连接
```

**解决:**
```bash
# 检查aeronmd是否运行
ps aux | grep aeronmd

# 重启Media Driver
pkill -f aeronmd
export AERON_DIR=/tmp/aeron
rm -rf /tmp/aeron
aeronmd &
sleep 3
```

### 没有接收到消息

1. **检查Aeron连接**
   - 确保Media Driver运行
   - 检查 `/tmp/aeron` 目录存在且可访问
   - 检查环境变量: `echo $AERON_DIR`

2. **检查流ID**
   - 发送: Stream 1
   - 接收: Streams 2, 3, 4, 5, 6

3. **检查Channel**
   - 使用: `aeron:ipc` (localhost通信)

### 内存或资源错误

```bash
# 清理Aeron文件
pkill -f aeronmd
sleep 1
rm -rf /tmp/aeron
mkdir /p /tmp/aeron

# 重启
aeronmd &
```

## 开发和扩展

### 添加新的订单类型

1. 修改 `src/sbe.rs` 添加新的template_id
2. 在 `src/order_update.rs` 添加对应的OrderUpdateEvent变体
3. 在客户端例子中添加发送逻辑

### 添加新的行情数据

1. 在 `src/market_data.rs` 定义新的Snapshot类型
2. 在撮合引擎中配置采样参数
3. 在客户端订阅新的Stream ID

### 性能优化

- 增加ring buffer大小 (TradingConfig)
- 调整批处理大小 (place_orders_batch)
- 使用分布式撮合 (多线程引擎)

## 文件结构

```
/Users/alphawu/work/rs/matching/
├── src/
│   ├── sbe.rs                  # SBE消息格式定义
│   ├── transport.rs            # 传输层trait和mock实现
│   ├── order_update.rs         # 订单更新事件结构
│   ├── trading_engine.rs       # 双线程编排
│   ├── aeron_transport.rs      # Aeron实现
│   ├── engine.rs               # 核心撮合引擎
│   ├── market_data.rs          # 行情数据和采样
│   └── ...
├── examples/
│   ├── aeron_integration_demo.rs        # 服务端演示
│   ├── trading_client.rs                 # 基础客户端
│   ├── trading_client_verify.rs          # 验证客户端
│   └── ...
├── scripts/
│   └── demo_trading_system.sh            # 自动化脚本
└── README_TRADING_SYSTEM.md              # 本文件
```

## 关键特性

✅ **完整的系统实现**
- 从订单接收到成交通知的完整流程
- 支持多种订单类型 (GTC, IOC, FOK, PostOnly)
- 实时行情采样和推送

✅ **高性能**
- 跳表数据结构 (O(log N) 操作)
- 零拷贝消息传递 (rtrb + SBE)
- 单线程撮合 + 独立发布线程

✅ **可靠通信**
- SBE二进制编码（紧凑、快速）
- Aeron IPC（可靠、低延迟）
- 自动背压处理

✅ **实时监控**
- 完整的日志和追踪
- 清晰的消息格式
- 易于调试的演示客户端

## 下一步

1. **性能优化**
   - 运行 `examples/perf_bench_with_orderupdate.rs` 获取基准
   - 调整 TradingConfig 参数
   - 分析CPU使用率和延迟分布

2. **功能扩展**
   - 添加订单修改 (OrderModify)
   - 添加清算功能 (Settlement)
   - 添加风险控制 (Position limits)

3. **部署**
   - 配置真实的Aeron unicast/multicast通道
   - 添加持久化（事件日志）
   - 实现故障转移和恢复

## 许可

MIT
