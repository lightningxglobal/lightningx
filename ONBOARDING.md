# 项目入门指南：Rust高频交易撮合系统 + Aeron集成

## 📌 项目概览

完整的加密货币交易撮合系统，集成了Aeron高性能IPC通信。

**核心架构：**
- 撮合引擎：O(log N) SkipList订单簿，6M+ TPS
- 双线程设计：匹配线程 + 发布线程（通过rtrb ring buffer通信）
- Aeron传输：6个独立流（入站订单、出站更新/成交/行情）
- SBE编解码：高效的二进制消息格式

**关键文件：**
```
src/
  engine.rs               ← MatchingEngine核心（已稳定）
  market_data.rs          ← 行情快照/聚合（已稳定）
  aeron_transport.rs      ← Aeron发布者/订阅者实现（已修复）
  trading_engine.rs       ← 双线程调度（已修复）
  sbe.rs                  ← 消息编解码（已稳定）

examples/
  aeron_integration_demo.rs    ← 服务端示例
  trading_client.rs            ← 客户端示例（已修复）
```

---

## 🚨 已知陷阱 & 解决方案

### 陷阱 1：Aeron消息"幽灵丢失"

**症状：** 
- 服务器的publish()日志显示"✅ sent"
- 但客户端接收0条消息
- EventCallback.on_data()从未被调用

**根本原因：**
```
Aeron消息接收需要两步：
  1. client.do_work()     ← 处理网络I/O
  2. subscriber.poll()    ← 读取订阅缓冲，触发回调

缺少第2步 = 消息在缓冲中永远得不到读取
```

**解决方案：**
```rust
// ❌ 错误（0条消息）
while elapsed < duration {
    client.do_work();  // 只做一半
    // 等待callback ... 永不发生
}

// ✅ 正确（100%消息）
while elapsed < duration {
    client.do_work();       // 处理I/O
    subscriber1.poll();     // 显式读取Stream 1
    subscriber2.poll();     // 显式读取Stream 2
    // ... 每个订阅都要poll()
    
    while let Ok(msg) = rx.try_recv() {
        // 现在callback已被触发，消息在这里
    }
}
```

**参考代码：** `examples/trading_client.rs` line 631-637 (receiver_thread_main)

---

### 陷阱 2：Aeron订阅被无声drop

**症状：**
- 订阅创建成功，日志显示"✓ Subscriptions已就绪"
- 但poll()永不返回消息
- 甚至加了poll()也无法接收

**根本原因：**
```rust
let _sub1 = client.add_subscription(...);  // 下划线 = 立即drop!
// _sub1已被unregistered，再也收不到消息了
```

Rust的`let _var = ...`用来抑制"未使用变量"警告，但同时会立即丢弃该值。对于Aeron订阅这样的有状态资源，这会导致资源被unregistered。

**解决方案：**
```rust
// ❌ 错误：立即drop
let _sub2 = client.add_subscription(...);

// ✅ 正确：保活整个生命周期
let mut sub2 = client.add_subscription(...);
// 保持所有权直到离开作用域
while elapsed < duration {
    sub2.poll();
}
// 这里才drop，Aeron整个循环中保持注册
```

**记住：** Rust下划线是"刻意忽略"的信号，而不只是代码风格。

---

### 陷阱 3：Aeron注册竞态条件

**症状：**
- 订阅和发布都创建了，但消息仍然丢失
- 增加poll()调用频率也没帮助
- 只有"运气好"时才收到消息

**根本原因：**
```
Aeron需要时间让Pub和Sub在Media Driver中协商同步。
如果发布消息时Sub还没完全注册，消息会丢失。
```

**解决方案：**
```rust
// 创建所有subscriptions后，等待它们在Media Driver中完全注册
for i in 0..200 {
    client.do_work();
    thread::sleep(Duration::from_millis(10));
}
// 现在可以安全地发送消息

// 同样，主线程发送数据前也要等待接收线程准备好
thread::sleep(Duration::from_millis(4000));  // 给receiver_thread足够时间
// 现在可以发送订单
```

**经验数字：**
- 单个client初始化：100x do_work() + 10ms sleep = ~1秒
- Pub和Sub同步：多加100次 = 2秒总等待

---

## 🔍 调试方法论

### 当遇到"消息丢失"时的标准诊断流程

```
第1步：验证发送方
  ✓ 在publish()函数中添加日志
  ✓ 检查："✅ sent"日志出现了吗？
  → 如果NO：问题在server，修复server
  → 如果YES：继续第2步

第2步：验证接收回调
  ✓ 在EventCallback.on_data()中添加日志
  ✓ 检查："on_data called"日志出现了吗？
  → 如果NO：问题在poll()或subscription，继续第3步
  → 如果YES：问题在消息处理，检查rx.try_recv()

第3步：检查poll()和subscription
  ✓ 搜索所有subscriber.poll()调用
  ✓ 检查：是否在接收循环中调用了？
  → 如果NO：添加poll()调用
  → 如果YES：继续第4步

第4步：检查subscription生命周期
  ✓ 搜索所有 let _sub... 或 let sub...
  ✓ 检查：是否使用了下划线前缀？
  → 如果YES：改为 let mut sub（保活）
  → 如果NO：继续第5步

第5步：增加注册等待时间
  ✓ 增加初始do_work()循环次数（100 → 200）
  ✓ 增加主线程等待时间（2秒 → 4秒）
  ✓ 重测
```

**关键诊断工具：**
```bash
# 查看所有subscribe调用
grep -n "add_subscription" examples/trading_client.rs

# 查看所有poll调用
grep -n "\.poll()" examples/trading_client.rs

# 查看Aeron日志
RUST_LOG=debug cargo run --example trading_client 2>&1 | grep -i aeron
```

---

## ✅ 最佳实践

### 1. Aeron集成的正确姿态

```rust
// 模板：完整的Aeron接收循环

struct MyCallback {
    tx: Sender<Message>,
}

impl PollCallback for MyCallback {
    fn on_data(&mut self, data: &[u8]) {
        // 解析数据，发送到mpsc
        let _ = self.tx.send(parse_message(data));
    }
}

fn main() {
    let client = AeronClient::new("/tmp/aeron")?;
    
    // 创建subscription（保持mut，避免_prefix）
    let mut sub = client.add_subscription(
        "aeron:ipc",
        2,
        10_000,
        MyCallback { tx },
        NoopLifecycle,
    )?;
    
    // 等待注册
    for i in 0..100 {
        client.do_work();
        if i % 20 == 0 { eprintln!("init {} steps", i); }
        thread::sleep(Duration::from_millis(10));
    }
    
    // 接收循环
    loop {
        client.do_work();   // 处理I/O
        sub.poll();         // 读取缓冲！这一行关键！
        
        while let Ok(msg) = rx.try_recv() {
            process(msg);
        }
        
        thread::sleep(Duration::from_millis(1));
    }
}
```

### 2. 分离验证：先server后client

开发Aeron系统时，**总是**按这个顺序测试：
1. 启动server，运行几秒钟，停止
2. 检查server日志："✅ published"
3. 启动client，观察是否接收
4. 看client日志的EventCallback调用和接收计数

**不要这样：** 一起启动server和client再调试。太复杂！

### 3. 诊断日志的位置

关键位置添加简单日志：
```rust
// 在publish()成功后
info!("Published message on stream {}", stream_id);

// 在on_data()被调用时
info!("Callback on stream {} received {} bytes", stream_id, data.len());

// 在poll()附近
info!("Polling subscription {}", stream_id);
```

**不要过度日志化。** 只在关键信号点添加。太多日志会淹没真正的问题。

### 4. 订阅的正确生命周期

```rust
// ❌ 错误：多个下划线前缀
let _sub1 = client.add_subscription(...)?;
let _sub2 = client.add_subscription(...)?;

// ✅ 正确：全部保活
let mut sub1 = client.add_subscription(...)?;
let mut sub2 = client.add_subscription(...)?;

while condition {
    sub1.poll();
    sub2.poll();
}
// 这里才drop，保证整个循环中Aeron保持注册
```

---

## 📚 参考资源

**官方examples（最可靠的参考）：**
- `aeron-wrapper/examples/pong.rs` - 订阅和回调的标准模式
- `aeron-wrapper/examples/cping.rs` - 发布者模式
- 这些examples的设计就是我们要遵循的

**本项目关键文件：**
- `src/aeron_transport.rs` - AeronOrderUpdatePublisher/AeronTradePublisher的实现
- `examples/trading_client.rs` - receiver_thread_main()展示完整的接收模式
- `examples/aeron_integration_demo.rs` - server端的正确setup

**之前的调试笔记：**
- 存储在项目memory目录，记录了完整的bug修复历程
- `aeron_subscriber_poll_requirement.md` - 这次修复的完整分析

---

## 🎯 快速检查清单

遇到Aeron相关问题时，快速扫一遍：

- [ ] Server的publish()成功吗？（查看✅日志）
- [ ] Client的EventCallback.on_data()被调用吗？（添加日志验证）
- [ ] 每个subscriber都call了poll()吗？（grep搜索）
- [ ] Subscription用了下划线前缀吗？（grep搜索let _sub）
- [ ] 初始等待是否足够？（try增加到200次do_work()）
- [ ] Main thread等待了足够长吗？（try增加sleep时间）

如果全部打勾但仍然失败，才值得深入代码审查。

---

## 📞 求助指南

如果你碰到消息相关的问题：

1. **第一步：** 按照上面的"标准诊断流程"从第1步开始
2. **第二步：** 添加诊断日志在poll()和on_data()
3. **第三步：** 对比你的代码和`examples/trading_client.rs`中的receiver_thread_main()
4. **第四步：** 查看memory目录中的`aeron_subscriber_poll_requirement.md`了解完整的bug历史

如果还是卡住了，这些问题值得提出来：
- "我的poll()在哪一行？"
- "Subscription用了_前缀吗？"
- "初始等待有多长？"

---

## 🚀 验证系统工作的命令

```bash
# 1. 启动 aeronmd（在单独的终端）
export AERON_DIR=/tmp/aeron
/Users/alphawu/work/cc/aeron/cppbuild/Release/binaries/aeronmd

# 2. 启动 server（在另一个终端）
export AERON_DIR=/tmp/aeron RUST_LOG=info
cargo run --release --example aeron_integration_demo

# 3. 启动 client（在第三个终端）
export AERON_DIR=/tmp/aeron RUST_LOG=info
cargo run --release --example trading_client

# 预期结果：
# - Server: ✅ published OrderUpdate (attempts=1)
# - Client: OrderUpdate: 11, Trade: 3, Depth20: 11, ...
```

---

**最后一个金言：**

> Aeron不会沉默地丢失消息。如果你看不到消息，那是因为poll()没被调用。  
> 当你加上poll()，一切都会工作。相信这个过程。

祝你编码愉快！
