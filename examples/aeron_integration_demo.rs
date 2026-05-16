/// Aeron Integration演示 - 真实的网络交易系统
///
/// 完整的双线程撮合系统：
/// - Thread 1: 撮合引擎（接收订单，执行匹配，生成事件）
/// - Thread 2: 发布线程（消费事件，编码SBE，发送响应）
///
/// Prerequisites:
/// - aeronmd 已启动: AERON_DIR=/tmp/aeron
/// - 配置流ID:
///   - Stream 1: 入站委托 (NewOrder/CancelOrder)
///   - Stream 2: 出站订单更新 (OrderUpdate)
///   - Stream 3: 出站成交 (TradeNotification)
///   - Stream 4-6: 出站行情 (Depth20, Depth50, Level2)

use matching_engine::{
    MatchingEngine, PoolConfig, MarketDataConfig,
};
use matching_engine::aeron_transport::{
    AeronOrderSubscriber, AeronOrderUpdatePublisher, AeronTradePublisher, AeronMarketDataPublisher,
};
use matching_engine::trading_engine::{TradingEngine, TradingConfig};
use std::time::Duration;
use tracing::info;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let aeron_dir = std::env::var("AERON_DIR").unwrap_or_else(|_| "/tmp/aeron".to_string());
    let channel = "aeron:ipc";

    info!("╔═════════════════════════════════════════════════════════════════╗");
    info!("║      Aeron Integration Demo - 完整的双线程撮合系统              ║");
    info!("╚═════════════════════════════════════════════════════════════════╝");
    info!("");
    info!("架构:");
    info!("  Thread 1: 撮合引擎 (接收订单 → 执行匹配 → 生成事件)");
    info!("  Thread 2: 发布线程 (消费事件 → 编码SBE → 发送响应)");
    info!("");
    info!("流ID配置:");
    info!("  Stream 1: 入站委托 (NewOrder/CancelOrder)");
    info!("  Stream 2: 订单更新 (ACCEPTED/FILLED/CANCELLED/REJECTED)");
    info!("  Stream 3: 成交通知 (Trade)");
    info!("  Stream 4: 行情快照 (Depth20)");
    info!("  Stream 5: 行情快照 (Depth50)");
    info!("  Stream 6: 行情快照 (Level2)");
    info!("");
    info!("Aeron Dir: {}", aeron_dir);
    info!("Channel: {}", channel);
    info!("");

    // 配置撮合引擎
    info!("初始化撮合引擎配置...");
    let pool_config = PoolConfig {
        order_capacity: 1_000_000,
        orderbook_type: matching_engine::orderbook_impl::OrderBookType::SkipList,
        queue_capacity: 10_000,
    };

    let market_data_config = MarketDataConfig::new(
        10_000_000,    // Depth20: 10ms
        true,          // 启用increments
        50_000_000,    // Depth50: 50ms
        100_000_000,   // Level2: 100ms
    );

    let trading_config = TradingConfig {
        pool_config,
        market_data_config,
        order_update_ring: 65536,
        trade_ring: 65536,
        depth_ring: 1024,
        depth50_ring: 256,
        level2_ring: 64,
    };

    info!("✓ 配置完成");
    info!("");

    // 创建TradingEngine
    info!("创建TradingEngine...");
    let engine = TradingEngine::new(trading_config);
    info!("✓ TradingEngine创建完成");
    info!("");

    // 创建Aeron transport
    info!("初始化Aeron transport...");
    let subscriber = Box::new(
        AeronOrderSubscriber::new(&aeron_dir, channel, 1)
            .map_err(|e| format!("Failed to create subscriber: {}", e))?
    );

    let order_update_pub = Box::new(
        AeronOrderUpdatePublisher::new(&aeron_dir, channel, 2)
            .map_err(|e| format!("Failed to create order update publisher: {}", e))?
    );

    let trade_pub = Box::new(
        AeronTradePublisher::new(&aeron_dir, channel, 3)
            .map_err(|e| format!("Failed to create trade publisher: {}", e))?
    );

    let market_data_pub = Box::new(
        AeronMarketDataPublisher::new(&aeron_dir, channel, 4, 5, 6)
            .map_err(|e| format!("Failed to create market data publisher: {}", e))?
    );

    info!("✓ Aeron transport初始化完成");
    info!("");

    // 启动双线程撮合系统
    info!("╔═════════════════════════════════════════════════════════════════╗");
    info!("║          启动双线程撮合系统 (Matching + Publishing)            ║");
    info!("╚═════════════════════════════════════════════════════════════════╝");
    info!("");

    let (matching_handle, publishing_handle) = engine.run(
        subscriber,
        order_update_pub,
        trade_pub,
        market_data_pub,
    );

    info!("✓ 撮合线程已启动 (Thread 1)");
    info!("✓ 发布线程已启动 (Thread 2)");
    info!("");

    // 定期打印线程存活状态
    let _monitor_handle = std::thread::spawn(move || {
        let mut matching_logged = false;
        let mut publishing_logged = false;
        loop {
            std::thread::sleep(Duration::from_secs(5));
            if matching_handle.is_finished() && !matching_logged {
                info!("⚠️ ALERT: Matching thread has finished!");
                matching_logged = true;
            }
            if publishing_handle.is_finished() && !publishing_logged {
                info!("⚠️ ALERT: Publishing thread has finished!");
                publishing_logged = true;
            }
        }
    });

    // 等待连接建立（多个Aeron客户端需要时间协调）
    info!("等待Aeron连接建立...");
    std::thread::sleep(std::time::Duration::from_secs(3));
    info!("✓ 连接建立完成");
    info!("");

    info!("═════════════════════════════════════════════════════════════════");
    info!("         系统就绪 - 等待Aeron委托输入");
    info!("═════════════════════════════════════════════════════════════════");
    info!("");
    info!("按Ctrl+C停止系统");
    info!("");

    // 主线程保持活跃，保持publishers/subscribers存活
    // 工作线程在无限循环中运行
    loop {
        std::thread::sleep(Duration::from_secs(1));
    }

    // 以下代码永不执行，但保留以供后续使用
    #[allow(unreachable_code)]
    {
        matching_handle.join().ok();
        publishing_handle.join().ok();

        info!("");
        info!("═════════════════════════════════════════════════════════════════");
        info!("✓ 系统已停止");
        Ok(())
    }
}
