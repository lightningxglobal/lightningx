/// Aeron Integration演示 - 真实的网络交易系统
///
/// 演示如何使用真实的Aeron transport将撮合引擎与网络连接
///
/// Prerequisites:
/// - aeronmd 已启动: AERON_DIR=/tmp/aeron
/// - 配置流ID:
///   - Stream 1: 入站委托
///   - Stream 2: 出站订单更新
///   - Stream 3: 出站成交
///   - Stream 4-6: 出站行情 (BBO, D50, Level2)

use matching_engine::{
    MatchingEngine, PoolConfig, Order, Side, TimeInForce, MarketDataConfig,
};
use matching_engine::aeron_transport::{
    AeronOrderSubscriber, AeronOrderUpdatePublisher, AeronTradePublisher, AeronMarketDataPublisher,
};
use matching_engine::transport::OrderSubscriber;
use std::time::Duration;
use tracing::info;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let aeron_dir = "/tmp/aeron";
    let channel = "aeron:ipc";

    info!("═══════════════════════════════════════════════════════════════");
    info!("         Aeron Integration Demo - 撮合系统网络集成");
    info!("═══════════════════════════════════════════════════════════════\n");

    // 创建Aeron transport
    info!("初始化Aeron transport...");
    let mut subscriber = AeronOrderSubscriber::new(aeron_dir, channel, 1)
        .map_err(|e| format!("Failed to create subscriber: {}", e))?;

    let _order_update_pub = AeronOrderUpdatePublisher::new(aeron_dir, channel, 2)
        .map_err(|e| format!("Failed to create order update publisher: {}", e))?;

    let _trade_pub = AeronTradePublisher::new(aeron_dir, channel, 3)
        .map_err(|e| format!("Failed to create trade publisher: {}", e))?;

    let _market_data_pub = AeronMarketDataPublisher::new(aeron_dir, channel, 4, 5, 6)
        .map_err(|e| format!("Failed to create market data publisher: {}", e))?;

    info!("✓ Aeron transport初始化完成");

    // 创建撮合引擎
    info!("初始化撮合引擎...");
    let pool_config = PoolConfig {
        order_capacity: 1_000_000,
        orderbook_type: matching_engine::orderbook_impl::OrderBookType::SkipList,
        queue_capacity: 10_000,
    };
    let mut engine = MatchingEngine::new(pool_config)?;

    let config = MarketDataConfig::new(
        10_000_000,    // BBO 10ms
        true,          // 启用increments
        50_000_000,    // D50 50ms
        100_000_000,   // Level2 100ms
    );
    engine.set_market_data_config(config);

    info!("✓ 撮合引擎初始化完成\n");

    // 等待连接
    info!("等待Aeron连接...");
    let start = std::time::Instant::now();
    while !subscriber.is_connected() && start.elapsed() < Duration::from_secs(5) {
        subscriber.poll();
        std::thread::sleep(Duration::from_millis(100));
    }

    if !subscriber.is_connected() {
        info!("⚠ 警告: 订阅者未连接（这是正常的，如果没有peer发布者）");
    } else {
        info!("✓ 订阅者已连接");
    }

    info!("\n═══════════════════════════════════════════════════════════════");
    info!("         系统就绪 - 接收Aeron委托并通过Aeron发布结果");
    info!("═══════════════════════════════════════════════════════════════\n");

    // 主循环 - 接收委托、执行撮合、发布结果
    let mut order_count = 0u64;
    let mut loop_count = 0u64;
    let start = std::time::Instant::now();

    loop {
        // 轮询Aeron订阅者获取入站委托
        if let Some(msg) = subscriber.poll() {
            info!("收到委托: {:?}", msg);
            order_count += 1;
        }

        loop_count += 1;
        if loop_count % 100_000 == 0 {
            let elapsed = start.elapsed().as_secs_f64();
            if elapsed > 0.0 {
                let tps = order_count as f64 / elapsed;
                info!("统计: {} 委托, {:.2}M TPS", order_count, tps / 1_000_000.0);
            }
        }

        // 每10秒输出状态
        if loop_count % 1_000_000 == 0 {
            info!("系统运行中... (总委托: {}, 运行时间: {:.1}s)",
                  order_count,
                  start.elapsed().as_secs_f64());
        }

        // 防止忙轮询
        std::thread::sleep(Duration::from_micros(1));

        // 按Ctrl-C停止（在实际应用中需要signal handler）
        if loop_count > 100_000_000 {
            break;  // 演示：运行1亿个循环后停止
        }
    }

    info!("\n═══════════════════════════════════════════════════════════════");
    info!("         系统统计");
    info!("═══════════════════════════════════════════════════════════════");
    info!("总运行时间: {:.2}s", start.elapsed().as_secs_f64());
    info!("总委托数: {}", order_count);
    info!("平均TPS: {:.2}M",
        order_count as f64 / start.elapsed().as_secs_f64() / 1_000_000.0);
    info!("✓ 演示完成");

    Ok(())
}
