/// 完整集成测试 - 使用Mock Transport验证核心逻辑
///
/// 这个测试不依赖Aeron，直接测试：
/// - TradingEngine 接收订单
/// - 执行撮合
/// - 生成 OrderUpdate, Trade, MarketData 事件
///
/// 如果这个测试通过，说明核心逻辑正确，问题在Aeron集成上
/// 如果这个测试失败，说明核心逻辑有问题

use matching_engine::{MatchingEngine, PoolConfig, Order, Side, TimeInForce, MarketDataConfig};
use matching_engine::transport::InboundMsg;
use matching_engine::sbe::{NewOrderRequest, CancelOrderRequest};
use std::time::Duration;
use std::thread;

fn main() {
    println!("╔═════════════════════════════════════════════════════════════════╗");
    println!("║     完整集成测试 - Mock Transport (无Aeron依赖)                  ║");
    println!("╚═════════════════════════════════════════════════════════════════╝");
    println!("");

    // 创建引擎
    let pool_config = PoolConfig {
        order_capacity: 1_000_000,
        orderbook_type: matching_engine::orderbook_impl::OrderBookType::SkipList,
        queue_capacity: 10_000,
    };

    let market_data_config = MarketDataConfig::new(
        10_000_000,    // BBO 10ms
        true,          // 启用increments
        50_000_000,    // D50 50ms
        100_000_000,   // Level2 100ms
    );

    let mut engine = MatchingEngine::new(pool_config).expect("Failed to create engine");
    engine.set_market_data_config(market_data_config);

    // 创建事件通道
    let (trade_tx, mut trade_rx) = rtrb::RingBuffer::new(256);
    let (depth_tx, mut depth_rx) = rtrb::RingBuffer::new(256);
    let (depth50_tx, mut depth50_rx) = rtrb::RingBuffer::new(64);
    let (level2_tx, mut level2_rx) = rtrb::RingBuffer::new(16);

    engine.set_trade_event_sender(trade_tx);
    engine.set_depth_snapshot_sender(depth_tx);
    engine.set_depth50_sender(depth50_tx);
    engine.set_level2_sender(level2_tx);

    println!("✓ 引擎初始化完成");
    println!("");

    // 测试场景1: 发送配对订单
    println!("═══════════════════════════════════════════════════════════════════");
    println!("场景1: 发送GTC买单");
    println!("═══════════════════════════════════════════════════════════════════");

    let order1 = Order::new(
        1,
        Side::Buy,
        50000.0,
        10.0,
        TimeInForce::GTC,
        0,
    );

    match let mut affected_makers = SmallVec::new(); engine.place_order(order1, &mut affected_makers, &mut smallvec::SmallVec::new()) {
        Ok(result) => {
            println!("✓ GTC买单已接受 - Status: {:?}", result.status);
            println!("  订单ID: 1, 价格: 50000.0, 数量: 10.0");
        }
        Err(e) => {
            println!("✗ 订单被拒绝: {:?}", e);
            return;
        }
    }

    println!("");
    println!("═══════════════════════════════════════════════════════════════════");
    println!("场景2: 发送IOC卖单与买单匹配");
    println!("═══════════════════════════════════════════════════════════════════");

    let order2 = Order::new(
        2,
        Side::Sell,
        50000.0,
        15.0,
        TimeInForce::IOC,
        0,
    );

    match let mut affected_makers = SmallVec::new(); engine.place_order(order2, &mut affected_makers, &mut smallvec::SmallVec::new()) {
        Ok(result) => {
            println!("✓ IOC卖单已处理 - Status: {:?}", result.status);
            println!("  订单ID: 2, 价格: 50000.0, 数量: 15.0");
            println!("  成交: {} 股", result.filled);
        }
        Err(e) => {
            println!("✗ 订单被拒绝: {:?}", e);
        }
    }

    // 让市场数据采样运行一下
    thread::sleep(Duration::from_millis(100));

    println!("");
    println!("═══════════════════════════════════════════════════════════════════");
    println!("收集生成的事件");
    println!("═══════════════════════════════════════════════════════════════════");

    let mut trade_count = 0;
    let mut depth_count = 0;
    let mut depth50_count = 0;
    let mut level2_count = 0;

    // 收集Trade事件
    while let Ok(evt) = trade_rx.pop() {
        trade_count += 1;
        println!("💰 Trade #{}: Taker#{} x Maker#{} @ {} qty={}",
                 evt.sequence, evt.taker_id, evt.maker_id, evt.price, evt.quantity);
    }

    // 收集Depth事件
    while let Ok(evt) = depth_rx.pop() {
        depth_count += 1;
        println!("📊 Depth20 #{}: {} bids, {} asks",
                 evt.sequence, evt.num_bids, evt.num_asks);
    }

    // 收集Depth50事件
    while let Ok(evt) = depth50_rx.pop() {
        depth50_count += 1;
        println!("📊 Depth50 #{}: {} bids, {} asks",
                 evt.sequence, evt.num_bids, evt.num_asks);
    }

    // 收集Level2事件
    while let Ok(evt) = level2_rx.pop() {
        level2_count += 1;
        println!("📊 Level2 #{}: {} bids, {} asks",
                 evt.sequence, evt.num_bids, evt.num_asks);
    }

    println!("");
    println!("═══════════════════════════════════════════════════════════════════");
    println!("统计信息");
    println!("═══════════════════════════════════════════════════════════════════");
    println!("交易数: {}", trade_count);
    println!("Depth20 快照: {}", depth_count);
    println!("Depth50 快照: {}", depth50_count);
    println!("Level2 快照: {}", level2_count);

    // 验证结果
    println!("");
    if trade_count > 0 {
        println!("✓ 成交事件生成正常");
    } else {
        println!("✗ 没有生成成交事件！问题在核心撮合逻辑");
    }

    if depth_count > 0 {
        println!("✓ 行情快照生成正常");
    } else {
        println!("✗ 没有生成行情快照！问题在市场数据采样");
    }

    println!("");
    if trade_count > 0 && depth_count > 0 {
        println!("✓ 核心逻辑正确，问题可能在Aeron集成上");
    } else {
        println!("✗ 核心逻辑有问题，需要修复撮合引擎");
    }
}
