/// 市场数据管道集成示例
/// 演示撮合引擎 → 市场数据引擎的深度采样和增量计算流程

use matching_engine::{
    MatchingEngine, PoolConfig, Order, Side, TimeInForce,
    DepthSnapshotEvent, MarketDataConfig,
    MarketDataEngine,
};
use rtrb::RingBuffer;

fn main() {
    println!("=== 市场数据管道集成示例 ===\n");

    // 1. 创建撮合引擎，配置深度采样
    let mut engine = MatchingEngine::new(PoolConfig::default())
        .expect("创建引擎失败");

    // 配置：启用Depth50和Level2增量采样
    let config = MarketDataConfig::new(
        10_000_000,   // 10ms - BBO + Depth20
        true,         // enable_depth_increments
        50_000_000,   // 50ms
        100_000_000,  // 100ms
    );
    engine.set_market_data_config(config);

    // 2. 创建ring buffer用于传输深度快照事件
    let (depth_tx, mut depth_rx) = RingBuffer::<DepthSnapshotEvent>::new(100);
    engine.set_depth_snapshot_sender(depth_tx);

    // 3. 创建市场数据引擎
    let mut market_data_engine = MarketDataEngine::new();

    // 4. 模拟一些交易生成深度数据
    println!("步骤 1: 生成初始深度数据...\n");

    let orders = vec![
        // 买单
        Order::new(1, Side::Buy, 50000.0, 10.0, TimeInForce::GTC, 0),
        Order::new(2, Side::Buy, 49999.0, 20.0, TimeInForce::GTC, 0),
        Order::new(3, Side::Buy, 49998.0, 15.0, TimeInForce::GTC, 0),
        // 卖单
        Order::new(4, Side::Sell, 50001.0, 12.0, TimeInForce::GTC, 0),
        Order::new(5, Side::Sell, 50002.0, 18.0, TimeInForce::GTC, 0),
        Order::new(6, Side::Sell, 50003.0, 25.0, TimeInForce::GTC, 0),
    ];

    for order in orders {
        let result = engine.place_order(order);
        match result {
            Ok(place_result) => {
                println!("订单 {} 已下单: {:?}", order.id, place_result.status);
            }
            Err(e) => eprintln!("订单 {} 错误: {:?}", order.id, e),
        }
    }

    println!("\n步骤 2: 处理深度快照事件...\n");

    // 5. 消费深度快照事件
    let mut snapshot_count = 0;
    while let Ok(snapshot) = depth_rx.pop() {
        snapshot_count += 1;
        println!("收到深度快照 #{}:", snapshot.sequence);
        println!("  时间戳: {}", snapshot.timestamp);
        println!("  买档数: {}, 卖档数: {}", snapshot.num_bids, snapshot.num_asks);

        // 显示前3档
        if snapshot.num_bids > 0 {
            print!("  最优买价: ");
            for i in 0..snapshot.num_bids.min(3) as usize {
                let (price, qty) = snapshot.bids[i];
                print!("{:.1}({:.1}) ", price, qty);
            }
            println!();
        }

        if snapshot.num_asks > 0 {
            print!("  最优卖价: ");
            for i in 0..snapshot.num_asks.min(3) as usize {
                let (price, qty) = snapshot.asks[i];
                print!("{:.1}({:.1}) ", price, qty);
            }
            println!();
        }

        // 市场数据引擎消费快照事件
        market_data_engine.consume_depth_snapshot(&snapshot);

        // 获取更新后的BBO快照
        let bbo = market_data_engine.get_bbo_snapshot();
        if bbo.best_bid_price > 0.0 && bbo.best_ask_price > 0.0 {
            println!("  BBO: 买{:.1}({:.1}) - 卖{:.1}({:.1}), 价差: {:.2}",
                bbo.best_bid_price, bbo.best_bid_qty,
                bbo.best_ask_price, bbo.best_ask_qty,
                bbo.spread());
        }

        println!();
    }

    println!("步骤 3: 性能统计...\n");

    // 6. 显示最终统计
    let stats = engine.stats();
    println!("引擎统计:");
    println!("  总订单数: {}", stats.total_orders);
    println!("  买档位数: {}", stats.buy_book_levels);
    println!("  卖档位数: {}", stats.sell_book_levels);
    println!("  深度快照数: {}", snapshot_count);

    let l2 = market_data_engine.get_level2_snapshot();
    println!("\nDepth-20 快照:");
    println!("  买档数: {}, 卖档数: {}", l2.num_bids, l2.num_asks);

    println!("\n=== 集成示例完成 ===");
}
