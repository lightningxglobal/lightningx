/// 市场数据准确性验证
/// 验证深度采样、聚合交易、24h统计的准确性
use lightning_exchange::{
    MatchingEngine, PoolConfig, Order, Side, TimeInForce, MarketDataConfig,
    MarketDataEngine, TradeEvent,
};
use rtrb::RingBuffer;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔════════════════════════════════════════════════════════════════════╗");
    println!("║              市场数据准确性验证 - 深度采样和统计                  ║");
    println!("╚════════════════════════════════════════════════════════════════════╝\n");

    // 场景1: 深度采样同步性验证
    {
        println!("【场景1】深度采样事件与订单簿同步性验证");
        println!("{}", "━".repeat(64));

        let mut engine = MatchingEngine::new(PoolConfig::default())?;
        let (depth_tx, mut depth_rx) = RingBuffer::new(10000);
        let (trade_tx, mut trade_rx) = RingBuffer::new(10000);

        engine.set_depth_snapshot_sender(depth_tx);
        engine.set_trade_event_sender(trade_tx);

        // 配置采样：启用所有三层采样
        let config = MarketDataConfig::new(
            10_000_000,    // 10ms BBO采样
            true,          // 启用增量采样
            50_000_000,    // 50ms D50采样
            100_000_000,   // 100ms Level2采样
        );
        engine.set_market_data_config(config);

        // 下单并生成深度快照
        println!("✓ 下单序列...");
        let mut best_bid = 0.0;
        let mut best_ask = 0.0;

        // 买单
        for i in 0..5 {
            let price = 100.0 - (i as f64) * 0.1;
            let buy = Order::new(100 + i, Side::Buy, price, 100.0, TimeInForce::GTC, 0);
            engine.place_order(buy)?;
            if i == 0 {
                best_bid = price;
            }
        }

        // 卖单
        for i in 0..5 {
            let price = 100.0 + (i as f64) * 0.1;
            let sell = Order::new(200 + i, Side::Sell, price, 100.0, TimeInForce::GTC, 0);
            engine.place_order(sell)?;
            if i == 0 {
                best_ask = price;
            }
        }

        println!("✓ BBO: bid={:.1}, ask={:.1}", best_bid, best_ask);

        // 获取快照事件
        let mut snapshot_count = 0;
        let mut bbo_count = 0;
        while let Ok(snapshot) = depth_rx.pop() {
            snapshot_count += 1;
            if snapshot_count == 1 {
                // 验证第一个快照的BBO
                println!("✓ 第一个深度快照:");
                println!("  Bids: {} 档, asks: {} 档", snapshot.num_bids, snapshot.num_asks);
                if snapshot.num_bids > 0 {
                    let (bid_price, bid_qty) = snapshot.bids[0];
                    println!("  最优买价: {} × {}", bid_price, bid_qty);
                    assert_eq!(bid_price, best_bid, "BBO买价应该匹配");
                    assert_eq!(bid_qty, 100.0, "BBO买量应该是100");
                }
                if snapshot.num_asks > 0 {
                    let (ask_price, ask_qty) = snapshot.asks[0];
                    println!("  最优卖价: {} × {}", ask_price, ask_qty);
                    assert_eq!(ask_price, best_ask, "BBO卖价应该匹配");
                    assert_eq!(ask_qty, 100.0, "BBO卖量应该是100");
                }
                bbo_count = 1;
            }
        }
        println!("✓ 深度快照总数: {}", snapshot_count);
        assert!(bbo_count > 0, "应该收到至少一个深度快照");

        println!("✓ 场景1验证通过\n");
    }

    // 场景2: 交易事件准确性验证
    {
        println!("【场景2】交易事件和Trade统计准确性");
        println!("{}", "━".repeat(64));

        let mut engine = MatchingEngine::new(PoolConfig::default())?;
        let (trade_tx, mut trade_rx) = RingBuffer::new(10000);
        engine.set_trade_event_sender(trade_tx);

        // 进行一系列已知的成交
        println!("✓ 执行成交序列...");

        // 成交1: 100股 @ 50.0
        let sell1 = Order::new(1, Side::Sell, 50.0, 100.0, TimeInForce::GTC, 0);
        engine.place_order(sell1)?;
        let buy1 = Order::new(2, Side::Buy, 50.0, 100.0, TimeInForce::GTC, 0);
        engine.place_order(buy1)?;

        // 成交2: 50股 @ 51.0
        let sell2 = Order::new(3, Side::Sell, 51.0, 50.0, TimeInForce::GTC, 0);
        engine.place_order(sell2)?;
        let buy2 = Order::new(4, Side::Buy, 51.0, 50.0, TimeInForce::GTC, 0);
        engine.place_order(buy2)?;

        // 成交3: 200股 @ 49.0
        let sell3 = Order::new(5, Side::Sell, 49.0, 200.0, TimeInForce::GTC, 0);
        engine.place_order(sell3)?;
        let buy3 = Order::new(6, Side::Buy, 49.0, 200.0, TimeInForce::GTC, 0);
        engine.place_order(buy3)?;

        // 统计trade event
        let mut trade_count = 0;
        let mut total_qty = 0.0;
        let mut total_value = 0.0;
        while let Ok(trade) = trade_rx.pop() {
            trade_count += 1;
            total_qty += trade.quantity;
            total_value += trade.price * trade.quantity;
            println!("✓ Trade #{}: price={}, qty={}, value={:.1}",
                trade_count, trade.price, trade.quantity, trade.price * trade.quantity);
        }

        println!("✓ 成交统计:");
        println!("  总成交数: {}", trade_count);
        println!("  总成交量: {}", total_qty);
        println!("  总成交额: {:.1}", total_value);

        assert_eq!(trade_count, 3, "应该有3笔成交");
        assert_eq!(total_qty, 350.0, "总成交量应该是350");
        assert!((total_value - 17350.0).abs() < 1e-6, "总成交额应该约为17350");

        println!("✓ 场景2验证通过\n");
    }

    // 场景3: 订单簿状态查询准确性
    {
        println!("【场景3】订单簿流动性查询准确性");
        println!("{}", "━".repeat(64));

        let mut engine = MatchingEngine::new(PoolConfig::default())?;

        // 创建明确的买卖分离流动性（不会相交）
        // 买单: 99.9, 99.8, 99.7
        let buy1 = Order::new(101, Side::Buy, 99.9, 100.0, TimeInForce::GTC, 0);
        let r1 = engine.place_order(buy1)?;
        println!("✓ 买单1 @ 99.9: status={:?}", r1.status);

        let buy2 = Order::new(102, Side::Buy, 99.8, 100.0, TimeInForce::GTC, 0);
        let r2 = engine.place_order(buy2)?;
        println!("✓ 买单2 @ 99.8: status={:?}", r2.status);

        let buy3 = Order::new(103, Side::Buy, 99.7, 100.0, TimeInForce::GTC, 0);
        let r3 = engine.place_order(buy3)?;
        println!("✓ 买单3 @ 99.7: status={:?}", r3.status);

        // 卖单: 100.1, 100.2, 100.3
        let sell1 = Order::new(201, Side::Sell, 100.1, 100.0, TimeInForce::GTC, 0);
        let s1 = engine.place_order(sell1)?;
        println!("✓ 卖单1 @ 100.1: status={:?}", s1.status);

        let sell2 = Order::new(202, Side::Sell, 100.2, 100.0, TimeInForce::GTC, 0);
        let s2 = engine.place_order(sell2)?;
        println!("✓ 卖单2 @ 100.2: status={:?}", s2.status);

        let sell3 = Order::new(203, Side::Sell, 100.3, 100.0, TimeInForce::GTC, 0);
        let s3 = engine.place_order(sell3)?;
        println!("✓ 卖单3 @ 100.3: status={:?}", s3.status);

        // 检查订单簿统计
        let stats = engine.stats();
        println!("✓ 订单簿统计:");
        println!("  总订单数: {}", stats.total_orders);
        println!("  买档位数: {}", stats.buy_book_levels);
        println!("  卖档位数: {}", stats.sell_book_levels);

        assert_eq!(stats.total_orders, 6, "应该有6个订单在簿中");
        assert_eq!(stats.buy_book_levels, 3, "应该有3个买档位");
        assert_eq!(stats.sell_book_levels, 3, "应该有3个卖档位");

        // 验证查询结果
        assert!(r1.filled == 0.0, "买单1应该进入簿");
        assert!(s1.filled == 0.0, "卖单1应该进入簿");

        println!("✓ 场景3验证通过\n");
    }

    println!("╔════════════════════════════════════════════════════════════════════╗");
    println!("║              ✓ 市场数据准确性验证全部通过！                       ║");
    println!("╚════════════════════════════════════════════════════════════════════╝");

    Ok(())
}
