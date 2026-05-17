/// 撮合引擎功能继承测试
/// 验证：trades、depth snapshots、level change events 的生成和内容正确性

use matching_engine::{
    MatchingEngine, PoolConfig, Order, Side, TimeInForce, MarketDataConfig,
};
use rtrb::RingBuffer;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║           撮合引擎功能继承测试                                  ║");
    println!("╚══════════════════════════════════════════════════════════════════╝\n");

    // 场景1: 基础匹配 - 买卖单成交
    {
        println!("【场景1】基础匹配 - 买卖单成交");
        println!("{}", "━".repeat(60));

        let pool_config = PoolConfig {
            order_capacity: 1_000_000,
            orderbook_type: matching_engine::orderbook_impl::OrderBookType::SkipList,
            queue_capacity: 10_000,
        };
        let mut engine = MatchingEngine::new(pool_config)?;

        // 创建trade event channel
        let (trade_tx, mut trade_rx) = RingBuffer::new(100);
        engine.set_trade_event_sender(trade_tx);

        // 下卖单：100股 @ 100.5
        let sell_order = Order::new(
            1u64,
            Side::Sell,
            100.5,
            100.0,
            TimeInForce::GTC,
            0,
        );
        let result = engine.place_order(sell_order)?;
        println!("✓ 卖单已下: id={}, qty={}", result.order_id, 100.0);

        // 下买单：50股 @ 100.5（部分成交）
        let buy_order = Order::new(
            2u64,
            Side::Buy,
            100.5,
            50.0,
            TimeInForce::GTC,
            0,
        );
        let result = engine.place_order(buy_order)?;
        println!("✓ 买单已下: id={}, qty={}, filled={}", result.order_id, 50.0, result.filled);
        assert_eq!(result.filled, 50.0, "买单应该成交50股");

        // 检查trade event
        let mut trade_count = 0;
        while let Ok(trade_event) = trade_rx.pop() {
            trade_count += 1;
            println!("✓ Trade事件 #{}: taker={}, maker={}, price={}, qty={}",
                trade_count, trade_event.taker_id, trade_event.maker_id, trade_event.price, trade_event.quantity);
            assert_eq!(trade_event.price, 100.5, "成交价应该是100.5");
            assert_eq!(trade_event.quantity, 50.0, "成交量应该是50");
        }
        assert_eq!(trade_count, 1, "应该有1条trade事件");
        println!("✓ Trade事件验证通过\n");
    }

    // 场景2: 深度快照生成
    {
        println!("【场景2】深度快照生成（BBO + Depth20）");
        println!("{}", "━".repeat(60));

        let pool_config = PoolConfig {
            order_capacity: 1_000_000,
            orderbook_type: matching_engine::orderbook_impl::OrderBookType::SkipList,
            queue_capacity: 10_000,
        };
        let mut engine = MatchingEngine::new(pool_config)?;

        // 配置深度采样：10ms采样一次
        let config = MarketDataConfig::new(
            10_000_000,    // 10ms BBO采样
            false,         // 不启用increments
            50_000_000,
            100_000_000,
        );
        engine.set_market_data_config(config);

        // 创建depth snapshot channel
        let (depth_tx, mut depth_rx) = RingBuffer::new(100);
        engine.set_depth_snapshot_sender(depth_tx);

        // 下多个买卖单在不同价位
        let prices_buy = vec![99.5, 99.4, 99.3, 99.2, 99.1];
        let prices_sell = vec![100.6, 100.7, 100.8, 100.9, 101.0];

        for (i, &price) in prices_buy.iter().enumerate() {
            let order = Order::new(
                100 + i as u64,
                Side::Buy,
                price,
                10.0 * (i as f64 + 1.0),
                TimeInForce::GTC,
                0,
            );
        }

        for (i, &price) in prices_sell.iter().enumerate() {
            let order = Order::new(
                200 + i as u64,
                Side::Sell,
                price,
                10.0 * (i as f64 + 1.0),
                TimeInForce::GTC,
                0,
            );
        }

        println!("✓ 已下5个买单和5个卖单");

        // 等待采样事件（最多再下几个单来触发采样）
        for i in 0..20 {
            let order = Order::new(
                300 + i,
                Side::Buy,
                99.5 - (i as f64) * 0.01,
                1.0,
                TimeInForce::IOC,
                0,
            );
        }

        // 检查depth snapshot
        let mut snapshot_count = 0;
        while let Ok(snapshot) = depth_rx.pop() {
            snapshot_count += 1;
            println!("✓ Depth快照 #{}: bids={}, asks={}",
                snapshot_count, snapshot.num_bids, snapshot.num_asks);

            // 验证数据内容
            if snapshot.num_bids > 0 {
                println!("  最优买价: {}, 数量: {}", snapshot.bids[0].0, snapshot.bids[0].1);
                assert!(snapshot.bids[0].0 > 0.0, "买价应该大于0");
            }
            if snapshot.num_asks > 0 {
                println!("  最优卖价: {}, 数量: {}", snapshot.asks[0].0, snapshot.asks[0].1);
                assert!(snapshot.asks[0].0 > 0.0, "卖价应该大于0");
            }

            // 验证买价递减（降序）
            for i in 1..snapshot.num_bids as usize {
                assert!(snapshot.bids[i].0 <= snapshot.bids[i-1].0,
                    "买价应该递减");
            }

            // 验证卖价递增（升序）
            for i in 1..snapshot.num_asks as usize {
                assert!(snapshot.asks[i].0 >= snapshot.asks[i-1].0,
                    "卖价应该递增");
            }
        }
        assert!(snapshot_count > 0, "应该有快照事件生成");
        println!("✓ 快照事件验证通过\n");
    }

    // 场景3: 完整三层采样 + LevelChangeEvent
    {
        println!("【场景3】完整三层采样（BBO + D50 + Level2）");
        println!("{}", "━".repeat(60));

        let pool_config = PoolConfig {
            order_capacity: 1_000_000,
            orderbook_type: matching_engine::orderbook_impl::OrderBookType::SkipList,
            queue_capacity: 10_000,
        };
        let mut engine = MatchingEngine::new(pool_config)?;

        // 配置三层采样
        let config = MarketDataConfig::new(
            10_000_000,    // 10ms BBO
            true,          // 启用increments
            50_000_000,    // 50ms D50
            100_000_000,   // 100ms Level2
        );
        engine.set_market_data_config(config);

        // 创建三个channel
        let (depth_tx, mut depth_rx) = RingBuffer::new(100);
        let (d50_tx, mut d50_rx) = RingBuffer::new(50);
        let (l2_tx, mut l2_rx) = RingBuffer::new(20);

        engine.set_depth_snapshot_sender(depth_tx);
        engine.set_depth50_sender(d50_tx);
        engine.set_level2_sender(l2_tx);

        println!("✓ 已配置三层采样");

        // 下30个买单和30个卖单（覆盖50档和400档）
        for i in 0..30 {
            let buy_price = 100.0 - (i as f64) * 0.1;
            let sell_price = 100.0 + (i as f64) * 0.1;

            let buy_order = Order::new(
                1000 + i,
                Side::Buy,
                buy_price,
                100.0,
                TimeInForce::GTC,
                0,
            );

            let sell_order = Order::new(
                2000 + i,
                Side::Sell,
                sell_price,
                100.0,
                TimeInForce::GTC,
                0,
            );
        }

        println!("✓ 已下30个买单和30个卖单");

        // 触发足够的采样事件（用GTC而不是IOC，避免消耗卖单）
        for i in 0..50 {
            let order = Order::new(
                3000 + i,
                Side::Buy,
                99.5 - (i as f64) * 0.01,
                1.0,
                TimeInForce::GTC,
                0,
            );
        }

        // 检查三层事件
        let mut bbo_count = 0;
        while let Ok(snapshot) = depth_rx.pop() {
            bbo_count += 1;
            assert!(snapshot.num_bids > 0, "BBO快照应该有买单");
            // 卖单可能在采样时已被匹配，但至少应该有买单
        }

        let mut d50_count = 0;
        while let Ok(snapshot) = d50_rx.pop() {
            d50_count += 1;
            println!("✓ D50快照 #{}: bids={}, asks={}", d50_count, snapshot.num_bids, snapshot.num_asks);
        }

        let mut l2_count = 0;
        while let Ok(snapshot) = l2_rx.pop() {
            l2_count += 1;
            println!("✓ Level2快照 #{}: bids={}, asks={}", l2_count, snapshot.num_bids, snapshot.num_asks);
        }

        println!("✓ BBO快照: {} 条", bbo_count);
        println!("✓ D50快照: {} 条", d50_count);
        println!("✓ Level2快照: {} 条", l2_count);
        assert!(bbo_count > 0, "应该有BBO快照");
        assert!(d50_count > 0, "应该有D50快照");
        assert!(l2_count > 0, "应该有Level2快照");
        println!("✓ 三层采样验证通过\n");
    }

    // 场景4: 订单取消
    {
        println!("【场景4】订单取消功能");
        println!("{}", "━".repeat(60));

        let pool_config = PoolConfig {
            order_capacity: 1_000_000,
            orderbook_type: matching_engine::orderbook_impl::OrderBookType::SkipList,
            queue_capacity: 10_000,
        };
        let mut engine = MatchingEngine::new(pool_config)?;

        // 下一个大卖单
        let sell_order = Order::new(
            1u64,
            Side::Sell,
            100.0,
            1000.0,
            TimeInForce::GTC,
            0,
        );
        println!("✓ 卖单已下: qty=1000@100.0, id=1");

        // 下一个买单
        let buy_order = Order::new(
            2u64,
            Side::Buy,
            100.0,
            100.0,
            TimeInForce::GTC,
            0,
        );
        println!("✓ 买单已下: qty=100@100.0, id=2, filled={}", result.filled);
        assert_eq!(result.filled, 100.0, "买单应该完全成交");

        // 取消卖单的剩余部分
        let cancel_result = engine.cancel_order(1)?;
        println!("✓ 卖单已取消: cancelled_qty={}", cancel_result.cancelled_quantity);
        assert_eq!(cancel_result.cancelled_quantity, 900.0, "应该取消900股");

        println!("✓ 取消功能验证通过\n");
    }

    // 场景5: 部分成交 + 剩余
    {
        println!("【场景5】部分成交和剩余委托");
        println!("{}", "━".repeat(60));

        let pool_config = PoolConfig {
            order_capacity: 1_000_000,
            orderbook_type: matching_engine::orderbook_impl::OrderBookType::SkipList,
            queue_capacity: 10_000,
        };
        let mut engine = MatchingEngine::new(pool_config)?;

        // 下卖单：100股
        let sell_order = Order::new(
            1u64,
            Side::Sell,
            100.0,
            100.0,
            TimeInForce::GTC,
            0,
        );

        // 下买单：150股（成交100，剩余50）
        let buy_order = Order::new(
            2u64,
            Side::Buy,
            100.0,
            150.0,
            TimeInForce::GTC,
            0,
        );
        println!("✓ 买单: qty=150, filled={}, status={:?}", result.filled, result.status);
        assert_eq!(result.filled, 100.0, "应该成交100");

        // 再下一个卖单，应该与剩余的买单成交
        let sell_order2 = Order::new(
            3u64,
            Side::Sell,
            100.0,
            50.0,
            TimeInForce::GTC,
            0,
        );
        println!("✓ 卖单2: qty=50, filled={}", result2.filled);
        assert_eq!(result2.filled, 50.0, "应该与剩余买单全部成交");

        println!("✓ 部分成交验证通过\n");
    }

    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║                    ✓ 所有功能测试通过！                          ║");
    println!("╚══════════════════════════════════════════════════════════════════╝");

    Ok(())
}
