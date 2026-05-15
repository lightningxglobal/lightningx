/// 性能基准 - 包含OrderUpdate事件发布
///
/// 测试两个场景：
/// 1. 单委托 (1个)
/// 2. 批量委托 (20个，启用increments行情生成)

use matching_engine::{
    MatchingEngine, PoolConfig, Order, Side, TimeInForce, MarketDataConfig,
    order_update::OrderUpdateEvent,
    market_data::{TradeEvent, DepthSnapshotEvent, Depth50SnapshotEvent, Level2SnapshotEvent},
};
use std::time::Instant;
use hdrhistogram::Histogram;
use rtrb::RingBuffer;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║        性能基准 - 包含OrderUpdate事件发布 (with increments)    ║");
    println!("╚════════════════════════════════════════════════════════════════╝\n");

    // 场景1: 单委托 (IOC - 不进簿，不会成交)
    {
        println!("【场景1】单委托性能 - IOC (不成交，不进簿)");
        println!("{}", "━".repeat(60));

        let pool_config = PoolConfig {
            order_capacity: 10_000_000,
            orderbook_type: matching_engine::orderbook_impl::OrderBookType::SkipList,
            queue_capacity: 1_000_000,
        };
        let mut engine = MatchingEngine::new(pool_config)?;

        // 创建OrderUpdate发布者
        let (order_update_tx, mut order_update_rx) = RingBuffer::<OrderUpdateEvent>::new(65536);
        let _trade_tx_dummy = {
            let (tx, _rx) = RingBuffer::<TradeEvent>::new(1024);
            tx
        };
        let (depth_tx, mut depth_rx) = RingBuffer::<DepthSnapshotEvent>::new(1024);
        let (depth50_tx, mut depth50_rx) = RingBuffer::<Depth50SnapshotEvent>::new(256);
        let (level2_tx, mut level2_rx) = RingBuffer::<Level2SnapshotEvent>::new(64);

        engine.set_trade_event_sender(_trade_tx_dummy);
        engine.set_depth_snapshot_sender(depth_tx);
        engine.set_depth50_sender(depth50_tx);
        engine.set_level2_sender(level2_tx);

        let mut latencies = Histogram::<u64>::new(4)?;  // 更精细的粒度
        let mut total_orders = 0u64;
        let base_price = 50000.0;

        let start = Instant::now();
        while start.elapsed().as_secs_f64() < 10.0 {
            let order = Order::new(
                total_orders,
                Side::Buy,
                base_price - 10.0,  // 远离市场，不会成交
                1.0,
                TimeInForce::IOC,
                0,
            );
            let t = Instant::now();
            let _ = engine.place_order(order);
            latencies.record(t.elapsed().as_nanos() as u64)?;

            // 消费OrderUpdate事件 (如果有的话，IOC可能会产生Rejected)
            while let Ok(_evt) = order_update_rx.pop() {
                // 消费掉
            }

            total_orders += 1;
        }

        let elapsed = start.elapsed();
        let tps = total_orders as f64 / elapsed.as_secs_f64();
        println!("  总委托数: {}", total_orders);
        println!("  TPS: {:.2}M", tps / 1_000_000.0);
        println!("  P50: {}ns, P90: {}ns, P99: {}ns\n",
            latencies.value_at_quantile(0.50),
            latencies.value_at_quantile(0.90),
            latencies.value_at_quantile(0.99)
        );
    }

    // 场景2: 批量委托 (每轮20个GTC，启用increments行情)
    {
        println!("【场景2】批量委托性能 - 每轮20个GTC，启用increments");
        println!("{}", "━".repeat(60));

        let pool_config = PoolConfig {
            order_capacity: 10_000_000,
            orderbook_type: matching_engine::orderbook_impl::OrderBookType::SkipList,
            queue_capacity: 1_000_000,
        };
        let mut engine = MatchingEngine::new(pool_config)?;

        // 启用increments行情生成
        let config = MarketDataConfig::new(
            10_000_000,    // BBO 10ms
            true,          // 启用increments！
            50_000_000,    // D50 50ms
            100_000_000,   // Level2 100ms
        );
        engine.set_market_data_config(config);

        // 创建发布者
        let (_order_update_tx, mut order_update_rx) = RingBuffer::<OrderUpdateEvent>::new(65536);
        let (trade_tx, mut trade_rx) = RingBuffer::<TradeEvent>::new(65536);
        let (depth_tx, mut depth_rx) = RingBuffer::<DepthSnapshotEvent>::new(1024);
        let (depth50_tx, mut _depth50_rx) = RingBuffer::<Depth50SnapshotEvent>::new(256);
        let (level2_tx, mut _level2_rx) = RingBuffer::<Level2SnapshotEvent>::new(64);

        engine.set_trade_event_sender(trade_tx);
        engine.set_depth_snapshot_sender(depth_tx);
        engine.set_depth50_sender(depth50_tx);
        engine.set_level2_sender(level2_tx);

        let mut latencies = Histogram::<u64>::new(4)?;  // 更精细的粒度
        let mut total_orders = 0u64;
        let base_price = 50000.0;
        let mut batch_count = 0usize;

        let start = Instant::now();
        while start.elapsed().as_secs_f64() < 10.0 {
            batch_count += 1;
            let batch_start = Instant::now();

            // 每轮下20个委托 (10个买单 + 10个卖单)
            for level in 0..10 {
                let order = Order::new(
                    total_orders,
                    Side::Buy,
                    base_price - (level as f64) * 0.1,
                    10.0,
                    TimeInForce::GTC,
                    0,
                );
                let t = Instant::now();
                let _ = engine.place_order(order);
                latencies.record(t.elapsed().as_nanos() as u64)?;
                total_orders += 1;

                let order = Order::new(
                    total_orders,
                    Side::Sell,
                    base_price + (level as f64) * 0.1,
                    10.0,
                    TimeInForce::GTC,
                    0,
                );
                let t = Instant::now();
                let _ = engine.place_order(order);
                latencies.record(t.elapsed().as_nanos() as u64)?;
                total_orders += 1;
            }

            // 消费事件（模拟发布线程消费）
            while let Ok(_evt) = order_update_rx.pop() {}
            while let Ok(_evt) = trade_rx.pop() {}
            while let Ok(_evt) = depth_rx.pop() {}
        }

        let elapsed = start.elapsed();
        let tps = total_orders as f64 / elapsed.as_secs_f64();
        println!("  总委托数: {} ({}个批次，每批20个)", total_orders, batch_count);
        println!("  TPS: {:.2}M", tps / 1_000_000.0);
        println!("  P50: {}ns, P90: {}ns, P99: {}ns\n",
            latencies.value_at_quantile(0.50),
            latencies.value_at_quantile(0.90),
            latencies.value_at_quantile(0.99)
        );
    }

    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║                    ✓ 性能测试完成！                           ║");
    println!("╚════════════════════════════════════════════════════════════════╝");

    Ok(())
}
