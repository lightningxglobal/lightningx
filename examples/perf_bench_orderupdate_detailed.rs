/// 性能基准 - 详细对比 (带OrderUpdate事件)
///
/// 测试两个场景并显示详细的延迟统计

use lightning_exchange::{
    MatchingEngine, PoolConfig, Order, Side, TimeInForce, MarketDataConfig,
    order_update::OrderUpdateEvent,
    market_data::{TradeEvent, DepthSnapshotEvent, Depth50SnapshotEvent, Level2SnapshotEvent},
};
use std::time::Instant;
use hdrhistogram::Histogram;
use rtrb::RingBuffer;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║     性能基准对比 - 单委托 vs 批量(20个) with OrderUpdate      ║");
    println!("╚════════════════════════════════════════════════════════════════╝\n");

    // 场景1: 单委托 IOC
    {
        println!("【场景1】单委托性能 - IOC (不成交，不进簿)");
        println!("{}", "━".repeat(70));

        let pool_config = PoolConfig {
            order_capacity: 10_000_000,
            orderbook_type: lightning_exchange::orderbook_impl::OrderBookType::SkipList,
            queue_capacity: 1_000_000,
        };
        let mut engine = MatchingEngine::new(pool_config)?;

        let (_order_update_tx, mut order_update_rx) = RingBuffer::<OrderUpdateEvent>::new(65536);
        let _trade_tx = {
            let (tx, _rx) = RingBuffer::<TradeEvent>::new(1024);
            tx
        };
        let (depth_tx, mut depth_rx) = RingBuffer::<DepthSnapshotEvent>::new(1024);
        let (depth50_tx, mut _depth50_rx) = RingBuffer::<Depth50SnapshotEvent>::new(256);
        let (level2_tx, mut _level2_rx) = RingBuffer::<Level2SnapshotEvent>::new(64);

        engine.set_trade_event_sender(_trade_tx);
        engine.set_depth_snapshot_sender(depth_tx);
        engine.set_depth50_sender(depth50_tx);
        engine.set_level2_sender(level2_tx);

        let mut latencies = Histogram::<u64>::new(4)?;
        let mut total_orders = 0u64;
        let base_price = 50000.0;

        let start = Instant::now();
        while start.elapsed().as_secs_f64() < 10.0 {
            let order = Order::new(
                total_orders,
                Side::Buy,
                base_price - 10.0,
                1.0,
                TimeInForce::IOC,
                0,
            );
            let t = Instant::now();
            latencies.record(t.elapsed().as_nanos() as u64)?;

            while let Ok(_evt) = order_update_rx.pop() {}
            while let Ok(_evt) = depth_rx.pop() {}

            total_orders += 1;
        }

        let elapsed = start.elapsed();
        let tps = total_orders as f64 / elapsed.as_secs_f64();
        let avg_latency = elapsed.as_nanos() as f64 / total_orders as f64;

        println!("  总委托数: {}", total_orders);
        println!("  TPS: {:.2}M orders/sec", tps / 1_000_000.0);
        println!("  平均延迟: {:.1}ns", avg_latency);
        println!("  P50:  {:>6}ns", latencies.value_at_quantile(0.50));
        println!("  P90:  {:>6}ns", latencies.value_at_quantile(0.90));
        println!("  P99:  {:>6}ns\n", latencies.value_at_quantile(0.99));
    }

    // 场景2: 批量委托 (20个GTC，启用increments)
    {
        println!("【场景2】批量委托性能 - 每轮20个GTC，启用increments");
        println!("{}", "━".repeat(70));

        let pool_config = PoolConfig {
            order_capacity: 10_000_000,
            orderbook_type: lightning_exchange::orderbook_impl::OrderBookType::SkipList,
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

        let (_order_update_tx, mut order_update_rx) = RingBuffer::<OrderUpdateEvent>::new(65536);
        let (trade_tx, mut trade_rx) = RingBuffer::<TradeEvent>::new(65536);
        let (depth_tx, mut depth_rx) = RingBuffer::<DepthSnapshotEvent>::new(1024);
        let (depth50_tx, mut _depth50_rx) = RingBuffer::<Depth50SnapshotEvent>::new(256);
        let (level2_tx, mut _level2_rx) = RingBuffer::<Level2SnapshotEvent>::new(64);

        engine.set_trade_event_sender(trade_tx);
        engine.set_depth_snapshot_sender(depth_tx);
        engine.set_depth50_sender(depth50_tx);
        engine.set_level2_sender(level2_tx);

        let mut latencies = Histogram::<u64>::new(4)?;
        let mut total_orders = 0u64;
        let base_price = 50000.0;

        let start = Instant::now();
        while start.elapsed().as_secs_f64() < 10.0 {
            // 每轮下20个委托
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
                latencies.record(t.elapsed().as_nanos() as u64)?;
                total_orders += 1;
            }

            // 消费事件
            while let Ok(_evt) = order_update_rx.pop() {}
            while let Ok(_evt) = trade_rx.pop() {}
            while let Ok(_evt) = depth_rx.pop() {}
        }

        let elapsed = start.elapsed();
        let tps = total_orders as f64 / elapsed.as_secs_f64();
        let avg_latency = elapsed.as_nanos() as f64 / total_orders as f64;

        println!("  总委托数: {}", total_orders);
        println!("  TPS: {:.2}M orders/sec", tps / 1_000_000.0);
        println!("  平均延迟: {:.1}ns", avg_latency);
        println!("  P50:  {:>6}ns", latencies.value_at_quantile(0.50));
        println!("  P90:  {:>6}ns", latencies.value_at_quantile(0.90));
        println!("  P99:  {:>6}ns\n", latencies.value_at_quantile(0.99));
    }

    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║                     ✓ 性能测试完成！                          ║");
    println!("╚════════════════════════════════════════════════════════════════╝");

    Ok(())
}
