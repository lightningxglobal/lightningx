/// 简单性能基准 - 逐个下单 vs 批量下单，对标之前的5M/10M结果

use matching_engine::{
    MatchingEngine, PoolConfig, Order, Side, TimeInForce,
};
use std::time::Instant;
use hdrhistogram::Histogram;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║           简单逐个委托性能 - 对标之前的基准                    ║");
    println!("╚════════════════════════════════════════════════════════════════╝\n");

    // 场景1: 逐个下单，每个委托立即成交（IOC）
    {
        let pool_config = PoolConfig {
            order_capacity: 10_000_000,
            orderbook_type: matching_engine::orderbook_impl::OrderBookType::SkipList,
            queue_capacity: 1_000_000,
        };
        let mut engine = MatchingEngine::new(pool_config)?;

        let mut latencies = Histogram::<u64>::new(3)?;
        let mut total_orders = 0u64;
        let base_price = 50000.0;

        let start = Instant::now();
        while start.elapsed().as_secs_f64() < 10.0 {
            // 下1个IOC买单（不会进簿）
            let order = Order::new(
                total_orders,
                Side::Buy,
                base_price - 10.0,  // 远离市场，不会成交
                1.0,
                TimeInForce::IOC,
                0,
            );
            let t = Instant::now();
            let _ = let mut affected_makers = SmallVec::new(); engine.place_order(order, &mut affected_makers, &mut smallvec::SmallVec::new());
            latencies.record(t.elapsed().as_nanos() as u64)?;
            total_orders += 1;
        }

        let elapsed = start.elapsed();
        let tps = total_orders as f64 / elapsed.as_secs_f64();
        println!("【IOC单委托】（不成交，不进簿）");
        println!("  总委托数: {}", total_orders);
        println!("  TPS: {:.2}M", tps / 1_000_000.0);
        println!("  P50: {}ns, P99: {}ns\n",
            latencies.value_at_quantile(0.50),
            latencies.value_at_quantile(0.99)
        );
    }

    // 场景2: GTC委托堆积（类似现在的基准）
    {
        let pool_config = PoolConfig {
            order_capacity: 10_000_000,
            orderbook_type: matching_engine::orderbook_impl::OrderBookType::SkipList,
            queue_capacity: 1_000_000,
        };
        let mut engine = MatchingEngine::new(pool_config)?;

        let mut latencies = Histogram::<u64>::new(3)?;
        let mut total_orders = 0u64;
        let base_price = 50000.0;

        let start = Instant::now();
        while start.elapsed().as_secs_f64() < 10.0 {
            // 下10个买单和10个卖单（都是GTC，堆积在簿中）
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
                let _ = let mut affected_makers = SmallVec::new(); engine.place_order(order, &mut affected_makers, &mut smallvec::SmallVec::new());
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
                let _ = let mut affected_makers = SmallVec::new(); engine.place_order(order, &mut affected_makers, &mut smallvec::SmallVec::new());
                latencies.record(t.elapsed().as_nanos() as u64)?;
                total_orders += 1;
            }
        }

        let elapsed = start.elapsed();
        let tps = total_orders as f64 / elapsed.as_secs_f64();
        println!("【GTC堆积】（20个GTC每循环，都进簿，订单簿持续增长）");
        println!("  总委托数: {}", total_orders);
        println!("  TPS: {:.2}M", tps / 1_000_000.0);
        println!("  P50: {}ns, P99: {}ns\n",
            latencies.value_at_quantile(0.50),
            latencies.value_at_quantile(0.99)
        );
    }

    // 场景3: 连续匹配（买卖相反，会成交）
    {
        let pool_config = PoolConfig {
            order_capacity: 10_000_000,
            orderbook_type: matching_engine::orderbook_impl::OrderBookType::SkipList,
            queue_capacity: 1_000_000,
        };
        let mut engine = MatchingEngine::new(pool_config)?;

        // 先下一个卖单作为对手
        let order = Order::new(
            0,
            Side::Sell,
            50000.0,
            1000.0,
            TimeInForce::GTC,
            0,
        );
        let _ = let mut affected_makers = SmallVec::new(); engine.place_order(order, &mut affected_makers, &mut smallvec::SmallVec::new());

        let mut latencies = Histogram::<u64>::new(3)?;
        let mut total_orders = 1u64;

        let start = Instant::now();
        while start.elapsed().as_secs_f64() < 10.0 {
            // 连续下买单，会与卖单成交
            let order = Order::new(
                total_orders,
                Side::Buy,
                50000.0,
                1.0,
                TimeInForce::GTC,
                0,
            );
            let t = Instant::now();
            let _ = let mut affected_makers = SmallVec::new(); engine.place_order(order, &mut affected_makers, &mut smallvec::SmallVec::new());
            latencies.record(t.elapsed().as_nanos() as u64)?;
            total_orders += 1;
        }

        let elapsed = start.elapsed();
        let tps = total_orders as f64 / elapsed.as_secs_f64();
        println!("【连续匹配】（每个买单都与卖单匹配）");
        println!("  总委托数: {}", total_orders);
        println!("  TPS: {:.2}M", tps / 1_000_000.0);
        println!("  P50: {}ns, P99: {}ns\n",
            latencies.value_at_quantile(0.50),
            latencies.value_at_quantile(0.99)
        );
    }

    Ok(())
}
