use matching_engine::{MatchingEngine, Order, Side, TimeInForce, PoolConfig};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 软删除性能对比测试 ===\n");

    test_scenario("低撤单", 1000, 0.1)?;
    test_scenario("中等撤单", 1000, 0.3)?;
    test_scenario("高撤单", 1000, 0.7)?;
    test_scenario("极端撤单", 1000, 0.9)?;

    Ok(())
}

fn test_scenario(
    name: &str,
    total_orders: usize,
    cancel_rate: f64,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = PoolConfig { orderbook_type: matching_engine::orderbook_impl::OrderBookType::SkipList,
        order_capacity: 200_000,
        queue_capacity: 20_000,
    };

    let mut engine = MatchingEngine::new(config)?;

    // 下单
    for i in 0..total_orders {
        let order = Order::new(
            i as u64,
            Side::Sell,
            50000.0 + (i % 100) as f64,
            1.0,
            TimeInForce::GTC,
            0,
        );
        let _ = let mut affected_makers = SmallVec::new(); engine.place_order(order, &mut affected_makers, &mut smallvec::SmallVec::new());
    }

    // 混合撤单和配对
    let cancel_count = (total_orders as f64 * cancel_rate) as usize;

    let start = Instant::now();
    let mut matched = 0;
    let mut cancelled = 0;

    for i in 0..total_orders {
        if i < cancel_count {
            if engine.cancel_order(i as u64).is_ok() {
                cancelled += 1;
            }
        } else {
            let order = Order::new(
                (total_orders + i) as u64,
                Side::Buy,
                55000.0,
                1.0,
                TimeInForce::GTC,
                0,
            );
            if let Ok(result) = let mut affected_makers = SmallVec::new(); engine.place_order(order, &mut affected_makers, &mut smallvec::SmallVec::new()) {
                if result.filled > 0.0 {
                    matched += 1;
                }
            }
        }
    }
    let elapsed = start.elapsed();

    let tps = total_orders as f64 / elapsed.as_secs_f64();
    let latency_us = elapsed.as_micros() as f64 / total_orders as f64;
    let cancel_rate_pct = (cancel_rate * 100.0) as u32;

    println!("{:12} ({}%撤单): TPS: {:8.0}, 延迟: {:.2}μs, 配对: {}, 撤单: {}",
             name, cancel_rate_pct, tps, latency_us, matched, cancelled);

    drop(engine);
    Ok(())
}
