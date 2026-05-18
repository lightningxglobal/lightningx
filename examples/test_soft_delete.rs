use lightning_exchange::{MatchingEngine, Order, Side, TimeInForce, PoolConfig};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 软删除性能测试 ===\n");

    // 场景1：低撤单率（10%）
    println!("场景1：低撤单率（10%撤单，90%配对）");
    test_soft_delete_scenario(1000, 0.1)?;

    println!("\n场景2：中等撤单率（30%撤单，70%配对）");
    test_soft_delete_scenario(1000, 0.3)?;

    println!("\n场景3：高撤单率（70%撤单，30%配对）");
    test_soft_delete_scenario(1000, 0.7)?;

    println!("\n场景4：极端情况（90%撤单，10%配对）");
    test_soft_delete_scenario(1000, 0.9)?;

    Ok(())
}

fn test_soft_delete_scenario(
    total_orders: usize,
    cancel_rate: f64,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = PoolConfig { orderbook_type: lightning_exchange::orderbook_impl::OrderBookType::SkipList,
        order_capacity: 200_000,
        queue_capacity: 20_000,
    };

    let mut engine = MatchingEngine::new(config)?;

    // 第一步：下单（生成sell订单作为流动性）
    println!("  下单阶段...");
    let start = Instant::now();
    for i in 0..total_orders {
        let order = Order::new(
            i as u64,
            Side::Sell,
            50000.0 + (i % 100) as f64,
            1.0,
            TimeInForce::GTC,
            0,
        );
        let _ = engine.place_order(order);
    }
    let place_time = start.elapsed();
    println!("    放置 {} 个订单: {:.3}ms", total_orders, place_time.as_secs_f64() * 1000.0);

    // 第二步：混合撤单和配对操作
    println!("  混合操作阶段...");
    let cancel_count = (total_orders as f64 * cancel_rate) as usize;
    let match_count = total_orders - cancel_count;

    let start = Instant::now();
    let mut matched = 0;
    let mut cancelled = 0;

    for i in 0..total_orders {
        if i < cancel_count {
            // 撤单：前cancel_count个订单
            if engine.cancel_order(i as u64).is_ok() {
                cancelled += 1;
            }
        } else {
            // 配对：后面的订单进行买单配对
            let order = Order::new(
                (total_orders + i) as u64,
                Side::Buy,
                55000.0,
                1.0,
                TimeInForce::GTC,
                0,
            );
            if let Ok(result) = engine.place_order(order) {
                if result.filled > 0.0 {
                    matched += 1;
                }
            }
        }
    }
    let mixed_time = start.elapsed();

    let tps = total_orders as f64 / mixed_time.as_secs_f64();
    let latency = mixed_time.as_micros() as f64 / total_orders as f64;

    println!("    取消订单数: {}", cancelled);
    println!("    成功配对数: {}", matched);
    println!("    混合操作: {:.3}ms, TPS: {:.0}, 延迟: {:.2}μs",
             mixed_time.as_secs_f64() * 1000.0, tps, latency);

    // 获取内存使用统计
    let stats = engine.stats();
    println!("    当前订单数: {}", stats.total_orders);
    println!("    买盘档位: {}", stats.buy_book_levels);
    println!("    卖盘档位: {}", stats.sell_book_levels);

    Ok(())
}
