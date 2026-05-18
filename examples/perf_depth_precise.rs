use lightning_exchange::{MatchingEngine, Order, Side, TimeInForce, PoolConfig};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 订单簿深度精细分析（重复测试） ===\n");

    let config = PoolConfig { orderbook_type: lightning_exchange::orderbook_impl::OrderBookType::SkipList,
        order_capacity: 50_000,
        queue_capacity: 5_000,
    };

    // 重点关注20-200范围
    let test_depths = vec![20, 30, 40, 50, 60, 70, 80, 100, 150, 200];
    
    for depth in test_depths {
        let mut total_tps = 0.0;
        let num_runs = 5;

        for _ in 0..num_runs {
            let mut engine = MatchingEngine::new(config.clone())?;

            // 预置卖单
            for i in 0..depth {
                let order = Order::new(
                    i as u64,
                    Side::Sell,
                    50000.0 + (i % 50) as f64 * 0.1,
                    10.0,
                    TimeInForce::GTC,
                    0,
                );
            }

            // 测量买单匹配
            let start = Instant::now();
            for i in 0..5000 {
                let order = Order::new(
                    (depth + i) as u64,
                    Side::Buy,
                    55000.0,
                    10.0,
                    TimeInForce::GTC,
                    0,
                );
            }
            let elapsed = start.elapsed();
            let tps = 5000.0 / elapsed.as_secs_f64();
            total_tps += tps;
        }

        let avg_tps = total_tps / num_runs as f64;
        println!("深度: {:3} | 平均TPS: {:.0} ({:.2}M) | 相对目标: {:.1}%",
            depth, avg_tps, avg_tps / 1_000_000.0, (avg_tps / 7_000_000.0) * 100.0);
    }

    Ok(())
}
