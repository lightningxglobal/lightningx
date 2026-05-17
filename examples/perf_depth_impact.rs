use matching_engine::{MatchingEngine, Order, Side, TimeInForce, PoolConfig};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 订单簿深度对性能的影响 ===\n");

    let config = PoolConfig { orderbook_type: matching_engine::orderbook_impl::OrderBookType::SkipList,
        order_capacity: 50_000,
        queue_capacity: 5_000,
    };

    // 不同的预设订单簿深度
    let depths = vec![0, 10, 50, 100, 500, 1000, 5000];

    for depth in depths {
        let mut engine = MatchingEngine::new(config.clone())?;

        // 预置卖单（建立订单簿）
        for i in 0..depth {
            let order = Order::new(
                i as u64,
                Side::Sell,
                50000.0 + (i % 100) as f64 * 0.1,  // 不同价格
                10.0,
                TimeInForce::GTC,
                0,
            );
        }

        // 测量买单匹配性能
        let start = Instant::now();
        let mut matches = 0;
        for i in 0..1000 {
            let order = Order::new(
                (depth + i) as u64,
                Side::Buy,
                55000.0,  // 远高于所有卖单，保证成交
                10.0,
                TimeInForce::GTC,
                0,
            );
                if result.filled > 0.0 {
                    matches += 1;
                }
            }
        }
        let elapsed = start.elapsed();
        let tps = 1000.0 / elapsed.as_secs_f64();

        println!("订单簿深度: {:5} | 1000次匹配: {} | TPS: {:.0} ({:.1}M) | 耗时: {:.2}ms",
            depth, matches, tps, tps / 1_000_000.0, elapsed.as_secs_f64() * 1000.0);
    }

    Ok(())
}
