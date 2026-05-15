use matching_engine::{MatchingEngine, Order, Side, TimeInForce, PoolConfig};
use matching_engine::orderbook_impl::OrderBookType;
use std::time::Instant;

fn bench_matching(book_type: OrderBookType, name: &str) -> (f64, f64) {
    let config = PoolConfig {
        order_capacity: 50_000,
        queue_capacity: 5_000,
        orderbook_type: book_type,
    };

    let mut engine = MatchingEngine::new(config).unwrap();

    // 预设 2500 个卖单
    for i in 0..2500 {
        let order = Order::new(
            i,
            Side::Sell,
            50000.0 + (i % 1000) as f64 * 0.1,
            10.0,
            TimeInForce::GTC,
            0,
        );
        let _ = engine.place_order(order);
    }

    // 计时：放入 2500 个买单进行撮合
    let start = Instant::now();
    let mut match_count = 0;
    for i in 2500..5000 {
        let order = Order::new(
            i,
            Side::Buy,
            55000.0,
            10.0,
            TimeInForce::GTC,
            0,
        );
        if let Ok(result) = engine.place_order(order) {
            if result.filled > 0.0 {
                match_count += 1;
            }
        }
    }
    let elapsed = start.elapsed();

    let tps = match_count as f64 / elapsed.as_secs_f64();
    let ms = elapsed.as_secs_f64() * 1000.0;

    println!("【{}】", name);
    println!("  成交笔数: {}", match_count);
    println!("  耗时: {:.4}ms", ms);
    println!("  TPS: {:.0} ({:.2}M)\n", tps, tps / 1_000_000.0);

    (tps, ms)
}

fn main() {
    println!("=== OrderBook 实现对比基准测试 ===\n");
    println!("场景：2500 个卖单 + 2500 个买单撮合\n");

    let (tps_skiplist, ms_skiplist) = bench_matching(OrderBookType::SkipList, "SkipList");
    let (tps_array, ms_array) = bench_matching(OrderBookType::Array, "ArrayOrderBook");

    println!("=== 性能对比 ===");
    println!("SkipList:       {:.2}M TPS ({:.4}ms)", tps_skiplist / 1_000_000.0, ms_skiplist);
    println!("ArrayOrderBook: {:.2}M TPS ({:.4}ms)", tps_array / 1_000_000.0, ms_array);

    let ratio = tps_array / tps_skiplist;
    if ratio > 1.0 {
        println!("ArrayOrderBook 快 {:.2}x", ratio);
    } else {
        println!("SkipList 快 {:.2}x", 1.0 / ratio);
    }

    println!("\n目标：7,000,000 TPS");
}
