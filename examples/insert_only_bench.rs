use matching_engine::{MatchingEngine, Order, Side, TimeInForce, PoolConfig};
use matching_engine::orderbook_impl::OrderBookType;
use std::time::Instant;

fn main() {
    println!("=== Insert-Only Benchmark (纯插入，无匹配) ===\n");

    for &book_type in &[OrderBookType::SkipList, OrderBookType::BTree] {
        let name = match book_type {
            OrderBookType::SkipList => "SkipList",
            OrderBookType::BTree => "BTree",
            OrderBookType::Array => "Array",
        };

        let config = PoolConfig {
            order_capacity: 50_000,
            queue_capacity: 5_000,
            orderbook_type: book_type,
        };

        let mut engine = MatchingEngine::new(config).unwrap();

        let start = Instant::now();
        for i in 0..5000 {
            let order = Order::new(
                i as u64,
                Side::Sell,
                50000.0 + (i as f64) * 0.01,
                10.0,
                TimeInForce::GTC,
                0,
            );
        }
        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
        let tps = 5000.0 / (elapsed / 1000.0);

        println!("【{}】", name);
        println!("  插入5000个订单");
        println!("  耗时: {:.4}ms", elapsed);
        println!("  TPS: {:.0} ({:.2}M)\n", tps, tps / 1_000_000.0);
    }
}
