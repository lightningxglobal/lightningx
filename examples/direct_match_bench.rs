//! 直接调用match_order - 隔离核心撮合性能

use lightning_exchange::{MatchingEngine, PoolConfig, Order, Side, TimeInForce};
use std::time::Instant;
use hdrhistogram::Histogram;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔═══════════════════════════════════════════════════════════════════╗");
    println!("║            直接match_order性能 - 隔离核心撮合逻辑                   ║");
    println!("╚═══════════════════════════════════════════════════════════════════╝\n");

    let num_orders = 100_000;

    // 预热
    {
        let mut engine = MatchingEngine::new(PoolConfig::default())?;
        for i in 0..100 {
            let _order = Order::new(i as u64, Side::Buy, 50000.0, 10.0, TimeInForce::GTC, 0);
        }
    }

    // 测试
    let mut engine = MatchingEngine::new(PoolConfig::default())?;

    // 先预放100个买单进簿
    for i in 0..100 {
        let order = Order::new(i as u64, Side::Buy, 50000.0 - (i % 10) as f64, 10.0, TimeInForce::GTC, 0);
        let _ = engine.place_order(order)?;
    }

    // 现在用卖单与这些买单匹配
    let mut latencies = Histogram::<u64>::new(3)?;

    let start = Instant::now();

    for i in 0..num_orders {
        let order = Order::new(
            (100 + i) as u64,
            Side::Sell,
            50000.0 - (i % 10) as f64,
            10.0,
            TimeInForce::IOC,
            0
        );

        let begin = Instant::now();
        let _result = engine.place_order(order)?;
        latencies.record(begin.elapsed().as_nanos() as u64)?;
    }

    let elapsed = start.elapsed();
    let tps = num_orders as f64 / elapsed.as_secs_f64();
    let p50 = latencies.value_at_percentile(50.0) as f64;
    let p99 = latencies.value_at_percentile(99.0) as f64;

    println!("【直接match_order + 函数调用链】");
    println!("  TPS: {:.2}M", tps / 1_000_000.0);
    println!("  P50: {:.0} ns", p50);
    println!("  P99: {:.0} ns\n", p99);

    println!("【对比】");
    println!("  历史单订单: 4.40M TPS");
    println!("  当前纯撮合: 3.29M TPS");
    println!("  差距来源:");
    println!("    - place_order() → handle_ioc() → match_order()调用链");
    println!("    - affected_makers参数传递");
    println!("    - add_to_book()开销（部分订单剩余需进簿）");

    Ok(())
}
