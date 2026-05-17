//! OrderBook 实现对比 - SkipList vs BTree vs Array
//! 测量三个实现的性能和延迟

use matching_engine::{
    MatchingEngine, PoolConfig, Order, Side, TimeInForce,
    orderbook_impl::OrderBookType,
};
use std::time::Instant;

#[derive(Debug)]
struct BenchmarkMetrics {
    impl_name: String,
    total_tps: f64,
    avg_latency_ns: f64,
    p50_latency_ns: u64,
    p99_latency_ns: u64,
}

impl BenchmarkMetrics {
    fn print_header() {
        println!("{:<12} {:<15} {:<18} {:<15} {:<15}",
            "实现", "TPS", "平均延迟(ns)", "P50延迟(ns)", "P99延迟(ns)");
        println!("{}", "─".repeat(75));
    }

    fn print(&self) {
        println!("{:<12} {:<15.0} {:<18.0} {:<15} {:<15}",
            self.impl_name,
            self.total_tps,
            self.avg_latency_ns,
            self.p50_latency_ns,
            self.p99_latency_ns,
        );
    }
}

fn benchmark_orderbook(book_type: OrderBookType, impl_name: &str, base_config: &PoolConfig) -> Result<BenchmarkMetrics, Box<dyn std::error::Error>> {
    let mut config = base_config.clone();
    config.orderbook_type = book_type;

    let mut engine = MatchingEngine::new(config.clone())?;
    let num_orders = 10000;
    let price_range = 100;

    // 预热 - 建立初始order book
    for i in 0..100 {
        let price = 50000.0 + (i % price_range) as f64;
        let order = Order::new(i as u64, Side::Buy, price, 10.0, TimeInForce::GTC, 0);
    }

    // 重置引擎进行真实测试
    engine = MatchingEngine::new(config)?;

    let start = Instant::now();
    let mut timings = Vec::new();

    for i in 0..num_orders {
        let price = 50000.0 + (i % price_range) as f64;

        // 买单
        let start_op = Instant::now();
        let buy = Order::new(i as u64 * 2, Side::Buy, price, 10.0, TimeInForce::GTC, 0);
        let elapsed = start_op.elapsed();
        timings.push(elapsed.as_nanos() as u64);

        // 卖单
        let start_op = Instant::now();
        let sell = Order::new(i as u64 * 2 + 1, Side::Sell, price, 10.0, TimeInForce::GTC, 0);
        engine.place_order(sell)?;
        let elapsed = start_op.elapsed();
        timings.push(elapsed.as_nanos() as u64);
    }

    let total_duration = start.elapsed();
    let total_ops = (num_orders * 2) as f64;
    let total_tps = total_ops / total_duration.as_secs_f64();
    let avg_latency_ns = total_duration.as_nanos() as f64 / total_ops;

    timings.sort_unstable();
    let p50_idx = timings.len() / 2;
    let p99_idx = (timings.len() * 99) / 100;
    let p50_latency_ns = timings[p50_idx];
    let p99_latency_ns = timings[p99_idx];

    Ok(BenchmarkMetrics {
        impl_name: impl_name.to_string(),
        total_tps,
        avg_latency_ns,
        p50_latency_ns,
        p99_latency_ns,
    })
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== OrderBook 实现对比基准测试 ===\n");
    println!("场景：10000轮，每轮1买1卖，价格范围0-99\n");

    let base_config = PoolConfig::default();

    let results = vec![
        benchmark_orderbook(OrderBookType::SkipList, "SkipList", &base_config)?,
        benchmark_orderbook(OrderBookType::BTree, "BTree", &base_config)?,
        benchmark_orderbook(OrderBookType::Array, "Array", &base_config)?,
    ];

    BenchmarkMetrics::print_header();
    for result in &results {
        result.print();
    }

    println!("\n【性能对比（相对于SkipList）】");

    let skiplist = &results[0];
    println!("SkipList (baseline):");
    println!("  TPS: {:.0}", skiplist.total_tps);
    println!("  Latency: {:.0}ns (avg), {}ns (P50), {}ns (P99)",
        skiplist.avg_latency_ns, skiplist.p50_latency_ns, skiplist.p99_latency_ns);

    for other in &results[1..] {
        let tps_diff = ((other.total_tps - skiplist.total_tps) / skiplist.total_tps) * 100.0;
        let latency_diff = ((other.avg_latency_ns - skiplist.avg_latency_ns) / skiplist.avg_latency_ns) * 100.0;
        let p50_diff = ((other.p50_latency_ns as f64 - skiplist.p50_latency_ns as f64) / skiplist.p50_latency_ns as f64) * 100.0;
        let p99_diff = ((other.p99_latency_ns as f64 - skiplist.p99_latency_ns as f64) / skiplist.p99_latency_ns as f64) * 100.0;

        println!("\n{} vs SkipList:", other.impl_name);
        println!("  TPS: {:+.2}% ({:.0})", tps_diff, other.total_tps);
        println!("  Latency (avg): {:+.2}% ({:.0}ns)", latency_diff, other.avg_latency_ns);
        println!("  Latency (P50): {:+.2}% ({}ns)", p50_diff, other.p50_latency_ns);
        println!("  Latency (P99): {:+.2}% ({}ns)", p99_diff, other.p99_latency_ns);
    }

    println!("\n【目标对标】");
    let best_tps = results.iter().map(|r| r.total_tps).fold(f64::NEG_INFINITY, f64::max);
    println!("  最优实现: {:.0} TPS", best_tps);
    println!("  目标: 7,000,000 TPS");
    println!("  进度: {:.1}%", (best_tps / 7_000_000.0) * 100.0);

    Ok(())
}
