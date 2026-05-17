/// 深度采样性能对比（公平对标）
/// 对比：启用vs禁用深度采样，使用与realistic_business_benchmark相同的参数
///
/// 场景: 持续不断的订单流（真实业务场景）
/// - 先放入5个买单(价格 49998-50000) - 这些会停留在簿中
/// - 再放入5个卖单(价格 50000-50004) - 这些会与买单成交
/// - 重复多次，产生实际的成交和插入

use matching_engine::{
    MatchingEngine, PoolConfig, Order, Side, TimeInForce,
    MarketDataConfig,
};
use std::time::Instant;
use hdrhistogram::Histogram;

fn benchmark_with_config(
    name: &str,
    enable_increments: bool,
    num_batches: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = MatchingEngine::new(PoolConfig::default())?;

    // 配置深度采样
    let config = MarketDataConfig::new(
        10_000_000,   // 10ms
        enable_increments,
        50_000_000,   // 50ms
        100_000_000,  // 100ms
    );
    engine.set_market_data_config(config);

    let mut latencies = Histogram::<u64>::new(3)?;

    let base_price = 50000.0;
    let mut total_orders = 0;
    let mut total_filled = 0.0;

    let total_start = Instant::now();

    for round in 0..num_batches {
        // 5个买单
        for i in 0..5 {
            let price = base_price - 2.0 + i as f64; // 49998-50002
            let qty = 10.0;

            let start = Instant::now();
            let order = Order::new(
                round as u64 * 1000 + i as u64,
                Side::Buy,
                price,
                qty,
                TimeInForce::GTC,
                0,
            );
            latencies.record(start.elapsed().as_nanos() as u64)?;

            total_orders += 1;
            total_filled += result.filled;
        }

        // 5个卖单 - 会与买单成交
        for i in 0..5 {
            let price = base_price + i as f64; // 50000-50004
            let qty = 10.0;

            let start = Instant::now();
            let order = Order::new(
                round as u64 * 1000 + 500 + i as u64,
                Side::Sell,
                price,
                qty,
                TimeInForce::GTC,
                0,
            );
            latencies.record(start.elapsed().as_nanos() as u64)?;

            total_orders += 1;
            total_filled += result.filled;
        }
    }

    let total_elapsed = total_start.elapsed();
    let tps = total_orders as f64 / total_elapsed.as_secs_f64();
    let p50_ns = latencies.value_at_quantile(0.5);
    let p99_ns = latencies.value_at_quantile(0.99);

    println!(
        "{:<40} | TPS: {:>7.2}M | P50: {:>5} ns | P99: {:>6} ns",
        name,
        tps / 1_000_000.0,
        p50_ns,
        p99_ns
    );

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 深度采样性能对比（公平对标，真实业务场景）===\n");

    // 预热
    println!("预热中...");
    let mut engine = MatchingEngine::new(PoolConfig::default())?;
    for i in 0..100 {
        let order = Order::new(i as u64, Side::Buy, 50000.0, 10.0, TimeInForce::GTC, 0);
    }
    println!("预热完成\n");

    let num_batches = 5000; // 每组10个委托，共50000个委托

    println!("测试配置: {} 轮 (每轮10个委托，共{}个委托)", num_batches, num_batches * 10);
    println!("基准价: 50000.0");
    println!("设置: 5买 + 5卖 = ~50%成交 + ~50%插入\n");

    println!("{}", "─".repeat(85));

    // 测试1: 禁用Increment
    benchmark_with_config(
        "禁用Increment（深度采样OFF）",
        false,
        num_batches,
    )?;

    // 测试2: 启用Increment
    benchmark_with_config(
        "启用Increment（深度采样ON）",
        true,
        num_batches,
    )?;

    println!("{}", "─".repeat(85));
    println!("\n说明：");
    println!("- 与 realistic_business_benchmark 使用相同的订单参数");
    println!("- 启用Increment后的开销来自：10ms BBO采样 + 50ms Depth50采样 + 100ms Level2采样");
    println!("- 采样本身是zero-allocation的，不会影响热路径");

    Ok(())
}
