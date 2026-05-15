/// 深度采样性能基准测试
/// 对比4种场景：
/// 1. 禁用increment + 1笔委托
/// 2. 禁用increment + 20笔委托（批量）
/// 3. 启用increment + 1笔委托
/// 4. 启用increment + 20笔委托（批量）

use matching_engine::{
    MatchingEngine, PoolConfig, Order, Side, TimeInForce,
    MarketDataConfig,
};
use smallvec::SmallVec;
use std::time::Instant;

fn benchmark_scenario(
    name: &str,
    enable_increments: bool,
    use_batch: bool,
    num_iterations: usize,
) {
    let mut engine = MatchingEngine::new(PoolConfig::default())
        .expect("创建引擎失败");

    // 配置
    let config = MarketDataConfig::new(
        10_000_000,   // 10ms - BBO + Depth20
        enable_increments,
        50_000_000,   // 50ms
        100_000_000,  // 100ms
    );
    engine.set_market_data_config(config);

    // 初始化订单簿
    for i in 0..10 {
        let buy_order = Order::new(
            i as u64,
            Side::Buy,
            50000.0 - i as f64 * 0.5,
            100.0,
            TimeInForce::GTC,
            0,
        );
        let _ = engine.place_order(buy_order);
    }

    for i in 0..10 {
        let sell_order = Order::new(
            (1000 + i) as u64,
            Side::Sell,
            50001.0 + i as f64 * 0.5,
            100.0,
            TimeInForce::GTC,
            0,
        );
        let _ = engine.place_order(sell_order);
    }

    // 性能测试
    let start = Instant::now();

    if use_batch {
        // 批量下单（20笔）
        for iteration in 0..num_iterations {
            let mut orders = SmallVec::<[Order; 20]>::new();
            for j in 0..20 {
                let order_id = (10000 + iteration * 20 + j) as u64;
                let order = Order::new(
                    order_id,
                    if j % 2 == 0 { Side::Buy } else { Side::Sell },
                    50000.0 + (j as f64 - 10.0) * 0.1,
                    10.0,
                    TimeInForce::IOC,
                    0,
                );
                orders.push(order);
            }

            let _ = engine.place_orders(orders);
        }
    } else {
        // 单笔下单
        for iteration in 0..num_iterations {
            for j in 0..20 {
                let order_id = (10000 + iteration * 20 + j) as u64;
                let order = Order::new(
                    order_id,
                    if j % 2 == 0 { Side::Buy } else { Side::Sell },
                    50000.0 + (j as f64 - 10.0) * 0.1,
                    10.0,
                    TimeInForce::IOC,
                    0,
                );

                let _ = engine.place_order(order);
            }
        }
    }

    let elapsed = start.elapsed();
    let total_orders = if use_batch {
        num_iterations * 20
    } else {
        num_iterations * 20
    };

    let tps = (total_orders as f64 / elapsed.as_secs_f64()).round() as u64;
    let latency_us = (elapsed.as_micros() as f64 / total_orders as f64).round() as u64;

    println!("{:<50} | TPS: {:>8} | 平均延迟: {:>6} μs | 总耗时: {:.3}s",
        name, tps, latency_us, elapsed.as_secs_f64());
}

fn main() {
    println!("=== 深度采样性能基准测试 ===\n");

    // 预热
    println!("预热中...");
    let mut engine = MatchingEngine::new(PoolConfig::default()).expect("创建引擎失败");
    for i in 0..100 {
        let order = Order::new(i as u64, Side::Buy, 50000.0, 10.0, TimeInForce::GTC, 0);
        let _ = engine.place_order(order);
    }
    println!("预热完成\n");

    // 基准测试（每个场景运行足够的迭代获得稳定结果）
    let num_iterations = 100_000;

    println!("配置                                              |      TPS      | 平均延迟 |  总耗时");
    println!("{}", "─".repeat(95));

    // 场景1: 禁用increment + 1笔委托（实际上是20笔但单个下单）
    benchmark_scenario(
        "禁用Increment + 单笔下单(20笔迭代)",
        false,
        false,
        num_iterations,
    );

    // 场景2: 禁用increment + 20笔委托（批量）
    benchmark_scenario(
        "禁用Increment + 批量下单(20笔/批)",
        false,
        true,
        num_iterations,
    );

    // 场景3: 启用increment + 1笔委托（实际上是20笔但单个下单）
    benchmark_scenario(
        "启用Increment + 单笔下单(20笔迭代)",
        true,
        false,
        num_iterations,
    );

    // 场景4: 启用increment + 20笔委托（批量）
    benchmark_scenario(
        "启用Increment + 批量下单(20笔/批)",
        true,
        true,
        num_iterations,
    );

    println!("\n=== 性能测试完成 ===");
    println!("\n说明：");
    println!("- TPS: 每秒交易笔数（越高越好）");
    println!("- 平均延迟: 单笔订单平均处理时间（越低越好）");
    println!("- 启用Increment会增加深度采样和增量计算的开销");
}
