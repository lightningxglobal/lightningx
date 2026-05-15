/// Micro benchmark - 测试 get_top_levels() 的成本
/// 看Vec分配对性能的影响

use matching_engine::{MatchingEngine, PoolConfig, Order, Side, TimeInForce};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = MatchingEngine::new(PoolConfig::default())?;

    // 初始化订单簿
    for i in 0..1000 {
        let price = 50000.0 - (i as f64) * 0.01;
        let order = Order::new(
            i as u64,
            Side::Buy,
            price,
            10.0,
            TimeInForce::GTC,
            0,
        );
        let _ = engine.place_order(order);
    }

    for i in 0..1000 {
        let price = 50001.0 + (i as f64) * 0.01;
        let order = Order::new(
            (1000 + i) as u64,
            Side::Sell,
            price,
            10.0,
            TimeInForce::GTC,
            0,
        );
        let _ = engine.place_order(order);
    }

    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║     get_top_levels() 性能开销测试 - Vec分配成本             ║");
    println!("╚════════════════════════════════════════════════════════════════╝\n");

    // Warmup
    for _ in 0..1000 {
        let _ = engine.get_top_levels(20, true);
    }

    // Test 1: get_top_levels(20)
    let iterations = 1_000_000;
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = engine.get_top_levels(20, true);
    }
    let elapsed = start.elapsed();
    let ns_per_call = elapsed.as_nanos() as f64 / iterations as f64;
    println!("get_top_levels(20):   {:.1} ns/call ({:.2}M calls/sec)",
             ns_per_call, 1_000.0 / ns_per_call);

    // Test 2: get_top_levels(50)
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = engine.get_top_levels(50, true);
    }
    let elapsed = start.elapsed();
    let ns_per_call = elapsed.as_nanos() as f64 / iterations as f64;
    println!("get_top_levels(50):   {:.1} ns/call ({:.2}M calls/sec)",
             ns_per_call, 1_000.0 / ns_per_call);

    // Test 3: get_top_levels(400)
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = engine.get_top_levels(400, true);
    }
    let elapsed = start.elapsed();
    let ns_per_call = elapsed.as_nanos() as f64 / iterations as f64;
    println!("get_top_levels(400):  {:.1} ns/call ({:.2}M calls/sec)",
             ns_per_call, 1_000.0 / ns_per_call);

    // Test 4: Dual calls (as in build_depth50_event)
    println!("\n模拟 build_depth50_event() 中的调用：");
    let start = Instant::now();
    for _ in 0..iterations {
        let bids = engine.get_top_levels(50, true);
        let asks = engine.get_top_levels(50, false);
        let _ = (bids, asks);
    }
    let elapsed = start.elapsed();
    let ns_per_call = elapsed.as_nanos() as f64 / iterations as f64;
    println!("get_top_levels(50) x2: {:.1} ns/call ({:.2}M calls/sec)",
             ns_per_call, 1_000.0 / ns_per_call);

    // Test 5: Dual calls for Level2
    println!("模拟 build_level2_event() 中的调用：");
    let start = Instant::now();
    for _ in 0..iterations {
        let bids = engine.get_top_levels(400, true);
        let asks = engine.get_top_levels(400, false);
        let _ = (bids, asks);
    }
    let elapsed = start.elapsed();
    let ns_per_call = elapsed.as_nanos() as f64 / iterations as f64;
    println!("get_top_levels(400) x2: {:.1} ns/call ({:.2}M calls/sec)",
             ns_per_call, 1_000.0 / ns_per_call);

    println!("\n【分析】");
    println!("这些数字应该显示Vec分配的真实成本");
    println!("如果cost很小(< 100ns)，难怪总体TPS没有显著下降");
    println!("如果cost很大(> 1000ns)，那should有明显的TPS差异");

    Ok(())
}
