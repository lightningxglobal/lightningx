//! 性能诊断工具 - 逐个测试各个组件的开销

use matching_engine::{MatchingEngine, PoolConfig, Order, Side, TimeInForce};
use std::time::Instant;
use std::collections::HashMap;

fn benchmark<F>(name: &str, _iterations: usize, f: F) -> f64
where
    F: FnOnce() -> usize,
{
    let start = Instant::now();
    let count = f();
    let elapsed = start.elapsed().as_nanos() as f64;
    let ns_per_op = elapsed / count as f64;
    let tps = count as f64 / (elapsed / 1_000_000_000.0);

    println!("{}: {:.0} ops/s, {:.1}ns/op ({} in {:.3}ms)",
        name, tps, ns_per_op, count, elapsed / 1_000_000.0);
    tps
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 性能诊断工具 ===\n");

    // 测试1: HashMap lookup开销
    println!("【测试1】HashMap lookup/insert 开销");
    println!("{}", "=".repeat(50));

    let mut map = HashMap::with_capacity(100_000);
    for i in 0..100_000 {
        map.insert(i, Order::new(i, Side::Buy, 50000.0, 10.0, TimeInForce::GTC, 0));
    }

    benchmark("HashMap get (100k keys)", 100_000, || {
        let mut count = 0;
        for i in 0..100_000 {
            if let Some(_) = map.get(&i) {
                count += 1;
            }
        }
        count
    });

    benchmark("HashMap get_mut (100k keys)", 100_000, || {
        let mut count = 0;
        for i in 0..100_000 {
            if let Some(_) = map.get_mut(&i) {
                count += 1;
            }
        }
        count
    });

    // 测试2: Order 结构克隆开销
    println!("\n【测试2】Order 结构操作开销");
    println!("{}", "=".repeat(50));

    let order = Order::new(1, Side::Buy, 50000.0, 10.0, TimeInForce::GTC, 0);

    benchmark("Order clone", 1_000_000, || {
        let mut count = 0;
        for _ in 0..1_000_000 {
            let _o = order.clone();
            count += 1;
        }
        count
    });

    benchmark("Order copy", 1_000_000, || {
        let mut count = 0;
        for _ in 0..1_000_000 {
            let _o = order;
            count += 1;
        }
        count
    });

    // 测试3: Vec操作开销
    println!("\n【测试3】Vec 操作开销");
    println!("{}", "=".repeat(50));

    benchmark("Vec push/clone", 10_000, || {
        let mut count = 0;
        for _ in 0..10_000 {
            let mut v = Vec::new();
            for i in 0..100 {
                let trade = (i, 50000.0, 10.0);
                v.push(trade);
            }
            count += v.len();
        }
        count
    });

    // 测试4: 匹配引擎的纯订单簿操作
    println!("\n【测试4】匹配引擎 - 纯放置 (无成交)");
    println!("{}", "=".repeat(50));

    let mut engine = MatchingEngine::new(PoolConfig { orderbook_type: matching_engine::orderbook_impl::OrderBookType::SkipList,
        order_capacity: 500_000,
        queue_capacity: 50_000,
    })? ;

    let tps_placement = benchmark("place_order (no match)", 5_000, || {
        let mut count = 0;
        for i in 0..5_000 {
            let order = Order::new(
                i as u64,
                Side::Buy,
                50000.0 + (i % 100) as f64,
                10.0,
                TimeInForce::GTC,
                0,
            );
            if let Ok(_) = engine.place_order(order) {
                count += 1;
            }
        }
        count
    });

    // 测试5: 匹配引擎的成交操作
    println!("\n【测试5】匹配引擎 - 成交匹配");
    println!("{}", "=".repeat(50));

    let mut engine2 = MatchingEngine::new(PoolConfig { orderbook_type: matching_engine::orderbook_impl::OrderBookType::SkipList,
        order_capacity: 500_000,
        queue_capacity: 50_000,
    })?;

    // 预放5000个卖单
    for i in 0..5_000 {
        let order = Order::new(
            i as u64,
            Side::Sell,
            50000.0,
            10.0,
            TimeInForce::GTC,
            0,
        );
        let _ = engine2.place_order(order);
    }

    let tps_matching = benchmark("place_order (with match)", 5_000, || {
        let mut count = 0;
        for i in 0..5_000 {
            let order = Order::new(
                (5_000 + i) as u64,
                Side::Buy,
                50000.0,
                10.0,
                TimeInForce::GTC,
                0,
            );
            if let Ok(result) = engine2.place_order(order) {
                if result.filled > 0.0 {
                    count += 1;
                }
            }
        }
        count
    });

    // 诊断总结
    println!("\n【诊断总结】");
    println!("{}", "=".repeat(50));
    println!("Placement TPS:    {:.0}", tps_placement);
    println!("Matching TPS:     {:.0}", tps_matching);
    println!("目标 TPS:         6,000,000");
    println!();

    if tps_matching < 3_000_000.0 {
        println!("⚠️  成交性能严重不足");
        println!();
        println!("可能原因:");
        println!("1. HashMap 操作 (orders.get/get_mut/insert)");
        println!("   - 在match_order()中有多次lookup");
        println!("   - 考虑使用DashMap或其他并发数据结构");
        println!();
        println!("2. 订单簿操作复杂度");
        println!("   - best_with_orders() 可能有遍历开销");
        println!("   - get_node_mut() 的实现效率");
        println!("   - remove_order_at_level() 的复杂度");
        println!();
        println!("3. 内存分配/VecDeque操作");
        println!("   - VecDeque 在高频操作中的开销");
        println!("   - 内存碎片化");
        println!();
        println!("4. Trade Vec 构建");
        println!("   - 每次成交都push到Vec");
        println!("   - Vec 重新分配开销");
    } else {
        println!("✓ 性能在可接受范围内");
    }

    Ok(())
}
