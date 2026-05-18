/// 采样时间戳调试 - 看时间是否真的推进

use lightning_exchange::{
    MatchingEngine, PoolConfig, Order, Side, TimeInForce,
    MarketDataConfig,
};
use std::time::Instant;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

// 全局计数器用于追踪时间戳
static TIME_PUSHES: AtomicU64 = AtomicU64::new(0);

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = MatchingEngine::new(PoolConfig::default())?;

    let config = MarketDataConfig::new(
        1_000_000,     // 1ms BBO采样
        false,         // 不启用increments
        5_000_000,
        10_000_000,
    );
    engine.set_market_data_config(config);

    println!("═══════════════════════════════════════════════════════════════");
    println!("      采样时间戳调试 - 观察时间戳推进");
    println!("═══════════════════════════════════════════════════════════════\n");

    // 直接测试current_time_ns()
    println!("【测试：1秒内time推进的次数】\n");

    let start = Instant::now();
    let mut last_logged_ms = 0i32;
    let mut orders_placed = 0;

    while start.elapsed().as_secs_f64() < 1.0 {
        for i in 0..20 {
            let order = Order::new(
                orders_placed,
                if i % 2 == 0 { Side::Buy } else { Side::Sell },
                50000.0 + (i as f64) * 0.1,
                10.0,
                TimeInForce::GTC,
                0,
            );
            orders_placed += 1;
        }

        // 每秒记录一次进度
        let elapsed_ms = start.elapsed().as_millis() as i32;
        if elapsed_ms / 1000 > last_logged_ms {
            last_logged_ms = elapsed_ms / 1000;
            println!("  {}秒: {} 个订单已下单", last_logged_ms, orders_placed);
        }
    }

    println!("\n【结果】");
    println!("1秒内共下单: {} 个订单", orders_placed);
    println!("平均TPS: {:.2}M", orders_placed as f64 / 1_000_000.0);
    println!("平均每订单耗时: {:.1} ns", 1_000_000_000.0 / (orders_placed as f64));

    println!("\n【分析】");
    println!("如果采样频率仅有25次（预期1000次），");
    println!("说明时间戳在这{}个订单中的推进不足，", orders_placed);
    println!("或者采样条件检查有问题。");

    Ok(())
}
