//! 只测成交的情况（不需要插入簿）
use matching_engine::{MatchingEngine, PoolConfig, Order, Side, TimeInForce};
use std::time::Instant;
use hdrhistogram::Histogram;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("测试：只成交，不插入簿\n");

    let num_batches = 5000;
    let mut engine = MatchingEngine::new(PoolConfig::default())?;
    let mut latencies = Histogram::<u64>::new(3)?;
    let base_price = 50000.0;

    // 预先插入对手订单
    for i in 0..1000 {
        let sell_order = Order::new(i, Side::Sell, base_price, 100.0, TimeInForce::GTC, 0);
        let _ = engine.place_order(sell_order)?;
    }

    println!("预插入1000个卖单，开始成交测试\n");

    let total_start = Instant::now();

    // 现在插入的买单都会立即成交
    for round in 0..num_batches {
        for i in 0..10 {
            let price = base_price;  // 与卖单同价格，立即成交
            let qty = 10.0;
            let start = Instant::now();
            let order = Order::new(10000 + round as u64 * 100 + i as u64, Side::Buy, price, qty, TimeInForce::GTC, 0);
            let _ = engine.place_order(order)?;
            latencies.record(start.elapsed().as_nanos() as u64)?;
        }
    }

    let total_elapsed = total_start.elapsed();
    let tps = (num_batches as f64 * 10.0) / total_elapsed.as_secs_f64();

    println!("╔════════════════════════════════════╗");
    println!("║ 仅成交（跳过insert to book）        ║");
    println!("╠════════════════════════════════════╣");
    println!("║ TPS:    {:.2}M", tps / 1_000_000.0);
    println!("║ P50:    {:.0} ns", latencies.value_at_percentile(50.0) as f64);
    println!("║ P99:    {:.0} ns", latencies.value_at_percentile(99.0) as f64);
    println!("╠════════════════════════════════════╣");
    println!("║ vs纯matching(3.85M): {:+.1}%", (tps / 1_000_000.0 - 3.85) / 3.85 * 100.0);
    println!("╚════════════════════════════════════╝\n");

    Ok(())
}
