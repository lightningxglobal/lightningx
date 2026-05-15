//! 直接对比: 仅撮合 vs 撮合+市场数据处理
//! 这次用相同的循环结构，看看真正的瓶颈在哪

use matching_engine::{MatchingEngine, PoolConfig, Order, Side, TimeInForce, TradeEvent, MarketDataEngine};
use std::time::Instant;
use std::sync::Arc;
use parking_lot::Mutex;
use rtrb::RingBuffer;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 直接对比: 仅撮合 vs 撮合+市场数据 ===\n");

    let config = PoolConfig::default();
    let num_rounds = 5000;

    // ========== 方式A: 仅撮合 ==========
    println!("【方式A】仅撮合引擎 - 无市场数据");
    let mut engine_a = MatchingEngine::new(config.clone())?;

    let start_a = Instant::now();
    for i in 0..num_rounds {
        let buy_order = Order::new(i as u64 * 2, Side::Buy, 50000.0 + (i % 100) as f64, 10.0, TimeInForce::GTC, 0);
        let _ = engine_a.place_order(buy_order)?;

        let sell_order = Order::new(i as u64 * 2 + 1, Side::Sell, 50000.0 + (i % 100) as f64, 10.0, TimeInForce::GTC, 0);
        let _ = engine_a.place_order(sell_order)?;
    }
    let duration_a = start_a.elapsed();
    let tps_a = (num_rounds as f64 * 2.0) / duration_a.as_secs_f64();

    println!("耗时:  {:.3}ms", duration_a.as_secs_f64() * 1000.0);
    println!("TPS:   {:.0} (10k orders)", tps_a);

    // ========== 方式B: 撮合 + TradeEvent发送 + 市场数据处理 ==========
    println!("\n【方式B】撮合 + TradeEvent + 市场数据处理 (同步)");
    let mut engine_b = MatchingEngine::new(config.clone())?;
    let (trade_tx_b, mut trade_rx_b) = RingBuffer::<TradeEvent>::new(1_000_000);
    engine_b.set_trade_event_sender(trade_tx_b);
    let market_engine = Arc::new(Mutex::new(MarketDataEngine::new()));

    let start_b = Instant::now();
    for i in 0..num_rounds {
        let buy_order = Order::new(i as u64 * 2, Side::Buy, 50000.0 + (i % 100) as f64, 10.0, TimeInForce::GTC, 0);
        let _ = engine_b.place_order(buy_order)?;

        let sell_order = Order::new(i as u64 * 2 + 1, Side::Sell, 50000.0 + (i % 100) as f64, 10.0, TimeInForce::GTC, 0);
        let _ = engine_b.place_order(sell_order)?;
    }

    // 在循环外处理所有事件
    let mut event_count = 0;
    while let Ok(event) = trade_rx_b.pop() {
        let mut md = market_engine.lock();
        md.consume_trade_event(event);
        event_count += 1;
    }

    let duration_b = start_b.elapsed();
    let tps_b = (num_rounds as f64 * 2.0) / duration_b.as_secs_f64();

    println!("耗时:  {:.3}ms (处理了 {} 个事件)", duration_b.as_secs_f64() * 1000.0, event_count);
    println!("TPS:   {:.0} (10k orders)", tps_b);

    // ========== 总结 ==========
    println!("\n【结果分析】");
    println!("{}", "=".repeat(60));
    println!("仅撮合:         {:.0} TPS (基线)", tps_a);
    println!("撮合+市场数据:  {:.0} TPS", tps_b);
    println!("性能下降:       {:.1}%", ((tps_a - tps_b) / tps_a) * 100.0);
    println!();

    if (tps_a - tps_b).abs() < tps_a * 0.1 {
        println!("✓ 市场数据处理的同步成本在 10% 以内");
    } else {
        println!("⚠️  市场数据处理导致显著性能下降！");
        println!();
        println!("可能原因:");
        println!("1. 锁竞争 (Arc<Mutex<MarketDataEngine>>)");
        println!("   - 同步锁会导致serialization");
        println!("   - 考虑使用RwLock或lock-free结构");
        println!();
        println!("2. consume_trade_event() 太慢");
        println!("   - HashMap/BTreeMap查询");
        println!("   - 订单簿更新");
        println!("   - 聚合计算");
        println!();
        println!("3. Channel try_recv() 的开销");
        println!("   - 原子操作");
        println!("   - 缓存行竞争");
    }

    Ok(())
}
