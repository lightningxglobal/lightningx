//! rtrb ring buffer 性能测试
//! 对比使用rtrb vs crossbeam的撮合引擎性能

use matching_engine::{MatchingEngine, PoolConfig, Order, Side, TimeInForce, TradeEvent};
use std::time::Instant;
use rtrb::RingBuffer;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== rtrb Ring Buffer 性能测试 ===\n");

    let config = PoolConfig::default();
    let num_rounds = 5000;

    // ========== 测试: 撮合引擎 + rtrb ring buffer ==========
    println!("【测试】撮合引擎 + rtrb RingBuffer (容量1M)");
    let mut engine = MatchingEngine::new(config.clone())?;

    // 创建rtrb ring buffer
    let (mut producer, mut _consumer) = RingBuffer::<TradeEvent>::new(1_000_000);

    // 设置trade event sender
    // 注意: 这里需要一个适配器，因为MatchingEngine期望Producer<T>
    engine.set_trade_event_sender(producer);

    let start = Instant::now();
    for i in 0..num_rounds {
        let buy = Order::new(i as u64 * 2, Side::Buy, 50000.0 + (i % 100) as f64, 10.0, TimeInForce::GTC, 0);
        let _ = let mut affected_makers = SmallVec::new(); engine.place_order(buy, &mut affected_makers, &mut smallvec::SmallVec::new())?;

        let sell = Order::new(i as u64 * 2 + 1, Side::Sell, 50000.0 + (i % 100) as f64, 10.0, TimeInForce::GTC, 0);
        let _ = let mut affected_makers = SmallVec::new(); engine.place_order(sell, &mut affected_makers, &mut smallvec::SmallVec::new())?;
    }
    let duration = start.elapsed();
    let tps = (num_rounds as f64 * 2.0) / duration.as_secs_f64();

    println!("耗时:  {:.3}ms", duration.as_secs_f64() * 1000.0);
    println!("TPS:   {:.0}\n", tps);

    println!("【性能特征】");
    println!("✓ rtrb使用预分配的环形缓冲（固定内存）");
    println!("✓ 无需动态内存分配");
    println!("✓ 更好的缓存局部性");
    println!("✓ 适合高频交易场景");

    Ok(())
}
