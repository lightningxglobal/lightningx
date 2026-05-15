//! 验证batch matching是否正确处理订单

use matching_engine::{MatchingEngine, PoolConfig, Order, Side, TimeInForce, TradeEvent};
use rtrb::RingBuffer;
use smallvec::SmallVec;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Batch Matching 验证 ===\n");

    // 创建一个小batch测试
    let mut engine = MatchingEngine::new(PoolConfig::default())?;
    let (tx, mut rx) = RingBuffer::<TradeEvent>::new(1_000_000);
    engine.set_trade_event_sender(tx);

    // 创建10个buy + 10个sell订单（20个总计）
    // 都是同一个价格，所以应该产生10个trades
    let mut batch: SmallVec<[Order; 20]> = SmallVec::new();

    for i in 0..10 {
        let buy = Order::new(i as u64 * 2, Side::Buy, 50000.0, 10.0, TimeInForce::GTC, 0);
        batch.push(buy);

        let sell = Order::new(i as u64 * 2 + 1, Side::Sell, 50000.0, 10.0, TimeInForce::GTC, 0);
        batch.push(sell);
    }

    println!("输入: {} 个订单", batch.len());

    // 执行batch matching
    let results = engine.match_orders_batch(batch)?;

    println!("输出: {} 个订单结果", results.len());

    // 计算总的成交数
    let mut total_filled = 0.0;
    let mut total_trades = 0;
    for (filled, trades) in results.iter() {
        total_filled += filled;
        total_trades += trades.len();
    }

    println!("总成交数量: {}", total_filled);
    println!("总Trade数: {}", total_trades);

    // 检查rtrb中的TradeEvents
    let mut trade_events = 0;
    while let Ok(_event) = rx.pop() {
        trade_events += 1;
    }

    println!("rtrb中的TradeEvents: {}\n", trade_events);

    // 验证
    println!("【验证结果】");
    if total_filled == 100.0 && total_trades == 10 && trade_events == 10 {
        println!("✅ 正确：10个buy + 10个sell = 10个trades");
    } else {
        println!("❌ 错误：");
        println!("  期望: 成交100, 10个trades, 10个events");
        println!("  实际: 成交{}, {}个trades, {}个events", total_filled, total_trades, trade_events);
    }

    Ok(())
}
