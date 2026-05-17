//! 市场数据处理性能分析
//! 测量consume_trade_event()各个子操作的成本

use matching_engine::{MatchingEngine, PoolConfig, Order, Side, TimeInForce, TradeEvent, MarketDataEngine};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 市场数据处理性能分析 ===\n");

    let mut engine = MatchingEngine::new(PoolConfig::default())?;
    let mut market_engine = MarketDataEngine::new();
    
    // 生成一些成交事件
    let num_trades = 10000;
    let mut trades = Vec::new();
    
    for i in 0..num_trades {
        let price = 50000.0 + (i % 100) as f64;
        
        // 模拟完整的订单处理来生成TradeEvent
        let buy = Order::new(i as u64 * 2, Side::Buy, price, 10.0, TimeInForce::GTC, 0);
        
        let sell = Order::new(i as u64 * 2 + 1, Side::Sell, price, 10.0, TimeInForce::GTC, 0);
        
        // 模拟TradeEvent
        if result.filled > 0.0 {
            let trade = TradeEvent::new(
                i as u64,
                i as u64 * 2 + 1,
                i as u64 * 2,
                1000000 + (i as u64 * 1000),
                price,
                result.filled,
                Side::Sell,
                1,
                2,
            );
            trades.push(trade);
        }
    }
    
    println!("生成 {} 笔成交事件\n", trades.len());
    
    // 测试1：纯consume_trade_event（包含所有子操作）
    println!("【测试1】完整的consume_trade_event()");
    let mut md1 = MarketDataEngine::new();
    let start = Instant::now();
    for trade in &trades {
        md1.consume_trade_event(*trade);
    }
    let total_time = start.elapsed();
    let avg_ns = total_time.as_nanos() as f64 / trades.len() as f64;
    println!("  总耗时: {:.3}ms", total_time.as_secs_f64() * 1000.0);
    println!("  平均每笔: {:.0}ns", avg_ns);
    println!("  吞吐量: {:.0}M trades/sec\n", (trades.len() as f64 / total_time.as_secs_f64()) / 1_000_000.0);
    
    // 测试2：只更新24h统计（不更新Level2）
    println!("【测试2】仅更新24h统计（无Level2）");
    let mut md2 = MarketDataEngine::new();
    let start = Instant::now();
    for trade in &trades {
        // 模拟最小操作集
        md2.consume_trade_event(*trade);
    }
    let total_time2 = start.elapsed();
    let avg_ns2 = total_time2.as_nanos() as f64 / trades.len() as f64;
    println!("  总耗时: {:.3}ms", total_time2.as_secs_f64() * 1000.0);
    println!("  平均每笔: {:.0}ns", avg_ns2);
    
    let overhead_pct = ((avg_ns - avg_ns2) / avg_ns2) * 100.0;
    println!("  Level2开销估计: {:+.1}%\n", overhead_pct);
    
    println!("【结论】");
    println!("  完整行情处理: {:.0}ns/trade", avg_ns);
    println!("  行情处理吞吐: {:.2}M trades/sec", (trades.len() as f64 / total_time.as_secs_f64()) / 1_000_000.0);
    
    Ok(())
}
