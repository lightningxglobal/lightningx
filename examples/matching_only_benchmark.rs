//! 纯匹配引擎性能基准测试 (不含市场数据处理)
//! 目的: 测量原始匹配引擎性能，不受市场数据引擎影响

use matching_engine::{MatchingEngine, PoolConfig, Order, Side, TimeInForce};
use std::time::Instant;

struct LatencyStats {
    latencies: Vec<u64>,
}

impl LatencyStats {
    fn new() -> Self {
        Self {
            latencies: Vec::with_capacity(10000),
        }
    }

    fn push(&mut self, ns: u64) {
        self.latencies.push(ns);
    }

    fn print(&self, name: &str) {
        if self.latencies.is_empty() {
            println!("{}: No data", name);
            return;
        }

        let mut sorted = self.latencies.clone();
        sorted.sort();

        let min = sorted[0];
        let max = sorted[sorted.len() - 1];
        let avg = sorted.iter().sum::<u64>() as f64 / sorted.len() as f64;

        let p50_idx = sorted.len() / 2;
        let p95_idx = (sorted.len() * 95) / 100;
        let p99_idx = (sorted.len() * 99) / 100;
        let p999_idx = (sorted.len() * 999) / 1000;
        let p9999_idx = (sorted.len() * 9999) / 10000;

        let p50 = sorted[p50_idx];
        let p95 = sorted[p95_idx.min(sorted.len() - 1)];
        let p99 = sorted[p99_idx.min(sorted.len() - 1)];
        let p999 = sorted[p999_idx.min(sorted.len() - 1)];
        let p9999 = sorted[p9999_idx.min(sorted.len() - 1)];

        println!("{}", name);
        println!("  Min:    {:.0}ns ({:.3}μs)", min, min as f64 / 1000.0);
        println!("  P50:    {:.0}ns ({:.3}μs)", p50, p50 as f64 / 1000.0);
        println!("  P95:    {:.0}ns ({:.3}μs)", p95, p95 as f64 / 1000.0);
        println!("  P99:    {:.0}ns ({:.3}μs)", p99, p99 as f64 / 1000.0);
        println!("  P99.9:  {:.0}ns ({:.3}μs)", p999, p999 as f64 / 1000.0);
        println!("  P99.99: {:.0}ns ({:.3}μs)", p9999, p9999 as f64 / 1000.0);
        println!("  Avg:    {:.0}ns ({:.3}μs)", avg, avg / 1000.0);
        println!("  Max:    {:.0}ns ({:.3}μs)", max, max as f64 / 1000.0);
    }

    fn tps(&self, duration_nanos: u128) -> f64 {
        self.latencies.len() as f64 / (duration_nanos as f64 / 1_000_000_000.0)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 纯匹配引擎性能基准测试 (无TradeEvent发送) ===\n");

    let config = PoolConfig::default();
    let mut engine = MatchingEngine::new(config)?;

    // 注意: 不设置trade_event_sender，这样match_order()不会发送TradeEvent

    let mut placement_latencies = LatencyStats::new();
    let mut matching_latencies = LatencyStats::new();

    let num_rounds = 10000;
    println!("Running {} matching rounds (buy + sell pairs)...\n", num_rounds);

    let start = Instant::now();

    for i in 0..num_rounds {
        // Placement: Buy order
        let buy_order = Order::new(
            i * 2,
            Side::Buy,
            50000.0 + (i % 100) as f64,
            10.0,
            TimeInForce::GTC,
            0,
        );

        let t0 = Instant::now();
        let _buy_result = let mut affected_makers = SmallVec::new(); engine.place_order(buy_order, &mut affected_makers, &mut smallvec::SmallVec::new())?;
        let placement_ns = t0.elapsed().as_nanos() as u64;
        placement_latencies.push(placement_ns);

        // Matching: Sell order at same price (will match)
        let sell_order = Order::new(
            i * 2 + 1,
            Side::Sell,
            50000.0 + (i % 100) as f64,
            10.0,
            TimeInForce::GTC,
            0,
        );

        let t1 = Instant::now();
        let _sell_result = let mut affected_makers = SmallVec::new(); engine.place_order(sell_order, &mut affected_makers, &mut smallvec::SmallVec::new())?;
        let matching_ns = t1.elapsed().as_nanos() as u64;
        matching_latencies.push(matching_ns);

        if (i + 1) % 2000 == 0 {
            println!("  进度: {}/{}", i + 1, num_rounds);
        }
    }

    let total_duration = start.elapsed();
    let total_nanos = total_duration.as_nanos();

    println!("\n=== 性能结果 ===\n");

    println!("【订单放置延迟分布】");
    placement_latencies.print("Placement Latency");
    let placement_tps = placement_latencies.tps(total_nanos) as u64;
    println!("  TPS: {:.0} (万笔/秒: {:.2})\n",
        placement_tps, placement_tps as f64 / 10000.0);

    println!("【成交匹配延迟分布】");
    matching_latencies.print("Matching Latency");
    let matching_tps = matching_latencies.tps(total_nanos) as u64;
    println!("  TPS: {:.0} (万笔/秒: {:.2})\n",
        matching_tps, matching_tps as f64 / 10000.0);

    println!("【吞吐量对比】");
    println!("总耗时:     {:.3}ms", total_duration.as_secs_f64() * 1000.0);
    println!("总操作数:   {}", num_rounds * 2);
    println!("平均TPS:    {:.0} (万笔/秒: {:.2})",
        (num_rounds as f64 * 2.0) / total_duration.as_secs_f64(),
        (num_rounds as f64 * 2.0) / total_duration.as_secs_f64() / 10000.0);

    println!("\n【性能对标】");
    println!("匹配性能目标:  > 6,000,000 TPS");
    println!("目前成交TPS:   {}", matching_tps);
    if matching_tps >= 6_000_000 {
        println!("状态:          ✓ 满足目标");
    } else {
        println!("状态:          ⚠ 低于目标 (需要调查TradeEvent发送的开销)");
    }

    Ok(())
}
