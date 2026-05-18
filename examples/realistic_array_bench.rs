use lightning_exchange::array_orderbook::{ArrayOrderBook, SortOrder as ArraySortOrder};
use lightning_exchange::skiplist::{SkipList, SortOrder as SkipListSortOrder};
use std::time::Instant;

const NUM_PRESALE: usize = 1000;   // 预设卖单
const NUM_MATCHES: usize = 10000;  // 匹配买单数

fn bench_skiplist_realistic() -> (f64, f64) {
    let mut book = SkipList::new_with_pool(SkipListSortOrder::Ascending, 50_000);

    // 预设 1000 个卖单价格档位（1000-1999）
    for i in 0..NUM_PRESALE {
        let price = 50000.0 + (i as f64) * 0.01;
        let _ = book.insert_level(price);
        let _ = book.add_order_at_level(price, i as u64, 10.0);
    }

    // 计时：10000 个买单向下匹配（从 1009.99 开始）
    let start = Instant::now();
    let mut matches = 0;
    for i in 0..NUM_MATCHES {
        // 模拟真实：买单价格会递增（市场上扬）
        let buy_price = 50009.99 + (i as f64) * 0.001;

        // 简单的贪心匹配：找 <= buy_price 的最高卖价
        let mut best = None;
        for j in 0..NUM_PRESALE {
            let sell_price = 50000.0 + (j as f64) * 0.01;
            if sell_price <= buy_price {
                if best.is_none() || sell_price > best.unwrap() {
                    best = Some(sell_price);
                }
            }
        }

        if let Some(price) = best {
            if book.get_node_at_price(price).is_some() {
                matches += 1;
            }
        }
    }
    let elapsed = start.elapsed();

    let tps = NUM_MATCHES as f64 / elapsed.as_secs_f64();
    (tps, NUM_MATCHES as f64 / elapsed.as_secs_f64() / 1_000_000.0)
}

fn bench_array_realistic() -> (f64, f64) {
    let mut book = ArrayOrderBook::new_with_pool(ArraySortOrder::Ascending, 50_000);

    // 预设 1000 个卖单价格档位
    for i in 0..NUM_PRESALE {
        let price = 50000.0 + (i as f64) * 0.01;
        let _ = book.insert_level(price);
        let _ = book.add_order_at_level(price, i as u64, 10.0);
    }

    // 计时：10000 个买单向下匹配
    let start = Instant::now();
    let mut matches = 0;
    for i in 0..NUM_MATCHES {
        let buy_price = 50009.99 + (i as f64) * 0.001;

        // 简单的贪心匹配：找 <= buy_price 的最高卖价
        let mut best = None;
        for j in 0..NUM_PRESALE {
            let sell_price = 50000.0 + (j as f64) * 0.01;
            if sell_price <= buy_price {
                if best.is_none() || sell_price > best.unwrap() {
                    best = Some(sell_price);
                }
            }
        }

        if let Some(price) = best {
            if book.get_node_at_price(price).is_some() {
                matches += 1;
            }
        }
    }
    let elapsed = start.elapsed();

    let tps = NUM_MATCHES as f64 / elapsed.as_secs_f64();
    (tps, NUM_MATCHES as f64 / elapsed.as_secs_f64() / 1_000_000.0)
}

fn main() {
    println!("=== 真实场景基准测试 ===");
    println!("预设 {} 个卖单，匹配 {} 个买单\n", NUM_PRESALE, NUM_MATCHES);

    println!("【SkipList】");
    let start = Instant::now();
    let (tps1, m_tps1) = bench_skiplist_realistic();
    let time1 = start.elapsed();
    println!("  耗时: {:.2}ms", time1.as_secs_f64() * 1000.0);
    println!("  TPS: {:.0} ({:.2}M)\n", tps1, m_tps1);

    println!("【ArrayOrderBook】");
    let start = Instant::now();
    let (tps2, m_tps2) = bench_array_realistic();
    let time2 = start.elapsed();
    println!("  耗时: {:.2}ms", time2.as_secs_f64() * 1000.0);
    println!("  TPS: {:.0} ({:.2}M)\n", tps2, m_tps2);

    println!("=== 对比 ===");
    let ratio = tps2 / tps1;
    println!("SkipList:       {:.2}M TPS", m_tps1);
    println!("ArrayOrderBook: {:.2}M TPS", m_tps2);
    println!("ArrayOrderBook 快 {:.2}x", ratio);
}
