use lightning_exchange::array_orderbook::{ArrayOrderBook, SortOrder as ArraySortOrder};
use lightning_exchange::skiplist::{SkipList, SortOrder as SkipListSortOrder};
use std::time::Instant;

const NUM_INSERTS: usize = 5000;
const NUM_LOOKUPS: usize = 50000;

fn bench_skiplist_inserts() -> f64 {
    let mut book = SkipList::new_with_pool(SkipListSortOrder::Ascending, 10_000);

    let start = Instant::now();
    for i in 0..NUM_INSERTS {
        let price = 50000.0 + (i as f64) * 0.01;
        let _ = book.insert_level(price);
    }
    let elapsed = start.elapsed();

    let ops_per_sec = NUM_INSERTS as f64 / elapsed.as_secs_f64();
    ops_per_sec / 1_000_000.0
}

fn bench_array_inserts() -> f64 {
    let mut book = ArrayOrderBook::new_with_pool(ArraySortOrder::Ascending, 10_000);

    let start = Instant::now();
    for i in 0..NUM_INSERTS {
        let price = 50000.0 + (i as f64) * 0.01;
        let _ = book.insert_level(price);
    }
    let elapsed = start.elapsed();

    let ops_per_sec = NUM_INSERTS as f64 / elapsed.as_secs_f64();
    ops_per_sec / 1_000_000.0
}

fn bench_skiplist_lookups() -> f64 {
    let mut book = SkipList::new_with_pool(SkipListSortOrder::Ascending, 10_000);

    // 先插入 1000 个价格
    for i in 0..1000 {
        let price = 50000.0 + (i as f64) * 0.1;
        let _ = book.insert_level(price);
    }

    let start = Instant::now();
    let mut hits = 0;
    for i in 0..NUM_LOOKUPS {
        let price = 50000.0 + ((i % 1000) as f64) * 0.1;
        if book.get_node_at_price(price).is_some() {
            hits += 1;
        }
    }
    let elapsed = start.elapsed();

    println!("  (hits: {}/{})", hits, NUM_LOOKUPS);
    let ops_per_sec = NUM_LOOKUPS as f64 / elapsed.as_secs_f64();
    ops_per_sec / 1_000_000.0
}

fn bench_array_lookups() -> f64 {
    let mut book = ArrayOrderBook::new_with_pool(ArraySortOrder::Ascending, 10_000);

    // 先插入 1000 个价格
    for i in 0..1000 {
        let price = 50000.0 + (i as f64) * 0.1;
        let _ = book.insert_level(price);
    }

    let start = Instant::now();
    let mut hits = 0;
    for i in 0..NUM_LOOKUPS {
        let price = 50000.0 + ((i % 1000) as f64) * 0.1;
        if book.get_node_at_price(price).is_some() {
            hits += 1;
        }
    }
    let elapsed = start.elapsed();

    println!("  (hits: {}/{})", hits, NUM_LOOKUPS);
    let ops_per_sec = NUM_LOOKUPS as f64 / elapsed.as_secs_f64();
    ops_per_sec / 1_000_000.0
}

fn bench_skiplist_add_orders() -> f64 {
    let mut book = SkipList::new_with_pool(SkipListSortOrder::Ascending, 50_000);

    // 先插入 100 个价格
    for i in 0..100 {
        let price = 50000.0 + (i as f64) * 0.1;
        let _ = book.insert_level(price);
    }

    let start = Instant::now();
    let mut count = 0;
    for i in 0..NUM_LOOKUPS {
        let price = 50000.0 + ((i % 100) as f64) * 0.1;
        if let Ok(_) = book.add_order_at_level(price, i as u64, 1.0) {
            count += 1;
        }
    }
    let elapsed = start.elapsed();

    println!("  (added: {}/{})", count, NUM_LOOKUPS);
    let ops_per_sec = NUM_LOOKUPS as f64 / elapsed.as_secs_f64();
    ops_per_sec / 1_000_000.0
}

fn bench_array_add_orders() -> f64 {
    let mut book = ArrayOrderBook::new_with_pool(ArraySortOrder::Ascending, 50_000);

    // 先插入 100 个价格
    for i in 0..100 {
        let price = 50000.0 + (i as f64) * 0.1;
        let _ = book.insert_level(price);
    }

    let start = Instant::now();
    let mut count = 0;
    for i in 0..NUM_LOOKUPS {
        let price = 50000.0 + ((i % 100) as f64) * 0.1;
        if let Ok(_) = book.add_order_at_level(price, i as u64, 1.0) {
            count += 1;
        }
    }
    let elapsed = start.elapsed();

    println!("  (added: {}/{})", count, NUM_LOOKUPS);
    let ops_per_sec = NUM_LOOKUPS as f64 / elapsed.as_secs_f64();
    ops_per_sec / 1_000_000.0
}

fn main() {
    println!("=== OrderBook 微基准测试 ===\n");

    println!("【操作1】Insert Level ({} 次插入)", NUM_INSERTS);
    println!("  SkipList:");
    let sl_insert = bench_skiplist_inserts();
    println!("    {:.2}M ops/sec\n", sl_insert);

    println!("  ArrayOrderBook:");
    let arr_insert = bench_array_inserts();
    println!("    {:.2}M ops/sec", arr_insert);
    println!("    快 {:.2}x\n", arr_insert / sl_insert);

    println!("【操作2】Get Node at Price ({} 次查询)", NUM_LOOKUPS);
    println!("  SkipList:");
    let sl_lookup = bench_skiplist_lookups();
    println!("    {:.2}M ops/sec\n", sl_lookup);

    println!("  ArrayOrderBook:");
    let arr_lookup = bench_array_lookups();
    println!("    {:.2}M ops/sec", arr_lookup);
    println!("    快 {:.2}x\n", arr_lookup / sl_lookup);

    println!("【操作3】Add Order ({} 次添加)", NUM_LOOKUPS);
    println!("  SkipList:");
    let sl_add = bench_skiplist_add_orders();
    println!("    {:.2}M ops/sec\n", sl_add);

    println!("  ArrayOrderBook:");
    let arr_add = bench_array_add_orders();
    println!("    {:.2}M ops/sec", arr_add);
    println!("    快 {:.2}x\n", arr_add / sl_add);

    println!("=== 总结 ===");
    println!("SkipList 总性能:       {:.2}M ops/sec (avg)", (sl_insert + sl_lookup + sl_add) / 3.0);
    println!("ArrayOrderBook 总性能: {:.2}M ops/sec (avg)", (arr_insert + arr_lookup + arr_add) / 3.0);
    let avg_improvement = ((arr_insert + arr_lookup + arr_add) / (sl_insert + sl_lookup + sl_add));
    println!("平均快 {:.2}x", avg_improvement);
}
