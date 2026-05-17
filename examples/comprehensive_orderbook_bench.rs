use matching_engine::{MatchingEngine, Order, Side, TimeInForce, PoolConfig};
use matching_engine::orderbook_impl::OrderBookType;
use std::time::Instant;

struct BenchResult {
    name: String,
    ops: u64,
    elapsed_ms: f64,
    tps: f64,
}

impl BenchResult {
    fn new(name: &str, ops: u64, elapsed_ms: f64) -> Self {
        let tps = (ops as f64 / (elapsed_ms / 1000.0));
        Self {
            name: name.to_string(),
            ops,
            elapsed_ms,
            tps,
        }
    }

    fn print(&self) {
        println!("  {} ops: {}", self.name, self.ops);
        println!("  耗时: {:.4}ms", self.elapsed_ms);
        println!("  TPS: {:.0} ({:.2}M)", self.tps, self.tps / 1_000_000.0);
    }

    fn ratio(&self, other: &BenchResult) -> f64 {
        other.tps / self.tps
    }
}

/// 测试1: 插入N个价格档位
fn bench_insert(book_type: OrderBookType, num_inserts: usize) -> BenchResult {
    let config = PoolConfig {
        order_capacity: num_inserts,
        queue_capacity: 1000,
        orderbook_type: book_type,
    };

    let mut engine = MatchingEngine::new(config).unwrap();

    let start = Instant::now();
    for i in 0..num_inserts {
        let order = Order::new(
            i as u64,
            Side::Sell,
            50000.0 + (i as f64) * 0.01,
            10.0,
            TimeInForce::GTC,
            0,
        );
    }
    let elapsed = start.elapsed();

    BenchResult::new("Insert", num_inserts as u64, elapsed.as_secs_f64() * 1000.0)
}

/// 测试2: 查询N个价格（随机）
fn bench_lookup(book_type: OrderBookType, num_lookups: usize) -> BenchResult {
    let config = PoolConfig {
        order_capacity: 50_000,
        queue_capacity: 1000,
        orderbook_type: book_type,
    };

    let mut engine = MatchingEngine::new(config).unwrap();

    // 先插入1000个价格
    for i in 0..1000 {
        let order = Order::new(
            i as u64,
            Side::Sell,
            50000.0 + (i as f64) * 0.1,
            10.0,
            TimeInForce::GTC,
            0,
        );
    }

    // 查询不会改变状态，所以这个测试主要测试引擎内部的快照生成
    // 因为引擎没有直接的查询接口，我们用快照生成来代替
    let start = Instant::now();
    let mut _snapshots = 0;
    for _i in 0..num_lookups {
        let _snapshot = engine.generate_depth_snapshot();
        _snapshots += 1;
    }
    let elapsed = start.elapsed();

    BenchResult::new("Lookup/Snapshot", num_lookups as u64, elapsed.as_secs_f64() * 1000.0)
}

/// 测试3: 完整撮合（预设+匹配）
fn bench_matching(book_type: OrderBookType, num_orders: usize) -> BenchResult {
    let config = PoolConfig {
        order_capacity: num_orders * 2,
        queue_capacity: num_orders,
        orderbook_type: book_type,
    };

    let mut engine = MatchingEngine::new(config).unwrap();

    // 预设卖单
    let half = num_orders / 2;
    for i in 0..half {
        let order = Order::new(
            i as u64,
            Side::Sell,
            50000.0 + (i % 1000) as f64 * 0.1,
            10.0,
            TimeInForce::GTC,
            0,
        );
    }

    // 计时：匹配买单
    let start = Instant::now();
    let mut match_count = 0;
    for i in half..num_orders {
        let order = Order::new(
            i as u64,
            Side::Buy,
            55000.0,
            10.0,
            TimeInForce::GTC,
            0,
        );
            if result.filled > 0.0 {
                match_count += 1;
            }
        }
    }
    let elapsed = start.elapsed();

    BenchResult::new("Matching", match_count as u64, elapsed.as_secs_f64() * 1000.0)
}

/// 测试4: 取消订单
fn bench_cancel(book_type: OrderBookType, num_orders: usize) -> BenchResult {
    let config = PoolConfig {
        order_capacity: num_orders,
        queue_capacity: 1000,
        orderbook_type: book_type,
    };

    let mut engine = MatchingEngine::new(config).unwrap();

    // 先下单
    let mut order_ids = Vec::new();
    for i in 0..num_orders {
        let order = Order::new(
            i as u64,
            Side::Buy,
            50000.0 - (i % 1000) as f64 * 0.1,
            10.0,
            TimeInForce::GTC,
            0,
        );
            Ok(result) => {
                order_ids.push(result.order_id);
            }
            Err(_) => break,
        }
    }

    // 计时：取消订单
    let start = Instant::now();
    let mut cancel_count = 0;
    for order_id in order_ids {
        match engine.cancel_order(order_id) {
            Ok(_) => cancel_count += 1,
            Err(_) => {}
        }
    }
    let elapsed = start.elapsed();

    BenchResult::new("Cancel", cancel_count as u64, elapsed.as_secs_f64() * 1000.0)
}

fn main() {
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ OrderBook 性能对比基准测试 (SkipList vs Array vs BTree)   ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let tests = vec![
        ("Insert 5000 prices", 5000),
        ("Lookup 1000 snapshots", 1000),
        ("Matching 5000 orders", 5000),
        ("Cancel 1000 orders", 1000),
    ];

    for (test_name, param) in tests {
        println!("\n┌─ {} ─────────────────────────────────┐", test_name);

        let result_skiplist = match test_name {
            "Insert 5000 prices" => bench_insert(OrderBookType::SkipList, param),
            "Lookup 1000 snapshots" => bench_lookup(OrderBookType::SkipList, param),
            "Matching 5000 orders" => bench_matching(OrderBookType::SkipList, param),
            "Cancel 1000 orders" => bench_cancel(OrderBookType::SkipList, param),
            _ => unreachable!(),
        };

        let result_array = match test_name {
            "Insert 5000 prices" => bench_insert(OrderBookType::Array, param),
            "Lookup 1000 snapshots" => bench_lookup(OrderBookType::Array, param),
            "Matching 5000 orders" => bench_matching(OrderBookType::Array, param),
            "Cancel 1000 orders" => bench_cancel(OrderBookType::Array, param),
            _ => unreachable!(),
        };

        let result_btree = match test_name {
            "Insert 5000 prices" => bench_insert(OrderBookType::BTree, param),
            "Lookup 1000 snapshots" => bench_lookup(OrderBookType::BTree, param),
            "Matching 5000 orders" => bench_matching(OrderBookType::BTree, param),
            "Cancel 1000 orders" => bench_cancel(OrderBookType::BTree, param),
            _ => unreachable!(),
        };

        println!("\n【SkipList】");
        result_skiplist.print();

        println!("\n【ArrayOrderBook】");
        result_array.print();

        println!("\n【BTreeOrderBook】");
        result_btree.print();

        println!("\n【对比结果】");
        let max_tps = result_skiplist.tps.max(result_array.tps).max(result_btree.tps);
        let winner = if (max_tps - result_skiplist.tps).abs() < 1.0 {
            "SkipList"
        } else if (max_tps - result_array.tps).abs() < 1.0 {
            "Array"
        } else {
            "BTree"
        };
        println!("  赢家: {} ({:.2}M TPS)", winner, max_tps / 1_000_000.0);
        println!("└────────────────────────────────────────────────────┘");
    }

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║                    性能汇总表                              ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    println!("测试场景         SkipList      Array      BTree      赢家");
    println!("───────────────────────────────────────────────────────────");

    let benchmarks = vec![
        ("Insert", bench_insert(OrderBookType::SkipList, 5000), bench_insert(OrderBookType::Array, 5000), bench_insert(OrderBookType::BTree, 5000)),
        ("Snapshot", bench_lookup(OrderBookType::SkipList, 1000), bench_lookup(OrderBookType::Array, 1000), bench_lookup(OrderBookType::BTree, 1000)),
        ("Matching", bench_matching(OrderBookType::SkipList, 5000), bench_matching(OrderBookType::Array, 5000), bench_matching(OrderBookType::BTree, 5000)),
        ("Cancel", bench_cancel(OrderBookType::SkipList, 1000), bench_cancel(OrderBookType::Array, 1000), bench_cancel(OrderBookType::BTree, 1000)),
    ];

    for (name, sl, arr, bt) in benchmarks {
        let max = sl.tps.max(arr.tps).max(bt.tps);
        let winner = if (max - sl.tps).abs() < 1.0 {
            "SkipList"
        } else if (max - arr.tps).abs() < 1.0 {
            "Array"
        } else {
            "BTree"
        };

        println!(
            "{:<12} {:.2}M     {:.2}M    {:.2}M    {}",
            name,
            sl.tps / 1_000_000.0,
            arr.tps / 1_000_000.0,
            bt.tps / 1_000_000.0,
            winner
        );
    }

    println!("\n目标: 7,000,000 TPS");
    println!("当前最好: TBD\n");
}
