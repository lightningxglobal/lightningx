use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use matching_engine::{MatchingEngine, Order, Side, TimeInForce, PoolConfig};
use std::time::Instant;

fn create_test_order(id: u64, side: Side, price: f64) -> Order {
    Order::new(
        id,
        side,
        price,
        1.0,
        TimeInForce::GTC,
        0,
    )
}

fn bench_place_order_only(c: &mut Criterion) {
    c.bench_function("place_order_10k", |b| {
        b.iter_batched(
            || {
                match MatchingEngine::new(PoolConfig::default()) {
                    Ok(engine) => Some(engine),
                    Err(_) => None,
                }
            },
            |engine| {
                if let Some(mut engine) = engine {
                    for i in 0..10_000 {
                        let order = black_box(create_test_order(i, Side::Buy, 50000.0 + i as f64));
                        let _ = engine.place_order(order);
                    }
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

fn bench_matching_only(c: &mut Criterion) {
    c.bench_function("matching_10k", |b| {
        b.iter_batched(
            || {
                match MatchingEngine::new(PoolConfig::default()) {
                    Ok(engine) => Some(engine),
                    Err(_) => None,
                }
            },
            |engine| {
                if let Some(mut engine) = engine {
                    // 预填充卖盘
                    for i in 0..5_000 {
                        let order = black_box(create_test_order(i, Side::Sell, 50000.0 + i as f64));
                        let _ = engine.place_order(order);
                    }

                    // 执行买单撮合
                    for i in 5_000..10_000 {
                        let order = black_box(create_test_order(i, Side::Buy, 55000.0));
                        let _ = engine.place_order(order);
                    }
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

fn bench_mixed_workload(c: &mut Criterion) {
    c.bench_function("mixed_workload_10k", |b| {
        b.iter_batched(
            || {
                match MatchingEngine::new(PoolConfig::default()) {
                    Ok(engine) => Some(engine),
                    Err(_) => None,
                }
            },
            |engine| {
                if let Some(mut engine) = engine {
                    // 混合工作负载：50%下单，30%撮合，20%撤单（简化）
                    for i in 0..10_000 {
                        let order = if i % 10 < 5 {
                            // 50% 下单（交替买卖）
                            let side = if i % 2 == 0 { Side::Buy } else { Side::Sell };
                            create_test_order(i, side, 50000.0 + (i % 1000) as f64)
                        } else if i % 10 < 8 {
                            // 30% 撮合（高价买单）
                            create_test_order(i, Side::Buy, 55000.0)
                        } else {
                            // 20% 撮合（低价卖单）
                            create_test_order(i, Side::Sell, 45000.0)
                        };
                        let _ = engine.place_order(black_box(order));
                    }
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, bench_place_order_only, bench_matching_only, bench_mixed_workload);
criterion_main!(benches);
