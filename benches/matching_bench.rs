use criterion::{black_box, criterion_group, criterion_main, Criterion};
use lightning_exchange::{MatchingEngine, Order, Side, TimeInForce, PoolConfig};

fn create_test_order(id: u64, side: Side, price_ticks: i64) -> Order {
    Order::new(
        id,
        side,
        price_ticks,
        1_000_000, // 1.0 BTC in lots (qty_step = 1e-6)
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
                        let order = black_box(create_test_order(i, Side::Buy, 5_000_000 + i as i64));
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
                    for i in 0..5_000 {
                        let order = black_box(create_test_order(i, Side::Sell, 5_000_000 + i as i64));
                        let _ = engine.place_order(order);
                    }

                    for i in 5_000..10_000 {
                        let order = black_box(create_test_order(i, Side::Buy, 5_500_000));
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
                    for i in 0..10_000 {
                        let order = if i % 10 < 5 {
                            let side = if i % 2 == 0 { Side::Buy } else { Side::Sell };
                            create_test_order(i, side, 5_000_000 + (i % 1000) as i64)
                        } else if i % 10 < 8 {
                            create_test_order(i, Side::Buy, 5_500_000)
                        } else {
                            create_test_order(i, Side::Sell, 4_500_000)
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
