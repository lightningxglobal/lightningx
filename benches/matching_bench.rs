use criterion::{black_box, criterion_group, criterion_main, Criterion};
use matching_engine::{MatchingEngine, Order, Side, TimeInForce, PoolConfig};

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
            || MatchingEngine::new(PoolConfig::default()),
            |engine| {
                if let Ok(mut engine) = engine {
                    for i in 0..10_000 {
                        let order = black_box(create_test_order(i, Side::Buy, 50000.0 + i as f64));
                        engine.place_order(order).ok();
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
                MatchingEngine::new(PoolConfig::default())
            },
            |engine| {
                if let Ok(mut engine) = engine {
                    // 预填充卖盘
                    for i in 0..5_000 {
                        let order = black_box(create_test_order(i, Side::Sell, 50000.0 + i as f64));
                        engine.place_order(order).ok();
                    }

                    // 执行买单撮合
                    for i in 5_000..10_000 {
                        let order = black_box(create_test_order(i, Side::Buy, 55000.0));
                        engine.place_order(order).ok();
                    }
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, bench_place_order_only, bench_matching_only);
criterion_main!(benches);
