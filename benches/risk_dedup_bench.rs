use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::collections::HashSet;

fn sort_dedup(mut user_ids: Vec<i64>) -> Vec<i64> {
    user_ids.sort_unstable();
    user_ids.dedup();
    user_ids
}

fn hashset_dedup(user_ids: &[i64]) -> Vec<i64> {
    let mut seen = HashSet::with_capacity(user_ids.len());
    let mut out = Vec::with_capacity(user_ids.len());
    for &user_id in user_ids {
        if seen.insert(user_id) {
            out.push(user_id);
        }
    }
    out
}

fn unique_users(count: i64) -> Vec<i64> {
    (0..count).collect()
}

fn two_positions_per_user(count: i64) -> Vec<i64> {
    let mut ids = Vec::with_capacity((count * 2) as usize);
    for user_id in 0..count {
        ids.push(user_id);
        ids.push(user_id);
    }
    ids
}

fn clustered_duplicates(unique_count: i64, repeats: i64) -> Vec<i64> {
    let mut ids = Vec::with_capacity((unique_count * repeats) as usize);
    for _ in 0..repeats {
        for user_id in 0..unique_count {
            ids.push(user_id);
        }
    }
    ids
}

fn bench_case(c: &mut Criterion, name: &str, input: Vec<i64>) {
    let mut group = c.benchmark_group(name);
    group.bench_function("sort_unstable_dedup", |b| {
        b.iter(|| sort_dedup(black_box(input.clone())))
    });
    group.bench_function("hashset_dedup", |b| {
        b.iter(|| hashset_dedup(black_box(&input)))
    });
    group.finish();
}

fn bench_risk_user_id_dedup(c: &mut Criterion) {
    bench_case(c, "risk_dedup_40k_unique_users", unique_users(40_000));
    bench_case(
        c,
        "risk_dedup_40k_users_two_positions_each",
        two_positions_per_user(40_000),
    );
    bench_case(
        c,
        "risk_dedup_10k_users_four_symbols_each",
        clustered_duplicates(10_000, 4),
    );
}

criterion_group!(benches, bench_risk_user_id_dedup);
criterion_main!(benches);
