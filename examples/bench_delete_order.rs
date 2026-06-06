//! Benchmark: per-id DELETE vs single-shot DELETE … WHERE id = ANY($1)
//! for terminal-state orders (FILLED / REJECTED).
//!
//! The DB worker currently runs one DbCmd::DeleteOrder per FILLED/REJECTED
//! event = 1 RT each. A trade-bot market order that crosses 5 maker
//! levels produces 5 individual DELETEs. Batching cuts to 1.
//!
//! Usage:
//!   DATABASE_URL=postgres://user:password@127.0.0.1:5432/mydb \
//!     cargo run --release --example bench_delete_order

use sqlx::PgPool;
use std::time::{Duration, Instant};

const BATCH_SIZE: usize = 10;
const WARMUP: usize = 5;
const TRIALS: usize = 30;
const TEST_EMAIL: &str = "bench_delete@lightning.test";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://user:password@127.0.0.1:5432/mydb".to_string());
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(8)
        .connect(&url)
        .await?;
    println!("connected to {url}");

    let user_id = ensure_user(&pool).await?;
    println!("test user_id={user_id}");
    sqlx::query("DELETE FROM orders WHERE user_id=$1")
        .bind(user_id)
        .execute(&pool)
        .await?;

    for _ in 0..WARMUP {
        let ids = seed(&pool, user_id, BATCH_SIZE).await?;
        run_single(&pool, &ids).await?;
        let ids = seed(&pool, user_id, BATCH_SIZE).await?;
        run_batch(&pool, &ids).await?;
    }

    let mut single_times = Vec::with_capacity(TRIALS);
    let mut batch_times = Vec::with_capacity(TRIALS);
    for trial in 0..TRIALS {
        let ids = seed(&pool, user_id, BATCH_SIZE).await?;
        let t = Instant::now();
        run_single(&pool, &ids).await?;
        single_times.push(t.elapsed());

        let ids = seed(&pool, user_id, BATCH_SIZE).await?;
        let t = Instant::now();
        run_batch(&pool, &ids).await?;
        batch_times.push(t.elapsed());

        if trial < 3 || trial == TRIALS - 1 {
            eprintln!(
                "trial {trial:2}: single={:>8?}  batch={:>8?}",
                single_times.last().unwrap(),
                batch_times.last().unwrap()
            );
        }
    }

    println!("\n=== results over {TRIALS} trials, batch size {BATCH_SIZE} ===");
    print_stats("single (10× DELETE)", &single_times);
    print_stats("batch  (1× DELETE ANY)", &batch_times);
    let s_p50 = pctl(&single_times, 50);
    let b_p50 = pctl(&batch_times, 50);
    println!(
        "\nbatch is {:.1}× faster at p50",
        s_p50.as_secs_f64() / b_p50.as_secs_f64()
    );
    Ok(())
}

async fn run_single(pool: &PgPool, ids: &[i64]) -> anyhow::Result<()> {
    for &id in ids {
        sqlx::query("DELETE FROM orders WHERE id=$1")
            .bind(id)
            .execute(pool)
            .await?;
    }
    Ok(())
}

async fn run_batch(pool: &PgPool, ids: &[i64]) -> anyhow::Result<()> {
    sqlx::query("DELETE FROM orders WHERE id = ANY($1)")
        .bind(ids)
        .execute(pool)
        .await?;
    Ok(())
}

async fn ensure_user(pool: &PgPool) -> anyhow::Result<i64> {
    let id: i64 = sqlx::query_scalar(
        "INSERT INTO users (email, password_hash, full_name)
         VALUES ($1, 'x', 'Delete Bench')
         ON CONFLICT (email) DO UPDATE SET id = users.id RETURNING id",
    )
    .bind(TEST_EMAIL)
    .fetch_one(pool)
    .await?;
    Ok(id)
}

async fn seed(pool: &PgPool, user_id: i64, n: usize) -> anyhow::Result<Vec<i64>> {
    static SEQ: std::sync::atomic::AtomicI64 =
        std::sync::atomic::AtomicI64::new(400_000_000_000_000);
    let start = SEQ.fetch_add(n as i64, std::sync::atomic::Ordering::Relaxed);
    let mut ids = Vec::with_capacity(n);
    let mut sql = String::from(
        "INSERT INTO orders (id, user_id, symbol, side, order_type, price, quantity, filled, status, freeze_price) VALUES ",
    );
    for i in 0..n {
        let id = start + i as i64;
        ids.push(id);
        let side = if i % 2 == 0 { "buy" } else { "sell" };
        let price = 70_000.0 + i as f64;
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!(
            "({id}, {user_id}, 'BTC_USDT', '{side}', 'market', {price}, 0.001, 0.001, 'COMPLETED', 0)"
        ));
    }
    sqlx::query(&sql).execute(pool).await?;
    Ok(ids)
}

fn pctl(times: &[Duration], pct: usize) -> Duration {
    let mut s = times.to_vec();
    s.sort();
    s[(s.len() * pct / 100).min(s.len() - 1)]
}

fn print_stats(name: &str, times: &[Duration]) {
    let min = times.iter().min().unwrap();
    let max = times.iter().max().unwrap();
    let sum: Duration = times.iter().sum();
    let avg = sum / times.len() as u32;
    let p50 = pctl(times, 50);
    let p99 = pctl(times, 99);
    println!(
        "{name}  min={:>7?}  p50={:>7?}  avg={:>7?}  p99={:>7?}  max={:>7?}",
        min, p50, avg, p99, max
    );
}
