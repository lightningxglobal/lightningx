//! Benchmark: per-id sequential CancelConfirmed vs single-shot batched
//! CancelConfirmed for a batch of N MM-side cancels.
//!
//! Measures the DB-worker round-trip cost of releasing frozen funds and
//! removing rows from `orders`. This is the hot path that bottlenecked
//! EC2 during cancel-replace cycles: each MM cycle issues a 20-id
//! batch_cancel; the engine acks each id individually; the spin thread
//! currently pushes 20 individual CancelConfirmed cmds; the DB worker
//! processes them serially with 3 SQL round-trips each (SELECT,
//! UPDATE accounts, DELETE) = 60 PG round-trips per batch.
//!
//! The proposed batched path collapses this to:
//!   1) DELETE FROM orders WHERE id = ANY($1) RETURNING ...
//!   2) one UPDATE accounts per distinct (user_id, asset)
//!
//! Two queries (3 in the typical bid+ask case where both USDT and BTC
//! need their frozen tally reduced).
//!
//! Usage:
//!   DATABASE_URL=postgres://user:password@127.0.0.1:5432/mydb \
//!     cargo run --release --example bench_cancel_confirm

use sqlx::PgPool;
use std::collections::HashMap;
use std::time::{Duration, Instant};

const BATCH_SIZE: usize = 20;
const WARMUP: usize = 5;
const TRIALS: usize = 30;
const TEST_EMAIL: &str = "bench_cancel@lightning.test";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgres://user:password@127.0.0.1:5432/mydb".to_string()
    });
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(8)
        .connect(&url)
        .await?;
    println!("connected to {url}");

    let user_id = ensure_test_user(&pool).await?;
    println!("test user_id={user_id}");

    // Warm-up — first few queries hit cold caches and pool init.
    for _ in 0..WARMUP {
        let ids = seed_pending_orders(&pool, user_id, BATCH_SIZE).await?;
        run_single_path(&pool, &ids).await?;
        let ids = seed_pending_orders(&pool, user_id, BATCH_SIZE).await?;
        run_batch_path(&pool, &ids).await?;
    }

    let mut single_times: Vec<Duration> = Vec::with_capacity(TRIALS);
    let mut batch_times: Vec<Duration> = Vec::with_capacity(TRIALS);

    for trial in 0..TRIALS {
        // Single — replicates current production CancelConfirmed handler.
        let ids = seed_pending_orders(&pool, user_id, BATCH_SIZE).await?;
        let t = Instant::now();
        run_single_path(&pool, &ids).await?;
        single_times.push(t.elapsed());

        // Batch — the proposed BatchCancelConfirmed handler.
        let ids = seed_pending_orders(&pool, user_id, BATCH_SIZE).await?;
        let t = Instant::now();
        run_batch_path(&pool, &ids).await?;
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
    print_stats("single (20× individual)", &single_times);
    print_stats("batch  (1× batched)    ", &batch_times);

    let single_p50 = pctl(&single_times, 50);
    let batch_p50 = pctl(&batch_times, 50);
    let speedup = single_p50.as_secs_f64() / batch_p50.as_secs_f64();
    println!("\nbatch is {speedup:.1}× faster at p50");

    cleanup(&pool, user_id).await?;
    Ok(())
}

// ── Single path (current CancelConfirmed) ────────────────────────────────────

async fn run_single_path(pool: &PgPool, ids: &[i64]) -> anyhow::Result<()> {
    for &id in ids {
        // 1) SELECT the order to learn what funds to release.
        let row: Option<(i64, String, String, f64, f64, f64)> = sqlx::query_as(
            "SELECT user_id, symbol, side, quantity, filled,
                    COALESCE(freeze_price, COALESCE(price, 0.0))
             FROM orders
             WHERE id=$1 AND status IN ('PENDING','TRADING')",
        )
        .bind(id)
        .fetch_optional(pool)
        .await?;
        let Some((uid, symbol, side, quantity, filled, freeze_price)) = row else {
            continue;
        };

        let release_qty = (quantity - filled).max(0.0);
        let (base, quote) = split_symbol(&symbol);
        let (asset, amount) = if side == "sell" {
            (base, release_qty)
        } else {
            (quote, freeze_price * release_qty)
        };

        // 2) UPDATE accounts to release the frozen amount.
        if amount > 0.0 {
            let _: Option<(f64, f64)> = sqlx::query_as(
                "UPDATE accounts
                 SET balance = balance + $1,
                     frozen  = GREATEST(frozen - $1, 0),
                     updated_at = NOW()
                 WHERE user_id=$2 AND asset=$3
                 RETURNING balance, frozen",
            )
            .bind(amount)
            .bind(uid)
            .bind(&asset)
            .fetch_optional(pool)
            .await?;
        }

        // 3) DELETE the order row.
        sqlx::query("DELETE FROM orders WHERE id=$1")
            .bind(id)
            .execute(pool)
            .await?;
    }
    Ok(())
}

// ── Batch path (proposed BatchCancelConfirmed) ───────────────────────────────

async fn run_batch_path(pool: &PgPool, ids: &[i64]) -> anyhow::Result<()> {
    // 1) DELETE ... RETURNING all order rows in one round-trip.
    let rows: Vec<(i64, i64, String, String, f64, f64, f64)> = sqlx::query_as(
        "DELETE FROM orders
         WHERE id = ANY($1) AND status IN ('PENDING','TRADING')
         RETURNING id, user_id, symbol, side, quantity, filled,
                   COALESCE(freeze_price, COALESCE(price, 0.0))",
    )
    .bind(ids)
    .fetch_all(pool)
    .await?;

    // 2) Group by (user_id, asset) and sum the release amounts.
    let mut releases: HashMap<(i64, String), f64> = HashMap::new();
    for (_id, uid, symbol, side, quantity, filled, freeze_price) in rows {
        let release_qty = (quantity - filled).max(0.0);
        let (base, quote) = split_symbol(&symbol);
        let (asset, amount) = if side == "sell" {
            (base, release_qty)
        } else {
            (quote, freeze_price * release_qty)
        };
        if amount > 0.0 {
            *releases.entry((uid, asset)).or_insert(0.0) += amount;
        }
    }

    // 3) One UPDATE accounts per (user_id, asset) — for MM bid+ask cancels
    //    that's two queries (USDT for bids, BTC for asks); for one-sided
    //    cancels it's a single query.
    for ((uid, asset), amount) in releases {
        sqlx::query(
            "UPDATE accounts
             SET balance = balance + $1,
                 frozen  = GREATEST(frozen - $1, 0),
                 updated_at = NOW()
             WHERE user_id=$2 AND asset=$3",
        )
        .bind(amount)
        .bind(uid)
        .bind(&asset)
        .execute(pool)
        .await?;
    }
    Ok(())
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn split_symbol(symbol: &str) -> (String, String) {
    let parts: Vec<&str> = symbol.splitn(2, '_').collect();
    let base = parts.first().copied().unwrap_or("BTC").to_string();
    let quote = parts.last().copied().unwrap_or("USDT").to_string();
    (base, quote)
}

async fn ensure_test_user(pool: &PgPool) -> anyhow::Result<i64> {
    let id: i64 = sqlx::query_scalar(
        "INSERT INTO users (email, password_hash, full_name)
         VALUES ($1, 'x', 'Cancel Bench')
         ON CONFLICT (email) DO UPDATE SET id = users.id
         RETURNING id",
    )
    .bind(TEST_EMAIL)
    .fetch_one(pool)
    .await?;

    // Generous balances so the bench never trips an insufficient-funds path.
    sqlx::query(
        "INSERT INTO accounts (user_id, asset, balance, frozen)
         VALUES ($1, 'USDT', 1e12, 1e12), ($1, 'BTC', 1e6, 1e6)
         ON CONFLICT (user_id, asset) DO UPDATE
         SET balance = 1e12, frozen = 1e12",
    )
    .bind(id)
    .execute(pool)
    .await?;
    Ok(id)
}

async fn seed_pending_orders(
    pool: &PgPool,
    user_id: i64,
    n: usize,
) -> anyhow::Result<Vec<i64>> {
    // Alternate buy/sell, varying prices, fixed quantity. coid is NULL so
    // the unique index doesn't get involved (it's the orders body cost we
    // want to measure, not coid contention).
    //
    // Reserve the high i64 range (>= 10^14) for the bench so a concurrently
    // running MM / trade-bot can never collide on orders.id. Per-bench
    // counter resets when no rows of the test user remain.
    static SEQ: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(100_000_000_000_000);
    let start = SEQ.fetch_add(n as i64, std::sync::atomic::Ordering::Relaxed);
    let next_id = start;
    let mut ids = Vec::with_capacity(n);
    let mut sql = String::from(
        "INSERT INTO orders (id, user_id, symbol, side, order_type,
                             price, quantity, filled, status, freeze_price)
         VALUES ",
    );
    for i in 0..n {
        let id = next_id + i as i64;
        ids.push(id);
        let side = if i % 2 == 0 { "buy" } else { "sell" };
        let price = 70_000.0 + (i as f64);
        let freeze_price = if side == "buy" { price } else { 0.0 };
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!(
            "({id}, {user_id}, 'BTC_USDT', '{side}', 'limit',
              {price}, 0.001, 0, 'PENDING', {freeze_price})"
        ));
    }
    sqlx::query(&sql).execute(pool).await?;
    Ok(ids)
}

async fn cleanup(pool: &PgPool, user_id: i64) -> anyhow::Result<()> {
    sqlx::query("DELETE FROM orders WHERE user_id=$1")
        .bind(user_id)
        .execute(pool)
        .await?;
    Ok(())
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
