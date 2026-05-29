//! Benchmark: per-id sequential UpsertOrder (current) vs single-shot batched
//! INSERT + grouped account freeze for a batch of N MM-side new orders.
//!
//! Current production code (desk_server.rs DbCmd::UpsertOrder) does, per id:
//!   1. INSERT INTO orders ... ON CONFLICT (id) DO UPDATE        (1 round-trip)
//!   2. UPDATE accounts SET frozen=frozen+X WHERE balance-frozen>=X
//!      (via AccountRepository::freeze_for_buy/sell)             (1 round-trip)
//!
//! For an MM 20-id batch_place that is 40 PG round-trips, mirroring the
//! cancel pipeline we already fixed.
//!
//! The proposed BatchUpsertOrder collapses this to:
//!   1. INSERT INTO orders ... VALUES (...),(...),(...) ON CONFLICT DO UPDATE
//!      (single multi-row INSERT, 1 round-trip)
//!   2. Group freezes by (user_id, asset), one UPDATE per group
//!      (typically 2: USDT for bids, BTC for asks)
//!
//! Usage:
//!   DATABASE_URL=postgres://user:password@127.0.0.1:5432/mydb \
//!     cargo run --release --example bench_upsert_order

use sqlx::PgPool;
use std::collections::HashMap;
use std::time::{Duration, Instant};

const BATCH_SIZE: usize = 20;
const WARMUP: usize = 5;
const TRIALS: usize = 30;
const TEST_EMAIL: &str = "bench_upsert@lightning.test";

#[derive(Clone)]
struct OrderRow {
    id: i64,
    user_id: i64,
    symbol: String,
    side: String, // "buy" | "sell"
    price: f64,
    quantity: f64,
    freeze_price: f64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://user:password@127.0.0.1:5432/mydb".to_string());
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(8)
        .connect(&url)
        .await?;
    println!("connected to {url}");

    let user_id = ensure_test_user(&pool).await?;
    println!("test user_id={user_id}");

    // Warm-up
    for _ in 0..WARMUP {
        let orders = build_batch(user_id, BATCH_SIZE);
        run_single_path(&pool, &orders).await?;
        cleanup_test_orders(&pool, user_id).await?;

        let orders = build_batch(user_id, BATCH_SIZE);
        run_batch_path(&pool, &orders).await?;
        cleanup_test_orders(&pool, user_id).await?;
    }

    let mut single_times: Vec<Duration> = Vec::with_capacity(TRIALS);
    let mut batch_times: Vec<Duration> = Vec::with_capacity(TRIALS);

    for trial in 0..TRIALS {
        let orders = build_batch(user_id, BATCH_SIZE);
        let t = Instant::now();
        run_single_path(&pool, &orders).await?;
        single_times.push(t.elapsed());
        cleanup_test_orders(&pool, user_id).await?;

        let orders = build_batch(user_id, BATCH_SIZE);
        let t = Instant::now();
        run_batch_path(&pool, &orders).await?;
        batch_times.push(t.elapsed());
        cleanup_test_orders(&pool, user_id).await?;

        if trial < 3 || trial == TRIALS - 1 {
            eprintln!(
                "trial {trial:2}: single={:>8?}  batch={:>8?}",
                single_times.last().unwrap(),
                batch_times.last().unwrap()
            );
        }
    }

    println!("\n=== results over {TRIALS} trials, batch size {BATCH_SIZE} ===");
    print_stats("single (20× INSERT + UPDATE)", &single_times);
    print_stats("batch  (1× multi-row + grouped)", &batch_times);

    let single_p50 = pctl(&single_times, 50);
    let batch_p50 = pctl(&batch_times, 50);
    let speedup = single_p50.as_secs_f64() / batch_p50.as_secs_f64();
    println!("\nbatch is {speedup:.1}× faster at p50");

    sqlx::query("DELETE FROM accounts WHERE user_id=$1")
        .bind(user_id)
        .execute(&pool)
        .await?;
    Ok(())
}

// ── Single path (current DbCmd::UpsertOrder, do_freeze=true) ────────────────

async fn run_single_path(pool: &PgPool, orders: &[OrderRow]) -> anyhow::Result<()> {
    for o in orders {
        // 1) INSERT INTO orders ... ON CONFLICT DO UPDATE
        sqlx::query(
            "INSERT INTO orders
               (id, user_id, symbol, side, order_type, price, quantity,
                filled, status, freeze_price)
             VALUES ($1, $2, $3, $4, 'limit', $5, $6, 0, 'PENDING', $7)
             ON CONFLICT (id) DO UPDATE SET status='PENDING', filled=0, updated_at=NOW()",
        )
        .bind(o.id)
        .bind(o.user_id)
        .bind(&o.symbol)
        .bind(&o.side)
        .bind(o.price)
        .bind(o.quantity)
        .bind(o.freeze_price)
        .execute(pool)
        .await?;

        // 2) UPDATE accounts to freeze funds.
        let (asset, amount) = freeze_for(o);
        sqlx::query(
            "UPDATE accounts
             SET frozen = frozen + $1,
                 updated_at = NOW()
             WHERE user_id=$2 AND asset=$3",
        )
        .bind(amount)
        .bind(o.user_id)
        .bind(&asset)
        .execute(pool)
        .await?;
    }
    Ok(())
}

// ── Batch path (proposed BatchUpsertOrder) ──────────────────────────────────

async fn run_batch_path(pool: &PgPool, orders: &[OrderRow]) -> anyhow::Result<()> {
    if orders.is_empty() {
        return Ok(());
    }

    // 1) Single multi-row INSERT.
    let mut sql = String::from(
        "INSERT INTO orders
           (id, user_id, symbol, side, order_type, price, quantity, filled, status, freeze_price)
         VALUES ",
    );
    let mut binds: Vec<String> = Vec::with_capacity(orders.len());
    for (i, _o) in orders.iter().enumerate() {
        let n = i * 7;
        binds.push(format!(
            "(${}, ${}, ${}, ${}, 'limit', ${}, ${}, 0, 'PENDING', ${})",
            n + 1, n + 2, n + 3, n + 4, n + 5, n + 6, n + 7
        ));
    }
    sql.push_str(&binds.join(","));
    sql.push_str(" ON CONFLICT (id) DO UPDATE SET status='PENDING', filled=0, updated_at=NOW()");

    let mut q = sqlx::query(&sql);
    for o in orders {
        q = q
            .bind(o.id)
            .bind(o.user_id)
            .bind(&o.symbol)
            .bind(&o.side)
            .bind(o.price)
            .bind(o.quantity)
            .bind(o.freeze_price);
    }
    q.execute(pool).await?;

    // 2) Group freezes by (user_id, asset) and run one UPDATE per group.
    let mut freezes: HashMap<(i64, String), f64> = HashMap::new();
    for o in orders {
        let (asset, amount) = freeze_for(o);
        *freezes.entry((o.user_id, asset)).or_insert(0.0) += amount;
    }
    for ((uid, asset), amount) in freezes {
        sqlx::query(
            "UPDATE accounts
             SET frozen = frozen + $1,
                 updated_at = NOW()
             WHERE user_id=$2 AND asset=$3",
        )
        .bind(amount)
        .bind(uid)
        .bind(asset)
        .execute(pool)
        .await?;
    }
    Ok(())
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn freeze_for(o: &OrderRow) -> (String, f64) {
    let parts: Vec<&str> = o.symbol.splitn(2, '_').collect();
    let base = parts.first().copied().unwrap_or("BTC").to_string();
    let quote = parts.last().copied().unwrap_or("USDT").to_string();
    if o.side == "sell" {
        (base, o.quantity)
    } else {
        (quote, o.freeze_price * o.quantity)
    }
}

fn build_batch(user_id: i64, n: usize) -> Vec<OrderRow> {
    // Reserve the high i64 range so bench rows can't clash with a concurrent
    // MM / trade-bot inserting via the live next_order_id counter.
    static SEQ: std::sync::atomic::AtomicI64 =
        std::sync::atomic::AtomicI64::new(200_000_000_000_000);
    let start = SEQ.fetch_add(n as i64, std::sync::atomic::Ordering::Relaxed);
    (0..n)
        .map(|i| {
            let side = if i % 2 == 0 { "buy" } else { "sell" }.to_string();
            let price = 70_000.0 + i as f64;
            let freeze_price = if side == "buy" { price } else { 0.0 };
            OrderRow {
                id: start + i as i64,
                user_id,
                symbol: "BTC_USDT".to_string(),
                side,
                price,
                quantity: 0.001,
                freeze_price,
            }
        })
        .collect()
}

async fn ensure_test_user(pool: &PgPool) -> anyhow::Result<i64> {
    let id: i64 = sqlx::query_scalar(
        "INSERT INTO users (email, password_hash, full_name)
         VALUES ($1, 'x', 'Upsert Bench')
         ON CONFLICT (email) DO UPDATE SET id = users.id
         RETURNING id",
    )
    .bind(TEST_EMAIL)
    .fetch_one(pool)
    .await?;
    sqlx::query(
        "INSERT INTO accounts (user_id, asset, balance, frozen)
         VALUES ($1, 'USDT', 1e12, 0), ($1, 'BTC', 1e6, 0)
         ON CONFLICT (user_id, asset) DO UPDATE
         SET balance = 1e12, frozen = 0",
    )
    .bind(id)
    .execute(pool)
    .await?;
    Ok(id)
}

async fn cleanup_test_orders(pool: &PgPool, user_id: i64) -> anyhow::Result<()> {
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
