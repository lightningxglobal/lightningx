//! Benchmark: per-trade transactional SettleTrade (current) vs single-shot
//! batched settlement for N fills that arrive in the same engine burst.
//!
//! Current production code (desk_server.rs DbCmd::SettleTrade) does, per fill:
//!   BEGIN;
//!     INSERT trade row                  (1 RT)
//!     UPDATE maker order filled/status  (1 RT)
//!     UPDATE buyer  account (quote)     (1 RT)
//!     UPSERT buyer  account (base)      (1 RT)
//!     UPDATE seller account (base)      (1 RT)
//!     UPSERT seller account (quote)     (1 RT)
//!   COMMIT;                             (1 RT)
//!
//! ≈ 7 round-trips per trade. A single trade-bot market order can produce
//! 3-6 maker fills, so that's 21-42 RTs per market submit.
//!
//! The proposed BatchSettleTrade does the same work for N fills in one txn:
//!   BEGIN;
//!     multi-row INSERT trades                                  (1 RT)
//!     UPDATE orders FROM (VALUES ...) AS u WHERE orders.id=u.id (1 RT)
//!     N grouped UPDATE accounts (per distinct user_id+asset)    (~2-4 RT)
//!   COMMIT;                                                     (1 RT)
//!
//! ≈ 5-7 round-trips total regardless of N — same cost as a single fill
//! gives every additional fill in the batch ~free.
//!
//! Usage:
//!   DATABASE_URL=postgres://user:password@127.0.0.1:5432/mydb \
//!     cargo run --release --example bench_settle_trade

use sqlx::{PgPool, Postgres, Transaction};
use std::collections::HashMap;
use std::time::{Duration, Instant};

const BATCH_SIZE: usize = 5; // realistic trade-bot market-order fill count
const WARMUP: usize = 5;
const TRIALS: usize = 30;
const TAKER_EMAIL: &str = "bench_settle_taker@lightning.test";
const MAKER_EMAIL: &str = "bench_settle_maker@lightning.test";

#[derive(Clone)]
struct Fill {
    taker_id: i64,
    maker_id: i64,
    taker_uid: i64,
    maker_uid: i64,
    price: f64,
    qty: f64,
    side: u8, // 0=buy taker, 1=sell taker
    symbol: String,
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

    let (taker_uid, maker_uid) = ensure_users(&pool).await?;
    println!("taker uid={taker_uid}  maker uid={maker_uid}");
    // Clear orders left behind by a previous aborted run so the
    // reserved-id-range SEQ doesn't collide.
    sqlx::query("DELETE FROM orders WHERE user_id = ANY($1)")
        .bind(&[taker_uid, maker_uid][..])
        .execute(&pool)
        .await?;

    for _ in 0..WARMUP {
        let fills = build_fills(&pool, BATCH_SIZE, taker_uid, maker_uid).await?;
        run_single_path(&pool, &fills).await?;
        let fills = build_fills(&pool, BATCH_SIZE, taker_uid, maker_uid).await?;
        run_batch_path(&pool, &fills).await?;
    }

    let mut single_times: Vec<Duration> = Vec::with_capacity(TRIALS);
    let mut batch_times: Vec<Duration> = Vec::with_capacity(TRIALS);

    for trial in 0..TRIALS {
        let fills = build_fills(&pool, BATCH_SIZE, taker_uid, maker_uid).await?;
        let t = Instant::now();
        run_single_path(&pool, &fills).await?;
        single_times.push(t.elapsed());

        let fills = build_fills(&pool, BATCH_SIZE, taker_uid, maker_uid).await?;
        let t = Instant::now();
        run_batch_path(&pool, &fills).await?;
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
    print_stats(
        &format!("single ({}× txn)        ", BATCH_SIZE),
        &single_times,
    );
    print_stats(" batch  (1× multi-row txn)", &batch_times);

    let single_p50 = pctl(&single_times, 50);
    let batch_p50 = pctl(&batch_times, 50);
    let speedup = single_p50.as_secs_f64() / batch_p50.as_secs_f64();
    println!("\nbatch is {speedup:.1}× faster at p50");

    cleanup(&pool, taker_uid, maker_uid).await?;
    Ok(())
}

// ── Single path (current DbCmd::SettleTrade, one txn per fill) ──────────────

async fn run_single_path(pool: &PgPool, fills: &[Fill]) -> anyhow::Result<()> {
    for f in fills {
        let mut txn = pool.begin().await?;
        settle_one(&mut txn, f).await?;
        txn.commit().await?;
    }
    Ok(())
}

async fn settle_one(txn: &mut Transaction<'_, Postgres>, f: &Fill) -> anyhow::Result<()> {
    let (base, quote) = split_symbol(&f.symbol);
    let cost = f.price * f.qty;
    let (buyer_id, seller_id, buy_oid, sell_oid) = if f.side == 0 {
        (f.taker_uid, f.maker_uid, f.taker_id, f.maker_id)
    } else {
        (f.maker_uid, f.taker_uid, f.maker_id, f.taker_id)
    };

    sqlx::query(
        "INSERT INTO trades (symbol, buy_order_id, sell_order_id, price, quantity, created_at)
         VALUES ($1,$2,$3,$4,$5,NOW())",
    )
    .bind(&f.symbol)
    .bind(buy_oid)
    .bind(sell_oid)
    .bind(f.price)
    .bind(f.qty)
    .execute(&mut **txn)
    .await?;

    sqlx::query(
        "UPDATE orders SET filled = filled + $1,
         status = CASE WHEN quantity - (filled + $1) < 1e-9 THEN 'COMPLETED' ELSE 'TRADING' END,
         updated_at = NOW() WHERE id = $2",
    )
    .bind(f.qty)
    .bind(f.maker_id)
    .execute(&mut **txn)
    .await?;

    sqlx::query(
        "UPDATE accounts SET balance = balance - $1, frozen = GREATEST(frozen - $1, 0),
         updated_at = NOW() WHERE user_id = $2 AND asset = $3",
    )
    .bind(cost)
    .bind(buyer_id)
    .bind(&quote)
    .execute(&mut **txn)
    .await?;

    sqlx::query(
        "INSERT INTO accounts (user_id, asset, balance, frozen) VALUES ($1,$2,$3,0)
         ON CONFLICT (user_id, asset) DO UPDATE
         SET balance = accounts.balance + $3, updated_at = NOW()",
    )
    .bind(buyer_id)
    .bind(&base)
    .bind(f.qty)
    .execute(&mut **txn)
    .await?;

    sqlx::query(
        "UPDATE accounts SET balance = balance - $1, frozen = GREATEST(frozen - $1, 0),
         updated_at = NOW() WHERE user_id = $2 AND asset = $3",
    )
    .bind(f.qty)
    .bind(seller_id)
    .bind(&base)
    .execute(&mut **txn)
    .await?;

    sqlx::query(
        "INSERT INTO accounts (user_id, asset, balance, frozen) VALUES ($1,$2,$3,0)
         ON CONFLICT (user_id, asset) DO UPDATE
         SET balance = accounts.balance + $3, updated_at = NOW()",
    )
    .bind(seller_id)
    .bind(&quote)
    .bind(cost)
    .execute(&mut **txn)
    .await?;

    Ok(())
}

// ── Batch path (proposed BatchSettleTrade) ──────────────────────────────────

async fn run_batch_path(pool: &PgPool, fills: &[Fill]) -> anyhow::Result<()> {
    if fills.is_empty() {
        return Ok(());
    }
    let mut txn = pool.begin().await?;

    // 1) Multi-row INSERT trades.
    let mut sql = String::from(
        "INSERT INTO trades (symbol, buy_order_id, sell_order_id, price, quantity, created_at) VALUES ",
    );
    let mut buy_oids: Vec<i64> = Vec::with_capacity(fills.len());
    let mut sell_oids: Vec<i64> = Vec::with_capacity(fills.len());
    for (i, f) in fills.iter().enumerate() {
        let (b, s) = if f.side == 0 {
            (f.taker_id, f.maker_id)
        } else {
            (f.maker_id, f.taker_id)
        };
        buy_oids.push(b);
        sell_oids.push(s);
        let n = i * 5;
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!(
            "(${},${},${},${},${},NOW())",
            n + 1,
            n + 2,
            n + 3,
            n + 4,
            n + 5,
        ));
    }
    let mut q = sqlx::query(&sql);
    for (i, f) in fills.iter().enumerate() {
        q = q
            .bind(&f.symbol)
            .bind(buy_oids[i])
            .bind(sell_oids[i])
            .bind(f.price)
            .bind(f.qty);
    }
    q.execute(&mut *txn).await?;

    // 2) Multi-row UPDATE orders using FROM (VALUES ...) AS u.
    let mut sql = String::from(
        "UPDATE orders SET
           filled = orders.filled + u.delta_qty,
           status = CASE WHEN orders.quantity - (orders.filled + u.delta_qty) < 1e-9 THEN 'COMPLETED' ELSE 'TRADING' END,
           updated_at = NOW()
         FROM (VALUES ",
    );
    for (i, _f) in fills.iter().enumerate() {
        if i > 0 {
            sql.push(',');
        }
        let n = i * 2;
        sql.push_str(&format!("(${}::bigint, ${}::float8)", n + 1, n + 2));
    }
    sql.push_str(") AS u(maker_id, delta_qty) WHERE orders.id = u.maker_id");
    let mut q = sqlx::query(&sql);
    for f in fills {
        q = q.bind(f.maker_id).bind(f.qty);
    }
    q.execute(&mut *txn).await?;

    // 3) Account deltas grouped by (user_id, asset). Each fill contributes:
    //   buyer  quote: balance -= cost, frozen -= cost
    //   buyer  base : balance += qty
    //   seller base : balance -= qty,  frozen -= qty
    //   seller quote: balance += cost
    #[derive(Default, Clone, Copy)]
    struct Delta {
        balance: f64,
        frozen_release: f64,
    }
    let mut deltas: HashMap<(i64, String), Delta> = HashMap::new();
    for f in fills {
        let (base, quote) = split_symbol(&f.symbol);
        let cost = f.price * f.qty;
        let (buyer, seller) = if f.side == 0 {
            (f.taker_uid, f.maker_uid)
        } else {
            (f.maker_uid, f.taker_uid)
        };
        let e = deltas.entry((buyer, quote.clone())).or_default();
        e.balance -= cost;
        e.frozen_release += cost;

        let e = deltas.entry((buyer, base.clone())).or_default();
        e.balance += f.qty;

        let e = deltas.entry((seller, base.clone())).or_default();
        e.balance -= f.qty;
        e.frozen_release += f.qty;

        let e = deltas.entry((seller, quote.clone())).or_default();
        e.balance += cost;
    }
    for ((uid, asset), d) in deltas {
        // Plain UPDATE — accounts row is pre-seeded by ensure_users / the
        // freeze step that precedes any settlement in production. The
        // INSERT...ON CONFLICT pattern would trip CHECK (balance >= 0) when
        // the proposed insert balance value is negative.
        sqlx::query(
            "UPDATE accounts SET
               balance = balance + $1,
               frozen  = GREATEST(frozen - $2, 0),
               updated_at = NOW()
             WHERE user_id = $3 AND asset = $4",
        )
        .bind(d.balance)
        .bind(d.frozen_release)
        .bind(uid)
        .bind(asset)
        .execute(&mut *txn)
        .await?;
    }

    txn.commit().await?;
    Ok(())
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn split_symbol(symbol: &str) -> (String, String) {
    let parts: Vec<&str> = symbol.splitn(2, '_').collect();
    let base = parts.first().copied().unwrap_or("BTC").to_string();
    let quote = parts.last().copied().unwrap_or("USDT").to_string();
    (base, quote)
}

async fn ensure_users(pool: &PgPool) -> anyhow::Result<(i64, i64)> {
    let taker: i64 = sqlx::query_scalar(
        "INSERT INTO users (email, password_hash, full_name)
         VALUES ($1, 'x', 'Settle Taker')
         ON CONFLICT (email) DO UPDATE SET id = users.id RETURNING id",
    )
    .bind(TAKER_EMAIL)
    .fetch_one(pool)
    .await?;
    let maker: i64 = sqlx::query_scalar(
        "INSERT INTO users (email, password_hash, full_name)
         VALUES ($1, 'x', 'Settle Maker')
         ON CONFLICT (email) DO UPDATE SET id = users.id RETURNING id",
    )
    .bind(MAKER_EMAIL)
    .fetch_one(pool)
    .await?;
    for uid in [taker, maker] {
        sqlx::query(
            "INSERT INTO accounts (user_id, asset, balance, frozen)
             VALUES ($1, 'USDT', 1e12, 0), ($1, 'BTC', 1e6, 0)
             ON CONFLICT (user_id, asset) DO UPDATE SET balance = 1e12, frozen = 0",
        )
        .bind(uid)
        .execute(pool)
        .await?;
    }
    Ok((taker, maker))
}

async fn reset_accounts(pool: &PgPool, taker_uid: i64, maker_uid: i64) -> anyhow::Result<()> {
    // Reset balances per trial so the check constraint (balance >= 0) doesn't
    // trip as the bench accumulates many simulated trades.
    for uid in [taker_uid, maker_uid] {
        sqlx::query("UPDATE accounts SET balance = 1e12, frozen = 0 WHERE user_id = $1")
            .bind(uid)
            .execute(pool)
            .await?;
    }
    Ok(())
}

async fn build_fills(
    pool: &PgPool,
    n: usize,
    taker_uid: i64,
    maker_uid: i64,
) -> anyhow::Result<Vec<Fill>> {
    reset_accounts(pool, taker_uid, maker_uid).await?;
    // Seed N pairs of (taker, maker) order rows so the UPDATE orders FROM (VALUES)
    // step has something to hit. Use the reserved high-i64 range to avoid
    // clashing with live MM.
    static SEQ: std::sync::atomic::AtomicI64 =
        std::sync::atomic::AtomicI64::new(300_000_000_000_000);
    let start = SEQ.fetch_add(2 * n as i64, std::sync::atomic::Ordering::Relaxed);

    let mut fills = Vec::with_capacity(n);
    let mut sql = String::from(
        "INSERT INTO orders (id, user_id, symbol, side, order_type, price, quantity, filled, status, freeze_price) VALUES ",
    );
    let mut first = true;
    for i in 0..n {
        let taker_id = start + (2 * i as i64);
        let maker_id = start + (2 * i as i64) + 1;
        let price = 70_000.0 + i as f64;
        let qty = 0.001_f64;
        let side = (i as u8) & 1; // alternate
        let (taker_side, maker_side) = if side == 0 {
            ("buy", "sell")
        } else {
            ("sell", "buy")
        };
        let taker_freeze = if taker_side == "buy" { price } else { 0.0 };
        let maker_freeze = if maker_side == "buy" { price } else { 0.0 };
        if !first {
            sql.push(',');
        }
        first = false;
        sql.push_str(&format!(
            "({taker_id},{taker_uid},'BTC_USDT','{taker_side}','market',{price},{qty},0,'PENDING',{taker_freeze}),
             ({maker_id},{maker_uid},'BTC_USDT','{maker_side}','limit',{price},{qty},0,'PENDING',{maker_freeze})",
        ));
        fills.push(Fill {
            taker_id,
            maker_id,
            taker_uid,
            maker_uid,
            price,
            qty,
            side,
            symbol: "BTC_USDT".to_string(),
        });
    }
    sqlx::query(&sql).execute(pool).await?;
    Ok(fills)
}

async fn cleanup(pool: &PgPool, taker_uid: i64, maker_uid: i64) -> anyhow::Result<()> {
    sqlx::query("DELETE FROM trades WHERE buy_order_id IN (SELECT id FROM orders WHERE user_id = ANY($1)) OR sell_order_id IN (SELECT id FROM orders WHERE user_id = ANY($1))")
        .bind(&[taker_uid, maker_uid][..])
        .execute(pool).await.ok();
    sqlx::query("DELETE FROM orders WHERE user_id = ANY($1)")
        .bind(&[taker_uid, maker_uid][..])
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
