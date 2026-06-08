//! S5 drill — trigger orders end to end against the REAL topology:
//!
//!   1. a sell-stop whose trigger the mark already satisfies fires
//!      immediately: status flips pending→triggered exactly once and the
//!      injected order TRADES (margin gate included);
//!   2. a deep sell-stop stays pending, survives `kill -9` of the desk
//!      (hydrate), and fires after the restart when the market is walked
//!      down through its trigger — proving the book is rebuilt and the
//!      firing loop still works in the reborn process.
//!
//! Dev-mode marks (no INDEX_SOURCES): mark = EWMA of the raw book mid.

mod common;

use std::process::{Command, Stdio};
use std::time::Duration;

use common::*;
use lightning_exchange::db;
use serde_json::{Value, json};
use uuid::Uuid;

const CONTROL_PORT: u16 = 19010;
const DESK_PORT: u16 = 14078;
const SYMBOL: &str = "BTC_USDT";
const JWT: &str = "chaos-trigger-fire-secret-0123456789abcdef0123456789abcdef00";

fn spawn_engine(aeron_dir: &str, control: &str) -> std::io::Result<ProcGuard> {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_exchange-engine"));
    cmd.env("DATABASE_URL", pg_url())
        .env("AERON_DIR", aeron_dir)
        .env("SYMBOLS", SYMBOL)
        .env("EXCHANGE_ARCHIVE_CONTROL", control)
        .env("ENGINE_IDLE_SPINS", "1000")
        .env("RUST_LOG", "debug")
        .stdout(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(format!("/tmp/chaos-trig-engine-{}.log", std::process::id()))
                .map(Stdio::from)
                .unwrap_or(Stdio::null()),
        )
        .stderr(Stdio::null());
    ProcGuard::spawn("engine", &mut cmd)
}

fn spawn_pg_writer(aeron_dir: &str, control: &str) -> std::io::Result<ProcGuard> {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_pg-writer"));
    cmd.env("DATABASE_URL", pg_url())
        .env("AERON_DIR", aeron_dir)
        .env("EXCHANGE_ARCHIVE_CONTROL", control)
        .env("PG_RECONCILE_SECS", "0")
        .env("PG_WRITER_FLUSH_MS", "20")
        .env("RUST_LOG", "info")
        .stdout(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(format!("/tmp/chaos-trig-pgw-{}.log", std::process::id()))
                .map(Stdio::from)
                .unwrap_or(Stdio::null()),
        )
        .stderr(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(format!("/tmp/chaos-trig-pgw-{}.log", std::process::id()))
                .map(Stdio::from)
                .unwrap_or(Stdio::null()),
        );
    ProcGuard::spawn("pg-writer", &mut cmd)
}

fn spawn_desk(aeron_dir: &str, control: &str) -> std::io::Result<ProcGuard> {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_desk-server"));
    cmd.env("DATABASE_URL", pg_url())
        .env(
            "REDIS_URL",
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/0".into()),
        )
        .env("AERON_DIR", aeron_dir)
        .env("EXCHANGE_ARCHIVE_CONTROL", control)
        .env("SYMBOLS", SYMBOL)
        .env("DESK_PORT", DESK_PORT.to_string())
        .env("DESK_ID", "0")
        .env("EXCHANGE_JWT_SECRET", JWT)
        .env("DESK_PUBLIC_MARKET_DATA", "1")
        .env("RUST_LOG", "info")
        .stdout(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(format!("/tmp/chaos-trig-desk-{}.log", std::process::id()))
                .map(Stdio::from)
                .unwrap_or(Stdio::null()),
        )
        .stderr(Stdio::null());
    ProcGuard::spawn("desk-server", &mut cmd)
}

fn base() -> String {
    format!("http://127.0.0.1:{DESK_PORT}")
}

async fn wait_rest_up(http: &reqwest::Client) {
    let deadline = std::time::Instant::now() + Duration::from_secs(40);
    loop {
        if let Ok(r) = http.get(format!("{}/api/tickers", base())).send().await {
            if r.status().is_success() {
                return;
            }
        }
        assert!(std::time::Instant::now() < deadline, "desk REST did not come up");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn register_login(http: &reqwest::Client, email: &str) -> String {
    let _ = http
        .post(format!("{}/api/auth/register", base()))
        .json(&json!({"email": email, "password": "chaos_pw_123456"}))
        .send()
        .await
        .expect("register");
    let resp: Value = http
        .post(format!("{}/api/auth/login", base()))
        .json(&json!({"email": email, "password": "chaos_pw_123456"}))
        .send()
        .await
        .expect("login")
        .json()
        .await
        .expect("login json");
    resp["token"].as_str().expect("token").to_string()
}

async fn place(http: &reqwest::Client, token: &str, side: &str, price: f64, qty: f64) {
    let r = http
        .post(format!("{}/api/orders", base()))
        .bearer_auth(token)
        .json(&json!({
            "symbol": SYMBOL, "side": side, "order_type": "limit",
            "price": price, "quantity": qty,
        }))
        .send()
        .await
        .expect("place");
    assert!(r.status().is_success(), "order rejected: {}", r.status());
}

async fn cancel_all(http: &reqwest::Client, token: &str) {
    let _ = http
        .delete(format!("{}/api/orders", base()))
        .bearer_auth(token)
        .send()
        .await;
}

async fn trigger_status(pg: &sqlx::PgPool, id: i64) -> String {
    sqlx::query_scalar("SELECT status FROM trigger_orders WHERE id = $1")
        .bind(id)
        .fetch_one(pg)
        .await
        .expect("trigger status")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn triggers_fire_on_mark_cross_and_survive_kill9() {
    let Some(jar) = jar_path() else {
        eprintln!("skip: aeron-all jar not found");
        return;
    };
    let Some(pg) = db::create_pool_sized(&pg_url(), 4).await.ok() else {
        eprintln!("skip: no PG");
        return;
    };
    db::run_migrations(&pg).await.expect("migrations");
    let Ok(driver) = start_archiving_driver(&jar, CONTROL_PORT) else {
        eprintln!("skip: cannot start ArchivingMediaDriver");
        return;
    };
    let aeron_dir = driver.aeron_dir.clone();
    let control = driver.control_channel.clone();
    let _drill_guard = common::DrillGuard::acquire();
    // Clean residue on our symbol.
    for sql in [
        "DELETE FROM trigger_orders WHERE symbol = $1",
        "DELETE FROM positions WHERE symbol = $1",
        "DELETE FROM funding_state WHERE symbol = $1",
        "DELETE FROM funding_history WHERE symbol = $1",
    ] {
        let _ = sqlx::query(sql).bind(SYMBOL).execute(&pg).await;
    }

    let _engine = spawn_engine(&aeron_dir, &control).expect("engine");
    let _pgw = spawn_pg_writer(&aeron_dir, &control).expect("pg-writer");
    let mut desk = spawn_desk(&aeron_dir, &control).expect("desk");
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    wait_rest_up(&http).await;

    // Two shard-0 users: a market mover and the trigger owner.
    let run = Uuid::new_v4().simple().to_string();
    let mut owned: Vec<(String, String)> = Vec::new();
    for i in 0..16 {
        let email = format!("chaos_trig_{run}_{i}@lightning.test");
        let token = register_login(&http, &email).await;
        let uid: i64 = sqlx::query_scalar("SELECT id FROM users WHERE email = $1")
            .bind(&email)
            .fetch_one(&pg)
            .await
            .expect("uid");
        if uid % 4 == 0 {
            owned.push((email, token));
            if owned.len() == 2 {
                break;
            }
        }
    }
    assert_eq!(owned.len(), 2, "need two shard-0 users");
    let (_, mover) = owned.remove(0);
    let (owner_email, owner) = owned.remove(0);
    let owner_uid: i64 = sqlx::query_scalar("SELECT id FROM users WHERE email = $1")
        .bind(&owner_email)
        .fetch_one(&pg)
        .await
        .expect("uid");

    // ── Build a market: resting bid+ask → depth → mark ≈ mid ≈ 50_000 ──
    place(&http, &mover, "buy", 49_900.0, 0.05).await;
    place(&http, &mover, "sell", 50_100.0, 0.05).await;
    tokio::time::sleep(Duration::from_millis(1_500)).await; // EWMA converges

    // ── T1: sell-stop already satisfied (falling, trigger ABOVE mark) ──
    let t1: Value = http
        .post(format!("{}/api/trigger-orders", base()))
        .bearer_auth(&owner)
        .json(&json!({
            "symbol": SYMBOL, "side": "sell", "order_type": "limit",
            "trigger_price": 60_000.0, "price": 49_850.0, "quantity": 0.01,
        }))
        .send()
        .await
        .expect("place t1")
        .json()
        .await
        .expect("t1 json");
    let t1_id = t1["id"].as_i64().expect("t1 id");

    // Fires within a few polling ticks; the injected IOC sells into the
    // resting bid → a trade with the owner as a participant.
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        if trigger_status(&pg, t1_id).await == "triggered" {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "T1 never fired");
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    let injected: i64 = sqlx::query_scalar(
        "SELECT triggered_order_id FROM trigger_orders WHERE id = $1",
    )
    .bind(t1_id)
    .fetch_one(&pg)
    .await
    .expect("injected id");
    // The swap-native trade footprint: the owner's SHORT position lands
    // in PG via the margin-frame path (maker/taker on_fill → frames →
    // pg-writer). (trades-table rows in the split topology have a known
    // separate gap — tracked in the checklist, not an S5 concern.)
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let pos: Option<(String, i64)> = sqlx::query_as(
            "SELECT side, qty_lots FROM positions WHERE user_id = $1 AND symbol = $2",
        )
        .bind(owner_uid)
        .bind(SYMBOL)
        .fetch_optional(&pg)
        .await
        .expect("pos");
        if pos == Some(("short".into(), 10_000)) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "T1's injected order {injected} never opened the short position: {pos:?}"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    eprintln!("chaos-trig: T1 fired, TRADED, short position durable (order {injected})");

    // ── T2: deep sell-stop stays pending, then survives kill -9 ────────
    let t2: Value = http
        .post(format!("{}/api/trigger-orders", base()))
        .bearer_auth(&owner)
        .json(&json!({
            "symbol": SYMBOL, "side": "sell", "order_type": "market",
            "trigger_price": 46_000.0, "quantity": 0.01,
        }))
        .send()
        .await
        .expect("place t2")
        .json()
        .await
        .expect("t2 json");
    let t2_id = t2["id"].as_i64().expect("t2 id");
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(trigger_status(&pg, t2_id).await, "pending", "deep stop stays armed");

    desk.kill9();
    tokio::time::sleep(Duration::from_millis(300)).await;
    desk = spawn_desk(&aeron_dir, &control).expect("respawn desk");
    wait_rest_up(&http).await;
    assert_eq!(
        trigger_status(&pg, t2_id).await,
        "pending",
        "pending trigger survives the crash"
    );
    eprintln!("chaos-trig: T2 pending across kill -9 — walking the market down");

    // Walk the mark down through 46_000: replace the market 45k/45.2k.
    cancel_all(&http, &mover).await;
    place(&http, &mover, "buy", 45_000.0, 0.05).await;
    place(&http, &mover, "sell", 45_200.0, 0.05).await;

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if trigger_status(&pg, t2_id).await == "triggered" {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "T2 never fired after restart + market walk-down"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let pos: Option<(String, i64)> = sqlx::query_as(
            "SELECT side, qty_lots FROM positions WHERE user_id = $1 AND symbol = $2",
        )
        .bind(owner_uid)
        .bind(SYMBOL)
        .fetch_optional(&pg)
        .await
        .expect("pos");
        if pos == Some(("short".into(), 20_000)) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "post-restart fire must grow the short to 0.02: {pos:?}"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    // The owner ends with a SHORT position from the two fired stops.
    let pos: Option<(String, i64)> = sqlx::query_as(
        "SELECT side, qty_lots FROM positions WHERE user_id = $1 AND symbol = $2",
    )
    .bind(owner_uid)
    .bind(SYMBOL)
    .fetch_optional(&pg)
    .await
    .expect("owner position");
    eprintln!("CHAOS-TRIG PASS: T1 fired+traded, T2 survived kill -9 and fired+traded (owner pos: {pos:?})");

    // Test hygiene: drop this run's accounts so account_reconcile's
    // top-100 hanging-frozen check isn't crowded out by drill residue.
    let _ = sqlx::query(
        "DELETE FROM positions WHERE user_id IN (SELECT id FROM users WHERE email LIKE $1)",
    )
    .bind(format!("chaos_trig_{run}_%"))
    .execute(&pg)
    .await;
    let _ = sqlx::query(
        "DELETE FROM accounts WHERE user_id IN (SELECT id FROM users WHERE email LIKE $1)",
    )
    .bind(format!("chaos_trig_{run}_%"))
    .execute(&pg)
    .await;
    let _ = sqlx::query(
        "DELETE FROM orders WHERE user_id IN (SELECT id FROM users WHERE email LIKE $1)",
    )
    .bind(format!("chaos_trig_{run}_%"))
    .execute(&pg)
    .await;
    // D4: also clean up trigger_orders rows created by this drill run so that
    // account_reconcile and recovery-reinjection scans are not crowded out by
    // drill residue across runs.
    let _ = sqlx::query(
        "DELETE FROM trigger_orders WHERE user_id IN (SELECT id FROM users WHERE email LIKE $1)",
    )
    .bind(format!("chaos_trig_{run}_%"))
    .execute(&pg)
    .await;
    drop(desk);
}
