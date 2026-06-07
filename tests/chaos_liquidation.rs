//! S6.4 — liquidation chaos against the REAL topology: tiered (50%)
//! liquidation in two rounds, with `kill -9` of the desk BETWEEN the
//! rounds. The reborn desk must hydrate the half-liquidated position,
//! re-detect the breach, and finish the job — without double-closing.
//!
//! Mark pressure is applied through the TEST_ENDPOINTS debug hook
//! (forces the engine's mark directly, below any book/clamp influence).

mod common;

use std::process::{Command, Stdio};
use std::time::Duration;

use common::*;
use lightning_exchange::db;
use serde_json::{Value, json};
use uuid::Uuid;

const CONTROL_PORT: u16 = 19020;
const DESK_PORT: u16 = 14079;
const SYMBOL: &str = "BTC_USDT";
const JWT: &str = "chaos-liquidation-secret-0123456789abcdef0123456789abcdef0000";

fn spawn_engine(aeron_dir: &str, control: &str) -> std::io::Result<ProcGuard> {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_exchange-engine"));
    cmd.env("DATABASE_URL", pg_url())
        .env("AERON_DIR", aeron_dir)
        .env("SYMBOLS", SYMBOL)
        .env("EXCHANGE_ARCHIVE_CONTROL", control)
        .env("ENGINE_IDLE_SPINS", "1000")
        .env("RUST_LOG", "warn")
        .stdout(Stdio::null())
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
        .env("RUST_LOG", "warn")
        .stdout(Stdio::null())
        .stderr(Stdio::null());
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
        .env("TEST_ENDPOINTS", "1")
        .env("LIQ_TRANCHE_BPS", "5000") // S6.1: 50% per round
        .env("RUST_LOG", "info")
        .stdout(Stdio::null())
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
        assert!(std::time::Instant::now() < deadline, "desk REST never came up");
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

async fn force_mark(http: &reqwest::Client, price: f64) {
    let r = http
        .post(format!("{}/api/debug/mark-price", base()))
        .json(&json!({"symbol": SYMBOL, "price": price}))
        .send()
        .await
        .expect("force mark");
    assert!(r.status().is_success(), "debug mark rejected: {}", r.status());
}

async fn victim_qty(pg: &sqlx::PgPool, uid: i64) -> Option<i64> {
    sqlx::query_scalar(
        "SELECT qty_lots FROM positions WHERE user_id = $1 AND symbol = $2",
    )
    .bind(uid)
    .bind(SYMBOL)
    .fetch_optional(pg)
    .await
    .expect("qty")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiered_liquidation_survives_kill9_between_rounds() {
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
    for sql in [
        "DELETE FROM positions WHERE symbol = $1",
        "DELETE FROM trigger_orders WHERE symbol = $1",
        "DELETE FROM funding_state WHERE symbol = $1",
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

    // Shard-0 users: 4 liquidity movers + the victim.
    let run = Uuid::new_v4().simple().to_string();
    let mut owned: Vec<(String, String)> = Vec::new();
    for i in 0..40 {
        let email = format!("chaos_liq_{run}_{i}@lightning.test");
        let token = register_login(&http, &email).await;
        let uid: i64 = sqlx::query_scalar("SELECT id FROM users WHERE email = $1")
            .bind(&email)
            .fetch_one(&pg)
            .await
            .expect("uid");
        if uid % 4 == 0 {
            owned.push((email, token));
            if owned.len() == 5 {
                break;
            }
        }
    }
    assert_eq!(owned.len(), 5);
    let movers: Vec<(String, String)> = owned.drain(..4).collect();
    let (victim_email, victim) = owned.remove(0);
    let victim_uid: i64 = sqlx::query_scalar("SELECT id FROM users WHERE email = $1")
        .bind(&victim_email)
        .fetch_one(&pg)
        .await
        .expect("uid");

    // Liquidity: each mover asks 0.25 @ $50k (victim lifts 1.0 total at
    // 10x MARGIN via a trigger-market order — the spot-style freeze path
    // cannot build a liquidatable position: max loss ≤ full collateral)
    // and bids 0.15 @ $49k (liquidation exit liquidity, Σ 0.6).
    for (_, m) in &movers {
        place(&http, m, "sell", 50_000.0, 0.25).await;
        place(&http, m, "buy", 49_000.0, 0.15).await;
    }
    tokio::time::sleep(Duration::from_millis(1_500)).await; // mark forms
    let r = http
        .post(format!("{}/api/trigger-orders", base()))
        .bearer_auth(&victim)
        .json(&json!({
            "symbol": SYMBOL, "side": "buy", "order_type": "market",
            "trigger_price": 49_500.0, "trigger_when": "rising", "quantity": 1.0,
        }))
        .send()
        .await
        .expect("victim trigger");
    assert!(r.status().is_success(), "victim trigger rejected");

    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        if victim_qty(&pg, victim_uid).await == Some(1_000_000) {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "victim 1 BTC long never landed");
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    eprintln!("chaos-liq: victim long 1 BTC at 10x durable — squeezing");

    // ── Round 1: force the mark so equity ∈ (0, maintenance] ───────────
    // equity ≈ $9,975 (10k − $25 taker fee on the 1-BTC entry), 1 BTC
    // long from $50k. At mark M: equity = M − 40025, maintenance =
    // 0.005·M → LiquidationPending window is M ∈ (40025, 40226]. Pick
    // $40,150 (equity ≈ $125 ≤ maintenance ≈ $201) — NOT Bankruptcy,
    // which would route to ADL instead of the tranche path under test.
    let mut reduced = None;
    for _ in 0..40 {
        force_mark(&http, 40_150.0).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        match victim_qty(&pg, victim_uid).await {
            Some(q) if q < 1_000_000 => {
                reduced = Some(q);
                break;
            }
            None => {
                reduced = Some(0);
                break;
            }
            _ => {}
        }
    }
    let after_r1 = reduced.expect("round 1 never reduced the position");
    assert!(
        after_r1 > 0 && after_r1 < 1_000_000,
        "round 1 must PARTIALLY close (tranche), got {after_r1}"
    );
    eprintln!("chaos-liq: round 1 reduced to {after_r1} lots — SIGKILL desk between rounds");

    // ── kill -9 between rounds ──────────────────────────────────────────
    desk.kill9();
    tokio::time::sleep(Duration::from_millis(300)).await;
    desk = spawn_desk(&aeron_dir, &control).expect("respawn desk");
    wait_rest_up(&http).await;
    assert_eq!(
        victim_qty(&pg, victim_uid).await,
        Some(after_r1),
        "partially-liquidated position survives the crash"
    );

    // ── Round 2: the reborn desk re-detects and finishes ───────────────
    // Each tranche RESCUES the account (that's the point of tiering), so
    // finishing the job requires progressively deeper marks — walk the
    // price down 7% per probe until flat.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut last = after_r1;
    let mut px = 40_150.0_f64;
    loop {
        px *= 0.97; // gentler walk: each tranche rescues, deeper marks finish
        force_mark(&http, px).await;
        tokio::time::sleep(Duration::from_millis(250)).await;
        match victim_qty(&pg, victim_uid).await {
            None => break, // flat
            Some(q) => {
                assert!(q <= last, "position must only shrink, {last} → {q}");
                last = q;
                assert!(
                    std::time::Instant::now() < deadline,
                    "liquidation rounds never finished, still {q} lots"
                );
            }
        }
    }
    // No over-liquidation: the victim is flat, not flipped.
    assert_eq!(victim_qty(&pg, victim_uid).await, None);
    eprintln!("CHAOS-LIQ PASS: 50% tranche → kill -9 → hydrate → final tranche, flat, no double-close");

    // Test hygiene: drop this run's accounts so account_reconcile's
    // top-100 hanging-frozen check isn't crowded out by drill residue.
    let _ = sqlx::query(
        "DELETE FROM positions WHERE user_id IN (SELECT id FROM users WHERE email LIKE $1)",
    )
    .bind(format!("chaos_liq_{run}_%"))
    .execute(&pg)
    .await;
    let _ = sqlx::query(
        "DELETE FROM accounts WHERE user_id IN (SELECT id FROM users WHERE email LIKE $1)",
    )
    .bind(format!("chaos_liq_{run}_%"))
    .execute(&pg)
    .await;
    let _ = sqlx::query(
        "DELETE FROM orders WHERE user_id IN (SELECT id FROM users WHERE email LIKE $1)",
    )
    .bind(format!("chaos_liq_{run}_%"))
    .execute(&pg)
    .await;
    drop(desk);
}
