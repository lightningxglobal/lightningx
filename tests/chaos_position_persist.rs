//! S1.6 chaos drill — REAL full topology, positions survive `kill -9`.
//!
//! driver+archive → exchange-engine → pg-writer → desk-server (REST).
//! Two users open opposite 0.01 BTC positions through the actual HTTP
//! API; the desk is SIGKILLed and restarted; the reborn desk must serve
//! the SAME positions from its hydrated in-memory engine (/api/positions
//! is the all-in-memory path — it cannot fake it from PG), and the users
//! must be able to CLOSE the pre-crash positions through the new process.
//!
//! Skips when java/jar or PG is unavailable.

mod common;

use std::process::{Command, Stdio};
use std::time::Duration;

use common::*;
use lightning_exchange::db;
use serde_json::{Value, json};
use sqlx::Row;
use uuid::Uuid;

const CONTROL_PORT: u16 = 18999;
const DESK_PORT: u16 = 14077;
const SYMBOL: &str = "BTC_USDT";
// 0.01 BTC at quantity_step 1e-6 = 10_000 lots.
const QTY: f64 = 0.01;
const QTY_LOTS: i64 = 10_000;
const PRICE: f64 = 50_000.0;
const JWT: &str = "chaos-position-persist-secret-0123456789abcdef0123456789abcdef";

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
        .env("RUST_LOG", "info")
        .stdout(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(format!("/tmp/chaos-pgw-{}.log", std::process::id()))
                .map(Stdio::from)
                .unwrap_or(Stdio::null()),
        )
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
        // The trade-stream consumer (settlement + MAKER margin updates)
        // lives behind this flag — without it makers never settle.
        .env("DESK_PUBLIC_MARKET_DATA", "1")
        // S3: fast funding so the drill sees a real settlement land.
        .env("FUNDING_PERIOD_SECS", "2")
        .env("FUNDING_SAMPLE_SECS", "1")
        .env("RUST_LOG", "info")
        .stdout(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(format!("/tmp/chaos-desk-{}.log", std::process::id()))
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
        if let Ok(resp) = http.get(format!("{}/api/tickers", base())).send().await {
            if resp.status().is_success() {
                return;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "desk REST did not come up"
        );
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

async fn place_limit(http: &reqwest::Client, token: &str, side: &str, qty: f64) {
    let resp = http
        .post(format!("{}/api/orders", base()))
        .bearer_auth(token)
        .json(&json!({
            "symbol": SYMBOL,
            "side": side,
            "order_type": "limit",
            "price": PRICE,
            "quantity": qty,
        }))
        .send()
        .await
        .expect("place order");
    assert!(
        resp.status().is_success(),
        "order rejected: {}",
        resp.text().await.unwrap_or_default()
    );
}

/// (side, qty_lots) of the user's position row, if any.
async fn pg_position(pg: &sqlx::PgPool, user_email: &str) -> Option<(String, i64)> {
    sqlx::query(
        "SELECT p.side, p.qty_lots FROM positions p
           JOIN users u ON u.id = p.user_id
          WHERE u.email = $1 AND p.symbol = $2",
    )
    .bind(user_email)
    .bind(SYMBOL)
    .fetch_optional(pg)
    .await
    .expect("pg position")
    .map(|r| (r.get("side"), r.get("qty_lots")))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn positions_survive_desk_kill9_and_remain_closable() {
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

    // Residue from earlier runs of THIS suite (positions/funding rows on
    // our symbol) would distort funding totals and the orphan guard —
    // start clean. Other suites use isolated symbols.
    for sql in [
        "DELETE FROM positions WHERE symbol = $1",
        "DELETE FROM funding_history WHERE symbol = $1",
        "DELETE FROM funding_state WHERE symbol = $1",
    ] {
        let _ = sqlx::query(sql).bind(SYMBOL).execute(&pg).await;
    }

    // Full topology, production start order.
    let _engine = spawn_engine(&aeron_dir, &control).expect("engine");
    let _pgw = spawn_pg_writer(&aeron_dir, &control).expect("pg-writer");
    let mut desk = spawn_desk(&aeron_dir, &control).expect("desk");

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    wait_rest_up(&http).await;

    // Desk 0 only serves users with id % 4 == 0 (counter_shard routing).
    // Register until two of OUR freshly-minted ids land on shard 0.
    let run = Uuid::new_v4().simple().to_string();
    let mut owned: Vec<(String, String)> = Vec::new(); // (email, token)
    for i in 0..16 {
        let email = format!("chaos_pos_{run}_{i}@lightning.test");
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
    assert_eq!(owned.len(), 2, "needed two shard-0 users out of 16 registrations");
    let (buyer_email, buyer) = owned.remove(0);
    let (seller_email, seller) = owned.remove(0);
    // Registration already seeds 10,000 USDT (user_service::register) —
    // ample margin for two 0.01 BTC positions at $50k and 10x.

    // ── Open opposite positions through the real fill pipeline ─────────
    place_limit(&http, &seller, "sell", QTY).await; // rests
    place_limit(&http, &buyer, "buy", QTY).await; // crosses → trade

    // Positions land in PG via desk → persist frames → pg-writer.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let b = pg_position(&pg, &buyer_email).await;
        let s = pg_position(&pg, &seller_email).await;
        if b == Some(("long".into(), QTY_LOTS)) && s == Some(("short".into(), QTY_LOTS)) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "positions never landed in PG: buyer={b:?} seller={s:?}"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    eprintln!("chaos-pos: positions durable in PG");

    // ── S3 end-to-end: a real funding settlement lands through the
    //    desk task → frame → pg-writer single-tx path. With premium 0
    //    (placeholder index) the rate is the interest term: longs pay. ──
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let row: Option<(i64, i64, i64)> = sqlx::query_as(
            "SELECT long_paid_atoms, short_received_atoms, fund_residue_atoms
               FROM funding_history
              WHERE symbol = $1 AND long_paid_atoms > 0
              ORDER BY id DESC LIMIT 1",
        )
        .bind(SYMBOL)
        .fetch_optional(&pg)
        .await
        .expect("funding history");
        if let Some((paid, recv, residue)) = row {
            assert_eq!(
                paid,
                recv + residue,
                "funding conservation: paid == received + fund residue"
            );
            eprintln!(
                "chaos-pos: funding settled for real (paid={paid}, recv={recv}, residue={residue})"
            );
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "no funding settlement with open positions within 20s"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    // The /api/funding view is live.
    let funding: Value = http
        .get(format!("{}/api/funding?symbol={SYMBOL}", base()))
        .send()
        .await
        .expect("funding api")
        .json()
        .await
        .expect("funding json");
    assert!(
        funding.to_string().contains(SYMBOL),
        "funding API must expose the symbol: {funding}"
    );
    eprintln!("chaos-pos: /api/funding live — SIGKILL desk");

    // ── THE KILL ─────────────────────────────────────────────────────────
    desk.kill9();
    tokio::time::sleep(Duration::from_millis(300)).await;
    desk = spawn_desk(&aeron_dir, &control).expect("respawn desk");
    wait_rest_up(&http).await;

    // PG rows untouched by the restart.
    assert_eq!(
        pg_position(&pg, &buyer_email).await,
        Some(("long".into(), QTY_LOTS)),
        "buyer position must survive in PG"
    );

    // The reborn desk serves the position from MEMORY (/api/positions is
    // the all-in-memory path) — this is hydrate working, not a PG echo.
    let positions: Value = http
        .get(format!("{}/api/positions", base()))
        .bearer_auth(&buyer)
        .send()
        .await
        .expect("positions")
        .json()
        .await
        .expect("positions json");
    let text = positions.to_string();
    assert!(
        text.contains(SYMBOL),
        "hydrated desk must serve the pre-crash position from memory: {text}"
    );
    eprintln!("chaos-pos: reborn desk serves the position from hydrated memory");

    // ── Seamlessness: CLOSE the pre-crash positions via the new process ─
    place_limit(&http, &buyer, "sell", QTY).await; // buyer closes long (rests)
    place_limit(&http, &seller, "buy", QTY).await; // seller closes short (crosses)

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let b = pg_position(&pg, &buyer_email).await;
        let s = pg_position(&pg, &seller_email).await;
        if b.is_none() && s.is_none() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "positions must close to flat after restart: buyer={b:?} seller={s:?}"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    eprintln!("CHAOS-POS PASS: positions survived kill -9 and closed through the reborn desk");

    // Test hygiene: drop this run's accounts so account_reconcile's
    // top-100 hanging-frozen check isn't crowded out by drill residue.
    let _ = sqlx::query(
        "DELETE FROM positions WHERE user_id IN (SELECT id FROM users WHERE email LIKE $1)",
    )
    .bind(format!("chaos_pos_{run}_%"))
    .execute(&pg)
    .await;
    let _ = sqlx::query(
        "DELETE FROM accounts WHERE user_id IN (SELECT id FROM users WHERE email LIKE $1)",
    )
    .bind(format!("chaos_pos_{run}_%"))
    .execute(&pg)
    .await;
    let _ = sqlx::query(
        "DELETE FROM orders WHERE user_id IN (SELECT id FROM users WHERE email LIKE $1)",
    )
    .bind(format!("chaos_pos_{run}_%"))
    .execute(&pg)
    .await;
    drop(desk);
}
