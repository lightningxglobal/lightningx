//! T2 — runtime ops console: halt rejects new orders at the REST edge,
//! resume restores them, fee override persists and survives a desk
//! restart (hydrate). Lighter than the full kill-9 drills: one desk +
//! engine + pg-writer, REST only, asserts the admin path end to end.

mod common;

use std::process::{Command, Stdio};
use std::time::Duration;

use common::*;
use lightning_exchange::db;
use serde_json::{Value, json};
use uuid::Uuid;

const CONTROL_PORT: u16 = 19040;
const DESK_PORT: u16 = 14080;
const SYMBOL: &str = "BTC_USDT";
const JWT: &str = "admin-ops-console-secret-0123456789abcdef0123456789abcdef0000";
const ADMIN: &str = "admin-ops-console-token-0123456789abcdef0123456789abcdef00000";

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
        .env("EXCHANGE_ADMIN_TOKEN", ADMIN)
        .env("RUST_LOG", "warn")
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
        .json(&json!({"email": email, "password": "ops_pw_123456"}))
        .send()
        .await;
    let resp: Value = http
        .post(format!("{}/api/auth/login", base()))
        .json(&json!({"email": email, "password": "ops_pw_123456"}))
        .send()
        .await
        .expect("login")
        .json()
        .await
        .expect("login json");
    resp["token"].as_str().expect("token").to_string()
}

async fn place_status(http: &reqwest::Client, token: &str) -> reqwest::StatusCode {
    http.post(format!("{}/api/orders", base()))
        .bearer_auth(token)
        .json(&json!({
            "symbol": SYMBOL, "side": "buy", "order_type": "limit",
            "price": 40_000.0, "quantity": 0.001,
        }))
        .send()
        .await
        .expect("place")
        .status()
}

async fn set_config(http: &reqwest::Client, body: Value) -> reqwest::StatusCode {
    http.post(format!("{}/api/admin/config", base()))
        .bearer_auth(ADMIN)
        .json(&body)
        .send()
        .await
        .expect("set config")
        .status()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn halt_resume_and_fee_override_persist() {
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
    let _ = sqlx::query("DELETE FROM exchange_config WHERE symbol = $1")
        .bind(SYMBOL)
        .execute(&pg)
        .await;

    let _engine = spawn_engine(&aeron_dir, &control).expect("engine");
    let _pgw = spawn_pg_writer(&aeron_dir, &control).expect("pg-writer");
    let mut desk = spawn_desk(&aeron_dir, &control).expect("desk");
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    wait_rest_up(&http).await;

    // Shard-0 user.
    let run = Uuid::new_v4().simple().to_string();
    let mut token = None;
    for i in 0..16 {
        let email = format!("ops_{run}_{i}@lightning.test");
        let t = register_login(&http, &email).await;
        let uid: i64 = sqlx::query_scalar("SELECT id FROM users WHERE email = $1")
            .bind(&email)
            .fetch_one(&pg)
            .await
            .expect("uid");
        if uid % 4 == 0 {
            token = Some(t);
            break;
        }
    }
    let token = token.expect("shard-0 user");

    // Baseline: order accepted.
    assert!(place_status(&http, &token).await.is_success(), "order ok before halt");

    // Admin without token → forbidden.
    let unauth = http
        .post(format!("{}/api/admin/config", base()))
        .json(&json!({"symbol": SYMBOL, "trading_halted": true}))
        .send()
        .await
        .expect("unauth")
        .status();
    assert_eq!(unauth, reqwest::StatusCode::FORBIDDEN, "admin needs the token");

    // Halt → order rejected at the REST edge.
    assert!(set_config(&http, json!({"symbol": SYMBOL, "trading_halted": true})).await.is_success());
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        place_status(&http, &token).await,
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "halted symbol must reject new orders"
    );

    // Fee override + resume in one call.
    assert!(
        set_config(
            &http,
            json!({"symbol": SYMBOL, "trading_halted": false, "taker_fee_bps": 25})
        )
        .await
        .is_success()
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(place_status(&http, &token).await.is_success(), "resume restores orders");

    // Override persisted to PG.
    let taker: Option<i64> =
        sqlx::query_scalar("SELECT taker_fee_bps FROM exchange_config WHERE symbol = $1")
            .bind(SYMBOL)
            .fetch_optional(&pg)
            .await
            .expect("config row");
    assert_eq!(taker, Some(25), "fee override durable");

    // ── Restart: the desk must hydrate the config (halt again first) ───
    assert!(set_config(&http, json!({"symbol": SYMBOL, "trading_halted": true})).await.is_success());
    tokio::time::sleep(Duration::from_millis(200)).await;
    desk.kill9();
    tokio::time::sleep(Duration::from_millis(300)).await;
    desk = spawn_desk(&aeron_dir, &control).expect("respawn");
    wait_rest_up(&http).await;
    // The reborn desk hydrated trading_halted=true → still rejects.
    assert_eq!(
        place_status(&http, &token).await,
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "halt survives desk restart via hydrate"
    );

    eprintln!("ADMIN-OPS PASS: halt/resume gates orders, fee override durable, halt survives restart");
    let _ = sqlx::query("DELETE FROM exchange_config WHERE symbol = $1")
        .bind(SYMBOL)
        .execute(&pg)
        .await;
    let _ = sqlx::query(
        "DELETE FROM accounts WHERE user_id IN (SELECT id FROM users WHERE email LIKE $1)",
    )
    .bind(format!("ops_{run}_%"))
    .execute(&pg)
    .await;
    drop(desk);
}
