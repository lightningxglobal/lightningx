//! Gate 2, three-way reconciliation clause — BOTH writers (pg-writer and
//! redis-writer) SIGKILLed in rotation under a mixed trade+account frame
//! stream; afterwards PG, Redis and the Archive journal must agree:
//!
//!   1. PG has EXACTLY all trades (none lost, none doubled);
//!   2. PG accounts equal the last AccountSet value per user;
//!   3. Redis accounts equal the same values (PG ↔ Redis diff = 0);
//!   4. the journal-audit binary (journal ↔ checkpoints ↔ PG) exits 0.
//!
//! redis-writer recovery relies on its journal catch-up (added together
//! with this drill — it previously had no replay path and lost frames
//! missed while down).
//!
//! S1.7: the stream also carries swap-margin frames (PositionUpsert /
//! RiskAccountSet) so the kill rotation proves the POSITION tables
//! converge exactly like trades/accounts do.

mod common;

use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use aeron_wrapper::AeronClient;
use common::*;
use lightning_exchange::db;
use lightning_exchange::desk::pg_store::load_checkpoints;
use lightning_exchange::transport::aeron_channels::{PERSIST_CHANNEL, PERSIST_STREAM};
use lightning_exchange::transport::aeron_transport::PersistPublisher;
use lightning_exchange::transport::journal::JournalRecorder;
use lightning_exchange::transport::persist_event::{
    AccountSetPayload, PersistFrame, PositionUpsertPayload, RiskAccountSetPayload,
    TradeInsertPayload, pack_str,
};

const CONTROL_PORT: u16 = 18980;
const PUBLISHER_ID: u16 = 62_002;
const ORDER_ID_BASE: i64 = 970_000_000;
const TOTAL_FRAMES: u64 = 2_400;
const ACCOUNT_EVERY: u64 = 40; // every 40th frame is an AccountSet
const POSITION_EVERY: u64 = 60; // every 60th frame is a PositionUpsert
const RISK_EVERY: u64 = 90; // every 90th frame is a RiskAccountSet
const ASSET: &str = "USDT";
const POS_SYMBOL: &str = "W3CHAOS"; // isolated from other suites

fn trade_frame(seq: u64) -> PersistFrame {
    let mut f = PersistFrame::trade_insert(TradeInsertPayload {
        buy_order_id: ORDER_ID_BASE + seq as i64,
        sell_order_id: ORDER_ID_BASE + seq as i64 + 10_000_000,
        symbol: pack_str("BTC_USDT"),
        price: 100.0,
        qty: 0.001,
        ts_ms: 1_700_000_000_000 + seq as i64,
    });
    f.publisher_id = PUBLISHER_ID;
    f.seq = seq;
    f
}

fn account_frame(seq: u64, user_id: i64) -> PersistFrame {
    let atoms = (seq as i64) * 1_000; // strictly increasing → last wins
    let mut f = PersistFrame::account_set(AccountSetPayload {
        user_id,
        asset: pack_str(ASSET),
        balance: atoms as f64 / 100_000_000.0,
        frozen: 0.0,
        balance_atoms: atoms,
        frozen_atoms: 0,
    });
    f.publisher_id = PUBLISHER_ID;
    f.seq = seq;
    f
}

fn position_frame(seq: u64, user_id: i64) -> PersistFrame {
    let mut f = PersistFrame::position_upsert(PositionUpsertPayload {
        user_id,
        symbol: pack_str(POS_SYMBOL),
        side: 0,
        leverage: 10,
        _pad: [0; 6],
        qty_lots: seq as i64, // strictly increasing → last wins
        entry_price_ticks: 5_000_000,
        used_margin_atoms: seq as i64 * 100,
        cost_atoms: seq as i64 * 1_000,
    });
    f.publisher_id = PUBLISHER_ID;
    f.seq = seq;
    f
}

fn risk_frame(seq: u64, user_id: i64) -> PersistFrame {
    let mut f = PersistFrame::risk_account_set(RiskAccountSetPayload {
        user_id,
        equity_atoms: seq as i64 * 1_000,
        used_margin_atoms: seq as i64 * 100,
        order_margin_atoms: 0,
        status: 0,
        _pad: [0; 7],
    });
    f.publisher_id = PUBLISHER_ID;
    f.seq = seq;
    f
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

fn spawn_redis_writer(aeron_dir: &str, control: &str) -> std::io::Result<ProcGuard> {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_redis-writer"));
    cmd.env("DATABASE_URL", pg_url())
        .env(
            "REDIS_URL",
            std::env::var("CHAOS_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/7".into()),
        )
        .env("AERON_DIR", aeron_dir)
        .env("EXCHANGE_ARCHIVE_CONTROL", control)
        .env("RUST_LOG", "warn")
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    ProcGuard::spawn("redis-writer", &mut cmd)
}

async fn make_users(pg: &sqlx::PgPool, n: usize) -> Vec<i64> {
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let email = format!("chaos3w_{}@lightning.test", uuid::Uuid::new_v4());
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO users (email, password_hash) VALUES ($1, $2) RETURNING id",
        )
        .bind(email)
        .bind("$argon2id$v=19$test$abcdefghijklmnopqrstuvwxyz0123456789")
        .fetch_one(pg)
        .await
        .expect("make user");
        out.push(id);
    }
    out
}

async fn cleanup(pg: &sqlx::PgPool, users: &[i64]) {
    let _ = sqlx::query("DELETE FROM persist_checkpoints WHERE publisher_id = $1")
        .bind(PUBLISHER_ID as i32)
        .execute(pg)
        .await;
    let _ = sqlx::query("DELETE FROM trades WHERE buy_order_id BETWEEN $1 AND $2")
        .bind(ORDER_ID_BASE)
        .bind(ORDER_ID_BASE + 20_000_000)
        .execute(pg)
        .await;
    let _ = sqlx::query("DELETE FROM positions WHERE symbol = $1")
        .bind(POS_SYMBOL)
        .execute(pg)
        .await;
    if !users.is_empty() {
        let _ = sqlx::query("DELETE FROM risk_accounts WHERE user_id = ANY($1::bigint[])")
            .bind(users)
            .execute(pg)
            .await;
        let _ = sqlx::query("DELETE FROM accounts WHERE user_id = ANY($1::bigint[])")
            .bind(users)
            .execute(pg)
            .await;
        let _ = sqlx::query("DELETE FROM users WHERE id = ANY($1::bigint[])")
            .bind(users)
            .execute(pg)
            .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn both_writers_killed_pg_redis_journal_agree() {
    let Some(jar) = jar_path() else {
        eprintln!("skip: aeron-all jar not found");
        return;
    };
    let Some(pg) = db::create_pool_sized(&pg_url(), 4).await.ok() else {
        eprintln!("skip: no PG");
        return;
    };
    db::run_migrations(&pg).await.expect("migrations");
    // DB 7: isolated from every other suite (they use db 0 and some of
    // them purge it mid-run — a shared-fixture race, not a system property).
    let redis_url =
        std::env::var("CHAOS_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/7".into());
    let Ok(redis_client) = redis::Client::open(redis_url.as_str()) else {
        eprintln!("skip: no Redis");
        return;
    };
    let Ok(mut redis_conn) = redis_client.get_multiplexed_async_connection().await else {
        eprintln!("skip: no Redis");
        return;
    };
    let Ok(driver) = start_archiving_driver(&jar, CONTROL_PORT) else {
        eprintln!("skip: cannot start ArchivingMediaDriver");
        return;
    };

    cleanup(&pg, &[]).await;
    // Symmetric cleanup on the Redis side: the drill reuses PUBLISHER_ID
    // with seq restarting at 1, which violates the production contract
    // (desk seqs are clock-seeded, monotonic across restarts). A leftover
    // floor from a previous run would silently reject every frame.
    let _: () = redis::cmd("HDEL")
        .arg("persist:floors")
        .arg(PUBLISHER_ID.to_string())
        .query_async(&mut redis_conn)
        .await
        .unwrap_or(());
    let users = make_users(&pg, 3).await;
    let kills: u32 = std::env::var("CHAOS_KILLS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    let aeron_dir = driver.aeron_dir.clone();
    let control = driver.control_channel.clone();

    // Frame kind per seq (mutually exclusive priority: position > risk >
    // account > trade) and expected last-write values per user.
    let kind_of = |seq: u64| -> u8 {
        if seq % POSITION_EVERY == 0 {
            1 // position
        } else if seq % RISK_EVERY == 0 {
            2 // risk account
        } else if seq % ACCOUNT_EVERY == 0 {
            3 // account
        } else {
            0 // trade
        }
    };
    let mut last_set: std::collections::HashMap<i64, i64> = Default::default();
    let mut last_pos: std::collections::HashMap<i64, i64> = Default::default();
    let mut last_risk: std::collections::HashMap<i64, i64> = Default::default();
    let mut trade_total = 0u64;
    for seq in 1..=TOTAL_FRAMES {
        let user = users[(seq as usize) % users.len()];
        match kind_of(seq) {
            1 => {
                last_pos.insert(user, seq as i64);
            }
            2 => {
                last_risk.insert(user, seq as i64 * 1_000);
            }
            3 => {
                last_set.insert(user, seq as i64 * 1_000);
            }
            _ => trade_total += 1,
        }
    }

    // ── Publisher thread ──────────────────────────────────────────────────
    let published = Arc::new(AtomicU64::new(0));
    let published_t = published.clone();
    let users_t = users.clone();
    let aeron_dir_pub = aeron_dir.clone();
    let control_pub = control.clone();
    let pub_thread = std::thread::spawn(move || {
        let client = wait_for("media driver", Duration::from_secs(20), || {
            AeronClient::new(&aeron_dir_pub).ok().map(Arc::new)
        });
        let cfg = aeron_wrapper::ArchiveConfig {
            control_request_channel: control_pub,
            ..aeron_wrapper::ArchiveConfig::default()
        };
        let _recorder = wait_for("archive connect", Duration::from_secs(20), || {
            JournalRecorder::start(&client, &cfg, PERSIST_CHANNEL, PERSIST_STREAM).ok()
        });
        let mut publisher = PersistPublisher::new(client.clone(), PERSIST_CHANNEL, PERSIST_STREAM)
            .expect("persist publisher");
        std::thread::sleep(Duration::from_millis(500));
        for seq in 1..=TOTAL_FRAMES {
            let user = users_t[(seq as usize) % users_t.len()];
            let f = if seq % POSITION_EVERY == 0 {
                position_frame(seq, user)
            } else if seq % RISK_EVERY == 0 {
                risk_frame(seq, user)
            } else if seq % ACCOUNT_EVERY == 0 {
                account_frame(seq, user)
            } else {
                trade_frame(seq)
            };
            wait_for("publish", Duration::from_secs(5), || {
                client.do_work();
                publisher.publish(&f).ok()
            });
            published_t.store(seq, Ordering::Relaxed);
            std::thread::sleep(Duration::from_millis(3));
        }
        std::thread::sleep(Duration::from_millis(500));
    });

    // ── Chaos: alternate kills between the two writers ────────────────────
    let mut rng = Lcg(0x3A11_C4A0_5C4A_05C4);
    let mut pgw = spawn_pg_writer(&aeron_dir, &control).expect("spawn pg-writer");
    let mut rdw = spawn_redis_writer(&aeron_dir, &control).expect("spawn redis-writer");
    for k in 1..=kills {
        std::thread::sleep(Duration::from_millis(rng.range(400, 1200)));
        if k % 2 == 1 {
            pgw.kill9();
            eprintln!(
                "chaos3w: kill #{k}/{kills} pg-writer (published={})",
                published.load(Ordering::Relaxed)
            );
            std::thread::sleep(Duration::from_millis(rng.range(100, 400)));
            pgw = spawn_pg_writer(&aeron_dir, &control).expect("respawn pg-writer");
        } else {
            rdw.kill9();
            eprintln!(
                "chaos3w: kill #{k}/{kills} redis-writer (published={})",
                published.load(Ordering::Relaxed)
            );
            std::thread::sleep(Duration::from_millis(rng.range(100, 400)));
            rdw = spawn_redis_writer(&aeron_dir, &control).expect("respawn redis-writer");
        }
    }
    pub_thread.join().expect("publisher thread");

    // ── Convergence: PG trades reach exactly trade_total ──────────────────
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM trades WHERE buy_order_id BETWEEN $1 AND $2")
                .bind(ORDER_ID_BASE)
                .bind(ORDER_ID_BASE + 20_000_000)
                .fetch_one(&pg)
                .await
                .expect("count");
        if count == trade_total as i64 {
            break;
        }
        assert!(count <= trade_total as i64, "double-apply: {count} > {trade_total}");
        assert!(
            Instant::now() < deadline,
            "PG did not converge (stuck at {count}/{trade_total})"
        );
        assert!(pgw.is_running(), "final pg-writer died");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    // The trade count converging does NOT imply the final frame flushed
    // (the last seq may be a position/account frame in a later batch) —
    // poll the floor instead of asserting a snapshot.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let floors = load_checkpoints(&pg).await.expect("floors");
        if floors.get(&PUBLISHER_ID) == Some(&TOTAL_FRAMES) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "PG floor never reached the last seq: {:?}",
            floors.get(&PUBLISHER_ID)
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    // ── PG accounts equal the last AccountSet per user ─────────────────────
    for (&user, &expect_atoms) in &last_set {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let got: Option<i64> = sqlx::query_scalar(
                "SELECT balance_atoms FROM accounts WHERE user_id = $1 AND asset = $2",
            )
            .bind(user)
            .bind(ASSET)
            .fetch_optional(&pg)
            .await
            .expect("pg account");
            if got == Some(expect_atoms) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "PG account user={user}: got {got:?}, want {expect_atoms}"
            );
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    }

    // ── Redis accounts equal the same values (diff = 0) ────────────────────
    for (&user, &expect_atoms) in &last_set {
        let key = format!("acct:{user}:{ASSET}");
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let got: Option<String> = redis::cmd("HGET")
                .arg(&key)
                .arg("balance_atoms")
                .query_async(&mut redis_conn)
                .await
                .unwrap_or(None);
            if got.as_deref().and_then(|s| s.parse::<i64>().ok()) == Some(expect_atoms) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "Redis account user={user}: got {got:?}, want {expect_atoms}"
            );
            assert!(rdw.is_running(), "final redis-writer died");
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    }
    eprintln!("chaos3w: PG ↔ Redis account diff = 0 for all {} users", users.len());

    // ── S1.7: swap-margin tables converged to the last-written values ───
    for (&user, &expect_qty) in &last_pos {
        let got: Option<i64> = sqlx::query_scalar(
            "SELECT qty_lots FROM positions WHERE user_id = $1 AND symbol = $2",
        )
        .bind(user)
        .bind(POS_SYMBOL)
        .fetch_optional(&pg)
        .await
        .expect("pg position");
        assert_eq!(got, Some(expect_qty), "position qty for user {user}");
    }
    for (&user, &expect_equity) in &last_risk {
        let got: Option<i64> =
            sqlx::query_scalar("SELECT equity_atoms FROM risk_accounts WHERE user_id = $1")
                .bind(user)
                .fetch_optional(&pg)
                .await
                .expect("pg risk account");
        assert_eq!(got, Some(expect_equity), "equity for user {user}");
    }
    eprintln!("chaos3w: positions + risk accounts converged (last-write-wins exact)");

    // ── journal-audit: journal ↔ checkpoints ↔ PG, exit 0 ─────────────────
    drop(pgw);
    drop(rdw);
    let audit = Command::new(env!("CARGO_BIN_EXE_journal-audit"))
        .env("DATABASE_URL", pg_url())
        .env("AERON_DIR", &aeron_dir)
        .env("EXCHANGE_ARCHIVE_CONTROL", &control)
        .env("RUST_LOG", "info")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("run journal-audit");
    assert!(
        audit.status.success(),
        "journal-audit reported inconsistency:\n{}\n{}",
        String::from_utf8_lossy(&audit.stdout),
        String::from_utf8_lossy(&audit.stderr)
    );

    eprintln!(
        "GATE 2 (3-way) PASS: {kills} kills across both writers — PG, Redis and journal agree"
    );
    cleanup(&pg, &users).await;
}
