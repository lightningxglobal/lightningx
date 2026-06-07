//! Gate 2 chaos drill — REAL pg-writer processes SIGKILLed under flow.
//!
//! A publisher streams M sequenced trade frames onto the journaled persist
//! stream while the actual pg-writer binary is repeatedly killed with
//! SIGKILL at random points (mid-flush, mid-replay, anywhere) and
//! restarted. Every restart replays the Archive journal from position 0;
//! the transactional checkpoint floor deduplicates.
//!
//! Pass criteria (the Gate 2 contract):
//!   * PostgreSQL ends with EXACTLY M trades — none lost, none doubled;
//!   * the committed checkpoint equals the highest published seq.
//!
//! Kill count defaults small for CI; the full drill is
//!   CHAOS_KILLS=100 cargo test --test chaos_gate2 -- --nocapture
//!
//! Skips when java/jar or PG is unavailable.

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
use lightning_exchange::transport::persist_event::{PersistFrame, TradeInsertPayload, pack_str};

const CONTROL_PORT: u16 = 18940;
const PUBLISHER_ID: u16 = 62_001;
const ORDER_ID_BASE: i64 = 950_000_000;
const TOTAL_FRAMES: u64 = 3_000;

fn frame(seq: u64) -> PersistFrame {
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

fn spawn_pg_writer(aeron_dir: &str, control: &str) -> std::io::Result<ProcGuard> {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_pg-writer"));
    cmd.env("DATABASE_URL", pg_url())
        .env("AERON_DIR", aeron_dir)
        .env("EXCHANGE_ARCHIVE_CONTROL", control)
        .env("PG_RECONCILE_SECS", "0")
        .env("PG_WRITER_FLUSH_MS", "20")
        .env("PG_WRITER_BATCH", "64")
        .env("RUST_LOG", "warn")
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    ProcGuard::spawn("pg-writer", &mut cmd)
}

async fn cleanup(pg: &sqlx::PgPool) {
    let _ = sqlx::query("DELETE FROM persist_checkpoints WHERE publisher_id = $1")
        .bind(PUBLISHER_ID as i32)
        .execute(pg)
        .await;
    let _ = sqlx::query("DELETE FROM trades WHERE buy_order_id BETWEEN $1 AND $2")
        .bind(ORDER_ID_BASE)
        .bind(ORDER_ID_BASE + 20_000_000)
        .execute(pg)
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn killed_writers_lose_nothing_and_double_nothing() {
    let Some(jar) = jar_path() else {
        eprintln!("skip: aeron-all jar not found");
        return;
    };
    let Some(pg) = db::create_pool_sized(&pg_url(), 4).await.ok() else {
        eprintln!("skip: no PG");
        return;
    };
    db::run_migrations(&pg).await.expect("migrations");
    cleanup(&pg).await;
    let Ok(driver) = start_archiving_driver(&jar, CONTROL_PORT) else {
        eprintln!("skip: cannot start ArchivingMediaDriver");
        return;
    };

    let kills: u32 = std::env::var("CHAOS_KILLS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(6);
    let aeron_dir = driver.aeron_dir.clone();
    let control = driver.control_channel.clone();

    // ── Publisher thread: record + stream all frames, paced ──────────────
    let published = Arc::new(AtomicU64::new(0));
    let published_t = published.clone();
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
        std::thread::sleep(Duration::from_millis(500)); // recording sub joins
        for seq in 1..=TOTAL_FRAMES {
            let f = frame(seq);
            wait_for("publish", Duration::from_secs(5), || {
                client.do_work();
                publisher.publish(&f).ok()
            });
            published_t.store(seq, Ordering::Relaxed);
            // Pace the stream so kills land mid-flow, not after the fact.
            std::thread::sleep(Duration::from_millis(3));
        }
        // Keep recording position flushed.
        std::thread::sleep(Duration::from_millis(500));
    });

    // ── Chaos loop: spawn → run briefly → SIGKILL → repeat ───────────────
    let mut rng = Lcg(0xC4A0_5C4A_05C4_A05C);
    let mut writer = spawn_pg_writer(&aeron_dir, &control).expect("spawn writer");
    for k in 1..=kills {
        let run_ms = rng.range(400, 1500);
        std::thread::sleep(Duration::from_millis(run_ms));
        writer.kill9();
        eprintln!(
            "chaos: kill #{k}/{kills} after {run_ms}ms (published={})",
            published.load(Ordering::Relaxed)
        );
        // Brief gap while dead — frames published now are "lost" from the
        // live stream's perspective and MUST come back via journal replay.
        std::thread::sleep(Duration::from_millis(rng.range(100, 400)));
        writer = spawn_pg_writer(&aeron_dir, &control).expect("respawn writer");
    }

    pub_thread.join().expect("publisher thread");

    // ── Convergence: the surviving writer must reach exactly TOTAL ───────
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut last_count: i64 = -1;
    loop {
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM trades WHERE buy_order_id BETWEEN $1 AND $2")
                .bind(ORDER_ID_BASE)
                .bind(ORDER_ID_BASE + 20_000_000)
                .fetch_one(&pg)
                .await
                .expect("count");
        if count != last_count {
            eprintln!("chaos: converging — {count}/{TOTAL_FRAMES}");
            last_count = count;
        }
        if count == TOTAL_FRAMES as i64 {
            break;
        }
        assert!(
            count <= TOTAL_FRAMES as i64,
            "MORE rows than published: double-apply detected ({count} > {TOTAL_FRAMES})"
        );
        assert!(
            Instant::now() < deadline,
            "did not converge to {TOTAL_FRAMES} (stuck at {count}) — frames lost"
        );
        assert!(writer.is_running(), "final pg-writer died unexpectedly");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // No duplicates hiding behind the count: every order id distinct.
    let distinct: i64 = sqlx::query_scalar(
        "SELECT COUNT(DISTINCT buy_order_id) FROM trades WHERE buy_order_id BETWEEN $1 AND $2",
    )
    .bind(ORDER_ID_BASE)
    .bind(ORDER_ID_BASE + 20_000_000)
    .fetch_one(&pg)
    .await
    .expect("distinct");
    assert_eq!(distinct, TOTAL_FRAMES as i64, "duplicate trades detected");

    // Checkpoint floor must equal the highest published seq.
    let floors = load_checkpoints(&pg).await.expect("floors");
    assert_eq!(
        floors.get(&PUBLISHER_ID),
        Some(&TOTAL_FRAMES),
        "checkpoint floor must equal the last published seq"
    );

    eprintln!(
        "GATE 2 PASS: {kills} SIGKILLs, {TOTAL_FRAMES} frames — zero lost, zero doubled"
    );
    drop(writer);
    cleanup(&pg).await;
}
