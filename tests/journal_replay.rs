//! End-to-end journal test: persist frames LOST by a down consumer are
//! recovered by replaying the Aeron Archive recording, with the checkpoint
//! floor deduplicating the already-applied prefix.
//!
//! Topology (all real): Java ArchivingMediaDriver + Aeron IPC + PostgreSQL.
//!   1. "desk": records the persist stream, publishes frames seq 1..=100;
//!   2. "writer, first life": applies only 1..=60 (commits floor 60) — the
//!      tail 61..=100 is never delivered (writer was down);
//!   3. "writer, restart": lists journal recordings, replays them, pushes
//!      every replayed frame through the checkpointed batch:
//!      1..=60 dropped as duplicates, 61..=100 recovered into PG.
//!
//! Skips when java / the aeron-all jar / PG are unavailable.

use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use aeron_wrapper::AeronClient;
use lightning_exchange::db;
use lightning_exchange::desk::pg_store::{PgWriteBatch, load_checkpoints};
use lightning_exchange::transport::aeron_channels::{PERSIST_CHANNEL, PERSIST_STREAM};
use lightning_exchange::transport::aeron_transport::{PersistPublisher, PersistSubscriber};
use lightning_exchange::transport::journal::{JournalReplayer, PERSIST_REPLAY_STREAM};
use lightning_exchange::transport::persist_event::{PersistFrame, TradeInsertPayload, pack_str};
use sqlx::PgPool;

const CONTROL_PORT: u16 = 18920;
const PUBLISHER_ID: u16 = 61_234;
const TOTAL: u64 = 100;
const APPLIED_BEFORE_CRASH: u64 = 60;
const ORDER_ID_BASE: i64 = 940_000_000;

fn jar_path() -> Option<String> {
    let candidates = [
        std::env::var("AERON_ALL_JAR").ok(),
        Some(
            "/home/alpha/work/3party/aeron/aeron-all/build/libs/aeron-all-1.52.0-SNAPSHOT.jar"
                .into(),
        ),
    ];
    candidates
        .into_iter()
        .flatten()
        .find(|p| std::path::Path::new(p).exists())
}

struct DriverGuard {
    child: Child,
    dir: std::path::PathBuf,
}

impl Drop for DriverGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn start_archiving_driver(jar: &str) -> std::io::Result<(DriverGuard, String)> {
    let base = std::env::temp_dir().join(format!(
        "journal-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&base)?;
    let aeron_dir = base.join("driver").to_string_lossy().into_owned();
    let archive_dir = base.join("archive").to_string_lossy().into_owned();

    let child = Command::new("java")
        .arg("--add-opens")
        .arg("java.base/jdk.internal.misc=ALL-UNNAMED")
        .arg("--add-opens")
        .arg("java.base/sun.nio.ch=ALL-UNNAMED")
        .arg("-cp")
        .arg(jar)
        .arg(format!("-Daeron.dir={aeron_dir}"))
        .arg(format!("-Daeron.archive.dir={archive_dir}"))
        .arg(format!(
            "-Daeron.archive.control.channel=aeron:udp?endpoint=localhost:{CONTROL_PORT}"
        ))
        .arg("-Daeron.archive.replication.channel=aeron:udp?endpoint=localhost:0")
        .arg("-Daeron.archive.recording.events.enabled=false")
        // Durability mode the production deployment will use. The catalog
        // sync level must be >= the data sync level or the archive refuses
        // to start ("catalogFileSyncLevel 0 < fileSyncLevel 2").
        .arg("-Daeron.archive.file.sync.level=2")
        .arg("-Daeron.archive.catalog.file.sync.level=2")
        .arg("-Daeron.threading.mode=SHARED")
        .arg("-Daeron.archive.threading.mode=SHARED")
        .arg("-Daeron.shared.idle.strategy=sleep-ns")
        .arg("io.aeron.archive.ArchivingMediaDriver")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;

    Ok((DriverGuard { child, dir: base }, aeron_dir))
}

fn wait_for<T>(what: &str, timeout: Duration, mut probe: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(v) = probe() {
            return v;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn frame(seq: u64) -> PersistFrame {
    let mut f = PersistFrame::trade_insert(TradeInsertPayload {
        buy_order_id: ORDER_ID_BASE + seq as i64,
        sell_order_id: ORDER_ID_BASE + seq as i64 + 1_000_000,
        symbol: pack_str("BTC_USDT"),
        price: 100.0 + seq as f64,
        qty: 0.001,
        ts_ms: 1_700_000_000_000 + seq as i64,
    });
    f.publisher_id = PUBLISHER_ID;
    f.seq = seq;
    f
}

async fn try_pg() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://user:password@localhost:5432/mydb".to_string());
    let pg = db::create_pool_sized(&url, 4).await.ok()?;
    db::run_migrations(&pg).await.ok()?;
    Some(pg)
}

async fn cleanup(pg: &PgPool) {
    let _ = sqlx::query("DELETE FROM persist_checkpoints WHERE publisher_id = $1")
        .bind(PUBLISHER_ID as i32)
        .execute(pg)
        .await;
    let _ = sqlx::query("DELETE FROM trades WHERE buy_order_id BETWEEN $1 AND $2")
        .bind(ORDER_ID_BASE)
        .bind(ORDER_ID_BASE + 10_000_000)
        .execute(pg)
        .await;
}

async fn trade_count(pg: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM trades WHERE buy_order_id BETWEEN $1 AND $2")
        .bind(ORDER_ID_BASE)
        .bind(ORDER_ID_BASE + 10_000_000)
        .fetch_one(pg)
        .await
        .expect("count")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frames_lost_by_down_consumer_are_recovered_from_journal() {
    let Some(jar) = jar_path() else {
        eprintln!("skip: aeron-all jar not found");
        return;
    };
    let Some(pg) = try_pg().await else {
        eprintln!("skip: no PG");
        return;
    };
    cleanup(&pg).await;
    let Ok((_driver, aeron_dir)) = start_archiving_driver(&jar) else {
        eprintln!("skip: cannot start ArchivingMediaDriver");
        return;
    };

    let config = aeron_wrapper::ArchiveConfig {
        control_request_channel: format!("aeron:udp?endpoint=localhost:{CONTROL_PORT}"),
        ..aeron_wrapper::ArchiveConfig::default()
    };

    // The Aeron + archive work happens on a blocking thread (the archive
    // control session must stay on the thread driving do_work).
    let cfg2 = config.clone();
    let pg2 = pg.clone();
    let result = tokio::task::spawn_blocking(move || {
        let client = wait_for("media driver", Duration::from_secs(20), || {
            AeronClient::new(&aeron_dir).ok().map(Arc::new)
        });

        // ── Phase 1: "desk" records + publishes 1..=100 ─────────────────
        let recorder = wait_for("archive connect", Duration::from_secs(20), || {
            lightning_exchange::transport::journal::JournalRecorder::start(
                &client,
                &cfg2,
                PERSIST_CHANNEL,
                PERSIST_STREAM,
            )
            .ok()
        });

        let mut publisher = PersistPublisher::new(client.clone(), PERSIST_CHANNEL, PERSIST_STREAM)
            .expect("persist publisher");
        // Recording subscription must be up before frames flow.
        std::thread::sleep(Duration::from_millis(500));
        for seq in 1..=TOTAL {
            let f = frame(seq);
            wait_for("publish", Duration::from_secs(5), || {
                client.do_work();
                publisher.publish(&f).ok()
            });
        }
        // Give the archive a moment to sink the tail (file.sync.level=2).
        std::thread::sleep(Duration::from_millis(800));
        drop(recorder);

        // ── Phase 3 (after the runtime applies 1..=60 below): replay ─────
        // Returning the pieces the async part needs.
        (client, pg2)
    })
    .await
    .expect("blocking phase");
    let (client, pg) = result;

    // ── Phase 2: "writer first life" applied only 1..=60, then crashed ──
    let mut batch = PgWriteBatch::new();
    for seq in 1..=APPLIED_BEFORE_CRASH {
        assert!(batch.push(&frame(seq)));
    }
    batch.flush(&pg).await.expect("flush first life");
    assert_eq!(trade_count(&pg).await, APPLIED_BEFORE_CRASH as i64);
    let floors = load_checkpoints(&pg).await.expect("load floors");
    assert_eq!(floors.get(&PUBLISHER_ID), Some(&APPLIED_BEFORE_CRASH));
    drop(batch); // writer "crashes"; frames 61..=100 were never delivered

    // ── Phase 3: restart with journal replay ────────────────────────────
    let replayed_frames = tokio::task::spawn_blocking(move || {
        let mut replayer =
            JournalReplayer::connect(&client, &config).expect("replayer connect");
        let recordings = replayer
            .recordings("ipc", PERSIST_STREAM)
            .expect("list recordings");
        assert!(
            !recordings.is_empty(),
            "the desk's recording must be discoverable without knowing its session id"
        );

        let mut frames: Vec<PersistFrame> = Vec::new();
        for rec in &recordings {
            let Some(_replay) = replayer
                .replay_bounded(rec, PERSIST_CHANNEL, PERSIST_REPLAY_STREAM)
                .expect("start replay")
            else {
                continue;
            };
            let mut sub =
                PersistSubscriber::new(client.clone(), PERSIST_CHANNEL, PERSIST_REPLAY_STREAM)
                    .expect("replay subscriber");
            let mut last = Instant::now();
            loop {
                client.do_work();
                sub.do_work();
                let mut got = false;
                while let Some(f) = sub.poll() {
                    got = true;
                    frames.push(f);
                }
                if got {
                    last = Instant::now();
                } else if last.elapsed() > Duration::from_secs(2) {
                    break;
                } else {
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        }
        frames
    })
    .await
    .expect("replay phase");

    assert_eq!(
        replayed_frames.len(),
        TOTAL as usize,
        "journal must re-deliver the complete prefix"
    );

    // Restarted writer: floor from PG dedups 1..=60, recovers 61..=100.
    let mut batch = PgWriteBatch::new();
    batch.set_applied_floors(load_checkpoints(&pg).await.expect("floors"));
    let mut admitted = 0u64;
    for f in &replayed_frames {
        if batch.push(f) {
            admitted += 1;
        }
    }
    assert_eq!(
        batch.duplicate_seq_frames, APPLIED_BEFORE_CRASH,
        "already-applied prefix must be dropped as duplicates"
    );
    assert_eq!(
        admitted,
        TOTAL - APPLIED_BEFORE_CRASH,
        "exactly the lost tail is admitted"
    );
    batch.flush(&pg).await.expect("flush recovery");

    assert_eq!(
        trade_count(&pg).await,
        TOTAL as i64,
        "all 100 trades present after journal recovery — nothing lost, nothing doubled"
    );
    let floors = load_checkpoints(&pg).await.expect("final floors");
    assert_eq!(floors.get(&PUBLISHER_ID), Some(&TOTAL));

    cleanup(&pg).await;
}
