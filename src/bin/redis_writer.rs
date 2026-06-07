//! Phase-2 redis-writer:
//!   1. Cold-hydrate Redis from PG on startup (if empty).
//!   2. Subscribe to the Aeron `persist` stream and apply each PersistFrame
//!      to Redis. Runs indefinitely; restarts safely (Redis state is
//!      durable enough that warm restart skips hydrate).
//!
//! Environment:
//!   DATABASE_URL    postgres://user:password@127.0.0.1:5432/mydb
//!   REDIS_URL       redis://127.0.0.1:6379/0
//!   AERON_DIR       /tmp/aeron (macOS default) or /dev/shm/aeron (Linux)
//!   FORCE_REHYDRATE non-empty → purge Redis then hydrate before subscribing
//!   ONESHOT_HYDRATE non-empty → hydrate and exit (do not subscribe)
//!
//! Usage:
//!   DATABASE_URL=... REDIS_URL=... cargo run --release --bin redis-writer

use aeron_wrapper::AeronClient;
use anyhow::Context;
use lightning_exchange::db;
use lightning_exchange::desk::redis_store;
use lightning_exchange::transport::aeron_channels::{PERSIST_CHANNEL, PERSIST_STREAM, aeron_dir};
use lightning_exchange::transport::aeron_transport::PersistSubscriber;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let pg_url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/0".to_string());
    let force = std::env::var("FORCE_REHYDRATE")
        .map(|s| !s.is_empty())
        .unwrap_or(false);
    let oneshot = std::env::var("ONESHOT_HYDRATE")
        .map(|s| !s.is_empty())
        .unwrap_or(false);
    let reconcile = std::env::var("RECONCILE_ON_START")
        .map(|s| !s.is_empty())
        .unwrap_or(false);

    info!(
        "redis-writer starting (pg={}, redis={})",
        redacted(&pg_url),
        redis_url
    );

    // Durability-enforcing pool (synchronous_commit=on per connection).
    let pg = db::create_pool_sized(&pg_url, 4)
        .await
        .context("connect PG")?;
    db::run_migrations(&pg).await.context("run migrations")?;

    let client = redis::Client::open(redis_url.as_str()).context("open Redis client")?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .context("connect Redis")?;
    let _: String = redis::cmd("PING")
        .query_async(&mut conn)
        .await
        .context("PING Redis")?;

    if force {
        info!("FORCE_REHYDRATE set — purging Redis L1 first");
        redis_store::purge_all(&mut conn).await?;
    }

    let hydrated = redis_store::is_hydrated(&mut conn).await?;
    if !hydrated || force {
        info!("cold hydrate from PG → Redis…");
        let stats = redis_store::hydrate_from_pg(&pg, &mut conn).await?;
        info!(
            "hydrate done: {} active orders, {} accounts, {} client_order_id mappings",
            stats.orders, stats.accounts, stats.user_coids
        );
        if stats.orders == 0 && stats.accounts == 0 {
            warn!("PG had no active orders and no accounts — nothing to hydrate");
        }
    } else {
        info!("Redis already hydrated; skipping cold load");
    }

    // Optional one-shot reconcile: drop Redis state for orders that no
    // longer exist in PG. Idempotent and cheap (one ANY-bigint query per
    // set). Useful after a publish-path bug left strays behind. Runs
    // after hydrate so the freshly-hydrated active set is the truth.
    if reconcile {
        info!("RECONCILE_ON_START set — sweeping orphans");
        let stats = redis_store::reconcile_orphans(&pg, &mut conn).await?;
        info!(
            "reconcile done: orders_scanned={} orders_removed={} user_sets_scanned={} accounts_scanned={}",
            stats.redis_orders_scanned,
            stats.orders_removed,
            stats.user_sets_scanned,
            stats.accounts_scanned,
        );
    }

    if oneshot {
        info!("ONESHOT_HYDRATE set; exiting after hydrate");
        return Ok(());
    }

    // Long-running phase: subscribe to PersistEvent and apply to Redis.
    let dir = aeron_dir();
    info!("connecting Aeron client (dir={})", dir);
    let aeron =
        AeronClient::new(&dir).map_err(|e| anyhow::anyhow!("AeronClient::new failed: {e:?}"))?;
    let aeron = Arc::new(aeron);

    let mut sub = PersistSubscriber::new(aeron.clone(), PERSIST_CHANNEL, PERSIST_STREAM)
        .map_err(anyhow::Error::msg)
        .context("create PersistSubscriber")?;

    info!(
        "subscribed to persist stream (channel={}, stream={}); waiting for frames…",
        PERSIST_CHANNEL, PERSIST_STREAM
    );

    // Journal catch-up (same contract as pg-writer): when
    // EXCHANGE_ARCHIVE_CONTROL is set, replay every recording of the
    // persist stream BEFORE going live — frames missed while this writer
    // was down (or dropped by its ring) are re-delivered; the floor below
    // dedups what was already applied. The live subscription above was
    // created first, so frames arriving during the replay wait in its
    // image buffer (gap-free hand-off). This thread drives the client's
    // conductor — archive threading contract satisfied.
    if let Some(cfg) = lightning_exchange::transport::journal::archive_config_from_env() {
        use lightning_exchange::transport::journal::{JournalReplayer, PERSIST_REPLAY_STREAM};
        // A dedicated replay stream id per consumer would let pg-writer and
        // redis-writer catch up simultaneously; +1 keeps them disjoint.
        let replay_stream = PERSIST_REPLAY_STREAM + 1;
        let mut replayer = JournalReplayer::connect(&aeron, &cfg)
            .map_err(|e| anyhow::anyhow!("journal replay requested but connect failed: {e}"))?;
        let recordings = replayer
            .recordings("ipc", PERSIST_STREAM)
            .map_err(|e| anyhow::anyhow!("list journal recordings: {e}"))?;
        info!("journal: {} recording(s) to replay", recordings.len());
        // Pre-load floors so replayed duplicates are skipped on apply.
        let pre_floors: std::collections::HashMap<u16, u64> =
            redis_store::load_persist_floors(&mut conn).await.unwrap_or_default();
        let mut catchup_floors = pre_floors.clone();
        for rec in &recordings {
            let Some(replay) = replayer
                .replay_bounded(rec, PERSIST_CHANNEL, replay_stream)
                .map_err(|e| anyhow::anyhow!("start journal replay: {e}"))?
            else {
                continue;
            };
            let mut replay_sub =
                PersistSubscriber::new(aeron.clone(), PERSIST_CHANNEL, replay_stream)
                    .map_err(anyhow::Error::msg)
                    .context("create replay subscriber")?;
            let mut replayed = 0u64;
            loop {
                aeron.do_work();
                replay_sub.do_work();
                let mut got = false;
                while let Some(frame) = replay_sub.poll() {
                    got = true;
                    let publisher_id: u16 = frame.publisher_id;
                    let seq: u64 = frame.seq;
                    if seq != 0 && seq <= catchup_floors.get(&publisher_id).copied().unwrap_or(0)
                    {
                        continue; // already applied in a previous life
                    }
                    if redis_store::apply_frame(&mut conn, &frame).await.is_ok() {
                        replayed += 1;
                        if seq != 0 {
                            catchup_floors.insert(publisher_id, seq);
                        }
                    }
                }
                if !got {
                    if replay_sub.replay_image_closed() {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
            info!(
                "journal: recording {} replayed {} frame(s)",
                replay.recording_id, replayed
            );
        }
        if let Err(e) = redis_store::store_persist_floors(&mut conn, &catchup_floors).await {
            tracing::warn!("journal: floor persist after catch-up failed: {e}");
        }
        info!("journal: catch-up complete, switching to live stream");
    }

    // Per-publisher checkpoint floors, persisted in Redis. Frames at or
    // below the floor are duplicates (replay/restart) and are skipped.
    // Redis apply is idempotent HSETs, so at-least-once checkpointing
    // (apply, then advance) is sufficient here — unlike PG where the
    // checkpoint commits transactionally with the data.
    let mut floors: std::collections::HashMap<u16, u64> =
        redis_store::load_persist_floors(&mut conn).await.unwrap_or_default();
    if !floors.is_empty() {
        info!("persist checkpoints loaded: {:?}", floors);
    }
    let mut applied: u64 = 0;
    let mut duplicates: u64 = 0;
    let mut floors_dirty = false;
    let mut last_ckpt = Instant::now();
    let mut last_log = Instant::now();
    loop {
        aeron.do_work();
        sub.do_work();
        let mut did_work = false;
        while let Some(frame) = sub.poll() {
            did_work = true;
            let publisher_id: u16 = frame.publisher_id;
            let seq: u64 = frame.seq;
            if seq != 0 && seq <= floors.get(&publisher_id).copied().unwrap_or(0) {
                duplicates += 1;
                continue;
            }
            if let Err(e) = redis_store::apply_frame(&mut conn, &frame).await {
                tracing::error!("apply_frame failed: {e}");
            } else {
                applied += 1;
                if seq != 0 {
                    floors.insert(publisher_id, seq);
                    floors_dirty = true;
                }
            }
        }
        // Persist floors periodically (1s), not per frame: a stale floor
        // only causes some duplicate idempotent re-applies after restart.
        if floors_dirty && last_ckpt.elapsed() >= Duration::from_secs(1) {
            if let Err(e) = redis_store::store_persist_floors(&mut conn, &floors).await {
                tracing::warn!("checkpoint store failed: {e}");
            } else {
                floors_dirty = false;
            }
            last_ckpt = Instant::now();
        }
        if last_log.elapsed() >= Duration::from_secs(30) {
            info!(
                "redis-writer: {applied} frames applied (dropped {} dup {})",
                sub.dropped_frames(),
                duplicates,
            );
            last_log = Instant::now();
        }
        if !did_work {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
}

/// Hide credentials when logging a DB URL.
fn redacted(url: &str) -> String {
    if let Some(at) = url.rfind('@') {
        if let Some(scheme_end) = url.find("://") {
            return format!("{}://***@{}", &url[..scheme_end], &url[at + 1..]);
        }
    }
    url.to_string()
}
