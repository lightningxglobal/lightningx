//! Phase-4 pg-writer:
//! Subscribe to the Aeron PersistEvent stream and batch-apply frames to
//! Postgres. Pairs with `redis-writer` (which applies the same stream to
//! Redis L1) so PG writes can be pulled out of the desk-server hot path.
//!
//! Environment:
//!   DATABASE_URL    postgres://user:password@127.0.0.1:5432/mydb
//!   AERON_DIR       /tmp/aeron (macOS default) or /dev/shm/aeron (Linux)
//!   PG_WRITER_BATCH        max frames buffered before forced flush (default 256)
//!   PG_WRITER_FLUSH_MS     time-based flush ceiling in ms        (default 50)
//!   PG_WRITER_MAX_CONNS    sqlx pool size                        (default 8)
//!
//! Runs alongside the existing desk-server DB worker — applies are
//! idempotent (INSERT ON CONFLICT / WHERE id = ANY) so dual-write is safe
//! during the cutover window. Once PR4b/c retire the desk-server PG path,
//! pg-writer becomes the sole writer to `orders`, `accounts`, `trades`.

use aeron_wrapper::AeronClient;
use anyhow::Context;
use crossbeam_queue::ArrayQueue;
use lightning_exchange::desk::pg_store::{backfill_from_redis, PgWriteBatch};
use lightning_exchange::transport::aeron_channels::{aeron_dir, PERSIST_CHANNEL, PERSIST_STREAM};
use lightning_exchange::transport::aeron_transport::PersistSubscriber;
use lightning_exchange::transport::persist_event::PersistFrame;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let pg_url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let max_conns: u32 = parse_env_u32("PG_WRITER_MAX_CONNS", 16);
    // Bigger batches amortize the per-flush PG round-trip cost. UNNEST
    // INSERT scales linearly per row, so 5 000-row batches take ~100-200 ms
    // on commodity PG vs ~20-50 ms for 256-row batches — but cut flush rate
    // from ~312/s to ~16/s, lifting effective throughput.
    let max_batch: usize = parse_env_u32("PG_WRITER_BATCH", 5_000) as usize;
    let flush_ms: u64 = parse_env_u32("PG_WRITER_FLUSH_MS", 50) as u64;
    // Aeron→PG bridge queue. Bounded so we can't OOM, but large enough
    // (~1 M frames × 144 B ≈ 144 MB) to absorb minutes of overload before
    // dropping. Sustained PG-can't-keep-up scenarios still drop, but the
    // Aeron client never times out because the polling thread doesn't
    // share its loop with PG `.await`.
    let queue_cap: usize = parse_env_u32("PG_WRITER_QUEUE", 1_000_000) as usize;
    let backfill = std::env::var("BACKFILL_ON_START")
        .map(|s| !s.is_empty())
        .unwrap_or(false);
    let oneshot_backfill = std::env::var("ONESHOT_BACKFILL")
        .map(|s| !s.is_empty())
        .unwrap_or(false);
    let redis_url = std::env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6379/0".to_string());

    info!(
        "pg-writer starting (pg={}, max_conns={}, max_batch={}, flush_ms={}, backfill={})",
        redacted(&pg_url),
        max_conns,
        max_batch,
        flush_ms,
        backfill,
    );

    let pg = sqlx::postgres::PgPoolOptions::new()
        .max_connections(max_conns)
        .connect(&pg_url)
        .await
        .context("connect PG")?;

    // Optional one-shot backfill: any id present in Redis active_orders
    // but missing from PG is rebuilt from the Redis HASH and INSERTed
    // into PG. Closes the startup gap when pg-writer is brought up after
    // desk-server (Aeron stream subscriptions don't replay historical
    // frames). Idempotent — safe to run repeatedly.
    if backfill {
        info!("BACKFILL_ON_START set — reconciling PG from Redis");
        match redis::Client::open(redis_url.as_str()) {
            Ok(client) => match client.get_multiplexed_async_connection().await {
                Ok(mut conn) => {
                    let _: Result<String, _> =
                        redis::cmd("PING").query_async(&mut conn).await;
                    match backfill_from_redis(&pg, &mut conn).await {
                        Ok(stats) => info!(
                            "backfill done: scanned={} missing={} backfilled={} skipped_decode={}",
                            stats.redis_orders_scanned,
                            stats.missing_in_pg,
                            stats.backfilled,
                            stats.skipped_decode,
                        ),
                        Err(e) => warn!("backfill failed: {e} — continuing anyway"),
                    }
                }
                Err(e) => warn!("Redis connect failed at {redis_url}: {e} — skipping backfill"),
            },
            Err(e) => warn!("Invalid REDIS_URL '{redis_url}': {e} — skipping backfill"),
        }
        if oneshot_backfill {
            info!("ONESHOT_BACKFILL set; exiting after backfill");
            return Ok(());
        }
    }

    let dir = aeron_dir();
    info!("connecting Aeron client (dir={})", dir);

    // Bridge queue between Aeron polling (dedicated std::thread) and PG
    // flushing (tokio task). MUST be separated because flush_now().await
    // can park the tokio task for 100ms-2s during PG INSERT, and if that
    // happens on the same thread as Aeron polling, the Aeron client's
    // conductor timeout fires (10s) → entire client dies. We measured
    // pg-writer dying with 47s service interval at 40K conn × 1 op/s.
    let queue: Arc<ArrayQueue<PersistFrame>> = Arc::new(ArrayQueue::new(queue_cap));
    let dropped_pushes = Arc::new(AtomicU64::new(0));

    let queue_tx = queue.clone();
    let dropped_tx = dropped_pushes.clone();
    let aeron_dir_owned = dir.clone();
    std::thread::Builder::new()
        .name("pg-writer-aeron".to_string())
        .spawn(move || {
            let aeron = Arc::new(
                AeronClient::new(&aeron_dir_owned).expect("AeronClient::new failed"),
            );
            let mut sub = PersistSubscriber::new(aeron.clone(), PERSIST_CHANNEL, PERSIST_STREAM)
                .map_err(|e| anyhow::anyhow!("{e}"))
                .expect("create PersistSubscriber");
            info!(
                "[aeron] subscribed to persist stream (channel={}, stream={})",
                PERSIST_CHANNEL, PERSIST_STREAM
            );
            loop {
                aeron.do_work();
                sub.do_work();
                let mut did_work = false;
                while let Some(frame) = sub.poll() {
                    did_work = true;
                    if queue_tx.push(frame).is_err() {
                        // Bridge queue full — PG flush is sustainably
                        // behind. Drop (rate-limited log). redis-writer is
                        // still applying so live state is preserved; PG
                        // can be backfilled later.
                        let n = dropped_tx.fetch_add(1, Ordering::Relaxed) + 1;
                        if n % 100_000 == 0 {
                            warn!("[aeron] bridge queue full — dropped {n} frames");
                        }
                    }
                }
                if !did_work {
                    std::hint::spin_loop();
                }
            }
        })
        .context("spawn pg-writer-aeron thread")?;

    let mut batch = PgWriteBatch::new();
    let mut last_flush = Instant::now();
    let mut total_applied: u64 = 0;
    let mut total_flushes: u64 = 0;
    let mut last_log = Instant::now();

    loop {
        let mut did_work = false;
        // Drain frames from the bridge queue.
        while let Some(frame) = queue.pop() {
            did_work = true;
            let _ = batch.push(&frame);
            if batch.len() >= max_batch {
                match flush_now(&pg, &mut batch).await {
                    Ok(n) => {
                        total_applied += n as u64;
                        total_flushes += 1;
                        last_flush = Instant::now();
                    }
                    Err(e) => error!("flush (size-trigger) failed: {e}"),
                }
            }
        }

        // Time-based flush ceiling: even sparse streams land on disk fast.
        if !batch.is_empty() && last_flush.elapsed() >= Duration::from_millis(flush_ms) {
            match flush_now(&pg, &mut batch).await {
                Ok(n) => {
                    total_applied += n as u64;
                    total_flushes += 1;
                }
                Err(e) => error!("flush (time-trigger) failed: {e}"),
            }
            last_flush = Instant::now();
        }

        if last_log.elapsed() >= Duration::from_secs(30) {
            let sc = batch.skip_counts();
            info!(
                "pg-writer: applied={} flushes={} skipped={} bridge_dropped={} queue_depth={} \
                 [unknown_kind={} upsert_bad_status={} upsert_bad_ts={} upsert_empty_str={} \
                 fill_bad_status={} account_empty_asset={} trade_empty_symbol={} \
                 trade_bad_ts={} decode_failed={}]",
                total_applied,
                total_flushes,
                batch.skipped(),
                dropped_pushes.load(Ordering::Relaxed),
                queue.len(),
                sc.unknown_kind,
                sc.upsert_bad_status,
                sc.upsert_bad_timestamp,
                sc.upsert_empty_string,
                sc.fill_bad_status,
                sc.account_empty_asset,
                sc.trade_empty_symbol,
                sc.trade_bad_timestamp,
                sc.payload_decode_failed,
            );
            last_log = Instant::now();
        }

        if !did_work {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
}

async fn flush_now(pg: &sqlx::PgPool, batch: &mut PgWriteBatch) -> anyhow::Result<usize> {
    if batch.is_empty() {
        return Ok(0);
    }
    let started = Instant::now();
    let pending = batch.len();
    let written = match batch.flush(pg).await {
        Ok(n) => n,
        Err(e) => {
            warn!(
                "pg flush failed after {:?} ({} frames in batch): {e} — dropping batch",
                started.elapsed(),
                pending
            );
            // Drop in-flight rows so the next iteration doesn't replay a
            // poison frame forever. Lifetime `skipped` counter is preserved.
            batch.clear_payloads();
            return Err(e);
        }
    };
    let dt = started.elapsed();
    if dt > Duration::from_millis(500) {
        warn!("slow pg flush: {} frames → {} rows in {:?}", pending, written, dt);
    }
    Ok(written)
}

fn parse_env_u32(key: &str, default: u32) -> u32 {
    match std::env::var(key) {
        Ok(v) => match v.parse() {
            Ok(n) => n,
            Err(e) => {
                warn!("invalid {key}='{v}' ({e}); using default {default}");
                default
            }
        },
        Err(_) => default,
    }
}

fn redacted(url: &str) -> String {
    if let Some(at) = url.rfind('@') {
        if let Some(scheme_end) = url.find("://") {
            return format!("{}://***@{}", &url[..scheme_end], &url[at + 1..]);
        }
    }
    url.to_string()
}
