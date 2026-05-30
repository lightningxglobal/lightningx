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
use lightning_exchange::desk::pg_store::PgWriteBatch;
use lightning_exchange::transport::aeron_channels::{aeron_dir, PERSIST_CHANNEL, PERSIST_STREAM};
use lightning_exchange::transport::aeron_transport::PersistSubscriber;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let pg_url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let max_conns: u32 = parse_env_u32("PG_WRITER_MAX_CONNS", 8);
    let max_batch: usize = parse_env_u32("PG_WRITER_BATCH", 256) as usize;
    let flush_ms: u64 = parse_env_u32("PG_WRITER_FLUSH_MS", 50) as u64;

    info!(
        "pg-writer starting (pg={}, max_conns={}, max_batch={}, flush_ms={})",
        redacted(&pg_url),
        max_conns,
        max_batch,
        flush_ms
    );

    let pg = sqlx::postgres::PgPoolOptions::new()
        .max_connections(max_conns)
        .connect(&pg_url)
        .await
        .context("connect PG")?;

    let dir = aeron_dir();
    info!("connecting Aeron client (dir={})", dir);
    let aeron = AeronClient::new(&dir)
        .map_err(|e| anyhow::anyhow!("AeronClient::new failed: {e:?}"))?;
    let aeron = Arc::new(aeron);

    let mut sub = PersistSubscriber::new(aeron.clone(), PERSIST_CHANNEL, PERSIST_STREAM)
        .map_err(anyhow::Error::msg)
        .context("create PersistSubscriber")?;

    info!(
        "subscribed to persist stream (channel={}, stream={}); waiting for frames…",
        PERSIST_CHANNEL, PERSIST_STREAM
    );

    let mut batch = PgWriteBatch::new();
    let mut last_flush = Instant::now();
    let mut total_applied: u64 = 0;
    let mut total_flushes: u64 = 0;
    let mut last_log = Instant::now();

    loop {
        aeron.do_work();
        sub.do_work();
        let mut did_work = false;

        while let Some(frame) = sub.poll() {
            did_work = true;
            if !batch.push(&frame) {
                // skipped — already counted internally
            }
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

        // batch.skipped() accumulates across the entire batch lifetime; flush
        // never resets it (see PgWriteBatch::flush — only the per-kind Vecs
        // are taken). So reporting it directly is correct.
        if last_log.elapsed() >= Duration::from_secs(30) {
            info!(
                "pg-writer: applied={} flushes={} skipped={} sub_dropped={}",
                total_applied,
                total_flushes,
                batch.skipped(),
                sub.dropped_frames()
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
