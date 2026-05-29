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
use lightning_exchange::desk::redis_store;
use lightning_exchange::transport::aeron_channels::{aeron_dir, PERSIST_CHANNEL, PERSIST_STREAM};
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

    info!(
        "redis-writer starting (pg={}, redis={})",
        redacted(&pg_url),
        redis_url
    );

    let pg = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(&pg_url)
        .await
        .context("connect PG")?;

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

    if oneshot {
        info!("ONESHOT_HYDRATE set; exiting after hydrate");
        return Ok(());
    }

    // Long-running phase: subscribe to PersistEvent and apply to Redis.
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

    let mut applied: u64 = 0;
    let mut last_log = Instant::now();
    loop {
        aeron.do_work();
        sub.do_work();
        let mut did_work = false;
        while let Some(frame) = sub.poll() {
            did_work = true;
            if let Err(e) = redis_store::apply_frame(&mut conn, &frame).await {
                tracing::error!("apply_frame failed: {e}");
            } else {
                applied += 1;
            }
        }
        if last_log.elapsed() >= Duration::from_secs(30) {
            info!(
                "redis-writer: {applied} frames applied (dropped {})",
                sub.dropped_frames()
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
