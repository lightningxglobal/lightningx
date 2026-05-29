//! Phase-1 redis-writer: cold hydrate only.
//!
//! Connects to PG + Redis. If Redis `active_orders` set is empty, loads all
//! active orders (status IN PENDING/TRADING) and the full accounts table
//! into Redis. Then exits (Phase-2 will turn this into a long-lived Aeron
//! subscriber that keeps Redis in sync with the engine event stream).
//!
//! Environment:
//!   DATABASE_URL    postgres://user:password@127.0.0.1:5432/mydb
//!   REDIS_URL       redis://127.0.0.1:6379/0
//!   FORCE_REHYDRATE if set (any non-empty value), purge Redis first
//!
//! Usage:
//!   DATABASE_URL=... REDIS_URL=... cargo run --release --bin redis-writer

use anyhow::Context;
use lightning_exchange::desk::redis_store;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let pg_url = std::env::var("DATABASE_URL")
        .context("DATABASE_URL must be set")?;
    let redis_url = std::env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6379/0".to_string());
    let force = std::env::var("FORCE_REHYDRATE")
        .map(|s| !s.is_empty())
        .unwrap_or(false);

    info!("redis-writer starting (pg={}, redis={})", redacted(&pg_url), redis_url);

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
    let _: String = redis::cmd("PING").query_async(&mut conn).await.context("PING Redis")?;

    if force {
        info!("FORCE_REHYDRATE set — purging Redis L1 first");
        redis_store::purge_all(&mut conn).await?;
    }

    let hydrated = redis_store::is_hydrated(&mut conn).await?;
    if hydrated && !force {
        info!("Redis already hydrated (active_orders non-empty); skipping cold load");
        return Ok(());
    }

    info!("cold hydrate from PG → Redis…");
    let stats = redis_store::hydrate_from_pg(&pg, &mut conn).await?;
    info!(
        "hydrate done: {} active orders, {} accounts, {} client_order_id mappings",
        stats.orders, stats.accounts, stats.user_coids
    );

    if stats.orders == 0 && stats.accounts == 0 {
        warn!("PG had no active orders and no accounts — nothing to hydrate");
    }
    Ok(())
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
