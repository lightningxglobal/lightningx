//! Redis L1 storage for `orders` and `accounts`.
//!
//! Data model:
//!
//! - `HASH order:{id}` — full row of an active (PENDING/TRADING) order.
//!   fields: user_id, symbol, side, order_type, price, qty, filled, status,
//!           freeze_price, client_order_id, created_at
//! - `SET active_orders` — global index of order ids (for full scans / cold
//!   hydrate checks).
//! - `SET user_orders:{user_id}` — per-user index for `/api/orders?status=open`.
//! - `HASH acct:{user_id}:{asset}` — fields: balance, frozen.
//! - `SET user_assets:{user_id}` — assets a user holds (for HMGET fanout).
//! - `HASH user_coid:{user_id}` — `client_order_id → order_id` (idempotency).
//!
//! The data model is HASH-per-record (not JSON blobs) so writers can use
//! field-level atomic ops like `HINCRBYFLOAT` for balance/frozen, and so
//! readers can `HMGET` specific fields without parsing.
//!
//! This module is **infra-only**: it owns the data model and the cold
//! hydrate routine. Writers / readers are layered on top.

use redis::AsyncCommands;
use sqlx::PgPool;

pub const KEY_ACTIVE_ORDERS: &str = "active_orders";

pub fn key_order(id: i64) -> String {
    format!("order:{id}")
}
pub fn key_user_orders(user_id: i64) -> String {
    format!("user_orders:{user_id}")
}
pub fn key_account(user_id: i64, asset: &str) -> String {
    format!("acct:{user_id}:{asset}")
}
pub fn key_user_assets(user_id: i64) -> String {
    format!("user_assets:{user_id}")
}
pub fn key_user_coid(user_id: i64) -> String {
    format!("user_coid:{user_id}")
}

/// True if Redis already has a non-empty `active_orders` set, meaning a
/// previous instance hydrated it. False means cold (empty) → we should
/// rehydrate from PG.
pub async fn is_hydrated(conn: &mut redis::aio::MultiplexedConnection) -> anyhow::Result<bool> {
    let n: u64 = conn.scard(KEY_ACTIVE_ORDERS).await?;
    Ok(n > 0)
}

/// Counts of rows written by `hydrate_from_pg`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct HydrateStats {
    pub orders: u64,
    pub accounts: u64,
    pub user_coids: u64,
}

/// Cold-load active orders + all accounts from PG into Redis.
///
/// Safe to call against a non-empty Redis — it overwrites the relevant
/// keys. Use `is_hydrated` first if you only want to hydrate on cold start.
pub async fn hydrate_from_pg(
    pg: &PgPool,
    conn: &mut redis::aio::MultiplexedConnection,
) -> anyhow::Result<HydrateStats> {
    let mut stats = HydrateStats::default();

    // 1) Active orders.
    let rows: Vec<(
        i64,
        i64,
        String,
        String,
        String,
        Option<f64>,
        f64,
        f64,
        String,
        Option<f64>,
        Option<String>,
        chrono::DateTime<chrono::Utc>,
    )> = sqlx::query_as(
        "SELECT id, user_id, symbol, side, order_type, price, quantity, filled,
                status, freeze_price, client_order_id, created_at
         FROM orders
         WHERE status IN ('PENDING','TRADING')",
    )
    .fetch_all(pg)
    .await?;

    if !rows.is_empty() {
        let mut pipe = redis::pipe();
        pipe.atomic();
        for (
            id,
            user_id,
            symbol,
            side,
            order_type,
            price,
            quantity,
            filled,
            status,
            freeze_price,
            client_order_id,
            created_at,
        ) in &rows
        {
            let fields: Vec<(&str, String)> = vec![
                ("user_id", user_id.to_string()),
                ("symbol", symbol.clone()),
                ("side", side.clone()),
                ("order_type", order_type.clone()),
                ("price", price.unwrap_or(0.0).to_string()),
                ("qty", quantity.to_string()),
                ("filled", filled.to_string()),
                ("status", status.clone()),
                ("freeze_price", freeze_price.unwrap_or(0.0).to_string()),
                (
                    "client_order_id",
                    client_order_id.clone().unwrap_or_default(),
                ),
                ("created_at_ms", created_at.timestamp_millis().to_string()),
            ];
            pipe.hset_multiple(key_order(*id), &fields).ignore();
            pipe.sadd(KEY_ACTIVE_ORDERS, *id).ignore();
            pipe.sadd(key_user_orders(*user_id), *id).ignore();
            if let Some(coid) = client_order_id {
                if !coid.is_empty() {
                    pipe.hset(key_user_coid(*user_id), coid, *id).ignore();
                    stats.user_coids += 1;
                }
            }
        }
        pipe.query_async::<()>(conn).await?;
        stats.orders = rows.len() as u64;
    }

    // 2) Accounts.
    let acct_rows: Vec<(i64, String, f64, f64)> = sqlx::query_as(
        "SELECT user_id, asset, balance, frozen FROM accounts",
    )
    .fetch_all(pg)
    .await?;

    if !acct_rows.is_empty() {
        let mut pipe = redis::pipe();
        pipe.atomic();
        for (user_id, asset, balance, frozen) in &acct_rows {
            pipe.hset_multiple(
                key_account(*user_id, asset),
                &[
                    ("balance", balance.to_string()),
                    ("frozen", frozen.to_string()),
                ],
            )
            .ignore();
            pipe.sadd(key_user_assets(*user_id), asset).ignore();
        }
        pipe.query_async::<()>(conn).await?;
        stats.accounts = acct_rows.len() as u64;
    }

    Ok(stats)
}

/// Wipe every key managed by this module — used by tests and by an explicit
/// re-hydrate command (`redis-writer --rehydrate`). Walks `active_orders`
/// and `user_assets` to find dependent keys; does NOT call FLUSHALL.
pub async fn purge_all(
    conn: &mut redis::aio::MultiplexedConnection,
) -> anyhow::Result<()> {
    let order_ids: Vec<i64> = conn.smembers(KEY_ACTIVE_ORDERS).await.unwrap_or_default();
    let mut user_ids: std::collections::HashSet<i64> = std::collections::HashSet::new();
    for id in &order_ids {
        let user_id: Option<i64> = conn.hget(key_order(*id), "user_id").await.unwrap_or(None);
        if let Some(uid) = user_id {
            user_ids.insert(uid);
        }
        let _: i64 = conn.del(key_order(*id)).await.unwrap_or(0);
    }
    let _: i64 = conn.del(KEY_ACTIVE_ORDERS).await.unwrap_or(0);
    // Find every (user_id, asset) by scanning user_assets and READING the
    // set members BEFORE deleting any keys — earlier we deleted user_assets:*
    // before SMEMBERS could read them, leaving acct:{uid}:{asset} hashes orphaned.
    let mut scan_cursor: redis::AsyncIter<String> =
        conn.scan_match("user_assets:*").await?;
    let mut user_asset_keys: Vec<String> = Vec::new();
    while let Some(k) = scan_cursor.next_item().await {
        user_asset_keys.push(k);
    }
    drop(scan_cursor);
    for k in &user_asset_keys {
        if let Some(rest) = k.strip_prefix("user_assets:") {
            if let Ok(uid) = rest.parse::<i64>() {
                user_ids.insert(uid);
            }
        }
    }
    // First pass: collect assets per user, then delete the per-asset hashes.
    for uid in &user_ids {
        let assets: Vec<String> = conn
            .smembers(key_user_assets(*uid))
            .await
            .unwrap_or_default();
        for asset in assets {
            let _: i64 = conn.del(key_account(*uid, &asset)).await.unwrap_or(0);
        }
    }
    // Second pass: tear down the index keys themselves.
    for uid in &user_ids {
        let _: i64 = conn.del(key_user_orders(*uid)).await.unwrap_or(0);
        let _: i64 = conn.del(key_user_coid(*uid)).await.unwrap_or(0);
        let _: i64 = conn.del(key_user_assets(*uid)).await.unwrap_or(0);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_helpers() {
        assert_eq!(key_order(42), "order:42");
        assert_eq!(key_user_orders(7), "user_orders:7");
        assert_eq!(key_account(7, "USDT"), "acct:7:USDT");
        assert_eq!(key_user_assets(7), "user_assets:7");
        assert_eq!(key_user_coid(7), "user_coid:7");
    }
}
