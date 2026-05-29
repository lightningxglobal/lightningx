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

// ── Frame application (used by redis-writer subscriber loop) ────────────────

use crate::transport::persist_event::{
    unpack_str, AccountSetPayload, OrderDeletePayload, OrderFillUpdatePayload, OrderUpsertPayload,
    PersistFrame, PersistKind, TradeInsertPayload,
};

/// Apply one PersistFrame to Redis L1. Idempotent and safe to replay
/// (subscriber may see duplicates after Aeron lag recovery).
pub async fn apply_frame(
    conn: &mut redis::aio::MultiplexedConnection,
    frame: &PersistFrame,
) -> anyhow::Result<()> {
    match frame.kind() {
        Some(PersistKind::OrderUpsert) => {
            if let Some(p) = frame.as_order_upsert() {
                apply_order_upsert(conn, &p).await?;
            }
        }
        Some(PersistKind::OrderDelete) => {
            if let Some(p) = frame.as_order_delete() {
                apply_order_delete(conn, &p).await?;
            }
        }
        Some(PersistKind::AccountSet) => {
            if let Some(p) = frame.as_account_set() {
                apply_account_set(conn, &p).await?;
            }
        }
        Some(PersistKind::OrderFillUpdate) => {
            if let Some(p) = frame.as_order_fill_update() {
                apply_order_fill_update(conn, &p).await?;
            }
        }
        Some(PersistKind::TradeInsert) => {
            // trades aren't held in Redis (append-only history); skip.
            // pg-writer is responsible for trades.
        }
        None => {
            tracing::warn!("PersistFrame with unknown kind={}", frame.kind);
        }
    }
    Ok(())
}

async fn apply_order_upsert(
    conn: &mut redis::aio::MultiplexedConnection,
    p: &OrderUpsertPayload,
) -> anyhow::Result<()> {
    // Copy packed fields to locals so we can borrow them.
    let id: i64 = p.id;
    let user_id: i64 = p.user_id;
    let side: u8 = p.side;
    let status: u8 = p.status;
    let price: f64 = p.price;
    let qty: f64 = p.qty;
    let filled: f64 = p.filled;
    let freeze_price: f64 = p.freeze_price;
    let created_at_ms: i64 = p.created_at_ms;
    let symbol = unpack_str(&p.symbol).to_owned();
    let order_type = unpack_str(&p.order_type).to_owned();
    let coid_str = unpack_str(&p.client_order_id).to_owned();

    let side_str = if side == 0 { "buy" } else { "sell" };
    let status_str = match status {
        1 => "PENDING",
        2 => "TRADING",
        3 => "COMPLETED",
        4 => "CANCELED",
        5 => "REJECTED",
        _ => "PENDING",
    };
    let mut pipe = redis::pipe();
    pipe.hset_multiple(
        key_order(id),
        &[
            ("user_id", user_id.to_string()),
            ("symbol", symbol),
            ("side", side_str.to_string()),
            ("order_type", order_type),
            ("price", price.to_string()),
            ("qty", qty.to_string()),
            ("filled", filled.to_string()),
            ("status", status_str.to_string()),
            ("freeze_price", freeze_price.to_string()),
            ("client_order_id", coid_str.clone()),
            ("created_at_ms", created_at_ms.to_string()),
        ],
    )
    .ignore();
    pipe.sadd(KEY_ACTIVE_ORDERS, id).ignore();
    pipe.sadd(key_user_orders(user_id), id).ignore();
    if !coid_str.is_empty() {
        pipe.hset(key_user_coid(user_id), coid_str, id).ignore();
    }
    pipe.query_async::<()>(conn).await?;
    Ok(())
}

async fn apply_order_delete(
    conn: &mut redis::aio::MultiplexedConnection,
    p: &OrderDeletePayload,
) -> anyhow::Result<()> {
    let id: i64 = p.id;
    // Read user_id + client_order_id so we can clean per-user indices too.
    let pre: (Option<String>, Option<String>) = redis::pipe()
        .hget(key_order(id), "user_id")
        .hget(key_order(id), "client_order_id")
        .query_async(conn)
        .await
        .unwrap_or((None, None));
    let user_id: Option<i64> = pre.0.and_then(|s| s.parse().ok());
    let coid: Option<String> = pre.1.filter(|s| !s.is_empty());

    let mut pipe = redis::pipe();
    pipe.del(key_order(id)).ignore();
    pipe.srem(KEY_ACTIVE_ORDERS, id).ignore();
    if let Some(uid) = user_id {
        pipe.srem(key_user_orders(uid), id).ignore();
        if let Some(c) = coid {
            pipe.hdel(key_user_coid(uid), c).ignore();
        }
    }
    pipe.query_async::<()>(conn).await?;
    Ok(())
}

async fn apply_order_fill_update(
    conn: &mut redis::aio::MultiplexedConnection,
    p: &OrderFillUpdatePayload,
) -> anyhow::Result<()> {
    let id: i64 = p.id;
    let filled: f64 = p.filled;
    let status: u8 = p.status;
    let status_str = match status {
        1 => "PENDING",
        2 => "TRADING",
        3 => "COMPLETED",
        4 => "CANCELED",
        5 => "REJECTED",
        _ => return Ok(()),
    };
    // Only update the two changing fields. Skip if the order isn't in
    // Redis (could happen during cold-start replay; OK to drop).
    let exists: bool = conn.exists(key_order(id)).await.unwrap_or(false);
    if !exists {
        return Ok(());
    }
    redis::pipe()
        .hset_multiple(
            key_order(id),
            &[
                ("filled", filled.to_string()),
                ("status", status_str.to_string()),
            ],
        )
        .query_async::<()>(conn)
        .await?;
    Ok(())
}

async fn apply_account_set(
    conn: &mut redis::aio::MultiplexedConnection,
    p: &AccountSetPayload,
) -> anyhow::Result<()> {
    let user_id: i64 = p.user_id;
    let balance: f64 = p.balance;
    let frozen: f64 = p.frozen;
    let asset = unpack_str(&p.asset).to_owned();
    if asset.is_empty() {
        return Ok(());
    }
    redis::pipe()
        .hset_multiple(
            key_account(user_id, &asset),
            &[
                ("balance", balance.to_string()),
                ("frozen", frozen.to_string()),
            ],
        )
        .sadd(key_user_assets(user_id), &asset)
        .query_async::<()>(conn)
        .await?;
    Ok(())
}

// Trade inserts are a no-op in Redis (trades aren't kept here). Kept as a
// `_` parameter so the import stays.
#[allow(dead_code)]
fn _no_op_trade_insert(_p: &TradeInsertPayload) {}

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

    let t_pg_orders = std::time::Instant::now();
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
    let dt_pg_orders = t_pg_orders.elapsed();

    let t_redis_orders = std::time::Instant::now();
    if !rows.is_empty() {
        let mut pipe = redis::pipe();
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
    let dt_redis_orders = t_redis_orders.elapsed();

    let t_pg_acct = std::time::Instant::now();
    // 2) Accounts.
    let acct_rows: Vec<(i64, String, f64, f64)> = sqlx::query_as(
        "SELECT user_id, asset, balance, frozen FROM accounts",
    )
    .fetch_all(pg)
    .await?;
    let dt_pg_acct = t_pg_acct.elapsed();

    let t_redis_acct = std::time::Instant::now();
    if !acct_rows.is_empty() {
        let mut pipe = redis::pipe();
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
    let dt_redis_acct = t_redis_acct.elapsed();

    tracing::info!(
        "hydrate timings: PG orders={:?} ({} rows), Redis orders write={:?}, PG accounts={:?} ({} rows), Redis accounts write={:?}",
        dt_pg_orders, rows.len(), dt_redis_orders,
        dt_pg_acct, acct_rows.len(), dt_redis_acct,
    );

    Ok(stats)
}

/// Wipe every key managed by this module — used by tests and by an explicit
/// re-hydrate (`FORCE_REHYDRATE`). Walks `active_orders` and `user_assets`
/// to find dependent keys, then issues all DELs in a single pipeline.
/// Does NOT call FLUSHALL.
pub async fn purge_all(
    conn: &mut redis::aio::MultiplexedConnection,
) -> anyhow::Result<()> {
    // Phase 1 — read all the index keys (one pipelined batch). After this
    // we know every key to delete; no more SCAN/SMEMBERS needed.
    let order_ids: Vec<i64> = conn.smembers(KEY_ACTIVE_ORDERS).await.unwrap_or_default();

    let mut scan_cursor: redis::AsyncIter<String> = conn.scan_match("user_assets:*").await?;
    let mut user_asset_keys: Vec<String> = Vec::new();
    while let Some(k) = scan_cursor.next_item().await {
        user_asset_keys.push(k);
    }
    drop(scan_cursor);

    let mut user_ids: std::collections::HashSet<i64> = std::collections::HashSet::new();
    for k in &user_asset_keys {
        if let Some(rest) = k.strip_prefix("user_assets:") {
            if let Ok(uid) = rest.parse::<i64>() {
                user_ids.insert(uid);
            }
        }
    }

    // Read user_id for every active order + the asset list per user — pipelined.
    let (order_user_ids, assets_per_user): (Vec<Option<i64>>, Vec<Vec<String>>) = {
        let mut pipe = redis::pipe();
        for id in &order_ids {
            pipe.hget(key_order(*id), "user_id");
        }
        let oids: Vec<Option<i64>> = if order_ids.is_empty() {
            Vec::new()
        } else {
            pipe.query_async(conn).await.unwrap_or_default()
        };

        let mut pipe = redis::pipe();
        let user_id_list: Vec<i64> = user_ids.iter().copied().collect();
        for uid in &user_id_list {
            pipe.smembers(key_user_assets(*uid));
        }
        let assets: Vec<Vec<String>> = if user_id_list.is_empty() {
            Vec::new()
        } else {
            pipe.query_async(conn).await.unwrap_or_default()
        };
        // Persist user_id order so the asset list maps back correctly.
        // We'll iterate via zip below.
        (oids, assets)
    };

    for uid in order_user_ids.into_iter().flatten() {
        user_ids.insert(uid);
    }
    let user_id_list: Vec<i64> = user_ids.iter().copied().collect();

    // Phase 2 — fire every DEL in one pipeline.
    let mut pipe = redis::pipe();
    for id in &order_ids {
        pipe.del(key_order(*id)).ignore();
    }
    pipe.del(KEY_ACTIVE_ORDERS).ignore();
    for (uid, assets) in user_id_list.iter().zip(assets_per_user.iter()) {
        for asset in assets {
            pipe.del(key_account(*uid, asset)).ignore();
        }
    }
    for uid in &user_id_list {
        pipe.del(key_user_orders(*uid)).ignore();
        pipe.del(key_user_coid(*uid)).ignore();
        pipe.del(key_user_assets(*uid)).ignore();
    }
    pipe.query_async::<()>(conn).await?;
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
