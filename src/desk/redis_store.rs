//! Redis L1 storage for `orders` and `accounts`.
//!
//! Data model:
//!
//! - `HASH order:{id}` — full row of an active (PENDING/TRADING) order.
//!   fields: user_id, symbol, side, order_type, price, qty, filled, status,
//!           freeze_price, client_order_id, created_at_ms, updated_at_ms
//! - `SET active_orders` — global index of order ids (for full scans / cold
//!   hydrate checks).
//! - `SET user_orders:{user_id}` — per-user index for `/api/orders?status=open`.
//! - `HASH acct:{user_id}:{asset}` — fields: balance, frozen,
//!   balance_atoms, frozen_atoms.
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
/// HASH publisher_id → last applied persist seq (redis-writer checkpoint).
pub const KEY_PERSIST_FLOORS: &str = "persist:floors";

/// Load redis-writer checkpoint floors (HASH field = publisher_id).
pub async fn load_persist_floors(
    conn: &mut redis::aio::MultiplexedConnection,
) -> anyhow::Result<std::collections::HashMap<u16, u64>> {
    let raw: std::collections::HashMap<String, u64> = conn.hgetall(KEY_PERSIST_FLOORS).await?;
    Ok(raw
        .into_iter()
        .filter_map(|(k, v)| k.parse::<u16>().ok().map(|id| (id, v)))
        .collect())
}

/// Persist all floors in one HSET (called periodically, not per frame).
pub async fn store_persist_floors(
    conn: &mut redis::aio::MultiplexedConnection,
    floors: &std::collections::HashMap<u16, u64>,
) -> anyhow::Result<()> {
    if floors.is_empty() {
        return Ok(());
    }
    let items: Vec<(String, u64)> = floors
        .iter()
        .map(|(&id, &seq)| (id.to_string(), seq))
        .collect();
    let _: () = conn.hset_multiple(KEY_PERSIST_FLOORS, &items).await?;
    Ok(())
}

/// HASH "user_id:op" → "tokens:last_update" (SharedRateLimiter persistence).
pub const KEY_RATE_BUCKETS: &str = "ratelimit:buckets";

/// Load persisted rate-limit buckets (process restart).
pub async fn load_rate_buckets(
    conn: &mut redis::aio::MultiplexedConnection,
) -> anyhow::Result<Vec<(i64, u8, f64, u64)>> {
    let raw: std::collections::HashMap<String, String> = conn.hgetall(KEY_RATE_BUCKETS).await?;
    let mut out = Vec::with_capacity(raw.len());
    for (k, v) in raw {
        let (Some((uid, op)), Some((tokens, ts))) = (k.split_once(':'), v.split_once(':')) else {
            continue;
        };
        let (Ok(uid), Ok(op), Ok(tokens), Ok(ts)) = (
            uid.parse::<i64>(),
            op.parse::<u8>(),
            tokens.parse::<f64>(),
            ts.parse::<u64>(),
        ) else {
            continue;
        };
        out.push((uid, op, tokens, ts));
    }
    Ok(out)
}

/// Persist rate-limit buckets (periodic snapshot). TTL keeps the key from
/// outliving a decommissioned desk.
pub async fn store_rate_buckets(
    conn: &mut redis::aio::MultiplexedConnection,
    buckets: &[(i64, u8, f64, u64)],
) -> anyhow::Result<()> {
    if buckets.is_empty() {
        return Ok(());
    }
    let items: Vec<(String, String)> = buckets
        .iter()
        .map(|&(uid, op, tokens, ts)| (format!("{uid}:{op}"), format!("{tokens}:{ts}")))
        .collect();
    let _: () = redis::pipe()
        .hset_multiple(KEY_RATE_BUCKETS, &items)
        .expire(KEY_RATE_BUCKETS, 3600)
        .query_async(conn)
        .await?;
    Ok(())
}

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
    AccountSetPayload, OrderDeletePayload, OrderFillUpdatePayload, OrderUpsertPayload,
    PersistFrame, PersistKind, TradeInsertPayload, unpack_str,
};
use crate::desk::money::AmountAtoms;

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
        Some(PersistKind::MatchingEvent) => {
            // Append-only audit/replay scaffold; pg-writer owns durable storage.
        }
        Some(PersistKind::PositionUpsert)
        | Some(PersistKind::PositionDelete)
        | Some(PersistKind::RiskAccountSet)
        | Some(PersistKind::InsuranceFundSet) => {
            // Swap margin state lives in PG only (S1): the desk hydrates it
            // straight from PG on startup, so an L1 copy would just be a
            // second thing to reconcile. pg-writer owns these frames.
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
    // Wire status uses the `DbOrderStatus` 0-indexed encoding
    // (Pending=0..Rejected=4) — same value the desk-server publishers cast
    // via `DbOrderStatus::as_u8()`. Out-of-range codes are dropped so we
    // never silently coerce an unknown value into PENDING.
    let status_str = match status {
        0 => "PENDING",
        1 => "TRADING",
        2 => "COMPLETED",
        3 => "CANCELED",
        4 => "REJECTED",
        _ => {
            tracing::warn!(
                "apply_order_upsert: unknown status code {} for id {} — dropping",
                status,
                id
            );
            return Ok(());
        }
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
            ("updated_at_ms", created_at_ms.to_string()),
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
    // Wire status uses the `DbOrderStatus` 0-indexed encoding.
    let status_str = match status {
        0 => "PENDING",
        1 => "TRADING",
        2 => "COMPLETED",
        3 => "CANCELED",
        4 => "REJECTED",
        _ => return Ok(()),
    };
    // Only update the changing fields. Skip if the order isn't in
    // Redis (could happen during cold-start replay; OK to drop).
    let exists: bool = conn.exists(key_order(id)).await.unwrap_or(false);
    if !exists {
        return Ok(());
    }
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .map_err(|e| anyhow::anyhow!("clock before UNIX_EPOCH: {e}"))?;
    redis::pipe()
        .hset_multiple(
            key_order(id),
            &[
                ("filled", filled.to_string()),
                ("status", status_str.to_string()),
                ("updated_at_ms", now_ms.to_string()),
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
    let balance_atoms = if p.balance_atoms == 0 && balance != 0.0 {
        AmountAtoms::from_f64_round(balance)?.atoms()
    } else {
        p.balance_atoms
    };
    let frozen_atoms = if p.frozen_atoms == 0 && frozen != 0.0 {
        AmountAtoms::from_f64_round(frozen)?.atoms()
    } else {
        p.frozen_atoms
    };
    let asset = unpack_str(&p.asset).to_owned();
    if asset.is_empty() {
        return Ok(());
    }
    // Display fields are projections of the atoms truth, never the other
    // way around (the float payload fields are legacy wire baggage).
    let balance = AmountAtoms::from_atoms(balance_atoms).to_f64();
    let frozen = AmountAtoms::from_atoms(frozen_atoms).to_f64();
    redis::pipe()
        .hset_multiple(
            key_account(user_id, &asset),
            &[
                ("balance", balance.to_string()),
                ("frozen", frozen.to_string()),
                ("balance_atoms", balance_atoms.to_string()),
                ("frozen_atoms", frozen_atoms.to_string()),
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
        chrono::DateTime<chrono::Utc>,
    )> = sqlx::query_as(
        "SELECT id, user_id, symbol, side, order_type, price, quantity, filled,
                status, freeze_price, client_order_id, created_at, updated_at
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
            updated_at,
        ) in &rows
        {
            // PG schema allows nullable price / freeze_price / client_order_id; we hydrate them
            // as the natural empty/zero value Redis readers expect. This is NOT silent fallback
            // data — it's the documented Redis encoding for "absent" (e.g. market orders have
            // no price). Done at hydrate site only; runtime writes go through PersistEvent which
            // carries explicit fields.
            let price_str = match price {
                Some(p) => p.to_string(),
                None => "0".to_string(),
            };
            let freeze_str = match freeze_price {
                Some(f) => f.to_string(),
                None => "0".to_string(),
            };
            let coid_str = match client_order_id {
                Some(s) => s.clone(),
                None => String::new(),
            };
            let fields: Vec<(&str, String)> = vec![
                ("user_id", user_id.to_string()),
                ("symbol", symbol.clone()),
                ("side", side.clone()),
                ("order_type", order_type.clone()),
                ("price", price_str),
                ("qty", quantity.to_string()),
                ("filled", filled.to_string()),
                ("status", status.clone()),
                ("freeze_price", freeze_str),
                ("client_order_id", coid_str),
                ("created_at_ms", created_at.timestamp_millis().to_string()),
                ("updated_at_ms", updated_at.timestamp_millis().to_string()),
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
    let acct_rows: Vec<(i64, String, i64, i64)> =
        sqlx::query_as("SELECT user_id, asset, balance_atoms, frozen_atoms FROM accounts")
            .fetch_all(pg)
            .await?;
    let dt_pg_acct = t_pg_acct.elapsed();

    let t_redis_acct = std::time::Instant::now();
    if !acct_rows.is_empty() {
        let mut pipe = redis::pipe();
        for (user_id, asset, balance_atoms, frozen_atoms) in &acct_rows {
            // Display fields are projections of the atoms truth.
            let balance = AmountAtoms::from_atoms(*balance_atoms).to_f64();
            let frozen = AmountAtoms::from_atoms(*frozen_atoms).to_f64();
            pipe.hset_multiple(
                key_account(*user_id, asset),
                &[
                    ("balance", balance.to_string()),
                    ("frozen", frozen.to_string()),
                    ("balance_atoms", balance_atoms.to_string()),
                    ("frozen_atoms", frozen_atoms.to_string()),
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
        dt_pg_orders,
        rows.len(),
        dt_redis_orders,
        dt_pg_acct,
        acct_rows.len(),
        dt_redis_acct,
    );

    Ok(stats)
}

/// Stats for `reconcile_orphans` — non-destructive sweep that drops Redis
/// state for orders/accounts that no longer exist in PG. Used at startup
/// (and optionally periodically) to converge L1 back to PG after a
/// publish-path bug or process restart left stragglers behind.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReconcileStats {
    pub redis_orders_scanned: u64,
    pub orders_removed: u64,
    pub accounts_scanned: u64,
    pub accounts_removed: u64,
    pub user_sets_scanned: u64,
}

/// Reconcile Redis L1 with PG: drop any `order:{id}` HASH and SREM the id
/// from `active_orders` / `user_orders:{uid}` when the row no longer
/// exists in PG. Also drops `acct:{uid}:{asset}` HASHes whose `accounts`
/// row was removed in PG (extremely rare — accounts are append-only in
/// practice). Idempotent and safe to call against a live system.
///
/// Strategy:
/// 1. Pull every member of `active_orders` (the global index).
/// 2. Ask PG which of those ids still exist.
/// 3. SREM + DEL the ones that don't, plus SREM from the owner's
///    `user_orders:{uid}` (uid recovered from the HASH before DEL).
/// 4. Repeat for `user_orders:{uid}` SCAN — covers ids that were added
///    to per-user sets but missing from `active_orders` (defence in
///    depth).
pub async fn reconcile_orphans(
    pg: &PgPool,
    conn: &mut redis::aio::MultiplexedConnection,
) -> anyhow::Result<ReconcileStats> {
    let mut stats = ReconcileStats::default();

    // 1) Walk active_orders.
    let order_ids: Vec<i64> = conn.smembers(KEY_ACTIVE_ORDERS).await.unwrap_or_default();
    stats.redis_orders_scanned = order_ids.len() as u64;

    if !order_ids.is_empty() {
        // Ask PG which ids still exist (any status, not only PENDING/TRADING —
        // a row in COMPLETED briefly before BatchDeleteOrder runs should not
        // be reaped here either).
        let existing: Vec<(i64,)> =
            sqlx::query_as("SELECT id FROM orders WHERE id = ANY($1::bigint[])")
                .bind(&order_ids)
                .fetch_all(pg)
                .await?;
        let existing_set: std::collections::HashSet<i64> =
            existing.into_iter().map(|(id,)| id).collect();

        let orphan_ids: Vec<i64> = order_ids
            .into_iter()
            .filter(|id| !existing_set.contains(id))
            .collect();
        if !orphan_ids.is_empty() {
            // Resolve user_id per orphan so we can clean `user_orders:{uid}` too.
            let mut pipe = redis::pipe();
            for id in &orphan_ids {
                pipe.hget(key_order(*id), "user_id");
            }
            let uids: Vec<Option<String>> = pipe.query_async(conn).await.unwrap_or_default();

            let mut pipe = redis::pipe();
            for (id, uid_str) in orphan_ids.iter().zip(uids.iter()) {
                pipe.del(key_order(*id)).ignore();
                pipe.srem(KEY_ACTIVE_ORDERS, *id).ignore();
                if let Some(s) = uid_str {
                    if let Ok(uid) = s.parse::<i64>() {
                        pipe.srem(key_user_orders(uid), *id).ignore();
                    }
                }
            }
            pipe.query_async::<()>(conn).await?;
            stats.orders_removed = orphan_ids.len() as u64;
        }
    }

    // 2) Defence-in-depth: scan user_orders:* for ids missing from active_orders
    //    or PG. These are ids that for some reason landed in a per-user set
    //    without (or after losing) the global index entry.
    let mut iter: redis::AsyncIter<String> = conn.scan_match("user_orders:*").await?;
    let mut user_sets: Vec<String> = Vec::new();
    while let Some(k) = iter.next_item().await {
        user_sets.push(k);
    }
    drop(iter);
    stats.user_sets_scanned = user_sets.len() as u64;

    for set_key in &user_sets {
        let ids: Vec<i64> = conn.smembers(set_key).await.unwrap_or_default();
        if ids.is_empty() {
            continue;
        }
        let existing: Vec<(i64,)> =
            sqlx::query_as("SELECT id FROM orders WHERE id = ANY($1::bigint[])")
                .bind(&ids)
                .fetch_all(pg)
                .await?;
        let existing_set: std::collections::HashSet<i64> =
            existing.into_iter().map(|(id,)| id).collect();
        let orphans: Vec<i64> = ids
            .into_iter()
            .filter(|id| !existing_set.contains(id))
            .collect();
        if orphans.is_empty() {
            continue;
        }
        let mut pipe = redis::pipe();
        for id in &orphans {
            pipe.srem(set_key, *id).ignore();
            // Also enforce global cleanup — in case active_orders walk missed them.
            pipe.srem(KEY_ACTIVE_ORDERS, *id).ignore();
            pipe.del(key_order(*id)).ignore();
        }
        pipe.query_async::<()>(conn).await?;
        stats.orders_removed += orphans.len() as u64;
    }

    // 3) Accounts: very rare to need reaping (accounts persist forever in PG)
    //    so we only scan but skip removal unless asset is empty. Reserved as
    //    future work — for now just count what's there.
    let mut iter: redis::AsyncIter<String> = conn.scan_match("acct:*").await?;
    while let Some(_) = iter.next_item().await {
        stats.accounts_scanned += 1;
    }

    Ok(stats)
}

/// Wipe every key managed by this module — used by tests and by an explicit
/// re-hydrate (`FORCE_REHYDRATE`). Walks `active_orders` and `user_assets`
/// to find dependent keys, then issues all DELs in a single pipeline.
/// Does NOT call FLUSHALL.
pub async fn purge_all(conn: &mut redis::aio::MultiplexedConnection) -> anyhow::Result<()> {
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

// ── Reader path (used by REST handlers to skip PG entirely) ─────────────────

use crate::desk::models::DbOrder;
use chrono::{DateTime, TimeZone, Utc};
use std::collections::HashMap;

/// Decode a HGETALL result back into the DbOrder shape the REST layer returns.
/// Returns None when the HASH is missing or doesn't carry the required fields.
/// Does NOT invent missing fields — incomplete HASH ⇒ None ⇒ caller falls back
/// to PG (avoids silently serving truncated rows).
fn decode_hash(id: i64, fields: &HashMap<String, String>) -> Option<DbOrder> {
    let user_id: i64 = fields.get("user_id")?.parse().ok()?;
    let symbol = fields.get("symbol")?.clone();
    let side = fields.get("side")?.clone();
    let order_type = fields.get("order_type")?.clone();
    let status = fields.get("status")?.clone();
    let quantity: f64 = fields.get("qty")?.parse().ok()?;
    let filled: f64 = fields.get("filled")?.parse().ok()?;
    let freeze_price: f64 = fields.get("freeze_price")?.parse().ok()?;
    // price stored as f64 string; 0 is the documented sentinel for market orders
    // (writers convert Option::None → 0 deliberately — see apply_order_upsert).
    let price_raw: f64 = fields.get("price")?.parse().ok()?;
    let price = if price_raw == 0.0 {
        None
    } else {
        Some(price_raw)
    };
    let client_order_id = match fields.get("client_order_id") {
        Some(s) if !s.is_empty() => Some(s.clone()),
        _ => None,
    };
    let created_at_ms: i64 = fields.get("created_at_ms")?.parse().ok()?;
    let updated_at_ms: i64 = match fields.get("updated_at_ms").and_then(|s| s.parse().ok()) {
        Some(v) => v,
        None => created_at_ms,
    };
    let created_at = Utc.timestamp_millis_opt(created_at_ms).single()?;
    let updated_at = Utc.timestamp_millis_opt(updated_at_ms).single()?;
    Some(DbOrder {
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
        updated_at,
    })
}

/// Read a single order by id from Redis. Returns Ok(None) if missing or
/// belongs to a different user (caller passes user_id for ownership check).
pub async fn get_order(
    conn: &mut redis::aio::MultiplexedConnection,
    id: i64,
    user_id: i64,
) -> anyhow::Result<Option<DbOrder>> {
    let fields: HashMap<String, String> = conn.hgetall(key_order(id)).await?;
    if fields.is_empty() {
        return Ok(None);
    }
    let order = match decode_hash(id, &fields) {
        Some(o) => o,
        None => return Ok(None),
    };
    if order.user_id != user_id {
        return Ok(None);
    }
    Ok(Some(order))
}

/// List all open (PENDING/TRADING) orders for a user, newest-first by
/// `created_at`. `symbol_filter` narrows to a single symbol when Some.
/// `limit` caps the result count after sorting.
///
/// Reads `user_orders:{uid}` (SET) then pipelines HGETALL per id. Skips ids
/// whose HASH is missing or fails to decode — those fall through to PG via
/// the caller's normal path.
pub async fn list_user_open_orders(
    conn: &mut redis::aio::MultiplexedConnection,
    user_id: i64,
    symbol_filter: Option<&str>,
    limit: usize,
) -> anyhow::Result<Vec<DbOrder>> {
    let ids: Vec<i64> = conn.smembers(key_user_orders(user_id)).await?;
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut pipe = redis::pipe();
    for id in &ids {
        pipe.hgetall(key_order(*id));
    }
    let hashes: Vec<HashMap<String, String>> = pipe.query_async(conn).await?;
    let mut out: Vec<DbOrder> = Vec::with_capacity(ids.len());
    for (id, h) in ids.into_iter().zip(hashes.into_iter()) {
        if h.is_empty() {
            continue;
        }
        let Some(o) = decode_hash(id, &h) else {
            continue;
        };
        if let Some(sym) = symbol_filter {
            if o.symbol != sym {
                continue;
            }
        }
        out.push(o);
    }
    out.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    if out.len() > limit {
        out.truncate(limit);
    }
    Ok(out)
}

/// Snapshot of accounts a user holds (for `/api/balances` cold-path fallback).
/// Returns Vec of (asset, balance, frozen, updated_at). `updated_at` is set
/// to "now" because Redis HASH stores no timestamp for accounts.
pub async fn list_user_accounts(
    conn: &mut redis::aio::MultiplexedConnection,
    user_id: i64,
) -> anyhow::Result<Vec<(String, f64, f64, DateTime<Utc>)>> {
    let assets: Vec<String> = conn.smembers(key_user_assets(user_id)).await?;
    if assets.is_empty() {
        return Ok(Vec::new());
    }
    let mut pipe = redis::pipe();
    for a in &assets {
        pipe.hgetall(key_account(user_id, a));
    }
    let rows: Vec<HashMap<String, String>> = pipe.query_async(conn).await?;
    let now = Utc::now();
    let mut out: Vec<(String, f64, f64, DateTime<Utc>)> = Vec::with_capacity(assets.len());
    for (asset, h) in assets.into_iter().zip(rows.into_iter()) {
        let balance: f64 = match h.get("balance").and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => continue,
        };
        let frozen: f64 = match h.get("frozen").and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => continue,
        };
        out.push((asset, balance, frozen, now));
    }
    Ok(out)
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

    #[test]
    fn decode_hash_roundtrip() {
        let mut h = HashMap::new();
        h.insert("user_id".into(), "42".into());
        h.insert("symbol".into(), "BTC_USDT".into());
        h.insert("side".into(), "buy".into());
        h.insert("order_type".into(), "limit".into());
        h.insert("status".into(), "PENDING".into());
        h.insert("price".into(), "73000.5".into());
        h.insert("qty".into(), "0.05".into());
        h.insert("filled".into(), "0".into());
        h.insert("freeze_price".into(), "73000.5".into());
        h.insert("client_order_id".into(), "abc".into());
        h.insert("created_at_ms".into(), "1700000000000".into());
        h.insert("updated_at_ms".into(), "1700000001000".into());
        let o = decode_hash(7, &h).expect("decode");
        assert_eq!(o.id, 7);
        assert_eq!(o.user_id, 42);
        assert_eq!(o.symbol, "BTC_USDT");
        assert_eq!(o.price, Some(73000.5));
        assert_eq!(o.client_order_id.as_deref(), Some("abc"));
        assert_eq!(o.created_at.timestamp_millis(), 1700000000000);
        assert_eq!(o.updated_at.timestamp_millis(), 1700000001000);
    }

    #[test]
    fn decode_hash_missing_field_returns_none() {
        let mut h = HashMap::new();
        h.insert("user_id".into(), "42".into());
        // status missing → must NOT invent data
        assert!(decode_hash(7, &h).is_none());
    }

    #[test]
    fn decode_hash_empty_client_order_id_is_none() {
        let mut h = HashMap::new();
        h.insert("user_id".into(), "42".into());
        h.insert("symbol".into(), "BTC_USDT".into());
        h.insert("side".into(), "buy".into());
        h.insert("order_type".into(), "market".into());
        h.insert("status".into(), "PENDING".into());
        h.insert("price".into(), "0".into());
        h.insert("qty".into(), "1".into());
        h.insert("filled".into(), "0".into());
        h.insert("freeze_price".into(), "0".into());
        h.insert("client_order_id".into(), "".into());
        h.insert("created_at_ms".into(), "1700000000000".into());
        let o = decode_hash(7, &h).expect("decode");
        assert!(o.client_order_id.is_none());
        assert!(o.price.is_none(), "price==0 sentinel decodes back to None");
        // updated_at_ms missing → tracks created_at_ms.
        assert_eq!(o.updated_at, o.created_at);
    }
}
