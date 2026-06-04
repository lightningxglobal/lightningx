//! Position calculation: per-user, per-base-asset cost basis, market value, and PnL.
//!
//! Quote asset is hard-coded to USDT for MVP (one symbol per base). `entry_price`
//! is the volume-weighted average of all the user's *buy* fills on the symbol
//! (i.e. the cost basis). When the user has never bought (only received test
//! funds), `entry_price` is 0.0 and PnL fields are returned as `None`.

use crate::api::{AccountCache, VwapCache};
use serde::Serialize;
use sqlx::PgPool;

#[derive(Debug, Clone, Serialize)]
pub struct Position {
    pub asset: String,
    pub symbol: String,
    pub quantity: f64,
    pub frozen: f64,
    pub available: f64,
    pub entry_price: f64,
    pub current_price: Option<f64>,
    pub market_value: Option<f64>,
    pub unrealized_pnl: Option<f64>,
    pub unrealized_pnl_pct: Option<f64>,
}

/// All non-USDT positions with non-zero balance for the user. In-memory
/// only — pulls (balance, frozen) from account_cache, current_price from
/// last_trade_price, and entry_price from the running vwap_cache. This
/// replaces a 3-query PG path (the 3rd of which was a 90ms aggregate
/// over the trades table per call).
pub fn all_positions_in_memory(
    user_id: i64,
    account_cache: &AccountCache,
    last_trade_price: &dashmap::DashMap<String, f64>,
    vwap_cache: &VwapCache,
) -> Vec<Position> {
    let Some(assets_ref) = account_cache.get(&user_id) else {
        return Vec::new();
    };
    let mut out: Vec<Position> = Vec::with_capacity(assets_ref.len());
    for (asset, &(balance, frozen)) in assets_ref.iter() {
        if asset == "USDT" || balance <= 0.0 {
            continue;
        }
        let symbol = format!("{}_USDT", asset);
        let current_price: Option<f64> = last_trade_price.get(&symbol).map(|v| *v);
        let vwap = vwap_cache
            .get(&(user_id, asset.clone()))
            .map(|v| (v.weighted_sum, v.qty_sum))
            .unwrap_or((0.0, 0.0));
        let entry_price = if vwap.1 > 0.0 { vwap.0 / vwap.1 } else { 0.0 };
        let available = balance - frozen;
        let market_value = current_price.map(|p| p * balance);
        let (unrealized_pnl, unrealized_pnl_pct) = match current_price {
            Some(c) if entry_price > 0.0 => (
                Some((c - entry_price) * available),
                Some((c - entry_price) / entry_price * 100.0),
            ),
            _ => (None, None),
        };
        out.push(Position {
            asset: asset.clone(),
            symbol,
            quantity: balance,
            frozen,
            available,
            entry_price,
            current_price,
            market_value,
            unrealized_pnl,
            unrealized_pnl_pct,
        });
    }
    out.sort_by(|a, b| a.asset.cmp(&b.asset));
    out
}

/// Build a Position row for one base asset (PG fallback path — used by
/// position_update_msg below for the WS push). Slower than the cached
/// path; only called from the WS settle handler.
pub async fn position_for_user_asset(
    pool: &PgPool,
    user_id: i64,
    asset: &str,
) -> Option<Position> {
    if asset == "USDT" {
        return None;
    }
    let symbol = format!("{}_USDT", asset);

    let (balance, frozen): (f64, f64) = sqlx::query_as(
        "SELECT balance, frozen FROM accounts WHERE user_id=$1 AND asset=$2",
    )
    .bind(user_id)
    .bind(asset)
    .fetch_optional(pool)
    .await
    .ok()
    .flatten()?;

    let available = balance - frozen;

    let current_price: Option<f64> = sqlx::query_scalar(
        "SELECT price FROM trades WHERE symbol=$1 ORDER BY created_at DESC LIMIT 1",
    )
    .bind(&symbol)
    .fetch_optional(pool)
    .await
    .ok()
    .flatten();

    let entry_price: f64 = sqlx::query_scalar::<_, Option<f64>>(
        "SELECT SUM(t.price * t.quantity) / NULLIF(SUM(t.quantity), 0)
         FROM trades t JOIN orders o ON o.id = t.buy_order_id
         WHERE o.user_id = $1 AND t.symbol = $2",
    )
    .bind(user_id)
    .bind(&symbol)
    .fetch_one(pool)
    .await
    .ok()
    .flatten()
    .unwrap_or(0.0);

    let market_value = current_price.map(|p| p * balance);
    let (unrealized_pnl, unrealized_pnl_pct) = match current_price {
        Some(c) if entry_price > 0.0 => (
            Some((c - entry_price) * available),
            Some((c - entry_price) / entry_price * 100.0),
        ),
        _ => (None, None),
    };

    Some(Position {
        asset: asset.to_string(),
        symbol,
        quantity: balance,
        frozen,
        available,
        entry_price,
        current_price,
        market_value,
        unrealized_pnl,
        unrealized_pnl_pct,
    })
}

/// Slow/cold REST path used by `lightning-data`.
/// Reads non-USDT account rows from PostgreSQL and builds positions from PG history.
pub async fn all_positions_for_user(pool: &PgPool, user_id: i64) -> Vec<Position> {
    let assets: Vec<String> = sqlx::query_scalar(
        "SELECT asset FROM accounts
         WHERE user_id = $1 AND asset <> 'USDT' AND balance > 0
         ORDER BY asset",
    )
    .bind(user_id)
    .fetch_all(pool)
    .await
    .unwrap_or_default();

    let mut out = Vec::with_capacity(assets.len());
    for asset in assets {
        if let Some(pos) = position_for_user_asset(pool, user_id, &asset).await {
            out.push(pos);
        }
    }
    out
}

/// Pre-built WS `position_update` JSON for one (user, base asset). Returns
/// None when the user has no account row for that asset.
pub async fn position_update_msg(pool: &PgPool, user_id: i64, asset: &str) -> Option<String> {
    let pos = position_for_user_asset(pool, user_id, asset).await?;
    let mut v = serde_json::to_value(&pos).ok()?;
    v.as_object_mut()?.insert(
        "type".to_string(),
        serde_json::Value::String("position_update".to_string()),
    );
    Some(v.to_string())
}
