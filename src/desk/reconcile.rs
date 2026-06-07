//! Off-hot-path account invariant checks.
//!
//! Run periodically by `pg-writer` (and usable from ops tooling) to surface
//! fund-safety violations early instead of discovering them at withdrawal
//! time. Pure SQL over indexed columns; never called from the order path.
//!
//! Checks:
//! 1. Hanging frozen funds — `frozen_atoms > 0` for a user with NO open
//!    orders at all. A correctly closed order lifecycle always releases or
//!    consumes its freeze, so any survivor here is a leaked freeze (e.g.
//!    crash between freeze and order INSERT). Conservative on purpose: a
//!    user with ANY open order is skipped, so false positives are limited
//!    to multi-asset edge cases rather than ordinary trading.
//! 2. Orders/trades float/atoms drift — those tables still carry float8
//!    originals next to the atoms twins until the order-path wire upgrade;
//!    divergence means a code path updated one family but not the other.
//!    (Accounts float8 columns were DROPPED pre-launch — migration 018.)

use crate::desk::money::AMOUNT_SCALE;
use anyhow::Result;
use redis::AsyncCommands;
use sqlx::{PgPool, Row};

/// Atom tolerance for the legacy-float comparison. f64 keeps ~15-16
/// significant digits, so balances up to ~1e7 quote units round-trip within
/// a few atoms; 10 atoms (1e-7) absorbs that without masking real bugs.
pub const DRIFT_TOLERANCE_ATOMS: i64 = 10;

/// Cap on rows returned per violation list. Counts are exact; row details
/// are a sample for the log/alert payload.
const REPORT_ROW_LIMIT: i64 = 100;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct HangingFrozenRow {
    pub user_id: i64,
    pub asset: String,
    pub frozen_atoms: i64,
}

#[derive(Debug, Default)]
pub struct ReconcileReport {
    pub hanging_frozen_total: i64,
    pub hanging_frozen: Vec<HangingFrozenRow>,
    /// orders rows whose float and atoms columns disagree (sample of ids).
    pub orders_drift_total: i64,
    pub orders_drift_ids: Vec<i64>,
    /// trades rows whose float and atoms columns disagree (sample of ids).
    pub trades_drift_total: i64,
    pub trades_drift_ids: Vec<i64>,
    /// orders with filled_atoms > quantity_atoms — over-fill is a settlement
    /// bug. Monitored here instead of a CHECK constraint during the
    /// dual-write window (see migration 014).
    pub orders_overfill_total: i64,
    pub orders_overfill_ids: Vec<i64>,
}

impl ReconcileReport {
    pub fn is_clean(&self) -> bool {
        self.hanging_frozen_total == 0
            && self.orders_drift_total == 0
            && self.trades_drift_total == 0
            && self.orders_overfill_total == 0
    }
}

#[derive(Debug, Clone)]
pub struct CrossStoreMismatch {
    pub user_id: i64,
    pub asset: String,
    pub pg_balance_atoms: i64,
    pub pg_frozen_atoms: i64,
    pub redis_balance_atoms: Option<i64>,
    pub redis_frozen_atoms: Option<i64>,
}

/// PG ↔ Redis account comparison (three-way reconciliation, L1 vs cold).
#[derive(Debug, Default)]
pub struct CrossStoreReport {
    pub compared: u64,
    pub missing_in_redis: u64,
    pub mismatched_total: u64,
    /// Detail sample, capped at 100 rows.
    pub mismatched: Vec<CrossStoreMismatch>,
}

impl CrossStoreReport {
    pub fn is_clean(&self) -> bool {
        self.missing_in_redis == 0 && self.mismatched_total == 0
    }
}

/// Compare recently-settled PG accounts against the Redis L1 cache.
///
/// Both stores are written asynchronously from the same persist stream, so
/// a row updated milliseconds ago may legitimately differ; only rows whose
/// PG `updated_at` is older than `grace_secs` are compared. A transient
/// nonzero result can still occur if Redis received a NEWER event than the
/// PG row — alert on PERSISTENT divergence across sweeps, not single hits.
/// Bounded to `limit` most-recently-updated rows per sweep; off hot path.
pub async fn check_pg_redis_accounts(
    pool: &PgPool,
    redis: &mut redis::aio::MultiplexedConnection,
    grace_secs: i64,
    limit: i64,
) -> Result<CrossStoreReport> {
    let rows: Vec<(i64, String, i64, i64)> = sqlx::query_as(
        "SELECT user_id, asset, balance_atoms, frozen_atoms
           FROM accounts
          WHERE updated_at < NOW() - make_interval(secs => $1)
          ORDER BY updated_at DESC
          LIMIT $2",
    )
    .bind(grace_secs as f64)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    let mut report = CrossStoreReport::default();
    for (user_id, asset, pg_balance_atoms, pg_frozen_atoms) in rows {
        report.compared += 1;
        let key = crate::desk::redis_store::key_account(user_id, &asset);
        let fields: Vec<Option<i64>> = redis
            .hget(&key, &["balance_atoms", "frozen_atoms"])
            .await
            .unwrap_or_else(|_| vec![None, None]);
        let redis_balance = fields.first().copied().flatten();
        let redis_frozen = fields.get(1).copied().flatten();
        match (redis_balance, redis_frozen) {
            (None, None) => {
                // Account never hydrated into L1 — only suspicious when the
                // PG row holds funds.
                if pg_balance_atoms != 0 || pg_frozen_atoms != 0 {
                    report.missing_in_redis += 1;
                }
            }
            _ => {
                let rb = redis_balance.unwrap_or(0);
                let rf = redis_frozen.unwrap_or(0);
                if rb != pg_balance_atoms || rf != pg_frozen_atoms {
                    report.mismatched_total += 1;
                    if report.mismatched.len() < 100 {
                        report.mismatched.push(CrossStoreMismatch {
                            user_id,
                            asset,
                            pg_balance_atoms,
                            pg_frozen_atoms,
                            redis_balance_atoms: redis_balance,
                            redis_frozen_atoms: redis_frozen,
                        });
                    }
                }
            }
        }
    }
    Ok(report)
}

/// Run all account invariant checks. Read-only.
/// One per-symbol net-exposure violation: in a swap venue every fill
/// opens equal-and-opposite exposure (liquidation orders included — they
/// trade against the book), so Σ(long lots) − Σ(short lots) per symbol
/// MUST be zero at all times. Non-zero means lost/duplicated position
/// frames or a unit bug — freeze-and-investigate territory.
#[derive(Debug)]
pub struct PositionNetRow {
    pub symbol: String,
    pub net_lots: i64,
}

/// risk_accounts.used_margin must equal the SUM of the user's position
/// margins (on_fill maintains both sides of this equality in memory; the
/// durable mirror must agree with itself).
#[derive(Debug)]
pub struct MarginMismatchRow {
    pub user_id: i64,
    pub account_used_margin_atoms: i64,
    pub positions_margin_sum_atoms: i64,
}

#[derive(Debug, Default)]
pub struct PositionReport {
    pub net_violations: Vec<PositionNetRow>,
    pub margin_mismatches: Vec<MarginMismatchRow>,
}

impl PositionReport {
    pub fn is_clean(&self) -> bool {
        self.net_violations.is_empty() && self.margin_mismatches.is_empty()
    }
}

/// S1.5 — swap-position invariants over the durable tables. Pure SQL
/// aggregation (two statements), safe to run from pg-writer's periodic
/// reconcile task at any frequency.
pub async fn check_position_invariants(pool: &PgPool) -> Result<PositionReport> {
    let mut report = PositionReport::default();

    let rows = sqlx::query(
        r#"
        SELECT symbol,
               SUM(CASE side WHEN 'long' THEN qty_lots ELSE -qty_lots END)::bigint AS net
          FROM positions
         GROUP BY symbol
        HAVING SUM(CASE side WHEN 'long' THEN qty_lots ELSE -qty_lots END) <> 0
        "#,
    )
    .fetch_all(pool)
    .await?;
    for row in rows {
        report.net_violations.push(PositionNetRow {
            symbol: row.get("symbol"),
            net_lots: row.get("net"),
        });
    }

    let rows = sqlx::query(
        r#"
        SELECT r.user_id,
               r.used_margin_atoms,
               COALESCE(p.margin_sum, 0)::bigint AS margin_sum
          FROM risk_accounts r
          LEFT JOIN (
              SELECT user_id, SUM(used_margin_atoms) AS margin_sum
                FROM positions GROUP BY user_id
          ) p USING (user_id)
         WHERE r.used_margin_atoms <> COALESCE(p.margin_sum, 0)
        "#,
    )
    .fetch_all(pool)
    .await?;
    for row in rows {
        report.margin_mismatches.push(MarginMismatchRow {
            user_id: row.get("user_id"),
            account_used_margin_atoms: row.get("used_margin_atoms"),
            positions_margin_sum_atoms: row.get("margin_sum"),
        });
    }

    Ok(report)
}

pub async fn check_account_invariants(pool: &PgPool) -> Result<ReconcileReport> {
    let mut report = ReconcileReport::default();

    // -- Check 1: hanging frozen funds ------------------------------------
    let hanging_filter = "frozen_atoms > 0
           AND NOT EXISTS (
               SELECT 1 FROM orders o
                WHERE o.user_id = accounts.user_id
                  AND o.status IN ('PENDING', 'TRADING')
           )";
    report.hanging_frozen_total = sqlx::query_scalar::<_, i64>(&format!(
        "SELECT COUNT(*) FROM accounts WHERE {hanging_filter}"
    ))
    .fetch_one(pool)
    .await?;
    if report.hanging_frozen_total > 0 {
        report.hanging_frozen = sqlx::query_as(&format!(
            "SELECT user_id, asset, frozen_atoms FROM accounts
              WHERE {hanging_filter}
              ORDER BY frozen_atoms DESC LIMIT {REPORT_ROW_LIMIT}"
        ))
        .fetch_all(pool)
        .await?;
    }

    // -- Check 3: orders float/atoms drift + over-fill ----------------------
    let orders_drift_filter = format!(
        "ABS(ROUND(quantity * {scale}) - quantity_atoms) > {tol}
          OR ABS(ROUND(filled * {scale}) - filled_atoms) > {tol}
          OR (price IS NOT NULL AND price_atoms IS NOT NULL
              AND ABS(ROUND(price * {scale}) - price_atoms) > {tol})",
        scale = AMOUNT_SCALE,
        tol = DRIFT_TOLERANCE_ATOMS,
    );
    report.orders_drift_total = sqlx::query_scalar::<_, i64>(&format!(
        "SELECT COUNT(*) FROM orders WHERE {orders_drift_filter}"
    ))
    .fetch_one(pool)
    .await?;
    if report.orders_drift_total > 0 {
        report.orders_drift_ids = sqlx::query_scalar(&format!(
            "SELECT id FROM orders WHERE {orders_drift_filter} ORDER BY id LIMIT {REPORT_ROW_LIMIT}"
        ))
        .fetch_all(pool)
        .await?;
    }

    report.orders_overfill_total =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM orders WHERE filled_atoms > quantity_atoms")
            .fetch_one(pool)
            .await?;
    if report.orders_overfill_total > 0 {
        report.orders_overfill_ids = sqlx::query_scalar(&format!(
            "SELECT id FROM orders WHERE filled_atoms > quantity_atoms ORDER BY id LIMIT {REPORT_ROW_LIMIT}"
        ))
        .fetch_all(pool)
        .await?;
    }

    // -- Check 4: trades float/atoms drift ---------------------------------
    let trades_drift_filter = format!(
        "ABS(ROUND(price * {scale}) - price_atoms) > {tol}
          OR ABS(ROUND(quantity * {scale}) - quantity_atoms) > {tol}",
        scale = AMOUNT_SCALE,
        tol = DRIFT_TOLERANCE_ATOMS,
    );
    report.trades_drift_total = sqlx::query_scalar::<_, i64>(&format!(
        "SELECT COUNT(*) FROM trades WHERE {trades_drift_filter}"
    ))
    .fetch_one(pool)
    .await?;
    if report.trades_drift_total > 0 {
        report.trades_drift_ids = sqlx::query_scalar(&format!(
            "SELECT id FROM trades WHERE {trades_drift_filter} ORDER BY id LIMIT {REPORT_ROW_LIMIT}"
        ))
        .fetch_all(pool)
        .await?;
    }

    Ok(report)
}
