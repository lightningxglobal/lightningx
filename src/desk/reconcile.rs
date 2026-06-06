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
//! 2. Legacy/atoms drift — during the float8 compatibility window both
//!    column families are written together; `|balance×1e8 − balance_atoms|`
//!    beyond a small tolerance means a code path updated one but not the
//!    other and must be fixed before the legacy columns are dropped.

use crate::desk::money::AMOUNT_SCALE;
use anyhow::Result;
use sqlx::PgPool;

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

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct AtomsDriftRow {
    pub user_id: i64,
    pub asset: String,
    pub balance: f64,
    pub balance_atoms: i64,
    pub frozen: f64,
    pub frozen_atoms: i64,
}

#[derive(Debug, Default)]
pub struct ReconcileReport {
    pub hanging_frozen_total: i64,
    pub hanging_frozen: Vec<HangingFrozenRow>,
    pub atoms_drift_total: i64,
    pub atoms_drift: Vec<AtomsDriftRow>,
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
            && self.atoms_drift_total == 0
            && self.orders_drift_total == 0
            && self.trades_drift_total == 0
            && self.orders_overfill_total == 0
    }
}

/// Run all account invariant checks. Read-only.
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

    // -- Check 2: legacy float vs atoms drift ------------------------------
    let drift_filter = format!(
        "ABS(ROUND(balance * {scale}) - balance_atoms) > {tol}
          OR ABS(ROUND(frozen * {scale}) - frozen_atoms) > {tol}",
        scale = AMOUNT_SCALE,
        tol = DRIFT_TOLERANCE_ATOMS,
    );
    report.atoms_drift_total = sqlx::query_scalar::<_, i64>(&format!(
        "SELECT COUNT(*) FROM accounts WHERE {drift_filter}"
    ))
    .fetch_one(pool)
    .await?;
    if report.atoms_drift_total > 0 {
        report.atoms_drift = sqlx::query_as(&format!(
            "SELECT user_id, asset, balance, balance_atoms, frozen, frozen_atoms
               FROM accounts
              WHERE {drift_filter}
              ORDER BY user_id LIMIT {REPORT_ROW_LIMIT}"
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
