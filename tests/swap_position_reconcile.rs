//! S1.5 — swap-position reconcile invariants:
//!   1. per-symbol net exposure Σlong − Σshort = 0 (every fill opens
//!      equal-and-opposite exposure; a violation means lost/duplicated
//!      position frames);
//!   2. risk_accounts.used_margin == Σ(position margins) per user.
//! Seeds violations directly in PG, asserts they are reported, then
//! repairs them and asserts the report comes back clean.

use lightning_exchange::db;
use lightning_exchange::reconcile::check_position_invariants;
use sqlx::PgPool;
use uuid::Uuid;

// Unique symbol: the invariant scans group by symbol, so our violations
// can't collide with rows other suites leave behind.
const SYM: &str = "RECON_TEST";

async fn try_pg() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://user:password@localhost:5432/mydb".to_string());
    let pg = db::create_pool_sized(&url, 2).await.ok()?;
    db::run_migrations(&pg).await.ok()?;
    Some(pg)
}

async fn make_user(pg: &PgPool) -> i64 {
    sqlx::query_scalar("INSERT INTO users (email, password_hash) VALUES ($1, $2) RETURNING id")
        .bind(format!("pos_rec_{}@lightning.test", Uuid::new_v4()))
        .bind("$argon2id$v=19$test$abcdefghijklmnopqrstuvwxyz0123456789")
        .fetch_one(pg)
        .await
        .expect("make user")
}

async fn seed_position(pg: &PgPool, user: i64, side: &str, qty: i64, margin: i64) {
    sqlx::query(
        "INSERT INTO positions
           (user_id, symbol, side, qty_lots, entry_price_ticks, leverage, used_margin_atoms)
         VALUES ($1, $2, $3, $4, 5000000, 10, $5)",
    )
    .bind(user)
    .bind(SYM)
    .bind(side)
    .bind(qty)
    .bind(margin)
    .execute(pg)
    .await
    .expect("seed position");
}

async fn seed_risk_account(pg: &PgPool, user: i64, used_margin: i64) {
    sqlx::query(
        "INSERT INTO risk_accounts (user_id, equity_atoms, used_margin_atoms, order_margin_atoms)
         VALUES ($1, 1000000000, $2, 0)
         ON CONFLICT (user_id) DO UPDATE SET used_margin_atoms = EXCLUDED.used_margin_atoms",
    )
    .bind(user)
    .bind(used_margin)
    .execute(pg)
    .await
    .expect("seed risk account");
}

async fn cleanup(pg: &PgPool, users: &[i64]) {
    let _ = sqlx::query("DELETE FROM positions WHERE symbol = $1")
        .bind(SYM)
        .execute(pg)
        .await;
    for sql in [
        "DELETE FROM risk_accounts WHERE user_id = ANY($1::bigint[])",
        "DELETE FROM users WHERE id = ANY($1::bigint[])",
    ] {
        let _ = sqlx::query(sql).bind(users).execute(pg).await;
    }
}

#[tokio::test]
async fn detects_net_exposure_and_margin_drift_then_clean() {
    let Some(pg) = try_pg().await else {
        eprintln!("skip: no PG");
        return;
    };
    let long_u = make_user(&pg).await;
    let short_u = make_user(&pg).await;
    cleanup(&pg, &[]).await; // clear residue rows for OUR symbol

    // ── Violation 1: net exposure ≠ 0 (long 100 vs short 60) ───────────
    seed_position(&pg, long_u, "long", 100, 5_000_000).await;
    seed_position(&pg, short_u, "short", 60, 3_000_000).await;
    // ── Violation 2: account says 9_999 but positions sum to 5_000_000 ─
    seed_risk_account(&pg, long_u, 9_999).await;
    // short_u's account agrees with its position sum: must NOT be flagged.
    seed_risk_account(&pg, short_u, 3_000_000).await;

    let report = check_position_invariants(&pg).await.expect("sweep");
    assert!(
        report
            .net_violations
            .iter()
            .any(|v| v.symbol == SYM && v.net_lots == 40),
        "net long surplus of 40 lots must be reported, got {:?}",
        report.net_violations
    );
    assert!(
        report
            .margin_mismatches
            .iter()
            .any(|m| m.user_id == long_u
                && m.account_used_margin_atoms == 9_999
                && m.positions_margin_sum_atoms == 5_000_000),
        "margin drift for long user must be reported"
    );
    assert!(
        !report.margin_mismatches.iter().any(|m| m.user_id == short_u),
        "consistent account must not be flagged"
    );

    // ── Repair both: report must come back clean for OUR rows ──────────
    // One row per (user, symbol): repair by bumping the short's qty/margin.
    sqlx::query("UPDATE positions SET qty_lots = 100, used_margin_atoms = 5_000_000 WHERE user_id = $1 AND symbol = $2")
        .bind(short_u)
        .bind(SYM)
        .execute(&pg)
        .await
        .expect("repair short");
    seed_risk_account(&pg, long_u, 5_000_000).await;
    seed_risk_account(&pg, short_u, 5_000_000).await;

    let report = check_position_invariants(&pg).await.expect("sweep 2");
    assert!(
        !report.net_violations.iter().any(|v| v.symbol == SYM),
        "repaired symbol must be net-flat, got {:?}",
        report.net_violations
    );
    for u in [long_u, short_u] {
        assert!(
            !report.margin_mismatches.iter().any(|m| m.user_id == u),
            "repaired accounts must not be flagged"
        );
    }

    cleanup(&pg, &[long_u, short_u]).await;
}
