//! S1.1 schema tests — migration 022 (positions / risk_accounts /
//! insurance_fund). Verifies the migration is idempotent and that every
//! integrity CHECK actually rejects what it is supposed to reject; the
//! constraints ARE the spec, so each one gets a hostile probe.
//!
//! Requires local PG (skips gracefully, same pattern as the other suites).

use lightning_exchange::db;
use sqlx::PgPool;
use uuid::Uuid;

async fn try_pg() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://user:password@localhost:5432/mydb".to_string());
    let pg = db::create_pool_sized(&url, 2).await.ok()?;
    db::run_migrations(&pg).await.ok()?;
    Some(pg)
}

async fn make_user(pg: &PgPool) -> i64 {
    sqlx::query_scalar("INSERT INTO users (email, password_hash) VALUES ($1, $2) RETURNING id")
        .bind(format!("pos_schema_{}@lightning.test", Uuid::new_v4()))
        .bind("$argon2id$v=19$test$abcdefghijklmnopqrstuvwxyz0123456789")
        .fetch_one(pg)
        .await
        .expect("make user")
}

async fn cleanup(pg: &PgPool, user: i64) {
    let _ = sqlx::query("DELETE FROM positions WHERE user_id = $1")
        .bind(user)
        .execute(pg)
        .await;
    let _ = sqlx::query("DELETE FROM risk_accounts WHERE user_id = $1")
        .bind(user)
        .execute(pg)
        .await;
    let _ = sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(user)
        .execute(pg)
        .await;
}

/// Insert a valid position row; helper keeps the hostile probes terse.
async fn insert_position(
    pg: &PgPool,
    user: i64,
    side: &str,
    qty: i64,
    entry: i64,
    leverage: i32,
    margin: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO positions
           (user_id, symbol, side, qty_lots, entry_price_ticks, leverage, used_margin_atoms)
         VALUES ($1, 'BTC_USDT', $2, $3, $4, $5, $6)
         ON CONFLICT (user_id, symbol) DO UPDATE SET
           side = EXCLUDED.side, qty_lots = EXCLUDED.qty_lots,
           entry_price_ticks = EXCLUDED.entry_price_ticks,
           leverage = EXCLUDED.leverage,
           used_margin_atoms = EXCLUDED.used_margin_atoms",
    )
    .bind(user)
    .bind(side)
    .bind(qty)
    .bind(entry)
    .bind(leverage as i16)
    .bind(margin)
    .execute(pg)
    .await
    .map(|_| ())
}

#[tokio::test]
async fn migration_is_idempotent_and_constraints_hold() {
    let Some(pg) = try_pg().await else {
        eprintln!("skip: no PG");
        return;
    };
    // Idempotency: running migrations again must be a no-op, not an error.
    db::run_migrations(&pg).await.expect("second run");

    let user = make_user(&pg).await;

    // ── positions: valid row passes, each CHECK rejects its violation ──
    insert_position(&pg, user, "long", 100, 50_000, 10, 5_000_000_000)
        .await
        .expect("valid long position");
    assert!(
        insert_position(&pg, user, "sideways", 100, 50_000, 10, 0).await.is_err(),
        "side outside long/short must be rejected"
    );
    assert!(
        insert_position(&pg, user, "long", 0, 50_000, 10, 0).await.is_err(),
        "zero quantity must be rejected (flat = row absent, not qty 0)"
    );
    assert!(
        insert_position(&pg, user, "long", 100, 0, 10, 0).await.is_err(),
        "zero entry price must be rejected"
    );
    assert!(
        insert_position(&pg, user, "long", 100, 50_000, 0, 0).await.is_err(),
        "leverage below 1 must be rejected"
    );
    assert!(
        insert_position(&pg, user, "long", 100, 50_000, 126, 0).await.is_err(),
        "leverage above 125 must be rejected"
    );
    assert!(
        insert_position(&pg, user, "long", 100, 50_000, 10, -1).await.is_err(),
        "negative used margin must be rejected"
    );

    // ── risk_accounts: negative equity ALLOWED, negative margins NOT ───
    sqlx::query(
        "INSERT INTO risk_accounts (user_id, equity_atoms, used_margin_atoms, order_margin_atoms)
         VALUES ($1, -123, 0, 0)", // bankruptcy moment: equity < 0 is legal
    )
    .bind(user)
    .execute(&pg)
    .await
    .expect("negative equity must be allowed (transient bankruptcy)");
    assert!(
        sqlx::query("UPDATE risk_accounts SET used_margin_atoms = -1 WHERE user_id = $1")
            .bind(user)
            .execute(&pg)
            .await
            .is_err(),
        "negative used_margin must be rejected"
    );
    assert!(
        sqlx::query("UPDATE risk_accounts SET status = 'on_vacation' WHERE user_id = $1")
            .bind(user)
            .execute(&pg)
            .await
            .is_err(),
        "unknown status must be rejected"
    );

    // ── insurance_fund: exactly one row, pinned id, non-negative ───────
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*)::bigint FROM insurance_fund")
        .fetch_one(&pg)
        .await
        .expect("fund row");
    assert_eq!(count, 1, "seed row must exist exactly once");
    assert!(
        sqlx::query("INSERT INTO insurance_fund (id, balance_atoms) VALUES (2, 0)")
            .execute(&pg)
            .await
            .is_err(),
        "a second fund row must be impossible (id pinned to 1)"
    );
    // 023: the fund CAN go negative (socialised-loss debt) — and must be
    // restorable. Round-trip a negative balance.
    sqlx::query("UPDATE insurance_fund SET balance_atoms = -1 WHERE id = 1")
        .execute(&pg)
        .await
        .expect("negative fund balance must be allowed (socialised losses)");
    sqlx::query("UPDATE insurance_fund SET balance_atoms = 0 WHERE id = 1")
        .execute(&pg)
        .await
        .expect("restore fund");

    cleanup(&pg, user).await;
}
