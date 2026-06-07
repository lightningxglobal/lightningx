//! Integration tests for desk::reconcile account invariant checks and the
//! durability-enforcing pool. (The legacy float/atoms drift check was
//! removed together with the float8 account columns — migration 018.) Requires a local PG (skips gracefully if
//! absent, same pattern as account_repository_atoms).

use lightning_exchange::db;
use lightning_exchange::money::AmountAtoms;
use lightning_exchange::reconcile::check_account_invariants;
use sqlx::{PgPool, Row};
use uuid::Uuid;

fn pg_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://user:password@localhost:5432/mydb".to_string())
}

async fn try_pg() -> Option<PgPool> {
    let pg = db::create_pool_sized(&pg_url(), 2).await.ok()?;
    db::run_migrations(&pg).await.ok()?;
    Some(pg)
}

async fn make_user(pg: &PgPool) -> Option<i64> {
    let email = format!("reconcile_{}@lightning.test", Uuid::new_v4());
    let row = sqlx::query("INSERT INTO users (email, password_hash) VALUES ($1, $2) RETURNING id")
        .bind(email)
        .bind("$argon2id$v=19$test$abcdefghijklmnopqrstuvwxyz0123456789")
        .fetch_one(pg)
        .await
        .ok()?;
    row.try_get("id").ok()
}

async fn seed_account_atoms(
    pg: &PgPool,
    user_id: i64,
    asset: &str,
    balance_atoms: i64,
    frozen_atoms: i64,
) {
    sqlx::query(
        "INSERT INTO accounts (user_id, asset, balance_atoms, frozen_atoms)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (user_id, asset) DO UPDATE SET
            balance_atoms = EXCLUDED.balance_atoms,
            frozen_atoms = EXCLUDED.frozen_atoms",
    )
    .bind(user_id)
    .bind(asset)
    .bind(balance_atoms)
    .bind(frozen_atoms)
    .execute(pg)
    .await
    .expect("seed account");
}

async fn seed_open_order(pg: &PgPool, user_id: i64) -> i64 {
    let id = (Uuid::new_v4().as_u128() % i64::MAX as u128) as i64;
    sqlx::query(
        "INSERT INTO orders (id, user_id, symbol, side, order_type, price_atoms, quantity_atoms, status)
         VALUES ($1, $2, 'BTC_USDT', 'buy', 'limit', 10000000000, 100000000, 'TRADING')",
    )
    .bind(id)
    .bind(user_id)
    .execute(pg)
    .await
    .expect("seed order");
    id
}

async fn cleanup(pg: &PgPool, user_ids: &[i64]) {
    let _ = sqlx::query("DELETE FROM orders WHERE user_id = ANY($1::bigint[])")
        .bind(user_ids)
        .execute(pg)
        .await;
    let _ = sqlx::query("DELETE FROM accounts WHERE user_id = ANY($1::bigint[])")
        .bind(user_ids)
        .execute(pg)
        .await;
    let _ = sqlx::query("DELETE FROM users WHERE id = ANY($1::bigint[])")
        .bind(user_ids)
        .execute(pg)
        .await;
}

#[tokio::test]
async fn pool_enforces_synchronous_commit_on() {
    let Some(pg) = try_pg().await else {
        eprintln!("skip: no PG");
        return;
    };
    let (value,): (String,) = sqlx::query_as("SHOW synchronous_commit")
        .fetch_one(&pg)
        .await
        .expect("show synchronous_commit");
    assert_eq!(value, "on");
}

#[tokio::test]
async fn detects_hanging_frozen_funds() {
    let Some(pg) = try_pg().await else {
        eprintln!("skip: no PG");
        return;
    };
    let Some(leaker) = make_user(&pg).await else {
        eprintln!("skip: cannot make user");
        return;
    };
    let Some(trader) = make_user(&pg).await else {
        eprintln!("skip: cannot make user");
        cleanup(&pg, &[leaker]).await;
        return;
    };

    // leaker: frozen funds, zero open orders → must be flagged.
    seed_account_atoms(&pg, leaker, "USDT", 1_000_000_000, 500_000_000).await;
    // trader: frozen funds backed by an open order → must NOT be flagged.
    seed_account_atoms(&pg, trader, "USDT", 1_000_000_000, 500_000_000).await;
    seed_open_order(&pg, trader).await;

    let report = check_account_invariants(&pg)
        .await
        .expect("reconcile sweep");

    assert!(
        report
            .hanging_frozen
            .iter()
            .any(|r| r.user_id == leaker && r.asset == "USDT" && r.frozen_atoms == 500_000_000),
        "leaked freeze must be reported"
    );
    assert!(
        !report.hanging_frozen.iter().any(|r| r.user_id == trader),
        "freeze backed by an open order must not be reported"
    );

    cleanup(&pg, &[leaker, trader]).await;
}

