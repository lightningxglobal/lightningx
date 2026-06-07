//! Integration tests for the PG ↔ Redis cross-store account reconciliation
//! (three-way reconciliation building block). Requires local PG + Redis;
//! skips gracefully when either is absent.

use lightning_exchange::db;
use lightning_exchange::reconcile::check_pg_redis_accounts;
use redis::AsyncCommands;
use sqlx::{PgPool, Row};
use uuid::Uuid;

async fn try_pg() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://user:password@localhost:5432/mydb".to_string());
    let pg = db::create_pool_sized(&url, 2).await.ok()?;
    db::run_migrations(&pg).await.ok()?;
    Some(pg)
}

async fn try_redis() -> Option<redis::aio::MultiplexedConnection> {
    let url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/0".to_string());
    let client = redis::Client::open(url).ok()?;
    let mut conn = client.get_multiplexed_async_connection().await.ok()?;
    let pong: String = redis::cmd("PING").query_async(&mut conn).await.ok()?;
    (pong == "PONG").then_some(conn)
}

async fn make_user(pg: &PgPool) -> Option<i64> {
    let email = format!("xstore_{}@lightning.test", Uuid::new_v4());
    sqlx::query("INSERT INTO users (email, password_hash) VALUES ($1, $2) RETURNING id")
        .bind(email)
        .bind("$argon2id$v=19$test$abcdefghijklmnopqrstuvwxyz0123456789")
        .fetch_one(pg)
        .await
        .ok()?
        .try_get("id")
        .ok()
}

/// Seed a PG account with updated_at pushed past the grace window so the
/// sweep is willing to compare it.
async fn seed_pg_account(pg: &PgPool, user_id: i64, asset: &str, balance_atoms: i64, frozen_atoms: i64) {
    sqlx::query(
        "INSERT INTO accounts (user_id, asset, balance_atoms, frozen_atoms, updated_at)
         VALUES ($1, $2, $3, $4, NOW() - INTERVAL '60 seconds')
         ON CONFLICT (user_id, asset) DO UPDATE SET
            balance_atoms = EXCLUDED.balance_atoms, frozen_atoms = EXCLUDED.frozen_atoms,
            updated_at = NOW() - INTERVAL '60 seconds'",
    )
    .bind(user_id)
    .bind(asset)
    .bind(balance_atoms)
    .bind(frozen_atoms)
    .execute(pg)
    .await
    .expect("seed pg account");
}

async fn seed_redis_account(
    conn: &mut redis::aio::MultiplexedConnection,
    user_id: i64,
    asset: &str,
    balance_atoms: i64,
    frozen_atoms: i64,
) {
    let key = format!("acct:{user_id}:{asset}");
    let _: () = conn
        .hset_multiple(
            &key,
            &[
                ("balance_atoms", balance_atoms.to_string()),
                ("frozen_atoms", frozen_atoms.to_string()),
            ],
        )
        .await
        .expect("seed redis account");
}

async fn cleanup(
    pg: &PgPool,
    conn: &mut redis::aio::MultiplexedConnection,
    user_ids: &[i64],
    assets: &[&str],
) {
    for &uid in user_ids {
        for asset in assets {
            let _: Result<(), _> = conn.del(format!("acct:{uid}:{asset}")).await;
        }
    }
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
async fn detects_mismatch_and_missing_between_stores() {
    let Some(pg) = try_pg().await else {
        eprintln!("skip: no PG");
        return;
    };
    let Some(mut redis) = try_redis().await else {
        eprintln!("skip: no Redis");
        return;
    };
    let (Some(consistent), Some(diverged), Some(unhydrated)) = (
        make_user(&pg).await,
        make_user(&pg).await,
        make_user(&pg).await,
    ) else {
        eprintln!("skip: cannot create users");
        return;
    };
    let users = [consistent, diverged, unhydrated];

    // consistent: PG and Redis agree exactly.
    seed_pg_account(&pg, consistent, "USDT", 5_000_000_000, 100).await;
    seed_redis_account(&mut redis, consistent, "USDT", 5_000_000_000, 100).await;
    // diverged: Redis lost a frame and holds a stale balance.
    seed_pg_account(&pg, diverged, "USDT", 7_000_000_000, 0).await;
    seed_redis_account(&mut redis, diverged, "USDT", 6_900_000_000, 0).await;
    // unhydrated: funds in PG, no Redis key at all.
    seed_pg_account(&pg, unhydrated, "USDT", 1_000_000_000, 0).await;

    let report = check_pg_redis_accounts(&pg, &mut redis, 10, 10_000)
        .await
        .expect("sweep");

    assert!(report.compared >= 3);
    assert!(
        report
            .mismatched
            .iter()
            .any(|m| m.user_id == diverged
                && m.redis_balance_atoms == Some(6_900_000_000)
                && m.pg_balance_atoms == 7_000_000_000),
        "stale Redis balance must be reported"
    );
    assert!(
        !report.mismatched.iter().any(|m| m.user_id == consistent),
        "consistent account must not be reported"
    );
    assert!(
        report.missing_in_redis >= 1,
        "funded-but-unhydrated account must count as missing"
    );

    cleanup(&pg, &mut redis, &users, &["USDT"]).await;
}

#[tokio::test]
async fn grace_window_excludes_recent_writes() {
    let Some(pg) = try_pg().await else {
        eprintln!("skip: no PG");
        return;
    };
    let Some(mut redis) = try_redis().await else {
        eprintln!("skip: no Redis");
        return;
    };
    let Some(user) = make_user(&pg).await else {
        eprintln!("skip: cannot create user");
        return;
    };

    // Freshly updated PG row (updated_at = NOW()) — inside the grace window,
    // diverged from Redis on purpose. The sweep must NOT flag it.
    sqlx::query(
        "INSERT INTO accounts (user_id, asset, balance_atoms, frozen_atoms)
         VALUES ($1, 'USDT', 1000000000, 0)",
    )
    .bind(user)
    .execute(&pg)
    .await
    .expect("insert fresh row");
    seed_redis_account(&mut redis, user, "USDT", 999, 0).await;

    let report = check_pg_redis_accounts(&pg, &mut redis, 30, 10_000)
        .await
        .expect("sweep");
    assert!(
        !report.mismatched.iter().any(|m| m.user_id == user),
        "rows inside the grace window must be skipped"
    );

    cleanup(&pg, &mut redis, &[user], &["USDT"]).await;
}
