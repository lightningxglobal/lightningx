//! Integration test for `pg_store::backfill_from_redis` — seed Redis with
//! orders that don't exist in PG, run backfill, verify rows now in PG.
//!
//! Requires PG at DATABASE_URL and Redis at REDIS_URL. Skips when either
//! is unreachable.

use lightning_exchange::desk::pg_store::backfill_from_redis;
use lightning_exchange::desk::redis_store::{
    KEY_ACTIVE_ORDERS, apply_frame, key_order, key_user_orders,
};
use lightning_exchange::transport::persist_event::{OrderUpsertPayload, PersistFrame, pack_str};
use redis::AsyncCommands;
use sqlx::{PgPool, Row};
use uuid::Uuid;

async fn try_pg() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://user:password@localhost:5432/mydb".to_string());
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .ok()
}

async fn try_redis() -> Option<redis::aio::MultiplexedConnection> {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/0".to_string());
    let client = redis::Client::open(url).ok()?;
    let mut conn = client.get_multiplexed_async_connection().await.ok()?;
    let _: String = redis::cmd("PING").query_async(&mut conn).await.ok()?;
    Some(conn)
}

async fn make_user(pg: &PgPool) -> Option<i64> {
    let email = format!("backfill_{}@lightning.test", Uuid::new_v4());
    let row = sqlx::query("INSERT INTO users (email, password_hash) VALUES ($1, $2) RETURNING id")
        .bind(email)
        .bind("$argon2id$v=19$test$abcdefghijklmnopqrstuvwxyz0123456789")
        .fetch_one(pg)
        .await
        .ok()?;
    let id: i64 = row.try_get("id").ok()?;
    Some(id)
}

async fn purge(conn: &mut redis::aio::MultiplexedConnection, user_id: i64, ids: &[i64]) {
    let mut pipe = redis::pipe();
    for id in ids {
        pipe.del(key_order(*id)).ignore();
        pipe.srem(KEY_ACTIVE_ORDERS, id).ignore();
    }
    pipe.del(key_user_orders(user_id)).ignore();
    let _: () = pipe.query_async(conn).await.unwrap_or(());
}

#[serial_test::serial]
#[tokio::test]
async fn backfill_inserts_missing_rows_from_redis() {
    let Some(pg) = try_pg().await else {
        eprintln!("skip: no PG");
        return;
    };
    let Some(mut conn) = try_redis().await else {
        eprintln!("skip: no Redis");
        return;
    };
    let Some(user_id) = make_user(&pg).await else {
        eprintln!("skip: cannot make user");
        return;
    };

    let id_present: i64 = 970_002_000_001;
    let id_missing: i64 = 970_002_000_002;
    purge(&mut conn, user_id, &[id_present, id_missing]).await;

    // Seed both in Redis.
    for id in [id_present, id_missing] {
        let f = PersistFrame::order_upsert(OrderUpsertPayload {
            id,
            user_id,
            symbol: pack_str("BTC_USDT"),
            side: 0,
            status: 0, // PENDING
            _pad: [0; 6],
            order_type: pack_str("limit"),
            price: 71000.0,
            qty: 0.05,
            filled: 0.0,
            freeze_price: 71000.0,
            client_order_id: pack_str(""),
            created_at_ms: 1_700_000_000_000,
        });
        apply_frame(&mut conn, &f).await.unwrap();
    }

    // Only `id_present` exists in PG.
    sqlx::query(
        "INSERT INTO orders (id, user_id, symbol, side, order_type, price_atoms, quantity_atoms, status)
         VALUES ($1, $2, 'BTC_USDT', 'buy', 'limit', 7100000000000, 5000000, 'PENDING')",
    )
    .bind(id_present)
    .bind(user_id)
    .execute(&pg)
    .await
    .expect("seed present in PG");

    // Sanity.
    let pre_in_pg: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM orders WHERE id = ANY($1::bigint[])")
            .bind(&[id_present, id_missing])
            .fetch_one(&pg)
            .await
            .unwrap();
    assert_eq!(pre_in_pg, 1, "PG should start with only id_present");

    let stats = backfill_from_redis(&pg, &mut conn).await.unwrap();
    assert!(stats.redis_orders_scanned >= 2);
    assert!(
        stats.missing_in_pg >= 1,
        "should detect id_missing as missing: stats={stats:?}"
    );
    assert!(stats.backfilled >= 1, "should backfill at least id_missing");

    let post_in_pg: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM orders WHERE id = ANY($1::bigint[])")
            .bind(&[id_present, id_missing])
            .fetch_one(&pg)
            .await
            .unwrap();
    assert_eq!(post_in_pg, 2, "PG should have both after backfill");

    // Confirm backfilled row carries the correct decoded fields.
    let row: (String, String, f64, f64) =
        sqlx::query_as("SELECT symbol, side, COALESCE(price, 0), quantity FROM orders WHERE id=$1")
            .bind(id_missing)
            .fetch_one(&pg)
            .await
            .unwrap();
    assert_eq!(row.0, "BTC_USDT");
    assert_eq!(row.1, "buy");
    assert!((row.2 - 71000.0).abs() < 1e-9);
    assert!((row.3 - 0.05).abs() < 1e-9);

    // Cleanup.
    sqlx::query("DELETE FROM orders WHERE id = ANY($1::bigint[])")
        .bind(&[id_present, id_missing])
        .execute(&pg)
        .await
        .ok();
    sqlx::query("DELETE FROM users WHERE id=$1")
        .bind(user_id)
        .execute(&pg)
        .await
        .ok();
    purge(&mut conn, user_id, &[id_present, id_missing]).await;
}

#[serial_test::serial]
#[tokio::test]
async fn backfill_is_idempotent_when_pg_already_has_everything() {
    let Some(pg) = try_pg().await else {
        return;
    };
    let Some(mut conn) = try_redis().await else {
        return;
    };

    // Run twice — second should report 0 backfilled.
    let _first = backfill_from_redis(&pg, &mut conn).await.unwrap();
    let second = backfill_from_redis(&pg, &mut conn).await.unwrap();
    assert_eq!(
        second.backfilled, 0,
        "second run should backfill 0 (state already converged): stats={second:?}"
    );
}
