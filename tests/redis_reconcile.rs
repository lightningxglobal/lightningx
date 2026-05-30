//! Integration test for `redis_store::reconcile_orphans` — verifies the
//! sweep removes ids that exist in Redis but not in PG without disturbing
//! ids that DO exist in PG.
//!
//! Requires PG at DATABASE_URL and Redis at REDIS_URL. Skips when either
//! is unreachable.

use lightning_exchange::desk::redis_store::{
    apply_frame, key_order, key_user_orders, reconcile_orphans, KEY_ACTIVE_ORDERS,
};
use lightning_exchange::transport::persist_event::{pack_str, OrderUpsertPayload, PersistFrame};
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
    let email = format!("reconcile_{}@lightning.test", Uuid::new_v4());
    let row = sqlx::query("INSERT INTO users (email, password_hash) VALUES ($1, $2) RETURNING id")
        .bind(email)
        .bind("$argon2id$v=19$test$abcdefghijklmnopqrstuvwxyz0123456789")
        .fetch_one(pg)
        .await
        .ok()?;
    let id: i64 = row.try_get("id").ok()?;
    Some(id)
}

async fn purge_test_state(
    conn: &mut redis::aio::MultiplexedConnection,
    user_id: i64,
    ids: &[i64],
) {
    let mut pipe = redis::pipe();
    for id in ids {
        pipe.del(key_order(*id)).ignore();
        pipe.srem(KEY_ACTIVE_ORDERS, id).ignore();
    }
    pipe.del(key_user_orders(user_id)).ignore();
    let _: () = pipe.query_async(conn).await.unwrap_or(());
}

fn upsert_frame(id: i64, user_id: i64) -> PersistFrame {
    PersistFrame::order_upsert(OrderUpsertPayload {
        id,
        user_id,
        symbol: pack_str("BTC_USDT"),
        side: 0,
        status: 0, // PENDING (0-indexed wire encoding)
        _pad: [0; 6],
        order_type: pack_str("limit"),
        price: 70000.0,
        qty: 0.1,
        filled: 0.0,
        freeze_price: 70000.0,
        client_order_id: pack_str(""),
        created_at_ms: 1_700_000_000_000,
    })
}

#[serial_test::serial]
#[tokio::test]
async fn reconcile_drops_orphans_keeps_pg_backed_ids() {
    let Some(pg) = try_pg().await else {
        eprintln!("skip: no PG");
        return;
    };
    let Some(mut conn) = try_redis().await else {
        eprintln!("skip: no Redis");
        return;
    };
    let Some(user_id) = make_user(&pg).await else {
        eprintln!("skip: cannot make_user");
        return;
    };

    // Orphan id (in Redis only) + survivor id (in both PG + Redis).
    let orphan_id: i64 = 970_001_000_001;
    let survivor_id: i64 = 970_001_000_002;
    purge_test_state(&mut conn, user_id, &[orphan_id, survivor_id]).await;

    // Seed Redis with both via apply_frame.
    apply_frame(&mut conn, &upsert_frame(orphan_id, user_id))
        .await
        .unwrap();
    apply_frame(&mut conn, &upsert_frame(survivor_id, user_id))
        .await
        .unwrap();
    // Seed PG with only the survivor.
    sqlx::query(
        "INSERT INTO orders (id, user_id, symbol, side, order_type, price, quantity, status)
         VALUES ($1, $2, 'BTC_USDT', 'buy', 'limit', 70000, 0.1, 'PENDING')",
    )
    .bind(survivor_id)
    .bind(user_id)
    .execute(&pg)
    .await
    .expect("seed survivor in PG");

    // Sanity: both in Redis before reconcile.
    let orphan_active_before: bool = conn
        .sismember(KEY_ACTIVE_ORDERS, orphan_id)
        .await
        .unwrap_or(false);
    let survivor_active_before: bool = conn
        .sismember(KEY_ACTIVE_ORDERS, survivor_id)
        .await
        .unwrap_or(false);
    assert!(orphan_active_before, "orphan should start in active_orders");
    assert!(survivor_active_before, "survivor should start in active_orders");

    let stats = reconcile_orphans(&pg, &mut conn).await.unwrap();
    assert!(
        stats.orders_removed >= 1,
        "should have removed at least 1 orphan, got stats={stats:?}"
    );

    let orphan_active_after: bool = conn
        .sismember(KEY_ACTIVE_ORDERS, orphan_id)
        .await
        .unwrap_or(false);
    let survivor_active_after: bool = conn
        .sismember(KEY_ACTIVE_ORDERS, survivor_id)
        .await
        .unwrap_or(false);
    assert!(!orphan_active_after, "orphan must be SREM'd from active_orders");
    assert!(
        survivor_active_after,
        "survivor must remain (still in PG)"
    );

    // user_orders cleanup too.
    let orphan_user: bool = conn
        .sismember(key_user_orders(user_id), orphan_id)
        .await
        .unwrap_or(false);
    let survivor_user: bool = conn
        .sismember(key_user_orders(user_id), survivor_id)
        .await
        .unwrap_or(false);
    assert!(!orphan_user, "orphan must be SREM'd from user_orders");
    assert!(survivor_user, "survivor must remain in user_orders");

    // HASH cleanup for orphan, retained for survivor.
    let orphan_exists: bool = conn.exists(key_order(orphan_id)).await.unwrap_or(false);
    let survivor_exists: bool = conn.exists(key_order(survivor_id)).await.unwrap_or(false);
    assert!(!orphan_exists, "orphan HASH must be DEL'd");
    assert!(survivor_exists, "survivor HASH must be retained");

    // Cleanup.
    purge_test_state(&mut conn, user_id, &[orphan_id, survivor_id]).await;
    sqlx::query("DELETE FROM orders WHERE id = ANY($1::bigint[])")
        .bind(&[orphan_id, survivor_id])
        .execute(&pg)
        .await
        .ok();
    sqlx::query("DELETE FROM users WHERE id=$1")
        .bind(user_id)
        .execute(&pg)
        .await
        .ok();
}

#[serial_test::serial]
#[tokio::test]
async fn reconcile_is_idempotent_on_clean_state() {
    let Some(pg) = try_pg().await else {
        eprintln!("skip: no PG");
        return;
    };
    let Some(mut conn) = try_redis().await else {
        eprintln!("skip: no Redis");
        return;
    };
    // Just run reconcile twice and check that nothing throws and the second
    // call removes <= as many orders as the first (in a clean window).
    let first = reconcile_orphans(&pg, &mut conn).await.unwrap();
    let second = reconcile_orphans(&pg, &mut conn).await.unwrap();
    assert!(
        second.orders_removed <= first.orders_removed.max(50),
        "second pass should converge: first={first:?} second={second:?}"
    );
}
