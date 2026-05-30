//! Integration test for `redis_store::apply_frame` — exercises the wire
//! format from publisher (encode) through subscriber (decode) into the
//! Redis HASH state, without needing a running Aeron driver.
//!
//! Requires Redis (REDIS_URL env or default localhost:6379/0). Skips
//! gracefully when Redis is unreachable.

use lightning_exchange::desk::redis_store::{
    apply_frame, key_account, key_order, key_user_orders, KEY_ACTIVE_ORDERS,
};
use lightning_exchange::transport::persist_event::{
    pack_str, AccountSetPayload, OrderDeletePayload, OrderFillUpdatePayload, OrderUpsertPayload,
    PersistFrame,
};
use redis::AsyncCommands;

async fn try_redis() -> Option<redis::aio::MultiplexedConnection> {
    let url = std::env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6379/0".to_string());
    let client = redis::Client::open(url).ok()?;
    let mut conn = client.get_multiplexed_async_connection().await.ok()?;
    let _: String = redis::cmd("PING").query_async(&mut conn).await.ok()?;
    Some(conn)
}

#[serial_test::serial]
#[tokio::test]
async fn upsert_then_fill_then_delete_via_frames() {
    let Some(mut conn) = try_redis().await else {
        eprintln!("skip: no Redis");
        return;
    };
    let id: i64 = 990_000_000_000_001;
    let user_id: i64 = 4242;
    // Cleanup any prior run.
    let _: i64 = conn.del(key_order(id)).await.unwrap_or(0);
    let _: i64 = conn.srem(KEY_ACTIVE_ORDERS, id).await.unwrap_or(0);
    let _: i64 = conn.srem(key_user_orders(user_id), id).await.unwrap_or(0);

    // 1) Apply OrderUpsert — full row lands as a HASH.
    let f = PersistFrame::order_upsert(OrderUpsertPayload {
        id,
        user_id,
        symbol: pack_str("BTC_USDT"),
        side: 0,
        status: 0, // PENDING (DbOrderStatus::Pending = 0)
        _pad: [0; 6],
        order_type: pack_str("limit"),
        price: 73000.5,
        qty: 0.05,
        filled: 0.0,
        freeze_price: 73000.5,
        client_order_id: pack_str("rht-upsert-frame"),
        created_at_ms: 1_700_000_000_000,
    });
    apply_frame(&mut conn, &f).await.expect("upsert");
    let fields: std::collections::HashMap<String, String> =
        conn.hgetall(key_order(id)).await.unwrap();
    assert_eq!(fields.get("status").map(String::as_str), Some("PENDING"));
    assert_eq!(fields.get("filled").map(String::as_str), Some("0"));
    assert_eq!(fields.get("price").map(String::as_str), Some("73000.5"));
    assert!(conn.sismember(KEY_ACTIVE_ORDERS, id).await.unwrap_or(false));
    assert!(
        conn.sismember(key_user_orders(user_id), id).await.unwrap_or(false),
        "user_orders set should include the id"
    );

    // 2) Apply OrderFillUpdate — only filled and status change; other fields
    //    must NOT be overwritten.
    let f = PersistFrame::order_fill_update(OrderFillUpdatePayload {
        id,
        filled: 0.03,
        status: 1, // TRADING (DbOrderStatus::Trading = 1)
        _pad: [0; 7],
    });
    apply_frame(&mut conn, &f).await.expect("fill update");
    let fields: std::collections::HashMap<String, String> =
        conn.hgetall(key_order(id)).await.unwrap();
    assert_eq!(fields.get("status").map(String::as_str), Some("TRADING"));
    assert_eq!(fields.get("filled").map(String::as_str), Some("0.03"));
    // Non-touched fields should still match the original.
    assert_eq!(
        fields.get("price").map(String::as_str),
        Some("73000.5"),
        "OrderFillUpdate must not overwrite price"
    );
    assert_eq!(
        fields.get("freeze_price").map(String::as_str),
        Some("73000.5"),
        "OrderFillUpdate must not overwrite freeze_price"
    );

    // 3) Apply OrderDelete — HASH gone, indexes cleaned.
    let f = PersistFrame::order_delete(OrderDeletePayload { id });
    apply_frame(&mut conn, &f).await.expect("delete");
    let exists: bool = conn.exists(key_order(id)).await.unwrap();
    assert!(!exists, "HASH should be deleted");
    assert!(
        !conn.sismember(KEY_ACTIVE_ORDERS, id).await.unwrap_or(false),
        "id must be removed from active_orders"
    );
    assert!(
        !conn
            .sismember(key_user_orders(user_id), id)
            .await
            .unwrap_or(false),
        "id must be removed from user_orders"
    );
}

#[serial_test::serial]
#[tokio::test]
async fn account_set_overwrites_balance_and_frozen() {
    let Some(mut conn) = try_redis().await else {
        eprintln!("skip: no Redis");
        return;
    };
    let user_id: i64 = 4242;
    let asset = "USDT";
    let _: i64 = conn.del(key_account(user_id, asset)).await.unwrap_or(0);

    let f = PersistFrame::account_set(AccountSetPayload {
        user_id,
        asset: pack_str(asset),
        balance: 100.5,
        frozen: 10.0,
    });
    apply_frame(&mut conn, &f).await.unwrap();
    let row: (String, String) = conn
        .hget(key_account(user_id, asset), &["balance", "frozen"])
        .await
        .unwrap();
    assert_eq!(row.0, "100.5");
    assert_eq!(row.1, "10");

    // Second update — overwrites.
    let f = PersistFrame::account_set(AccountSetPayload {
        user_id,
        asset: pack_str(asset),
        balance: 99.0,
        frozen: 9.5,
    });
    apply_frame(&mut conn, &f).await.unwrap();
    let row: (String, String) = conn
        .hget(key_account(user_id, asset), &["balance", "frozen"])
        .await
        .unwrap();
    assert_eq!(row.0, "99");
    assert_eq!(row.1, "9.5");

    let _: i64 = conn.del(key_account(user_id, asset)).await.unwrap_or(0);
}

#[serial_test::serial]
#[tokio::test]
async fn fill_update_on_missing_id_is_noop() {
    let Some(mut conn) = try_redis().await else {
        eprintln!("skip: no Redis");
        return;
    };
    let id: i64 = 990_000_000_000_777;
    let _: i64 = conn.del(key_order(id)).await.unwrap_or(0);

    let f = PersistFrame::order_fill_update(OrderFillUpdatePayload {
        id,
        filled: 0.1,
        status: 1, // TRADING
        _pad: [0; 7],
    });
    apply_frame(&mut conn, &f).await.unwrap();
    // Must NOT create a partial hash from the fill update — we lack
    // the schema fields, so applying to a non-existent id is a no-op.
    let exists: bool = conn.exists(key_order(id)).await.unwrap();
    assert!(!exists, "fill update must not create a partial HASH");
}
