//! Integration tests for the Redis reader path used by REST handlers
//! (`get_order`, `list_user_open_orders`). Seeds state via the PR2 wire
//! format then reads back through the same path REST will use.
//!
//! Requires Redis at REDIS_URL (defaults to redis://127.0.0.1:6379/0).
//! Skips gracefully when Redis is unreachable.

use lightning_exchange::desk::redis_store::{
    KEY_ACTIVE_ORDERS, apply_frame, get_order, key_account, key_order, key_user_assets,
    key_user_coid, key_user_orders, list_user_accounts, list_user_open_orders,
};
use lightning_exchange::transport::persist_event::{
    AccountSetPayload, OrderDeletePayload, OrderFillUpdatePayload, OrderUpsertPayload,
    PersistFrame, pack_str,
};
use lightning_exchange::money::AmountAtoms;
use redis::AsyncCommands;

async fn try_redis() -> Option<redis::aio::MultiplexedConnection> {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/0".to_string());
    let client = redis::Client::open(url).ok()?;
    let mut conn = client.get_multiplexed_async_connection().await.ok()?;
    let _: String = redis::cmd("PING").query_async(&mut conn).await.ok()?;
    Some(conn)
}

async fn purge_user(conn: &mut redis::aio::MultiplexedConnection, user_id: i64, ids: &[i64]) {
    let mut pipe = redis::pipe();
    for id in ids {
        pipe.del(key_order(*id)).ignore();
        pipe.srem(KEY_ACTIVE_ORDERS, id).ignore();
    }
    pipe.del(key_user_orders(user_id)).ignore();
    pipe.del(key_user_coid(user_id)).ignore();
    let assets: Vec<String> = conn
        .smembers(key_user_assets(user_id))
        .await
        .unwrap_or_default();
    for a in &assets {
        pipe.del(key_account(user_id, a)).ignore();
    }
    pipe.del(key_user_assets(user_id)).ignore();
    let _: () = pipe.query_async(conn).await.unwrap_or(());
}

fn upsert_frame(id: i64, user_id: i64, symbol: &str, price: f64, qty: f64) -> PersistFrame {
    PersistFrame::order_upsert(OrderUpsertPayload {
        id,
        user_id,
        symbol: pack_str(symbol),
        side: 0,
        status: 0, // PENDING (DbOrderStatus::Pending = 0)
        _pad: [0; 6],
        order_type: pack_str("limit"),
        price,
        qty,
        filled: 0.0,
        freeze_price: price,
        client_order_id: pack_str(""),
        created_at_ms: 1_700_000_000_000 + id, // strictly increasing for sort assertions
    })
}

fn account_set_frame(user_id: i64, asset: &str, balance: f64, frozen: f64) -> PersistFrame {
    PersistFrame::account_set(AccountSetPayload {
        user_id,
        asset: pack_str(asset),
        balance,
        frozen,
        balance_atoms: AmountAtoms::from_f64_round(balance).unwrap().atoms(),
        frozen_atoms: AmountAtoms::from_f64_round(frozen).unwrap().atoms(),
    })
}

#[serial_test::serial]
#[tokio::test]
async fn list_user_open_orders_returns_sorted_and_filtered() {
    let Some(mut conn) = try_redis().await else {
        eprintln!("skip: no Redis");
        return;
    };
    let user_id: i64 = 991_001;
    let ids = [
        980_000_000_001,
        980_000_000_002,
        980_000_000_003,
        980_000_000_004,
    ];
    purge_user(&mut conn, user_id, &ids).await;

    // Seed 2 BTC + 2 ETH orders.
    apply_frame(
        &mut conn,
        &upsert_frame(ids[0], user_id, "BTC_USDT", 70000.0, 0.01),
    )
    .await
    .unwrap();
    apply_frame(
        &mut conn,
        &upsert_frame(ids[1], user_id, "BTC_USDT", 70100.0, 0.02),
    )
    .await
    .unwrap();
    apply_frame(
        &mut conn,
        &upsert_frame(ids[2], user_id, "ETH_USDT", 3500.0, 1.0),
    )
    .await
    .unwrap();
    apply_frame(
        &mut conn,
        &upsert_frame(ids[3], user_id, "ETH_USDT", 3510.0, 1.5),
    )
    .await
    .unwrap();

    // No symbol filter, all 4 returned newest-first.
    let all = list_user_open_orders(&mut conn, user_id, None, 100)
        .await
        .unwrap();
    assert_eq!(all.len(), 4);
    // ids were strictly increasing by created_at_ms, so newest = ids[3].
    assert_eq!(all[0].id, ids[3]);
    assert_eq!(all[1].id, ids[2]);
    assert_eq!(all[2].id, ids[1]);
    assert_eq!(all[3].id, ids[0]);

    // Symbol filter.
    let btc = list_user_open_orders(&mut conn, user_id, Some("BTC_USDT"), 100)
        .await
        .unwrap();
    assert_eq!(btc.len(), 2);
    assert!(btc.iter().all(|o| o.symbol == "BTC_USDT"));

    // Limit truncates after sort — top-N newest.
    let top2 = list_user_open_orders(&mut conn, user_id, None, 2)
        .await
        .unwrap();
    assert_eq!(top2.len(), 2);
    assert_eq!(top2[0].id, ids[3]);
    assert_eq!(top2[1].id, ids[2]);

    purge_user(&mut conn, user_id, &ids).await;
}

#[serial_test::serial]
#[tokio::test]
async fn get_order_enforces_user_ownership() {
    let Some(mut conn) = try_redis().await else {
        eprintln!("skip: no Redis");
        return;
    };
    let owner: i64 = 991_002;
    let other: i64 = 991_003;
    let id: i64 = 980_000_000_777;
    purge_user(&mut conn, owner, &[id]).await;

    apply_frame(
        &mut conn,
        &upsert_frame(id, owner, "BTC_USDT", 70000.0, 0.5),
    )
    .await
    .unwrap();

    let owned = get_order(&mut conn, id, owner).await.unwrap();
    assert!(owned.is_some());
    let foreign = get_order(&mut conn, id, other).await.unwrap();
    assert!(foreign.is_none(), "must not leak orders to other users");

    purge_user(&mut conn, owner, &[id]).await;
}

#[serial_test::serial]
#[tokio::test]
async fn fill_update_bumps_updated_at_ms() {
    let Some(mut conn) = try_redis().await else {
        eprintln!("skip: no Redis");
        return;
    };
    let user_id: i64 = 991_004;
    let id: i64 = 980_000_000_555;
    purge_user(&mut conn, user_id, &[id]).await;

    // Use a fixed past timestamp so the fill-update's now_ms() is guaranteed greater.
    let mut frame = upsert_frame(id, user_id, "BTC_USDT", 70000.0, 0.1);
    if let Some(mut p) = frame.as_order_upsert() {
        p.created_at_ms = 1; // epoch+1ms
        frame = PersistFrame::order_upsert(p);
    }
    apply_frame(&mut conn, &frame).await.unwrap();
    let before = get_order(&mut conn, id, user_id).await.unwrap().unwrap();
    let created_ms = before.created_at.timestamp_millis();
    let updated_ms_before = before.updated_at.timestamp_millis();
    assert_eq!(created_ms, 1);
    assert_eq!(
        updated_ms_before, created_ms,
        "on upsert, updated_at_ms tracks created_at_ms"
    );

    // Sleep a hair to ensure the now_ms() inside apply_order_fill_update advances past created_at_ms.
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    apply_frame(
        &mut conn,
        &PersistFrame::order_fill_update(OrderFillUpdatePayload {
            id,
            filled: 0.03,
            status: 1, // TRADING
            _pad: [0; 7],
        }),
    )
    .await
    .unwrap();
    let after = get_order(&mut conn, id, user_id).await.unwrap().unwrap();
    assert_eq!(after.status, "TRADING", "fill update status=1 → TRADING");
    assert_eq!(after.filled, 0.03);
    assert!(
        after.updated_at.timestamp_millis() > created_ms,
        "updated_at_ms must advance on fill: before={created_ms} after={}",
        after.updated_at.timestamp_millis()
    );

    apply_frame(
        &mut conn,
        &PersistFrame::order_delete(OrderDeletePayload { id }),
    )
    .await
    .unwrap();
    purge_user(&mut conn, user_id, &[id]).await;
}

#[serial_test::serial]
#[tokio::test]
async fn list_user_accounts_returns_seeded_assets() {
    let Some(mut conn) = try_redis().await else {
        eprintln!("skip: no Redis");
        return;
    };
    let user_id: i64 = 991_005;
    purge_user(&mut conn, user_id, &[]).await;

    for (asset, balance, frozen) in &[("USDT", 1000.0, 100.0), ("BTC", 0.5, 0.05)] {
        apply_frame(
            &mut conn,
            &account_set_frame(user_id, asset, *balance, *frozen),
        )
        .await
        .unwrap();
    }

    let mut rows = list_user_accounts(&mut conn, user_id).await.unwrap();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, "BTC");
    assert!((rows[0].1 - 0.5).abs() < 1e-9);
    assert!((rows[0].2 - 0.05).abs() < 1e-9);
    assert_eq!(rows[1].0, "USDT");
    assert!((rows[1].1 - 1000.0).abs() < 1e-9);
    assert!((rows[1].2 - 100.0).abs() < 1e-9);

    purge_user(&mut conn, user_id, &[]).await;
}
