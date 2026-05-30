//! Integration test for `pg_store::PgWriteBatch` — exercises the wire-format
//! to PG path without running Aeron. Verifies row count, status, fill, and
//! account writes survive a flush.
//!
//! Requires PG at DATABASE_URL (defaults to
//! postgres://user:password@localhost:5432/mydb). Skips gracefully when PG
//! is unreachable.

use lightning_exchange::desk::pg_store::PgWriteBatch;
use lightning_exchange::transport::persist_event::{
    pack_str, AccountSetPayload, OrderDeletePayload, OrderFillUpdatePayload, OrderUpsertPayload,
    PersistFrame, TradeInsertPayload,
};
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

async fn make_user(pg: &PgPool) -> Option<i64> {
    let email = format!("pgwriter_{}@lightning.test", Uuid::new_v4());
    let row = sqlx::query(
        "INSERT INTO users (email, password_hash) VALUES ($1, $2) RETURNING id",
    )
    .bind(email)
    .bind("$argon2id$v=19$test$abcdefghijklmnopqrstuvwxyz0123456789")
    .fetch_one(pg)
    .await
    .ok()?;
    let id: i64 = row.try_get("id").ok()?;
    Some(id)
}

async fn cleanup(pg: &PgPool, user_id: i64) {
    let _ = sqlx::query("DELETE FROM trades WHERE buy_order_id IN (SELECT id FROM orders WHERE user_id=$1) OR sell_order_id IN (SELECT id FROM orders WHERE user_id=$1)")
        .bind(user_id)
        .execute(pg)
        .await;
    let _ = sqlx::query("DELETE FROM orders WHERE user_id=$1")
        .bind(user_id)
        .execute(pg)
        .await;
    let _ = sqlx::query("DELETE FROM accounts WHERE user_id=$1")
        .bind(user_id)
        .execute(pg)
        .await;
    let _ = sqlx::query("DELETE FROM users WHERE id=$1")
        .bind(user_id)
        .execute(pg)
        .await;
}

fn upsert(id: i64, user_id: i64, status: u8) -> PersistFrame {
    PersistFrame::order_upsert(OrderUpsertPayload {
        id,
        user_id,
        symbol: pack_str("BTC_USDT"),
        side: 0,
        status,
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

#[tokio::test]
async fn upsert_then_fill_then_delete_round_trips_through_pg() {
    let Some(pg) = try_pg().await else {
        eprintln!("skip: no PG");
        return;
    };
    // Need to bootstrap an accounts row to satisfy CHECK on flush_accounts.
    let Some(user_id) = make_user(&pg).await else {
        eprintln!("skip: cannot make_user");
        return;
    };

    let id_a: i64 = 980_000_100_001;
    let id_b: i64 = 980_000_100_002;

    let mut batch = PgWriteBatch::new();
    assert!(batch.push(&upsert(id_a, user_id, 1))); // PENDING
    assert!(batch.push(&upsert(id_b, user_id, 1)));
    let written = batch.flush(&pg).await.expect("flush upserts");
    assert_eq!(written, 2);

    // Verify rows landed with the right status.
    let rows: Vec<(i64, String, f64)> = sqlx::query_as(
        "SELECT id, status, COALESCE(price, -1) FROM orders WHERE id = ANY($1::bigint[]) ORDER BY id",
    )
    .bind(&vec![id_a, id_b])
    .fetch_all(&pg)
    .await
    .expect("select after upsert");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].1, "PENDING");
    assert_eq!(rows[0].2, 70000.0);

    // Apply a fill update — only status and filled may change.
    let mut batch = PgWriteBatch::new();
    let fill = PersistFrame::order_fill_update(OrderFillUpdatePayload {
        id: id_a,
        filled: 0.04,
        status: 2, // TRADING
        _pad: [0; 7],
    });
    assert!(batch.push(&fill));
    batch.flush(&pg).await.expect("flush fill");

    let row: (String, f64) = sqlx::query_as("SELECT status, filled FROM orders WHERE id=$1")
        .bind(id_a)
        .fetch_one(&pg)
        .await
        .expect("select after fill");
    assert_eq!(row.0, "TRADING");
    assert!((row.1 - 0.04).abs() < 1e-9);

    // Other order untouched.
    let row_b: (String, f64) = sqlx::query_as("SELECT status, filled FROM orders WHERE id=$1")
        .bind(id_b)
        .fetch_one(&pg)
        .await
        .expect("select other");
    assert_eq!(row_b.0, "PENDING");
    assert!(row_b.1.abs() < 1e-9);

    // Delete one — verify single-row drop.
    let mut batch = PgWriteBatch::new();
    assert!(batch.push(&PersistFrame::order_delete(OrderDeletePayload { id: id_a })));
    let removed = batch.flush(&pg).await.expect("flush delete");
    assert_eq!(removed, 1);
    let still: Option<(i64,)> = sqlx::query_as("SELECT id FROM orders WHERE id=$1")
        .bind(id_a)
        .fetch_optional(&pg)
        .await
        .expect("select after delete");
    assert!(still.is_none(), "delete must remove the row");

    cleanup(&pg, user_id).await;
}

#[tokio::test]
async fn account_set_upserts() {
    let Some(pg) = try_pg().await else {
        eprintln!("skip: no PG");
        return;
    };
    let Some(user_id) = make_user(&pg).await else {
        eprintln!("skip: cannot make_user");
        return;
    };

    let f1 = PersistFrame::account_set(AccountSetPayload {
        user_id,
        asset: pack_str("USDT"),
        balance: 1000.0,
        frozen: 100.0,
    });
    let mut batch = PgWriteBatch::new();
    assert!(batch.push(&f1));
    batch.flush(&pg).await.expect("flush insert");

    let row: (f64, f64) = sqlx::query_as("SELECT balance, frozen FROM accounts WHERE user_id=$1 AND asset='USDT'")
        .bind(user_id)
        .fetch_one(&pg)
        .await
        .expect("select after first upsert");
    assert!((row.0 - 1000.0).abs() < 1e-9);
    assert!((row.1 - 100.0).abs() < 1e-9);

    // Second set must overwrite.
    let f2 = PersistFrame::account_set(AccountSetPayload {
        user_id,
        asset: pack_str("USDT"),
        balance: 900.0,
        frozen: 50.0,
    });
    let mut batch = PgWriteBatch::new();
    assert!(batch.push(&f2));
    batch.flush(&pg).await.expect("flush update");
    let row: (f64, f64) = sqlx::query_as("SELECT balance, frozen FROM accounts WHERE user_id=$1 AND asset='USDT'")
        .bind(user_id)
        .fetch_one(&pg)
        .await
        .expect("select after second upsert");
    assert!((row.0 - 900.0).abs() < 1e-9);
    assert!((row.1 - 50.0).abs() < 1e-9);

    cleanup(&pg, user_id).await;
}

#[tokio::test]
async fn trade_insert_lands() {
    let Some(pg) = try_pg().await else {
        eprintln!("skip: no PG");
        return;
    };
    let Some(user_id) = make_user(&pg).await else {
        eprintln!("skip: cannot make_user");
        return;
    };

    let buy_id: i64 = 980_000_200_001;
    let sell_id: i64 = 980_000_200_002;
    let f = PersistFrame::trade_insert(TradeInsertPayload {
        buy_order_id: buy_id,
        sell_order_id: sell_id,
        symbol: pack_str("BTC_USDT"),
        price: 70123.5,
        qty: 0.0123,
        ts_ms: 1_700_000_001_000,
    });
    let mut batch = PgWriteBatch::new();
    assert!(batch.push(&f));
    let n = batch.flush(&pg).await.expect("flush trade");
    assert_eq!(n, 1);

    let row: (String, i64, i64, f64, f64) = sqlx::query_as(
        "SELECT symbol, buy_order_id, sell_order_id, price, quantity FROM trades WHERE buy_order_id=$1 AND sell_order_id=$2",
    )
    .bind(buy_id)
    .bind(sell_id)
    .fetch_one(&pg)
    .await
    .expect("select trade");
    assert_eq!(row.0, "BTC_USDT");
    assert!((row.3 - 70123.5).abs() < 1e-9);
    assert!((row.4 - 0.0123).abs() < 1e-9);

    cleanup(&pg, user_id).await;
}
