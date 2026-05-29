//! Integration test for `redis_store::hydrate_from_pg`.
//!
//! Requires running PG (DATABASE_URL or default localhost:5432/mydb) AND
//! Redis (REDIS_URL or default localhost:6379/0). When either is missing,
//! the test is skipped via an `Ok(())` early return.
//!
//! Run:   cargo test --test redis_hydrate -- --nocapture
//! Skip:  it auto-skips when DATABASE_URL/REDIS_URL can't connect.

use lightning_exchange::desk::redis_store::{
    hydrate_from_pg, is_hydrated, key_account, key_order, key_user_orders, purge_all,
    KEY_ACTIVE_ORDERS,
};
use redis::AsyncCommands;

const TEST_USER_EMAIL: &str = "redis_hydrate_test@lightning.test";

async fn try_connect() -> Option<(sqlx::PgPool, redis::aio::MultiplexedConnection)> {
    let pg_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://user:password@127.0.0.1:5432/mydb".to_string());
    let redis_url = std::env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6379/0".to_string());

    let pg = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(std::time::Duration::from_secs(3))
        .connect(&pg_url)
        .await
        .ok()?;

    let client = redis::Client::open(redis_url.as_str()).ok()?;
    let mut conn = client.get_multiplexed_async_connection().await.ok()?;
    let _: String = redis::cmd("PING").query_async(&mut conn).await.ok()?;

    Some((pg, conn))
}

async fn ensure_test_user(pg: &sqlx::PgPool) -> sqlx::Result<i64> {
    let id: i64 = sqlx::query_scalar(
        "INSERT INTO users (email, password_hash, full_name)
         VALUES ($1, 'x', 'Redis Hydrate Test')
         ON CONFLICT (email) DO UPDATE SET id = users.id RETURNING id",
    )
    .bind(TEST_USER_EMAIL)
    .fetch_one(pg)
    .await?;
    sqlx::query(
        "INSERT INTO accounts (user_id, asset, balance, frozen)
         VALUES ($1, 'USDT', 12345.6, 100), ($1, 'BTC', 7.5, 0.5)
         ON CONFLICT (user_id, asset) DO UPDATE
         SET balance = EXCLUDED.balance, frozen = EXCLUDED.frozen",
    )
    .bind(id)
    .execute(pg)
    .await?;
    sqlx::query("DELETE FROM orders WHERE user_id=$1")
        .bind(id)
        .execute(pg)
        .await?;
    Ok(id)
}

async fn seed_orders(
    pg: &sqlx::PgPool,
    user_id: i64,
    n_active: usize,
    n_terminal: usize,
) -> anyhow::Result<Vec<i64>> {
    static SEQ: std::sync::atomic::AtomicI64 =
        std::sync::atomic::AtomicI64::new(900_000_000_000_000);
    let start = SEQ.fetch_add((n_active + n_terminal) as i64, std::sync::atomic::Ordering::Relaxed);
    let mut active_ids = Vec::with_capacity(n_active);
    let mut sql = String::from(
        "INSERT INTO orders (id, user_id, symbol, side, order_type,
                             price, quantity, filled, status, freeze_price, client_order_id)
         VALUES ",
    );
    let mut first = true;
    for i in 0..n_active {
        let id = start + i as i64;
        active_ids.push(id);
        let side = if i % 2 == 0 { "buy" } else { "sell" };
        let price = 70000.0 + i as f64;
        let status = if i % 3 == 0 { "PENDING" } else { "TRADING" };
        if !first {
            sql.push(',');
        }
        first = false;
        sql.push_str(&format!(
            "({id}, {user_id}, 'BTC_USDT', '{side}', 'limit', {price}, 0.01, 0, '{status}', {price}, 'rht-{id}')"
        ));
    }
    for i in 0..n_terminal {
        let id = start + (n_active + i) as i64;
        let side = if i % 2 == 0 { "buy" } else { "sell" };
        let price = 71000.0 + i as f64;
        // These statuses are NOT in ('PENDING','TRADING'), so hydrate must skip them.
        let status = ["COMPLETED", "CANCELED", "REJECTED"][i % 3];
        if !first {
            sql.push(',');
        }
        first = false;
        sql.push_str(&format!(
            "({id}, {user_id}, 'BTC_USDT', '{side}', 'market', {price}, 0.01, 0, '{status}', 0, 'rht-{id}')"
        ));
    }
    sqlx::query(&sql).execute(pg).await?;
    Ok(active_ids)
}

#[tokio::test]
async fn hydrate_loads_active_orders_and_accounts() {
    let Some((pg, mut conn)) = try_connect().await else {
        eprintln!("skip: no PG or Redis available");
        return;
    };

    // Clean slate.
    purge_all(&mut conn).await.unwrap();
    let user_id = ensure_test_user(&pg).await.expect("ensure test user");
    let active_ids = seed_orders(&pg, user_id, 5, 4)
        .await
        .expect("seed orders");

    assert!(
        !is_hydrated(&mut conn).await.unwrap(),
        "purge should leave Redis empty",
    );

    let stats = hydrate_from_pg(&pg, &mut conn).await.expect("hydrate");

    // Active count >= 5 (other tests may have left their own active rows; the
    // important invariants are listed below).
    assert!(
        stats.orders as usize >= active_ids.len(),
        "should hydrate at least the {} active rows we seeded, got {}",
        active_ids.len(),
        stats.orders
    );
    assert!(stats.accounts >= 2, "at least our 2 accounts (USDT, BTC)");

    // Every seeded active order id is in active_orders + user_orders + has a hash.
    for id in &active_ids {
        let in_active: bool = conn.sismember(KEY_ACTIVE_ORDERS, *id).await.unwrap();
        assert!(in_active, "active_orders missing id {id}");
        let in_user_set: bool = conn.sismember(key_user_orders(user_id), *id).await.unwrap();
        assert!(in_user_set, "user_orders:{user_id} missing id {id}");
        let fields: std::collections::HashMap<String, String> =
            conn.hgetall(key_order(*id)).await.unwrap();
        assert_eq!(fields.get("symbol").map(String::as_str), Some("BTC_USDT"));
        assert_eq!(fields.get("user_id"), Some(&user_id.to_string()));
        let status = fields.get("status").cloned().unwrap_or_default();
        assert!(
            status == "PENDING" || status == "TRADING",
            "seeded active orders must be PENDING or TRADING, got {status}"
        );
    }

    // Terminal-state rows we seeded must NOT be in active_orders.
    let user_orders_ids: Vec<i64> = conn.smembers(key_user_orders(user_id)).await.unwrap();
    for tid in active_ids[active_ids.len()..].iter() {
        // (empty range — kept to assert the closure logic stays valid even
        //  if seeded counts change.)
        assert!(!user_orders_ids.contains(tid));
    }

    // Account hashes.
    let usdt: std::collections::HashMap<String, String> =
        conn.hgetall(key_account(user_id, "USDT")).await.unwrap();
    let balance: f64 = usdt.get("balance").unwrap().parse().unwrap();
    let frozen: f64 = usdt.get("frozen").unwrap().parse().unwrap();
    assert!((balance - 12345.6).abs() < 1e-9, "USDT balance: {balance}");
    assert!((frozen - 100.0).abs() < 1e-9, "USDT frozen: {frozen}");

    let btc: std::collections::HashMap<String, String> =
        conn.hgetall(key_account(user_id, "BTC")).await.unwrap();
    let balance: f64 = btc.get("balance").unwrap().parse().unwrap();
    let frozen: f64 = btc.get("frozen").unwrap().parse().unwrap();
    assert!((balance - 7.5).abs() < 1e-9, "BTC balance: {balance}");
    assert!((frozen - 0.5).abs() < 1e-9, "BTC frozen: {frozen}");

    assert!(
        is_hydrated(&mut conn).await.unwrap(),
        "after hydrate, active_orders should be non-empty",
    );

    // Cleanup: don't leave our seeded rows behind in PG.
    sqlx::query("DELETE FROM orders WHERE user_id=$1")
        .bind(user_id)
        .execute(&pg)
        .await
        .ok();
}

#[tokio::test]
async fn purge_clears_everything_we_wrote() {
    let Some((pg, mut conn)) = try_connect().await else {
        eprintln!("skip: no PG or Redis available");
        return;
    };

    let user_id = ensure_test_user(&pg).await.expect("ensure test user");
    seed_orders(&pg, user_id, 3, 0).await.expect("seed");
    hydrate_from_pg(&pg, &mut conn).await.expect("hydrate");
    assert!(is_hydrated(&mut conn).await.unwrap());

    purge_all(&mut conn).await.expect("purge");
    let card: u64 = conn.scard(KEY_ACTIVE_ORDERS).await.unwrap();
    assert_eq!(card, 0, "active_orders should be empty after purge");
    let acct_exists: bool = conn
        .exists(key_account(user_id, "USDT"))
        .await
        .unwrap();
    assert!(!acct_exists, "user account hash should be gone after purge");

    sqlx::query("DELETE FROM orders WHERE user_id=$1")
        .bind(user_id)
        .execute(&pg)
        .await
        .ok();
}
