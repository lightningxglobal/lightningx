//! Integration test: SharedRateLimiter bucket persistence via Redis.
//! Verifies the snapshot → store → load → restore cycle that desk-server
//! runs (2s snapshot task + restore at startup), i.e. a process restart
//! cannot be used to mint fresh rate budgets.

use lightning_exchange::desk::redis_store::{load_rate_buckets, store_rate_buckets};
use lightning_exchange::rate_limit::{OpClass, RateLimitPolicy, SharedRateLimiter};
use redis::AsyncCommands;

async fn try_redis() -> Option<redis::aio::MultiplexedConnection> {
    let url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/0".to_string());
    let client = redis::Client::open(url).ok()?;
    let mut conn = client.get_multiplexed_async_connection().await.ok()?;
    let pong: String = redis::cmd("PING").query_async(&mut conn).await.ok()?;
    (pong == "PONG").then_some(conn)
}

#[tokio::test]
async fn buckets_survive_simulated_restart_via_redis() {
    let Some(mut conn) = try_redis().await else {
        eprintln!("skip: no Redis");
        return;
    };
    // Isolate from any real desk state.
    let _: Result<(), _> = conn
        .del(lightning_exchange::desk::redis_store::KEY_RATE_BUCKETS)
        .await;

    let policy = RateLimitPolicy {
        requests_per_second: 10,
        burst_capacity: 10,
    };
    let user: i64 = 987_654_321;

    // "First process lifetime": burn most of the budget, snapshot to Redis.
    let limiter = SharedRateLimiter::new(Some(policy), None);
    for _ in 0..8 {
        limiter.try_consume(user, OpClass::Place).expect("budget");
    }
    store_rate_buckets(&mut conn, &limiter.snapshot())
        .await
        .expect("store");

    // "Restart": fresh limiter restores from Redis. Without restore the
    // user would get a fresh burst of 10; with it only ~2 remain.
    let restarted = SharedRateLimiter::new(Some(policy), None);
    let loaded = load_rate_buckets(&mut conn).await.expect("load");
    assert!(
        loaded.iter().any(|&(uid, op, _, _)| uid == user && op == 0),
        "persisted bucket present"
    );
    restarted.restore(&loaded);

    let mut granted = 0;
    while restarted.try_consume(user, OpClass::Place).is_ok() {
        granted += 1;
        assert!(granted < 100, "must exhaust");
    }
    assert!(
        granted <= 3,
        "restart must not mint a fresh budget (granted {granted}, expected ≤3)"
    );

    let _: Result<(), _> = conn
        .del(lightning_exchange::desk::redis_store::KEY_RATE_BUCKETS)
        .await;

    // Hostile/corrupt persisted fields must be skipped, not panic.
    corrupted_entries_check(&mut conn).await;
}

/// Runs inside the main test (not standalone): both scenarios touch the
/// same shared Redis key and would race under parallel test execution.
async fn corrupted_entries_check(conn: &mut redis::aio::MultiplexedConnection) {
    let key = lightning_exchange::desk::redis_store::KEY_RATE_BUCKETS;
    let _: Result<(), _> = conn.del(key).await;
    // Hostile/corrupt fields must not panic the loader or poison entries.
    let _: () = conn
        .hset_multiple(
            key,
            &[
                ("garbage", "junk"),
                ("1:0", "not_a_float:99"),
                ("2:0", "5.0:100"), // valid
                ("3", "1.0:1"),     // missing op
            ],
        )
        .await
        .expect("seed corrupt");

    let loaded = load_rate_buckets(conn).await.expect("load");
    assert_eq!(loaded.len(), 1, "only the valid entry survives");
    assert_eq!(loaded[0].0, 2);

    let _: Result<(), _> = conn.del(key).await;
}
