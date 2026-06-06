//! Consumer-checkpoint (exactly-once) tests for the persist stream.
//!
//! pg-writer records "applied up to (publisher_id, seq)" in the SAME
//! transaction as the data flush. These tests drive PgWriteBatch directly
//! against the local PG: duplicates must be dropped, checkpoints must
//! survive a simulated restart, and clock-seeded publisher restarts must
//! not be miscounted as frame loss.

use lightning_exchange::db;
use lightning_exchange::desk::pg_store::{PgWriteBatch, load_checkpoints};
use lightning_exchange::transport::persist_event::{PersistFrame, TradeInsertPayload, pack_str};
use sqlx::PgPool;

fn pg_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://user:password@localhost:5432/mydb".to_string())
}

async fn try_pg() -> Option<PgPool> {
    let pg = db::create_pool_sized(&pg_url(), 2).await.ok()?;
    db::run_migrations(&pg).await.ok()?;
    Some(pg)
}

fn trade_frame(publisher_id: u16, seq: u64, buy_order_id: i64) -> PersistFrame {
    let mut f = PersistFrame::trade_insert(TradeInsertPayload {
        buy_order_id,
        sell_order_id: buy_order_id + 1,
        symbol: pack_str("BTC_USDT"),
        price: 100.0,
        qty: 0.001,
        ts_ms: 1_700_000_000_000,
    });
    f.publisher_id = publisher_id;
    f.seq = seq;
    f
}

async fn cleanup(pg: &PgPool, publisher_id: u16, order_ids: &[i64]) {
    let _ = sqlx::query("DELETE FROM persist_checkpoints WHERE publisher_id = $1")
        .bind(publisher_id as i32)
        .execute(pg)
        .await;
    let _ = sqlx::query("DELETE FROM trades WHERE buy_order_id = ANY($1::bigint[])")
        .bind(order_ids)
        .execute(pg)
        .await;
}

async fn trade_count(pg: &PgPool, order_ids: &[i64]) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM trades WHERE buy_order_id = ANY($1::bigint[])")
        .bind(order_ids)
        .fetch_one(pg)
        .await
        .expect("count")
}

#[tokio::test]
async fn checkpoint_commits_with_data_and_dedups_replay() {
    let Some(pg) = try_pg().await else {
        eprintln!("skip: no PG");
        return;
    };
    // Distinct publisher id per test to avoid cross-test interference.
    let pub_id: u16 = 60_001;
    let oids: Vec<i64> = (910_000_001..=910_000_004).collect();
    cleanup(&pg, pub_id, &oids).await;

    let mut batch = PgWriteBatch::new();
    assert!(batch.push(&trade_frame(pub_id, 100, oids[0])));
    assert!(batch.push(&trade_frame(pub_id, 101, oids[1])));
    // Same-batch duplicate must be rejected at admission.
    assert!(!batch.push(&trade_frame(pub_id, 101, oids[2])));
    batch.flush(&pg).await.expect("flush");

    // Checkpoint row committed together with the trades.
    let floors = load_checkpoints(&pg).await.expect("load");
    assert_eq!(floors.get(&pub_id), Some(&101));
    assert_eq!(trade_count(&pg, &oids).await, 2);

    // Aeron replay within the same process: seqs ≤ floor are dropped.
    assert!(!batch.push(&trade_frame(pub_id, 100, oids[2])));
    assert!(!batch.push(&trade_frame(pub_id, 101, oids[2])));
    assert_eq!(batch.duplicate_seq_frames, 3);
    // New seq still flows.
    assert!(batch.push(&trade_frame(pub_id, 102, oids[2])));
    batch.flush(&pg).await.expect("flush 2");
    assert_eq!(trade_count(&pg, &oids).await, 3);

    cleanup(&pg, pub_id, &oids).await;
}

#[tokio::test]
async fn restarted_consumer_resumes_from_committed_floor() {
    let Some(pg) = try_pg().await else {
        eprintln!("skip: no PG");
        return;
    };
    let pub_id: u16 = 60_002;
    let oids: Vec<i64> = (920_000_001..=920_000_003).collect();
    cleanup(&pg, pub_id, &oids).await;

    // First consumer lifetime.
    let mut batch = PgWriteBatch::new();
    assert!(batch.push(&trade_frame(pub_id, 7, oids[0])));
    batch.flush(&pg).await.expect("flush");
    drop(batch);

    // Restart: a fresh batch seeded from PG must drop the replayed frame
    // and accept the next one.
    let mut batch2 = PgWriteBatch::new();
    batch2.set_applied_floors(load_checkpoints(&pg).await.expect("load"));
    assert!(!batch2.push(&trade_frame(pub_id, 7, oids[1])), "replayed dup");
    assert!(batch2.push(&trade_frame(pub_id, 8, oids[1])));
    batch2.flush(&pg).await.expect("flush 2");

    assert_eq!(trade_count(&pg, &oids).await, 2);
    let floors = load_checkpoints(&pg).await.expect("load 2");
    assert_eq!(floors.get(&pub_id), Some(&8));

    cleanup(&pg, pub_id, &oids).await;
}

#[tokio::test]
async fn gap_and_publisher_restart_are_distinguished() {
    let Some(pg) = try_pg().await else {
        eprintln!("skip: no PG");
        return;
    };
    let pub_id: u16 = 60_003;
    let oids: Vec<i64> = (930_000_001..=930_000_004).collect();
    cleanup(&pg, pub_id, &oids).await;

    let mut batch = PgWriteBatch::new();
    assert!(batch.push(&trade_frame(pub_id, 10, oids[0])));
    // Small hole (lost frames upstream) → gap accounting.
    assert!(batch.push(&trade_frame(pub_id, 14, oids[1])));
    assert_eq!(batch.seq_gap_frames, 3);
    assert_eq!(batch.publisher_restarts, 0);

    // Clock-seeded restart: jump of ~hours-of-nanoseconds → restart, not loss.
    let restart_seq = 14 + (1u64 << 50);
    assert!(batch.push(&trade_frame(pub_id, restart_seq, oids[2])));
    assert_eq!(batch.seq_gap_frames, 3, "restart must not inflate gap count");
    assert_eq!(batch.publisher_restarts, 1);

    // Unsequenced frames (seq=0, legacy/test producers) always pass.
    assert!(batch.push(&trade_frame(pub_id, 0, oids[3])));
    batch.flush(&pg).await.expect("flush");
    assert_eq!(trade_count(&pg, &oids).await, 4);

    cleanup(&pg, pub_id, &oids).await;
}
