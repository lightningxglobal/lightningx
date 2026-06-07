//! S1.3 integration — swap-margin frames flow through PgWriteBatch into
//! PG exactly once. Covers: upsert→row, close→DELETE, keep-last dedup
//! inside one batch, account upsert (incl. negative equity), fund update
//! (incl. negative), and checkpoint-based replay rejection.

use lightning_exchange::db;
use lightning_exchange::desk::pg_store::{PgWriteBatch, load_checkpoints};
use lightning_exchange::transport::persist_event::{
    InsuranceFundSetPayload, PersistFrame, PositionDeletePayload, PositionUpsertPayload,
    RiskAccountSetPayload, pack_str,
};
use sqlx::PgPool;
use uuid::Uuid;

const PUBLISHER: u16 = 63_001;

async fn try_pg() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://user:password@localhost:5432/mydb".to_string());
    let pg = db::create_pool_sized(&url, 2).await.ok()?;
    db::run_migrations(&pg).await.ok()?;
    Some(pg)
}

async fn make_user(pg: &PgPool) -> i64 {
    sqlx::query_scalar("INSERT INTO users (email, password_hash) VALUES ($1, $2) RETURNING id")
        .bind(format!("pos_flush_{}@lightning.test", Uuid::new_v4()))
        .bind("$argon2id$v=19$test$abcdefghijklmnopqrstuvwxyz0123456789")
        .fetch_one(pg)
        .await
        .expect("make user")
}

async fn cleanup(pg: &PgPool, user: i64) {
    for sql in [
        "DELETE FROM positions WHERE user_id = $1",
        "DELETE FROM risk_accounts WHERE user_id = $1",
        "DELETE FROM users WHERE id = $1",
    ] {
        let _ = sqlx::query(sql).bind(user).execute(pg).await;
    }
    let _ = sqlx::query("DELETE FROM persist_checkpoints WHERE publisher_id = $1")
        .bind(PUBLISHER as i32)
        .execute(pg)
        .await;
}

fn seq(mut f: PersistFrame, n: u64) -> PersistFrame {
    f.publisher_id = PUBLISHER;
    f.seq = n;
    f
}

fn pos_frame(user: i64, qty: i64, entry: i64, margin_atoms: i64) -> PersistFrame {
    PersistFrame::position_upsert(PositionUpsertPayload {
        user_id: user,
        symbol: pack_str("BTC_USDT"),
        side: 0, // long
        leverage: 10,
        _pad: [0; 6],
        qty_lots: qty,
        entry_price_ticks: entry,
        used_margin_atoms: margin_atoms,
        cost_atoms: margin_atoms * 10, // 10x leverage notional
    })
}

#[tokio::test]
async fn margin_frames_flush_exactly_once() {
    let Some(pg) = try_pg().await else {
        eprintln!("skip: no PG");
        return;
    };
    // Clear any stale checkpoint for this publisher from a previous run,
    // then create the test user.
    let _ = sqlx::query("DELETE FROM persist_checkpoints WHERE publisher_id = $1")
        .bind(PUBLISHER as i32)
        .execute(&pg)
        .await;
    let user = make_user(&pg).await;

    let mut batch = PgWriteBatch::new();

    // ── Batch 1: two upserts for the SAME key (keep-last must win),
    //    an account row with NEGATIVE equity, and a fund update. ────────
    assert!(batch.push(&seq(pos_frame(user, 5, 49_000, 1_000_000), 1)));
    assert!(batch.push(&seq(pos_frame(user, 12, 50_500, 2_500_000), 2))); // survivor
    assert!(batch.push(&seq(
        PersistFrame::risk_account_set(RiskAccountSetPayload {
            user_id: user,
            equity_atoms: -42_000_000, // transient bankruptcy is representable
            used_margin_atoms: 2_500_000,
            order_margin_atoms: 0,
            status: 5, // bankruptcy
            _pad: [0; 7],
        }),
        3
    )));
    assert!(batch.push(&seq(
        PersistFrame::insurance_fund_set(InsuranceFundSetPayload {
            balance_atoms: -777, // socialised-loss debt (migration 023)
        }),
        4
    )));
    batch.flush(&pg).await.expect("flush 1");

    let (qty, entry, margin, side): (i64, i64, i64, String) = sqlx::query_as(
        "SELECT qty_lots, entry_price_ticks, used_margin_atoms, side
           FROM positions WHERE user_id = $1 AND symbol = 'BTC_USDT'",
    )
    .bind(user)
    .fetch_one(&pg)
    .await
    .expect("position row");
    assert_eq!(
        (qty, entry, margin, side.as_str()),
        (12, 50_500, 2_500_000, "long"),
        "the LAST upsert in the batch must be the stored state"
    );
    let (equity, status): (i64, String) = sqlx::query_as(
        "SELECT equity_atoms, status FROM risk_accounts WHERE user_id = $1",
    )
    .bind(user)
    .fetch_one(&pg)
    .await
    .expect("risk account row");
    assert_eq!((equity, status.as_str()), (-42_000_000, "bankruptcy"));
    let fund: i64 = sqlx::query_scalar("SELECT balance_atoms FROM insurance_fund WHERE id = 1")
        .fetch_one(&pg)
        .await
        .expect("fund");
    assert_eq!(fund, -777);

    // ── Replay of batch 1 (same seqs) must be FULLY deduplicated. ──────
    let mut replay = PgWriteBatch::new();
    replay.set_applied_floors(load_checkpoints(&pg).await.expect("floors"));
    assert!(!replay.push(&seq(pos_frame(user, 99, 99_999, 9), 1)), "dup rejected");
    assert!(!replay.push(&seq(pos_frame(user, 99, 99_999, 9), 2)), "dup rejected");
    let (qty_after,): (i64,) =
        sqlx::query_as("SELECT qty_lots FROM positions WHERE user_id = $1")
            .bind(user)
            .fetch_one(&pg)
            .await
            .expect("row still intact");
    assert_eq!(qty_after, 12, "replayed frames must not change state");

    // ── Batch 2: close the position → row DELETEd; fund restored. ──────
    let mut batch2 = PgWriteBatch::new();
    batch2.set_applied_floors(load_checkpoints(&pg).await.expect("floors"));
    assert!(batch2.push(&seq(
        PersistFrame::position_delete(PositionDeletePayload {
            user_id: user,
            symbol: pack_str("BTC_USDT"),
        }),
        5
    )));
    assert!(batch2.push(&seq(
        PersistFrame::insurance_fund_set(InsuranceFundSetPayload { balance_atoms: 0 }),
        6
    )));
    batch2.flush(&pg).await.expect("flush 2");
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM positions WHERE user_id = $1")
        .bind(user)
        .fetch_one(&pg)
        .await
        .expect("count");
    assert_eq!(count, 0, "flat = row deleted");

    // Deleting again (replay of a close) must be harmless.
    let mut batch3 = PgWriteBatch::new();
    batch3.set_applied_floors(load_checkpoints(&pg).await.expect("floors"));
    assert!(batch3.push(&seq(
        PersistFrame::position_delete(PositionDeletePayload {
            user_id: user,
            symbol: pack_str("BTC_USDT"),
        }),
        7
    )));
    batch3.flush(&pg).await.expect("idempotent delete");

    cleanup(&pg, user).await;
}
