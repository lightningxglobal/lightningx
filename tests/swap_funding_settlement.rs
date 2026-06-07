//! S3.3/S3.4 — funding settlement through pg-writer's single-transaction
//! frame path:
//!   * every account delta matches funding::compute_settlement exactly;
//!   * Σ(deltas) + fund residue == 0 (the conservation law, no tolerance);
//!   * history row and schedule advance land in the SAME transaction;
//!   * replaying the frame is fully deduplicated (exactly-once);
//!   * a negative rate mirrors the direction (shorts pay longs).

use lightning_exchange::db;
use lightning_exchange::desk::funding::compute_settlement;
use lightning_exchange::desk::pg_store::{PgWriteBatch, load_checkpoints};
use lightning_exchange::desk::risk::PositionSide;
use lightning_exchange::transport::persist_event::{FundingSettledPayload, PersistFrame, pack_str};
use sqlx::PgPool;
use uuid::Uuid;

const PUBLISHER: u16 = 63_003;
const SYM: &str = "FUND_TST";
const SCALE: i64 = 1_000_000;
const MARK: i64 = 5_000_001; // odd mark → per-position truncation differs
const RATE: i64 = 100_000; // +0.01%: longs pay shorts

async fn try_pg() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://user:password@localhost:5432/mydb".to_string());
    let pg = db::create_pool_sized(&url, 2).await.ok()?;
    db::run_migrations(&pg).await.ok()?;
    Some(pg)
}

async fn make_user(pg: &PgPool) -> i64 {
    sqlx::query_scalar("INSERT INTO users (email, password_hash) VALUES ($1, $2) RETURNING id")
        .bind(format!("fund_{}@lightning.test", Uuid::new_v4()))
        .bind("$argon2id$v=19$test$abcdefghijklmnopqrstuvwxyz0123456789")
        .fetch_one(pg)
        .await
        .expect("make user")
}

async fn seed(pg: &PgPool, user: i64, side: &str, qty: i64) {
    sqlx::query(
        "INSERT INTO positions
           (user_id, symbol, side, qty_lots, entry_price_ticks, leverage,
            used_margin_atoms, cost_atoms)
         VALUES ($1, $2, $3, $4, 5000000, 10, 1000, 10000)",
    )
    .bind(user)
    .bind(SYM)
    .bind(side)
    .bind(qty)
    .execute(pg)
    .await
    .expect("seed position");
    sqlx::query(
        "INSERT INTO risk_accounts (user_id, equity_atoms, used_margin_atoms, order_margin_atoms)
         VALUES ($1, 1000000000000, 1000, 0)",
    )
    .bind(user)
    .execute(pg)
    .await
    .expect("seed account");
}

async fn equity(pg: &PgPool, user: i64) -> i64 {
    sqlx::query_scalar("SELECT equity_atoms FROM risk_accounts WHERE user_id = $1")
        .bind(user)
        .fetch_one(pg)
        .await
        .expect("equity")
}

async fn fund(pg: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT balance_atoms FROM insurance_fund WHERE id = 1")
        .fetch_one(pg)
        .await
        .expect("fund")
}

async fn cleanup(pg: &PgPool, users: &[i64]) {
    let _ = sqlx::query("DELETE FROM positions WHERE symbol = $1").bind(SYM).execute(pg).await;
    let _ = sqlx::query("DELETE FROM funding_history WHERE symbol = $1").bind(SYM).execute(pg).await;
    let _ = sqlx::query("DELETE FROM funding_state WHERE symbol = $1").bind(SYM).execute(pg).await;
    let _ = sqlx::query("DELETE FROM persist_checkpoints WHERE publisher_id = $1")
        .bind(PUBLISHER as i32)
        .execute(pg)
        .await;
    let _ = sqlx::query("UPDATE insurance_fund SET balance_atoms = 0 WHERE id = 1")
        .execute(pg)
        .await;
    if !users.is_empty() {
        for sql in [
            "DELETE FROM risk_accounts WHERE user_id = ANY($1::bigint[])",
            "DELETE FROM users WHERE id = ANY($1::bigint[])",
        ] {
            let _ = sqlx::query(sql).bind(users).execute(pg).await;
        }
    }
}

fn frame(seq: u64, rate_e9: i64, settled_at_ms: i64) -> PersistFrame {
    let mut f = PersistFrame::funding_settled(FundingSettledPayload {
        symbol: pack_str(SYM),
        rate_e9,
        mark_price_ticks: MARK,
        notional_scale: SCALE,
        settled_at_ms,
        next_settlement_at_ms: settled_at_ms + 28_800_000,
    });
    f.publisher_id = PUBLISHER;
    f.seq = seq;
    f
}

#[tokio::test]
async fn settlement_is_atomic_exact_and_replay_safe() {
    let Some(pg) = try_pg().await else {
        eprintln!("skip: no PG");
        return;
    };
    cleanup(&pg, &[]).await;
    // Uneven partition of equal OI so truncation residue is non-trivial.
    let (l1, l2, s1, s2) =
        (make_user(&pg).await, make_user(&pg).await, make_user(&pg).await, make_user(&pg).await);
    seed(&pg, l1, "long", 33_333).await;
    seed(&pg, l2, "long", 66_667).await;
    seed(&pg, s1, "short", 99_999).await;
    seed(&pg, s2, "short", 1).await;
    let users = [l1, l2, s1, s2];
    let before: Vec<i64> = {
        let mut v = Vec::new();
        for &u in &users {
            v.push(equity(&pg, u).await);
        }
        v
    };

    // ── Apply via the frame path ────────────────────────────────────────
    let mut batch = PgWriteBatch::new();
    assert!(batch.push(&frame(1, RATE, 1_700_000_000_000)));
    batch.flush(&pg).await.expect("settlement flush");

    // Expected deltas straight from the shared function.
    let expected = compute_settlement(
        vec![
            (l1, PositionSide::Long, 33_333),
            (l2, PositionSide::Long, 66_667),
            (s1, PositionSide::Short, 99_999),
            (s2, PositionSide::Short, 1),
        ]
        .into_iter(),
        MARK,
        SCALE,
        RATE,
    );

    let mut sum_deltas: i64 = 0;
    for (i, &u) in users.iter().enumerate() {
        let got = equity(&pg, u).await - before[i];
        let want = expected
            .deltas
            .iter()
            .find(|d| d.user_id == u)
            .map(|d| d.equity_delta_atoms)
            .unwrap_or(0);
        assert_eq!(got, want, "user {u} delta must match the shared math exactly");
        sum_deltas += got;
    }
    let fund_now = fund(&pg).await;
    assert_eq!(
        sum_deltas + fund_now,
        0,
        "Σ(account deltas) + fund residue must be EXACTLY zero"
    );
    assert_eq!(fund_now, expected.residue_atoms, "residue lands in the fund");
    // Longs paid, shorts received (positive rate).
    assert!(equity(&pg, l1).await < before[0] && equity(&pg, s1).await > before[2]);

    // History + schedule advanced in the same transaction.
    let (hist_count, paid, recv, residue): (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT COUNT(*)::bigint, COALESCE(SUM(long_paid_atoms),0)::bigint,
                COALESCE(SUM(short_received_atoms),0)::bigint,
                COALESCE(SUM(fund_residue_atoms),0)::bigint
           FROM funding_history WHERE symbol = $1",
    )
    .bind(SYM)
    .fetch_one(&pg)
    .await
    .expect("history");
    assert_eq!(hist_count, 1);
    assert_eq!(paid, expected.long_paid_atoms);
    assert_eq!(recv, expected.short_received_atoms);
    assert_eq!(residue, expected.residue_atoms);
    let next_ms: i64 = sqlx::query_scalar(
        "SELECT (EXTRACT(EPOCH FROM next_settlement_at) * 1000)::bigint
           FROM funding_state WHERE symbol = $1",
    )
    .bind(SYM)
    .fetch_one(&pg)
    .await
    .expect("state");
    assert_eq!(next_ms, 1_700_000_000_000 + 28_800_000, "schedule advanced");

    // ── Replay: same seq must be fully deduplicated ─────────────────────
    let mut replay = PgWriteBatch::new();
    replay.set_applied_floors(load_checkpoints(&pg).await.expect("floors"));
    assert!(!replay.push(&frame(1, RATE, 1_700_000_000_000)), "dup rejected");
    let fund_after_replay = fund(&pg).await;
    assert_eq!(fund_after_replay, fund_now, "replay must not settle twice");

    // ── Negative rate mirrors direction ─────────────────────────────────
    let l1_before = equity(&pg, l1).await;
    let s1_before = equity(&pg, s1).await;
    let mut batch2 = PgWriteBatch::new();
    batch2.set_applied_floors(load_checkpoints(&pg).await.expect("floors"));
    assert!(batch2.push(&frame(2, -RATE, 1_700_000_100_000)));
    batch2.flush(&pg).await.expect("negative-rate flush");
    assert!(
        equity(&pg, l1).await > l1_before && equity(&pg, s1).await < s1_before,
        "negative rate: shorts pay longs"
    );

    cleanup(&pg, &users).await;
}
