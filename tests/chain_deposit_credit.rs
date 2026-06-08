//! Deposit credit interface — the narrow path the on-chain watcher calls.
//! Tests the exactly-once guarantee that matters for custody:
//!   1. a confirmed transfer credits once and writes a fund_audit row;
//!   2. re-delivering the SAME (chain, tx_hash, log_index) is a no-op
//!      (idempotent — a watcher restart / re-org rescan can't double-credit);
//!   3. concurrent duplicate deliveries credit exactly once (the UNIQUE
//!      constraint + single transaction);
//!   4. distinct transfers accumulate;
//!   5. credited balance == Σ(fund_audit deposit rows) — conservation.

use lightning_exchange::account_repository::AccountRepository;
use lightning_exchange::db;
use sqlx::PgPool;
use std::sync::Arc;
use uuid::Uuid;

async fn try_pg() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://user:password@localhost:5432/mydb".to_string());
    let pg = db::create_pool_sized(&url, 6).await.ok()?;
    db::run_migrations(&pg).await.ok()?;
    Some(pg)
}

async fn make_user(pg: &PgPool) -> i64 {
    sqlx::query_scalar("INSERT INTO users (email, password_hash) VALUES ($1, $2) RETURNING id")
        .bind(format!("deposit_{}@lightning.test", Uuid::new_v4()))
        .bind("$argon2id$v=19$test$abcdefghijklmnopqrstuvwxyz0123456789")
        .fetch_one(pg)
        .await
        .expect("make user")
}

async fn cleanup(pg: &PgPool, user: i64) {
    for sql in [
        "DELETE FROM chain_deposits WHERE user_id = $1",
        "DELETE FROM fund_audit WHERE user_id = $1",
        "DELETE FROM accounts WHERE user_id = $1",
        "DELETE FROM users WHERE id = $1",
    ] {
        let _ = sqlx::query(sql).bind(user).execute(pg).await;
    }
}

#[tokio::test]
async fn deposit_credit_is_exactly_once_and_conserves() {
    let Some(pg) = try_pg().await else {
        eprintln!("skip: no PG");
        return;
    };
    let user = make_user(&pg).await;
    let repo = AccountRepository::new(&pg);

    // ── First credit: 1,000 USDT (1e11 atoms) ──────────────────────────
    let (credited, bal) = repo
        .credit_chain_deposit(
            "TRON", "0xabc", 0, user, "USDT", 100_000_000_000,
            Some("Tfrom"), Some("Tto"),
        )
        .await
        .expect("credit");
    assert!(credited, "first delivery credits");
    assert_eq!(bal, 100_000_000_000);

    // ── Replay the SAME transfer: idempotent no-op ─────────────────────
    let (credited2, bal2) = repo
        .credit_chain_deposit(
            "TRON", "0xabc", 0, user, "USDT", 100_000_000_000, None, None,
        )
        .await
        .expect("replay");
    assert!(!credited2, "replay must NOT credit again");
    assert_eq!(bal2, 100_000_000_000, "balance unchanged on replay");

    // ── A different output of the same tx (log_index 1) is distinct ────
    let (credited3, bal3) = repo
        .credit_chain_deposit("TRON", "0xabc", 1, user, "USDT", 50_000_000_000, None, None)
        .await
        .expect("second output");
    assert!(credited3);
    assert_eq!(bal3, 150_000_000_000, "distinct output accumulates");

    // ── Concurrent duplicate deliveries → exactly one credit ───────────
    let repo_pool = pg.clone();
    let mut handles = Vec::new();
    for _ in 0..8 {
        let pool = repo_pool.clone();
        handles.push(tokio::spawn(async move {
            let r = AccountRepository::new(&pool);
            r.credit_chain_deposit("ETH", "0xrace", 0, user, "USDT", 7_000_000_000, None, None)
                .await
                .map(|(c, _)| c)
                .unwrap_or(false)
        }));
    }
    let mut credited_count = 0;
    for h in handles {
        if h.await.unwrap() {
            credited_count += 1;
        }
    }
    assert_eq!(credited_count, 1, "concurrent duplicates credit EXACTLY once");
    let _ = Arc::new(()); // keep import tidy

    // ── Conservation: balance == Σ(fund_audit deposit rows) ────────────
    let balance: i64 = sqlx::query_scalar(
        "SELECT balance_atoms FROM accounts WHERE user_id = $1 AND asset = 'USDT'",
    )
    .bind(user)
    .fetch_one(&pg)
    .await
    .expect("balance");
    let audited: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(amount_atoms),0)::bigint FROM fund_audit
          WHERE user_id = $1 AND kind = 'deposit'",
    )
    .bind(user)
    .fetch_one(&pg)
    .await
    .expect("audited");
    assert_eq!(
        balance, audited,
        "credited balance must equal the sum of deposit audit rows"
    );
    // 1000 + 500 + 70 USDT = 157,000,000,000 atoms.
    assert_eq!(balance, 157_000_000_000);

    // ── Exactly 3 deposit rows recorded (no phantom credits) ───────────
    let deposit_rows: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM chain_deposits WHERE user_id = $1")
            .bind(user)
            .fetch_one(&pg)
            .await
            .expect("deposit rows");
    assert_eq!(deposit_rows, 3);

    cleanup(&pg, user).await;
}

#[tokio::test]
async fn deposit_rejects_nonpositive_amount() {
    let Some(pg) = try_pg().await else {
        eprintln!("skip: no PG");
        return;
    };
    let user = make_user(&pg).await;
    let repo = AccountRepository::new(&pg);
    assert!(
        repo.credit_chain_deposit("TRON", "0xz", 0, user, "USDT", 0, None, None)
            .await
            .is_err(),
        "zero amount rejected"
    );
    assert!(
        repo.credit_chain_deposit("TRON", "0xz", 0, user, "USDT", -5, None, None)
            .await
            .is_err(),
        "negative amount rejected"
    );
    cleanup(&pg, user).await;
}
