//! Withdrawal interface — two-phase, idempotent, conserving.
//!
//!   request → freeze (amount+fee); confirm → debit frozen; or
//!   fail → release frozen. Tests the custody-critical guarantees:
//!     1. request freezes exactly amount+fee (spendable drops, total same);
//!     2. confirm debits once — re-delivery is a no-op (no double-debit);
//!     3. fail releases once — re-delivery is a no-op (no double-release);
//!     4. confirm and fail are mutually exclusive on a row;
//!     5. insufficient balance is rejected with NO freeze;
//!     6. conservation: balance == initial − Σ(confirmed debits), and
//!        frozen returns to 0 after every request resolves.

use lightning_exchange::account_repository::AccountRepository;
use lightning_exchange::db;
use sqlx::PgPool;
use uuid::Uuid;

const USDT: &str = "USDT";
const INITIAL: i64 = 1_000_000_000_000; // 10,000 USDT
const FEE: i64 = 100_000_000; // 1 USDT

async fn try_pg() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://user:password@localhost:5432/mydb".to_string());
    let pg = db::create_pool_sized(&url, 6).await.ok()?;
    db::run_migrations(&pg).await.ok()?;
    Some(pg)
}

async fn make_funded_user(pg: &PgPool) -> i64 {
    let id: i64 =
        sqlx::query_scalar("INSERT INTO users (email, password_hash) VALUES ($1, $2) RETURNING id")
            .bind(format!("wd_{}@lightning.test", Uuid::new_v4()))
            .bind("$argon2id$v=19$test$abcdefghijklmnopqrstuvwxyz0123456789")
            .fetch_one(pg)
            .await
            .expect("user");
    sqlx::query(
        "INSERT INTO accounts (user_id, asset, balance_atoms, frozen_atoms) VALUES ($1, $2, $3, 0)",
    )
    .bind(id)
    .bind(USDT)
    .bind(INITIAL)
    .execute(pg)
    .await
    .expect("fund");
    id
}

async fn balance(pg: &PgPool, user: i64) -> (i64, i64) {
    sqlx::query_as("SELECT balance_atoms, frozen_atoms FROM accounts WHERE user_id = $1 AND asset = $2")
        .bind(user)
        .bind(USDT)
        .fetch_one(pg)
        .await
        .expect("balance")
}

async fn cleanup(pg: &PgPool, user: i64) {
    for sql in [
        "DELETE FROM withdrawals WHERE user_id = $1",
        "DELETE FROM fund_audit WHERE user_id = $1",
        "DELETE FROM accounts WHERE user_id = $1",
        "DELETE FROM users WHERE id = $1",
    ] {
        let _ = sqlx::query(sql).bind(user).execute(pg).await;
    }
}

#[tokio::test]
async fn confirm_path_debits_exactly_once() {
    let Some(pg) = try_pg().await else {
        eprintln!("skip: no PG");
        return;
    };
    let user = make_funded_user(&pg).await;
    let repo = AccountRepository::new(&pg);
    let amount = 500_000_000_000; // 5,000 USDT

    // request → freeze amount+fee.
    let (id, bal) = repo
        .request_withdrawal(user, USDT, "TRON", "Taddr", amount, FEE)
        .await
        .expect("request");
    assert_eq!(bal.frozen_atoms, amount + FEE, "frozen == amount + fee");
    let (b, f) = balance(&pg, user).await;
    assert_eq!(b, INITIAL, "balance unchanged at request (only frozen)");
    assert_eq!(f, amount + FEE);

    // approve → broadcast (no balance change).
    assert!(repo.set_withdrawal_status(id, "pending", "approved", None).await.unwrap());
    assert!(repo.set_withdrawal_status(id, "approved", "broadcast", Some("0xsent")).await.unwrap());
    let (b, f) = balance(&pg, user).await;
    assert_eq!((b, f), (INITIAL, amount + FEE), "approve/broadcast don't move funds");

    // confirm → debit.
    assert!(repo.confirm_withdrawal(id, "0xsent").await.unwrap(), "first confirm debits");
    let (b, f) = balance(&pg, user).await;
    assert_eq!(b, INITIAL - amount - FEE, "balance debited by amount+fee");
    assert_eq!(f, 0, "frozen released");

    // re-confirm → idempotent no-op.
    assert!(!repo.confirm_withdrawal(id, "0xsent").await.unwrap(), "re-confirm is a no-op");
    let (b2, f2) = balance(&pg, user).await;
    assert_eq!((b2, f2), (b, f), "no double debit");

    // fail after confirm → rejected (terminal).
    assert!(repo.fail_withdrawal(id, "late").await.is_err(), "cannot fail a confirmed withdrawal");

    cleanup(&pg, user).await;
}

#[tokio::test]
async fn fail_path_releases_exactly_once() {
    let Some(pg) = try_pg().await else {
        eprintln!("skip: no PG");
        return;
    };
    let user = make_funded_user(&pg).await;
    let repo = AccountRepository::new(&pg);
    let amount = 300_000_000_000; // 3,000 USDT

    let (id, _) = repo
        .request_withdrawal(user, USDT, "TRON", "Taddr", amount, FEE)
        .await
        .expect("request");
    repo.set_withdrawal_status(id, "pending", "approved", None).await.unwrap();

    // fail → release the freeze, funds spendable again.
    assert!(repo.fail_withdrawal(id, "chain rejected").await.unwrap(), "first fail releases");
    let (b, f) = balance(&pg, user).await;
    assert_eq!(b, INITIAL, "balance never debited");
    assert_eq!(f, 0, "freeze released in full");

    // re-fail → idempotent.
    assert!(!repo.fail_withdrawal(id, "again").await.unwrap(), "re-fail is a no-op");
    // confirm after fail → rejected.
    assert!(repo.confirm_withdrawal(id, "0x").await.is_err(), "cannot confirm a failed withdrawal");
    let (b2, f2) = balance(&pg, user).await;
    assert_eq!((b2, f2), (INITIAL, 0), "no spurious movement");

    cleanup(&pg, user).await;
}

#[tokio::test]
async fn insufficient_balance_rejected_without_freeze() {
    let Some(pg) = try_pg().await else {
        eprintln!("skip: no PG");
        return;
    };
    let user = make_funded_user(&pg).await;
    let repo = AccountRepository::new(&pg);
    // Request more than the balance (incl. fee) → rejected, no freeze.
    assert!(
        repo.request_withdrawal(user, USDT, "TRON", "Taddr", INITIAL, FEE)
            .await
            .is_err(),
        "over-balance request rejected"
    );
    let (b, f) = balance(&pg, user).await;
    assert_eq!((b, f), (INITIAL, 0), "no freeze on a rejected request");
    // No withdrawal row created.
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM withdrawals WHERE user_id = $1")
        .bind(user)
        .fetch_one(&pg)
        .await
        .unwrap();
    assert_eq!(n, 0);
    cleanup(&pg, user).await;
}

#[tokio::test]
async fn conservation_across_mixed_outcomes() {
    let Some(pg) = try_pg().await else {
        eprintln!("skip: no PG");
        return;
    };
    let user = make_funded_user(&pg).await;
    let repo = AccountRepository::new(&pg);

    // Two confirmed, one failed.
    let mut debited = 0i64;
    for (amt, confirm) in [(100_000_000_000, true), (200_000_000_000, false), (150_000_000_000, true)] {
        let (id, _) = repo
            .request_withdrawal(user, USDT, "TRON", "Taddr", amt, FEE)
            .await
            .expect("request");
        repo.set_withdrawal_status(id, "pending", "approved", None).await.unwrap();
        if confirm {
            repo.confirm_withdrawal(id, "0x").await.unwrap();
            debited += amt + FEE;
        } else {
            repo.fail_withdrawal(id, "x").await.unwrap();
        }
    }
    let (b, f) = balance(&pg, user).await;
    assert_eq!(b, INITIAL - debited, "balance == initial − Σ confirmed debits");
    assert_eq!(f, 0, "no funds left frozen after all resolve");

    // Audit conservation: net of (freeze − release − debit) on frozen
    // is consistent — every wd_freeze has a matching wd_release or
    // wd_debit of the same total.
    let (frz, rel, deb): (i64, i64, i64) = sqlx::query_as(
        "SELECT
            COALESCE(SUM(amount_atoms) FILTER (WHERE kind='wd_freeze'),0)::bigint,
            COALESCE(SUM(amount_atoms) FILTER (WHERE kind='wd_release'),0)::bigint,
            COALESCE(SUM(amount_atoms) FILTER (WHERE kind='wd_debit'),0)::bigint
           FROM fund_audit WHERE user_id = $1",
    )
    .bind(user)
    .fetch_one(&pg)
    .await
    .expect("audit sums");
    assert_eq!(frz, rel + deb, "every freeze is matched by a release or a debit");

    cleanup(&pg, user).await;
}
