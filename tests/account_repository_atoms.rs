use lightning_exchange::account_repository::AccountRepository;
use lightning_exchange::db;
use lightning_exchange::money::AmountAtoms;
use sqlx::{PgPool, Row};
use uuid::Uuid;

async fn try_pg() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://user:password@localhost:5432/mydb".to_string());
    let pg = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .ok()?;
    db::run_migrations(&pg).await.ok()?;
    Some(pg)
}

async fn make_user(pg: &PgPool) -> Option<i64> {
    let email = format!("acct_atoms_{}@lightning.test", Uuid::new_v4());
    let row = sqlx::query("INSERT INTO users (email, password_hash) VALUES ($1, $2) RETURNING id")
        .bind(email)
        .bind("$argon2id$v=19$test$abcdefghijklmnopqrstuvwxyz0123456789")
        .fetch_one(pg)
        .await
        .ok()?;
    row.try_get("id").ok()
}

async fn cleanup(pg: &PgPool, user_ids: &[i64]) {
    let _ = sqlx::query("DELETE FROM accounts WHERE user_id = ANY($1::bigint[])")
        .bind(user_ids)
        .execute(pg)
        .await;
    let _ = sqlx::query("DELETE FROM users WHERE id = ANY($1::bigint[])")
        .bind(user_ids)
        .execute(pg)
        .await;
}

async fn seed_account(pg: &PgPool, user_id: i64, asset: &str, balance: &str, frozen: &str) {
    let balance_atoms = AmountAtoms::from_decimal_str(balance).unwrap().atoms();
    let frozen_atoms = AmountAtoms::from_decimal_str(frozen).unwrap().atoms();
    let balance_f = balance.parse::<f64>().unwrap();
    let frozen_f = frozen.parse::<f64>().unwrap();
    sqlx::query(
        "INSERT INTO accounts (user_id, asset, balance, frozen, balance_atoms, frozen_atoms)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (user_id, asset) DO UPDATE SET
            balance = EXCLUDED.balance,
            frozen = EXCLUDED.frozen,
            balance_atoms = EXCLUDED.balance_atoms,
            frozen_atoms = EXCLUDED.frozen_atoms",
    )
    .bind(user_id)
    .bind(asset)
    .bind(balance_f)
    .bind(frozen_f)
    .bind(balance_atoms)
    .bind(frozen_atoms)
    .execute(pg)
    .await
    .expect("seed account");
}

async fn account_amounts(pg: &PgPool, user_id: i64, asset: &str) -> (f64, f64, i64, i64) {
    sqlx::query_as(
        "SELECT balance, frozen, balance_atoms, frozen_atoms FROM accounts WHERE user_id=$1 AND asset=$2",
    )
    .bind(user_id)
    .bind(asset)
    .fetch_one(pg)
    .await
    .expect("select account amounts")
}

#[tokio::test]
async fn repository_freeze_release_and_settle_keep_atoms_in_sync() {
    let Some(pg) = try_pg().await else {
        eprintln!("skip: no PG");
        return;
    };
    let Some(buyer) = make_user(&pg).await else {
        eprintln!("skip: cannot make buyer");
        return;
    };
    let Some(seller) = make_user(&pg).await else {
        eprintln!("skip: cannot make seller");
        cleanup(&pg, &[buyer]).await;
        return;
    };

    seed_account(&pg, buyer, "USDT", "1000", "0").await;
    seed_account(&pg, seller, "BTC", "2", "0").await;

    let repo = AccountRepository::new(&pg);
    repo.freeze_for_buy(buyer, "USDT", 300.0)
        .await
        .expect("freeze buyer quote");
    repo.freeze_for_sell(seller, "BTC", 0.5)
        .await
        .expect("freeze seller base");

    let buyer_quote = account_amounts(&pg, buyer, "USDT").await;
    assert_eq!(buyer_quote.2, 100_000_000_000);
    assert_eq!(buyer_quote.3, 30_000_000_000);
    let seller_base = account_amounts(&pg, seller, "BTC").await;
    assert_eq!(seller_base.2, 200_000_000);
    assert_eq!(seller_base.3, 50_000_000);

    repo.release_frozen(buyer, "USDT", 100.0)
        .await
        .expect("release buyer quote");
    let buyer_quote = account_amounts(&pg, buyer, "USDT").await;
    assert_eq!(buyer_quote.3, 20_000_000_000);

    repo.freeze_for_buy(buyer, "USDT", 50.0)
        .await
        .expect("refreeze buyer quote");
    repo.settle_trade(buyer, seller, "BTC", "USDT", 500.0, 0.5, 0.0, 0.0)
        .await
        .expect("settle trade");

    let buyer_quote = account_amounts(&pg, buyer, "USDT").await;
    assert!((buyer_quote.0 - 750.0).abs() < 1e-9);
    assert_eq!(buyer_quote.2, 75_000_000_000);
    assert_eq!(buyer_quote.3, 0);

    let buyer_base = account_amounts(&pg, buyer, "BTC").await;
    assert!((buyer_base.0 - 0.5).abs() < 1e-9);
    assert_eq!(buyer_base.2, 50_000_000);

    let seller_base = account_amounts(&pg, seller, "BTC").await;
    assert!((seller_base.0 - 1.5).abs() < 1e-9);
    assert_eq!(seller_base.2, 150_000_000);
    assert_eq!(seller_base.3, 0);

    let seller_quote = account_amounts(&pg, seller, "USDT").await;
    assert!((seller_quote.0 - 250.0).abs() < 1e-9);
    assert_eq!(seller_quote.2, 25_000_000_000);

    cleanup(&pg, &[buyer, seller]).await;
}
