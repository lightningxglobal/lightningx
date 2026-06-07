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
    sqlx::query(
        "INSERT INTO accounts (user_id, asset, balance_atoms, frozen_atoms)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (user_id, asset) DO UPDATE SET
            balance_atoms = EXCLUDED.balance_atoms,
            frozen_atoms = EXCLUDED.frozen_atoms",
    )
    .bind(user_id)
    .bind(asset)
    .bind(balance_atoms)
    .bind(frozen_atoms)
    .execute(pg)
    .await
    .expect("seed account");
}

/// (balance_f64_projection, frozen_f64_projection, balance_atoms, frozen_atoms)
/// — the float values are computed FROM atoms (the only stored truth).
async fn account_amounts(pg: &PgPool, user_id: i64, asset: &str) -> (f64, f64, i64, i64) {
    sqlx::query_as(
        "SELECT balance_atoms::float8 / 100000000.0,
                frozen_atoms::float8 / 100000000.0,
                balance_atoms, frozen_atoms
           FROM accounts WHERE user_id=$1 AND asset=$2",
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

#[tokio::test]
async fn settle_trade_atoms_is_exact_and_conserves_quote() {
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

    // Price/qty chosen so the product is NOT exactly representable in f64:
    // 75893.61 × 0.00123456 = 93.6952151616 → 9_369_521_516 atoms (half-up).
    let price = AmountAtoms::from_decimal_str("75893.61").unwrap();
    let qty = AmountAtoms::from_decimal_str("0.00123456").unwrap();
    let buy_fee = AmountAtoms::from_decimal_str("0.09369522").unwrap();
    let sell_fee = AmountAtoms::from_decimal_str("0.04684761").unwrap();
    let cost_atoms: i64 = 9_369_521_516;

    seed_account(&pg, buyer, "USDT", "1000", "0").await;
    seed_account(&pg, seller, "BTC", "1", "0").await;

    let repo = AccountRepository::new(&pg);
    repo.freeze_atoms(
        buyer,
        "USDT",
        AmountAtoms::from_atoms(cost_atoms + buy_fee.atoms()),
    )
    .await
    .expect("freeze buyer quote");
    repo.freeze_atoms(seller, "BTC", qty)
        .await
        .expect("freeze seller base");

    repo.settle_trade_atoms(buyer, seller, "BTC", "USDT", price, qty, buy_fee, sell_fee, None)
        .await
        .expect("settle trade atoms");

    let buyer_quote = account_amounts(&pg, buyer, "USDT").await;
    let buyer_base = account_amounts(&pg, buyer, "BTC").await;
    let seller_base = account_amounts(&pg, seller, "BTC").await;
    let seller_quote = account_amounts(&pg, seller, "USDT").await;

    // Exact atom-level expectations — no epsilon anywhere.
    let buyer_debit = cost_atoms + buy_fee.atoms();
    assert_eq!(buyer_quote.2, 100_000_000_000 - buyer_debit);
    assert_eq!(buyer_quote.3, 0);
    assert_eq!(buyer_base.2, qty.atoms());
    assert_eq!(seller_base.2, 100_000_000 - qty.atoms());
    assert_eq!(seller_base.3, 0);
    assert_eq!(seller_quote.2, cost_atoms - sell_fee.atoms());

    // Quote conservation: buyer's loss = seller's gain + exchange fee revenue.
    let buyer_loss = 100_000_000_000 - buyer_quote.2;
    let seller_gain = seller_quote.2;
    let fee_revenue = buy_fee.atoms() + sell_fee.atoms();
    assert_eq!(buyer_loss, seller_gain + fee_revenue);

    // Base conservation: seller's loss = buyer's gain (no base-side fee).
    assert_eq!(100_000_000 - seller_base.2, buyer_base.2);

    cleanup(&pg, &[buyer, seller]).await;
}

#[tokio::test]
async fn settle_with_trade_record_is_atomic_and_idempotent() {
    let Some(pg) = try_pg().await else {
        eprintln!("skip: no PG");
        return;
    };
    let (Some(buyer), Some(seller)) = (make_user(&pg).await, make_user(&pg).await) else {
        eprintln!("skip: cannot make users");
        return;
    };
    seed_account(&pg, buyer, "USDT", "1000", "0").await;
    seed_account(&pg, seller, "BTC", "1", "0").await;
    let repo = AccountRepository::new(&pg);

    let price = AmountAtoms::from_decimal_str("100").unwrap();
    let qty = AmountAtoms::from_decimal_str("0.5").unwrap();
    repo.freeze_atoms(buyer, "USDT", AmountAtoms::from_decimal_str("50").unwrap())
        .await
        .expect("freeze buyer");
    repo.freeze_atoms(seller, "BTC", qty).await.expect("freeze seller");

    let trade = lightning_exchange::account_repository::SettleTradeRecord {
        symbol: "BTC_USDT".into(),
        buy_order_id: 955_000_001,
        sell_order_id: 955_000_002,
    };
    repo.settle_trade_atoms(
        buyer, seller, "BTC", "USDT", price, qty,
        AmountAtoms::ZERO, AmountAtoms::ZERO, Some(&trade),
    )
    .await
    .expect("settle with trade");

    // Trade row landed with exact atoms, in the same transaction.
    let (p_atoms, q_atoms): (i64, i64) = sqlx::query_as(
        "SELECT price_atoms, quantity_atoms FROM trades
          WHERE buy_order_id = $1 AND sell_order_id = $2",
    )
    .bind(trade.buy_order_id)
    .bind(trade.sell_order_id)
    .fetch_one(&pg)
    .await
    .expect("trade row");
    assert_eq!(p_atoms, price.atoms());
    assert_eq!(q_atoms, qty.atoms());

    // Fund audit: freeze ×2 + settle legs ×4, append-only.
    let audit_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM fund_audit WHERE user_id = ANY($1::bigint[])",
    )
    .bind(vec![buyer, seller])
    .fetch_one(&pg)
    .await
    .expect("audit count");
    assert_eq!(audit_rows, 6, "2 freezes + 4 settle legs audited");
    let mutate = sqlx::query("DELETE FROM fund_audit WHERE user_id = $1")
        .bind(buyer)
        .execute(&pg)
        .await;
    assert!(mutate.is_err(), "fund_audit must be append-only");

    // Idempotency: replaying the SAME trade pair is a no-op for the trades
    // table (unique pair), regardless of which path delivers it.
    let dup: i64 = sqlx::query_scalar(
        "INSERT INTO trades (symbol, buy_order_id, sell_order_id,
                             price_atoms, quantity_atoms)
         VALUES ('BTC_USDT', $1, $2, $3, $4)
         ON CONFLICT (buy_order_id, sell_order_id) DO NOTHING
         RETURNING 1",
    )
    .bind(trade.buy_order_id)
    .bind(trade.sell_order_id)
    .bind(price.atoms())
    .bind(qty.atoms())
    .fetch_optional(&pg)
    .await
    .expect("dup insert")
    .unwrap_or(0);
    assert_eq!(dup, 0, "duplicate trade pair must not insert");
    let n: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM trades WHERE buy_order_id = $1")
            .bind(trade.buy_order_id)
            .fetch_one(&pg)
            .await
            .expect("count");
    assert_eq!(n, 1);

    // cleanup (trades row removable: no trigger there)
    let _ = sqlx::query("DELETE FROM trades WHERE buy_order_id = $1")
        .bind(trade.buy_order_id)
        .execute(&pg)
        .await;
    cleanup(&pg, &[buyer, seller]).await;
}

#[tokio::test]
async fn freeze_atoms_rejects_negative_and_over_available() {
    let Some(pg) = try_pg().await else {
        eprintln!("skip: no PG");
        return;
    };
    let Some(user) = make_user(&pg).await else {
        eprintln!("skip: cannot make user");
        return;
    };

    seed_account(&pg, user, "USDT", "10", "0").await;
    let repo = AccountRepository::new(&pg);

    assert!(
        repo.freeze_atoms(user, "USDT", AmountAtoms::from_atoms(-1))
            .await
            .is_err(),
        "negative freeze must be rejected"
    );
    assert!(
        repo.freeze_atoms(user, "USDT", AmountAtoms::from_decimal_str("10.00000001").unwrap())
            .await
            .is_err(),
        "freeze over available must be rejected"
    );
    // Exactly available must succeed.
    repo.freeze_atoms(user, "USDT", AmountAtoms::from_decimal_str("10").unwrap())
        .await
        .expect("freeze exactly available");

    cleanup(&pg, &[user]).await;
}
