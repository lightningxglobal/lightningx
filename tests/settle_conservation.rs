//! Gate-1 fund-conservation property test (integration, PG).
//!
//! Drives a seeded pseudo-random sequence of freeze → settle_trade_atoms
//! cycles across a pool of users and asserts, in exact atoms:
//!   1. base-asset conservation: Σ(base balances) is constant — base only
//!      moves between users, never appears or disappears;
//!   2. quote-asset conservation: Σ(quote balances before) − Σ(after)
//!      == Σ(fees collected) — the only quote sink is fee revenue;
//!   3. no negative balances, no residual frozen after all settles.
//!
//! Deterministic: a fixed-seed LCG generates prices/quantities/fees, so a
//! failure reproduces exactly.

use lightning_exchange::account_repository::AccountRepository;
use lightning_exchange::db;
use lightning_exchange::money::AmountAtoms;
use sqlx::{PgPool, Row};
use uuid::Uuid;

const USERS: usize = 6;
const TRADES: usize = 200;
const SEED: u64 = 0x5EED_CAFE_F00D_0001;

struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        // Numerical Recipes LCG constants; quality is irrelevant here,
        // determinism is the point.
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0 >> 16
    }
    fn range(&mut self, lo: i64, hi: i64) -> i64 {
        lo + (self.next() % (hi - lo + 1) as u64) as i64
    }
}

async fn try_pg() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://user:password@localhost:5432/mydb".to_string());
    let pg = db::create_pool_sized(&url, 4).await.ok()?;
    db::run_migrations(&pg).await.ok()?;
    Some(pg)
}

async fn make_user(pg: &PgPool) -> Option<i64> {
    let email = format!("conserve_{}@lightning.test", Uuid::new_v4());
    sqlx::query("INSERT INTO users (email, password_hash) VALUES ($1, $2) RETURNING id")
        .bind(email)
        .bind("$argon2id$v=19$test$abcdefghijklmnopqrstuvwxyz0123456789")
        .fetch_one(pg)
        .await
        .ok()?
        .try_get("id")
        .ok()
}

async fn seed_account(pg: &PgPool, user_id: i64, asset: &str, balance_atoms: i64) {
    sqlx::query(
        "INSERT INTO accounts (user_id, asset, balance_atoms, frozen_atoms)
         VALUES ($1, $2, $3, 0)
         ON CONFLICT (user_id, asset) DO UPDATE SET
            balance_atoms = EXCLUDED.balance_atoms, frozen_atoms = EXCLUDED.frozen_atoms",
    )
    .bind(user_id)
    .bind(asset)
    .bind(balance_atoms)
    .execute(pg)
    .await
    .expect("seed account");
}

async fn sum_balances(pg: &PgPool, users: &[i64], asset: &str) -> (i64, i64) {
    sqlx::query_as(
        "SELECT COALESCE(SUM(balance_atoms),0)::bigint, COALESCE(SUM(frozen_atoms),0)::bigint
           FROM accounts WHERE user_id = ANY($1::bigint[]) AND asset = $2",
    )
    .bind(users)
    .bind(asset)
    .fetch_one(pg)
    .await
    .expect("sum balances")
}

async fn cleanup(pg: &PgPool, users: &[i64]) {
    let _ = sqlx::query("DELETE FROM accounts WHERE user_id = ANY($1::bigint[])")
        .bind(users)
        .execute(pg)
        .await;
    let _ = sqlx::query("DELETE FROM users WHERE id = ANY($1::bigint[])")
        .bind(users)
        .execute(pg)
        .await;
}

#[tokio::test]
async fn random_settle_sequence_conserves_funds_exactly() {
    let Some(pg) = try_pg().await else {
        eprintln!("skip: no PG");
        return;
    };

    let mut users = Vec::with_capacity(USERS);
    for _ in 0..USERS {
        let Some(u) = make_user(&pg).await else {
            eprintln!("skip: cannot create users");
            cleanup(&pg, &users).await;
            return;
        };
        users.push(u);
    }

    // Everyone starts with 1,000,000 USDT and 100 BASE (atoms).
    const QUOTE_START: i64 = 1_000_000 * 100_000_000;
    const BASE_START: i64 = 100 * 100_000_000;
    for &u in &users {
        seed_account(&pg, u, "USDT", QUOTE_START).await;
        seed_account(&pg, u, "BASE", BASE_START).await;
    }

    let (quote_before, _) = sum_balances(&pg, &users, "USDT").await;
    let (base_before, _) = sum_balances(&pg, &users, "BASE").await;

    let repo = AccountRepository::new(&pg);
    let mut rng = Lcg(SEED);
    let mut total_fees_atoms: i64 = 0;
    let mut settled = 0usize;

    for _ in 0..TRADES {
        let buyer = users[rng.range(0, USERS as i64 - 1) as usize];
        let seller = users[rng.range(0, USERS as i64 - 1) as usize];
        if buyer == seller {
            continue; // self-trades are compliance's offline concern
        }
        // price in [0.01, 1000.00] atoms-aligned; qty in [0.0001, 0.5] BASE.
        let price = AmountAtoms::from_atoms(rng.range(1_000_000, 100_000_000_000));
        let qty = AmountAtoms::from_atoms(rng.range(10_000, 50_000_000));
        let cost = price.checked_mul_scaled(qty).expect("cost");
        // Fees: 0–20 bps of cost, integer division (floor).
        let buy_fee = AmountAtoms::from_atoms(cost.atoms() * rng.range(0, 20) / 10_000);
        let sell_fee = AmountAtoms::from_atoms(cost.atoms() * rng.range(0, 20) / 10_000);

        // Freeze exactly what settle will debit; skip the trade if a party
        // can't afford it (mirrors order-entry rejection).
        let buyer_needs = cost.checked_add(buy_fee).expect("debit");
        if repo.freeze_atoms(buyer, "USDT", buyer_needs).await.is_err() {
            continue;
        }
        if repo.freeze_atoms(seller, "BASE", qty).await.is_err() {
            // release the buyer freeze we just took
            repo.release_frozen_atoms(buyer, "USDT", buyer_needs)
                .await
                .expect("release");
            continue;
        }

        repo.settle_trade_atoms(buyer, seller, "BASE", "USDT", price, qty, buy_fee, sell_fee, None)
            .await
            .expect("settle");
        total_fees_atoms += buy_fee.atoms() + sell_fee.atoms();
        settled += 1;
    }

    assert!(settled > TRADES / 2, "test should settle most trades, got {settled}");

    let (quote_after, quote_frozen) = sum_balances(&pg, &users, "USDT").await;
    let (base_after, base_frozen) = sum_balances(&pg, &users, "BASE").await;

    // 1. Base conservation: exact.
    assert_eq!(base_before, base_after, "base asset must be conserved");
    // 2. Quote conservation: only sink is fee revenue. Exact.
    assert_eq!(
        quote_before - quote_after,
        total_fees_atoms,
        "quote delta must equal collected fees exactly (settled={settled})"
    );
    // 3. No residual frozen, no negative balances.
    assert_eq!(quote_frozen, 0, "no residual quote freeze");
    assert_eq!(base_frozen, 0, "no residual base freeze");
    let negatives: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM accounts WHERE user_id = ANY($1::bigint[])
           AND (balance_atoms < 0 OR frozen_atoms < 0)",
    )
    .bind(&users)
    .fetch_one(&pg)
    .await
    .expect("negatives");
    assert_eq!(negatives, 0, "no negative balances");

    cleanup(&pg, &users).await;
}
