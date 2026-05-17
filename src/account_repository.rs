/// Persistent account operations backed by PostgreSQL.
/// Mirrors the in-memory AccountManager semantics with DB atomicity.
use crate::models::DbAccount;
use anyhow::{anyhow, Result};
use sqlx::PgPool;

pub struct AccountRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> AccountRepository<'a> {
    pub fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    pub async fn get_account(&self, user_id: i64, asset: &str) -> Result<DbAccount> {
        sqlx::query_as::<_, DbAccount>(
            "SELECT * FROM accounts WHERE user_id = $1 AND asset = $2",
        )
        .bind(user_id)
        .bind(asset)
        .fetch_optional(self.pool)
        .await?
        .ok_or_else(|| anyhow!("Account not found: user={} asset={}", user_id, asset))
    }

    pub async fn get_all_accounts(&self, user_id: i64) -> Result<Vec<DbAccount>> {
        let rows = sqlx::query_as::<_, DbAccount>(
            "SELECT * FROM accounts WHERE user_id = $1 ORDER BY asset",
        )
        .bind(user_id)
        .fetch_all(self.pool)
        .await?;
        Ok(rows)
    }

    /// Ensure an account row exists; creates it with zero balance if not.
    pub async fn ensure_account(&self, user_id: i64, asset: &str) -> Result<()> {
        sqlx::query(
            "INSERT INTO accounts (user_id, asset, balance, frozen)
             VALUES ($1, $2, 0, 0) ON CONFLICT DO NOTHING",
        )
        .bind(user_id)
        .bind(asset)
        .execute(self.pool)
        .await?;
        Ok(())
    }

    /// Freeze funds for a buy order atomically.
    pub async fn freeze_for_buy(&self, user_id: i64, asset: &str, amount: f64) -> Result<()> {
        let rows = sqlx::query(
            "UPDATE accounts SET frozen = frozen + $1, updated_at = NOW()
             WHERE user_id = $2 AND asset = $3
               AND (balance - frozen) >= $1",
        )
        .bind(amount)
        .bind(user_id)
        .bind(asset)
        .execute(self.pool)
        .await?
        .rows_affected();

        if rows == 0 {
            Err(anyhow!("Insufficient {} balance to freeze {}", asset, amount))
        } else {
            Ok(())
        }
    }

    /// Freeze position for a sell order atomically.
    pub async fn freeze_for_sell(&self, user_id: i64, asset: &str, qty: f64) -> Result<()> {
        let rows = sqlx::query(
            "UPDATE accounts SET frozen = frozen + $1, updated_at = NOW()
             WHERE user_id = $2 AND asset = $3
               AND (balance - frozen) >= $1",
        )
        .bind(qty)
        .bind(user_id)
        .bind(asset)
        .execute(self.pool)
        .await?
        .rows_affected();

        if rows == 0 {
            Err(anyhow!("Insufficient {} position to freeze {}", asset, qty))
        } else {
            Ok(())
        }
    }

    /// Release frozen funds (on order cancel).
    pub async fn release_frozen(&self, user_id: i64, asset: &str, amount: f64) -> Result<()> {
        sqlx::query(
            "UPDATE accounts SET frozen = GREATEST(frozen - $1, 0), updated_at = NOW()
             WHERE user_id = $2 AND asset = $3",
        )
        .bind(amount)
        .bind(user_id)
        .bind(asset)
        .execute(self.pool)
        .await?;
        Ok(())
    }

    /// Settle a fill: debit quote asset from buyer, credit base asset; debit base from seller, credit quote.
    /// All four account updates run in a single transaction.
    pub async fn settle_trade(
        &self,
        buyer_id: i64,
        seller_id: i64,
        base_asset: &str,   // e.g. "BTC"
        quote_asset: &str,  // e.g. "USDT"
        price: f64,
        quantity: f64,
        buy_fee: f64,
        sell_fee: f64,
    ) -> Result<()> {
        let cost = price * quantity;
        let mut tx = self.pool.begin().await?;

        // Buyer: deduct frozen USDT (cost + fee), credit BTC
        sqlx::query(
            "UPDATE accounts SET balance = balance - $1, frozen = frozen - $1, updated_at = NOW()
             WHERE user_id = $2 AND asset = $3",
        )
        .bind(cost + buy_fee)
        .bind(buyer_id)
        .bind(quote_asset)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            "INSERT INTO accounts (user_id, asset, balance, frozen) VALUES ($1, $2, $3, 0)
             ON CONFLICT (user_id, asset) DO UPDATE
               SET balance = accounts.balance + $3, updated_at = NOW()",
        )
        .bind(buyer_id)
        .bind(base_asset)
        .bind(quantity)
        .execute(&mut *tx)
        .await?;

        // Seller: deduct frozen BTC, credit USDT (proceeds - fee)
        sqlx::query(
            "UPDATE accounts SET balance = balance - $1, frozen = frozen - $1, updated_at = NOW()
             WHERE user_id = $2 AND asset = $3",
        )
        .bind(quantity)
        .bind(seller_id)
        .bind(base_asset)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            "INSERT INTO accounts (user_id, asset, balance, frozen) VALUES ($1, $2, $3, 0)
             ON CONFLICT (user_id, asset) DO UPDATE
               SET balance = accounts.balance + $3, updated_at = NOW()",
        )
        .bind(seller_id)
        .bind(quote_asset)
        .bind(cost - sell_fee)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }
}
