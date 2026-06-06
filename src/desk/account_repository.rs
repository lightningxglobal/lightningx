/// Persistent account operations backed by PostgreSQL.
/// Mirrors the in-memory AccountManager semantics with DB atomicity.
use crate::desk::money::AmountAtoms;
use crate::models::DbAccount;
use anyhow::{Result, anyhow};
use sqlx::PgPool;

pub struct AccountRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> AccountRepository<'a> {
    pub fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    pub async fn get_account(&self, user_id: i64, asset: &str) -> Result<DbAccount> {
        sqlx::query_as::<_, DbAccount>("SELECT * FROM accounts WHERE user_id = $1 AND asset = $2")
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
            "INSERT INTO accounts (user_id, asset, balance, frozen, balance_atoms, frozen_atoms)
             VALUES ($1, $2, 0, 0, 0, 0) ON CONFLICT DO NOTHING",
        )
        .bind(user_id)
        .bind(asset)
        .execute(self.pool)
        .await?;
        Ok(())
    }

    /// Freeze funds for a buy order atomically. Returns (balance, frozen) after update.
    pub async fn freeze_for_buy(
        &self,
        user_id: i64,
        asset: &str,
        amount: f64,
    ) -> Result<(f64, f64)> {
        let amount_atoms = AmountAtoms::from_f64_round(amount)?.atoms();
        let row: Option<(f64, f64)> = sqlx::query_as(
            "UPDATE accounts SET
                frozen = frozen + $1,
                frozen_atoms = frozen_atoms + $2,
                updated_at = NOW()
             WHERE user_id = $3 AND asset = $4
               AND (balance_atoms - frozen_atoms) >= $2
             RETURNING balance, frozen",
        )
        .bind(amount)
        .bind(amount_atoms)
        .bind(user_id)
        .bind(asset)
        .fetch_optional(self.pool)
        .await?;

        row.ok_or_else(|| anyhow!("Insufficient {} balance to freeze {}", asset, amount))
    }

    /// Freeze position for a sell order atomically. Returns (balance, frozen) after update.
    pub async fn freeze_for_sell(&self, user_id: i64, asset: &str, qty: f64) -> Result<(f64, f64)> {
        let qty_atoms = AmountAtoms::from_f64_round(qty)?.atoms();
        let row: Option<(f64, f64)> = sqlx::query_as(
            "UPDATE accounts SET
                frozen = frozen + $1,
                frozen_atoms = frozen_atoms + $2,
                updated_at = NOW()
             WHERE user_id = $3 AND asset = $4
               AND (balance_atoms - frozen_atoms) >= $2
             RETURNING balance, frozen",
        )
        .bind(qty)
        .bind(qty_atoms)
        .bind(user_id)
        .bind(asset)
        .fetch_optional(self.pool)
        .await?;

        row.ok_or_else(|| anyhow!("Insufficient {} position to freeze {}", asset, qty))
    }

    /// Release frozen funds (on order cancel). Returns (balance, frozen) after update.
    pub async fn release_frozen(
        &self,
        user_id: i64,
        asset: &str,
        amount: f64,
    ) -> Result<(f64, f64)> {
        let amount_atoms = AmountAtoms::from_f64_round(amount)?.atoms();
        let row: Option<(f64, f64)> = sqlx::query_as(
            "UPDATE accounts SET
                frozen = GREATEST(frozen - $1, 0),
                frozen_atoms = GREATEST(frozen_atoms - $2, 0),
                updated_at = NOW()
             WHERE user_id = $3 AND asset = $4
             RETURNING balance, frozen",
        )
        .bind(amount)
        .bind(amount_atoms)
        .bind(user_id)
        .bind(asset)
        .fetch_optional(self.pool)
        .await?;

        // If no row matched (account doesn't exist), treat as a no-op.
        Ok(row.unwrap_or((0.0, 0.0)))
    }

    /// Settle a fill: debit quote asset from buyer, credit base asset; debit base from seller, credit quote.
    /// All four account updates run in a single transaction.
    pub async fn settle_trade(
        &self,
        buyer_id: i64,
        seller_id: i64,
        base_asset: &str,  // e.g. "BTC"
        quote_asset: &str, // e.g. "USDT"
        price: f64,
        quantity: f64,
        buy_fee: f64,
        sell_fee: f64,
    ) -> Result<()> {
        let cost = price * quantity;
        let cost_atoms = AmountAtoms::from_f64_round(cost)?.atoms();
        let buy_fee_atoms = AmountAtoms::from_f64_round(buy_fee)?.atoms();
        let sell_fee_atoms = AmountAtoms::from_f64_round(sell_fee)?.atoms();
        let quantity_atoms = AmountAtoms::from_f64_round(quantity)?.atoms();
        let buyer_debit_atoms = cost_atoms
            .checked_add(buy_fee_atoms)
            .ok_or_else(|| anyhow!("buyer debit amount overflow"))?;
        let seller_credit_atoms = cost_atoms
            .checked_sub(sell_fee_atoms)
            .ok_or_else(|| anyhow!("seller credit amount overflow"))?;
        let mut tx = self.pool.begin().await?;

        // Buyer: deduct frozen USDT (cost + fee), credit BTC
        sqlx::query(
            "UPDATE accounts SET
                balance = balance - $1,
                frozen = frozen - $1,
                balance_atoms = balance_atoms - $2,
                frozen_atoms = frozen_atoms - $2,
                updated_at = NOW()
             WHERE user_id = $3 AND asset = $4",
        )
        .bind(cost + buy_fee)
        .bind(buyer_debit_atoms)
        .bind(buyer_id)
        .bind(quote_asset)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            "INSERT INTO accounts (user_id, asset, balance, frozen, balance_atoms, frozen_atoms)
             VALUES ($1, $2, $3, 0, $4, 0)
             ON CONFLICT (user_id, asset) DO UPDATE
               SET balance = accounts.balance + $3,
                   balance_atoms = accounts.balance_atoms + $4,
                   updated_at = NOW()",
        )
        .bind(buyer_id)
        .bind(base_asset)
        .bind(quantity)
        .bind(quantity_atoms)
        .execute(&mut *tx)
        .await?;

        // Seller: deduct frozen BTC, credit USDT (proceeds - fee)
        sqlx::query(
            "UPDATE accounts SET
                balance = balance - $1,
                frozen = frozen - $1,
                balance_atoms = balance_atoms - $2,
                frozen_atoms = frozen_atoms - $2,
                updated_at = NOW()
             WHERE user_id = $3 AND asset = $4",
        )
        .bind(quantity)
        .bind(quantity_atoms)
        .bind(seller_id)
        .bind(base_asset)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            "INSERT INTO accounts (user_id, asset, balance, frozen, balance_atoms, frozen_atoms)
             VALUES ($1, $2, $3, 0, $4, 0)
             ON CONFLICT (user_id, asset) DO UPDATE
               SET balance = accounts.balance + $3,
                   balance_atoms = accounts.balance_atoms + $4,
                   updated_at = NOW()",
        )
        .bind(seller_id)
        .bind(quote_asset)
        .bind(cost - sell_fee)
        .bind(seller_credit_atoms)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }
}
