/// Persistent account operations backed by PostgreSQL.
/// Mirrors the in-memory AccountManager semantics with DB atomicity.
///
/// All monetary arithmetic happens in fixed-point atoms (i64, 1e-8). The
/// `*_atoms` methods are the canonical API; the f64 variants are thin
/// boundary wrappers kept for legacy callers and perform exactly one
/// conversion at entry. The float8 legacy columns were dropped pre-launch
/// (migration 018): atoms are the only representation of money.
use crate::desk::money::{AccountBalance, AmountAtoms};
use crate::models::DbAccount;
use anyhow::{Result, anyhow};
use sqlx::PgPool;

/// Trade row recorded atomically with the settlement legs.
#[derive(Debug, Clone)]
pub struct SettleTradeRecord {
    pub symbol: String,
    pub buy_order_id: i64,
    pub sell_order_id: i64,
}

pub struct AccountRepository<'a> {
    pool: &'a PgPool,
}

/// Append one fund_audit row inside the caller's transaction.
async fn fund_audit(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    user_id: i64,
    asset: &str,
    kind: &str,
    amount_atoms: i64,
    ref_id: i64,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO fund_audit (user_id, asset, kind, amount_atoms, ref_id)
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(user_id)
    .bind(asset)
    .bind(kind)
    .bind(amount_atoms)
    .bind(ref_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
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
            "INSERT INTO accounts (user_id, asset, balance_atoms, frozen_atoms)
             VALUES ($1, $2, 0, 0) ON CONFLICT DO NOTHING",
        )
        .bind(user_id)
        .bind(asset)
        .execute(self.pool)
        .await?;
        Ok(())
    }

    /// Freeze funds for a buy order atomically. Returns fixed-point balance after update.
    /// Legacy f64 boundary wrapper around [`Self::freeze_atoms`].
    pub async fn freeze_for_buy(
        &self,
        user_id: i64,
        asset: &str,
        amount: f64,
    ) -> Result<AccountBalance> {
        self.freeze_atoms(user_id, asset, AmountAtoms::from_f64_round(amount)?)
            .await
    }

    /// Freeze position for a sell order atomically. Returns fixed-point balance after update.
    /// Legacy f64 boundary wrapper around [`Self::freeze_atoms`].
    pub async fn freeze_for_sell(
        &self,
        user_id: i64,
        asset: &str,
        qty: f64,
    ) -> Result<AccountBalance> {
        self.freeze_atoms(user_id, asset, AmountAtoms::from_f64_round(qty)?)
            .await
    }

    /// Freeze an atom amount against available (balance - frozen) atomically.
    pub async fn freeze_atoms(
        &self,
        user_id: i64,
        asset: &str,
        amount: AmountAtoms,
    ) -> Result<AccountBalance> {
        if amount.atoms() < 0 {
            return Err(anyhow!("cannot freeze a negative amount"));
        }
        let mut tx = self.pool.begin().await?;
        let row: Option<(i64, i64)> = sqlx::query_as(
            "UPDATE accounts SET
                frozen_atoms = frozen_atoms + $1,
                updated_at = NOW()
             WHERE user_id = $2 AND asset = $3
               AND (balance_atoms - frozen_atoms) >= $1
             RETURNING balance_atoms, frozen_atoms",
        )
        .bind(amount.atoms())
        .bind(user_id)
        .bind(asset)
        .fetch_optional(&mut *tx)
        .await?;
        let Some((balance_atoms, frozen_atoms)) = row else {
            tx.rollback().await.ok();
            return Err(anyhow!(
                "Insufficient {} balance to freeze {}",
                asset,
                amount.to_decimal_string()
            ));
        };
        fund_audit(&mut tx, user_id, asset, "freeze", amount.atoms(), 0).await?;
        tx.commit().await?;
        Ok(AccountBalance::from_atoms(balance_atoms, frozen_atoms))
    }

    /// Release frozen funds (on order cancel). Returns fixed-point balance after update.
    /// Legacy f64 boundary wrapper around [`Self::release_frozen_atoms`].
    pub async fn release_frozen(
        &self,
        user_id: i64,
        asset: &str,
        amount: f64,
    ) -> Result<AccountBalance> {
        self.release_frozen_atoms(user_id, asset, AmountAtoms::from_f64_round(amount)?)
            .await
    }

    /// Release frozen atoms (on order cancel). Clamps at zero.
    pub async fn release_frozen_atoms(
        &self,
        user_id: i64,
        asset: &str,
        amount: AmountAtoms,
    ) -> Result<AccountBalance> {
        if amount.atoms() < 0 {
            return Err(anyhow!("cannot release a negative amount"));
        }
        let mut tx = self.pool.begin().await?;
        let row: Option<(i64, i64)> = sqlx::query_as(
            "UPDATE accounts SET
                frozen_atoms = GREATEST(frozen_atoms - $1, 0),
                updated_at = NOW()
             WHERE user_id = $2 AND asset = $3
             RETURNING balance_atoms, frozen_atoms",
        )
        .bind(amount.atoms())
        .bind(user_id)
        .bind(asset)
        .fetch_optional(&mut *tx)
        .await?;
        if row.is_some() {
            fund_audit(&mut tx, user_id, asset, "release", amount.atoms(), 0).await?;
        }
        tx.commit().await?;
        // If no row matched (account doesn't exist), treat as a no-op.
        Ok(row
            .map(|(balance_atoms, frozen_atoms)| {
                AccountBalance::from_atoms(balance_atoms, frozen_atoms)
            })
            .unwrap_or_default())
    }

    /// Settle a fill. Legacy f64 boundary wrapper: converts once at entry,
    /// then all arithmetic (cost = price × quantity, fee legs) runs in the
    /// integer domain inside [`Self::settle_trade_atoms`].
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
        self.settle_trade_atoms(
            buyer_id,
            seller_id,
            base_asset,
            quote_asset,
            AmountAtoms::from_f64_round(price)?,
            AmountAtoms::from_f64_round(quantity)?,
            AmountAtoms::from_f64_round(buy_fee)?,
            AmountAtoms::from_f64_round(sell_fee)?,
            None,
        )
        .await
    }

    /// Settle a fill: debit quote asset from buyer, credit base asset; debit base
    /// from seller, credit quote. All four account updates run in a single
    /// transaction; cost/fee arithmetic is integer-only (i128 intermediate,
    /// round-half-up on the price × quantity scale division).
    ///
    /// Quote conservation holds by construction:
    ///   buyer_debit = cost + buy_fee = (cost - sell_fee) + (buy_fee + sell_fee)
    ///               = seller_credit + fee_revenue.
    /// `trade`: when provided, the trade row is INSERTed in the SAME
    /// transaction as the four account legs (idempotent via the
    /// (buy_order_id, sell_order_id) unique pair) — settlement and trade
    /// record commit or roll back together.
    #[allow(clippy::too_many_arguments)]
    pub async fn settle_trade_atoms(
        &self,
        buyer_id: i64,
        seller_id: i64,
        base_asset: &str,
        quote_asset: &str,
        price: AmountAtoms,
        quantity: AmountAtoms,
        buy_fee: AmountAtoms,
        sell_fee: AmountAtoms,
        trade: Option<&SettleTradeRecord>,
    ) -> Result<()> {
        let cost = price.checked_mul_scaled(quantity)?;
        let buyer_debit = cost.checked_add(buy_fee)?;
        let seller_credit = cost.checked_sub(sell_fee)?;
        if seller_credit.atoms() < 0 {
            return Err(anyhow!("sell fee exceeds trade cost"));
        }
        let mut tx = self.pool.begin().await?;

        // Buyer: deduct frozen quote (cost + fee), credit base
        sqlx::query(
            "UPDATE accounts SET
                balance_atoms = balance_atoms - $1,
                frozen_atoms = frozen_atoms - $1,
                updated_at = NOW()
             WHERE user_id = $2 AND asset = $3",
        )
        .bind(buyer_debit.atoms())
        .bind(buyer_id)
        .bind(quote_asset)
        .execute(&mut *tx)
        .await
        .map_err(|e| anyhow!("settle stmt-1: {e}"))?;

        sqlx::query(
            "INSERT INTO accounts (user_id, asset, balance_atoms, frozen_atoms)
             VALUES ($1, $2, $3, 0)
             ON CONFLICT (user_id, asset) DO UPDATE
               SET balance_atoms = accounts.balance_atoms + $3,
                   updated_at = NOW()",
        )
        .bind(buyer_id)
        .bind(base_asset)
        .bind(quantity.atoms())
        .execute(&mut *tx)
        .await
        .map_err(|e| anyhow!("settle stmt-2: {e}"))?;

        // Seller: deduct frozen base, credit quote (proceeds - fee)
        sqlx::query(
            "UPDATE accounts SET
                balance_atoms = balance_atoms - $1,
                frozen_atoms = frozen_atoms - $1,
                updated_at = NOW()
             WHERE user_id = $2 AND asset = $3",
        )
        .bind(quantity.atoms())
        .bind(seller_id)
        .bind(base_asset)
        .execute(&mut *tx)
        .await
        .map_err(|e| anyhow!("settle stmt-3: {e}"))?;

        sqlx::query(
            "INSERT INTO accounts (user_id, asset, balance_atoms, frozen_atoms)
             VALUES ($1, $2, $3, 0)
             ON CONFLICT (user_id, asset) DO UPDATE
               SET balance_atoms = accounts.balance_atoms + $3,
                   updated_at = NOW()",
        )
        .bind(seller_id)
        .bind(quote_asset)
        .bind(seller_credit.atoms())
        .execute(&mut *tx)
        .await
        .map_err(|e| anyhow!("settle stmt-4: {e}"))?;

        // Trade record commits atomically with the settlement legs.
        if let Some(t) = trade {
            sqlx::query(
                "INSERT INTO trades (symbol, buy_order_id, sell_order_id,
                                     price_atoms, quantity_atoms)
                 VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT (buy_order_id, sell_order_id) DO NOTHING",
            )
            .bind(&t.symbol)
            .bind(t.buy_order_id)
            .bind(t.sell_order_id)
            .bind(price.atoms())
            .bind(quantity.atoms())
            .execute(&mut *tx)
            .await
            .map_err(|e| anyhow!("settle trade insert: {e}"))?;
        }

        // Fund audit: all four legs, same transaction.
        let trade_ref = trade.map(|t| t.buy_order_id).unwrap_or(0);
        fund_audit(&mut tx, buyer_id, quote_asset, "settle_debit", buyer_debit.atoms(), trade_ref)
            .await?;
        fund_audit(&mut tx, buyer_id, base_asset, "settle_credit", quantity.atoms(), trade_ref)
            .await?;
        fund_audit(&mut tx, seller_id, base_asset, "settle_debit", quantity.atoms(), trade_ref)
            .await?;
        fund_audit(
            &mut tx,
            seller_id,
            quote_asset,
            "settle_credit",
            seller_credit.atoms(),
            trade_ref,
        )
        .await?;

        tx.commit().await?;
        Ok(())
    }
}
