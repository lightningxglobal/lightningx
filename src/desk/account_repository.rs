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

    /// Credit a confirmed on-chain deposit to a user's virtual sub-account
    /// — the narrow interface the deposit watcher calls after N
    /// confirmations. EXACTLY ONCE per (chain, tx_hash, log_index): the
    /// chain_deposits UNIQUE constraint + ON CONFLICT DO NOTHING makes a
    /// replayed/duplicate delivery a harmless no-op. All in ONE
    /// transaction: deposit row + balance credit + fund_audit, so a crash
    /// can neither credit without recording nor record without crediting.
    ///
    /// Returns (credited, new_balance_atoms): credited=false means this
    /// transfer was already applied (idempotent skip).
    pub async fn credit_chain_deposit(
        &self,
        chain: &str,
        tx_hash: &str,
        log_index: i32,
        user_id: i64,
        asset: &str,
        amount_atoms: i64,
        from_address: Option<&str>,
        to_address: Option<&str>,
    ) -> Result<(bool, i64)> {
        if amount_atoms <= 0 {
            return Err(anyhow!("deposit amount must be positive"));
        }
        let mut tx = self.pool.begin().await?;

        // Idempotency anchor: insert the deposit row. ON CONFLICT means
        // this (chain, tx_hash, log_index) was already credited.
        let inserted = sqlx::query(
            "INSERT INTO chain_deposits
                (chain, tx_hash, log_index, user_id, asset, amount_atoms,
                 from_address, to_address)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
             ON CONFLICT (chain, tx_hash, log_index) DO NOTHING",
        )
        .bind(chain)
        .bind(tx_hash)
        .bind(log_index)
        .bind(user_id)
        .bind(asset)
        .bind(amount_atoms)
        .bind(from_address)
        .bind(to_address)
        .execute(&mut *tx)
        .await?;

        if inserted.rows_affected() == 0 {
            // Already credited — return the current balance, no double credit.
            tx.rollback().await?;
            let bal: i64 = sqlx::query_scalar(
                "SELECT balance_atoms FROM accounts WHERE user_id = $1 AND asset = $2",
            )
            .bind(user_id)
            .bind(asset)
            .fetch_optional(self.pool)
            .await?
            .unwrap_or(0);
            return Ok((false, bal));
        }

        // Credit the balance (create the account row if first-ever).
        let new_balance: i64 = sqlx::query_scalar(
            "INSERT INTO accounts (user_id, asset, balance_atoms, frozen_atoms)
             VALUES ($1, $2, $3, 0)
             ON CONFLICT (user_id, asset) DO UPDATE SET
                balance_atoms = accounts.balance_atoms + EXCLUDED.balance_atoms,
                updated_at = NOW()
             RETURNING balance_atoms",
        )
        .bind(user_id)
        .bind(asset)
        .bind(amount_atoms)
        .fetch_one(&mut *tx)
        .await?;

        // Append-only audit row in the SAME transaction.
        sqlx::query(
            "INSERT INTO fund_audit (user_id, asset, kind, amount_atoms, ref_id)
             VALUES ($1, $2, 'deposit', $3, $4)",
        )
        .bind(user_id)
        .bind(asset)
        .bind(amount_atoms)
        .bind(0i64)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok((true, new_balance))
    }

    /// Phase 1 — request a withdrawal: freeze (amount + fee) and insert a
    /// 'pending' row, in ONE transaction. Returns the withdrawal id and
    /// the post-freeze balance. Fails (no row, no freeze) on insufficient
    /// spendable balance. ref_id of the audit row is the withdrawal id.
    pub async fn request_withdrawal(
        &self,
        user_id: i64,
        asset: &str,
        chain: &str,
        to_address: &str,
        amount_atoms: i64,
        fee_atoms: i64,
    ) -> Result<(i64, AccountBalance)> {
        if amount_atoms <= 0 || fee_atoms < 0 {
            return Err(anyhow!("withdrawal amount must be positive, fee non-negative"));
        }
        let total = amount_atoms
            .checked_add(fee_atoms)
            .ok_or_else(|| anyhow!("amount + fee overflow"))?;
        let mut tx = self.pool.begin().await?;
        // Freeze the total (amount + fee) atomically — only succeeds if
        // spendable (balance - frozen) covers it.
        let row: Option<(i64, i64)> = sqlx::query_as(
            "UPDATE accounts SET frozen_atoms = frozen_atoms + $1, updated_at = NOW()
              WHERE user_id = $2 AND asset = $3 AND (balance_atoms - frozen_atoms) >= $1
              RETURNING balance_atoms, frozen_atoms",
        )
        .bind(total)
        .bind(user_id)
        .bind(asset)
        .fetch_optional(&mut *tx)
        .await?;
        let Some((balance_atoms, frozen_atoms)) = row else {
            tx.rollback().await.ok();
            return Err(anyhow!("insufficient {asset} balance to withdraw"));
        };
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO withdrawals
                (user_id, asset, chain, to_address, amount_atoms, fee_atoms, status)
             VALUES ($1, $2, $3, $4, $5, $6, 'pending') RETURNING id",
        )
        .bind(user_id)
        .bind(asset)
        .bind(chain)
        .bind(to_address)
        .bind(amount_atoms)
        .bind(fee_atoms)
        .fetch_one(&mut *tx)
        .await?;
        fund_audit(&mut tx, user_id, asset, "wd_freeze", total, id).await?;
        tx.commit().await?;
        Ok((id, AccountBalance::from_atoms(balance_atoms, frozen_atoms)))
    }

    /// Move a withdrawal between non-terminal statuses (approve/broadcast)
    /// WITHOUT touching the balance. Idempotent on `from`: only a row in
    /// `from` transitions, so a re-delivery is a no-op (returns false).
    pub async fn set_withdrawal_status(
        &self,
        id: i64,
        from: &str,
        to: &str,
        tx_hash: Option<&str>,
    ) -> Result<bool> {
        let res = sqlx::query(
            "UPDATE withdrawals
                SET status = $3, tx_hash = COALESCE($4, tx_hash), updated_at = NOW()
              WHERE id = $1 AND status = $2",
        )
        .bind(id)
        .bind(from)
        .bind(to)
        .bind(tx_hash)
        .execute(self.pool)
        .await?;
        Ok(res.rows_affected() == 1)
    }

    /// Phase 2a — confirm: the chain settled the send, so DEBIT the frozen
    /// amount + fee (balance -= total, frozen -= total) and mark
    /// 'confirmed'. Idempotent: only a non-terminal ('approved' or
    /// 'broadcast') row debits, so a re-delivery cannot double-debit.
    /// Returns true if this call performed the debit.
    pub async fn confirm_withdrawal(&self, id: i64, tx_hash: &str) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        // Lock the row and read its terminal-ness + amounts.
        let row: Option<(i64, String, i64, i64, String)> = sqlx::query_as(
            "SELECT user_id, asset, amount_atoms, fee_atoms, status
               FROM withdrawals WHERE id = $1 FOR UPDATE",
        )
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some((user_id, asset, amount, fee, status)) = row else {
            tx.rollback().await.ok();
            return Err(anyhow!("withdrawal {id} not found"));
        };
        if status == "confirmed" {
            tx.rollback().await.ok();
            return Ok(false); // already confirmed — idempotent no-op
        }
        if status != "approved" && status != "broadcast" {
            tx.rollback().await.ok();
            return Err(anyhow!("withdrawal {id} not in a confirmable state ({status})"));
        }
        let total = amount + fee;
        // Debit: the frozen funds actually leave the exchange.
        sqlx::query(
            "UPDATE accounts SET
                balance_atoms = balance_atoms - $1,
                frozen_atoms  = frozen_atoms - $1,
                updated_at = NOW()
              WHERE user_id = $2 AND asset = $3",
        )
        .bind(total)
        .bind(user_id)
        .bind(&asset)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE withdrawals SET status='confirmed', tx_hash=$2, updated_at=NOW() WHERE id=$1",
        )
        .bind(id)
        .bind(tx_hash)
        .execute(&mut *tx)
        .await?;
        fund_audit(&mut tx, user_id, &asset, "wd_debit", total, id).await?;
        tx.commit().await?;
        Ok(true)
    }

    /// Phase 2b — fail: the send did not happen, so RELEASE the freeze
    /// (frozen -= total, funds spendable again) and mark 'failed'.
    /// Idempotent: only a non-terminal row releases.
    pub async fn fail_withdrawal(&self, id: i64, reason: &str) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        let row: Option<(i64, String, i64, i64, String)> = sqlx::query_as(
            "SELECT user_id, asset, amount_atoms, fee_atoms, status
               FROM withdrawals WHERE id = $1 FOR UPDATE",
        )
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some((user_id, asset, amount, fee, status)) = row else {
            tx.rollback().await.ok();
            return Err(anyhow!("withdrawal {id} not found"));
        };
        if status == "failed" || status == "cancelled" {
            tx.rollback().await.ok();
            return Ok(false); // already released — idempotent
        }
        if status == "confirmed" {
            tx.rollback().await.ok();
            return Err(anyhow!("withdrawal {id} already confirmed — cannot fail"));
        }
        let total = amount + fee;
        sqlx::query(
            "UPDATE accounts SET frozen_atoms = frozen_atoms - $1, updated_at = NOW()
              WHERE user_id = $2 AND asset = $3",
        )
        .bind(total)
        .bind(user_id)
        .bind(&asset)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE withdrawals SET status='failed', fail_reason=$2, updated_at=NOW() WHERE id=$1",
        )
        .bind(id)
        .bind(reason)
        .execute(&mut *tx)
        .await?;
        fund_audit(&mut tx, user_id, &asset, "wd_release", total, id).await?;
        tx.commit().await?;
        Ok(true)
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
