//! Runtime exchange controls — testnet ops console (T2).
//!
//! A lock-free in-memory mirror of the `exchange_config` table: the desk
//! hydrates it at startup and the admin endpoints refresh a symbol's row
//! on every change, so operators halt a market / retune fees WITHOUT a
//! process restart. Hot-path reads (the order-entry halt check, the fee
//! lookup at fill time) are DashMap point reads.

use dashmap::DashMap;
use std::sync::Arc;

/// One symbol's runtime controls. `None` fee = fall through to the
/// env/SymbolRules default.
#[derive(Clone, Copy, Debug, Default)]
pub struct SymbolControl {
    pub trading_halted: bool,
    pub maker_fee_bps: Option<i64>,
    pub taker_fee_bps: Option<i64>,
}

#[derive(Default)]
pub struct ExchangeConfig {
    by_symbol: DashMap<String, SymbolControl>,
}

impl ExchangeConfig {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// True when order ENTRY is halted for `symbol` (cancels and
    /// liquidations are never gated by this — you must be able to
    /// de-risk a halted market).
    pub fn is_halted(&self, symbol: &str) -> bool {
        self.by_symbol.get(symbol).map(|c| c.trading_halted).unwrap_or(false)
    }

    /// Effective (maker, taker) fee bps: the per-symbol override if set,
    /// else the env/SymbolRules default.
    pub fn fee_bps(&self, symbol: &str) -> (i64, i64) {
        let (d_maker, d_taker) = crate::desk::symbol_rules::SymbolRules::fee_bps(symbol);
        match self.by_symbol.get(symbol) {
            Some(c) => (
                c.maker_fee_bps.unwrap_or(d_maker),
                c.taker_fee_bps.unwrap_or(d_taker),
            ),
            None => (d_maker, d_taker),
        }
    }

    /// Update the in-memory control for `symbol`. The caller is responsible for
    /// persisting the new state to PG so that a desk restart restores it via
    /// `hydrate`.
    ///
    /// D3-TODO: persist halt state to PG table exchange_config (key TEXT PK, value TEXT).
    // DDL: CREATE TABLE IF NOT EXISTS exchange_config (key TEXT PRIMARY KEY, value TEXT NOT NULL);
    // On halt: INSERT INTO exchange_config (key,value)
    //          VALUES (format!("market_halt:{symbol}"), "true")
    //          ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
    // On resume: same upsert with value = "false".
    // On startup: query exchange_config WHERE key LIKE 'market_halt:%' and restore
    //             trading_halted for each symbol before accepting orders.
    pub fn set(&self, symbol: &str, control: SymbolControl) {
        self.by_symbol.insert(symbol.to_string(), control);
    }

    pub fn get(&self, symbol: &str) -> SymbolControl {
        self.by_symbol.get(symbol).map(|c| *c).unwrap_or_default()
    }

    /// Snapshot for the admin GET endpoint.
    pub fn all(&self) -> Vec<(String, SymbolControl)> {
        self.by_symbol.iter().map(|e| (e.key().clone(), *e.value())).collect()
    }

    /// Load every row from PG (called at desk startup).
    pub async fn hydrate(&self, pool: &sqlx::PgPool) -> anyhow::Result<usize> {
        use sqlx::Row;
        let rows = sqlx::query(
            "SELECT symbol, trading_halted, maker_fee_bps, taker_fee_bps FROM exchange_config",
        )
        .fetch_all(pool)
        .await?;
        let n = rows.len();
        for row in rows {
            self.by_symbol.insert(
                row.get::<String, _>("symbol"),
                SymbolControl {
                    trading_halted: row.get("trading_halted"),
                    maker_fee_bps: row.get("maker_fee_bps"),
                    taker_fee_bps: row.get("taker_fee_bps"),
                },
            );
        }
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn halt_and_fee_override_fall_through() {
        let cfg = ExchangeConfig::new();
        // Unknown symbol: not halted, default fees.
        assert!(!cfg.is_halted("BTC_USDT"));
        let (m, t) = cfg.fee_bps("BTC_USDT");
        assert!(m >= 0 && t > 0);

        // Halt + override taker only; maker still falls through.
        cfg.set(
            "BTC_USDT",
            SymbolControl {
                trading_halted: true,
                maker_fee_bps: None,
                taker_fee_bps: Some(20),
            },
        );
        assert!(cfg.is_halted("BTC_USDT"));
        let (m2, t2) = cfg.fee_bps("BTC_USDT");
        assert_eq!(m2, m, "maker falls through to default");
        assert_eq!(t2, 20, "taker uses the override");

        // Resume.
        cfg.set("BTC_USDT", SymbolControl::default());
        assert!(!cfg.is_halted("BTC_USDT"));
    }
}
