use dashmap::DashMap;
use std::sync::Arc;

use super::types::{AccountRiskState, PositionRiskState, PositionSide};
#[cfg(test)]
use super::types::RiskStatus;
use super::calc;

pub struct RiskEngine {
    pub accounts: DashMap<i64, AccountRiskState>,
    pub positions: DashMap<(i64, [u8; 16]), PositionRiskState>,
    pub mark_prices: DashMap<[u8; 16], i64>,
}

impl RiskEngine {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            accounts: DashMap::new(),
            positions: DashMap::new(),
            mark_prices: DashMap::new(),
        })
    }

    pub fn initialize_account(&self, user_id: i64, usdt_balance_cents: i64) {
        self.accounts
            .insert(user_id, AccountRiskState::new(user_id, usdt_balance_cents));
    }

    pub fn check_and_reserve_margin(
        &self,
        user_id: i64,
        initial_margin_cents: i64,
    ) -> Result<i64, &'static str> {
        let Some(mut entry) = self.accounts.get_mut(&user_id) else {
            return Err("Account not found");
        };
        if !entry.can_place_order() {
            return Err("Account in liquidation");
        }
        if entry.available_margin < initial_margin_cents {
            return Err("Insufficient margin");
        }
        entry.available_margin -= initial_margin_cents;
        entry.order_margin += initial_margin_cents;
        Ok(initial_margin_cents)
    }

    pub fn release_order_margin(&self, user_id: i64, initial_margin_cents: i64) {
        if let Some(mut entry) = self.accounts.get_mut(&user_id) {
            entry.order_margin = (entry.order_margin - initial_margin_cents).max(0);
            entry.available_margin += initial_margin_cents;
        }
    }

    pub fn on_fill(
        &self,
        user_id: i64,
        symbol: [u8; 16],
        side: PositionSide,
        price_ticks: i64,
        qty_lots: i64,
        initial_margin_cents: i64,
        notional_scale: i64,
        leverage: u8,
        maintenance_rate_bps: i64,
    ) {
        // Phase 1 stub: log only. Full position tracking in Phase 2.
        tracing::debug!(
            user_id,
            price_ticks,
            qty_lots,
            initial_margin_cents,
            "on_fill stub — position tracking deferred to Phase 2"
        );
        let _ = (symbol, side, notional_scale, leverage, maintenance_rate_bps);
        let _ = calc::calc_notional_cents; // suppress unused import warning
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_engine_with_account(balance_cents: i64) -> (Arc<RiskEngine>, i64) {
        let engine = RiskEngine::new();
        let user_id = 42i64;
        engine.initialize_account(user_id, balance_cents);
        (engine, user_id)
    }

    #[test]
    fn initialize_account_sets_available_margin() {
        let (engine, user_id) = make_engine_with_account(100_000); // $1000
        let state = engine.accounts.get(&user_id).unwrap();
        assert_eq!(state.available_margin, 100_000);
        assert_eq!(state.order_margin, 0);
        assert_eq!(state.used_margin, 0);
        assert_eq!(state.equity, 100_000);
    }

    #[test]
    fn reserve_margin_succeeds_when_sufficient() {
        let (engine, user_id) = make_engine_with_account(100_000);
        let result = engine.check_and_reserve_margin(user_id, 10_000);
        assert_eq!(result, Ok(10_000));
        let state = engine.accounts.get(&user_id).unwrap();
        assert_eq!(state.available_margin, 90_000);
        assert_eq!(state.order_margin, 10_000);
    }

    #[test]
    fn reserve_margin_fails_when_insufficient() {
        let (engine, user_id) = make_engine_with_account(5_000);
        let result = engine.check_and_reserve_margin(user_id, 10_000);
        assert!(result.is_err());
        // State unchanged
        let state = engine.accounts.get(&user_id).unwrap();
        assert_eq!(state.available_margin, 5_000);
        assert_eq!(state.order_margin, 0);
    }

    #[test]
    fn release_restores_available_margin() {
        let (engine, user_id) = make_engine_with_account(100_000);
        engine.check_and_reserve_margin(user_id, 10_000).unwrap();
        engine.release_order_margin(user_id, 10_000);
        let state = engine.accounts.get(&user_id).unwrap();
        assert_eq!(state.available_margin, 100_000);
        assert_eq!(state.order_margin, 0);
    }

    #[test]
    fn concurrent_reserve_one_must_fail() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let engine = RiskEngine::new();
        let user_id = 7i64;
        engine.initialize_account(user_id, 15_000); // only $150

        let engine = Arc::new(engine);
        let success = Arc::new(AtomicUsize::new(0));
        let failure = Arc::new(AtomicUsize::new(0));

        let handles: Vec<_> = (0..2)
            .map(|_| {
                let e = engine.clone();
                let s = success.clone();
                let f = failure.clone();
                std::thread::spawn(move || {
                    match e.check_and_reserve_margin(user_id, 10_000) {
                        Ok(_) => s.fetch_add(1, Ordering::Relaxed),
                        Err(_) => f.fetch_add(1, Ordering::Relaxed),
                    };
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(success.load(Ordering::Relaxed), 1, "exactly one should succeed");
        assert_eq!(failure.load(Ordering::Relaxed), 1, "exactly one should fail");
        let state = engine.accounts.get(&user_id).unwrap();
        assert_eq!(state.available_margin, 5_000);
        assert_eq!(state.order_margin, 10_000);
    }

    #[test]
    fn liquidation_pending_account_rejected() {
        let engine = RiskEngine::new();
        let user_id = 99i64;
        engine.initialize_account(user_id, 100_000);
        // Force status to LiquidationPending
        if let Some(mut entry) = engine.accounts.get_mut(&user_id) {
            entry.status = RiskStatus::LiquidationPending;
        }
        let result = engine.check_and_reserve_margin(user_id, 1_000);
        assert!(result.is_err());
    }
}
