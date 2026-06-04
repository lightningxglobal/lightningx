use dashmap::DashMap;
use std::sync::Arc;

use super::calc;
use super::types::{AccountRiskState, PositionRiskState, PositionSide, RiskStatus};

pub struct RiskEngine {
    pub accounts: DashMap<i64, AccountRiskState>,
    pub positions: DashMap<(i64, [u8; 16]), PositionRiskState>,
    pub mark_prices: DashMap<[u8; 16], i64>,
    // symbol → user_ids with open positions (for mark-price scan in Phase 3)
    pub symbol_position_index: DashMap<[u8; 16], Vec<i64>>,
}

impl RiskEngine {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            accounts: DashMap::new(),
            positions: DashMap::new(),
            mark_prices: DashMap::new(),
            symbol_position_index: DashMap::new(),
        })
    }

    pub fn initialize_account(&self, user_id: i64, usdt_balance_cents: i64) {
        self.accounts
            .insert(user_id, AccountRiskState::new(user_id, usdt_balance_cents));
    }

    /// Hot path: O(1) shard-locked reserve. Returns reserved cents on success.
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

    /// Called on every FILLED or PARTIAL_FILL event.
    ///
    /// order_side: 0=buy (long), 1=sell (short) — same encoding as SBE/OrderRuntimeMeta.
    /// fill_margin_cents: proportional initial margin for this fill (caller computes from
    ///   original order margin × fill_qty / order_qty).
    pub fn on_fill(
        &self,
        user_id: i64,
        symbol: [u8; 16],
        order_side: u8,
        fill_price_ticks: i64,
        fill_qty_lots: i64,
        fill_margin_cents: i64,
        notional_scale: i64,
        leverage: u8,
        maintenance_rate_bps: i64,
    ) {
        if fill_qty_lots <= 0 {
            return;
        }

        let fill_side = if order_side == 0 {
            PositionSide::Long
        } else {
            PositionSide::Short
        };
        let key = (user_id, symbol);

        // Compute new position state without holding shard locks.
        let (new_pos, released_used_margin, realized_pnl_cents) = {
            let existing = self.positions.get(&key);
            compute_position_update(
                existing.as_deref(),
                user_id,
                symbol,
                fill_side,
                fill_price_ticks,
                fill_qty_lots,
                fill_margin_cents,
                notional_scale,
                leverage,
                maintenance_rate_bps,
            )
        };

        // Apply position update.
        match new_pos {
            Some(p) => {
                self.positions.insert(key, p);
            }
            None => {
                self.positions.remove(&key);
            }
        }

        // Apply account update: move fill_margin from order_margin → used_margin (opening),
        // or release used_margin and credit realized PnL (closing).
        if let Some(mut acct) = self.accounts.get_mut(&user_id) {
            acct.order_margin = (acct.order_margin - fill_margin_cents).max(0);
            if released_used_margin > 0 {
                // Closing: release position's used_margin and credit realized PnL.
                // fill_margin_cents was reserved for the close order and is also returned.
                acct.used_margin = (acct.used_margin - released_used_margin).max(0);
                acct.available_margin = (acct.available_margin
                    + fill_margin_cents
                    + released_used_margin
                    + realized_pnl_cents)
                    .max(0);
            } else {
                // Opening: move order_margin → used_margin.
                acct.used_margin += fill_margin_cents;
            }
            acct.equity = acct.available_margin
                + acct.order_margin
                + acct.used_margin
                + acct.unrealized_pnl;
        }

        // Maintain symbol_position_index for Phase 3 mark-price scan.
        let has_open = self
            .positions
            .get(&key)
            .map(|p| p.qty_lots > 0)
            .unwrap_or(false);
        if has_open {
            let mut idx = self.symbol_position_index.entry(symbol).or_default();
            if !idx.contains(&user_id) {
                idx.push(user_id);
            }
        } else if let Some(mut idx) = self.symbol_position_index.get_mut(&symbol) {
            idx.retain(|&id| id != user_id);
        }
    }
}

/// Pure function — computes next position state given existing position and a fill.
/// Returns (new_position_or_none, released_used_margin, realized_pnl_cents).
fn compute_position_update(
    existing: Option<&PositionRiskState>,
    user_id: i64,
    symbol: [u8; 16],
    fill_side: PositionSide,
    fill_price_ticks: i64,
    fill_qty_lots: i64,
    fill_margin_cents: i64,
    notional_scale: i64,
    leverage: u8,
    maintenance_rate_bps: i64,
) -> (Option<PositionRiskState>, i64, i64) {
    let Some(pos) = existing else {
        // No existing position — open new.
        let notional = calc::calc_notional_cents(fill_price_ticks, fill_qty_lots, notional_scale);
        let maint = calc::calc_maintenance_margin_cents(notional, maintenance_rate_bps);
        let liq = calc::calc_liquidation_price_ticks(
            fill_price_ticks,
            leverage,
            maintenance_rate_bps,
            fill_side,
        );
        let bkrpt =
            calc::calc_bankruptcy_price_ticks(fill_price_ticks, leverage, fill_side);
        return (
            Some(PositionRiskState {
                user_id,
                symbol,
                side: fill_side,
                qty_lots: fill_qty_lots,
                entry_price_ticks: fill_price_ticks,
                mark_price_ticks: fill_price_ticks,
                initial_margin: fill_margin_cents,
                maintenance_margin: maint,
                liquidation_price_ticks: liq,
                bankruptcy_price_ticks: bkrpt,
                leverage,
            }),
            0,
            0,
        );
    };

    let mut updated = pos.clone();
    updated.mark_price_ticks = fill_price_ticks;

    if pos.side == fill_side {
        // Adding to same-side position — weighted-average entry price.
        let old_qty = pos.qty_lots;
        let new_qty = old_qty + fill_qty_lots;
        updated.entry_price_ticks = ((pos.entry_price_ticks as i128 * old_qty as i128
            + fill_price_ticks as i128 * fill_qty_lots as i128)
            / new_qty as i128) as i64;
        updated.qty_lots = new_qty;
        updated.initial_margin += fill_margin_cents;
        refresh_risk_fields(&mut updated, notional_scale, maintenance_rate_bps);
        return (Some(updated), 0, 0);
    }

    // Opposite side — reducing or closing the position.
    let sign: i128 = if pos.side == PositionSide::Long { 1 } else { -1 };

    if fill_qty_lots >= pos.qty_lots {
        // Close (and possibly flip).
        let close_qty = pos.qty_lots;
        let realized_pnl = sign
            * (fill_price_ticks - pos.entry_price_ticks) as i128
            * close_qty as i128
            / notional_scale as i128;
        let released_margin = pos.initial_margin;

        let remaining_qty = fill_qty_lots - close_qty;
        if remaining_qty == 0 {
            return (None, released_margin, realized_pnl as i64);
        }

        // Flipped to opposite side.
        let flip_margin = fill_margin_cents * remaining_qty / fill_qty_lots;
        let notional = calc::calc_notional_cents(fill_price_ticks, remaining_qty, notional_scale);
        let maint = calc::calc_maintenance_margin_cents(notional, maintenance_rate_bps);
        let liq = calc::calc_liquidation_price_ticks(
            fill_price_ticks,
            leverage,
            maintenance_rate_bps,
            fill_side,
        );
        let bkrpt = calc::calc_bankruptcy_price_ticks(fill_price_ticks, leverage, fill_side);
        (
            Some(PositionRiskState {
                user_id,
                symbol,
                side: fill_side,
                qty_lots: remaining_qty,
                entry_price_ticks: fill_price_ticks,
                mark_price_ticks: fill_price_ticks,
                initial_margin: flip_margin,
                maintenance_margin: maint,
                liquidation_price_ticks: liq,
                bankruptcy_price_ticks: bkrpt,
                leverage,
            }),
            released_margin,
            realized_pnl as i64,
        )
    } else {
        // Partial close.
        let close_qty = fill_qty_lots;
        let realized_pnl = sign
            * (fill_price_ticks - pos.entry_price_ticks) as i128
            * close_qty as i128
            / notional_scale as i128;
        let released_margin = pos.initial_margin * close_qty / pos.qty_lots;

        updated.qty_lots = pos.qty_lots - close_qty;
        updated.initial_margin = pos.initial_margin - released_margin;
        refresh_risk_fields(&mut updated, notional_scale, maintenance_rate_bps);
        (Some(updated), released_margin, realized_pnl as i64)
    }
}

/// Recompute maintenance_margin, liquidation_price, bankruptcy_price from current qty/entry.
fn refresh_risk_fields(pos: &mut PositionRiskState, notional_scale: i64, maintenance_rate_bps: i64) {
    let notional =
        calc::calc_notional_cents(pos.entry_price_ticks, pos.qty_lots, notional_scale);
    pos.maintenance_margin = calc::calc_maintenance_margin_cents(notional, maintenance_rate_bps);
    pos.liquidation_price_ticks = calc::calc_liquidation_price_ticks(
        pos.entry_price_ticks,
        pos.leverage,
        maintenance_rate_bps,
        pos.side,
    );
    pos.bankruptcy_price_ticks =
        calc::calc_bankruptcy_price_ticks(pos.entry_price_ticks, pos.leverage, pos.side);
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

    fn btc_sym() -> [u8; 16] {
        let mut s = [0u8; 16];
        s[..7].copy_from_slice(b"BTC_USD");
        s[7] = b'T';
        s
    }

    // BTC_USDT notional_scale=1_000_000, default_leverage=10, maintenance_rate_bps=50
    const NOTIONAL_SCALE: i64 = 1_000_000;
    const LEVERAGE: u8 = 10;
    const MAINT_BPS: i64 = 50;

    #[test]
    fn initialize_account_sets_available_margin() {
        let (engine, user_id) = make_engine_with_account(100_000);
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
        let engine = RiskEngine::new();
        let user_id = 7i64;
        engine.initialize_account(user_id, 15_000);
        let engine = Arc::new(engine);
        let success = Arc::new(AtomicUsize::new(0));
        let failure = Arc::new(AtomicUsize::new(0));
        let handles: Vec<_> = (0..2)
            .map(|_| {
                let e = engine.clone();
                let s = success.clone();
                let f = failure.clone();
                std::thread::spawn(move || match e.check_and_reserve_margin(user_id, 10_000) {
                    Ok(_) => s.fetch_add(1, Ordering::Relaxed),
                    Err(_) => f.fetch_add(1, Ordering::Relaxed),
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(success.load(Ordering::Relaxed), 1);
        assert_eq!(failure.load(Ordering::Relaxed), 1);
        let state = engine.accounts.get(&user_id).unwrap();
        assert_eq!(state.available_margin, 5_000);
        assert_eq!(state.order_margin, 10_000);
    }

    #[test]
    fn liquidation_pending_account_rejected() {
        let engine = RiskEngine::new();
        let user_id = 99i64;
        engine.initialize_account(user_id, 100_000);
        if let Some(mut entry) = engine.accounts.get_mut(&user_id) {
            entry.status = RiskStatus::LiquidationPending;
        }
        assert!(engine.check_and_reserve_margin(user_id, 1_000).is_err());
    }

    // ── on_fill tests ──────────────────────────────────────────────────────────

    // BTC at $50,000 → price_ticks = 5_000_000
    // 0.1 BTC = 100_000 lots
    // notional = 5_000_000 * 100_000 / 1_000_000 = 500_000 cents = $5,000
    // initial_margin (10x) = 50_000 cents = $500
    const PRICE_TICKS: i64 = 5_000_000;
    const QTY_LOTS: i64 = 100_000;
    const MARGIN_CENTS: i64 = 50_000;

    fn setup_with_reserved(balance_cents: i64) -> (Arc<RiskEngine>, i64) {
        let (engine, uid) = make_engine_with_account(balance_cents);
        engine.check_and_reserve_margin(uid, MARGIN_CENTS).unwrap();
        (engine, uid)
    }

    #[test]
    fn on_fill_opens_long_position() {
        let (engine, uid) = setup_with_reserved(200_000);
        engine.on_fill(uid, btc_sym(), 0, PRICE_TICKS, QTY_LOTS, MARGIN_CENTS, NOTIONAL_SCALE, LEVERAGE, MAINT_BPS);

        let pos = engine.positions.get(&(uid, btc_sym())).unwrap();
        assert_eq!(pos.qty_lots, QTY_LOTS);
        assert_eq!(pos.entry_price_ticks, PRICE_TICKS);
        assert_eq!(pos.side, PositionSide::Long);
        assert_eq!(pos.initial_margin, MARGIN_CENTS);

        let acct = engine.accounts.get(&uid).unwrap();
        assert_eq!(acct.order_margin, 0);
        assert_eq!(acct.used_margin, MARGIN_CENTS);
    }

    #[test]
    fn on_fill_opens_short_position() {
        let (engine, uid) = setup_with_reserved(200_000);
        engine.on_fill(uid, btc_sym(), 1, PRICE_TICKS, QTY_LOTS, MARGIN_CENTS, NOTIONAL_SCALE, LEVERAGE, MAINT_BPS);

        let pos = engine.positions.get(&(uid, btc_sym())).unwrap();
        assert_eq!(pos.side, PositionSide::Short);
        assert_eq!(pos.qty_lots, QTY_LOTS);
    }

    #[test]
    fn on_fill_adds_to_existing_long_vwap() {
        let (engine, uid) = make_engine_with_account(500_000);
        engine.check_and_reserve_margin(uid, MARGIN_CENTS).unwrap();
        engine.on_fill(uid, btc_sym(), 0, PRICE_TICKS, QTY_LOTS, MARGIN_CENTS, NOTIONAL_SCALE, LEVERAGE, MAINT_BPS);

        // Second fill at $55,000 for same qty
        let price2 = 5_500_000i64;
        engine.check_and_reserve_margin(uid, MARGIN_CENTS).unwrap();
        engine.on_fill(uid, btc_sym(), 0, price2, QTY_LOTS, MARGIN_CENTS, NOTIONAL_SCALE, LEVERAGE, MAINT_BPS);

        let pos = engine.positions.get(&(uid, btc_sym())).unwrap();
        assert_eq!(pos.qty_lots, QTY_LOTS * 2);
        // VWAP = (5_000_000 * 100_000 + 5_500_000 * 100_000) / 200_000 = 5_250_000
        assert_eq!(pos.entry_price_ticks, 5_250_000);
        assert_eq!(pos.initial_margin, MARGIN_CENTS * 2);
    }

    #[test]
    fn on_fill_closes_long_fully_releases_margin_and_credits_pnl() {
        let (engine, uid) = make_engine_with_account(500_000);
        // Open long at $50,000
        engine.check_and_reserve_margin(uid, MARGIN_CENTS).unwrap();
        engine.on_fill(uid, btc_sym(), 0, PRICE_TICKS, QTY_LOTS, MARGIN_CENTS, NOTIONAL_SCALE, LEVERAGE, MAINT_BPS);

        // Close at $55,000: profit = ($55,000 - $50,000) * 0.1 BTC = $500 = 50_000 cents
        let close_price = 5_500_000i64;
        engine.check_and_reserve_margin(uid, MARGIN_CENTS).unwrap();
        engine.on_fill(uid, btc_sym(), 1, close_price, QTY_LOTS, MARGIN_CENTS, NOTIONAL_SCALE, LEVERAGE, MAINT_BPS);

        // Position should be gone
        assert!(engine.positions.get(&(uid, btc_sym())).is_none());

        let acct = engine.accounts.get(&uid).unwrap();
        assert_eq!(acct.used_margin, 0);
        assert_eq!(acct.order_margin, 0);
        // open:  500_000 - 50_000 (reserve) = 450_000 available, 50_000 order_margin
        //        on_fill: order_margin → used_margin, available unchanged at 450_000
        // close: 450_000 - 50_000 (reserve) = 400_000 available, 50_000 order_margin
        //        on_fill: +50_000 (close reservation) + 50_000 (used_margin) + 50_000 (profit) = 550_000
        assert_eq!(acct.available_margin, 550_000);
    }

    #[test]
    fn on_fill_partial_close_reduces_position() {
        let (engine, uid) = make_engine_with_account(500_000);
        engine.check_and_reserve_margin(uid, MARGIN_CENTS).unwrap();
        engine.on_fill(uid, btc_sym(), 0, PRICE_TICKS, QTY_LOTS, MARGIN_CENTS, NOTIONAL_SCALE, LEVERAGE, MAINT_BPS);

        // Close half at $50,000 (no PnL)
        let half_qty = QTY_LOTS / 2;
        let half_margin = MARGIN_CENTS / 2;
        engine.check_and_reserve_margin(uid, half_margin).unwrap();
        engine.on_fill(uid, btc_sym(), 1, PRICE_TICKS, half_qty, half_margin, NOTIONAL_SCALE, LEVERAGE, MAINT_BPS);

        let pos = engine.positions.get(&(uid, btc_sym())).unwrap();
        assert_eq!(pos.qty_lots, half_qty);
        assert_eq!(pos.side, PositionSide::Long);

        let acct = engine.accounts.get(&uid).unwrap();
        assert_eq!(acct.used_margin, half_margin); // only half still used
        assert_eq!(acct.order_margin, 0);
    }

    #[test]
    fn on_fill_symbol_position_index_maintained() {
        let (engine, uid) = setup_with_reserved(200_000);
        engine.on_fill(uid, btc_sym(), 0, PRICE_TICKS, QTY_LOTS, MARGIN_CENTS, NOTIONAL_SCALE, LEVERAGE, MAINT_BPS);

        // Should be in index
        let idx = engine.symbol_position_index.get(&btc_sym()).unwrap();
        assert!(idx.contains(&uid));
        drop(idx);

        // Close: index entry removed
        engine.check_and_reserve_margin(uid, MARGIN_CENTS).unwrap();
        engine.on_fill(uid, btc_sym(), 1, PRICE_TICKS, QTY_LOTS, MARGIN_CENTS, NOTIONAL_SCALE, LEVERAGE, MAINT_BPS);

        let idx = engine.symbol_position_index.get(&btc_sym());
        assert!(idx.map(|v| !v.contains(&uid)).unwrap_or(true));
    }

    #[test]
    fn on_fill_liquidation_price_set_on_open() {
        let (engine, uid) = setup_with_reserved(200_000);
        engine.on_fill(uid, btc_sym(), 0, PRICE_TICKS, QTY_LOTS, MARGIN_CENTS, NOTIONAL_SCALE, LEVERAGE, MAINT_BPS);
        let pos = engine.positions.get(&(uid, btc_sym())).unwrap();
        // liq_price_long = entry * (10*10000 - 10000 + 50) / (10*10000) = 5_000_000 * 90050 / 100000 = 4_502_500
        assert_eq!(pos.liquidation_price_ticks, 4_502_500);
        assert_eq!(pos.bankruptcy_price_ticks, 4_500_000);
    }
}
