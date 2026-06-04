use dashmap::DashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use super::calc;
use super::types::{AccountRiskState, PositionRiskState, PositionSide, RiskStatus};

pub struct RiskEngine {
    pub accounts: DashMap<i64, AccountRiskState>,
    pub positions: DashMap<(i64, [u8; 16]), PositionRiskState>,
    pub mark_prices: DashMap<[u8; 16], i64>,
    // symbol → user_ids with open positions (for mark-price scan in Phase 3)
    pub symbol_position_index: DashMap<[u8; 16], Vec<i64>>,
    /// Insurance fund balance in cents.  Positive = surplus absorbed from profitable
    /// liquidations; negative = fund debt from socialised losses.
    pub insurance_fund_cents: AtomicI64,
}

impl RiskEngine {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            accounts: DashMap::new(),
            positions: DashMap::new(),
            mark_prices: DashMap::new(),
            symbol_position_index: DashMap::new(),
            insurance_fund_cents: AtomicI64::new(0),
        })
    }

    /// Returns the current insurance fund balance in cents.
    pub fn insurance_fund(&self) -> i64 {
        self.insurance_fund_cents.load(Ordering::Relaxed)
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
        liq_price_ticks: i64, // non-zero for forced-liquidation: user settled here, spread → insurance fund
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

        // For liquidation orders, the user is settled at liq_price_ticks (not the actual
        // fill price). The spread between fill and liq price is exchange revenue.
        let settlement_price_ticks = if liq_price_ticks != 0 {
            liq_price_ticks
        } else {
            fill_price_ticks
        };

        // Compute new position state without holding shard locks.
        let (new_pos, released_used_margin, realized_pnl_cents) = {
            let existing = self.positions.get(&key);
            compute_position_update(
                existing.as_deref(),
                user_id,
                symbol,
                fill_side,
                settlement_price_ticks,
                fill_qty_lots,
                fill_margin_cents,
                notional_scale,
                leverage,
                maintenance_rate_bps,
            )
        };

        // Apply position update.
        match new_pos {
            Some(ref p) => {
                self.positions.insert(key, p.clone());
            }
            None => {
                self.positions.remove(&key);
            }
        }

        // Insurance fund accounting for liquidation closes.
        // When liq_price_ticks is set: exchange revenue = |fill - liq| × qty / scale.
        // When the account is Liquidating without liq_price: fund absorbs the full realized_pnl
        // (legacy path, should not occur in normal operation).
        let insurance_delta: i64 = if released_used_margin > 0 {
            if liq_price_ticks != 0 {
                // Exchange pockets the spread between actual fill and liquidation price.
                // sell close (long liq): fill > liq → (fill - liq) * qty / scale > 0
                // buy  close (short liq): liq > fill → (liq - fill) * qty / scale > 0
                let sign: i64 = if order_side == 1 { 1 } else { -1 }; // sell=+1, buy=-1
                ((fill_price_ticks - liq_price_ticks) as i128 * sign as i128
                    * fill_qty_lots as i128
                    / notional_scale as i128) as i64
            } else {
                let is_liquidating = self
                    .accounts
                    .get(&user_id)
                    .map(|a| a.status == RiskStatus::Liquidating)
                    .unwrap_or(false);
                if is_liquidating {
                    realized_pnl_cents
                } else {
                    0
                }
            }
        } else {
            0
        };
        if insurance_delta != 0 {
            self.insurance_fund_cents
                .fetch_add(insurance_delta, Ordering::Relaxed);
        }

        // Apply account update: move fill_margin from order_margin → used_margin (opening),
        // or release used_margin and credit realized PnL (closing).
        //
        // Flip case: closed old position AND opened a new one on the opposite side.
        // The flip's new_pos.side == fill_side (e.g. closed long via sell → new Short pos).
        // For a simple partial close, new_pos.side is still the old side → flip_margin = 0.
        let flip_margin = match &new_pos {
            Some(p) if p.side == fill_side => p.initial_margin,
            _ => 0,
        };
        if let Some(mut acct) = self.accounts.get_mut(&user_id) {
            acct.order_margin = (acct.order_margin - fill_margin_cents).max(0);
            if released_used_margin > 0 {
                // Closing (or closing+flipping): release old position's used_margin and credit pnl.
                acct.used_margin = (acct.used_margin - released_used_margin).max(0);
                // For flip: flip_margin portion stays in used_margin, not available.
                acct.used_margin += flip_margin;
                acct.available_margin = (acct.available_margin
                    + fill_margin_cents
                    + released_used_margin
                    + realized_pnl_cents
                    - flip_margin)
                    .max(0);
                // After forced liquidation close, let run_risk_tick re-evaluate status.
                if acct.status == RiskStatus::Liquidating {
                    acct.status = RiskStatus::Normal;
                }
            } else {
                // Opening: move order_margin → used_margin.
                acct.used_margin += fill_margin_cents;
            }
            acct.equity = acct.available_margin
                + acct.order_margin
                + acct.used_margin
                + acct.unrealized_pnl;
        }

        // Seed mark_prices with entry price the first time any position opens.
        self.mark_prices.entry(symbol).or_insert(fill_price_ticks);

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

        // Recompute account-level maintenance_margin as sum of all open positions.
        self.recompute_account_maintenance(user_id);
    }

    /// Recomputes `account.maintenance_margin` as the sum of all open positions'
    /// maintenance_margin.  Must be called after any position change.
    fn recompute_account_maintenance(&self, user_id: i64) {
        let total: i64 = self
            .positions
            .iter()
            .filter(|e| e.key().0 == user_id)
            .map(|e| e.value().maintenance_margin)
            .sum();
        if let Some(mut acct) = self.accounts.get_mut(&user_id) {
            acct.maintenance_margin = total;
        }
    }

    // -------------------------------------------------------------------------
    // Phase 3: mark price + incremental unrealized PnL + risk tick
    // -------------------------------------------------------------------------

    /// Update the EWMA mark price for a symbol and recompute unrealized PnL
    /// for all users with open positions.  Called from the WS Aeron spin thread
    /// every time a depth snapshot arrives (~10ms cadence).
    ///
    /// EWMA: mark = 0.1 × new_price + 0.9 × old_mark  (α=0.1)
    pub fn update_mark_price(&self, symbol: [u8; 16], new_price_ticks: i64, notional_scale: i64) {
        if new_price_ticks <= 0 {
            return;
        }

        // Compute the new EWMA mark price.
        let mark_ticks = {
            let old = self
                .mark_prices
                .get(&symbol)
                .map(|v| *v)
                .unwrap_or(new_price_ticks);
            // α=0.1 in integer: (1*new + 9*old) / 10
            (new_price_ticks + 9 * old) / 10
        };
        self.mark_prices.insert(symbol, mark_ticks);

        // Update unrealized PnL for every user with a position in this symbol.
        let user_ids: Vec<i64> = self
            .symbol_position_index
            .get(&symbol)
            .map(|v| v.clone())
            .unwrap_or_default();

        for uid in user_ids {
            let key = (uid, symbol);
            // Update mark price on this position.
            {
                let Some(mut pos) = self.positions.get_mut(&key) else {
                    continue;
                };
                pos.mark_price_ticks = mark_ticks;
            }
            // Sum unrealized PnL across all open positions for this user so
            // multi-symbol accounts accumulate correctly instead of overwriting.
            let total_upnl: i64 = self
                .positions
                .iter()
                .filter(|e| e.key().0 == uid)
                .map(|e| {
                    let p = e.value();
                    calc::calc_unrealized_pnl_cents(
                        p.side,
                        p.qty_lots,
                        p.entry_price_ticks,
                        p.mark_price_ticks,
                        notional_scale,
                    )
                })
                .sum();
            if let Some(mut acct) = self.accounts.get_mut(&uid) {
                acct.unrealized_pnl = total_upnl;
                acct.equity = acct.available_margin
                    + acct.order_margin
                    + acct.used_margin
                    + acct.unrealized_pnl;
            }
        }
    }

    /// Called every ~10ms from a dedicated timer task.  Scans open-position users,
    /// updates RiskStatus, and returns a vec of positions that need liquidation.
    ///
    /// Two-pass design to avoid nested DashMap shard lock acquisition:
    ///   Pass 1 (positions.iter + accounts.get): compute new statuses for users with exposure
    ///            that transition to LiquidationPending.
    ///   Pass 2 (positions.iter — shared read): collect LiquidationEvents for those UIDs.
    ///   Pass 3 (accounts.get_mut — per-key write): apply status updates.
    pub fn run_risk_tick(&self) -> Vec<super::types::LiquidationEvent> {
        use super::types::RiskStatus;

        // Only accounts with open positions can breach maintenance margin.
        // Scanning every cached account every 10ms regresses 40K/100K WS tests even
        // when the test only places resting orders and no position exists.
        let mut risk_user_ids: Vec<i64> = self.positions.iter().map(|e| e.key().0).collect();
        if risk_user_ids.is_empty() {
            return Vec::new();
        }
        risk_user_ids.sort_unstable();
        risk_user_ids.dedup();

        // Pass 1: read exposed accounts, compute new status for each.
        struct Update {
            user_id: i64,
            old_status: RiskStatus,
            new_status: RiskStatus,
        }

        let updates: Vec<Update> = risk_user_ids
            .iter()
            .filter_map(|user_id| self.accounts.get(user_id))
            .filter_map(|entry| {
                let acct = entry.value();
                if matches!(
                    acct.status,
                    RiskStatus::Liquidating | RiskStatus::Liquidated | RiskStatus::Bankruptcy
                ) {
                    return None;
                }
                let equity = acct.equity;
                let used_margin = acct.used_margin;
                let maintenance_margin = acct.maintenance_margin;

                let new_status = if used_margin == 0 {
                    RiskStatus::Normal
                } else if equity <= 0 {
                    RiskStatus::Bankruptcy
                } else if equity <= maintenance_margin {
                    RiskStatus::LiquidationPending
                } else if equity * 2 < 3 * maintenance_margin {
                    RiskStatus::MarginCall
                } else {
                    RiskStatus::Normal
                };

                Some(Update { user_id: acct.user_id, old_status: acct.status, new_status })
            })
            .collect();

        // Pass 2: scan positions for UIDs newly entering LiquidationPending.
        let liq_uids: Vec<i64> = updates
            .iter()
            .filter(|u| {
                u.new_status == RiskStatus::LiquidationPending
                    && u.old_status != RiskStatus::LiquidationPending
            })
            .map(|u| u.user_id)
            .collect();

        let to_liquidate: Vec<super::types::LiquidationEvent> = if liq_uids.is_empty() {
            Vec::new()
        } else {
            let mut liq_uids = liq_uids;
            liq_uids.sort_unstable();
            self.positions
                .iter()
                .filter(|e| liq_uids.binary_search(&e.key().0).is_ok())
                .map(|e| super::types::LiquidationEvent {
                    user_id: e.key().0,
                    symbol: e.key().1,
                    side: e.value().side,
                    qty_lots: e.value().qty_lots,
                    liq_price_ticks: e.value().liquidation_price_ticks,
                })
                .collect()
        };

        // Pass 3: apply status updates one at a time (no iter_mut).
        // Compare-and-swap: only update if status hasn't been changed by on_fill
        // (e.g. Liquidating set by the tick task) between Pass 1 and Pass 3.
        for u in &updates {
            if let Some(mut acct) = self.accounts.get_mut(&u.user_id) {
                if acct.status == u.old_status {
                    acct.status = u.new_status;
                }
            }
        }

        to_liquidate
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
        engine.on_fill(uid, btc_sym(), 0, PRICE_TICKS, QTY_LOTS, MARGIN_CENTS, NOTIONAL_SCALE, LEVERAGE, MAINT_BPS, 0);

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
        engine.on_fill(uid, btc_sym(), 1, PRICE_TICKS, QTY_LOTS, MARGIN_CENTS, NOTIONAL_SCALE, LEVERAGE, MAINT_BPS, 0);

        let pos = engine.positions.get(&(uid, btc_sym())).unwrap();
        assert_eq!(pos.side, PositionSide::Short);
        assert_eq!(pos.qty_lots, QTY_LOTS);
    }

    #[test]
    fn on_fill_adds_to_existing_long_vwap() {
        let (engine, uid) = make_engine_with_account(500_000);
        engine.check_and_reserve_margin(uid, MARGIN_CENTS).unwrap();
        engine.on_fill(uid, btc_sym(), 0, PRICE_TICKS, QTY_LOTS, MARGIN_CENTS, NOTIONAL_SCALE, LEVERAGE, MAINT_BPS, 0);

        // Second fill at $55,000 for same qty
        let price2 = 5_500_000i64;
        engine.check_and_reserve_margin(uid, MARGIN_CENTS).unwrap();
        engine.on_fill(uid, btc_sym(), 0, price2, QTY_LOTS, MARGIN_CENTS, NOTIONAL_SCALE, LEVERAGE, MAINT_BPS, 0);

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
        engine.on_fill(uid, btc_sym(), 0, PRICE_TICKS, QTY_LOTS, MARGIN_CENTS, NOTIONAL_SCALE, LEVERAGE, MAINT_BPS, 0);

        // Close at $55,000: profit = ($55,000 - $50,000) * 0.1 BTC = $500 = 50_000 cents
        let close_price = 5_500_000i64;
        engine.check_and_reserve_margin(uid, MARGIN_CENTS).unwrap();
        engine.on_fill(uid, btc_sym(), 1, close_price, QTY_LOTS, MARGIN_CENTS, NOTIONAL_SCALE, LEVERAGE, MAINT_BPS, 0);

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
        engine.on_fill(uid, btc_sym(), 0, PRICE_TICKS, QTY_LOTS, MARGIN_CENTS, NOTIONAL_SCALE, LEVERAGE, MAINT_BPS, 0);

        // Close half at $50,000 (no PnL)
        let half_qty = QTY_LOTS / 2;
        let half_margin = MARGIN_CENTS / 2;
        engine.check_and_reserve_margin(uid, half_margin).unwrap();
        engine.on_fill(uid, btc_sym(), 1, PRICE_TICKS, half_qty, half_margin, NOTIONAL_SCALE, LEVERAGE, MAINT_BPS, 0);

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
        engine.on_fill(uid, btc_sym(), 0, PRICE_TICKS, QTY_LOTS, MARGIN_CENTS, NOTIONAL_SCALE, LEVERAGE, MAINT_BPS, 0);

        // Should be in index
        let idx = engine.symbol_position_index.get(&btc_sym()).unwrap();
        assert!(idx.contains(&uid));
        drop(idx);

        // Close: index entry removed
        engine.check_and_reserve_margin(uid, MARGIN_CENTS).unwrap();
        engine.on_fill(uid, btc_sym(), 1, PRICE_TICKS, QTY_LOTS, MARGIN_CENTS, NOTIONAL_SCALE, LEVERAGE, MAINT_BPS, 0);

        let idx = engine.symbol_position_index.get(&btc_sym());
        assert!(idx.map(|v| !v.contains(&uid)).unwrap_or(true));
    }

    #[test]
    fn on_fill_liquidation_price_set_on_open() {
        let (engine, uid) = setup_with_reserved(200_000);
        engine.on_fill(uid, btc_sym(), 0, PRICE_TICKS, QTY_LOTS, MARGIN_CENTS, NOTIONAL_SCALE, LEVERAGE, MAINT_BPS, 0);
        let pos = engine.positions.get(&(uid, btc_sym())).unwrap();
        // liq_price_long = entry * (10*10000 - 10000 + 50) / (10*10000) = 5_000_000 * 90050 / 100000 = 4_502_500
        assert_eq!(pos.liquidation_price_ticks, 4_502_500);
        assert_eq!(pos.bankruptcy_price_ticks, 4_500_000);
    }

    // ── Phase 3: mark price + unrealized PnL + risk tick ─────────────────────

    #[test]
    fn update_mark_price_sets_ewma_and_upnl() {
        let (engine, uid) = setup_with_reserved(500_000);
        // Open long: 0.1 BTC at $50,000, margin $500
        engine.on_fill(uid, btc_sym(), 0, PRICE_TICKS, QTY_LOTS, MARGIN_CENTS, NOTIONAL_SCALE, LEVERAGE, MAINT_BPS, 0);

        // Mark price jumps to $51,000 (price_ticks = 5_100_000).
        // First update: EWMA = (5_100_000 + 9 * 5_000_000) / 10 = 5_010_000
        engine.update_mark_price(btc_sym(), 5_100_000, NOTIONAL_SCALE);

        let pos = engine.positions.get(&(uid, btc_sym())).unwrap();
        assert_eq!(pos.mark_price_ticks, 5_010_000);

        // unrealized_pnl = (5_010_000 - 5_000_000) * 100_000 / 1_000_000 = 1_000 cents = $10
        let acct = engine.accounts.get(&uid).unwrap();
        assert_eq!(acct.unrealized_pnl, 1_000);
        // equity = available(450k - nothing since open moved to used) + used(50k) + upnl(1k) + order(0)
        // At open: available = 500_000 - 50_000 = 450_000, used = 50_000
        assert_eq!(acct.equity, 450_000 + 50_000 + 1_000);
    }

    #[test]
    fn update_mark_price_ignores_zero() {
        let (engine, uid) = setup_with_reserved(200_000);
        engine.on_fill(uid, btc_sym(), 0, PRICE_TICKS, QTY_LOTS, MARGIN_CENTS, NOTIONAL_SCALE, LEVERAGE, MAINT_BPS, 0);
        engine.update_mark_price(btc_sym(), 0, NOTIONAL_SCALE);
        let pos = engine.positions.get(&(uid, btc_sym())).unwrap();
        assert_eq!(pos.mark_price_ticks, PRICE_TICKS); // unchanged
    }

    #[test]
    fn risk_tick_normal_when_margin_healthy() {
        let (engine, uid) = setup_with_reserved(500_000);
        engine.on_fill(uid, btc_sym(), 0, PRICE_TICKS, QTY_LOTS, MARGIN_CENTS, NOTIONAL_SCALE, LEVERAGE, MAINT_BPS, 0);
        let events = engine.run_risk_tick();
        assert!(events.is_empty());
        let acct = engine.accounts.get(&uid).unwrap();
        assert_eq!(acct.status, RiskStatus::Normal);
    }

    #[test]
    fn risk_tick_ignores_accounts_without_positions() {
        let engine = RiskEngine::new();
        for user_id in 1..=1_000 {
            engine.initialize_account(user_id, 100_000);
            if let Some(mut acct) = engine.accounts.get_mut(&user_id) {
                acct.equity = 0;
                acct.maintenance_margin = 1;
            }
        }

        let events = engine.run_risk_tick();
        assert!(events.is_empty());
        assert!(engine
            .accounts
            .iter()
            .all(|acct| acct.status == RiskStatus::Normal));
    }

    #[test]
    fn account_maintenance_margin_aggregated_from_positions() {
        let (engine, uid) = setup_with_reserved(500_000);
        // notional = 5_000_000 * 100_000 / 1_000_000 = 500_000 cents
        // maintenance_margin = 500_000 * 50 / 10_000 = 2_500 cents
        engine.on_fill(uid, btc_sym(), 0, PRICE_TICKS, QTY_LOTS, MARGIN_CENTS, NOTIONAL_SCALE, LEVERAGE, MAINT_BPS, 0);
        let acct = engine.accounts.get(&uid).unwrap();
        assert_eq!(acct.maintenance_margin, 2_500);
    }

    #[test]
    fn risk_tick_triggers_liquidation_pending_when_equity_below_maintenance() {
        let (engine, uid) = make_engine_with_account(500_000);
        engine.check_and_reserve_margin(uid, MARGIN_CENTS).unwrap();
        engine.on_fill(uid, btc_sym(), 0, PRICE_TICKS, QTY_LOTS, MARGIN_CENTS, NOTIONAL_SCALE, LEVERAGE, MAINT_BPS, 0);

        // Maintenance margin at open = 500_000 * 50 / 10_000 = 2_500 cents ($25).
        // Force equity below maintenance via unrealized PnL.
        // equity = available(450_000) + used(50_000) + upnl = 2_499 → upnl = -497_501
        if let Some(mut acct) = engine.accounts.get_mut(&uid) {
            acct.unrealized_pnl = -497_501;
            acct.equity = acct.available_margin + acct.order_margin + acct.used_margin + acct.unrealized_pnl;
        }

        let events = engine.run_risk_tick();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].user_id, uid);

        let acct = engine.accounts.get(&uid).unwrap();
        assert_eq!(acct.status, RiskStatus::LiquidationPending);
    }

    // ── Integration: full mark-price → liquidation pipeline ──────────────────

    /// Full pipeline: update_mark_price drives equity below maintenance → run_risk_tick
    /// emits LiquidationEvent → second tick emits nothing (no duplicate events).
    ///
    /// Uses a tight account so a ~10% EWMA-smoothed price drop triggers liquidation:
    ///   balance=52_600 cents, margin=50_000, available=2_600, maintenance=2_500
    ///   After 20 calls to update_mark_price($44,000):
    ///     mark converges toward 44_000 ticks; equity drops below maintenance (2_500).
    #[test]
    fn integration_mark_price_triggers_liquidation() {
        let (engine, uid) = make_engine_with_account(52_600);
        engine.check_and_reserve_margin(uid, MARGIN_CENTS).unwrap();
        engine.on_fill(uid, btc_sym(), 0, PRICE_TICKS, QTY_LOTS, MARGIN_CENTS, NOTIONAL_SCALE, LEVERAGE, MAINT_BPS, 0);
        // After open: available=2_600, used=50_000, maintenance=2_500
        // mark converges to $44,000: after ~18 calls equity drops below maintenance.
        // Each call: mark = (4_400_000 + 9 * old) / 10  (EWMA α=0.1)

        let mut triggered = false;
        for _ in 0..30 {
            engine.update_mark_price(btc_sym(), 4_400_000, NOTIONAL_SCALE);
            let acct = engine.accounts.get(&uid).unwrap();
            if acct.equity <= acct.maintenance_margin {
                triggered = true;
                break;
            }
        }
        assert!(triggered, "equity should drop below maintenance after repeated mark price updates");

        let events = engine.run_risk_tick();
        assert_eq!(events.len(), 1, "should emit one liquidation event");
        assert_eq!(events[0].user_id, uid);
        assert_eq!(events[0].qty_lots, QTY_LOTS);

        // Second tick: already LiquidationPending — no repeat events.
        let events2 = engine.run_risk_tick();
        assert!(events2.is_empty(), "no duplicate liquidation events");
    }

    /// With healthy equity, run_risk_tick emits nothing and account stays Normal.
    #[test]
    fn integration_healthy_account_no_liquidation_events() {
        let (engine, uid) = setup_with_reserved(1_000_000);
        engine.on_fill(uid, btc_sym(), 0, PRICE_TICKS, QTY_LOTS, MARGIN_CENTS, NOTIONAL_SCALE, LEVERAGE, MAINT_BPS, 0);

        // Mark price moves +1% — well within margin.
        engine.update_mark_price(btc_sym(), PRICE_TICKS + 50_000, NOTIONAL_SCALE);

        for _ in 0..5 {
            let events = engine.run_risk_tick();
            assert!(events.is_empty());
        }
        let acct = engine.accounts.get(&uid).unwrap();
        assert_eq!(acct.status, RiskStatus::Normal);
    }

    // ── Phase 6: insurance fund ───────────────────────────────────────────────

    /// When a Liquidating account closes a long position at a PROFIT vs entry
    /// (liquidated at a price better than entry), the surplus goes to the insurance fund.
    ///
    /// Note: the system-generated liquidation order does NOT call check_and_reserve_margin
    /// (the account is Liquidating so that would be blocked). fill_margin_cents = 0.
    #[test]
    fn insurance_fund_gains_on_profitable_liquidation_close() {
        let (engine, uid) = setup_with_reserved(500_000);
        // Open long at $50,000; margin = 50_000 cents
        engine.on_fill(uid, btc_sym(), 0, PRICE_TICKS, QTY_LOTS, MARGIN_CENTS, NOTIONAL_SCALE, LEVERAGE, MAINT_BPS, 0);

        // Simulate the account being set to Liquidating (done by the tick task).
        if let Some(mut acct) = engine.accounts.get_mut(&uid) {
            acct.status = RiskStatus::Liquidating;
        }

        // System close at $51,000 (profit $100 = 10_000 cents).
        // No margin was pre-reserved (system bypass), so fill_margin_cents = 0.
        let close_price_ticks = 5_100_000i64;
        engine.on_fill(uid, btc_sym(), 1, close_price_ticks, QTY_LOTS, 0, NOTIONAL_SCALE, LEVERAGE, MAINT_BPS, 0);

        // After the forced close, on_fill resets status to Normal so run_risk_tick
        // can re-evaluate on the next tick (H3 fix: Liquidated was a terminal sink).
        let acct = engine.accounts.get(&uid).unwrap();
        assert_eq!(acct.status, RiskStatus::Normal);

        // Insurance fund should have gained the realized pnl:
        //   pnl = (5_100_000 − 5_000_000) × 100_000 / 1_000_000 = 10_000 cents
        //   (bkrpt_pnl = 0 per Phase 6 TODO, so fund gains full realized_pnl)
        assert_eq!(engine.insurance_fund(), 10_000);
    }

    /// When liq_price_ticks is set, the spread between actual fill and liq_price
    /// flows to the insurance fund.  The user is settled at liq_price (worse for them).
    ///
    /// Example (long liq, sell close):
    ///   entry = $50,000  liq_price = $49,000  actual fill = $49,800
    ///   User pnl  = (49,000 − 50,000) × 0.1 / 1 = −$100  → loss of 10_000 cents
    ///   Exchange  = (49,800 − 49,000) × 0.1  = $80  → 8_000 cents to insurance fund
    #[test]
    fn liq_price_spread_flows_to_insurance_fund() {
        const SCALE: i64 = 1_000_000; // BTC: price_ticks/tick * lots / 1_000_000 = cents
        let (engine, uid) = setup_with_reserved(2_000_000); // $20,000 initial balance

        // Open long BTC at $50,000 (entry_price_ticks = 5_000_000, qty = 0.1 BTC = 100_000 lots)
        let entry_ticks = 5_000_000i64; // $50,000 at $0.01/tick
        let qty_lots = 100_000i64;       // 0.1 BTC at 1e-6 lot size
        let open_notional = entry_ticks * qty_lots / SCALE; // = 500_000 cents = $5,000
        let open_margin = open_notional / 10; // 10x → 50_000 cents = $500
        engine.check_and_reserve_margin(uid, open_margin).unwrap();
        engine.on_fill(uid, btc_sym(), 0, entry_ticks, qty_lots, open_margin, SCALE, 10, 50, 0);

        // Risk tick decides to liquidate; liq_price = $49,000 (below mark, more aggressive)
        let liq_ticks = 4_900_000i64; // $49,000
        // Actual market fill comes in at $49,800 (better for the buyer / worse for liq engine)
        let fill_ticks = 4_980_000i64; // $49,800
        if let Some(mut acct) = engine.accounts.get_mut(&uid) {
            acct.status = RiskStatus::Liquidating;
        }

        // System close: sell order at liq_price (IOC limit), fills at $49,800.
        engine.on_fill(uid, btc_sym(), 1, fill_ticks, qty_lots, 0, SCALE, 10, 50, liq_ticks);

        // Account settled at liq_price ($49,000), not fill price ($49,800).
        // After forced close, status → Normal so run_risk_tick can re-evaluate (H3 fix).
        let acct = engine.accounts.get(&uid).unwrap();
        assert_eq!(acct.status, RiskStatus::Normal);

        // Exchange revenue = (fill − liq) × qty / scale (sell close, sign=+1)
        //   = (4_980_000 − 4_900_000) × 100_000 / 1_000_000
        //   = 80_000 × 100_000 / 1_000_000 = 8_000_000_000 / 1_000_000 = 8_000 cents = $80
        assert_eq!(engine.insurance_fund(), 8_000);
    }

    /// For non-Liquidating closes, insurance fund is not touched.
    #[test]
    fn insurance_fund_unchanged_on_normal_close() {
        let (engine, uid) = setup_with_reserved(500_000);
        engine.on_fill(uid, btc_sym(), 0, PRICE_TICKS, QTY_LOTS, MARGIN_CENTS, NOTIONAL_SCALE, LEVERAGE, MAINT_BPS, 0);

        // Normal user close at same price (no PnL, account stays Normal).
        engine.check_and_reserve_margin(uid, MARGIN_CENTS).unwrap();
        engine.on_fill(uid, btc_sym(), 1, PRICE_TICKS, QTY_LOTS, MARGIN_CENTS, NOTIONAL_SCALE, LEVERAGE, MAINT_BPS, 0);

        assert_eq!(engine.insurance_fund(), 0);
    }
}
