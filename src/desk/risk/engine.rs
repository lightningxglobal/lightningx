use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use super::calc;
use super::types::{AccountRiskState, PositionRiskState, PositionSide, RiskStatus};

pub struct RiskEngine {
    pub accounts: DashMap<i64, AccountRiskState>,
    pub positions: DashMap<(i64, [u8; 16]), PositionRiskState>,
    pub mark_prices: DashMap<[u8; 16], i64>,
    // symbol → user_ids with open positions (for mark-price scan in Phase 3).
    // HashSet for O(1) insert/remove in on_fill (hot recv-spin path).
    pub symbol_position_index: DashMap<[u8; 16], std::collections::HashSet<i64>>,
    /// Gross open interest per symbol in lots: Σ qty_lots over every open
    /// position. Maintained incrementally by on_fill (O(1)); read by the
    /// order-entry OI cap check.
    pub symbol_oi_lots: DashMap<[u8; 16], AtomicI64>,
    /// Insurance fund balance in cents.  Positive = surplus absorbed from profitable
    /// liquidations; negative = fund debt from socialised losses.
    pub insurance_fund_atoms: AtomicI64,
}

impl RiskEngine {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            accounts: DashMap::new(),
            positions: DashMap::new(),
            mark_prices: DashMap::new(),
            symbol_position_index: DashMap::new(),
            symbol_oi_lots: DashMap::new(),
            insurance_fund_atoms: AtomicI64::new(0),
        })
    }

    /// Returns the current insurance fund balance in cents.
    pub fn insurance_fund(&self) -> i64 {
        self.insurance_fund_atoms.load(Ordering::Relaxed)
    }

    pub fn initialize_account(&self, user_id: i64, usdt_balance_atoms: i64) {
        self.accounts
            .insert(user_id, AccountRiskState::new(user_id, usdt_balance_atoms));
    }

    pub fn account_count(&self) -> usize {
        self.accounts.len()
    }

    pub fn mark_price_ticks(&self, symbol: &[u8; 16]) -> Option<i64> {
        self.mark_prices.get(symbol).map(|v| *v)
    }

    pub fn force_mark_price_for_test(&self, symbol: [u8; 16], price_ticks: i64) {
        self.mark_prices.insert(symbol, price_ticks);
    }

    pub fn set_account_status(&self, user_id: i64, status: RiskStatus) -> bool {
        let Some(mut acct) = self.accounts.get_mut(&user_id) else {
            return false;
        };
        acct.status = status;
        true
    }

    pub fn set_account_status_if(
        &self,
        user_id: i64,
        expected: RiskStatus,
        status: RiskStatus,
    ) -> bool {
        let Some(mut acct) = self.accounts.get_mut(&user_id) else {
            return false;
        };
        if acct.status != expected {
            return false;
        }
        acct.status = status;
        true
    }

    /// Hot path: lock-free reserve using CAS on `available_margin`.
    /// Uses `accounts.get()` (shared shard read-lock) so concurrent
    /// `release_order_margin` calls on other users in the same shard never block.
    /// Gross open interest for a symbol in lots. O(1).
    pub fn symbol_open_interest_lots(&self, symbol: &[u8; 16]) -> i64 {
        self.symbol_oi_lots
            .get(symbol)
            .map(|v| v.load(Ordering::Relaxed).max(0))
            .unwrap_or(0)
    }

    /// Per-user-per-symbol position cap. `max_position_lots == 0` disables.
    /// O(1): one DashMap read. Conservative: counts the existing position
    /// plus the full new order quantity (a closing order may be rejected
    /// near the cap — acceptable, the user can close in smaller clips).
    pub fn check_position_limit(
        &self,
        user_id: i64,
        symbol: &[u8; 16],
        additional_lots: i64,
        max_position_lots: i64,
    ) -> Result<(), &'static str> {
        if max_position_lots <= 0 {
            return Ok(());
        }
        let current = self
            .positions
            .get(&(user_id, *symbol))
            .map(|p| p.qty_lots)
            .unwrap_or(0);
        if current.saturating_add(additional_lots) > max_position_lots {
            return Err("Position limit exceeded");
        }
        Ok(())
    }

    /// Market-wide gross open interest cap for a symbol.
    /// `max_oi_lots == 0` disables. O(1).
    pub fn check_symbol_oi_limit(
        &self,
        symbol: &[u8; 16],
        additional_lots: i64,
        max_oi_lots: i64,
    ) -> Result<(), &'static str> {
        if max_oi_lots <= 0 {
            return Ok(());
        }
        if self
            .symbol_open_interest_lots(symbol)
            .saturating_add(additional_lots)
            > max_oi_lots
        {
            return Err("Symbol open interest limit exceeded");
        }
        Ok(())
    }

    pub fn check_and_reserve_margin(
        &self,
        user_id: i64,
        initial_margin_atoms: i64,
    ) -> Result<i64, &'static str> {
        use std::sync::atomic::Ordering;
        let entry = self.accounts.get(&user_id).ok_or("Account not found")?;
        if !entry.can_place_order() {
            return Err("Account in liquidation");
        }
        loop {
            let cur = entry.available_margin.load(Ordering::Relaxed);
            if cur < initial_margin_atoms {
                return Err("Insufficient margin");
            }
            if entry
                .available_margin
                .compare_exchange_weak(
                    cur,
                    cur - initial_margin_atoms,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                entry
                    .order_margin
                    .fetch_add(initial_margin_atoms, Ordering::Relaxed);
                return Ok(initial_margin_atoms);
            }
        }
    }

    /// Hot path (recv-spin): lock-free release using fetch_add/fetch_sub.
    /// Uses `accounts.get()` (shared shard read-lock) — never blocks concurrent
    /// `check_and_reserve_margin` on other users in the same shard.
    pub fn release_order_margin(&self, user_id: i64, initial_margin_atoms: i64) {
        use std::sync::atomic::Ordering;
        if let Some(entry) = self.accounts.get(&user_id) {
            entry
                .available_margin
                .fetch_add(initial_margin_atoms, Ordering::Relaxed);
            entry
                .order_margin
                .fetch_sub(initial_margin_atoms, Ordering::Relaxed);
        }
    }

    /// Called on every FILLED or PARTIAL_FILL event.
    ///
    /// order_side: 0=buy (long), 1=sell (short) — same encoding as SBE/OrderRuntimeMeta.
    /// fill_margin_atoms: proportional initial margin for this fill (caller computes from
    ///   original order margin × fill_qty / order_qty).
    pub fn on_fill(
        &self,
        user_id: i64,
        symbol: [u8; 16],
        order_side: u8,
        fill_price_ticks: i64,
        fill_qty_lots: i64,
        fill_margin_atoms: i64,
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
        let (old_unrealized_pnl, old_maintenance_margin, old_qty_lots) = self
            .positions
            .get(&key)
            .map(|p| (p.unrealized_pnl, p.maintenance_margin, p.qty_lots))
            .unwrap_or((0, 0, 0));

        let (new_pos, released_used_margin, realized_pnl_cents) = {
            let existing = self.positions.get(&key);
            compute_position_update(
                existing.as_deref(),
                user_id,
                symbol,
                fill_side,
                settlement_price_ticks,
                fill_qty_lots,
                fill_margin_atoms,
                notional_scale,
                leverage,
                maintenance_rate_bps,
            )
        };
        let new_unrealized_pnl = new_pos.as_ref().map(|p| p.unrealized_pnl).unwrap_or(0);
        let unrealized_pnl_delta = new_unrealized_pnl - old_unrealized_pnl;
        let new_maintenance_margin = new_pos.as_ref().map(|p| p.maintenance_margin).unwrap_or(0);
        let maintenance_margin_delta = new_maintenance_margin - old_maintenance_margin;

        // Apply position update.
        match new_pos {
            Some(ref p) => {
                self.positions.insert(key, p.clone());
            }
            None => {
                self.positions.remove(&key);
            }
        }

        // Incremental gross open interest: delta of this user's position
        // size on this symbol. O(1) atomic add; clamping handled at read.
        let new_qty_lots = new_pos.as_ref().map(|p| p.qty_lots).unwrap_or(0);
        let oi_delta = new_qty_lots - old_qty_lots;
        if oi_delta != 0 {
            self.symbol_oi_lots
                .entry(symbol)
                .or_insert_with(|| AtomicI64::new(0))
                .fetch_add(oi_delta, Ordering::Relaxed);
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
                // ×1e6: ticks×lots/scale yields cents; the engine books
                // ATOMS (S2). Same convention as calc::calc_notional_atoms.
                ((fill_price_ticks - liq_price_ticks) as i128
                    * sign as i128
                    * fill_qty_lots as i128
                    * 1_000_000
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
            self.insurance_fund_atoms
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
            use std::sync::atomic::Ordering::Relaxed;
            let old_om = acct.order_margin.load(Relaxed);
            acct.order_margin
                .store((old_om - fill_margin_atoms).max(0), Relaxed);
            if released_used_margin > 0 {
                // Closing (or closing+flipping): release old position's used_margin and credit pnl.
                acct.used_margin = (acct.used_margin - released_used_margin).max(0);
                // For flip: flip_margin portion stays in used_margin, not available.
                acct.used_margin += flip_margin;
                let old_av = acct.available_margin.load(Relaxed);
                acct.available_margin.store(
                    (old_av + fill_margin_atoms + released_used_margin + realized_pnl_cents
                        - flip_margin)
                        .max(0),
                    Relaxed,
                );
                // After forced liquidation close, let run_risk_tick re-evaluate status.
                if acct.status == RiskStatus::Liquidating {
                    acct.status = RiskStatus::Normal;
                }
            } else {
                // Opening: move order_margin → used_margin.
                acct.used_margin += fill_margin_atoms;
            }
            acct.unrealized_pnl += unrealized_pnl_delta;
            acct.maintenance_margin = (acct.maintenance_margin + maintenance_margin_delta).max(0);
            acct.equity = acct.available_margin.load(Relaxed)
                + acct.order_margin.load(Relaxed)
                + acct.used_margin
                + acct.unrealized_pnl;
        }

        // Seed mark_prices with entry price the first time any position opens.
        self.mark_prices.entry(symbol).or_insert(fill_price_ticks);

        // Maintain symbol_position_index for Phase 3 mark-price scan.
        // HashSet insert/remove are O(1) — safe on recv-spin hot path.
        let has_open = self
            .positions
            .get(&key)
            .map(|p| p.qty_lots > 0)
            .unwrap_or(false);
        if has_open {
            self.symbol_position_index
                .entry(symbol)
                .or_default()
                .insert(user_id);
        } else if let Some(mut idx) = self.symbol_position_index.get_mut(&symbol) {
            idx.remove(&user_id);
        }
    }

    // -------------------------------------------------------------------------
    // Phase 3: mark price + incremental unrealized PnL + risk tick
    // -------------------------------------------------------------------------

    /// Update the EWMA mark price for a symbol and incrementally adjust unrealized PnL
    /// for users with open positions.  Called from the WS Aeron public receive thread
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
            .map(|v| v.iter().copied().collect())
            .unwrap_or_default();

        for uid in user_ids {
            let key = (uid, symbol);
            let upnl_delta = {
                let Some(mut pos) = self.positions.get_mut(&key) else {
                    continue;
                };
                let old_upnl = pos.unrealized_pnl;
                let new_upnl = calc::calc_unrealized_pnl_atoms(
                    pos.side,
                    pos.qty_lots,
                    pos.entry_price_ticks,
                    mark_ticks,
                    notional_scale,
                );
                pos.mark_price_ticks = mark_ticks;
                pos.unrealized_pnl = new_upnl;
                new_upnl - old_upnl
            };
            if let Some(mut acct) = self.accounts.get_mut(&uid) {
                use std::sync::atomic::Ordering::Relaxed;
                acct.unrealized_pnl += upnl_delta;
                acct.equity = acct.available_margin.load(Relaxed)
                    + acct.order_margin.load(Relaxed)
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
        // Keep sort+dedup for the current pressure-test shape. Benchmarked in
        // benches/risk_dedup_bench.rs on 2026-06-05: with 40K users and 1-2
        // positions/user, sort+dedup is ~7-9x faster than HashSet. HashSet only
        // wins in high-duplicate shapes such as 10K users x 4 symbols. Revisit
        // if the typical account position fanout grows beyond ~3 positions/user.
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

                Some(Update {
                    user_id: acct.user_id,
                    old_status: acct.status,
                    new_status,
                })
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
    fill_margin_atoms: i64,
    notional_scale: i64,
    leverage: u8,
    maintenance_rate_bps: i64,
) -> (Option<PositionRiskState>, i64, i64) {
    let Some(pos) = existing else {
        // No existing position — open new.
        let notional = calc::calc_notional_atoms(fill_price_ticks, fill_qty_lots, notional_scale);
        let maint = calc::calc_maintenance_margin_atoms(notional, maintenance_rate_bps);
        let liq = calc::calc_liquidation_price_ticks(
            fill_price_ticks,
            leverage,
            maintenance_rate_bps,
            fill_side,
        );
        let bkrpt = calc::calc_bankruptcy_price_ticks(fill_price_ticks, leverage, fill_side);
        return (
            Some(PositionRiskState {
                user_id,
                symbol,
                side: fill_side,
                qty_lots: fill_qty_lots,
                entry_price_ticks: fill_price_ticks,
                mark_price_ticks: fill_price_ticks,
                unrealized_pnl: 0,
                initial_margin: fill_margin_atoms,
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
        updated.initial_margin += fill_margin_atoms;
        refresh_risk_fields(&mut updated, notional_scale, maintenance_rate_bps);
        refresh_unrealized_pnl(&mut updated, notional_scale);
        return (Some(updated), 0, 0);
    }

    // Opposite side — reducing or closing the position.
    let sign: i128 = if pos.side == PositionSide::Long {
        1
    } else {
        -1
    };

    if fill_qty_lots >= pos.qty_lots {
        // Close (and possibly flip).
        let close_qty = pos.qty_lots;
        // ×1e6 → atoms (S2), matching calc::calc_unrealized_pnl_atoms.
        let realized_pnl =
            sign * (fill_price_ticks - pos.entry_price_ticks) as i128 * close_qty as i128
                * 1_000_000
                / notional_scale as i128;
        let released_margin = pos.initial_margin;

        let remaining_qty = fill_qty_lots - close_qty;
        if remaining_qty == 0 {
            return (None, released_margin, realized_pnl as i64);
        }

        // Flipped to opposite side.
        let flip_margin = fill_margin_atoms * remaining_qty / fill_qty_lots;
        let notional = calc::calc_notional_atoms(fill_price_ticks, remaining_qty, notional_scale);
        let maint = calc::calc_maintenance_margin_atoms(notional, maintenance_rate_bps);
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
                unrealized_pnl: 0,
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
        // ×1e6 → atoms (S2), matching calc::calc_unrealized_pnl_atoms.
        let realized_pnl =
            sign * (fill_price_ticks - pos.entry_price_ticks) as i128 * close_qty as i128
                * 1_000_000
                / notional_scale as i128;
        let released_margin = pos.initial_margin * close_qty / pos.qty_lots;

        updated.qty_lots = pos.qty_lots - close_qty;
        updated.initial_margin = pos.initial_margin - released_margin;
        refresh_risk_fields(&mut updated, notional_scale, maintenance_rate_bps);
        refresh_unrealized_pnl(&mut updated, notional_scale);
        (Some(updated), released_margin, realized_pnl as i64)
    }
}

/// Recompute maintenance_margin, liquidation_price, bankruptcy_price from current qty/entry.
fn refresh_risk_fields(
    pos: &mut PositionRiskState,
    notional_scale: i64,
    maintenance_rate_bps: i64,
) {
    let notional = calc::calc_notional_atoms(pos.entry_price_ticks, pos.qty_lots, notional_scale);
    pos.maintenance_margin = calc::calc_maintenance_margin_atoms(notional, maintenance_rate_bps);
    pos.liquidation_price_ticks = calc::calc_liquidation_price_ticks(
        pos.entry_price_ticks,
        pos.leverage,
        maintenance_rate_bps,
        pos.side,
    );
    pos.bankruptcy_price_ticks =
        calc::calc_bankruptcy_price_ticks(pos.entry_price_ticks, pos.leverage, pos.side);
}

fn refresh_unrealized_pnl(pos: &mut PositionRiskState, notional_scale: i64) {
    pos.unrealized_pnl = calc::calc_unrealized_pnl_atoms(
        pos.side,
        pos.qty_lots,
        pos.entry_price_ticks,
        pos.mark_price_ticks,
        notional_scale,
    );
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

    fn eth_sym() -> [u8; 16] {
        let mut s = [0u8; 16];
        s[..7].copy_from_slice(b"ETH_USD");
        s[7] = b'T';
        s
    }

    // BTC_USDT notional_scale=1_000_000, default_leverage=10, maintenance_rate_bps=50
    const NOTIONAL_SCALE: i64 = 1_000_000;
    const LEVERAGE: u8 = 10;
    const MAINT_BPS: i64 = 50;
    /// Atoms per cent — S2 moved the engine to atoms; tests keep their
    /// historical cent-denominated economics readable by scaling with A.
    const A: i64 = 1_000_000;

    #[test]
    fn initialize_account_sets_available_margin() {
        let (engine, user_id) = make_engine_with_account(100_000 * A);
        let state = engine.accounts.get(&user_id).unwrap();
        use std::sync::atomic::Ordering::Relaxed;
        assert_eq!(state.available_margin.load(Relaxed), 100_000 * A);
        assert_eq!(state.order_margin.load(Relaxed), 0);
        assert_eq!(state.used_margin, 0);
        assert_eq!(state.equity, 100_000 * A);
    }

    #[test]
    fn reserve_margin_succeeds_when_sufficient() {
        let (engine, user_id) = make_engine_with_account(100_000 * A);
        let result = engine.check_and_reserve_margin(user_id, 10_000 * A);
        assert_eq!(result, Ok(10_000 * A));
        let state = engine.accounts.get(&user_id).unwrap();
        use std::sync::atomic::Ordering::Relaxed;
        assert_eq!(state.available_margin.load(Relaxed), 90_000 * A);
        assert_eq!(state.order_margin.load(Relaxed), 10_000 * A);
    }

    #[test]
    fn reserve_margin_fails_when_insufficient() {
        let (engine, user_id) = make_engine_with_account(5_000 * A);
        let result = engine.check_and_reserve_margin(user_id, 10_000 * A);
        assert!(result.is_err());
        let state = engine.accounts.get(&user_id).unwrap();
        use std::sync::atomic::Ordering::Relaxed;
        assert_eq!(state.available_margin.load(Relaxed), 5_000 * A);
        assert_eq!(state.order_margin.load(Relaxed), 0);
    }

    #[test]
    fn release_restores_available_margin() {
        let (engine, user_id) = make_engine_with_account(100_000 * A);
        engine.check_and_reserve_margin(user_id, 10_000 * A).unwrap();
        engine.release_order_margin(user_id, 10_000 * A);
        let state = engine.accounts.get(&user_id).unwrap();
        use std::sync::atomic::Ordering::Relaxed;
        assert_eq!(state.available_margin.load(Relaxed), 100_000 * A);
        assert_eq!(state.order_margin.load(Relaxed), 0);
    }

    #[test]
    fn concurrent_reserve_one_must_fail() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let engine = RiskEngine::new();
        let user_id = 7i64;
        engine.initialize_account(user_id, 15_000 * A);
        let engine = Arc::new(engine);
        let success = Arc::new(AtomicUsize::new(0));
        let failure = Arc::new(AtomicUsize::new(0));
        let handles: Vec<_> = (0..2)
            .map(|_| {
                let e = engine.clone();
                let s = success.clone();
                let f = failure.clone();
                std::thread::spawn(move || match e.check_and_reserve_margin(user_id, 10_000 * A) {
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
        use std::sync::atomic::Ordering::Relaxed;
        assert_eq!(state.available_margin.load(Relaxed), 5_000 * A);
        assert_eq!(state.order_margin.load(Relaxed), 10_000 * A);
    }

    #[test]
    fn liquidation_pending_account_rejected() {
        let engine = RiskEngine::new();
        let user_id = 99i64;
        engine.initialize_account(user_id, 100_000 * A);
        if let Some(mut entry) = engine.accounts.get_mut(&user_id) {
            entry.status = RiskStatus::LiquidationPending;
        }
        assert!(engine.check_and_reserve_margin(user_id, 1_000 * A).is_err());
    }

    // ── on_fill tests ──────────────────────────────────────────────────────────

    // BTC at $50,000 → price_ticks = 5_000_000
    // 0.1 BTC = 100_000 lots
    // notional = 5_000_000 * 100_000 / 1_000_000 = 500_000 cents = $5,000
    // initial_margin (10x) = 50_000 cents = $500
    const PRICE_TICKS: i64 = 5_000_000;
    const QTY_LOTS: i64 = 100_000;
    const MARGIN_ATOMS: i64 = 50_000 * A;

    fn setup_with_reserved(balance_atoms: i64) -> (Arc<RiskEngine>, i64) {
        let (engine, uid) = make_engine_with_account(balance_atoms);
        engine.check_and_reserve_margin(uid, MARGIN_ATOMS).unwrap();
        (engine, uid)
    }

    #[test]
    fn open_interest_tracks_position_lifecycle() {
        let (engine, uid) = setup_with_reserved(10_000_000 * A);
        let sym = btc_sym();
        assert_eq!(engine.symbol_open_interest_lots(&sym), 0);

        // Open long 100k lots → OI 100k.
        engine.on_fill(uid, sym, 0, PRICE_TICKS, QTY_LOTS, MARGIN_ATOMS,
            NOTIONAL_SCALE, LEVERAGE, MAINT_BPS, 0);
        assert_eq!(engine.symbol_open_interest_lots(&sym), QTY_LOTS);

        // Add 50k → OI 150k.
        engine.check_and_reserve_margin(uid, MARGIN_ATOMS / 2).unwrap();
        engine.on_fill(uid, sym, 0, PRICE_TICKS, QTY_LOTS / 2, MARGIN_ATOMS / 2,
            NOTIONAL_SCALE, LEVERAGE, MAINT_BPS, 0);
        assert_eq!(engine.symbol_open_interest_lots(&sym), QTY_LOTS + QTY_LOTS / 2);

        // Close 50k (sell) → OI back to 100k.
        engine.on_fill(uid, sym, 1, PRICE_TICKS, QTY_LOTS / 2, 0,
            NOTIONAL_SCALE, LEVERAGE, MAINT_BPS, 0);
        assert_eq!(engine.symbol_open_interest_lots(&sym), QTY_LOTS);

        // Close fully → OI 0.
        engine.on_fill(uid, sym, 1, PRICE_TICKS, QTY_LOTS, 0,
            NOTIONAL_SCALE, LEVERAGE, MAINT_BPS, 0);
        assert_eq!(engine.symbol_open_interest_lots(&sym), 0);
    }

    #[test]
    fn position_and_oi_limits_enforced_o1() {
        let (engine, uid) = setup_with_reserved(10_000_000 * A);
        let sym = btc_sym();

        // Disabled limits always pass.
        assert!(engine.check_position_limit(uid, &sym, i64::MAX / 2, 0).is_ok());
        assert!(engine.check_symbol_oi_limit(&sym, i64::MAX / 2, 0).is_ok());

        // No position yet: order up to the cap passes, beyond fails.
        assert!(engine.check_position_limit(uid, &sym, 100, 100).is_ok());
        assert!(engine.check_position_limit(uid, &sym, 101, 100).is_err());

        // Open 100k lots, cap 150k: 50k more ok, 50k+1 rejected.
        engine.on_fill(uid, sym, 0, PRICE_TICKS, QTY_LOTS, MARGIN_ATOMS,
            NOTIONAL_SCALE, LEVERAGE, MAINT_BPS, 0);
        assert!(engine.check_position_limit(uid, &sym, QTY_LOTS / 2, QTY_LOTS + QTY_LOTS / 2).is_ok());
        assert!(engine.check_position_limit(uid, &sym, QTY_LOTS / 2 + 1, QTY_LOTS + QTY_LOTS / 2).is_err());

        // Market-wide OI cap counts everyone's positions.
        assert!(engine.check_symbol_oi_limit(&sym, QTY_LOTS, 2 * QTY_LOTS).is_ok());
        assert!(engine.check_symbol_oi_limit(&sym, QTY_LOTS + 1, 2 * QTY_LOTS).is_err());

        // Saturating add: no overflow panic at extreme values.
        assert!(engine.check_position_limit(uid, &sym, i64::MAX, 1).is_err());
    }

    #[test]
    fn on_fill_opens_long_position() {
        let (engine, uid) = setup_with_reserved(200_000 * A);
        engine.on_fill(
            uid,
            btc_sym(),
            0,
            PRICE_TICKS,
            QTY_LOTS,
            MARGIN_ATOMS,
            NOTIONAL_SCALE,
            LEVERAGE,
            MAINT_BPS,
            0,
        );

        let pos = engine.positions.get(&(uid, btc_sym())).unwrap();
        assert_eq!(pos.qty_lots, QTY_LOTS);
        assert_eq!(pos.entry_price_ticks, PRICE_TICKS);
        assert_eq!(pos.side, PositionSide::Long);
        assert_eq!(pos.initial_margin, MARGIN_ATOMS);

        let acct = engine.accounts.get(&uid).unwrap();
        use std::sync::atomic::Ordering::Relaxed;
        assert_eq!(acct.order_margin.load(Relaxed), 0);
        assert_eq!(acct.used_margin, MARGIN_ATOMS);
    }

    #[test]
    fn on_fill_opens_short_position() {
        let (engine, uid) = setup_with_reserved(200_000 * A);
        engine.on_fill(
            uid,
            btc_sym(),
            1,
            PRICE_TICKS,
            QTY_LOTS,
            MARGIN_ATOMS,
            NOTIONAL_SCALE,
            LEVERAGE,
            MAINT_BPS,
            0,
        );

        let pos = engine.positions.get(&(uid, btc_sym())).unwrap();
        assert_eq!(pos.side, PositionSide::Short);
        assert_eq!(pos.qty_lots, QTY_LOTS);
    }

    #[test]
    fn on_fill_adds_to_existing_long_vwap() {
        let (engine, uid) = make_engine_with_account(500_000 * A);
        engine.check_and_reserve_margin(uid, MARGIN_ATOMS).unwrap();
        engine.on_fill(
            uid,
            btc_sym(),
            0,
            PRICE_TICKS,
            QTY_LOTS,
            MARGIN_ATOMS,
            NOTIONAL_SCALE,
            LEVERAGE,
            MAINT_BPS,
            0,
        );

        // Second fill at $55,000 for same qty
        let price2 = 5_500_000i64;
        engine.check_and_reserve_margin(uid, MARGIN_ATOMS).unwrap();
        engine.on_fill(
            uid,
            btc_sym(),
            0,
            price2,
            QTY_LOTS,
            MARGIN_ATOMS,
            NOTIONAL_SCALE,
            LEVERAGE,
            MAINT_BPS,
            0,
        );

        let pos = engine.positions.get(&(uid, btc_sym())).unwrap();
        assert_eq!(pos.qty_lots, QTY_LOTS * 2);
        // VWAP = (5_000_000 * 100_000 + 5_500_000 * 100_000) / 200_000 = 5_250_000
        assert_eq!(pos.entry_price_ticks, 5_250_000);
        assert_eq!(pos.initial_margin, MARGIN_ATOMS * 2);
    }

    #[test]
    fn on_fill_closes_long_fully_releases_margin_and_credits_pnl() {
        let (engine, uid) = make_engine_with_account(500_000 * A);
        // Open long at $50,000
        engine.check_and_reserve_margin(uid, MARGIN_ATOMS).unwrap();
        engine.on_fill(
            uid,
            btc_sym(),
            0,
            PRICE_TICKS,
            QTY_LOTS,
            MARGIN_ATOMS,
            NOTIONAL_SCALE,
            LEVERAGE,
            MAINT_BPS,
            0,
        );

        // Close at $55,000: profit = ($55,000 - $50,000) * 0.1 BTC = $500 = 50_000 cents
        let close_price = 5_500_000i64;
        engine.check_and_reserve_margin(uid, MARGIN_ATOMS).unwrap();
        engine.on_fill(
            uid,
            btc_sym(),
            1,
            close_price,
            QTY_LOTS,
            MARGIN_ATOMS,
            NOTIONAL_SCALE,
            LEVERAGE,
            MAINT_BPS,
            0,
        );

        // Position should be gone
        assert!(engine.positions.get(&(uid, btc_sym())).is_none());

        let acct = engine.accounts.get(&uid).unwrap();
        use std::sync::atomic::Ordering::Relaxed;
        assert_eq!(acct.used_margin, 0);
        assert_eq!(acct.order_margin.load(Relaxed), 0);
        // open:  500_000 - 50_000 (reserve) = 450_000 available, 50_000 order_margin
        //        on_fill: order_margin → used_margin, available unchanged at 450_000
        // close: 450_000 - 50_000 (reserve) = 400_000 available, 50_000 order_margin
        //        on_fill: +50_000 (close reservation) + 50_000 (used_margin) + 50_000 (profit) = 550_000
        assert_eq!(acct.available_margin.load(Relaxed), 550_000 * A);
    }

    #[test]
    fn on_fill_partial_close_reduces_position() {
        let (engine, uid) = make_engine_with_account(500_000 * A);
        engine.check_and_reserve_margin(uid, MARGIN_ATOMS).unwrap();
        engine.on_fill(
            uid,
            btc_sym(),
            0,
            PRICE_TICKS,
            QTY_LOTS,
            MARGIN_ATOMS,
            NOTIONAL_SCALE,
            LEVERAGE,
            MAINT_BPS,
            0,
        );

        // Close half at $50,000 (no PnL)
        let half_qty = QTY_LOTS / 2;
        let half_margin = MARGIN_ATOMS / 2;
        engine.check_and_reserve_margin(uid, half_margin).unwrap();
        engine.on_fill(
            uid,
            btc_sym(),
            1,
            PRICE_TICKS,
            half_qty,
            half_margin,
            NOTIONAL_SCALE,
            LEVERAGE,
            MAINT_BPS,
            0,
        );

        let pos = engine.positions.get(&(uid, btc_sym())).unwrap();
        assert_eq!(pos.qty_lots, half_qty);
        assert_eq!(pos.side, PositionSide::Long);

        let acct = engine.accounts.get(&uid).unwrap();
        use std::sync::atomic::Ordering::Relaxed;
        assert_eq!(acct.used_margin, half_margin); // only half still used
        assert_eq!(acct.order_margin.load(Relaxed), 0);
    }

    #[test]
    fn on_fill_symbol_position_index_maintained() {
        let (engine, uid) = setup_with_reserved(200_000 * A);
        engine.on_fill(
            uid,
            btc_sym(),
            0,
            PRICE_TICKS,
            QTY_LOTS,
            MARGIN_ATOMS,
            NOTIONAL_SCALE,
            LEVERAGE,
            MAINT_BPS,
            0,
        );

        // Should be in index
        let idx = engine.symbol_position_index.get(&btc_sym()).unwrap();
        assert!(idx.contains(&uid));
        drop(idx);

        // Close: index entry removed
        engine.check_and_reserve_margin(uid, MARGIN_ATOMS).unwrap();
        engine.on_fill(
            uid,
            btc_sym(),
            1,
            PRICE_TICKS,
            QTY_LOTS,
            MARGIN_ATOMS,
            NOTIONAL_SCALE,
            LEVERAGE,
            MAINT_BPS,
            0,
        );

        let idx = engine.symbol_position_index.get(&btc_sym());
        assert!(idx.map(|v| !v.contains(&uid)).unwrap_or(true));
    }

    #[test]
    fn on_fill_liquidation_price_set_on_open() {
        let (engine, uid) = setup_with_reserved(200_000 * A);
        engine.on_fill(
            uid,
            btc_sym(),
            0,
            PRICE_TICKS,
            QTY_LOTS,
            MARGIN_ATOMS,
            NOTIONAL_SCALE,
            LEVERAGE,
            MAINT_BPS,
            0,
        );
        let pos = engine.positions.get(&(uid, btc_sym())).unwrap();
        // liq_price_long = entry * (10*10000 - 10000 + 50) / (10*10000) = 5_000_000 * 90050 / 100000 = 4_502_500
        assert_eq!(pos.liquidation_price_ticks, 4_502_500);
        assert_eq!(pos.bankruptcy_price_ticks, 4_500_000);
    }

    // ── Phase 3: mark price + unrealized PnL + risk tick ─────────────────────

    #[test]
    fn update_mark_price_sets_ewma_and_upnl() {
        let (engine, uid) = setup_with_reserved(500_000 * A);
        // Open long: 0.1 BTC at $50,000, margin $500
        engine.on_fill(
            uid,
            btc_sym(),
            0,
            PRICE_TICKS,
            QTY_LOTS,
            MARGIN_ATOMS,
            NOTIONAL_SCALE,
            LEVERAGE,
            MAINT_BPS,
            0,
        );

        // Mark price jumps to $51,000 (price_ticks = 5_100_000).
        // First update: EWMA = (5_100_000 + 9 * 5_000_000) / 10 = 5_010_000
        engine.update_mark_price(btc_sym(), 5_100_000, NOTIONAL_SCALE);

        let pos = engine.positions.get(&(uid, btc_sym())).unwrap();
        assert_eq!(pos.mark_price_ticks, 5_010_000);

        // unrealized_pnl = (5_010_000 - 5_000_000) * 100_000 / 1_000_000 = 1_000 cents = $10
        let acct = engine.accounts.get(&uid).unwrap();
        assert_eq!(acct.unrealized_pnl, 1_000 * A);
        // equity = available(450k - nothing since open moved to used) + used(50k) + upnl(1k) + order(0)
        // At open: available = 500_000 - 50_000 = 450_000, used = 50_000
        assert_eq!(acct.equity, (450_000 + 50_000 + 1_000) * A);
    }

    #[test]
    fn update_mark_price_accumulates_multi_symbol_upnl_incrementally() {
        let (engine, uid) = make_engine_with_account(1_000_000 * A);

        engine.check_and_reserve_margin(uid, MARGIN_ATOMS).unwrap();
        engine.on_fill(
            uid,
            btc_sym(),
            0,
            PRICE_TICKS,
            QTY_LOTS,
            MARGIN_ATOMS,
            NOTIONAL_SCALE,
            LEVERAGE,
            MAINT_BPS,
            0,
        );

        let eth_price = 300_000i64;
        let eth_qty = 100_000i64;
        let eth_margin = 3_000i64 * A;
        engine.check_and_reserve_margin(uid, eth_margin).unwrap();
        engine.on_fill(
            uid,
            eth_sym(),
            0,
            eth_price,
            eth_qty,
            eth_margin,
            NOTIONAL_SCALE,
            LEVERAGE,
            MAINT_BPS,
            0,
        );

        engine.update_mark_price(btc_sym(), 5_100_000, NOTIONAL_SCALE);
        engine.update_mark_price(eth_sym(), 310_000, NOTIONAL_SCALE);

        let btc_pos = engine.positions.get(&(uid, btc_sym())).unwrap();
        let btc_upnl = btc_pos.unrealized_pnl;
        drop(btc_pos);
        let eth_pos = engine.positions.get(&(uid, eth_sym())).unwrap();
        let eth_upnl = eth_pos.unrealized_pnl;
        drop(eth_pos);

        let acct = engine.accounts.get(&uid).unwrap();
        assert_eq!(acct.unrealized_pnl, btc_upnl + eth_upnl);
        assert_eq!(btc_upnl, 1_000 * A);
        assert_eq!(eth_upnl, 100 * A);
    }

    #[test]
    fn update_mark_price_ignores_zero() {
        let (engine, uid) = setup_with_reserved(200_000 * A);
        engine.on_fill(
            uid,
            btc_sym(),
            0,
            PRICE_TICKS,
            QTY_LOTS,
            MARGIN_ATOMS,
            NOTIONAL_SCALE,
            LEVERAGE,
            MAINT_BPS,
            0,
        );
        engine.update_mark_price(btc_sym(), 0, NOTIONAL_SCALE);
        let pos = engine.positions.get(&(uid, btc_sym())).unwrap();
        assert_eq!(pos.mark_price_ticks, PRICE_TICKS); // unchanged
    }

    #[test]
    fn risk_tick_normal_when_margin_healthy() {
        let (engine, uid) = setup_with_reserved(500_000 * A);
        engine.on_fill(
            uid,
            btc_sym(),
            0,
            PRICE_TICKS,
            QTY_LOTS,
            MARGIN_ATOMS,
            NOTIONAL_SCALE,
            LEVERAGE,
            MAINT_BPS,
            0,
        );
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
        assert!(
            engine
                .accounts
                .iter()
                .all(|acct| acct.status == RiskStatus::Normal)
        );
    }

    #[test]
    fn account_maintenance_margin_aggregated_from_positions() {
        let (engine, uid) = setup_with_reserved(500_000 * A);
        // notional = 5_000_000 * 100_000 / 1_000_000 = 500_000 cents
        // maintenance_margin = 500_000 * 50 / 10_000 = 2_500 cents
        engine.on_fill(
            uid,
            btc_sym(),
            0,
            PRICE_TICKS,
            QTY_LOTS,
            MARGIN_ATOMS,
            NOTIONAL_SCALE,
            LEVERAGE,
            MAINT_BPS,
            0,
        );
        let acct = engine.accounts.get(&uid).unwrap();
        assert_eq!(acct.maintenance_margin, 2_500 * A);
    }

    #[test]
    fn account_maintenance_margin_updates_incrementally_across_symbols() {
        let (engine, uid) = make_engine_with_account(1_000_000 * A);

        engine.check_and_reserve_margin(uid, MARGIN_ATOMS).unwrap();
        engine.on_fill(
            uid,
            btc_sym(),
            0,
            PRICE_TICKS,
            QTY_LOTS,
            MARGIN_ATOMS,
            NOTIONAL_SCALE,
            LEVERAGE,
            MAINT_BPS,
            0,
        );

        let eth_price = 300_000i64;
        let eth_qty = 100_000i64;
        let eth_margin = 3_000i64;
        engine.check_and_reserve_margin(uid, eth_margin).unwrap();
        engine.on_fill(
            uid,
            eth_sym(),
            0,
            eth_price,
            eth_qty,
            eth_margin,
            NOTIONAL_SCALE,
            LEVERAGE,
            MAINT_BPS,
            0,
        );

        {
            let acct = engine.accounts.get(&uid).unwrap();
            assert_eq!(acct.maintenance_margin, (2_500 + 150) * A);
        }

        engine.check_and_reserve_margin(uid, eth_margin).unwrap();
        engine.on_fill(
            uid,
            eth_sym(),
            1,
            eth_price,
            eth_qty,
            eth_margin,
            NOTIONAL_SCALE,
            LEVERAGE,
            MAINT_BPS,
            0,
        );

        let acct = engine.accounts.get(&uid).unwrap();
        assert_eq!(acct.maintenance_margin, 2_500 * A);
    }

    #[test]
    fn risk_tick_triggers_liquidation_pending_when_equity_below_maintenance() {
        let (engine, uid) = make_engine_with_account(500_000 * A);
        engine.check_and_reserve_margin(uid, MARGIN_ATOMS).unwrap();
        engine.on_fill(
            uid,
            btc_sym(),
            0,
            PRICE_TICKS,
            QTY_LOTS,
            MARGIN_ATOMS,
            NOTIONAL_SCALE,
            LEVERAGE,
            MAINT_BPS,
            0,
        );

        // Maintenance margin at open = 500_000 * 50 / 10_000 = 2_500 cents ($25).
        // Force equity below maintenance via unrealized PnL.
        // equity = available(450_000) + used(50_000) + upnl = 2_499 → upnl = -497_501
        if let Some(mut acct) = engine.accounts.get_mut(&uid) {
            acct.unrealized_pnl = -497_501 * A;
            use std::sync::atomic::Ordering::Relaxed;
            acct.equity = acct.available_margin.load(Relaxed)
                + acct.order_margin.load(Relaxed)
                + acct.used_margin
                + acct.unrealized_pnl;
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
        let (engine, uid) = make_engine_with_account(52_600 * A);
        engine.check_and_reserve_margin(uid, MARGIN_ATOMS).unwrap();
        engine.on_fill(
            uid,
            btc_sym(),
            0,
            PRICE_TICKS,
            QTY_LOTS,
            MARGIN_ATOMS,
            NOTIONAL_SCALE,
            LEVERAGE,
            MAINT_BPS,
            0,
        );
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
        assert!(
            triggered,
            "equity should drop below maintenance after repeated mark price updates"
        );

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
        let (engine, uid) = setup_with_reserved(1_000_000 * A);
        engine.on_fill(
            uid,
            btc_sym(),
            0,
            PRICE_TICKS,
            QTY_LOTS,
            MARGIN_ATOMS,
            NOTIONAL_SCALE,
            LEVERAGE,
            MAINT_BPS,
            0,
        );

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
    /// (the account is Liquidating so that would be blocked). fill_margin_atoms = 0.
    #[test]
    fn insurance_fund_gains_on_profitable_liquidation_close() {
        let (engine, uid) = setup_with_reserved(500_000 * A);
        // Open long at $50,000; margin = 50_000 cents
        engine.on_fill(
            uid,
            btc_sym(),
            0,
            PRICE_TICKS,
            QTY_LOTS,
            MARGIN_ATOMS,
            NOTIONAL_SCALE,
            LEVERAGE,
            MAINT_BPS,
            0,
        );

        // Simulate the account being set to Liquidating (done by the tick task).
        if let Some(mut acct) = engine.accounts.get_mut(&uid) {
            acct.status = RiskStatus::Liquidating;
        }

        // System close at $51,000 (profit $100 = 10_000 cents).
        // No margin was pre-reserved (system bypass), so fill_margin_atoms = 0.
        let close_price_ticks = 5_100_000i64;
        engine.on_fill(
            uid,
            btc_sym(),
            1,
            close_price_ticks,
            QTY_LOTS,
            0,
            NOTIONAL_SCALE,
            LEVERAGE,
            MAINT_BPS,
            0,
        );

        // After the forced close, on_fill resets status to Normal so run_risk_tick
        // can re-evaluate on the next tick (H3 fix: Liquidated was a terminal sink).
        let acct = engine.accounts.get(&uid).unwrap();
        assert_eq!(acct.status, RiskStatus::Normal);

        // Insurance fund should have gained the realized pnl:
        //   pnl = (5_100_000 − 5_000_000) × 100_000 / 1_000_000 = 10_000 cents
        //   (bkrpt_pnl = 0 per Phase 6 TODO, so fund gains full realized_pnl)
        assert_eq!(engine.insurance_fund(), 10_000 * A);
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
        let (engine, uid) = setup_with_reserved(2_000_000 * A); // $20,000 initial balance

        // Open long BTC at $50,000 (entry_price_ticks = 5_000_000, qty = 0.1 BTC = 100_000 lots)
        let entry_ticks = 5_000_000i64; // $50,000 at $0.01/tick
        let qty_lots = 100_000i64; // 0.1 BTC at 1e-6 lot size
        let open_notional = entry_ticks * qty_lots / SCALE; // = 500_000 cents = $5,000
        let open_margin = open_notional / 10; // 10x → 50_000 cents = $500
        engine.check_and_reserve_margin(uid, open_margin).unwrap();
        engine.on_fill(
            uid,
            btc_sym(),
            0,
            entry_ticks,
            qty_lots,
            open_margin,
            SCALE,
            10,
            50,
            0,
        );

        // Risk tick decides to liquidate; liq_price = $49,000 (below mark, more aggressive)
        let liq_ticks = 4_900_000i64; // $49,000
        // Actual market fill comes in at $49,800 (better for the buyer / worse for liq engine)
        let fill_ticks = 4_980_000i64; // $49,800
        if let Some(mut acct) = engine.accounts.get_mut(&uid) {
            acct.status = RiskStatus::Liquidating;
        }

        // System close: sell order at liq_price (IOC limit), fills at $49,800.
        engine.on_fill(
            uid,
            btc_sym(),
            1,
            fill_ticks,
            qty_lots,
            0,
            SCALE,
            10,
            50,
            liq_ticks,
        );

        // Account settled at liq_price ($49,000), not fill price ($49,800).
        // After forced close, status → Normal so run_risk_tick can re-evaluate (H3 fix).
        let acct = engine.accounts.get(&uid).unwrap();
        assert_eq!(acct.status, RiskStatus::Normal);

        // Exchange revenue = (fill − liq) × qty / scale (sell close, sign=+1)
        //   = (4_980_000 − 4_900_000) × 100_000 / 1_000_000
        //   = 80_000 × 100_000 / 1_000_000 = 8_000_000_000 / 1_000_000 = 8_000 cents = $80
        assert_eq!(engine.insurance_fund(), 8_000 * A);
    }

    /// For non-Liquidating closes, insurance fund is not touched.
    #[test]
    fn insurance_fund_unchanged_on_normal_close() {
        let (engine, uid) = setup_with_reserved(500_000 * A);
        engine.on_fill(
            uid,
            btc_sym(),
            0,
            PRICE_TICKS,
            QTY_LOTS,
            MARGIN_ATOMS,
            NOTIONAL_SCALE,
            LEVERAGE,
            MAINT_BPS,
            0,
        );

        // Normal user close at same price (no PnL, account stays Normal).
        engine.check_and_reserve_margin(uid, MARGIN_ATOMS).unwrap();
        engine.on_fill(
            uid,
            btc_sym(),
            1,
            PRICE_TICKS,
            QTY_LOTS,
            MARGIN_ATOMS,
            NOTIONAL_SCALE,
            LEVERAGE,
            MAINT_BPS,
            0,
        );

        assert_eq!(engine.insurance_fund(), 0);
    }

    // ── Full end-to-end integration: order book fill → hold → liquidation ────
    //
    // This test drives the complete lifecycle without mocking:
    //   1. Market maker places resting sell orders in the MatchingEngine book.
    //   2. Taker places a crossing buy → MatchingEngine produces fills.
    //   3. RiskEngine.on_fill() opens a long position for the taker.
    //   4. [CHECK] positions, balances, liq_price, unrealized PnL correct.
    //   5. Mark price is driven down via repeated update_mark_price() calls.
    //   6. [CHECK] equity drops below maintenance_margin.
    //   7. run_risk_tick() emits LiquidationEvent with correct liq_price_ticks.
    //   8. Liquidation fill arrives at a price better than liq_price
    //      → insurance fund captures the spread.
    //   9. [CHECK] position closed, realized PnL correct, fund credited, equity final.
    //
    // All monetary values in cents ($1 = 100); prices in ticks ($0.01/tick for BTC_USDT).
    #[test]
    fn end_to_end_open_hold_liquidate() {
        use crate::matching::engine::{MatchingEngine, PoolConfig};
        use crate::matching::order::{Order, Side, TimeInForce};
        use std::sync::atomic::Ordering::Relaxed;

        // ── BTC_USDT constants ────────────────────────────────────────────────
        // price_tick = $0.01  →  $50,000 = 5_000_000 ticks
        // lot_size   = 0.000001 BTC  →  0.1 BTC = 100_000 lots
        // notional_scale = 1_000_000
        const SCALE: i64 = 1_000_000;
        const LEV: u8 = 10;
        const MBPS: i64 = 50; // maintenance rate in basis points

        // Entry fill at $50,000 for 0.1 BTC
        const ENTRY_TICKS: i64 = 5_000_000;
        const QTY_LOTS: i64 = 100_000;

        // Derived:
        //   notional  = 5_000_000 * 100_000 / 1_000_000 = 500_000 cents ($5,000)
        //   im        = 500_000 / 10               = 50_000  cents ($500)
        //   mm        = 500_000 * 50 / 10_000      = 2_500   cents ($25)
        //   liq_price = 5_000_000 * (10*10000-10000+50)/(10*10000)
        //             = 5_000_000 * 90050 / 100000 = 4_502_500 ticks ($45,025)
        //   bkrpt     = 5_000_000 * 9 / 10         = 4_500_000 ticks ($45,000)
        const NOTIONAL: i64 = 500_000 * 1_000_000; // atoms
        const IM: i64 = 50_000 * 1_000_000;
        const MM: i64 = 2_500 * 1_000_000;
        const LIQ_TICKS: i64 = 4_502_500;
        const BKRPT_TICKS: i64 = 4_500_000;

        // Taker balance chosen tight so liquidation triggers after ~20 mark-price
        // EWMA updates toward $44,000.
        //   equity_at_open = 55_000 cents ($550)
        //   For trigger: equity < MM  →  55_000 + upnl < 2_500
        //                upnl < -52_500  →  mark < ~4_475_000
        //   EWMA α=0.1: after ~20 calls to 4_400_000, mark converges past threshold.
        const TAKER_BALANCE: i64 = 55_000 * 1_000_000; // atoms (A not in scope here)

        // ── Setup: matching engine + risk engine ─────────────────────────────
        let mut me = MatchingEngine::new(PoolConfig::default()).unwrap();
        let risk = RiskEngine::new();

        const MAKER_ID: i64 = 1;
        const TAKER_ID: i64 = 2;
        risk.initialize_account(MAKER_ID, 10_000_000 * 1_000_000); // maker has plenty
        risk.initialize_account(TAKER_ID, TAKER_BALANCE);

        // ── Phase 1: market maker places resting sell orders ─────────────────
        // Two asks: 100_000 lots at $50,000 and 50_000 lots at $51,000.
        let ask1 = Order::new(101, Side::Sell, ENTRY_TICKS, QTY_LOTS, TimeInForce::GTC, 0);
        let ask2 = Order::new(102, Side::Sell, 5_100_000, 50_000, TimeInForce::GTC, 0);
        me.place_order(ask1).unwrap();
        me.place_order(ask2).unwrap();

        // ── Phase 2: taker places crossing buy order ──────────────────────────
        // Buy 100_000 lots at $50,000 — fully fills against ask1.
        let taker_buy = Order::new(201, Side::Buy, ENTRY_TICKS, QTY_LOTS, TimeInForce::GTC, 0);
        let result = me.place_order(taker_buy).unwrap();
        assert_eq!(
            result.filled_lots, QTY_LOTS,
            "taker order should be fully filled"
        );
        assert_eq!(result.fills.len(), 1, "should have one fill leg");
        let (_maker_oid, fill_ticks, fill_lots) = result.fills[0];
        assert_eq!(fill_ticks, ENTRY_TICKS);
        assert_eq!(fill_lots, QTY_LOTS);

        // ── Phase 3: register fill in risk engine ────────────────────────────
        // In production, desk_server.rs does check_and_reserve_margin before
        // submitting the order, so margin is already reserved when the fill arrives.
        risk.check_and_reserve_margin(TAKER_ID, IM).unwrap();
        // Taker is the buyer (side=0=Long).
        risk.on_fill(
            TAKER_ID,
            btc_sym(),
            0,
            fill_ticks as i64,
            fill_lots as i64,
            IM,
            SCALE,
            LEV,
            MBPS,
            0,
        );

        // ── CHECK A: positions and balances after open ────────────────────────
        // In production this state is also persisted to PostgreSQL via pg-writer
        // (PersistEvent::BatchSettleTrade). Here we verify the in-memory source of truth.
        {
            let pos = risk
                .positions
                .get(&(TAKER_ID, btc_sym()))
                .expect("Phase 3: position must exist");
            assert_eq!(pos.side, PositionSide::Long, "long position");
            assert_eq!(pos.qty_lots, QTY_LOTS, "qty");
            assert_eq!(pos.entry_price_ticks, ENTRY_TICKS, "entry price");
            assert_eq!(pos.initial_margin, IM, "initial margin");
            assert_eq!(pos.maintenance_margin, MM, "maintenance margin");
            assert_eq!(pos.liquidation_price_ticks, LIQ_TICKS, "liq price");
            assert_eq!(pos.bankruptcy_price_ticks, BKRPT_TICKS, "bankruptcy price");
            assert_eq!(pos.unrealized_pnl, 0, "upnl=0 at open (mark=entry)");
            drop(pos);

            let acct = risk.accounts.get(&TAKER_ID).unwrap();
            // After reserve + fill:
            //   available = TAKER_BALANCE - IM = 55_000 - 50_000 = 5_000
            //   order_margin = 0 (consumed by on_fill: moved to used)
            //   used_margin  = IM = 50_000
            assert_eq!(
                acct.available_margin.load(Relaxed),
                TAKER_BALANCE - IM,
                "available after open"
            );
            assert_eq!(
                acct.order_margin.load(Relaxed),
                0,
                "order_margin after fill"
            );
            assert_eq!(acct.used_margin, IM, "used_margin = IM");
            assert_eq!(acct.unrealized_pnl, 0, "upnl=0");
            assert_eq!(acct.maintenance_margin, MM, "account MM");
            assert_eq!(acct.equity, TAKER_BALANCE, "equity = full balance at open");
            assert_eq!(acct.status, RiskStatus::Normal, "status Normal");
        }

        // ── Phase 4: drive mark price toward $44,000 ─────────────────────────
        // EWMA α=0.1 → after ~22 calls mark drops below ~4_475_000 and
        // equity < MM triggers liquidation.
        let mut mark_at_trigger = 0i64;
        let mut equity_at_trigger = 0i64;
        for _ in 0..40 {
            risk.update_mark_price(btc_sym(), 4_400_000, SCALE);
            let acct = risk.accounts.get(&TAKER_ID).unwrap();
            if acct.equity <= acct.maintenance_margin {
                mark_at_trigger = *risk.mark_prices.get(&btc_sym()).unwrap();
                equity_at_trigger = acct.equity;
                break;
            }
        }
        assert!(
            mark_at_trigger > 0,
            "mark price should have dropped enough to trigger liquidation"
        );

        // ── CHECK B: account state just before risk tick ──────────────────────
        {
            let acct = risk.accounts.get(&TAKER_ID).unwrap();
            let pos = risk.positions.get(&(TAKER_ID, btc_sym())).unwrap();

            // Mark price is below liq_price — position is underwater.
            assert!(
                mark_at_trigger < LIQ_TICKS,
                "mark {} should be below liq {} when liquidation triggers",
                mark_at_trigger,
                LIQ_TICKS
            );
            // unrealized_pnl is negative and dragged equity below MM.
            assert!(acct.unrealized_pnl < 0, "upnl must be negative");
            assert!(pos.unrealized_pnl < 0, "position upnl matches");
            assert!(
                equity_at_trigger <= MM,
                "equity {} <= MM {}",
                equity_at_trigger,
                MM
            );
            // used_margin still intact (position not yet closed).
            assert_eq!(acct.used_margin, IM, "used_margin unchanged while holding");
            // Status still Normal — risk tick is what changes it.
            assert_eq!(acct.status, RiskStatus::Normal);
        }

        // ── Phase 5: run_risk_tick emits LiquidationEvent ─────────────────────
        let events = risk.run_risk_tick();
        assert_eq!(events.len(), 1, "should emit exactly one liquidation event");
        let evt = &events[0];
        assert_eq!(evt.user_id, TAKER_ID, "event user_id");
        assert_eq!(evt.symbol, btc_sym(), "event symbol");
        assert_eq!(evt.side, PositionSide::Long, "event side");
        assert_eq!(evt.qty_lots, QTY_LOTS, "event qty");
        // The liq_price_ticks in the event is what the system computed at open
        // and stored on the position — it should match our formula.
        assert_eq!(
            evt.liq_price_ticks, LIQ_TICKS,
            "liq_price_ticks in event: expected {}  ($45,025), got {}",
            LIQ_TICKS, evt.liq_price_ticks
        );

        {
            let acct = risk.accounts.get(&TAKER_ID).unwrap();
            assert_eq!(acct.status, RiskStatus::LiquidationPending);
        }

        // ── Phase 6: desk_server sends liquidation market order ───────────────
        // (In production: tick task sets Liquidating, then places IOC market order.)
        if let Some(mut acct) = risk.accounts.get_mut(&TAKER_ID) {
            acct.status = RiskStatus::Liquidating;
        }

        // The liquidation order fills at $46,000 — above liq_price $45,025.
        // Exchange captures the spread; user is settled at liq_price (worse for user).
        let actual_fill_ticks: i64 = 4_600_000; // $46,000
        // liq_price from the event is passed to on_fill as liq_price_ticks.
        let liq_price_ticks: i64 = evt.liq_price_ticks;

        // Realized PnL at settlement price (liq_price):
        //   pnl = (liq_price - entry) × qty / scale
        //       = (4_502_500 - 5_000_000) × 100_000 / 1_000_000
        //       = -497_500 × 100 = -49_750 cents  (-$497.50)
        // ×1e6 → atoms (S2), mirroring the engine's settlement math.
        let realized_pnl: i64 = (liq_price_ticks - ENTRY_TICKS) * QTY_LOTS / SCALE * 1_000_000;
        assert_eq!(realized_pnl, -49_750 * 1_000_000, "pre-computed realized PnL");

        // Insurance fund = (actual_fill - liq_price) × qty / scale (sell side, sign=+1)
        //   = (4_600_000 - 4_502_500) × 100_000 / 1_000_000
        //   = 97_500 × 100 = 9_750 cents  ($97.50)
        let expected_insurance: i64 =
            (actual_fill_ticks - liq_price_ticks) * QTY_LOTS / SCALE * 1_000_000;
        assert_eq!(
            expected_insurance,
            9_750 * 1_000_000,
            "pre-computed insurance fund credit"
        );

        // fill_margin = 0: no margin was reserved for the system-generated liquidation order.
        risk.on_fill(
            TAKER_ID,
            btc_sym(),
            1,
            actual_fill_ticks,
            QTY_LOTS,
            0,
            SCALE,
            LEV,
            MBPS,
            liq_price_ticks,
        );

        // ── CHECK C: post-liquidation state ───────────────────────────────────
        // In production, pg-writer persists the closed position and updated balance.
        {
            // Position must be gone.
            assert!(
                risk.positions.get(&(TAKER_ID, btc_sym())).is_none(),
                "position should be removed after liquidation close"
            );

            let acct = risk.accounts.get(&TAKER_ID).unwrap();

            // Status resets to Normal so run_risk_tick can re-evaluate on next tick.
            assert_eq!(
                acct.status,
                RiskStatus::Normal,
                "status reset to Normal after liq close"
            );

            // No open position → no used margin, no unrealized PnL.
            assert_eq!(acct.used_margin, 0, "used_margin = 0 after close");
            assert_eq!(acct.unrealized_pnl, 0, "upnl = 0 after close");

            // available = prev_available + fill_margin(0) + released_used_margin(IM) + realized_pnl
            //           = (TAKER_BALANCE - IM) + 0 + IM + realized_pnl
            //           = TAKER_BALANCE + realized_pnl
            //           = 55_000 + (-49_750) = 5_250 cents  ($52.50)
            let expected_available = (TAKER_BALANCE - IM) + IM + realized_pnl; // 5_250
            assert_eq!(
                acct.available_margin.load(Relaxed),
                expected_available.max(0),
                "available_margin after liq: expected {} (${})",
                expected_available,
                expected_available / 100_000_000 // atoms → USDT for the message
            );

            // Equity = available + 0 + 0 + 0 = 5_250.
            assert_eq!(acct.equity, expected_available.max(0), "equity after close");

            // order_margin still 0 throughout.
            assert_eq!(acct.order_margin.load(Relaxed), 0, "order_margin = 0");

            // Insurance fund captures the fill-vs-liq-price spread.
            assert_eq!(
                risk.insurance_fund(),
                expected_insurance,
                "insurance fund: expected {} ({} fill - {} liq) × qty / scale",
                expected_insurance,
                actual_fill_ticks,
                liq_price_ticks
            );
        }
    }
}
