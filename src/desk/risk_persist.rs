//! Bridge from the in-memory margin engine to the persist stream — S1.3.
//!
//! After any state-changing risk event (fill, liquidation settlement) the
//! desk calls [`margin_state_frames`] and publishes the returned frames.
//! Frames carry ABSOLUTE state (not deltas), so consumers stay idempotent
//! under journal replay and at-least-once delivery; "position absent" maps
//! to PositionDelete because schema 022 defines flat as "no row".
//!
//! Units: the engine, the frames and the schema all speak ATOMS
//! (1e-8 USDT) since S2 — this module copies values through verbatim,
//! no conversion exists anywhere on this path.

use crate::desk::risk::RiskEngine;
use crate::transport::persist_event::{
    InsuranceFundSetPayload, PersistFrame, PositionDeletePayload, PositionUpsertPayload,
    RiskAccountSetPayload,
};
use std::sync::atomic::Ordering;

/// Snapshot the margin state touched by an event on (user, symbol) into
/// persist frames, in apply-safe order:
///   1. position upsert/delete for that symbol,
///   2. the user's account row,
///   3. the insurance fund (only changed by liquidations, but it is one
///      cheap absolute frame — publishing it unconditionally beats a
///      "did the fund change?" protocol).
///
/// Read-only and O(1): two DashMap point reads + one atomic load.
pub fn margin_state_frames(
    engine: &RiskEngine,
    user_id: i64,
    symbol: &[u8; 16],
) -> Vec<PersistFrame> {
    let mut frames = Vec::with_capacity(3);

    match engine.positions.get(&(user_id, *symbol)) {
        Some(pos) => {
            frames.push(PersistFrame::position_upsert(PositionUpsertPayload {
                user_id,
                symbol: *symbol,
                side: pos.side.to_u8(),
                leverage: pos.leverage,
                _pad: [0; 6],
                qty_lots: pos.qty_lots,
                entry_price_ticks: pos.entry_price_ticks,
                used_margin_atoms: pos.initial_margin,
            }));
        }
        None => {
            // Flat after a full close (or the event never opened anything):
            // an idempotent DELETE — removing an absent row is a no-op.
            frames.push(PersistFrame::position_delete(PositionDeletePayload {
                user_id,
                symbol: *symbol,
            }));
        }
    }

    if let Some(acct) = engine.accounts.get(&user_id) {
        frames.push(PersistFrame::risk_account_set(RiskAccountSetPayload {
            user_id,
            equity_atoms: acct.equity,
            used_margin_atoms: acct.used_margin,
            order_margin_atoms: acct.order_margin.load(Ordering::Relaxed),
            status: acct.status.to_u8(),
            _pad: [0; 7],
        }));
    }

    frames.push(PersistFrame::insurance_fund_set(InsuranceFundSetPayload {
        balance_atoms: engine.insurance_fund(),
    }));

    frames
}

/// Wait until pg-writer's checkpoint floors stop advancing (two identical
/// reads `interval` apart) before hydrating — S1.4 startup-race guard.
///
/// The scenario: desk dies, restarts quickly; pg-writer is still
/// journal-replaying the dead desk's last frames, so risk_accounts/
/// positions in PG lag the true pre-crash state by up to the replay tail.
/// Hydrating mid-catch-up would resurrect a stale book of positions.
/// "Floors quiet" is a heuristic, not a proof — but the tail is bounded
/// (flush every ~20ms + replay at memory speed), so two quiet reads make
/// staleness vanishingly unlikely; the reconcile sweep (S1.5) is the
/// backstop that would flag a miss.
pub async fn wait_for_writer_quiesce(
    pool: &sqlx::PgPool,
    interval: std::time::Duration,
    timeout: std::time::Duration,
) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    let mut prev = crate::desk::pg_store::load_checkpoints(pool).await.ok();
    while std::time::Instant::now() < deadline {
        tokio::time::sleep(interval).await;
        let cur = crate::desk::pg_store::load_checkpoints(pool).await.ok();
        if cur.is_some() && cur == prev {
            return true;
        }
        prev = cur;
    }
    false
}

/// Counters returned by [`hydrate_from_pg`] for the startup log line.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct HydrateStats {
    pub accounts: usize,
    pub positions: usize,
    pub insurance_fund_atoms: i64,
}

/// Rebuild the margin engine's durable state from PG — S1.4, the inverse
/// of the frame path. Call AFTER the naive balance seed: rows here are
/// authoritative and overwrite it. Volatile values are RECOMPUTED, not
/// loaded: mark price starts at entry (the first mark-price tick corrects
/// it within ~10ms), uPnL starts 0, maintenance/liq/bankruptcy prices are
/// derived from the persisted essentials via the same calc functions
/// on_fill uses — so a hydrated engine is field-equivalent to one that
/// never died, modulo the one mark-price tick.
pub async fn hydrate_from_pg(
    engine: &RiskEngine,
    pool: &sqlx::PgPool,
) -> anyhow::Result<HydrateStats> {
    use crate::desk::risk::calc;
    use crate::desk::risk::{AccountRiskState, PositionRiskState, PositionSide, RiskStatus};
    use sqlx::Row;
    let mut stats = HydrateStats::default();

    // ── Accounts ────────────────────────────────────────────────────────
    let rows = sqlx::query(
        "SELECT user_id, equity_atoms, used_margin_atoms, order_margin_atoms, status
           FROM risk_accounts",
    )
    .fetch_all(pool)
    .await?;
    for row in rows {
        let user_id: i64 = row.get("user_id");
        let equity = row.get::<i64, _>("equity_atoms");
        let used = row.get::<i64, _>("used_margin_atoms");
        let order = row.get::<i64, _>("order_margin_atoms");
        let status_s: String = row.get("status");
        let Some(status) = RiskStatus::from_db_str(&status_s) else {
            anyhow::bail!("risk_accounts user {user_id}: unknown status '{status_s}'");
        };
        // Persisted equity included uPnL at snapshot time; uPnL restarts
        // at 0 here, so available absorbs the difference until the first
        // mark-price update recomputes equity from live positions.
        let acct = AccountRiskState {
            user_id,
            equity,
            available_margin: std::sync::atomic::AtomicI64::new(equity - used - order),
            order_margin: std::sync::atomic::AtomicI64::new(order),
            used_margin: used,
            maintenance_margin: 0, // recomputed by the next risk tick
            unrealized_pnl: 0,
            status,
        };
        engine.accounts.insert(user_id, acct);
        stats.accounts += 1;
    }

    // ── Positions (+ the indexes on_fill normally maintains) ───────────
    let rows = sqlx::query(
        "SELECT user_id, symbol, side, qty_lots, entry_price_ticks, leverage,
                used_margin_atoms
           FROM positions",
    )
    .fetch_all(pool)
    .await?;
    // Account-level maintenance margin is the SUM over the user's
    // positions. on_fill maintains it incrementally; hydrate must
    // aggregate it here or the risk tick's "equity <= maintenance"
    // branch can never fire after a restart (caught by the round-trip
    // test: only the too-late Bankruptcy branch would remain).
    let mut maint_by_user: std::collections::HashMap<i64, i64> = Default::default();
    for row in rows {
        let user_id: i64 = row.get("user_id");
        let symbol_s: String = row.get("symbol");
        let symbol = crate::transport::persist_event::pack_str(&symbol_s);
        let side_s: String = row.get("side");
        let side = match side_s.as_str() {
            "long" => PositionSide::Long,
            "short" => PositionSide::Short,
            other => anyhow::bail!("positions user {user_id}: unknown side '{other}'"),
        };
        let qty_lots: i64 = row.get("qty_lots");
        let entry: i64 = row.get("entry_price_ticks");
        let leverage: u8 = row.get::<i16, _>("leverage") as u8;
        let margin_atoms: i64 = row.get("used_margin_atoms");

        // Recompute derived prices with the SAME calc path as on_fill.
        let rules = crate::desk::symbol_rules::SymbolRules::for_symbol(&symbol_s);
        let notional = calc::calc_notional_atoms(entry, qty_lots, rules.notional_scale);
        let maint = calc::calc_maintenance_margin_atoms(notional, rules.maintenance_rate_bps);
        let liq =
            calc::calc_liquidation_price_ticks(entry, leverage, rules.maintenance_rate_bps, side);
        let bkrpt = calc::calc_bankruptcy_price_ticks(entry, leverage, side);

        engine.positions.insert(
            (user_id, symbol),
            PositionRiskState {
                user_id,
                symbol,
                side,
                qty_lots,
                entry_price_ticks: entry,
                mark_price_ticks: entry, // corrected by the first mark tick
                unrealized_pnl: 0,
                initial_margin: margin_atoms,
                maintenance_margin: maint,
                liquidation_price_ticks: liq,
                bankruptcy_price_ticks: bkrpt,
                leverage,
            },
        );
        engine
            .symbol_position_index
            .entry(symbol)
            .or_default()
            .insert(user_id);
        engine
            .symbol_oi_lots
            .entry(symbol)
            .or_insert_with(|| std::sync::atomic::AtomicI64::new(0))
            .fetch_add(qty_lots, Ordering::Relaxed);
        *maint_by_user.entry(user_id).or_insert(0) += maint;
        stats.positions += 1;
    }
    for (user_id, maint) in maint_by_user {
        if let Some(mut acct) = engine.accounts.get_mut(&user_id) {
            acct.maintenance_margin = maint;
        }
    }

    // ── Insurance fund ──────────────────────────────────────────────────
    let fund_atoms: i64 =
        sqlx::query_scalar("SELECT balance_atoms FROM insurance_fund WHERE id = 1")
            .fetch_one(pool)
            .await?;
    stats.insurance_fund_atoms = fund_atoms;
    engine
        .insurance_fund_atoms
        .store(stats.insurance_fund_atoms, Ordering::Relaxed);

    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::desk::risk::{PositionSide, RiskStatus};
    use crate::transport::persist_event::{PersistKind, pack_str};

    const SYM: [u8; 16] = *b"BTC_USDT\0\0\0\0\0\0\0\0";

    fn engine_with_fill() -> std::sync::Arc<RiskEngine> {
        let engine = RiskEngine::new();
        engine.initialize_account(7, 1_000_000); // atoms (0.01 USDT — unit-agnostic test)
        // Real order sequence: reserve order margin, then fill moves it
        // into the position (otherwise equity double-counts the margin —
        // equity = available + used + order + uPnL).
        engine
            .check_and_reserve_margin(7, 10_000)
            .expect("reserve margin");
        // Open a long: 10 lots @ 50_000 ticks, margin 10_000 atoms.
        engine.on_fill(7, SYM, 0, 50_000, 10, 10_000, 1_000_000, 10, 50, 0);
        engine
    }

    #[test]
    fn open_position_emits_upsert_account_and_fund() {
        let engine = engine_with_fill();
        let frames = margin_state_frames(&engine, 7, &SYM);
        assert_eq!(frames.len(), 3);

        let pos = frames[0].as_position_upsert().expect("position frame");
        let (uid, side, qty, entry, margin) = (
            pos.user_id,
            pos.side,
            pos.qty_lots,
            pos.entry_price_ticks,
            pos.used_margin_atoms,
        );
        assert_eq!(uid, 7);
        assert_eq!(side, PositionSide::Long.to_u8());
        assert_eq!(qty, 10);
        assert_eq!(entry, 50_000);
        // verbatim copy (S2): the engine value IS the wire value.
        assert_eq!(margin, 10_000);
        assert_eq!(unpack_sym(&pos.symbol), "BTC_USDT");

        let acct = frames[1].as_risk_account_set().expect("account frame");
        let (uid, status) = (acct.user_id, acct.status);
        assert_eq!(uid, 7);
        assert_eq!(status, RiskStatus::Normal.to_u8());
        // equity is still the full deposit (no realized pnl on open).
        let eq = acct.equity_atoms;
        assert_eq!(eq, 1_000_000);

        let fund = frames[2].as_insurance_fund_set().expect("fund frame");
        let bal = fund.balance_atoms;
        assert_eq!(bal, 0);
    }

    #[test]
    fn flat_user_emits_delete() {
        let engine = RiskEngine::new();
        engine.initialize_account(8, 500_000);
        let frames = margin_state_frames(&engine, 8, &SYM);
        assert_eq!(frames[0].kind(), Some(PersistKind::PositionDelete));
        let del = frames[0].as_position_delete().expect("delete frame");
        let uid = del.user_id;
        assert_eq!(uid, 8);
    }

    /// S2 boundary guard: the frame must carry the engine's value
    /// VERBATIM — any scaling reintroduced here is a unit bug.
    #[test]
    fn frames_carry_engine_values_verbatim() {
        let engine = RiskEngine::new();
        engine.initialize_account(9, -123); // negative equity flows through
        engine
            .insurance_fund_atoms
            .store(-777, std::sync::atomic::Ordering::Relaxed);
        let frames = margin_state_frames(&engine, 9, &SYM);
        let acct = frames[1].as_risk_account_set().unwrap();
        let eq = acct.equity_atoms;
        assert_eq!(eq, -123);
        let fund = frames[2].as_insurance_fund_set().unwrap();
        let bal = fund.balance_atoms;
        assert_eq!(bal, -777);
    }

    fn unpack_sym(sym: &[u8; 16]) -> &str {
        let end = sym.iter().position(|&b| b == 0).unwrap_or(16);
        std::str::from_utf8(&sym[..end]).unwrap()
    }

    #[test]
    fn symbol_roundtrip_helper_matches_pack() {
        let packed = pack_str("ETH_USDT");
        assert_eq!(unpack_sym(&packed), "ETH_USDT");
    }
}
