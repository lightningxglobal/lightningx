//! Bridge from the in-memory margin engine to the persist stream — S1.3.
//!
//! After any state-changing risk event (fill, liquidation settlement) the
//! desk calls [`margin_state_frames`] and publishes the returned frames.
//! Frames carry ABSOLUTE state (not deltas), so consumers stay idempotent
//! under journal replay and at-least-once delivery; "position absent" maps
//! to PositionDelete because schema 022 defines flat as "no row".
//!
//! Unit boundary (checklist S2): the engine still computes in CENTS
//! (1e-2 USDT) while the durable schema is ATOMS (1e-8 USDT). This module
//! is the ONE place where cents→atoms happens; when S2 unifies the engine
//! on atoms, `CENTS_TO_ATOMS` disappears and this becomes a plain copy.

use crate::desk::risk::RiskEngine;
use crate::transport::persist_event::{
    InsuranceFundSetPayload, PersistFrame, PositionDeletePayload, PositionUpsertPayload,
    RiskAccountSetPayload,
};
use std::sync::atomic::Ordering;

/// 1 cent = 0.01 USDT = 10^6 atoms (atoms are 1e-8 USDT).
const CENTS_TO_ATOMS: i64 = 1_000_000;

/// Saturating cents→atoms. Margins/equity are externally bounded well
/// below i64::MAX/1e6 (≈92 billion USDT); saturation is a belt-and-braces
/// guard so a corrupted in-memory value can never wrap into a *plausible*
/// wrong number on disk — it pins to the rail instead, which reconcile
/// (S1.5) flags immediately.
fn cents_to_atoms(cents: i64) -> i64 {
    cents.saturating_mul(CENTS_TO_ATOMS)
}

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
                used_margin_atoms: cents_to_atoms(pos.initial_margin),
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
            equity_atoms: cents_to_atoms(acct.equity),
            used_margin_atoms: cents_to_atoms(acct.used_margin),
            order_margin_atoms: cents_to_atoms(acct.order_margin.load(Ordering::Relaxed)),
            status: acct.status.to_u8(),
            _pad: [0; 7],
        }));
    }

    frames.push(PersistFrame::insurance_fund_set(InsuranceFundSetPayload {
        balance_atoms: cents_to_atoms(engine.insurance_fund()),
    }));

    frames
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::desk::risk::{PositionSide, RiskStatus};
    use crate::transport::persist_event::{PersistKind, pack_str};

    const SYM: [u8; 16] = *b"BTC_USDT\0\0\0\0\0\0\0\0";

    fn engine_with_fill() -> std::sync::Arc<RiskEngine> {
        let engine = RiskEngine::new();
        engine.initialize_account(7, 1_000_000); // 10,000 USDT in cents
        // Real order sequence: reserve order margin, then fill moves it
        // into the position (otherwise equity double-counts the margin —
        // equity = available + used + order + uPnL).
        engine
            .check_and_reserve_margin(7, 10_000)
            .expect("reserve margin");
        // Open a long: 10 lots @ 50_000 ticks, margin 100_00 cents.
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
        // cents → atoms exactly once: 10_000 cents = 100 USDT = 1e10 atoms.
        assert_eq!(margin, 10_000 * 1_000_000);
        assert_eq!(unpack_sym(&pos.symbol), "BTC_USDT");

        let acct = frames[1].as_risk_account_set().expect("account frame");
        let (uid, status) = (acct.user_id, acct.status);
        assert_eq!(uid, 7);
        assert_eq!(status, RiskStatus::Normal.to_u8());
        // equity is still the full deposit (no realized pnl on open).
        let eq = acct.equity_atoms;
        assert_eq!(eq, 1_000_000 * 1_000_000);

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

    #[test]
    fn saturating_conversion_never_wraps() {
        assert_eq!(cents_to_atoms(i64::MAX), i64::MAX);
        assert_eq!(cents_to_atoms(i64::MIN), i64::MIN);
        assert_eq!(cents_to_atoms(-5), -5_000_000); // negative equity flows through
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
