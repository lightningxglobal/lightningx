//! Trigger-order engine (stop / take-profit) — S5.
//!
//! Hot-path contract: the index is two BTreeMaps per symbol keyed by
//! (trigger_price, id). A mark move costs O(log n) to find the crossing
//! boundary plus O(k) for the k orders that actually fire — never a full
//! scan (checklist S5.2 forbids ≥O(n) here).
//!
//! Exactly-once firing is anchored in PG, not memory: the desk performs
//! `UPDATE … SET status='triggered', triggered_order_id=$oid WHERE id=$id
//! AND status='pending'` and only a rows_affected==1 winner injects the
//! order. Restart recovery (`needs_reinjection`) re-injects a
//! 'triggered' row only when its pre-allocated order id is provably
//! absent from both `orders` and `matching_events` — an injected order
//! ALWAYS leaves one of those footprints (the desk inserts the orders
//! row before publishing; the engine's ACCEPT appends a matching event).

use crate::desk::risk::PositionSide;
use std::collections::BTreeMap;

/// In-memory copy of a pending trigger row (what firing needs).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingTrigger {
    pub id: i64,
    pub user_id: i64,
    pub side: u8,       // 0 = buy, 1 = sell
    pub is_market: bool,
    pub trigger_price_ticks: i64,
    /// Limit price; for market orders the firing path substitutes an
    /// aggressive cap derived from the mark.
    pub price_ticks: Option<i64>,
    pub qty_lots: i64,
}

/// Which side of the mark the trigger arms on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerWhen {
    /// Fire when mark >= trigger (buy-stop / short take-profit).
    Rising,
    /// Fire when mark <= trigger (sell-stop / long take-profit).
    Falling,
}

impl TriggerWhen {
    pub fn as_db_str(self) -> &'static str {
        match self {
            TriggerWhen::Rising => "rising",
            TriggerWhen::Falling => "falling",
        }
    }

    pub fn from_db_str(s: &str) -> Option<Self> {
        Some(match s {
            "rising" => TriggerWhen::Rising,
            "falling" => TriggerWhen::Falling,
            _ => return None,
        })
    }

    /// The conventional default when the user doesn't specify: a stop
    /// order protects an existing position, so a SELL trigger fires on
    /// the way DOWN and a BUY trigger on the way UP.
    pub fn default_for_side(side: u8) -> Self {
        if side == 0 { TriggerWhen::Rising } else { TriggerWhen::Falling }
    }
}

/// Per-symbol trigger book.
#[derive(Default)]
pub struct TriggerBook {
    /// Fire when mark >= key.0 — ascending; due set = range ..=mark.
    rising: BTreeMap<(i64, i64), PendingTrigger>,
    /// Fire when mark <= key.0 — due set = range mark..
    falling: BTreeMap<(i64, i64), PendingTrigger>,
}

impl TriggerBook {
    pub fn insert(&mut self, when: TriggerWhen, t: PendingTrigger) {
        let key = (t.trigger_price_ticks, t.id);
        match when {
            TriggerWhen::Rising => self.rising.insert(key, t),
            TriggerWhen::Falling => self.falling.insert(key, t),
        };
    }

    /// Remove by id (user cancel). O(log n) given the trigger price.
    pub fn remove(&mut self, when: TriggerWhen, trigger_price_ticks: i64, id: i64) -> bool {
        let key = (trigger_price_ticks, id);
        match when {
            TriggerWhen::Rising => self.rising.remove(&key).is_some(),
            TriggerWhen::Falling => self.falling.remove(&key).is_some(),
        }
    }

    pub fn len(&self) -> usize {
        self.rising.len() + self.falling.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Pop every trigger due at `mark`. Crossing-boundary lookup is
    /// O(log n); extraction is O(k log n) for the k fired orders.
    /// Equality fires (mark == trigger is a touch).
    pub fn due(&mut self, mark_ticks: i64) -> Vec<PendingTrigger> {
        let mut fired = Vec::new();
        // Rising: all triggers with price <= mark.
        loop {
            let Some((&key, _)) = self.rising.range(..=(mark_ticks, i64::MAX)).next_back()
            else {
                break;
            };
            // next_back gives the HIGHEST due key; popping from the front
            // would also work — order among simultaneously-due triggers
            // is by (price, id), deterministic either way.
            fired.push(self.rising.remove(&key).expect("present"));
            if self.rising.range(..=(mark_ticks, i64::MAX)).next().is_none() {
                break;
            }
        }
        // Falling: all triggers with price >= mark.
        loop {
            let Some((&key, _)) = self.falling.range((mark_ticks, i64::MIN)..).next() else {
                break;
            };
            fired.push(self.falling.remove(&key).expect("present"));
        }
        fired
    }
}

/// Restart-recovery decision for a 'triggered' row: re-inject ONLY when
/// the pre-allocated order id left no footprint. `has_orders_row` /
/// `has_matching_event` are the two EXISTS probes (see module docs for
/// why their union is complete).
pub fn needs_reinjection(has_orders_row: bool, has_matching_event: bool) -> bool {
    !has_orders_row && !has_matching_event
}

/// S5.4 — margin the firing order must reserve before injection, atoms.
/// Same math as the regular order-entry path; a failure cancels the
/// trigger (with reason) instead of injecting a doomed order.
pub fn firing_margin_atoms(
    price_ticks: i64,
    qty_lots: i64,
    notional_scale: i64,
    leverage: u8,
) -> i64 {
    let notional = crate::desk::risk::calc::calc_notional_atoms(price_ticks, qty_lots, notional_scale);
    crate::desk::risk::calc::calc_initial_margin_atoms(notional, leverage)
}

/// Side of the POSITION this trigger reduces, for reduce-only semantics
/// later (S6); unused in v1 but kept next to the math it belongs to.
pub fn closes_side(order_side: u8) -> PositionSide {
    if order_side == 0 { PositionSide::Short } else { PositionSide::Long }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(id: i64, trigger: i64) -> PendingTrigger {
        PendingTrigger {
            id,
            user_id: 1,
            side: 1,
            is_market: true,
            trigger_price_ticks: trigger,
            price_ticks: None,
            qty_lots: 10,
        }
    }

    #[test]
    fn falling_fires_at_or_below_trigger_only() {
        let mut book = TriggerBook::default();
        for (id, trig) in [(1, 4_900_000), (2, 4_950_000), (3, 4_800_000)] {
            book.insert(TriggerWhen::Falling, t(id, trig));
        }
        // Mark drops to 4_900_000: triggers at 4_900_000 and 4_950_000
        // are touched (mark <= trigger); 4_800_000 still armed.
        let fired = book.due(4_900_000);
        let mut ids: Vec<i64> = fired.iter().map(|x| x.id).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![1, 2]);
        assert_eq!(book.len(), 1, "deep trigger still armed");
        // Equality touch on the survivor.
        let fired = book.due(4_800_000);
        assert_eq!(fired.len(), 1);
        assert!(book.is_empty());
    }

    #[test]
    fn rising_fires_at_or_above_trigger_only() {
        let mut book = TriggerBook::default();
        book.insert(TriggerWhen::Rising, t(1, 5_100_000));
        book.insert(TriggerWhen::Rising, t(2, 5_200_000));
        assert!(book.due(5_099_999).is_empty(), "below both");
        let fired = book.due(5_100_000);
        assert_eq!(fired.len(), 1, "touch fires");
        assert_eq!(fired[0].id, 1);
        let fired = book.due(5_300_000);
        assert_eq!(fired.len(), 1, "cross fires the rest");
    }

    #[test]
    fn cancel_removes_without_firing() {
        let mut book = TriggerBook::default();
        book.insert(TriggerWhen::Falling, t(1, 4_900_000));
        assert!(book.remove(TriggerWhen::Falling, 4_900_000, 1));
        assert!(!book.remove(TriggerWhen::Falling, 4_900_000, 1), "second cancel is a no-op");
        assert!(book.due(0).is_empty(), "cancelled trigger never fires");
    }

    #[test]
    fn storm_extraction_is_not_linear_per_tick() {
        // S5.5 shape check: 100k armed triggers far from the mark; a
        // non-crossing tick must do O(log n) work — bounded by wall time
        // here (loose, but catches an accidental full scan immediately).
        let mut book = TriggerBook::default();
        for id in 0..100_000 {
            book.insert(TriggerWhen::Falling, t(id, 1_000_000 + id));
        }
        let start = std::time::Instant::now();
        for mark in 6_000_000..6_010_000 {
            assert!(book.due(mark).is_empty());
        }
        assert!(
            start.elapsed() < std::time::Duration::from_millis(200),
            "10k non-crossing ticks over 100k triggers took {:?} — smells like O(n)",
            start.elapsed()
        );
        // And a single crossing tick fires all 100k exactly once.
        let fired = book.due(0);
        assert_eq!(fired.len(), 100_000);
        assert!(book.is_empty());
    }

    #[test]
    fn recovery_truth_table() {
        assert!(needs_reinjection(false, false), "no footprint → re-inject");
        assert!(!needs_reinjection(true, false), "orders row → already injected");
        assert!(!needs_reinjection(false, true), "matching event → already injected");
        assert!(!needs_reinjection(true, true));
    }

    #[test]
    fn default_arming_matches_stop_semantics() {
        assert_eq!(TriggerWhen::default_for_side(0), TriggerWhen::Rising, "buy-stop");
        assert_eq!(TriggerWhen::default_for_side(1), TriggerWhen::Falling, "sell-stop");
    }
}
