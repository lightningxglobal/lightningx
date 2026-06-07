//! Perpetual-swap funding — S3.
//!
//! Anchors the perp to its index: every period (default 8h) longs pay
//! shorts (or vice versa) a rate derived from how far the mark trades
//! above/below the index.
//!
//! Architecture (decided for exactly-once):
//!   * the desk samples premium and, at the period boundary, computes the
//!     rate and publishes ONE `FundingSettled` frame;
//!   * pg-writer applies the whole settlement INSIDE its flush
//!     transaction (account deltas from the positions table, truncation
//!     residue → insurance fund, history row, next-period state) —
//!     exactly-once by the existing (publisher, seq) floor;
//!   * the desk mirrors the same deltas in memory using THE SAME
//!     [`compute_payment`] function over the same position set (its
//!     memory and the table agree at the frame's seq by construction),
//!     so there is exactly one implementation of the money math.
//!
//! Rates are integers in 1e-9 units ("e9"): 100_000 e9 = 0.01%.
//!
//! Index price (S4): behind [`IndexPriceSource`]; until the external
//! aggregation lands, the placeholder returns the book's own mark, which
//! makes premium ≈ 0 and funding ≈ the interest term — the correct
//! degenerate behavior, not a hack.

use crate::desk::risk::PositionSide;
use crate::desk::risk::calc;

/// 1e-9 rate unit scale.
pub const RATE_SCALE_E9: i128 = 1_000_000_000;

/// Pluggable index-price source (S4 will provide multi-source median).
pub trait IndexPriceSource: Send + Sync {
    /// Index price in ticks for the symbol, `None` when unavailable
    /// (samples are skipped — never guessed).
    fn index_price_ticks(&self, symbol: &[u8; 16]) -> Option<i64>;
}

#[derive(Clone, Copy, Debug)]
pub struct FundingConfig {
    /// Settlement period in seconds (default 8h).
    pub period_secs: u64,
    /// Fixed interest component per period, e9 (default 0.01% = 100_000).
    pub interest_e9: i64,
    /// Symmetric clamp on the final rate, e9 (default 0.75% = 7_500_000).
    pub clamp_e9: i64,
}

impl FundingConfig {
    pub fn from_env() -> Self {
        let get = |k: &str, d: i64| -> i64 {
            std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
        };
        Self {
            period_secs: get("FUNDING_PERIOD_SECS", 28_800) as u64,
            interest_e9: get("FUNDING_INTEREST_E9", 100_000),
            clamp_e9: get("FUNDING_CLAMP_E9", 7_500_000),
        }
    }
}

/// Accumulates premium samples over one funding period (plain integer
/// running mean — every sample weighs the same, i.e. a TWAP when sampled
/// on a fixed cadence).
#[derive(Default, Debug)]
pub struct PremiumTracker {
    sum_e9: i128,
    samples: u64,
}

impl PremiumTracker {
    /// premium_e9 = (mark − index) / index, in 1e-9 units.
    /// Skips nonsensical inputs (index must be positive).
    pub fn sample(&mut self, mark_ticks: i64, index_ticks: i64) {
        if index_ticks <= 0 || mark_ticks <= 0 {
            return;
        }
        let premium =
            (mark_ticks as i128 - index_ticks as i128) * RATE_SCALE_E9 / index_ticks as i128;
        self.sum_e9 += premium;
        self.samples += 1;
    }

    pub fn samples(&self) -> u64 {
        self.samples
    }

    /// TWAP of the collected samples, e9. Zero when no samples were taken
    /// (no signal → only the interest term funds).
    pub fn twap_e9(&self) -> i64 {
        if self.samples == 0 {
            return 0;
        }
        (self.sum_e9 / self.samples as i128) as i64
    }

    /// Final rate for the period: clamp(twap + interest, ±clamp), then
    /// reset for the next period.
    pub fn close_period(&mut self, cfg: &FundingConfig) -> i64 {
        let raw = self.twap_e9() as i128 + cfg.interest_e9 as i128;
        let clamped = raw.clamp(-(cfg.clamp_e9 as i128), cfg.clamp_e9 as i128) as i64;
        self.sum_e9 = 0;
        self.samples = 0;
        clamped
    }
}

/// The funding payment for one position, atoms. Positive = the LONG side
/// pays this amount (standard convention: rate > 0 → longs pay shorts).
///
/// payment = mark_value(qty) × rate / 1e9, i128 throughout, truncating
/// toward zero. This is THE money function — pg-writer and the desk's
/// in-memory mirror both call it; per-position truncation residue is
/// swept into the insurance fund by the caller so the venue-wide books
/// stay exactly zero-sum (S2.4 law).
pub fn compute_payment(qty_lots: i64, mark_ticks: i64, notional_scale: i64, rate_e9: i64) -> i64 {
    let value = calc::calc_notional_atoms(mark_ticks, qty_lots, notional_scale) as i128;
    (value * rate_e9 as i128 / RATE_SCALE_E9) as i64
}

/// One user's settlement delta.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FundingDelta {
    pub user_id: i64,
    /// Signed equity change in atoms (negative = paid).
    pub equity_delta_atoms: i64,
}

/// Settlement outcome over a position set: per-user deltas + the residue
/// that keeps Σ(deltas) + residue == 0 exactly.
#[derive(Debug, Default)]
pub struct FundingSettlement {
    pub deltas: Vec<FundingDelta>,
    pub long_paid_atoms: i64,
    pub short_received_atoms: i64,
    /// → insurance fund. Σ(equity deltas) + residue == 0, EXACT.
    pub residue_atoms: i64,
}

/// Compute the full settlement for one symbol from an iterator of
/// (user_id, side, qty_lots). Pure — shared verbatim by pg-writer (rows
/// from the positions table) and the desk (its in-memory DashMap).
///
/// rate > 0: longs pay |payment|, shorts receive |payment|.
/// rate < 0: mirrored. Per-position truncation makes Σpaid ≠ Σreceived
/// by a few atoms; the difference lands in `residue_atoms` (fund), so
/// conservation is exact regardless of the partition of open interest.
pub fn compute_settlement(
    positions: impl Iterator<Item = (i64, PositionSide, i64)>,
    mark_ticks: i64,
    notional_scale: i64,
    rate_e9: i64,
) -> FundingSettlement {
    let mut out = FundingSettlement::default();
    if rate_e9 == 0 {
        return out;
    }
    for (user_id, side, qty_lots) in positions {
        let p = compute_payment(qty_lots, mark_ticks, notional_scale, rate_e9);
        // rate>0: long pays p, short receives p. rate<0: p is negative —
        // long "pays" a negative amount (receives), short mirrors.
        let delta = match side {
            PositionSide::Long => -p,
            PositionSide::Short => p,
        };
        if delta == 0 {
            continue;
        }
        if delta < 0 {
            out.long_paid_atoms += -delta;
        } else {
            out.short_received_atoms += delta;
        }
        out.deltas.push(FundingDelta {
            user_id,
            equity_delta_atoms: delta,
        });
    }
    // Whatever the per-position truncation left over goes to the fund:
    // Σdeltas + residue == 0 exactly, by definition of residue.
    let sum: i64 = out.deltas.iter().map(|d| d.equity_delta_atoms).sum();
    out.residue_atoms = -sum;
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::desk::risk::PositionSide::{Long, Short};

    const SCALE: i64 = 1_000_000;

    #[test]
    fn premium_twap_and_clamp() {
        let cfg = FundingConfig {
            period_secs: 28_800,
            interest_e9: 100_000,
            clamp_e9: 7_500_000,
        };
        let mut t = PremiumTracker::default();
        // mark 1% above index → premium = 1e7 e9 each sample.
        t.sample(5_050_000, 5_000_000);
        t.sample(5_050_000, 5_000_000);
        assert_eq!(t.twap_e9(), 10_000_000);
        // 1% + 0.01% interest would exceed the 0.75% clamp.
        assert_eq!(t.close_period(&cfg), 7_500_000);
        // Reset: next period starts clean → interest only.
        assert_eq!(t.samples(), 0);
        assert_eq!(t.close_period(&cfg), 100_000);

        // Negative premium clamps symmetrically.
        let mut t = PremiumTracker::default();
        t.sample(4_900_000, 5_000_000); // -2%
        assert_eq!(t.close_period(&cfg), -7_500_000);

        // Garbage inputs are skipped, not booked.
        let mut t = PremiumTracker::default();
        t.sample(0, 5_000_000);
        t.sample(5_000_000, 0);
        t.sample(5_000_000, -1);
        assert_eq!(t.samples(), 0);
    }

    #[test]
    fn payment_math_known_values() {
        // 0.1 BTC at $50k = 5_000 USDT value = 5e11 atoms.
        // rate 0.01% (100_000 e9) → payment = 5e11 × 1e5 / 1e9 = 50_000_000
        // atoms = 0.5 USDT.
        assert_eq!(compute_payment(100_000, 5_000_000, SCALE, 100_000), 50_000_000);
        // Negative rate mirrors.
        assert_eq!(compute_payment(100_000, 5_000_000, SCALE, -100_000), -50_000_000);
    }

    #[test]
    fn settlement_is_exactly_zero_sum_with_residue() {
        // Long OI == short OI (venue invariant) but partitioned unevenly,
        // with quantities chosen so per-position truncation differs.
        let positions = vec![
            (1, Long, 33_333),
            (2, Long, 66_667), // longs: 100_000 total
            (3, Short, 99_999),
            (4, Short, 1), // shorts: 100_000 total
        ];
        let s = compute_settlement(positions.into_iter(), 5_000_001, SCALE, 100_000);
        let sum: i64 = s.deltas.iter().map(|d| d.equity_delta_atoms).sum();
        assert_eq!(sum + s.residue_atoms, 0, "conservation is exact by construction");
        assert!(s.long_paid_atoms > 0 && s.short_received_atoms > 0);
        // The dust position pays/receives 0 and is omitted.
        assert!(s.deltas.iter().all(|d| d.equity_delta_atoms != 0));
    }

    #[test]
    fn zero_rate_settles_nothing() {
        let s = compute_settlement(
            vec![(1, Long, 100_000), (2, Short, 100_000)].into_iter(),
            5_000_000,
            SCALE,
            0,
        );
        assert!(s.deltas.is_empty());
        assert_eq!(s.residue_atoms, 0);
    }
}
