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
    pub fn from_env() -> anyhow::Result<Self> {
        let get = |k: &str, d: i64| -> i64 {
            std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
        };
        let clamp_e9 = get("FUNDING_CLAMP_E9", 7_500_000);
        let interest_e9 = get("FUNDING_INTEREST_E9", 100_000);
        // B7: reject nonsensical values that would silently disable or corrupt funding math.
        if clamp_e9 < 0 {
            return Err(anyhow::anyhow!(
                "FUNDING_CLAMP_E9 must be >= 0, got {}",
                clamp_e9
            ));
        }
        if interest_e9.abs() > 1_000_000 {
            return Err(anyhow::anyhow!(
                "FUNDING_INTEREST_E9 out of range (|{}| > 1_000_000)",
                interest_e9
            ));
        }
        Ok(Self {
            period_secs: get("FUNDING_PERIOD_SECS", 28_800) as u64,
            interest_e9,
            clamp_e9,
        })
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

/// Mirror a computed settlement into the in-memory engine — the desk-side
/// twin of pg-writer's transactional apply. Equity and available move by
/// the same delta (funding is realized cash, not margin), the residue
/// lands in the fund. Call ONLY with the settlement built from this
/// engine's own positions at the same instant the frame is published.
pub fn apply_in_memory(engine: &crate::desk::risk::RiskEngine, s: &FundingSettlement) {
    use std::sync::atomic::Ordering;
    for d in &s.deltas {
        if let Some(mut acct) = engine.accounts.get_mut(&d.user_id) {
            acct.equity += d.equity_delta_atoms;
            acct.available_margin
                .fetch_add(d.equity_delta_atoms, Ordering::Relaxed);
        }
    }
    if s.residue_atoms != 0 {
        engine
            .insurance_fund_atoms
            .fetch_add(s.residue_atoms, Ordering::Relaxed);
    }
}

/// Per-symbol settlement schedule, desk-side. The DURABLE anchor is
/// funding_state (advanced by pg-writer inside the settlement
/// transaction); the desk loads it at startup and only tracks the next
/// boundary in memory between settlements — a restart therefore resumes
/// the schedule exactly where the last committed settlement left it
/// (S3.5: can neither skip nor repeat).
#[derive(Debug)]
pub struct FundingScheduler {
    pub next_settlement_at_ms: i64,
    period_ms: i64,
}

impl FundingScheduler {
    /// `persisted` = funding_state row if any; otherwise the first
    /// settlement lands one full period from now (no retroactive charge
    /// on a freshly listed symbol).
    pub fn new(persisted_next_ms: Option<i64>, now_ms: i64, cfg: &FundingConfig) -> Self {
        let period_ms = cfg.period_secs as i64 * 1000;
        Self {
            next_settlement_at_ms: persisted_next_ms.unwrap_or(now_ms + period_ms),
            period_ms,
        }
    }

    /// True when the boundary has passed; advances to the NEXT boundary
    /// and returns the one being settled. Catch-up after long downtime
    /// settles period-by-period (each call returns one boundary) so the
    /// history stays honest rather than collapsing N periods into one.
    pub fn due(&mut self, now_ms: i64) -> Option<i64> {
        if now_ms < self.next_settlement_at_ms {
            return None;
        }
        let boundary = self.next_settlement_at_ms;
        self.next_settlement_at_ms += self.period_ms;
        Some(boundary)
    }
}

/// Load persisted next-settlement times for the desk's symbols.
pub async fn load_funding_schedule(
    pool: &sqlx::PgPool,
) -> anyhow::Result<std::collections::HashMap<String, i64>> {
    use sqlx::Row;
    let rows = sqlx::query(
        "SELECT symbol, (EXTRACT(EPOCH FROM next_settlement_at) * 1000)::bigint AS ms
           FROM funding_state",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| (r.get::<String, _>("symbol"), r.get::<i64, _>("ms")))
        .collect())
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
    fn scheduler_resumes_from_persisted_anchor_and_catches_up() {
        let cfg = FundingConfig {
            period_secs: 28_800,
            interest_e9: 0,
            clamp_e9: 1,
        };
        let period = 28_800_000i64;
        // Fresh symbol: first boundary one period out, nothing due now.
        let mut s = FundingScheduler::new(None, 1_000_000, &cfg);
        assert_eq!(s.due(1_000_000), None);
        assert_eq!(s.due(1_000_000 + period), Some(1_000_000 + period));

        // Restart with a persisted FUTURE anchor: must NOT settle early
        // (the last settlement already advanced the state).
        let mut s = FundingScheduler::new(Some(2_000_000_000), 1_999_999_999, &cfg);
        assert_eq!(s.due(1_999_999_999), None, "no double settlement after restart");

        // Restart after long downtime: catch up one period per call,
        // every boundary accounted for.
        let mut s = FundingScheduler::new(Some(1_000_000), 1_000_000 + 3 * period + 5, &cfg);
        let now = 1_000_000 + 3 * period + 5;
        assert_eq!(s.due(now), Some(1_000_000));
        assert_eq!(s.due(now), Some(1_000_000 + period));
        assert_eq!(s.due(now), Some(1_000_000 + 2 * period));
        assert_eq!(s.due(now), Some(1_000_000 + 3 * period));
        assert_eq!(s.due(now), None, "caught up");
    }

    #[test]
    fn in_memory_apply_mirrors_settlement_and_keeps_conservation() {
        use crate::desk::risk::RiskEngine;
        let engine = RiskEngine::new();
        engine.initialize_account(1, 1_000_000_000);
        engine.initialize_account(2, 1_000_000_000);
        let e0: i64 = engine.accounts.iter().map(|a| a.equity).sum();
        let s = compute_settlement(
            vec![(1, Long, 33_333), (2, Short, 33_333)].into_iter(),
            5_000_001,
            SCALE,
            100_000,
        );
        apply_in_memory(&engine, &s);
        let e1: i64 = engine.accounts.iter().map(|a| a.equity).sum();
        assert_eq!(
            e1 + engine.insurance_fund(),
            e0,
            "in-memory mirror preserves the conservation law exactly"
        );
        // Long paid: equity strictly down; short received.
        assert!(engine.accounts.get(&1).unwrap().equity < 1_000_000_000);
        assert!(engine.accounts.get(&2).unwrap().equity > 1_000_000_000);
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

    /// B7: negative clamp must return Err.
    #[test]
    fn negative_clamp_returns_err() {
        std::env::set_var("FUNDING_CLAMP_E9", "-1");
        let result = FundingConfig::from_env();
        std::env::remove_var("FUNDING_CLAMP_E9");
        assert!(
            result.is_err(),
            "B7: negative FUNDING_CLAMP_E9 must return Err"
        );
    }
}
