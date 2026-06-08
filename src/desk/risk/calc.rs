use super::types::PositionSide;

/// Money flows through the risk engine in ATOMS (1e-8 USDT) — the same
/// unit as the ledger, the persist frames and schema 022 (decision
/// S2.2). `notional_scale` keeps its historical meaning ("divide
/// ticks×lots to get cents"), so landing in atoms multiplies by the
/// 1e6 atoms-per-cent factor; intermediates are i128, making overflow
/// arithmetically impossible for any representable position
/// (max |ticks×lots×1e6| ≪ i128::MAX).
const ATOMS_PER_CENT: i128 = 1_000_000;

pub fn calc_notional_atoms(price_ticks: i64, qty_lots: i64, notional_scale: i64) -> i64 {
    ((price_ticks as i128 * qty_lots as i128 * ATOMS_PER_CENT) / notional_scale as i128) as i64
}

pub fn calc_initial_margin_atoms(notional_atoms: i64, leverage: u8) -> i64 {
    if leverage == 0 {
        return 0;
    }
    notional_atoms / leverage as i64
}

pub fn calc_maintenance_margin_atoms(notional_atoms: i64, rate_bps: i64) -> i64 {
    ((notional_atoms as i128 * rate_bps as i128) / 10_000) as i64
}

pub fn calc_liquidation_price_ticks(
    entry_ticks: i64,
    leverage: u8,
    maintenance_rate_bps: i64,
    side: PositionSide,
) -> i64 {
    if leverage == 0 {
        return 0;
    }
    let lev = leverage as i128;
    let entry = entry_ticks as i128;
    let rate = maintenance_rate_bps as i128;
    let denom = lev * 10_000;
    match side {
        PositionSide::Long => {
            let numer = entry * (lev * 10_000 - 10_000 + rate);
            (numer / denom) as i64
        }
        PositionSide::Short => {
            let numer = entry * (lev * 10_000 + 10_000 - rate);
            (numer / denom) as i64
        }
    }
}

pub fn calc_bankruptcy_price_ticks(entry_ticks: i64, leverage: u8, side: PositionSide) -> i64 {
    let lev = leverage as i128;
    let entry = entry_ticks as i128;
    match side {
        PositionSide::Long => ((entry * (lev - 1)) / lev) as i64,
        PositionSide::Short => ((entry * (lev + 1)) / lev) as i64,
    }
}

pub fn calc_unrealized_pnl_atoms(
    side: PositionSide,
    qty_lots: i64,
    entry_price_ticks: i64,
    mark_price_ticks: i64,
    notional_scale: i64,
) -> i64 {
    let diff = match side {
        PositionSide::Long => mark_price_ticks - entry_price_ticks,
        PositionSide::Short => entry_price_ticks - mark_price_ticks,
    };
    ((diff as i128 * qty_lots as i128 * ATOMS_PER_CENT) / notional_scale as i128) as i64
}

/// S7.1 — fee on a fill: notional × fee_bps / 10_000, atoms. Negative
/// bps (maker rebate) yields a negative fee (a credit). i128 throughout.
pub fn fee_atoms(notional_atoms: i64, fee_bps: i64) -> i64 {
    ((notional_atoms as i128 * fee_bps as i128) / 10_000) as i64
}

/// S6.1 — tiered liquidation: liquidate `tranche_bps` of the position
/// per round instead of the whole book at once; each partial close
/// resets the account to Normal and the next risk tick re-evaluates, so
/// rounds converge naturally. A残 tail smaller than 20% of the original
/// is folded into the tranche (a dust position isn't worth a round).
/// tranche_bps = 10_000 → full liquidation (the pre-S6 behavior).
pub fn liquidation_tranche_lots(position_lots: i64, tranche_bps: i64) -> i64 {
    if tranche_bps >= 10_000 || position_lots <= 0 {
        return position_lots;
    }
    let tranche = ((position_lots as i128 * tranche_bps as i128) / 10_000) as i64;
    let tranche = tranche.max(1);
    let remainder = position_lots - tranche;
    if remainder * 5 < position_lots {
        position_lots // fold the dust tail into this round
    } else {
        tranche
    }
}

/// S6.3 — leverage risk tiers: the bigger the position's notional, the
/// less leverage it may use. Returns the max leverage permitted at
/// `notional_atoms`. Tiers ascend; the first bound that fits wins.
/// Format mirrors RISK_TIERS env: "bound_usdt:lev,bound:lev,:lev"
/// (empty bound = catch-all).
pub fn max_leverage_for_notional(notional_atoms: i64, tiers: &[(i64, u8)]) -> u8 {
    for &(bound_atoms, lev) in tiers {
        if bound_atoms == i64::MAX || notional_atoms <= bound_atoms {
            return lev;
        }
    }
    1 // empty/misconfigured table: most conservative
}

/// Parse "1000000:20,5000000:10,:5" (bounds in whole USDT) into atoms
/// tiers. Unparseable entries are logged and skipped; an empty result means
/// "no tiering" and callers should treat it as disabled.
pub fn parse_risk_tiers(spec: &str) -> Vec<(i64, u8)> {
    let tiers: Vec<(i64, u8)> = spec
        .split(',')
        .filter_map(|part| {
            let Some((bound, lev)) = part.split_once(':') else {
                // B6: log each malformed entry so operators notice misconfiguration.
                tracing::error!("RISK_TIERS: invalid entry (missing ':'): {:?}", part);
                return None;
            };
            let Ok(lev) = lev.trim().parse::<u8>() else {
                tracing::error!("RISK_TIERS: invalid leverage in entry {:?}", part);
                return None;
            };
            let bound_atoms = if bound.trim().is_empty() {
                i64::MAX
            } else {
                let Ok(usdt) = bound.trim().parse::<i64>() else {
                    tracing::error!("RISK_TIERS: invalid bound in entry {:?}", part);
                    return None;
                };
                let Some(atoms) = usdt.checked_mul(100_000_000) else {
                    tracing::error!("RISK_TIERS: bound overflow in entry {:?}", part);
                    return None;
                };
                atoms
            };
            Some((bound_atoms, lev))
        })
        .collect();
    // B6: if every entry failed to parse, leverage limits are silently disabled.
    if tiers.is_empty() && !spec.trim().is_empty() {
        tracing::warn!(
            "leverage limits disabled -- all RISK_TIERS entries failed to parse (spec={:?})",
            spec
        );
    }
    tiers
}

#[cfg(test)]
mod tests {
    use super::super::types::PositionSide;
    use super::*;

    // BTC_USDT: price_tick=0.01, qty_step=0.000001
    // notional_scale = 1_000_000
    // price = $50,000 → price_ticks = 5_000_000 (50000 / 0.01)
    // qty = 0.001 BTC → qty_lots = 1000 (0.001 / 0.000001)
    // notional = $50 → notional_atoms = 5_000_000_000 (50 USDT × 1e8)
    const BTC_SCALE: i64 = 1_000_000;
    const A: i64 = 1_000_000; // atoms per cent, for readable expectations

    #[test]
    fn notional_btc_50k_001btc() {
        // price = 50000, price_ticks = 50000 / 0.01 = 5_000_000
        // qty = 0.001, qty_lots = 0.001 / 0.000001 = 1000
        // notional = price_ticks * qty_lots / scale = 5_000_000 * 1000 / 1_000_000 = 5000 cents = $50
        let price_ticks = 5_000_000i64;
        let qty_lots = 1000i64;
        let notional = calc_notional_atoms(price_ticks, qty_lots, BTC_SCALE);
        assert_eq!(notional, 5000 * A); // $50 in atoms
    }

    #[test]
    fn initial_margin_10x() {
        // notional $50, leverage 10 → initial margin $5
        let margin = calc_initial_margin_atoms(5000 * A, 10);
        assert_eq!(margin, 500 * A);
    }

    #[test]
    fn maintenance_margin_50bps() {
        // notional $50, rate 50bps → mm = $0.25
        let mm = calc_maintenance_margin_atoms(5000 * A, 50);
        assert_eq!(mm, 25 * A);
    }

    #[test]
    fn liquidation_price_long_10x_50bps() {
        // entry = $50,000, leverage = 10, rate = 50bps
        // liq = entry * (10 * 10000 - 10000 + 50) / (10 * 10000)
        //     = entry * (100000 - 10000 + 50) / 100000
        //     = entry * 90050 / 100000
        //     = 50000 * 90050 / 100000 = 45025
        let price_ticks = 5_000_000i64;
        let liq = calc_liquidation_price_ticks(price_ticks, 10, 50, PositionSide::Long);
        // 5_000_000 * 90050 / 100000 = 4_502_500
        assert_eq!(liq, 4_502_500);
    }

    #[test]
    fn liquidation_price_short_10x_50bps() {
        // short: entry * (10 * 10000 + 10000 - 50) / (10 * 10000)
        //      = entry * (100000 + 10000 - 50) / 100000
        //      = entry * 109950 / 100000
        //      = 50000 * 109950 / 100000 = 54975
        let price_ticks = 5_000_000i64;
        let liq = calc_liquidation_price_ticks(price_ticks, 10, 50, PositionSide::Short);
        // 5_000_000 * 109950 / 100000 = 5_497_500
        assert_eq!(liq, 5_497_500);
    }

    #[test]
    fn bankruptcy_price_long_10x() {
        // long: entry * (10-1) / 10 = 50000 * 9/10 = 45000
        let price_ticks = 5_000_000i64;
        let bk = calc_bankruptcy_price_ticks(price_ticks, 10, PositionSide::Long);
        // 5_000_000 * 9 / 10 = 4_500_000
        assert_eq!(bk, 4_500_000);
    }

    #[test]
    fn bankruptcy_price_short_10x() {
        // short: entry * (10+1) / 10 = 50000 * 11/10 = 55000
        let price_ticks = 5_000_000i64;
        let bk = calc_bankruptcy_price_ticks(price_ticks, 10, PositionSide::Short);
        // 5_000_000 * 11 / 10 = 5_500_000
        assert_eq!(bk, 5_500_000);
    }

    #[test]
    fn unrealized_pnl_long_positive() {
        // long, entry=$50000, mark=$51000, qty=0.001 BTC
        // pnl = (51000 - 50000) * 1000 / 1_000_000 = 1_000_000 / 1_000_000 = 1000000/1000000
        // entry_ticks=5_000_000, mark_ticks=5_100_000, qty_lots=1000
        // diff = 100_000, pnl = 100_000 * 1000 / 1_000_000 = 100 cents = $1
        let pnl =
            calc_unrealized_pnl_atoms(PositionSide::Long, 1000, 5_000_000, 5_100_000, BTC_SCALE);
        assert_eq!(pnl, 100 * A); // $1

    }

    #[test]
    fn unrealized_pnl_long_negative() {
        // mark < entry → negative pnl
        let pnl =
            calc_unrealized_pnl_atoms(PositionSide::Long, 1000, 5_100_000, 5_000_000, BTC_SCALE);
        assert_eq!(pnl, -100 * A);
    }

    #[test]
    fn leverage_1_initial_margin_equals_notional() {
        let notional = calc_notional_atoms(5_000_000, 1000, BTC_SCALE);
        let margin = calc_initial_margin_atoms(notional, 1);
        assert_eq!(margin, notional);
    }

    #[test]
    fn tiny_position_no_overflow() {
        // 1 lot at $50000 → notional 5 cents = 5_000_000 atoms; at 10x the
        // margin is 500_000 atoms — atoms preserve the sub-cent precision
        // cents used to round away.
        let notional = calc_notional_atoms(5_000_000, 1, BTC_SCALE);
        assert_eq!(notional, 5 * A);
        let margin = calc_initial_margin_atoms(notional, 10);
        assert_eq!(margin, 500_000);
    }

    #[test]
    fn fee_basic_and_rebate() {
        // 5000 USDT notional = 5e11 atoms; 5 bps taker = 2.5 USDT = 2.5e8.
        assert_eq!(fee_atoms(500_000_000_000, 5), 250_000_000);
        // Maker rebate (-1 bp) is a credit (negative fee).
        assert_eq!(fee_atoms(500_000_000_000, -1), -50_000_000);
        assert_eq!(fee_atoms(500_000_000_000, 0), 0);
    }

    #[test]
    fn tranche_folds_dust_and_full_at_10000() {
        // 50% of 100k = 50k; remainder 50k ≥ 20% → partial.
        assert_eq!(liquidation_tranche_lots(100_000, 5_000), 50_000);
        // 90% tranche leaves 10% < 20% → fold to full.
        assert_eq!(liquidation_tranche_lots(100_000, 9_000), 100_000);
        // Default/disabled = full.
        assert_eq!(liquidation_tranche_lots(100_000, 10_000), 100_000);
        // Tiny positions never produce a 0-lot order.
        assert_eq!(liquidation_tranche_lots(1, 5_000), 1);
    }

    #[test]
    fn risk_tiers_parse_and_select() {
        let tiers = parse_risk_tiers("1000000:20,5000000:10,:5");
        assert_eq!(tiers.len(), 3);
        let a = 100_000_000i64; // atoms per USDT
        assert_eq!(max_leverage_for_notional(500_000 * a, &tiers), 20);
        assert_eq!(max_leverage_for_notional(1_000_000 * a, &tiers), 20, "boundary inclusive");
        assert_eq!(max_leverage_for_notional(3_000_000 * a, &tiers), 10);
        assert_eq!(max_leverage_for_notional(50_000_000 * a, &tiers), 5);
        // Garbage entries skipped; empty disables (callers' contract).
        assert!(parse_risk_tiers("nonsense").is_empty());
        assert_eq!(max_leverage_for_notional(1, &[]), 1);
    }

    /// S2 boundary guard: a position big enough to overflow naive i64
    /// math (1000 BTC at $100k) — i128 intermediates make it exact.
    #[test]
    fn huge_position_no_overflow() {
        let price_ticks = 10_000_000i64; // $100,000
        let qty_lots = 1_000_000_000i64; // 1000 BTC
        // naive i64: ticks*lots*1e6 = 1e22 — would wrap. i128: exact.
        let notional = calc_notional_atoms(price_ticks, qty_lots, BTC_SCALE);
        assert_eq!(notional, 10_000_000_000_000_000); // 100M USDT in atoms
    }

    /// B6: a fully malformed RISK_TIERS string must produce an empty Vec
    /// (no panic, no partially-valid state).
    #[test]
    fn parse_risk_tiers_malformed_returns_empty() {
        let tiers = parse_risk_tiers("nonsense,alsobad,12345");
        assert!(
            tiers.is_empty(),
            "B6: all malformed entries must yield an empty tier list, got: {:?}",
            tiers
        );
    }
}
