use super::types::PositionSide;

pub fn calc_notional_cents(price_ticks: i64, qty_lots: i64, notional_scale: i64) -> i64 {
    ((price_ticks as i128 * qty_lots as i128) / notional_scale as i128) as i64
}

pub fn calc_initial_margin_cents(notional_cents: i64, leverage: u8) -> i64 {
    if leverage == 0 {
        return 0;
    }
    notional_cents / leverage as i64
}

pub fn calc_maintenance_margin_cents(notional_cents: i64, rate_bps: i64) -> i64 {
    ((notional_cents as i128 * rate_bps as i128) / 10_000) as i64
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

pub fn calc_unrealized_pnl_cents(
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
    ((diff as i128 * qty_lots as i128) / notional_scale as i128) as i64
}

#[cfg(test)]
mod tests {
    use super::super::types::PositionSide;
    use super::*;

    // BTC_USDT: price_tick=0.01, qty_step=0.000001
    // notional_scale = 1_000_000
    // price = $50,000 → price_ticks = 5_000_000 (50000 / 0.01)
    // qty = 0.001 BTC → qty_lots = 1000 (0.001 / 0.000001)
    // notional = $50 → notional_cents = 5000
    const BTC_SCALE: i64 = 1_000_000;

    #[test]
    fn notional_btc_50k_001btc() {
        // price = 50000, price_ticks = 50000 / 0.01 = 5_000_000
        // qty = 0.001, qty_lots = 0.001 / 0.000001 = 1000
        // notional = price_ticks * qty_lots / scale = 5_000_000 * 1000 / 1_000_000 = 5000 cents = $50
        let price_ticks = 5_000_000i64;
        let qty_lots = 1000i64;
        let notional = calc_notional_cents(price_ticks, qty_lots, BTC_SCALE);
        assert_eq!(notional, 5000); // $50 in cents
    }

    #[test]
    fn initial_margin_10x() {
        // notional = $50 (5000 cents), leverage = 10 → initial_margin = 500 cents = $5
        let margin = calc_initial_margin_cents(5000, 10);
        assert_eq!(margin, 500);
    }

    #[test]
    fn maintenance_margin_50bps() {
        // notional = $50 (5000 cents), rate = 50bps → mm = 5000 * 50 / 10000 = 25 cents
        let mm = calc_maintenance_margin_cents(5000, 50);
        assert_eq!(mm, 25);
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
            calc_unrealized_pnl_cents(PositionSide::Long, 1000, 5_000_000, 5_100_000, BTC_SCALE);
        assert_eq!(pnl, 100);
    }

    #[test]
    fn unrealized_pnl_long_negative() {
        // mark < entry → negative pnl
        let pnl =
            calc_unrealized_pnl_cents(PositionSide::Long, 1000, 5_100_000, 5_000_000, BTC_SCALE);
        assert_eq!(pnl, -100);
    }

    #[test]
    fn leverage_1_initial_margin_equals_notional() {
        let notional = calc_notional_cents(5_000_000, 1000, BTC_SCALE);
        let margin = calc_initial_margin_cents(notional, 1);
        assert_eq!(margin, notional);
    }

    #[test]
    fn tiny_position_no_overflow() {
        // 1 lot at $50000 → notional = 5_000_000 * 1 / 1_000_000 = 5 cents
        let notional = calc_notional_cents(5_000_000, 1, BTC_SCALE);
        assert_eq!(notional, 5);
        let margin = calc_initial_margin_cents(notional, 10);
        assert_eq!(margin, 0); // rounds down — 5/10=0
    }
}
