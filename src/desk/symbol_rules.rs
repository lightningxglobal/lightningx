use crate::desk::money::AmountAtoms;
use crate::order::{Side, TimeInForce};

#[derive(Debug, Clone, Copy)]
pub struct SymbolRules {
    pub price_tick: f64,
    pub quantity_step: f64,
    /// price_tick expressed in atoms (1e-8). Integer twin of `price_tick`,
    /// hardcoded per symbol so tick↔atoms conversion never touches f64.
    pub price_tick_atoms: i64,
    /// quantity_step expressed in atoms (1e-8). Integer twin of `quantity_step`.
    pub quantity_step_atoms: i64,
    pub min_notional: f64,
    /// notional_cents = price_ticks * qty_lots / notional_scale
    pub notional_scale: i64,
    /// Default leverage for this symbol
    pub default_leverage: u8,
    /// Maintenance margin rate in basis points (50 = 0.5%)
    pub maintenance_rate_bps: i64,
    /// Margin call threshold in basis points (75 = 0.75%)
    pub margin_call_rate_bps: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FixedOrderShape {
    pub price_ticks: Option<i64>,
    pub quantity_lots: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FixedOrderInput {
    pub id: u64,
    pub side: Side,
    pub price_ticks: i64,
    pub quantity_lots: i64,
    pub time_in_force: TimeInForce,
    pub timestamp: u64,
    pub is_market: bool,
}

impl FixedOrderInput {
    pub fn from_order_fields(
        id: u64,
        symbol: &str,
        side: Side,
        order_type: &str,
        price: Option<f64>,
        quantity: f64,
        time_in_force: TimeInForce,
        timestamp: u64,
    ) -> Result<Self, &'static str> {
        let shape = normalize_order_shape(symbol, order_type, price, quantity)?;
        Ok(Self {
            id,
            side,
            price_ticks: shape.price_ticks.unwrap_or(0),
            quantity_lots: shape.quantity_lots,
            time_in_force,
            timestamp,
            is_market: shape.price_ticks.is_none(),
        })
    }
}

impl SymbolRules {
    pub fn for_symbol(symbol: &str) -> Self {
        match symbol {
            "BTC_USDT" => Self {
                price_tick: 0.01,
                quantity_step: 0.000001,
                price_tick_atoms: 1_000_000,
                quantity_step_atoms: 100,
                min_notional: 5.0,
                notional_scale: 1_000_000,
                default_leverage: 10,
                maintenance_rate_bps: 50,
                margin_call_rate_bps: 75,
            },
            "ETH_USDT" => Self {
                price_tick: 0.01,
                quantity_step: 0.0001,
                price_tick_atoms: 1_000_000,
                quantity_step_atoms: 10_000,
                min_notional: 5.0,
                notional_scale: 10_000,
                default_leverage: 10,
                maintenance_rate_bps: 50,
                margin_call_rate_bps: 75,
            },
            "SOL_USDT" => Self {
                price_tick: 0.001,
                quantity_step: 0.001,
                price_tick_atoms: 100_000,
                quantity_step_atoms: 100_000,
                min_notional: 5.0,
                notional_scale: 10_000,
                default_leverage: 10,
                maintenance_rate_bps: 50,
                margin_call_rate_bps: 75,
            },
            _ => Self {
                price_tick: 0.01,
                quantity_step: 0.000001,
                price_tick_atoms: 1_000_000,
                quantity_step_atoms: 100,
                min_notional: 1.0,
                notional_scale: 1_000_000,
                default_leverage: 10,
                maintenance_rate_bps: 50,
                margin_call_rate_bps: 75,
            },
        }
    }

    pub fn price_to_ticks(self, price: f64) -> Result<i64, &'static str> {
        to_units(price, self.price_tick, "price does not match tick size")
    }

    pub fn quantity_to_lots(self, quantity: f64) -> Result<i64, &'static str> {
        to_units(
            quantity,
            self.quantity_step,
            "quantity does not match lot size",
        )
    }

    pub fn ticks_to_price(self, ticks: i64) -> f64 {
        ticks as f64 * self.price_tick
    }

    pub fn lots_to_quantity(self, lots: i64) -> f64 {
        lots as f64 * self.quantity_step
    }

    // ── Integer-domain conversions (no f64 anywhere) ─────────────────────

    /// ticks → price atoms, exact: ticks × price_tick_atoms (checked).
    pub fn ticks_to_price_atoms(self, ticks: i64) -> Result<AmountAtoms, &'static str> {
        ticks
            .checked_mul(self.price_tick_atoms)
            .map(AmountAtoms::from_atoms)
            .ok_or("price overflows atoms")
    }

    /// lots → quantity atoms, exact: lots × quantity_step_atoms (checked).
    pub fn lots_to_quantity_atoms(self, lots: i64) -> Result<AmountAtoms, &'static str> {
        lots.checked_mul(self.quantity_step_atoms)
            .map(AmountAtoms::from_atoms)
            .ok_or("quantity overflows atoms")
    }

    /// price atoms → ticks. Exact-division requirement replaces the float
    /// epsilon alignment check: a price parsed from a decimal string is on
    /// the tick grid iff atoms % tick_atoms == 0.
    pub fn price_atoms_to_ticks(self, atoms: AmountAtoms) -> Result<i64, &'static str> {
        let a = atoms.atoms();
        if a <= 0 {
            return Err("price must be > 0");
        }
        if a % self.price_tick_atoms != 0 {
            return Err("price does not match tick size");
        }
        Ok(a / self.price_tick_atoms)
    }

    /// quantity atoms → lots, exact division (see price_atoms_to_ticks).
    pub fn quantity_atoms_to_lots(self, atoms: AmountAtoms) -> Result<i64, &'static str> {
        let a = atoms.atoms();
        if a <= 0 {
            return Err("quantity must be > 0");
        }
        if a % self.quantity_step_atoms != 0 {
            return Err("quantity does not match lot size");
        }
        Ok(a / self.quantity_step_atoms)
    }
}

fn to_units(value: f64, step: f64, misaligned_error: &'static str) -> Result<i64, &'static str> {
    if value <= 0.0 || !value.is_finite() || step <= 0.0 || !step.is_finite() {
        return Err(misaligned_error);
    }
    let units = value / step;
    if units > i64::MAX as f64 || units < i64::MIN as f64 {
        return Err(misaligned_error);
    }
    let rounded = units.round();
    // Use a relative tolerance so division by small steps (e.g. 1e-6) doesn't
    // accumulate floating-point error past the fixed threshold.
    if (units - rounded).abs() < units.abs().max(1.0) * 1e-9 {
        Ok(rounded as i64)
    } else {
        Err(misaligned_error)
    }
}

/// Hard cap on symbol length, matching the `[u8; 16]` null-padded wire
/// fields (NewOrderRequest, persist payloads, risk-engine keys). Input
/// longer than this used to be SILENTLY truncated by pack_str16 — meaning
/// two distinct user-supplied strings could alias the same wire symbol.
/// Reject at entry instead. Raising this requires a coordinated wire-format
/// change across every packed struct carrying a symbol.
pub const MAX_SYMBOL_LEN: usize = 16;

/// Validate a user-supplied symbol before it touches any fixed-width wire
/// field or cache key. O(len), len ≤ a few bytes.
pub fn validate_symbol(symbol: &str) -> Result<(), &'static str> {
    if symbol.is_empty() {
        return Err("symbol must not be empty");
    }
    if symbol.len() > MAX_SYMBOL_LEN {
        return Err("symbol too long");
    }
    if !symbol
        .bytes()
        .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
    {
        return Err("symbol has invalid characters");
    }
    Ok(())
}

pub fn normalize_order_shape(
    symbol: &str,
    order_type: &str,
    price: Option<f64>,
    quantity: f64,
) -> Result<FixedOrderShape, &'static str> {
    validate_symbol(symbol)?;
    let rules = SymbolRules::for_symbol(symbol);
    if quantity <= 0.0 || !quantity.is_finite() {
        return Err("quantity must be > 0");
    }
    let quantity_lots = rules.quantity_to_lots(quantity)?;
    if order_type != "market" {
        let price = price.ok_or("price required for non-market orders")?;
        if price <= 0.0 || !price.is_finite() {
            return Err("price required for non-market orders");
        }
        let price_ticks = rules.price_to_ticks(price)?;
        if price * quantity < rules.min_notional {
            return Err("order notional is below minimum");
        }
        return Ok(FixedOrderShape {
            price_ticks: Some(price_ticks),
            quantity_lots,
        });
    }
    Ok(FixedOrderShape {
        price_ticks: None,
        quantity_lots,
    })
}

pub fn validate_order_shape(
    symbol: &str,
    order_type: &str,
    price: Option<f64>,
    quantity: f64,
) -> Result<(), &'static str> {
    normalize_order_shape(symbol, order_type, price, quantity).map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn atoms_twins_match_float_rules_for_all_symbols() {
        // Guard: if someone edits price_tick/quantity_step without updating
        // the integer twin, this catches the divergence.
        for sym in ["BTC_USDT", "ETH_USDT", "SOL_USDT", "OTHER"] {
            let r = SymbolRules::for_symbol(sym);
            assert_eq!(
                AmountAtoms::from_f64_round(r.price_tick).unwrap().atoms(),
                r.price_tick_atoms,
                "price tick twin mismatch for {sym}"
            );
            assert_eq!(
                AmountAtoms::from_f64_round(r.quantity_step).unwrap().atoms(),
                r.quantity_step_atoms,
                "quantity step twin mismatch for {sym}"
            );
        }
    }

    #[test]
    fn integer_conversions_are_exact_and_roundtrip() {
        let r = SymbolRules::for_symbol("BTC_USDT");

        // 75893.61 → 7_589_361 ticks → back to atoms, exactly.
        let price = AmountAtoms::from_decimal_str("75893.61").unwrap();
        let ticks = r.price_atoms_to_ticks(price).unwrap();
        assert_eq!(ticks, 7_589_361);
        assert_eq!(r.ticks_to_price_atoms(ticks).unwrap(), price);

        // 0.00123456 BTC → 1234.56 lots? No: step 1e-6 → 1234.56 is OFF-grid.
        // 0.001234 → 1234 lots, exact.
        let qty = AmountAtoms::from_decimal_str("0.001234").unwrap();
        let lots = r.quantity_atoms_to_lots(qty).unwrap();
        assert_eq!(lots, 1234);
        assert_eq!(r.lots_to_quantity_atoms(lots).unwrap(), qty);

        // Off-grid values are rejected by exact division, no epsilon.
        let off_tick = AmountAtoms::from_decimal_str("75893.611").unwrap();
        assert_eq!(
            r.price_atoms_to_ticks(off_tick).unwrap_err(),
            "price does not match tick size"
        );
        let off_step = AmountAtoms::from_decimal_str("0.0012345").unwrap();
        assert_eq!(
            r.quantity_atoms_to_lots(off_step).unwrap_err(),
            "quantity does not match lot size"
        );

        // Zero/negative rejected; overflow checked.
        assert!(r.price_atoms_to_ticks(AmountAtoms::ZERO).is_err());
        assert!(r.ticks_to_price_atoms(i64::MAX).is_err());
    }

    #[test]
    fn symbol_validation_rejects_overlong_and_junk_input() {
        // The exact wire width must pass; one byte more must fail —
        // silent pack_str16 truncation is what this guards against.
        assert!(validate_symbol("ABCDEFGH_ABCDEFG").is_ok()); // 16 bytes
        assert_eq!(
            validate_symbol("ABCDEFGH_ABCDEFGH").unwrap_err(), // 17 bytes
            "symbol too long"
        );
        assert!(validate_symbol("BTC_USDT").is_ok());
        assert!(validate_symbol("1000SHIB_USDT").is_ok());
        assert_eq!(validate_symbol("").unwrap_err(), "symbol must not be empty");
        for bad in ["btc_usdt", "BTC-USDT", "BTC USDT", "BTC\0USDT", "BTC.USDT"] {
            assert_eq!(
                validate_symbol(bad).unwrap_err(),
                "symbol has invalid characters",
                "must reject {bad:?}"
            );
        }
    }

    #[test]
    fn order_shape_rejects_invalid_symbol_at_entry() {
        assert_eq!(
            normalize_order_shape("ABCDEFGH_ABCDEFGH", "limit", Some(100.0), 1.0).unwrap_err(),
            "symbol too long"
        );
        assert_eq!(
            validate_order_shape("btc_usdt", "market", None, 1.0).unwrap_err(),
            "symbol has invalid characters"
        );
    }

    #[test]
    fn normalizes_btc_limit_to_ticks_and_lots() {
        let shape = normalize_order_shape("BTC_USDT", "limit", Some(75893.60), 0.000123)
            .expect("valid fixed shape");
        assert_eq!(shape.price_ticks, Some(7_589_360));
        assert_eq!(shape.quantity_lots, 123);
    }

    #[test]
    fn rejects_misaligned_price_and_quantity() {
        assert_eq!(
            normalize_order_shape("BTC_USDT", "limit", Some(100.001), 0.000123).unwrap_err(),
            "price does not match tick size"
        );
        assert_eq!(
            normalize_order_shape("ETH_USDT", "limit", Some(100.01), 0.00001).unwrap_err(),
            "quantity does not match lot size"
        );
    }

    #[test]
    fn market_order_has_no_price_ticks() {
        let shape = normalize_order_shape("ETH_USDT", "market", None, 0.001).unwrap();
        assert_eq!(shape.price_ticks, None);
        assert_eq!(shape.quantity_lots, 10);
    }

    #[test]
    fn fixed_order_input_keeps_integer_shape() {
        let fixed = FixedOrderInput::from_order_fields(
            9,
            "BTC_USDT",
            Side::Buy,
            "limit",
            Some(75893.60),
            0.000123,
            TimeInForce::GTC,
            12345,
        )
        .unwrap();
        assert_eq!(fixed.price_ticks, 7_589_360);
        assert_eq!(fixed.quantity_lots, 123);
        assert!(!fixed.is_market);
    }

    #[test]
    fn fixed_market_order_keeps_integer_shape() {
        let fixed = FixedOrderInput::from_order_fields(
            10,
            "ETH_USDT",
            Side::Sell,
            "market",
            None,
            0.001,
            TimeInForce::IOC,
            12346,
        )
        .unwrap();
        assert_eq!(fixed.price_ticks, 0);
        assert_eq!(fixed.quantity_lots, 10);
        assert!(fixed.is_market);
    }
}
