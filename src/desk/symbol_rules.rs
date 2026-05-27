use crate::order::{Side, TimeInForce};

#[derive(Debug, Clone, Copy)]
pub struct SymbolRules {
    pub price_tick: f64,
    pub quantity_step: f64,
    pub min_notional: f64,
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
                min_notional: 5.0,
            },
            "ETH_USDT" => Self {
                price_tick: 0.01,
                quantity_step: 0.0001,
                min_notional: 5.0,
            },
            "SOL_USDT" => Self {
                price_tick: 0.001,
                quantity_step: 0.001,
                min_notional: 5.0,
            },
            _ => Self {
                price_tick: 0.01,
                quantity_step: 0.000001,
                min_notional: 1.0,
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

pub fn normalize_order_shape(
    symbol: &str,
    order_type: &str,
    price: Option<f64>,
    quantity: f64,
) -> Result<FixedOrderShape, &'static str> {
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
