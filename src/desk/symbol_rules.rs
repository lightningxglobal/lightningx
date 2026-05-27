use crate::float_ext::FLOAT_EPS;

#[derive(Debug, Clone, Copy)]
pub struct SymbolRules {
    pub price_tick: f64,
    pub quantity_step: f64,
    pub min_notional: f64,
}

impl SymbolRules {
    fn for_symbol(symbol: &str) -> Self {
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
}

fn aligned(value: f64, step: f64) -> bool {
    if step <= 0.0 {
        return true;
    }
    let units = value / step;
    (units - units.round()).abs() < FLOAT_EPS * 10.0
}

pub fn validate_order_shape(
    symbol: &str,
    order_type: &str,
    price: Option<f64>,
    quantity: f64,
) -> Result<(), &'static str> {
    let rules = SymbolRules::for_symbol(symbol);
    if quantity <= 0.0 || !quantity.is_finite() {
        return Err("quantity must be > 0");
    }
    if !aligned(quantity, rules.quantity_step) {
        return Err("quantity does not match lot size");
    }
    if order_type != "market" {
        let price = price.ok_or("price required for non-market orders")?;
        if price <= 0.0 || !price.is_finite() {
            return Err("price required for non-market orders");
        }
        if !aligned(price, rules.price_tick) {
            return Err("price does not match tick size");
        }
        if price * quantity < rules.min_notional {
            return Err("order notional is below minimum");
        }
    }
    Ok(())
}
