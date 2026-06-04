// All monetary values in "cents" (0.01 USDT = 1 unit). Prices in ticks. Quantities in lots.

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RiskStatus {
    #[default]
    Normal,
    MarginCall,
    LiquidationPending,
    Liquidating,
    Liquidated,
    Bankruptcy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PositionSide {
    Long,
    Short,
}

#[derive(Debug, Clone)]
pub struct AccountRiskState {
    pub user_id: i64,
    pub equity: i64,
    pub available_margin: i64,
    pub order_margin: i64,
    pub used_margin: i64,
    pub maintenance_margin: i64,
    pub unrealized_pnl: i64,
    pub status: RiskStatus,
}

impl AccountRiskState {
    pub fn new(user_id: i64, usdt_balance_cents: i64) -> Self {
        Self {
            user_id,
            equity: usdt_balance_cents,
            available_margin: usdt_balance_cents,
            order_margin: 0,
            used_margin: 0,
            maintenance_margin: 0,
            unrealized_pnl: 0,
            status: RiskStatus::Normal,
        }
    }

    pub fn can_place_order(&self) -> bool {
        matches!(self.status, RiskStatus::Normal | RiskStatus::MarginCall)
    }
}

#[derive(Debug, Clone)]
pub struct PositionRiskState {
    pub user_id: i64,
    pub symbol: [u8; 16],
    pub side: PositionSide,
    pub qty_lots: i64,
    pub entry_price_ticks: i64,
    pub mark_price_ticks: i64,
    pub unrealized_pnl: i64,
    pub initial_margin: i64,
    pub maintenance_margin: i64,
    pub liquidation_price_ticks: i64,
    pub bankruptcy_price_ticks: i64,
    pub leverage: u8,
}

/// Emitted by run_risk_tick() when a position should be liquidated.
#[derive(Debug, Clone)]
pub struct LiquidationEvent {
    pub user_id: i64,
    pub symbol: [u8; 16],
    pub side: PositionSide,
    pub qty_lots: i64,
    /// The price at which the user is settled (from PositionRiskState::liquidation_price_ticks).
    /// Exchange revenue = |actual_fill_price - liq_price| * qty / notional_scale.
    pub liq_price_ticks: i64,
}
