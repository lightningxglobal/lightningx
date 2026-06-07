// All monetary values in "cents" (0.01 USDT = 1 unit). Prices in ticks. Quantities in lots.

use std::sync::atomic::AtomicI64;

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

/// Hot-path fields (`available_margin`, `order_margin`) are `AtomicI64` so
/// `check_and_reserve_margin` and `release_order_margin` can use a shared
/// DashMap read-lock (`get`) instead of an exclusive write-lock (`get_mut`).
/// All other fields are only written under `get_mut` (on_fill, risk_tick).
pub struct AccountRiskState {
    pub user_id: i64,
    pub equity: i64,
    pub available_margin: AtomicI64,
    pub order_margin: AtomicI64,
    pub used_margin: i64,
    pub maintenance_margin: i64,
    pub unrealized_pnl: i64,
    pub status: RiskStatus,
}

impl RiskStatus {
    /// Stable wire/DB discriminant (PersistFrame RiskAccountSet.status and
    /// the risk_accounts.status CHECK in migration 022 both key off this).
    pub fn to_u8(self) -> u8 {
        match self {
            RiskStatus::Normal => 0,
            RiskStatus::MarginCall => 1,
            RiskStatus::LiquidationPending => 2,
            RiskStatus::Liquidating => 3,
            RiskStatus::Liquidated => 4,
            RiskStatus::Bankruptcy => 5,
        }
    }

    pub fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            0 => RiskStatus::Normal,
            1 => RiskStatus::MarginCall,
            2 => RiskStatus::LiquidationPending,
            3 => RiskStatus::Liquidating,
            4 => RiskStatus::Liquidated,
            5 => RiskStatus::Bankruptcy,
            _ => return None,
        })
    }

    /// The exact strings accepted by the risk_accounts.status CHECK.
    pub fn as_db_str(self) -> &'static str {
        match self {
            RiskStatus::Normal => "normal",
            RiskStatus::MarginCall => "margin_call",
            RiskStatus::LiquidationPending => "liquidation_pending",
            RiskStatus::Liquidating => "liquidating",
            RiskStatus::Liquidated => "liquidated",
            RiskStatus::Bankruptcy => "bankruptcy",
        }
    }
}

impl PositionSide {
    /// Wire/DB discriminant: 0 = long, 1 = short (PositionUpsert.side and
    /// the positions.side CHECK).
    pub fn to_u8(self) -> u8 {
        match self {
            PositionSide::Long => 0,
            PositionSide::Short => 1,
        }
    }

    pub fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            0 => PositionSide::Long,
            1 => PositionSide::Short,
            _ => return None,
        })
    }

    pub fn as_db_str(self) -> &'static str {
        match self {
            PositionSide::Long => "long",
            PositionSide::Short => "short",
        }
    }
}

impl AccountRiskState {
    pub fn new(user_id: i64, usdt_balance_cents: i64) -> Self {
        Self {
            user_id,
            equity: usdt_balance_cents,
            available_margin: AtomicI64::new(usdt_balance_cents),
            order_margin: AtomicI64::new(0),
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
