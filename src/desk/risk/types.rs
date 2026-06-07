// All monetary values in ATOMS (1e-8 USDT = 1 unit, ledger-wide since S2).
// Prices in ticks. Quantities in lots.

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

    /// Inverse of [`Self::as_db_str`]; `None` for unknown strings so the
    /// hydrate path fails fast instead of guessing.
    pub fn from_db_str(s: &str) -> Option<Self> {
        Some(match s {
            "normal" => RiskStatus::Normal,
            "margin_call" => RiskStatus::MarginCall,
            "liquidation_pending" => RiskStatus::LiquidationPending,
            "liquidating" => RiskStatus::Liquidating,
            "liquidated" => RiskStatus::Liquidated,
            "bankruptcy" => RiskStatus::Bankruptcy,
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
    pub fn new(user_id: i64, usdt_balance_atoms: i64) -> Self {
        Self {
            user_id,
            equity: usdt_balance_atoms,
            available_margin: AtomicI64::new(usdt_balance_atoms),
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
    /// Cumulative open cost in ATOMS (Σ fill values). PnL is computed
    /// against this — NOT against entry_price_ticks — so the books stay
    /// zero-sum to the atom: the VWAP entry truncates to whole ticks and
    /// the S2.4 conservation test proved that truncation literally mints
    /// money. entry_price_ticks remains the display/liq-price reference.
    pub cost_atoms: i64,
    pub entry_price_ticks: i64,
    pub mark_price_ticks: i64,
    pub unrealized_pnl: i64,
    pub initial_margin: i64,
    pub maintenance_margin: i64,
    pub liquidation_price_ticks: i64,
    pub bankruptcy_price_ticks: i64,
    pub leverage: u8,
}

/// One ADL pairing (S6.2): `counterparty` was force-closed against the
/// bankrupt account at the bankruptcy price.
#[derive(Debug, Clone)]
pub struct AdlEvent {
    pub bankrupt_user_id: i64,
    pub counterparty_user_id: i64,
    pub qty_lots: i64,
    pub price_ticks: i64,
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
