use crate::OrderStatus;
use crate::transport::order_update_kind;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DbOrderStatus {
    Pending = 0,
    Trading = 1,
    Completed = 2,
    Canceled = 3,
    Rejected = 4,
}

impl DbOrderStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Trading => "TRADING",
            Self::Completed => "COMPLETED",
            Self::Canceled => "CANCELED",
            Self::Rejected => "REJECTED",
        }
    }

    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsOrderStatus {
    Open,
    Partial,
    PartialFill,
    Filled,
    Canceled,
    Rejected,
}

impl WsOrderStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Open => "OPEN",
            Self::Partial => "PARTIAL",
            Self::PartialFill => "PARTIAL_FILL",
            Self::Filled => "FILLED",
            Self::Canceled => "CANCELED",
            Self::Rejected => "REJECTED",
        }
    }
}

pub const OPEN_DB_STATUSES: &[&str] = &[
    DbOrderStatus::Pending.as_str(),
    DbOrderStatus::Trading.as_str(),
];
pub const HISTORY_DB_STATUSES: &[&str] = &[
    DbOrderStatus::Completed.as_str(),
    DbOrderStatus::Canceled.as_str(),
    DbOrderStatus::Rejected.as_str(),
];

pub const fn db_status_from_update_kind(kind: u8) -> DbOrderStatus {
    match kind {
        k if k == order_update_kind::ACCEPTED => DbOrderStatus::Pending,
        k if k == order_update_kind::PARTIAL_FILL => DbOrderStatus::Trading,
        k if k == order_update_kind::FILLED => DbOrderStatus::Completed,
        k if k == order_update_kind::CANCELLED => DbOrderStatus::Canceled,
        _ => DbOrderStatus::Rejected,
    }
}

pub const fn ws_status_from_update_kind(kind: u8) -> WsOrderStatus {
    match kind {
        k if k == order_update_kind::ACCEPTED => WsOrderStatus::Open,
        k if k == order_update_kind::PARTIAL_FILL => WsOrderStatus::Partial,
        k if k == order_update_kind::FILLED => WsOrderStatus::Filled,
        k if k == order_update_kind::CANCELLED => WsOrderStatus::Canceled,
        _ => WsOrderStatus::Rejected,
    }
}

pub const fn db_status_from_engine(status: OrderStatus) -> DbOrderStatus {
    match status {
        OrderStatus::Accepted => DbOrderStatus::Pending,
        OrderStatus::PartiallyFilled => DbOrderStatus::Trading,
        OrderStatus::Filled => DbOrderStatus::Completed,
        OrderStatus::Cancelled => DbOrderStatus::Canceled,
        OrderStatus::Rejected => DbOrderStatus::Rejected,
    }
}

pub const fn ws_status_from_engine(
    status: OrderStatus,
    cancelled_after_fill: bool,
) -> WsOrderStatus {
    match status {
        OrderStatus::Accepted => WsOrderStatus::Open,
        OrderStatus::PartiallyFilled => WsOrderStatus::PartialFill,
        OrderStatus::Filled => WsOrderStatus::Filled,
        OrderStatus::Cancelled if cancelled_after_fill => WsOrderStatus::PartialFill,
        OrderStatus::Cancelled => WsOrderStatus::Canceled,
        OrderStatus::Rejected => WsOrderStatus::Rejected,
    }
}

pub const fn maker_ws_status_from_db_status(status: &str) -> WsOrderStatus {
    match status.as_bytes() {
        b"COMPLETED" => WsOrderStatus::Filled,
        b"TRADING" => WsOrderStatus::PartialFill,
        _ => WsOrderStatus::PartialFill,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_aeron_update_kind_to_db_and_ws_status() {
        assert_eq!(
            db_status_from_update_kind(order_update_kind::ACCEPTED),
            DbOrderStatus::Pending
        );
        assert_eq!(
            db_status_from_update_kind(order_update_kind::PARTIAL_FILL),
            DbOrderStatus::Trading
        );
        assert_eq!(
            ws_status_from_update_kind(order_update_kind::PARTIAL_FILL).as_str(),
            "PARTIAL"
        );
        assert_eq!(
            ws_status_from_update_kind(order_update_kind::CANCELLED),
            WsOrderStatus::Canceled
        );
    }

    #[test]
    fn maps_engine_status_to_db_and_ws_status() {
        assert_eq!(
            db_status_from_engine(OrderStatus::Filled),
            DbOrderStatus::Completed
        );
        assert_eq!(
            ws_status_from_engine(OrderStatus::Cancelled, true),
            WsOrderStatus::PartialFill
        );
        assert_eq!(
            ws_status_from_engine(OrderStatus::Cancelled, false),
            WsOrderStatus::Canceled
        );
    }
}
