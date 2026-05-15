/// Order Update Event - 用于rtrb跨线程传递（Thread 1 → Thread 2）
///
/// 必须是Copy类型（rtrb要求），64字节单个缓存行对齐

pub mod kind {
    pub const ACCEPTED: u8 = 1;
    pub const FILLED: u8 = 2;
    pub const PARTIAL_FILL: u8 = 3;
    pub const CANCELLED: u8 = 4;
    pub const REJECTED: u8 = 5;
}

#[repr(C, align(64))]
#[derive(Clone, Copy)]
pub struct OrderUpdateEvent {
    pub kind: u8,
    pub reject_reason: u8,
    _pad: [u8; 6],
    pub order_id: u64,
    pub client_order_id: u64,
    pub participant_id: u64,
    pub fill_price: f64,
    pub fill_qty: f64,
    pub remaining_qty: f64,
    pub timestamp: u64,
}

static_assertions::const_assert_eq!(std::mem::size_of::<OrderUpdateEvent>(), 64);

impl OrderUpdateEvent {
    pub fn accepted(order_id: u64, client_order_id: u64, participant_id: u64, ts: u64) -> Self {
        Self {
            kind: kind::ACCEPTED,
            reject_reason: 0,
            _pad: [0; 6],
            order_id,
            client_order_id,
            participant_id,
            fill_price: 0.0,
            fill_qty: 0.0,
            remaining_qty: 0.0,
            timestamp: ts,
        }
    }

    pub fn filled(order_id: u64, client_order_id: u64, participant_id: u64, price: f64, qty: f64, ts: u64) -> Self {
        Self {
            kind: kind::FILLED,
            reject_reason: 0,
            _pad: [0; 6],
            order_id,
            client_order_id,
            participant_id,
            fill_price: price,
            fill_qty: qty,
            remaining_qty: 0.0,
            timestamp: ts,
        }
    }

    pub fn partial_fill(order_id: u64, client_order_id: u64, participant_id: u64, price: f64, filled: f64, remaining: f64, ts: u64) -> Self {
        Self {
            kind: kind::PARTIAL_FILL,
            reject_reason: 0,
            _pad: [0; 6],
            order_id,
            client_order_id,
            participant_id,
            fill_price: price,
            fill_qty: filled,
            remaining_qty: remaining,
            timestamp: ts,
        }
    }

    pub fn cancelled(order_id: u64, client_order_id: u64, participant_id: u64, cancelled_qty: f64, ts: u64) -> Self {
        Self {
            kind: kind::CANCELLED,
            reject_reason: 0,
            _pad: [0; 6],
            order_id,
            client_order_id,
            participant_id,
            fill_price: 0.0,
            fill_qty: cancelled_qty,
            remaining_qty: 0.0,
            timestamp: ts,
        }
    }

    pub fn rejected(client_order_id: u64, participant_id: u64, reason: u8, ts: u64) -> Self {
        Self {
            kind: kind::REJECTED,
            reject_reason: reason,
            _pad: [0; 6],
            order_id: 0,
            client_order_id,
            participant_id,
            fill_price: 0.0,
            fill_qty: 0.0,
            remaining_qty: 0.0,
            timestamp: ts,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_order_update_event_size() {
        assert_eq!(std::mem::size_of::<OrderUpdateEvent>(), 64);
    }

    #[test]
    fn test_order_update_event_is_copy() {
        let evt = OrderUpdateEvent::accepted(1, 2, 3, 1000);
        let _evt2 = evt; // Copy should work
        let _evt3 = evt; // Can use again
        assert_eq!(evt.kind, kind::ACCEPTED);
    }

    #[test]
    fn test_order_update_accepted() {
        let evt = OrderUpdateEvent::accepted(1, 2, 3, 1000);
        assert_eq!(evt.kind, kind::ACCEPTED);
        assert_eq!(evt.order_id, 1);
        assert_eq!(evt.client_order_id, 2);
        assert_eq!(evt.participant_id, 3);
        assert_eq!(evt.timestamp, 1000);
    }

    #[test]
    fn test_order_update_filled() {
        let evt = OrderUpdateEvent::filled(1, 2, 3, 100.5, 10.0, 1000);
        assert_eq!(evt.kind, kind::FILLED);
        assert_eq!(evt.fill_price, 100.5);
        assert_eq!(evt.fill_qty, 10.0);
    }

    #[test]
    fn test_order_update_partial_fill() {
        let evt = OrderUpdateEvent::partial_fill(1, 2, 3, 100.5, 5.0, 5.0, 1000);
        assert_eq!(evt.kind, kind::PARTIAL_FILL);
        assert_eq!(evt.fill_qty, 5.0);
        assert_eq!(evt.remaining_qty, 5.0);
    }

    #[test]
    fn test_order_update_cancelled() {
        let evt = OrderUpdateEvent::cancelled(1, 2, 3, 10.0, 1000);
        assert_eq!(evt.kind, kind::CANCELLED);
        assert_eq!(evt.fill_qty, 10.0);
    }

    #[test]
    fn test_order_update_rejected() {
        let evt = OrderUpdateEvent::rejected(2, 3, 5, 1000);
        assert_eq!(evt.kind, kind::REJECTED);
        assert_eq!(evt.reject_reason, 5);
        assert_eq!(evt.client_order_id, 2);
    }
}
