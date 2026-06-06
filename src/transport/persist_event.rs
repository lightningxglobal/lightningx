//! `PersistEvent` — the wire format the desk-server publishes to its
//! "persist" Aeron stream and the redis-writer (and later pg-writer)
//! subscribes to.
//!
//! Hot path on desk-server only updates in-memory cache + WS pushes +
//! publishes one of these events. Persistence consumers run in their own
//! processes (independent restart / crash isolation) and translate events
//! into Redis HASH updates and batched PG writes.
//!
//! Wire format: fixed 144 bytes (8-byte header + 136-byte payload union).
//! POD/Copy struct, sent via Aeron as raw bytes — same pattern as
//! `OrderUpdateMsg` / `TradeNotification`.
//!
//! Encoding helpers do unaligned reads with `std::ptr::read_unaligned` to
//! satisfy clippy + miri on architectures that care.

use std::mem::size_of;

/// Event kind discriminator.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PersistKind {
    /// Active-order INSERT (full row) — new ACCEPTED order.
    OrderUpsert = 1,
    /// Terminal-state DELETE (FILLED / REJECTED / CANCELLED).
    OrderDelete = 2,
    /// Account row absolute (user_id, asset) → balance, frozen.
    AccountSet = 3,
    /// Append a trade row.
    TradeInsert = 4,
    /// Partial-fill update for an already-known order: only filled (and
    /// status) change. Avoids overwriting price/qty/etc with sentinels.
    OrderFillUpdate = 5,
    /// Append-only matching output event for audit/replay scaffolding.
    MatchingEvent = 6,
}

impl PersistKind {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::OrderUpsert),
            2 => Some(Self::OrderDelete),
            3 => Some(Self::AccountSet),
            4 => Some(Self::TradeInsert),
            5 => Some(Self::OrderFillUpdate),
            6 => Some(Self::MatchingEvent),
            _ => None,
        }
    }
}

pub mod matching_event_kind {
    pub const ACCEPTED: u8 = 1;
    pub const FILLED: u8 = 2;
    pub const PARTIAL_FILL: u8 = 3;
    pub const CANCELLED: u8 = 4;
    pub const REJECTED: u8 = 5;
    pub const TRADE: u8 = 6;
}

#[repr(C, packed)]
#[derive(Clone, Copy)]
pub struct OrderFillUpdatePayload {
    pub id: i64,
    pub filled: f64,
    pub status: u8, // DbOrderStatus
    pub _pad: [u8; 7],
}

#[repr(C, packed)]
#[derive(Clone, Copy)]
pub struct OrderUpsertPayload {
    pub id: i64,
    pub user_id: i64,
    pub symbol: [u8; 16], // null-padded
    pub side: u8,         // 0 = buy, 1 = sell
    pub status: u8,       // DbOrderStatus as u8
    pub _pad: [u8; 6],
    pub order_type: [u8; 16], // null-padded
    pub price: f64,
    pub qty: f64,
    pub filled: f64,
    pub freeze_price: f64,
    pub client_order_id: [u8; 32], // null-padded
    pub created_at_ms: i64,
}

#[repr(C, packed)]
#[derive(Clone, Copy)]
pub struct OrderDeletePayload {
    pub id: i64,
}

#[repr(C, packed)]
#[derive(Clone, Copy)]
pub struct AccountSetPayload {
    pub user_id: i64,
    pub asset: [u8; 16], // null-padded
    pub balance: f64,
    pub frozen: f64,
    pub balance_atoms: i64,
    pub frozen_atoms: i64,
}

#[repr(C, packed)]
#[derive(Clone, Copy)]
pub struct TradeInsertPayload {
    pub buy_order_id: i64,
    pub sell_order_id: i64,
    pub symbol: [u8; 16], // null-padded
    pub price: f64,
    pub qty: f64,
    pub ts_ms: i64,
}

#[repr(C, packed)]
#[derive(Clone, Copy)]
pub struct MatchingEventPayload {
    pub sequence: u64,
    pub response_stream_id: i32,
    pub event_kind: u8,
    pub _pad: [u8; 3],
    pub order_id: i64,
    pub client_order_id: i64,
    pub participant_id: i64,
    pub counterparty_order_id: i64,
    pub symbol: [u8; 16], // null-padded
    pub price_ticks: i64,
    pub quantity_lots: i64,
    pub remaining_lots: i64,
    pub ts_ns: u64,
}

/// Largest payload size. Anything new must fit here or the union grows.
pub const MAX_PAYLOAD: usize = 136;

const _: () = {
    // Compile-time check that every payload fits in the union.
    assert!(size_of::<OrderUpsertPayload>() <= MAX_PAYLOAD);
    assert!(size_of::<OrderDeletePayload>() <= MAX_PAYLOAD);
    assert!(size_of::<AccountSetPayload>() <= MAX_PAYLOAD);
    assert!(size_of::<TradeInsertPayload>() <= MAX_PAYLOAD);
    assert!(size_of::<OrderFillUpdatePayload>() <= MAX_PAYLOAD);
    assert!(size_of::<MatchingEventPayload>() <= MAX_PAYLOAD);
};

#[repr(C, packed)]
#[derive(Clone, Copy)]
pub struct PersistFrame {
    pub kind: u8,
    pub _pad: [u8; 7],
    pub payload: [u8; MAX_PAYLOAD],
}

pub const FRAME_SIZE: usize = size_of::<PersistFrame>();

const _: () = {
    assert!(FRAME_SIZE == 8 + MAX_PAYLOAD);
};

impl PersistFrame {
    pub fn zero() -> Self {
        Self {
            kind: 0,
            _pad: [0; 7],
            payload: [0; MAX_PAYLOAD],
        }
    }

    pub fn order_upsert(p: OrderUpsertPayload) -> Self {
        let mut f = Self::zero();
        f.kind = PersistKind::OrderUpsert as u8;
        let bytes = unsafe {
            std::slice::from_raw_parts(&p as *const _ as *const u8, size_of::<OrderUpsertPayload>())
        };
        f.payload[..bytes.len()].copy_from_slice(bytes);
        f
    }

    pub fn order_delete(p: OrderDeletePayload) -> Self {
        let mut f = Self::zero();
        f.kind = PersistKind::OrderDelete as u8;
        let bytes = unsafe {
            std::slice::from_raw_parts(&p as *const _ as *const u8, size_of::<OrderDeletePayload>())
        };
        f.payload[..bytes.len()].copy_from_slice(bytes);
        f
    }

    pub fn account_set(p: AccountSetPayload) -> Self {
        let mut f = Self::zero();
        f.kind = PersistKind::AccountSet as u8;
        let bytes = unsafe {
            std::slice::from_raw_parts(&p as *const _ as *const u8, size_of::<AccountSetPayload>())
        };
        f.payload[..bytes.len()].copy_from_slice(bytes);
        f
    }

    pub fn order_fill_update(p: OrderFillUpdatePayload) -> Self {
        let mut f = Self::zero();
        f.kind = PersistKind::OrderFillUpdate as u8;
        let bytes = unsafe {
            std::slice::from_raw_parts(
                &p as *const _ as *const u8,
                size_of::<OrderFillUpdatePayload>(),
            )
        };
        f.payload[..bytes.len()].copy_from_slice(bytes);
        f
    }

    pub fn trade_insert(p: TradeInsertPayload) -> Self {
        let mut f = Self::zero();
        f.kind = PersistKind::TradeInsert as u8;
        let bytes = unsafe {
            std::slice::from_raw_parts(&p as *const _ as *const u8, size_of::<TradeInsertPayload>())
        };
        f.payload[..bytes.len()].copy_from_slice(bytes);
        f
    }

    pub fn matching_event(p: MatchingEventPayload) -> Self {
        let mut f = Self::zero();
        f.kind = PersistKind::MatchingEvent as u8;
        let bytes = unsafe {
            std::slice::from_raw_parts(
                &p as *const _ as *const u8,
                size_of::<MatchingEventPayload>(),
            )
        };
        f.payload[..bytes.len()].copy_from_slice(bytes);
        f
    }

    /// Cast the frame as a flat byte slice for an Aeron `publish`.
    pub fn as_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self as *const _ as *const u8, FRAME_SIZE) }
    }

    /// Parse an incoming Aeron payload into a frame, validating length.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < FRAME_SIZE {
            return None;
        }
        let frame = unsafe { std::ptr::read_unaligned(data.as_ptr() as *const PersistFrame) };
        Some(frame)
    }

    pub fn kind(&self) -> Option<PersistKind> {
        PersistKind::from_u8(self.kind)
    }

    pub fn as_order_upsert(&self) -> Option<OrderUpsertPayload> {
        if self.kind() != Some(PersistKind::OrderUpsert) {
            return None;
        }
        Some(unsafe {
            std::ptr::read_unaligned(self.payload.as_ptr() as *const OrderUpsertPayload)
        })
    }

    pub fn as_order_delete(&self) -> Option<OrderDeletePayload> {
        if self.kind() != Some(PersistKind::OrderDelete) {
            return None;
        }
        Some(unsafe {
            std::ptr::read_unaligned(self.payload.as_ptr() as *const OrderDeletePayload)
        })
    }

    pub fn as_account_set(&self) -> Option<AccountSetPayload> {
        if self.kind() != Some(PersistKind::AccountSet) {
            return None;
        }
        Some(unsafe { std::ptr::read_unaligned(self.payload.as_ptr() as *const AccountSetPayload) })
    }

    pub fn as_order_fill_update(&self) -> Option<OrderFillUpdatePayload> {
        if self.kind() != Some(PersistKind::OrderFillUpdate) {
            return None;
        }
        Some(unsafe {
            std::ptr::read_unaligned(self.payload.as_ptr() as *const OrderFillUpdatePayload)
        })
    }

    pub fn as_trade_insert(&self) -> Option<TradeInsertPayload> {
        if self.kind() != Some(PersistKind::TradeInsert) {
            return None;
        }
        Some(unsafe {
            std::ptr::read_unaligned(self.payload.as_ptr() as *const TradeInsertPayload)
        })
    }

    pub fn as_matching_event(&self) -> Option<MatchingEventPayload> {
        if self.kind() != Some(PersistKind::MatchingEvent) {
            return None;
        }
        Some(unsafe {
            std::ptr::read_unaligned(self.payload.as_ptr() as *const MatchingEventPayload)
        })
    }
}

/// Pack a `&str` into a fixed-length null-padded byte array.
pub fn pack_str<const N: usize>(s: &str) -> [u8; N] {
    let mut out = [0u8; N];
    let bytes = s.as_bytes();
    let n = bytes.len().min(N);
    out[..n].copy_from_slice(&bytes[..n]);
    out
}

/// Read a null-padded fixed-length byte array back as a `&str`.
pub fn unpack_str(bytes: &[u8]) -> &str {
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    std::str::from_utf8(&bytes[..end]).unwrap_or("")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_size_known() {
        // 8-byte header + 136-byte payload = 144 bytes; document it.
        assert_eq!(FRAME_SIZE, 144);
    }

    #[test]
    fn order_upsert_roundtrip() {
        let p = OrderUpsertPayload {
            id: 42,
            user_id: 7,
            symbol: pack_str("BTC_USDT"),
            side: 1,
            status: 2,
            _pad: [0; 6],
            order_type: pack_str("limit"),
            price: 73000.5,
            qty: 0.123,
            filled: 0.045,
            freeze_price: 73000.5,
            client_order_id: pack_str("mm-abc-7"),
            created_at_ms: 1_700_000_000_000,
        };
        let f = PersistFrame::order_upsert(p);
        let bytes = f.as_bytes();
        assert_eq!(bytes.len(), FRAME_SIZE);
        let back = PersistFrame::from_bytes(bytes).expect("parse");
        let q = back.as_order_upsert().expect("upsert");
        assert_eq!({ q.id }, 42);
        assert_eq!({ q.user_id }, 7);
        assert_eq!(unpack_str(&q.symbol), "BTC_USDT");
        assert_eq!(q.side, 1);
        assert_eq!(q.status, 2);
        assert_eq!(unpack_str(&q.order_type), "limit");
        assert!(({ q.price } - 73000.5).abs() < 1e-9);
        assert!(({ q.qty } - 0.123).abs() < 1e-9);
        assert!(({ q.filled } - 0.045).abs() < 1e-9);
        assert!(({ q.freeze_price } - 73000.5).abs() < 1e-9);
        assert_eq!(unpack_str(&q.client_order_id), "mm-abc-7");
        assert_eq!({ q.created_at_ms }, 1_700_000_000_000);
    }

    #[test]
    fn order_delete_roundtrip() {
        let f = PersistFrame::order_delete(OrderDeletePayload { id: 999 });
        let back = PersistFrame::from_bytes(f.as_bytes()).expect("parse");
        let p = back.as_order_delete().expect("delete");
        assert_eq!({ p.id }, 999);
        assert!(back.as_order_upsert().is_none());
    }

    #[test]
    fn account_set_roundtrip() {
        let p = AccountSetPayload {
            user_id: 16,
            asset: pack_str("USDT"),
            balance: 12345.6,
            frozen: 100.0,
            balance_atoms: 1_234_560_000_000,
            frozen_atoms: 10_000_000_000,
        };
        let f = PersistFrame::account_set(p);
        let back = PersistFrame::from_bytes(f.as_bytes()).expect("parse");
        let q = back.as_account_set().expect("acct");
        assert_eq!({ q.user_id }, 16);
        assert_eq!(unpack_str(&q.asset), "USDT");
        assert!(({ q.balance } - 12345.6).abs() < 1e-9);
        assert!(({ q.frozen } - 100.0).abs() < 1e-9);
        assert_eq!({ q.balance_atoms }, 1_234_560_000_000);
        assert_eq!({ q.frozen_atoms }, 10_000_000_000);
    }

    #[test]
    fn trade_insert_roundtrip() {
        let p = TradeInsertPayload {
            buy_order_id: 100,
            sell_order_id: 101,
            symbol: pack_str("BTC_USDT"),
            price: 73000.0,
            qty: 0.001,
            ts_ms: 1_700_000_000_000,
        };
        let f = PersistFrame::trade_insert(p);
        let back = PersistFrame::from_bytes(f.as_bytes()).expect("parse");
        let q = back.as_trade_insert().expect("trade");
        assert_eq!({ q.buy_order_id }, 100);
        assert_eq!({ q.sell_order_id }, 101);
        assert_eq!(unpack_str(&q.symbol), "BTC_USDT");
    }

    #[test]
    fn matching_event_roundtrip() {
        let p = MatchingEventPayload {
            sequence: 9,
            response_stream_id: 200,
            event_kind: matching_event_kind::ACCEPTED,
            _pad: [0; 3],
            order_id: 1001,
            client_order_id: 1000,
            participant_id: 77,
            counterparty_order_id: 0,
            symbol: pack_str("BTC_USDT"),
            price_ticks: 7_000_000,
            quantity_lots: 12_345,
            remaining_lots: 12_345,
            ts_ns: 1_700_000_001_000_000_000,
        };
        let f = PersistFrame::matching_event(p);
        assert_eq!(f.kind(), Some(PersistKind::MatchingEvent));
        let back = PersistFrame::from_bytes(f.as_bytes()).expect("parse");
        let q = back.as_matching_event().expect("matching event");
        assert_eq!({ q.sequence }, 9);
        assert_eq!({ q.response_stream_id }, 200);
        assert_eq!(q.event_kind, matching_event_kind::ACCEPTED);
        assert_eq!({ q.order_id }, 1001);
        assert_eq!(unpack_str(&q.symbol), "BTC_USDT");
        assert_eq!({ q.price_ticks }, 7_000_000);
        assert_eq!({ q.quantity_lots }, 12_345);
    }

    #[test]
    fn rejects_short_buffer() {
        assert!(PersistFrame::from_bytes(&[0u8; 10]).is_none());
    }

    #[test]
    fn rejects_wrong_kind() {
        let mut f = PersistFrame::zero();
        f.kind = 99;
        assert!(f.kind().is_none());
        assert!(f.as_order_upsert().is_none());
    }
}
