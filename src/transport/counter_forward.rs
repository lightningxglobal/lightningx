use crate::sbe::{CancelOrderRequest, NewOrderRequest};
use crate::transport::pack_str16;

pub const COUNTER_FORWARD_KIND_NEW_ORDER: u8 = 1;
pub const COUNTER_FORWARD_KIND_CANCEL: u8 = 2;
pub const COUNTER_FORWARD_KIND_WS_FRAME: u8 = 3;

pub const COUNTER_FORWARD_MAX_CLIENT_ID: usize = 32;
pub const COUNTER_FORWARD_MAX_WS_FRAME: usize = 512;

#[derive(Clone, Copy)]
pub enum CounterForwardMsg {
    NewOrder(CounterForwardNewOrder),
    Cancel(CounterForwardCancel),
    WsFrame(CounterForwardWsFrame),
}

#[repr(C, packed)]
#[derive(Clone, Copy)]
pub struct CounterForwardOrderMeta {
    pub user_id: i64,
    pub symbol: [u8; 16],
    pub side: u8,
    pub _pad1: [u8; 7],
    pub order_type: [u8; 16],
    pub price: f64,
    pub qty: f64,
    pub freeze_price: f64,
    pub initial_margin_atoms: i64,
    pub client_order_id_len: u8,
    pub _pad2: [u8; 7],
    pub client_order_id: [u8; COUNTER_FORWARD_MAX_CLIENT_ID],
}

#[repr(C, packed)]
#[derive(Clone, Copy)]
pub struct CounterForwardNewOrder {
    pub kind: u8,
    pub ingress_desk_id: u16,
    pub _pad: [u8; 5],
    pub req: NewOrderRequest,
    pub meta: CounterForwardOrderMeta,
}

#[repr(C, packed)]
#[derive(Clone, Copy)]
pub struct CounterForwardCancel {
    pub kind: u8,
    pub ingress_desk_id: u16,
    pub _pad: [u8; 5],
    pub req: CancelOrderRequest,
}

#[repr(C, packed)]
#[derive(Clone, Copy)]
pub struct CounterForwardWsFrame {
    pub kind: u8,
    pub user_id: i64,
    pub order_id: u64,
    pub len: u16,
    pub _pad: [u8; 5],
    pub payload: [u8; COUNTER_FORWARD_MAX_WS_FRAME],
}

static_assertions::const_assert_eq!(std::mem::size_of::<CounterForwardOrderMeta>(), 120);
static_assertions::const_assert_eq!(std::mem::size_of::<CounterForwardNewOrder>(), 192);
static_assertions::const_assert_eq!(std::mem::size_of::<CounterForwardCancel>(), 32);
static_assertions::const_assert_eq!(std::mem::size_of::<CounterForwardWsFrame>(), 536);

#[inline]
pub fn pack_client_order_id(value: &str) -> (u8, [u8; COUNTER_FORWARD_MAX_CLIENT_ID]) {
    let mut out = [0u8; COUNTER_FORWARD_MAX_CLIENT_ID];
    let bytes = value.as_bytes();
    let n = bytes.len().min(COUNTER_FORWARD_MAX_CLIENT_ID);
    out[..n].copy_from_slice(&bytes[..n]);
    (n as u8, out)
}

#[inline]
pub fn unpack_client_order_id(len: u8, value: &[u8; COUNTER_FORWARD_MAX_CLIENT_ID]) -> &str {
    let n = (len as usize).min(COUNTER_FORWARD_MAX_CLIENT_ID);
    std::str::from_utf8(&value[..n]).unwrap_or("")
}

impl CounterForwardOrderMeta {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        user_id: i64,
        symbol: [u8; 16],
        side: u8,
        order_type: &str,
        price: Option<f64>,
        qty: f64,
        freeze_price: f64,
        initial_margin_atoms: i64,
        client_order_id: &str,
    ) -> Self {
        let (client_order_id_len, client_order_id) = pack_client_order_id(client_order_id);
        Self {
            user_id,
            symbol,
            side,
            _pad1: [0; 7],
            order_type: pack_str16(order_type),
            price: price.unwrap_or(0.0),
            qty,
            freeze_price,
            initial_margin_atoms,
            client_order_id_len,
            _pad2: [0; 7],
            client_order_id,
        }
    }

    #[inline]
    pub fn client_order_id_string(&self) -> String {
        let len = self.client_order_id_len;
        let value = self.client_order_id;
        unpack_client_order_id(len, &value).to_string()
    }
}

impl CounterForwardNewOrder {
    pub fn new(ingress_desk_id: u16, req: NewOrderRequest, meta: CounterForwardOrderMeta) -> Self {
        Self {
            kind: COUNTER_FORWARD_KIND_NEW_ORDER,
            ingress_desk_id,
            _pad: [0; 5],
            req,
            meta,
        }
    }
}

impl CounterForwardCancel {
    pub fn new(ingress_desk_id: u16, req: CancelOrderRequest) -> Self {
        Self {
            kind: COUNTER_FORWARD_KIND_CANCEL,
            ingress_desk_id,
            _pad: [0; 5],
            req,
        }
    }
}

impl CounterForwardWsFrame {
    pub fn new(user_id: i64, order_id: u64, payload: &[u8]) -> Option<Self> {
        if payload.len() > COUNTER_FORWARD_MAX_WS_FRAME {
            return None;
        }
        let mut out = Self {
            kind: COUNTER_FORWARD_KIND_WS_FRAME,
            user_id,
            order_id,
            len: payload.len() as u16,
            _pad: [0; 5],
            payload: [0; COUNTER_FORWARD_MAX_WS_FRAME],
        };
        out.payload[..payload.len()].copy_from_slice(payload);
        Some(out)
    }

    #[inline]
    pub fn payload(&self) -> &[u8] {
        let len = (self.len as usize).min(COUNTER_FORWARD_MAX_WS_FRAME);
        &self.payload[..len]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sym16(symbol: &str) -> [u8; 16] {
        let mut out = [0u8; 16];
        let bytes = symbol.as_bytes();
        out[..bytes.len().min(16)].copy_from_slice(&bytes[..bytes.len().min(16)]);
        out
    }

    #[test]
    fn forwarded_order_meta_round_trips_client_order_id() {
        let meta = CounterForwardOrderMeta::new(
            42,
            sym16("BTC_USDT"),
            0,
            "limit",
            Some(65000.0),
            0.01,
            650.0,
            65000,
            "client-order-1",
        );
        assert_eq!(meta.client_order_id_string(), "client-order-1");
        assert_eq!({ meta.user_id }, 42);
        assert_eq!({ meta.initial_margin_atoms }, 65000);
    }

    #[test]
    fn forwarded_ws_frame_rejects_oversized_payload() {
        let too_large = vec![1u8; COUNTER_FORWARD_MAX_WS_FRAME + 1];
        assert!(CounterForwardWsFrame::new(1, 2, &too_large).is_none());
    }

    #[test]
    fn forwarded_ws_frame_keeps_payload_bytes() {
        let payload = [3u8, 1, 2, 3, 4];
        let frame = CounterForwardWsFrame::new(7, 9, &payload).unwrap();
        assert_eq!(frame.payload(), payload);
        assert_eq!({ frame.user_id }, 7);
        assert_eq!({ frame.order_id }, 9);
    }

    #[test]
    fn forwarded_new_order_keeps_ingress_and_owner_response_stream() {
        let owner_desk = 2;
        let req = NewOrderRequest {
            client_order_id: 99,
            participant_id: 42,
            price_ticks: 123,
            quantity_lots: 456,
            side: 0,
            time_in_force: 0,
            response_stream_id: crate::aeron_channels::order_update_stream_for_desk(owner_desk),
            _pad: [0; 10],
            symbol: sym16("SOL_USDT"),
        };
        let meta = CounterForwardOrderMeta::new(
            42,
            sym16("SOL_USDT"),
            0,
            "limit",
            Some(12.3),
            4.56,
            12.3,
            123,
            "c-99",
        );
        let frame = CounterForwardNewOrder::new(1, req, meta);
        assert_eq!({ frame.kind }, COUNTER_FORWARD_KIND_NEW_ORDER);
        assert_eq!({ frame.ingress_desk_id }, 1);
        assert_eq!(
            { frame.req.response_stream_id },
            crate::aeron_channels::order_update_stream_for_desk(owner_desk)
        );
        assert_eq!({ frame.meta.user_id }, 42);
        assert_eq!(frame.meta.client_order_id_string(), "c-99");
    }

    #[test]
    fn forwarded_cancel_keeps_ingress_and_response_stream() {
        let owner_desk = 3;
        let req = CancelOrderRequest {
            order_id: 77,
            participant_id: 43,
            response_stream_id: crate::aeron_channels::order_update_stream_for_desk(owner_desk),
            _pad: [0; 4],
        };
        let frame = CounterForwardCancel::new(0, req);
        assert_eq!({ frame.kind }, COUNTER_FORWARD_KIND_CANCEL);
        assert_eq!({ frame.ingress_desk_id }, 0);
        assert_eq!({ frame.req.order_id }, 77);
        assert_eq!(
            { frame.req.response_stream_id },
            crate::aeron_channels::order_update_stream_for_desk(owner_desk)
        );
    }
}
