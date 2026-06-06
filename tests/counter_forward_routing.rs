use lightning_exchange::aeron_channels::{
    COUNTER_FORWARD_CMD_STREAM_BASE, COUNTER_FORWARD_RESP_STREAM_BASE, ORDER_UPDATE_STREAM_BASE,
    counter_forward_cmd_stream_for_desk, counter_forward_resp_stream_for_desk,
    order_update_stream_for_desk,
};
use lightning_exchange::desk::counter_shard::{COUNTER_SHARD_COUNT, owner_shard_for_user_id};
use lightning_exchange::sbe::{CancelOrderRequest, NewOrderRequest};
use lightning_exchange::transport::counter_forward::{
    COUNTER_FORWARD_KIND_CANCEL, COUNTER_FORWARD_KIND_NEW_ORDER, COUNTER_FORWARD_KIND_WS_FRAME,
    CounterForwardCancel, CounterForwardNewOrder, CounterForwardOrderMeta, CounterForwardWsFrame,
};

fn sym16(symbol: &str) -> [u8; 16] {
    let mut out = [0u8; 16];
    let bytes = symbol.as_bytes();
    out[..bytes.len().min(16)].copy_from_slice(&bytes[..bytes.len().min(16)]);
    out
}

#[test]
fn user_owner_maps_to_forward_command_and_response_streams() {
    for user_id in 1..=64 {
        let owner = owner_shard_for_user_id(user_id);
        assert!(owner < COUNTER_SHARD_COUNT);
        assert_eq!(
            counter_forward_cmd_stream_for_desk(owner),
            COUNTER_FORWARD_CMD_STREAM_BASE + owner as i32
        );
        assert_eq!(
            counter_forward_resp_stream_for_desk(owner),
            COUNTER_FORWARD_RESP_STREAM_BASE + owner as i32
        );
        assert_eq!(
            order_update_stream_for_desk(owner),
            ORDER_UPDATE_STREAM_BASE + owner as i32
        );
    }
}

#[test]
fn forwarded_order_targets_owner_private_update_stream() {
    let user_id = 10;
    let owner = owner_shard_for_user_id(user_id);
    let req = NewOrderRequest {
        client_order_id: 1001,
        participant_id: user_id as u64,
        price_ticks: 50_000,
        quantity_lots: 1,
        side: 0,
        time_in_force: 0,
        response_stream_id: order_update_stream_for_desk(owner),
        _pad: [0; 10],
        symbol: sym16("BTC_USDT"),
    };
    let meta = CounterForwardOrderMeta::new(
        user_id,
        sym16("BTC_USDT"),
        0,
        "limit",
        Some(50_000.0),
        1.0,
        50_000.0,
        5_000,
        "client-1001",
    );
    let frame = CounterForwardNewOrder::new(3, req, meta);
    assert_eq!({ frame.kind }, COUNTER_FORWARD_KIND_NEW_ORDER);
    assert_eq!({ frame.ingress_desk_id }, 3);
    assert_eq!(
        { frame.req.response_stream_id },
        order_update_stream_for_desk(owner)
    );
    assert_eq!({ frame.meta.user_id }, user_id);
}

#[test]
fn wrong_owner_order_uses_owner_cmd_stream_and_ingress_resp_stream() {
    let user_id = 10;
    let owner = owner_shard_for_user_id(user_id);
    let ingress = (owner + 1) % COUNTER_SHARD_COUNT;
    assert_ne!(ingress, owner);

    let req = NewOrderRequest {
        client_order_id: 2001,
        participant_id: user_id as u64,
        price_ticks: 51_000,
        quantity_lots: 2,
        side: 0,
        time_in_force: 0,
        response_stream_id: order_update_stream_for_desk(owner),
        _pad: [0; 10],
        symbol: sym16("BTC_USDT"),
    };
    let meta = CounterForwardOrderMeta::new(
        user_id,
        sym16("BTC_USDT"),
        0,
        "limit",
        Some(51_000.0),
        2.0,
        102_000.0,
        10_200,
        "client-2001",
    );
    let frame = CounterForwardNewOrder::new(ingress, req, meta);

    assert_eq!(
        counter_forward_cmd_stream_for_desk(owner),
        COUNTER_FORWARD_CMD_STREAM_BASE + owner as i32
    );
    assert_eq!(
        counter_forward_resp_stream_for_desk(ingress),
        COUNTER_FORWARD_RESP_STREAM_BASE + ingress as i32
    );
    assert_eq!({ frame.ingress_desk_id }, ingress);
    assert_eq!(
        { frame.req.response_stream_id },
        order_update_stream_for_desk(owner)
    );
}

#[test]
fn forwarded_cancel_targets_owner_private_update_stream() {
    let user_id = 11;
    let owner = owner_shard_for_user_id(user_id);
    let ingress = (owner + 2) % COUNTER_SHARD_COUNT;
    assert_ne!(ingress, owner);

    let req = CancelOrderRequest {
        order_id: 7001,
        participant_id: user_id as u64,
        response_stream_id: order_update_stream_for_desk(owner),
        _pad: [0; 4],
    };
    let frame = CounterForwardCancel::new(ingress, req);

    assert_eq!({ frame.kind }, COUNTER_FORWARD_KIND_CANCEL);
    assert_eq!({ frame.ingress_desk_id }, ingress);
    assert_eq!({ frame.req.participant_id }, user_id as u64);
    assert_eq!(
        { frame.req.response_stream_id },
        order_update_stream_for_desk(owner)
    );
}

#[test]
fn forwarded_ws_frame_returns_to_ingress_payload_unchanged() {
    let user_id = 13;
    let ingress = 2;
    let payload = [9u8, 8, 7, 6, 5, 4];
    let frame = CounterForwardWsFrame::new(user_id, 9001, &payload).unwrap();

    assert_eq!(
        counter_forward_resp_stream_for_desk(ingress),
        COUNTER_FORWARD_RESP_STREAM_BASE + ingress as i32
    );
    assert_eq!({ frame.kind }, COUNTER_FORWARD_KIND_WS_FRAME);
    assert_eq!({ frame.user_id }, user_id);
    assert_eq!({ frame.order_id }, 9001);
    assert_eq!(frame.payload(), payload);
}
