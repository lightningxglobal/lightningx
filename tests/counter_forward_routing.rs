use lightning_exchange::aeron_channels::{
    COUNTER_FORWARD_CMD_STREAM_BASE, COUNTER_FORWARD_RESP_STREAM_BASE, ORDER_UPDATE_STREAM_BASE,
    counter_forward_cmd_stream_for_desk, counter_forward_resp_stream_for_desk,
    order_update_stream_for_desk,
};
use lightning_exchange::desk::counter_shard::{COUNTER_SHARD_COUNT, owner_shard_for_user_id};
use lightning_exchange::sbe::NewOrderRequest;
use lightning_exchange::transport::counter_forward::{
    COUNTER_FORWARD_KIND_NEW_ORDER, CounterForwardNewOrder, CounterForwardOrderMeta,
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
