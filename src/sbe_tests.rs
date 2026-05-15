#[cfg(test)]
mod sbe_encoding_tests {
    use crate::transport::OrderUpdateMsg;
    use crate::sbe::TradeNotification;

    const SBE_HEADER_SIZE: usize = 8;

    // Test OrderUpdateMsg SBE encoding (with header)
    #[test]
    fn test_orderupdate_sbe_encoding_with_header() {
        let msg = OrderUpdateMsg {
            kind: 1, // ACCEPTED
            reject_reason: 0,
            _pad1: [0; 6],
            order_id: 12345,
            client_order_id: 67890,
            participant_id: 1,
            fill_price: 50000.0,
            fill_qty: 15.0,
            remaining_qty: 5.0,
            timestamp: 1234567890,
        };

        // Simulate server-side encoding (what AeronOrderUpdatePublisher does)
        let mut data = vec![0u8; 72]; // 8-byte header + 64-byte body

        // SBE Header (8 bytes)
        data[0..2].copy_from_slice(&56u16.to_le_bytes()); // block_length
        data[2..4].copy_from_slice(&2u16.to_le_bytes()); // template_id = 2
        data[4..6].copy_from_slice(&1u16.to_le_bytes()); // schema_id
        data[6..8].copy_from_slice(&0u16.to_le_bytes()); // version

        // Copy OrderUpdateMsg into body
        let msg_bytes = unsafe {
            std::slice::from_raw_parts(&msg as *const OrderUpdateMsg as *const u8, 64)
        };
        data[8..72].copy_from_slice(msg_bytes);

        // Verify header
        assert_eq!(u16::from_le_bytes([data[0], data[1]]), 56); // block_length
        assert_eq!(u16::from_le_bytes([data[2], data[3]]), 2); // template_id
        assert_eq!(u16::from_le_bytes([data[4], data[5]]), 1); // schema_id
        assert_eq!(u16::from_le_bytes([data[6], data[7]]), 0); // version

        // Verify message fields (read from byte 8 onward)
        assert_eq!(data[8], 1); // kind
        assert_eq!(u64::from_le_bytes([data[16], data[17], data[18], data[19],
                                       data[20], data[21], data[22], data[23]]), 12345); // order_id
        assert_eq!(u64::from_le_bytes([data[24], data[25], data[26], data[27],
                                       data[28], data[29], data[30], data[31]]), 67890); // client_order_id
    }

    // Test OrderUpdateMsg SBE decoding (client-side)
    #[test]
    fn test_orderupdate_sbe_decoding_with_header() {
        // Simulate received message from Aeron (with header)
        let mut data = vec![0u8; 72];

        // Header
        data[0..2].copy_from_slice(&56u16.to_le_bytes());
        data[2..4].copy_from_slice(&2u16.to_le_bytes());
        data[4..6].copy_from_slice(&1u16.to_le_bytes());
        data[6..8].copy_from_slice(&0u16.to_le_bytes());

        // OrderUpdateMsg fields (starting at byte 8)
        data[8] = 1; // kind = ACCEPTED
        data[9] = 0; // reject_reason
        let order_id: u64 = 12345;
        data[16..24].copy_from_slice(&order_id.to_le_bytes());
        let client_order_id: u64 = 67890;
        data[24..32].copy_from_slice(&client_order_id.to_le_bytes());
        let participant_id: u64 = 1;
        data[32..40].copy_from_slice(&participant_id.to_le_bytes());
        let fill_price: f64 = 50000.0;
        data[40..48].copy_from_slice(&fill_price.to_le_bytes());
        let fill_qty: f64 = 15.0;
        data[48..56].copy_from_slice(&fill_qty.to_le_bytes());
        let remaining_qty: f64 = 5.0;
        data[56..64].copy_from_slice(&remaining_qty.to_le_bytes());
        let timestamp: u64 = 1234567890;
        data[64..72].copy_from_slice(&timestamp.to_le_bytes());

        // Client-side parsing (skip header, read from byte 8)
        let kind = data[8];
        let order_id_parsed = u64::from_le_bytes(data[16..24].try_into().unwrap());
        let client_order_id_parsed = u64::from_le_bytes(data[24..32].try_into().unwrap());
        let participant_id_parsed = u64::from_le_bytes(data[32..40].try_into().unwrap());
        let fill_price_parsed = f64::from_le_bytes(data[40..48].try_into().unwrap());
        let fill_qty_parsed = f64::from_le_bytes(data[48..56].try_into().unwrap());
        let remaining_qty_parsed = f64::from_le_bytes(data[56..64].try_into().unwrap());
        let timestamp_parsed = u64::from_le_bytes(data[64..72].try_into().unwrap());

        assert_eq!(kind, 1);
        assert_eq!(order_id_parsed, 12345);
        assert_eq!(client_order_id_parsed, 67890);
        assert_eq!(participant_id_parsed, 1);
        assert_eq!(fill_price_parsed, 50000.0);
        assert_eq!(fill_qty_parsed, 15.0);
        assert_eq!(remaining_qty_parsed, 5.0);
        assert_eq!(timestamp_parsed, 1234567890);
    }

    // Test Trade SBE encoding (with header)
    #[test]
    fn test_trade_sbe_encoding_with_header() {
        let msg = TradeNotification {
            sequence: 0,
            taker_order_id: 3,
            maker_order_id: 2,
            price: 50000.0,
            quantity: 15.0,
            side: 1, // SELL
            _pad: [0; 7],
        };

        // Simulate server-side encoding
        let mut data = vec![0u8; 56]; // 8-byte header + 48-byte body

        // SBE Header
        data[0..2].copy_from_slice(&48u16.to_le_bytes());
        data[2..4].copy_from_slice(&3u16.to_le_bytes()); // template_id = 3
        data[4..6].copy_from_slice(&1u16.to_le_bytes());
        data[6..8].copy_from_slice(&0u16.to_le_bytes());

        // Copy TradeNotification
        let msg_bytes = unsafe {
            std::slice::from_raw_parts(&msg as *const TradeNotification as *const u8, 48)
        };
        data[8..56].copy_from_slice(msg_bytes);

        // Verify header
        assert_eq!(u16::from_le_bytes([data[0], data[1]]), 48);
        assert_eq!(u16::from_le_bytes([data[2], data[3]]), 3); // template_id

        // Verify body
        let sequence = u64::from_le_bytes(data[8..16].try_into().unwrap());
        let taker_order_id = u64::from_le_bytes(data[16..24].try_into().unwrap());
        let maker_order_id = u64::from_le_bytes(data[24..32].try_into().unwrap());
        let price = f64::from_le_bytes(data[32..40].try_into().unwrap());
        let quantity = f64::from_le_bytes(data[40..48].try_into().unwrap());
        let side = data[48];

        assert_eq!(sequence, 0);
        assert_eq!(taker_order_id, 3);
        assert_eq!(maker_order_id, 2);
        assert_eq!(price, 50000.0);
        assert_eq!(quantity, 15.0);
        assert_eq!(side, 1);
    }

    // Test Trade SBE decoding (client-side)
    #[test]
    fn test_trade_sbe_decoding_with_header() {
        // Simulate received message
        let mut data = vec![0u8; 56];

        // Header
        data[0..2].copy_from_slice(&48u16.to_le_bytes());
        data[2..4].copy_from_slice(&3u16.to_le_bytes());
        data[4..6].copy_from_slice(&1u16.to_le_bytes());
        data[6..8].copy_from_slice(&0u16.to_le_bytes());

        // TradeNotification fields (starting at byte 8)
        let sequence: u64 = 0;
        data[8..16].copy_from_slice(&sequence.to_le_bytes());
        let taker_order_id: u64 = 3;
        data[16..24].copy_from_slice(&taker_order_id.to_le_bytes());
        let maker_order_id: u64 = 2;
        data[24..32].copy_from_slice(&maker_order_id.to_le_bytes());
        let price: f64 = 50000.0;
        data[32..40].copy_from_slice(&price.to_le_bytes());
        let quantity: f64 = 15.0;
        data[40..48].copy_from_slice(&quantity.to_le_bytes());
        data[48] = 1; // side = SELL

        // Client-side parsing
        let sequence_parsed = u64::from_le_bytes(data[8..16].try_into().unwrap());
        let taker_order_id_parsed = u64::from_le_bytes(data[16..24].try_into().unwrap());
        let maker_order_id_parsed = u64::from_le_bytes(data[24..32].try_into().unwrap());
        let price_parsed = f64::from_le_bytes(data[32..40].try_into().unwrap());
        let quantity_parsed = f64::from_le_bytes(data[40..48].try_into().unwrap());
        let side_parsed = data[48];

        assert_eq!(sequence_parsed, 0);
        assert_eq!(taker_order_id_parsed, 3);
        assert_eq!(maker_order_id_parsed, 2);
        assert_eq!(price_parsed, 50000.0);
        assert_eq!(quantity_parsed, 15.0);
        assert_eq!(side_parsed, 1);
    }

    // Test that message sizes are correct
    #[test]
    fn test_message_struct_sizes() {
        assert_eq!(std::mem::size_of::<OrderUpdateMsg>(), 64);
        assert_eq!(std::mem::size_of::<TradeNotification>(), 48);
    }

    // Test full round-trip: create → encode → decode → verify
    #[test]
    fn test_orderupdate_roundtrip() {
        let original = OrderUpdateMsg {
            kind: 2, // FILLED
            reject_reason: 0,
            _pad1: [0; 6],
            order_id: 999,
            client_order_id: 888,
            participant_id: 777,
            fill_price: 49999.5,
            fill_qty: 20.5,
            remaining_qty: 0.0,
            timestamp: 9876543210,
        };

        // Encode
        let mut encoded = vec![0u8; 72];
        encoded[0..2].copy_from_slice(&56u16.to_le_bytes());
        encoded[2..4].copy_from_slice(&2u16.to_le_bytes());
        encoded[4..6].copy_from_slice(&1u16.to_le_bytes());
        encoded[6..8].copy_from_slice(&0u16.to_le_bytes());

        let msg_bytes = unsafe {
            std::slice::from_raw_parts(&original as *const OrderUpdateMsg as *const u8, 64)
        };
        encoded[8..72].copy_from_slice(msg_bytes);

        // Decode
        let kind = encoded[8];
        let order_id = u64::from_le_bytes(encoded[16..24].try_into().unwrap());
        let client_order_id = u64::from_le_bytes(encoded[24..32].try_into().unwrap());
        let participant_id = u64::from_le_bytes(encoded[32..40].try_into().unwrap());
        let fill_price = f64::from_le_bytes(encoded[40..48].try_into().unwrap());
        let fill_qty = f64::from_le_bytes(encoded[48..56].try_into().unwrap());
        let remaining_qty = f64::from_le_bytes(encoded[56..64].try_into().unwrap());
        let timestamp = u64::from_le_bytes(encoded[64..72].try_into().unwrap());

        // Verify
        assert_eq!(kind, 2);
        assert_eq!(order_id, 999);
        assert_eq!(client_order_id, 888);
        assert_eq!(participant_id, 777);
        assert_eq!(fill_price, 49999.5);
        assert_eq!(fill_qty, 20.5);
        assert_eq!(remaining_qty, 0.0);
        assert_eq!(timestamp, 9876543210);
    }
}
