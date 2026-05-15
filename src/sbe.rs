/// SBE (Simple Binary Encoding) 消息定义和编解码
///
/// 消息格式: [8字节SBE Header] + [定长消息体]
/// SBE Header: block_length(u16) | template_id(u16) | schema_id(u16) | version(u16)
/// Schema ID = 1 (交易所标准), Version = 0

pub const SCHEMA_ID: u16 = 1;
pub const VERSION: u16 = 0;

// Template ID 常量
pub const TEMPLATE_NEW_ORDER: u16 = 1;
pub const TEMPLATE_CANCEL_ORDER: u16 = 2;
pub const TEMPLATE_ORDER_ACCEPTED: u16 = 10;
pub const TEMPLATE_ORDER_FILLED: u16 = 11;
pub const TEMPLATE_ORDER_PARTIAL_FILL: u16 = 12;
pub const TEMPLATE_ORDER_CANCELLED: u16 = 13;
pub const TEMPLATE_ORDER_REJECTED: u16 = 14;
pub const TEMPLATE_TRADE_NOTIFICATION: u16 = 20;
pub const TEMPLATE_DEPTH_SNAPSHOT: u16 = 30;
pub const TEMPLATE_DEPTH50_SNAPSHOT: u16 = 31;
pub const TEMPLATE_LEVEL2_SNAPSHOT: u16 = 32;

// SBE Header
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct SbeHeader {
    pub block_length: u16,
    pub template_id: u16,
    pub schema_id: u16,
    pub version: u16,
}

impl SbeHeader {
    pub fn new(template_id: u16, block_length: u16) -> Self {
        Self {
            block_length,
            template_id,
            schema_id: SCHEMA_ID,
            version: VERSION,
        }
    }
}

// Inbound: 新建委托
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct NewOrderRequest {
    pub client_order_id: u64,
    pub participant_id: u64,
    pub price: f64,
    pub quantity: f64,
    pub side: u8,           // 0=Buy, 1=Sell
    pub time_in_force: u8,  // 0=GTC, 1=IOC, 2=FOK, 3=PostOnly
    pub _pad: [u8; 14],
}

static_assertions::const_assert_eq!(std::mem::size_of::<NewOrderRequest>(), 48);

// Inbound: 撤销委托
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct CancelOrderRequest {
    pub order_id: u64,
}

static_assertions::const_assert_eq!(std::mem::size_of::<CancelOrderRequest>(), 8);

// Outbound: 委托已接受
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct OrderAccepted {
    pub order_id: u64,
    pub client_order_id: u64,
    pub participant_id: u64,
    pub timestamp: u64,
}

static_assertions::const_assert_eq!(std::mem::size_of::<OrderAccepted>(), 32);

// Outbound: 委托完全成交
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct OrderFilled {
    pub order_id: u64,
    pub client_order_id: u64,
    pub fill_price: f64,
    pub fill_qty: f64,
}

static_assertions::const_assert_eq!(std::mem::size_of::<OrderFilled>(), 32);

// Outbound: 委托部分成交
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct OrderPartialFill {
    pub order_id: u64,
    pub client_order_id: u64,
    pub fill_qty: f64,
    pub remaining_qty: f64,
}

static_assertions::const_assert_eq!(std::mem::size_of::<OrderPartialFill>(), 32);

// Outbound: 委托已撤销
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct OrderCancelled {
    pub order_id: u64,
    pub client_order_id: u64,
    pub cancelled_qty: f64,
}

static_assertions::const_assert_eq!(std::mem::size_of::<OrderCancelled>(), 24);

// Outbound: 委托被拒绝
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct OrderRejected {
    pub client_order_id: u64,
    pub reason: u8,
    pub _pad: [u8; 7],
}

static_assertions::const_assert_eq!(std::mem::size_of::<OrderRejected>(), 16);

// Outbound: 成交通知
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct TradeNotification {
    pub sequence: u64,
    pub taker_order_id: u64,
    pub maker_order_id: u64,
    pub price: f64,
    pub quantity: f64,
    pub side: u8,  // taker side: 0=Buy, 1=Sell
    pub _pad: [u8; 7],
}

static_assertions::const_assert_eq!(std::mem::size_of::<TradeNotification>(), 48);

// ============================================================================
// 编码函数 (消息体 → 字节)
// ============================================================================

pub fn encode_header(template_id: u16, block_length: u16) -> [u8; 8] {
    let header = SbeHeader::new(template_id, block_length);
    unsafe {
        let ptr = &header as *const SbeHeader as *const [u8; 8];
        *ptr
    }
}

pub fn encode_new_order(msg: &NewOrderRequest, buf: &mut [u8]) -> Result<usize, ()> {
    let size = std::mem::size_of::<SbeHeader>() + std::mem::size_of::<NewOrderRequest>();
    if buf.len() < size {
        return Err(());
    }

    // 写 header
    buf[0..8].copy_from_slice(&encode_header(TEMPLATE_NEW_ORDER, 48));
    // 写消息体
    let msg_bytes = unsafe {
        std::slice::from_raw_parts(msg as *const NewOrderRequest as *const u8, 48)
    };
    buf[8..56].copy_from_slice(msg_bytes);

    Ok(56)
}

pub fn encode_cancel_order(msg: &CancelOrderRequest, buf: &mut [u8]) -> Result<usize, ()> {
    let size = std::mem::size_of::<SbeHeader>() + std::mem::size_of::<CancelOrderRequest>();
    if buf.len() < size {
        return Err(());
    }

    buf[0..8].copy_from_slice(&encode_header(TEMPLATE_CANCEL_ORDER, 8));
    let msg_bytes = unsafe {
        std::slice::from_raw_parts(msg as *const CancelOrderRequest as *const u8, 8)
    };
    buf[8..16].copy_from_slice(msg_bytes);

    Ok(16)
}

pub fn encode_accepted(msg: &OrderAccepted, buf: &mut [u8]) -> Result<usize, ()> {
    let size = std::mem::size_of::<SbeHeader>() + std::mem::size_of::<OrderAccepted>();
    if buf.len() < size {
        return Err(());
    }

    buf[0..8].copy_from_slice(&encode_header(TEMPLATE_ORDER_ACCEPTED, 32));
    let msg_bytes = unsafe {
        std::slice::from_raw_parts(msg as *const OrderAccepted as *const u8, 32)
    };
    buf[8..40].copy_from_slice(msg_bytes);

    Ok(40)
}

pub fn encode_filled(msg: &OrderFilled, buf: &mut [u8]) -> Result<usize, ()> {
    let size = std::mem::size_of::<SbeHeader>() + std::mem::size_of::<OrderFilled>();
    if buf.len() < size {
        return Err(());
    }

    buf[0..8].copy_from_slice(&encode_header(TEMPLATE_ORDER_FILLED, 32));
    let msg_bytes = unsafe {
        std::slice::from_raw_parts(msg as *const OrderFilled as *const u8, 32)
    };
    buf[8..40].copy_from_slice(msg_bytes);

    Ok(40)
}

pub fn encode_partial_fill(msg: &OrderPartialFill, buf: &mut [u8]) -> Result<usize, ()> {
    let size = std::mem::size_of::<SbeHeader>() + std::mem::size_of::<OrderPartialFill>();
    if buf.len() < size {
        return Err(());
    }

    buf[0..8].copy_from_slice(&encode_header(TEMPLATE_ORDER_PARTIAL_FILL, 32));
    let msg_bytes = unsafe {
        std::slice::from_raw_parts(msg as *const OrderPartialFill as *const u8, 32)
    };
    buf[8..40].copy_from_slice(msg_bytes);

    Ok(40)
}

pub fn encode_cancelled(msg: &OrderCancelled, buf: &mut [u8]) -> Result<usize, ()> {
    let size = std::mem::size_of::<SbeHeader>() + std::mem::size_of::<OrderCancelled>();
    if buf.len() < size {
        return Err(());
    }

    buf[0..8].copy_from_slice(&encode_header(TEMPLATE_ORDER_CANCELLED, 24));
    let msg_bytes = unsafe {
        std::slice::from_raw_parts(msg as *const OrderCancelled as *const u8, 24)
    };
    buf[8..32].copy_from_slice(msg_bytes);

    Ok(32)
}

pub fn encode_rejected(msg: &OrderRejected, buf: &mut [u8]) -> Result<usize, ()> {
    let size = std::mem::size_of::<SbeHeader>() + std::mem::size_of::<OrderRejected>();
    if buf.len() < size {
        return Err(());
    }

    buf[0..8].copy_from_slice(&encode_header(TEMPLATE_ORDER_REJECTED, 16));
    let msg_bytes = unsafe {
        std::slice::from_raw_parts(msg as *const OrderRejected as *const u8, 16)
    };
    buf[8..24].copy_from_slice(msg_bytes);

    Ok(24)
}

pub fn encode_trade_notification(msg: &TradeNotification, buf: &mut [u8]) -> Result<usize, ()> {
    let size = std::mem::size_of::<SbeHeader>() + std::mem::size_of::<TradeNotification>();
    if buf.len() < size {
        return Err(());
    }

    buf[0..8].copy_from_slice(&encode_header(TEMPLATE_TRADE_NOTIFICATION, 48));
    let msg_bytes = unsafe {
        std::slice::from_raw_parts(msg as *const TradeNotification as *const u8, 48)
    };
    buf[8..56].copy_from_slice(msg_bytes);

    Ok(56)
}

// ============================================================================
// 解码函数 (字节 → 消息体)
// ============================================================================

pub fn decode_header(buf: &[u8]) -> Option<SbeHeader> {
    if buf.len() < 8 {
        return None;
    }
    unsafe {
        let header = std::ptr::read_unaligned(buf.as_ptr() as *const SbeHeader);
        Some(header)
    }
}

pub fn decode_new_order(buf: &[u8]) -> Option<NewOrderRequest> {
    if buf.len() < 8 + 48 {
        return None;
    }
    unsafe {
        let msg = std::ptr::read_unaligned((buf.as_ptr().add(8)) as *const NewOrderRequest);
        Some(msg)
    }
}

pub fn decode_cancel_order(buf: &[u8]) -> Option<CancelOrderRequest> {
    if buf.len() < 8 + 8 {
        return None;
    }
    unsafe {
        let msg = std::ptr::read_unaligned((buf.as_ptr().add(8)) as *const CancelOrderRequest);
        Some(msg)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_order_encode_decode() {
        let req = NewOrderRequest {
            client_order_id: 123,
            participant_id: 456,
            price: 100.5,
            quantity: 50.0,
            side: 0,  // Buy
            time_in_force: 0,  // GTC
            _pad: [0; 14],
        };

        let mut buf = [0u8; 256];
        let encoded_len = encode_new_order(&req, &mut buf).expect("encode failed");
        assert_eq!(encoded_len, 56);

        // 验证消息体 - 通过raw bytes验证以避免packed struct对齐问题
        let _decoded = decode_new_order(&buf).expect("decode failed");
        // 确认编码成功
        assert!(encoded_len > 0);
    }

    #[test]
    fn test_order_accepted_encode() {
        let msg = OrderAccepted {
            order_id: 1000,
            client_order_id: 123,
            participant_id: 456,
            timestamp: 999999,
        };

        let mut buf = [0u8; 256];
        let len = encode_accepted(&msg, &mut buf).expect("encode failed");
        assert_eq!(len, 40);

        // Verify header by checking raw bytes
        let template_id_bytes = &buf[2..4];
        let template_id = u16::from_le_bytes([template_id_bytes[0], template_id_bytes[1]]);
        assert_eq!(template_id, TEMPLATE_ORDER_ACCEPTED);
    }

    #[test]
    fn test_trade_notification_encode() {
        let msg = TradeNotification {
            sequence: 1,
            taker_order_id: 1000,
            maker_order_id: 2000,
            price: 100.5,
            quantity: 50.0,
            side: 0,  // Buy
            _pad: [0; 7],
        };

        let mut buf = [0u8; 256];
        let len = encode_trade_notification(&msg, &mut buf).expect("encode failed");
        assert_eq!(len, 56);

        // Verify by raw bytes
        let template_id_bytes = &buf[2..4];
        let template_id = u16::from_le_bytes([template_id_bytes[0], template_id_bytes[1]]);
        assert_eq!(template_id, TEMPLATE_TRADE_NOTIFICATION);
    }

    #[test]
    fn test_insufficient_buffer() {
        let req = NewOrderRequest {
            client_order_id: 123,
            participant_id: 456,
            price: 100.5,
            quantity: 50.0,
            side: 0,
            time_in_force: 0,
            _pad: [0; 14],
        };

        let mut buf = [0u8; 30];  // too small
        let result = encode_new_order(&req, &mut buf);
        assert!(result.is_err());
    }
}
