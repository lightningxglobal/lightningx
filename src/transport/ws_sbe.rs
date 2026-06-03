/// WebSocket SBE (binary) encoding for server→client messages.
///
/// All frames: `[msg_type: u8][payload bytes...]`
/// All multi-byte integers/floats are little-endian.

// ── Unicast message type constants ────────────────────────────────────────────
pub const AUTH_OK: u8 = 1;
pub const AUTH_ERROR: u8 = 2;
pub const ORDER_ACCEPTED: u8 = 3;
pub const ORDER_REJECTED: u8 = 4;
pub const ORDER_SUBMITTED: u8 = 5;
pub const ORDER_UPDATE: u8 = 6;
pub const CANCEL_SUBMITTED: u8 = 7;
pub const BALANCE_UPDATE: u8 = 8;
pub const POSITION_UPDATE: u8 = 9;
pub const CANCEL_ALL_OK: u8 = 10;
pub const BATCH_CANCEL_OK: u8 = 11;
pub const PONG_MSG: u8 = 12;
pub const ERROR_MSG: u8 = 13;
pub const ORDERS_PLACED: u8 = 14;
pub const ORDER_DEPTH_RESP: u8 = 15;

// ── Broadcast message type constants ─────────────────────────────────────────
pub const TRADE_MSG: u8 = 20;
pub const DEPTH_MSG: u8 = 21;
pub const TICKER_MSG: u8 = 22;
pub const KLINE_MSG: u8 = 23;
pub const AGG_TRADE_MSG: u8 = 24;

// ── Status byte constants for ORDER_UPDATE ───────────────────────────────────
pub const WS_STATUS_OPEN: u8 = 0;
pub const WS_STATUS_PARTIAL_FILL: u8 = 1;
pub const WS_STATUS_FILLED: u8 = 2;
pub const WS_STATUS_CANCELED: u8 = 3;
pub const WS_STATUS_REJECTED: u8 = 4;

// ── Helper ───────────────────────────────────────────────────────────────────

/// Copy up to 8 ASCII bytes from `s` into a null-padded fixed array.
#[inline]
fn pack8(s: &str) -> [u8; 8] {
    let mut buf = [0u8; 8];
    let b = s.as_bytes();
    let n = b.len().min(8);
    buf[..n].copy_from_slice(&b[..n]);
    buf
}

// ── Encode functions ──────────────────────────────────────────────────────────

/// AUTH_OK = 1  [user_id: i64]  → 9 B
#[inline]
pub fn encode_auth_ok(user_id: i64) -> Vec<u8> {
    let mut v = Vec::with_capacity(9);
    v.push(AUTH_OK);
    v.extend_from_slice(&user_id.to_le_bytes());
    v
}

/// AUTH_ERROR = 2  [len: u8][reason_utf8 × len]  → 2+len
#[inline]
pub fn encode_auth_error(reason: &str) -> Vec<u8> {
    let b = reason.as_bytes();
    let len = b.len().min(255) as u8;
    let mut v = Vec::with_capacity(2 + len as usize);
    v.push(AUTH_ERROR);
    v.push(len);
    v.extend_from_slice(&b[..len as usize]);
    v
}

/// ORDER_ACCEPTED = 3
/// [order_id:u64][client_order_id:u64][price:f64][qty:f64][side:u8][_pad:7][symbol:u8×8][ts:u64]
/// → 57 B
#[inline]
pub fn encode_order_accepted(
    order_id: u64,
    client_order_id: u64,
    price: f64,
    qty: f64,
    side: u8,
    symbol: &str,
    ts: u64,
) -> Vec<u8> {
    let mut v = Vec::with_capacity(57);
    v.push(ORDER_ACCEPTED);
    v.extend_from_slice(&order_id.to_le_bytes());
    v.extend_from_slice(&client_order_id.to_le_bytes());
    v.extend_from_slice(&price.to_le_bytes());
    v.extend_from_slice(&qty.to_le_bytes());
    v.push(side);
    v.extend_from_slice(&[0u8; 7]); // _pad
    v.extend_from_slice(&pack8(symbol));
    v.extend_from_slice(&ts.to_le_bytes());
    v
}

/// ORDER_REJECTED = 4  [client_order_id:u64][len:u8][reason_utf8 × len]  → 10+len
#[inline]
pub fn encode_order_rejected(client_order_id: u64, reason: &str) -> Vec<u8> {
    let b = reason.as_bytes();
    let len = b.len().min(255) as u8;
    let mut v = Vec::with_capacity(10 + len as usize);
    v.push(ORDER_REJECTED);
    v.extend_from_slice(&client_order_id.to_le_bytes());
    v.push(len);
    v.extend_from_slice(&b[..len as usize]);
    v
}

/// ORDER_SUBMITTED = 5  [order_id:u64][client_order_id:u64][ts:u64]  → 25 B
#[inline]
pub fn encode_order_submitted(order_id: u64, client_order_id: u64, ts: u64) -> Vec<u8> {
    let mut v = Vec::with_capacity(25);
    v.push(ORDER_SUBMITTED);
    v.extend_from_slice(&order_id.to_le_bytes());
    v.extend_from_slice(&client_order_id.to_le_bytes());
    v.extend_from_slice(&ts.to_le_bytes());
    v
}

/// ORDER_UPDATE = 6
/// [order_id:u64][client_order_id:u64][status:u8][_pad:3][filled_qty:f64][avg_price:f64][ts:u64]
/// → 45 B
#[inline]
pub fn encode_order_update(
    order_id: u64,
    client_order_id: u64,
    status: u8,
    filled_qty: f64,
    avg_price: f64,
    ts: u64,
) -> Vec<u8> {
    let mut v = Vec::with_capacity(45);
    v.push(ORDER_UPDATE);
    v.extend_from_slice(&order_id.to_le_bytes());
    v.extend_from_slice(&client_order_id.to_le_bytes());
    v.push(status);
    v.extend_from_slice(&[0u8; 3]); // _pad
    v.extend_from_slice(&filled_qty.to_le_bytes());
    v.extend_from_slice(&avg_price.to_le_bytes());
    v.extend_from_slice(&ts.to_le_bytes());
    v
}

/// CANCEL_SUBMITTED = 7  [order_id:u64][ts:u64]  → 17 B
#[inline]
pub fn encode_cancel_submitted(order_id: u64, ts: u64) -> Vec<u8> {
    let mut v = Vec::with_capacity(17);
    v.push(CANCEL_SUBMITTED);
    v.extend_from_slice(&order_id.to_le_bytes());
    v.extend_from_slice(&ts.to_le_bytes());
    v
}

/// BALANCE_UPDATE = 8  [asset:u8×8][balance:f64][available:f64][frozen:f64]  → 33 B
#[inline]
pub fn encode_balance_update(
    asset: &str,
    balance: f64,
    available: f64,
    frozen: f64,
) -> Vec<u8> {
    let mut v = Vec::with_capacity(33);
    v.push(BALANCE_UPDATE);
    v.extend_from_slice(&pack8(asset));
    v.extend_from_slice(&balance.to_le_bytes());
    v.extend_from_slice(&available.to_le_bytes());
    v.extend_from_slice(&frozen.to_le_bytes());
    v
}

/// POSITION_UPDATE = 9  [symbol:u8×8][qty:f64][avg_price:f64]  → 25 B
#[inline]
pub fn encode_position_update(symbol: &str, qty: f64, avg_price: f64) -> Vec<u8> {
    let mut v = Vec::with_capacity(25);
    v.push(POSITION_UPDATE);
    v.extend_from_slice(&pack8(symbol));
    v.extend_from_slice(&qty.to_le_bytes());
    v.extend_from_slice(&avg_price.to_le_bytes());
    v
}

/// CANCEL_ALL_OK = 10  [cancelled:u32]  → 5 B
#[inline]
pub fn encode_cancel_all_ok(cancelled: u32) -> Vec<u8> {
    let mut v = Vec::with_capacity(5);
    v.push(CANCEL_ALL_OK);
    v.extend_from_slice(&cancelled.to_le_bytes());
    v
}

/// BATCH_CANCEL_OK = 11  [cancelled:u32]  → 5 B
#[inline]
pub fn encode_batch_cancel_ok(cancelled: u32) -> Vec<u8> {
    let mut v = Vec::with_capacity(5);
    v.push(BATCH_CANCEL_OK);
    v.extend_from_slice(&cancelled.to_le_bytes());
    v
}

/// PONG_MSG = 12  (empty payload)  → 1 B
#[inline]
pub fn encode_pong() -> Vec<u8> {
    vec![PONG_MSG]
}

/// ERROR_MSG = 13  [len:u8][message_utf8 × len]  → 2+len
#[inline]
pub fn encode_error(message: &str) -> Vec<u8> {
    let b = message.as_bytes();
    let len = b.len().min(255) as u8;
    let mut v = Vec::with_capacity(2 + len as usize);
    v.push(ERROR_MSG);
    v.push(len);
    v.extend_from_slice(&b[..len as usize]);
    v
}

/// ORDERS_PLACED = 14
/// [batch_id:u8×16][count:u16][_pad:6]
/// [[client_order_id:u64][order_id:u64][status:u8][_pad:7]] × count
///
/// `results` is (client_order_id, order_id, status)
#[inline]
pub fn encode_orders_placed(batch_id: &str, results: &[(u64, u64, u8)]) -> Vec<u8> {
    let count = results.len().min(u16::MAX as usize) as u16;
    let header = 1 + 16 + 2 + 6; // type + batch_id + count + pad
    let per_entry = 8 + 8 + 1 + 7; // coid + oid + status + pad
    let cap = header + per_entry * count as usize;
    let mut v = Vec::with_capacity(cap);
    v.push(ORDERS_PLACED);
    // batch_id: 16 bytes null-padded
    let mut bid = [0u8; 16];
    let bb = batch_id.as_bytes();
    let n = bb.len().min(16);
    bid[..n].copy_from_slice(&bb[..n]);
    v.extend_from_slice(&bid);
    v.extend_from_slice(&count.to_le_bytes());
    v.extend_from_slice(&[0u8; 6]); // _pad
    for &(coid, oid, status) in &results[..count as usize] {
        v.extend_from_slice(&coid.to_le_bytes());
        v.extend_from_slice(&oid.to_le_bytes());
        v.push(status);
        v.extend_from_slice(&[0u8; 7]); // _pad
    }
    v
}

/// ORDER_DEPTH_RESP = 15
/// [ts:u64][symbol:u8×8][num_bids:u16][num_asks:u16][_pad:4]
/// [[price:f64][qty:f64]]×(nb+na)
#[inline]
pub fn encode_order_depth_resp(
    ts: u64,
    symbol: &str,
    bids: &[(f64, f64)],
    asks: &[(f64, f64)],
) -> Vec<u8> {
    let nb = bids.len().min(u16::MAX as usize) as u16;
    let na = asks.len().min(u16::MAX as usize) as u16;
    let header = 1 + 8 + 8 + 2 + 2 + 4; // type + ts + symbol + nb + na + pad
    let cap = header + 16 * (nb + na) as usize;
    let mut v = Vec::with_capacity(cap);
    v.push(ORDER_DEPTH_RESP);
    v.extend_from_slice(&ts.to_le_bytes());
    v.extend_from_slice(&pack8(symbol));
    v.extend_from_slice(&nb.to_le_bytes());
    v.extend_from_slice(&na.to_le_bytes());
    v.extend_from_slice(&[0u8; 4]); // _pad
    for &(p, q) in &bids[..nb as usize] {
        v.extend_from_slice(&p.to_le_bytes());
        v.extend_from_slice(&q.to_le_bytes());
    }
    for &(p, q) in &asks[..na as usize] {
        v.extend_from_slice(&p.to_le_bytes());
        v.extend_from_slice(&q.to_le_bytes());
    }
    v
}

/// TRADE_MSG = 20
/// [price:f64][qty:f64][side:u8][_pad:7][ts:u64][symbol:u8×8]  → 41 B
#[inline]
pub fn encode_trade(price: f64, qty: f64, side: u8, ts: u64, symbol: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(41);
    v.push(TRADE_MSG);
    v.extend_from_slice(&price.to_le_bytes());
    v.extend_from_slice(&qty.to_le_bytes());
    v.push(side);
    v.extend_from_slice(&[0u8; 7]); // _pad
    v.extend_from_slice(&ts.to_le_bytes());
    v.extend_from_slice(&pack8(symbol));
    v
}

/// DEPTH_MSG = 21
/// [ts:u64][symbol:u8×8][num_bids:u16][num_asks:u16][_pad:4]
/// [[price:f64][qty:f64]]×(nb+na)
#[inline]
pub fn encode_depth(
    ts: u64,
    symbol: &str,
    bids: &[(f64, f64)],
    asks: &[(f64, f64)],
) -> Vec<u8> {
    let nb = bids.len().min(u16::MAX as usize) as u16;
    let na = asks.len().min(u16::MAX as usize) as u16;
    let header = 1 + 8 + 8 + 2 + 2 + 4;
    let cap = header + 16 * (nb + na) as usize;
    let mut v = Vec::with_capacity(cap);
    v.push(DEPTH_MSG);
    v.extend_from_slice(&ts.to_le_bytes());
    v.extend_from_slice(&pack8(symbol));
    v.extend_from_slice(&nb.to_le_bytes());
    v.extend_from_slice(&na.to_le_bytes());
    v.extend_from_slice(&[0u8; 4]); // _pad
    for &(p, q) in &bids[..nb as usize] {
        v.extend_from_slice(&p.to_le_bytes());
        v.extend_from_slice(&q.to_le_bytes());
    }
    for &(p, q) in &asks[..na as usize] {
        v.extend_from_slice(&p.to_le_bytes());
        v.extend_from_slice(&q.to_le_bytes());
    }
    v
}

/// TICKER_MSG = 22
/// [symbol:u8×8][last:f64][change:f64][high:f64][low:f64][volume:f64]  → 49 B
#[inline]
pub fn encode_ticker(
    symbol: &str,
    last: f64,
    change: f64,
    high: f64,
    low: f64,
    volume: f64,
) -> Vec<u8> {
    let mut v = Vec::with_capacity(49);
    v.push(TICKER_MSG);
    v.extend_from_slice(&pack8(symbol));
    v.extend_from_slice(&last.to_le_bytes());
    v.extend_from_slice(&change.to_le_bytes());
    v.extend_from_slice(&high.to_le_bytes());
    v.extend_from_slice(&low.to_le_bytes());
    v.extend_from_slice(&volume.to_le_bytes());
    v
}

/// KLINE_MSG = 23
/// [symbol:u8×8][interval:u8][_pad:7][time:u64][open:f64][high:f64][low:f64][close:f64][volume:f64]
/// → 73 B
#[inline]
pub fn encode_kline(
    symbol: &str,
    interval: u8,
    time: u64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
) -> Vec<u8> {
    let mut v = Vec::with_capacity(73);
    v.push(KLINE_MSG);
    v.extend_from_slice(&pack8(symbol));
    v.push(interval);
    v.extend_from_slice(&[0u8; 7]); // _pad
    v.extend_from_slice(&time.to_le_bytes());
    v.extend_from_slice(&open.to_le_bytes());
    v.extend_from_slice(&high.to_le_bytes());
    v.extend_from_slice(&low.to_le_bytes());
    v.extend_from_slice(&close.to_le_bytes());
    v.extend_from_slice(&volume.to_le_bytes());
    v
}

/// AGG_TRADE_MSG = 24
/// [symbol:u8×8][interval:u8][_pad:7][time:u64][open:f64][high:f64][low:f64][close:f64]
/// [volume:f64][trade_count:u32][_pad2:4]  → 77 B
#[inline]
pub fn encode_agg_trade(
    symbol: &str,
    interval: u8,
    time: u64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    trade_count: u32,
) -> Vec<u8> {
    let mut v = Vec::with_capacity(77);
    v.push(AGG_TRADE_MSG);
    v.extend_from_slice(&pack8(symbol));
    v.push(interval);
    v.extend_from_slice(&[0u8; 7]); // _pad
    v.extend_from_slice(&time.to_le_bytes());
    v.extend_from_slice(&open.to_le_bytes());
    v.extend_from_slice(&high.to_le_bytes());
    v.extend_from_slice(&low.to_le_bytes());
    v.extend_from_slice(&close.to_le_bytes());
    v.extend_from_slice(&volume.to_le_bytes());
    v.extend_from_slice(&trade_count.to_le_bytes());
    v.extend_from_slice(&[0u8; 4]); // _pad2
    v
}

// ── Decode functions (for pressure_client) ────────────────────────────────────

/// Returns the message type byte, or None if the buffer is empty.
#[inline]
pub fn decode_msg_type(buf: &[u8]) -> Option<u8> {
    buf.first().copied()
}

/// Decode ORDER_UPDATE = 6
/// Returns (order_id, client_order_id, status, filled_qty, avg_price, ts)
pub fn decode_order_update(buf: &[u8]) -> Option<(u64, u64, u8, f64, f64, u64)> {
    // 1 + 8 + 8 + 1 + 3 + 8 + 8 + 8 = 45
    if buf.len() < 45 || buf[0] != ORDER_UPDATE {
        return None;
    }
    let order_id = u64::from_le_bytes(buf[1..9].try_into().ok()?);
    let client_order_id = u64::from_le_bytes(buf[9..17].try_into().ok()?);
    let status = buf[17];
    // skip 3 pad bytes at [18..21]
    let filled_qty = f64::from_le_bytes(buf[21..29].try_into().ok()?);
    let avg_price = f64::from_le_bytes(buf[29..37].try_into().ok()?);
    let ts = u64::from_le_bytes(buf[37..45].try_into().ok()?);
    Some((order_id, client_order_id, status, filled_qty, avg_price, ts))
}

/// Decode ORDER_ACCEPTED = 3
/// Returns (order_id, client_order_id)
pub fn decode_order_accepted(buf: &[u8]) -> Option<(u64, u64)> {
    // 1 + 8 + 8 + ... = at least 17
    if buf.len() < 17 || buf[0] != ORDER_ACCEPTED {
        return None;
    }
    let order_id = u64::from_le_bytes(buf[1..9].try_into().ok()?);
    let client_order_id = u64::from_le_bytes(buf[9..17].try_into().ok()?);
    Some((order_id, client_order_id))
}

/// Decode ORDER_REJECTED = 4
/// Returns (client_order_id, reason)
pub fn decode_order_rejected(buf: &[u8]) -> Option<(u64, String)> {
    // 1 + 8 + 1 = at least 10
    if buf.len() < 10 || buf[0] != ORDER_REJECTED {
        return None;
    }
    let client_order_id = u64::from_le_bytes(buf[1..9].try_into().ok()?);
    let len = buf[9] as usize;
    if buf.len() < 10 + len {
        return None;
    }
    let reason = String::from_utf8_lossy(&buf[10..10 + len]).into_owned();
    Some((client_order_id, reason))
}

/// Decode ORDER_SUBMITTED = 5
/// Returns (order_id, client_order_id, ts)
pub fn decode_order_submitted(buf: &[u8]) -> Option<(u64, u64, u64)> {
    // 1 + 8 + 8 + 8 = 25
    if buf.len() < 25 || buf[0] != ORDER_SUBMITTED {
        return None;
    }
    let order_id = u64::from_le_bytes(buf[1..9].try_into().ok()?);
    let client_order_id = u64::from_le_bytes(buf[9..17].try_into().ok()?);
    let ts = u64::from_le_bytes(buf[17..25].try_into().ok()?);
    Some((order_id, client_order_id, ts))
}

/// Extract symbol bytes from a DEPTH_MSG frame (bytes [9..17]).
pub fn decode_symbol_from_depth(buf: &[u8]) -> Option<&[u8]> {
    if buf.len() >= 17 && buf[0] == DEPTH_MSG {
        Some(&buf[9..17])
    } else {
        None
    }
}

/// Extract symbol bytes from a TRADE_MSG frame (bytes [33..41]).
pub fn decode_symbol_from_trade(buf: &[u8]) -> Option<&[u8]> {
    // TRADE_MSG: [type:1][price:8][qty:8][side:1][_pad:7][ts:8][symbol:8]
    if buf.len() >= 41 && buf[0] == TRADE_MSG {
        Some(&buf[33..41])
    } else {
        None
    }
}

/// Extract symbol bytes from a TICKER_MSG frame (bytes [1..9]).
pub fn decode_symbol_from_ticker(buf: &[u8]) -> Option<&[u8]> {
    if buf.len() >= 9 && buf[0] == TICKER_MSG {
        Some(&buf[1..9])
    } else {
        None
    }
}

/// Extract symbol bytes from a KLINE_MSG frame (bytes [1..9]).
pub fn decode_symbol_from_kline(buf: &[u8]) -> Option<&[u8]> {
    if buf.len() >= 9 && buf[0] == KLINE_MSG {
        Some(&buf[1..9])
    } else {
        None
    }
}

/// Extract symbol bytes from an AGG_TRADE_MSG frame (bytes [1..9]).
pub fn decode_symbol_from_agg_trade(buf: &[u8]) -> Option<&[u8]> {
    if buf.len() >= 9 && buf[0] == AGG_TRADE_MSG {
        Some(&buf[1..9])
    } else {
        None
    }
}

/// Map a WsOrderStatus string to its SBE status byte.
#[inline]
pub fn ws_status_byte_from_str(s: &str) -> u8 {
    match s {
        "OPEN" => WS_STATUS_OPEN,
        "PARTIAL" | "PARTIAL_FILL" => WS_STATUS_PARTIAL_FILL,
        "FILLED" => WS_STATUS_FILLED,
        "CANCELED" => WS_STATUS_CANCELED,
        _ => WS_STATUS_REJECTED,
    }
}

/// Convert a null-padded 8-byte symbol field to a &str.
pub fn sym8_to_str(sym: &[u8]) -> &str {
    let end = sym.iter().position(|&b| b == 0).unwrap_or(sym.len());
    std::str::from_utf8(&sym[..end]).unwrap_or("")
}

// ── REST decode helpers ───────────────────────────────────────────────────────

/// Decode a DEPTH_MSG frame into a `serde_json::Value` for REST responses.
/// Returns `{"symbol":..., "bids":[[p,q],...], "asks":[[p,q],...], "ts":...}`.
pub fn decode_depth_for_rest(buf: &[u8]) -> Option<serde_json::Value> {
    // header: type(1) + ts(8) + symbol(8) + nb(2) + na(2) + pad(4) = 25
    if buf.len() < 25 || buf[0] != DEPTH_MSG {
        return None;
    }
    let ts = u64::from_le_bytes(buf[1..9].try_into().ok()?);
    let symbol = sym8_to_str(&buf[9..17]).to_owned();
    let nb = u16::from_le_bytes(buf[17..19].try_into().ok()?) as usize;
    let na = u16::from_le_bytes(buf[19..21].try_into().ok()?) as usize;
    let expected = 25 + (nb + na) * 16;
    if buf.len() < expected {
        return None;
    }
    let mut off = 25usize;
    let mut bids = Vec::with_capacity(nb);
    for _ in 0..nb {
        let p = f64::from_le_bytes(buf[off..off + 8].try_into().ok()?);
        let q = f64::from_le_bytes(buf[off + 8..off + 16].try_into().ok()?);
        bids.push(serde_json::json!([p, q]));
        off += 16;
    }
    let mut asks = Vec::with_capacity(na);
    for _ in 0..na {
        let p = f64::from_le_bytes(buf[off..off + 8].try_into().ok()?);
        let q = f64::from_le_bytes(buf[off + 8..off + 16].try_into().ok()?);
        asks.push(serde_json::json!([p, q]));
        off += 16;
    }
    Some(serde_json::json!({ "symbol": symbol, "bids": bids, "asks": asks, "ts": ts }))
}

/// Decode a TICKER_MSG frame into a `serde_json::Value` for REST responses.
/// Returns `{"symbol":..., "last":..., "change":..., "high":..., "low":..., "volume":...}`.
pub fn decode_ticker_for_rest(buf: &[u8]) -> Option<serde_json::Value> {
    // type(1) + symbol(8) + last(8) + change(8) + high(8) + low(8) + volume(8) = 49
    if buf.len() < 49 || buf[0] != TICKER_MSG {
        return None;
    }
    let symbol = sym8_to_str(&buf[1..9]).to_owned();
    let last   = f64::from_le_bytes(buf[9..17].try_into().ok()?);
    let change = f64::from_le_bytes(buf[17..25].try_into().ok()?);
    let high   = f64::from_le_bytes(buf[25..33].try_into().ok()?);
    let low    = f64::from_le_bytes(buf[33..41].try_into().ok()?);
    let volume = f64::from_le_bytes(buf[41..49].try_into().ok()?);
    Some(serde_json::json!({ "symbol": symbol, "last": last, "change": change,
                             "high": high, "low": low, "volume": volume }))
}

/// Return the best bid or best ask price from a DEPTH_MSG frame.
/// `want_ask = true` → first ask price; `false` → first bid price.
pub fn decode_best_price(buf: &[u8], want_ask: bool) -> Option<f64> {
    if buf.len() < 25 || buf[0] != DEPTH_MSG {
        return None;
    }
    let nb = u16::from_le_bytes(buf[17..19].try_into().ok()?) as usize;
    let na = u16::from_le_bytes(buf[19..21].try_into().ok()?) as usize;
    if want_ask {
        // asks start after the bid levels
        let off = 25 + nb * 16;
        if na == 0 || buf.len() < off + 8 { return None; }
        Some(f64::from_le_bytes(buf[off..off + 8].try_into().ok()?))
    } else {
        // first bid is right after the header
        if nb == 0 || buf.len() < 33 { return None; }
        Some(f64::from_le_bytes(buf[25..33].try_into().ok()?))
    }
}

/// Extract the symbol string from any broadcast message frame (TRADE/DEPTH/TICKER/KLINE/AGG_TRADE).
/// Returns None if the frame is too short or not a recognized broadcast type.
pub fn decode_broadcast_symbol(buf: &[u8]) -> Option<String> {
    let sym_bytes = match buf.first().copied()? {
        DEPTH_MSG => decode_symbol_from_depth(buf)?,
        TRADE_MSG => decode_symbol_from_trade(buf)?,
        TICKER_MSG => decode_symbol_from_ticker(buf)?,
        KLINE_MSG => decode_symbol_from_kline(buf)?,
        AGG_TRADE_MSG => decode_symbol_from_agg_trade(buf)?,
        _ => return None,
    };
    Some(sym8_to_str(sym_bytes).to_owned())
}
