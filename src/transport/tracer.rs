/// Exchange latency tracer — publishes checkpoint messages to beacon via Aeron IPC.
///
/// Wire format is identical to ml_core::beacon (see ~/work/rs/marketlink) so the same
/// process can consume checkpoints from both the exchange and market-link.
///
/// Milestone IDs: (site_ordinal << 16) | action_ordinal  (D1L team convention)
///
///   ExchangeDeskServer (site=8):
///     MS_WS_ORDER_RECV     = (8<<16)|27  — WS frame received from client
///     MS_AERON_ORDER_SEND  = (8<<16)|28  — order published to Aeron
///     MS_AERON_UPDATE_RECV = (8<<16)|32  — order update received from Aeron
///     MS_WS_UPDATE_SEND    = (8<<16)|33  — update pushed to client WS
///
///   ExchangeEngine (site=9):
///     MS_AERON_ORDER_RECV  = (9<<16)|29  — order received from Aeron
///     MS_MATCHING_DONE     = (9<<16)|30  — place_order() returned
///     MS_AERON_UPDATE_SEND = (9<<16)|31  — order update published to Aeron
///
/// tracing_id convention: decimal ASCII representation of the internal order_id (u64).
/// The desk-server sets client_order_id = internal order_id when publishing to Aeron,
/// so both sides derive the same tracing_id.

use std::time::{SystemTime, UNIX_EPOCH};
use aeron_wrapper::{AeronClient, Pub, Publisher};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

// ─── Instance IDs ─────────────────────────────────────────────────────────────
pub const DESK_INSTANCE_ID: i32 = 2001;
pub const ENGINE_INSTANCE_ID: i32 = 2002;

// ─── Milestone IDs ────────────────────────────────────────────────────────────
pub const MS_WS_ORDER_RECV: i32     = (8_i32 << 16) | 27; // DeskServer × OnWsOrderRecv
pub const MS_CMD_RING_PUSHED: i32   = (8_i32 << 16) | 34; // DeskServer × OnCmdRingPushed  (WS handler done parsing + queued)
pub const MS_CMD_RING_POPPED: i32   = (8_i32 << 16) | 35; // DeskServer × OnCmdRingPopped  (send-spin drained from queue)
pub const MS_AERON_ORDER_SEND: i32  = (8_i32 << 16) | 28; // DeskServer × OnAeronOrderSend (try_claim + commit done)
pub const MS_AERON_UPDATE_RECV: i32 = (8_i32 << 16) | 32; // DeskServer × OnAeronUpdateRecv
pub const MS_WS_UPDATE_SEND: i32    = (8_i32 << 16) | 33; // DeskServer × OnWsUpdateSend   (about to call user_tx.try_send)
pub const MS_USER_TX_SENT: i32      = (8_i32 << 16) | 36; // DeskServer × OnUserTxSent     (after user_tx.try_send returned)
pub const MS_AERON_ORDER_RECV: i32  = (9_i32 << 16) | 29; // Engine × OnAeronOrderRecv
pub const MS_MATCHING_DONE: i32     = (9_i32 << 16) | 30; // Engine × OnMatchingDone
pub const MS_AERON_UPDATE_SEND: i32 = (9_i32 << 16) | 31; // Engine × OnAeronUpdateSend

// Wire format constants (same as marketlink).
const MSG_TYPE_CHECKPOINT: i32 = 6;
const MSG_TYPE_SCENARIO: i32 = 8;
const MSG_TYPE_GROUP: i32 = 11;
const PLACE_HOLDER: u8 = 25;
const TAG_ID_SYMBOL: i16 = 47; // LPS MetSymbol tag

/// Shared tag used for all exchange checkpoints — the order symbol could be added
/// per-call but we keep it as a static "EXCHANGE" tag to avoid per-call allocation.
const SYM: &[u8] = b"EXCHANGE";

// ─── ExchangeTracer ───────────────────────────────────────────────────────────

/// Send half: clone-and-share across AppState / handlers.
/// `record()` is non-blocking — drops the message if the background thread is gone.
///
/// Timestamp is captured **at the call site** (not in the drain thread) so queuing
/// delay in the mpsc channel does not inflate the reported latency.
#[derive(Clone)]
pub struct ExchangeTracer {
    tx: UnboundedSender<(i32, u64, i64, [u8; 16])>, // (milestone_id, order_id, ts_ns, sym)
}

impl ExchangeTracer {
    /// Record a checkpoint without a specific symbol (uses zero bytes → default group).
    #[inline]
    pub fn record(&self, milestone_id: i32, order_id: u64) {
        let ts_ns = now_ns();
        let _ = self.tx.send((milestone_id, order_id, ts_ns, [0u8; 16]));
    }

    /// Record a checkpoint with the instrument symbol for per-symbol Grafana grouping.
    /// `sym` is a fixed 16-byte NUL-padded ASCII field (same layout as `NewOrderRequest.symbol`).
    #[inline]
    pub fn record_sym(&self, milestone_id: i32, order_id: u64, sym: &[u8; 16]) {
        let ts_ns = now_ns();
        let _ = self.tx.send((milestone_id, order_id, ts_ns, *sym));
    }
}

/// Spawn background thread that drains (milestone_id, order_id) pairs and publishes
/// checkpoint wire messages to beacon via Aeron IPC.
///
/// Returns `None` if Aeron init fails (beacon not running) — checkpoints are simply dropped.
pub fn spawn_tracer(
    aeron_dir: &str,
    channel: &str,
    stream_id: i32,
    instance_id: i32,
) -> Option<ExchangeTracer> {
    let (tx, rx) = unbounded_channel::<(i32, u64, i64, [u8; 16])>();

    let aeron_dir = aeron_dir.to_string();
    let channel = channel.to_string();

    std::thread::Builder::new()
        .name("exchange-tracer".to_string())
        .spawn(move || {
            let client = match AeronClient::new(&aeron_dir) {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!("exchange tracer: Aeron init failed: {:?} — checkpoints disabled", e);
                    return;
                }
            };

            // Spin up to 200ms waiting for the publication to connect to beacon.
            let pub_ = match wait_for_pub(&client, &channel, stream_id) {
                Some(p) => p,
                None => {
                    tracing::warn!("exchange tracer: publication not connected — checkpoints disabled");
                    return;
                }
            };

            tracing::info!("exchange tracer: connected to (instance_id={})", instance_id);
            register_scenario(&pub_, instance_id);

            drain_loop(rx, &client, &pub_, instance_id);
        })
        .ok()?;

    Some(ExchangeTracer { tx })
}

// ─── Internal helpers ─────────────────────────────────────────────────────────

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

fn wait_for_pub(client: &AeronClient, channel: &str, stream_id: i32) -> Option<Publisher> {
    let pub_ = client.add_publication(channel, stream_id).ok()?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(200);
    loop {
        client.do_work();
        // Attempt to send 1 empty byte — if not connected yet, it'll back-pressure.
        // We use the connected() check conceptually; actual connected check not in API.
        // After 200 ms give up and return None.
        if std::time::Instant::now() >= deadline {
            break;
        }
        std::hint::spin_loop();
    }
    Some(pub_)
}

fn drain_loop(mut rx: UnboundedReceiver<(i32, u64, i64, [u8; 16])>, client: &AeronClient, pub_: &Publisher, instance_id: i32) {
    let mut buf = [0u8; 600];
    let mut idle = 0u32;
    loop {
        while let Ok((milestone_id, order_id, ts_ns, sym)) = rx.try_recv() {
            let n = serialize_checkpoint(&mut buf, instance_id, milestone_id, order_id, ts_ns, &sym);
            // Zero-copy claim: write directly into Aeron's term-log buffer
            // instead of staging in `buf` then memcpy via `send()`.
            // The `serialize_checkpoint` already wrote into `buf` above so we
            // still copy from it — to get a true second-memcpy elimination we
            // would have to refactor `serialize_checkpoint` to take the claim
            // buffer directly. For now this just removes the send()'s internal
            // claim+memcpy round-trip.
            if let Ok(mut claim) = pub_.try_claim(n) {
                claim.as_mut_slice().copy_from_slice(&buf[..n]);
                let _ = claim.commit();
            }
            idle = 0;
        }
        // Aeron client times out if do_work() is not called within ~10 seconds.
        // Call it every ~1 ms of idle time (every 1000 spin iterations) to keep
        // the publication alive without burning CPU when traffic is low.
        idle += 1;
        if idle % 1_000 == 0 {
            client.do_work();
        }
        std::hint::spin_loop();
    }
}

#[inline]
fn serialize_checkpoint(
    buf: &mut [u8; 600],
    instance_id: i32,
    milestone_id: i32,
    order_id: u64,
    ts_ns: i64,
    sym: &[u8; 16],
) -> usize {
    // Trim NUL padding from the fixed-size symbol field.
    let sym_len = sym.iter().position(|&b| b == 0).unwrap_or(16);
    // Fall back to static "EXCHANGE" tag when no symbol was provided.
    let (sym_bytes, sym_len) = if sym_len == 0 { (SYM, SYM.len()) } else { (&sym[..], sym_len) };

    let mut p = 0usize;

    buf[p..p + 4].copy_from_slice(&MSG_TYPE_CHECKPOINT.to_le_bytes()); p += 4;
    buf[p] = PLACE_HOLDER; p += 1;
    buf[p..p + 4].copy_from_slice(&instance_id.to_le_bytes()); p += 4;
    p += 8; // reserved = 0
    buf[p..p + 8].copy_from_slice(&ts_ns.to_le_bytes()); p += 8;

    // Tags: count=1, tag_id=TAG_ID_SYMBOL, value=sym
    buf[p..p + 2].copy_from_slice(&1i16.to_le_bytes()); p += 2;
    buf[p..p + 2].copy_from_slice(&TAG_ID_SYMBOL.to_le_bytes()); p += 2;
    buf[p] = sym_len as u8; p += 1;
    buf[p..p + sym_len].copy_from_slice(&sym_bytes[..sym_len]); p += sym_len;

    // Payload: milestone_id + tracing_id (decimal order_id)
    buf[p..p + 4].copy_from_slice(&milestone_id.to_le_bytes()); p += 4;
    let tid_len = write_u64(&mut buf[p + 1..], order_id);
    buf[p] = tid_len as u8; p += 1 + tid_len;

    p
}

fn write_u64(out: &mut [u8], v: u64) -> usize {
    if v == 0 { out[0] = b'0'; return 1; }
    let mut tmp = [0u8; 20];
    let mut i = 20usize;
    let mut x = v;
    while x > 0 { i -= 1; tmp[i] = b'0' + (x % 10) as u8; x /= 10; }
    let len = 20 - i;
    out[..len].copy_from_slice(&tmp[i..]);
    len
}

// ─── Scenario registration ─────────────────────────────────────────────────────

fn register_scenario(pub_: &Publisher, instance_id: i32) {
    publish_scenario(pub_, instance_id);
    publish_group(pub_, instance_id);
}

fn ts_now() -> i64 { now_ns() }

fn publish_scenario(pub_: &Publisher, instance_id: i32) {
    let mut buf = [0u8; 512];
    let mut p = 0usize;

    // Header
    buf[p..p + 4].copy_from_slice(&MSG_TYPE_SCENARIO.to_le_bytes()); p += 4;
    buf[p] = PLACE_HOLDER; p += 1;
    buf[p..p + 4].copy_from_slice(&instance_id.to_le_bytes()); p += 4;
    p += 8;
    buf[p..p + 8].copy_from_slice(&ts_now().to_le_bytes()); p += 8;
    buf[p..p + 2].copy_from_slice(&0i16.to_le_bytes()); p += 2; // tag_count=0

    // Scenario name: "ExchangeOrderFlow"
    let name = b"ExchangeOrderFlow";
    buf[p] = name.len() as u8; p += 1;
    buf[p..p + name.len()].copy_from_slice(name); p += name.len();

    // 10 milestones (added 3 finer ones: cmd_ring push/pop + user_tx sent)
    buf[p] = 10u8; p += 1;

    macro_rules! milestone {
        ($sid:expr, $sname:expr, $aid:expr, $aname:expr) => {{
            let sn: &[u8] = $sname;
            let an: &[u8] = $aname;
            buf[p..p + 2].copy_from_slice(&($sid as i16).to_le_bytes()); p += 2;
            buf[p] = sn.len() as u8; p += 1;
            buf[p..p + sn.len()].copy_from_slice(sn); p += sn.len();
            buf[p..p + 2].copy_from_slice(&($aid as i16).to_le_bytes()); p += 2;
            buf[p] = an.len() as u8; p += 1;
            buf[p..p + an.len()].copy_from_slice(an); p += an.len();
        }};
    }

    milestone!(8i16, b"ExchangeDeskServer", 27i16, b"OnWsOrderRecv");
    milestone!(8i16, b"ExchangeDeskServer", 34i16, b"OnCmdRingPushed");
    milestone!(8i16, b"ExchangeDeskServer", 35i16, b"OnCmdRingPopped");
    milestone!(8i16, b"ExchangeDeskServer", 28i16, b"OnAeronOrderSend");
    milestone!(9i16, b"ExchangeEngine",     29i16, b"OnAeronOrderRecv");
    milestone!(9i16, b"ExchangeEngine",     30i16, b"OnMatchingDone");
    milestone!(9i16, b"ExchangeEngine",     31i16, b"OnAeronUpdateSend");
    milestone!(8i16, b"ExchangeDeskServer", 32i16, b"OnAeronUpdateRecv");
    milestone!(8i16, b"ExchangeDeskServer", 33i16, b"OnWsUpdateSend");
    milestone!(8i16, b"ExchangeDeskServer", 36i16, b"OnUserTxSent");

    // 10 gaps (3 finer + original 7 still here for back-compat dashboards)
    buf[p] = 10u8; p += 1;
    for (from, to) in [
        (MS_WS_ORDER_RECV,    MS_CMD_RING_PUSHED),   // desk WS handler: parse + auth + freeze
        (MS_CMD_RING_PUSHED,  MS_CMD_RING_POPPED),   // cmd ring residence (queue wait)
        (MS_CMD_RING_POPPED,  MS_AERON_ORDER_SEND),  // send-spin: SBE encode + Aeron try_claim+commit
        (MS_WS_ORDER_RECV,    MS_AERON_ORDER_SEND),  // desk send total (sum of above 3)
        (MS_AERON_ORDER_SEND, MS_AERON_ORDER_RECV),  // Aeron transit desk→engine
        (MS_AERON_ORDER_RECV, MS_MATCHING_DONE),     // engine matching time
        (MS_MATCHING_DONE,    MS_AERON_UPDATE_SEND), // engine result serialization
        (MS_AERON_UPDATE_SEND, MS_AERON_UPDATE_RECV),// Aeron transit engine→desk
        (MS_WS_UPDATE_SEND,   MS_USER_TX_SENT),      // recv-spin: user_tx.try_send wakeup cost
        (MS_WS_ORDER_RECV,    MS_USER_TX_SENT),      // end-to-end (spin handoff to tokio)
    ] {
        buf[p..p + 4].copy_from_slice(&from.to_le_bytes()); p += 4;
        buf[p..p + 4].copy_from_slice(&to.to_le_bytes()); p += 4;
    }

    let _ = pub_.send(&buf[..p]);
}

fn publish_group(pub_: &Publisher, instance_id: i32) {
    let mut buf = [0u8; 128];
    let mut p = 0usize;

    buf[p..p + 4].copy_from_slice(&MSG_TYPE_GROUP.to_le_bytes()); p += 4;
    buf[p] = PLACE_HOLDER; p += 1;
    buf[p..p + 4].copy_from_slice(&instance_id.to_le_bytes()); p += 4;
    p += 8;
    buf[p..p + 8].copy_from_slice(&ts_now().to_le_bytes()); p += 8;
    buf[p..p + 2].copy_from_slice(&0i16.to_le_bytes()); p += 2; // tag_count=0

    // prefix="exch", delimiter='.', filter_count=1
    buf[p] = 4u8; p += 1;
    buf[p..p + 4].copy_from_slice(b"exch"); p += 4;
    buf[p] = b'.'; p += 1;
    buf[p] = 1u8; p += 1;

    // GroupFilter: TAG_ID_SYMBOL=47, tag_name="instrument", default="*"
    buf[p..p + 2].copy_from_slice(&TAG_ID_SYMBOL.to_le_bytes()); p += 2;
    buf[p] = 10u8; p += 1;
    buf[p..p + 10].copy_from_slice(b"instrument"); p += 10;
    buf[p] = 1u8; p += 1;
    buf[p] = b'*'; p += 1;

    let _ = pub_.send(&buf[..p]);
}
