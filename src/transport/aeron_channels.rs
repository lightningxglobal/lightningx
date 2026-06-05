/// Aeron channel and stream ID constants shared by all binaries.
///
/// Transport selection — AERON_TRANSPORT env var
/// ───────────────────────────────────────────────
///   ipc  (default) — shared memory, same machine, ~1–5μs RTT, stable
///   udp             — UDP sockets, simulates cross-machine production topology
///
/// Production topology (udp mode)
/// ───────────────────────────────
///   Engine machine  : exchange_engine  +  kline_service  +  beacon
///   Desk machine    : desk_server
///
/// For cross-machine deployment set ENGINE_HOST to the engine's IP.
/// On macOS, UDP loopback has unavoidable jitter (scheduler, BSD network stack).
/// For accurate UDP latency measurements, run on Linux with:
///   AERON_SENDER_IDLE_STRATEGY=noop AERON_RECEIVER_IDLE_STRATEGY=noop
///   ethtool -C <nic> rx-usecs 0 tx-usecs 0   (disable interrupt coalescing)

/// Aeron media driver directory.
/// Linux default: /dev/shm/aeron (tmpfs, zero disk I/O).
/// macOS default: /tmp/aeron (no /dev/shm on macOS).
/// Override with AERON_DIR env var.
pub fn aeron_dir() -> String {
    if let Ok(v) = std::env::var("AERON_DIR") {
        return v;
    }
    if cfg!(target_os = "linux") {
        "/dev/shm/aeron".to_string()
    } else {
        "/tmp/aeron".to_string()
    }
}

/// Keep AERON_DIR as a legacy constant for macOS builds that still use /tmp/aeron.
/// New code should call aeron_dir() instead.
pub const AERON_DIR: &str = "/tmp/aeron";

/// Resolve the engine host: ENGINE_HOST env var, defaulting to localhost.
fn engine_host() -> String {
    std::env::var("ENGINE_HOST").unwrap_or_else(|_| "localhost".to_string())
}

/// Build a channel URI for a given local port.
/// ipc mode  → "aeron:ipc"  (ignores port, same channel for all)
/// udp mode  → "aeron:udp?endpoint=<ENGINE_HOST>:<port>"
fn channel(port: u16) -> String {
    match std::env::var("AERON_TRANSPORT").as_deref() {
        Ok("udp") => format!("aeron:udp?endpoint={}:{}", engine_host(), port),
        _ => "aeron:ipc".to_string(), // default: IPC
    }
}

// ── Per-channel accessors ──────────────────────────────────────────────────

/// Desk → Engine: NewOrder / CancelOrder (SBE)
/// Per-symbol streams start at ORDERS_STREAM_BASE (10, 11, 12 …).
/// Stream 1 is kept as a legacy fallback for unknown symbols.
pub fn orders_channel() -> String {
    channel(20121)
}
pub const ORDERS_STREAM: i32 = 1; // legacy / unknown-symbol fallback
pub const ORDERS_STREAM_BASE: i32 = 10; // per-symbol streams: base + sorted_index

/// Deterministic stream ID for a given symbol.
/// Both desk_server (publisher) and exchange_engine (subscriber) call this function
/// so they always agree on which stream carries which symbol's orders.
/// Unknown symbols fall back to ORDERS_STREAM (1) so old single-engine deploys still work.
pub fn orders_stream_for_symbol(symbol: &str) -> i32 {
    if let Ok(symbols) = std::env::var("SYMBOLS") {
        let mut configured: Vec<&str> = symbols
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect();
        configured.sort_unstable();
        configured.dedup();
        if let Some(index) = configured.iter().position(|s| *s == symbol) {
            return ORDERS_STREAM_BASE + index as i32;
        }
    }

    // Sorted alphabetically so the mapping is stable regardless of SYMBOLS env ordering.
    const TABLE: &[(&str, i32)] = &[
        ("BTC_USDT", ORDERS_STREAM_BASE),
        ("ETH_USDT", ORDERS_STREAM_BASE + 1),
        ("SOL_USDT", ORDERS_STREAM_BASE + 2),
    ];
    TABLE
        .iter()
        .find(|(s, _)| *s == symbol)
        .map(|(_, id)| *id)
        .unwrap_or(ORDERS_STREAM)
}

/// Engine → Desk: OrderUpdate (SBE)
pub fn order_update_channel() -> String {
    channel(20122)
}
pub const ORDER_UPDATE_STREAM: i32 = 2; // legacy single-desk stream
pub const ORDER_UPDATE_STREAM_BASE: i32 = 200; // sharded private updates: base + desk_id

/// Private response stream owned by one desk/counter.
///
/// New orders and cancels carry this stream id in their SBE request, so the
/// matching thread publishes private updates only to the desk that owns the
/// client session. This avoids broadcasting every private update to every desk.
pub fn order_update_stream_for_desk(desk_id: u16) -> i32 {
    ORDER_UPDATE_STREAM_BASE + desk_id as i32
}

/// Engine → Desk + kline_service: TradeNotification (SBE)
/// Multiple subscribers share the same Aeron media driver at /tmp/aeron,
/// so both receive every message from a single UDP socket.
pub fn trade_channel() -> String {
    channel(20123)
}
pub const TRADE_STREAM: i32 = 3;

/// Engine → Desk: depth snapshots (10-level / 50-level / 400-level on streams 4/5/6)
/// All three depth tiers share one channel; stream ID distinguishes the tier.
pub fn depth_channel() -> String {
    channel(20124)
}
pub const DEPTH_STREAM: i32 = 4;
pub const DEPTH50_STREAM: i32 = 5;
pub const LEVEL2_STREAM: i32 = 6;

/// Tracing checkpoint publisher → Beacon (GSL metrics)
/// Always IPC: the beacon runs co-located with the publisher.
pub const METRICS_CHANNEL: &str = "aeron:ipc";
pub const METRICS_STREAM: i32 = 1001;

/// Desk → persistence consumers: `PersistEvent` (orders / accounts / trades
/// state changes). Subscribed by redis-writer and (later) pg-writer so
/// neither lives on the hot path. Always IPC — both writers co-locate with
/// desk-server.
pub const PERSIST_CHANNEL: &str = "aeron:ipc";
pub const PERSIST_STREAM: i32 = 30;
