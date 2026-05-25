/// Aeron channel and stream ID constants shared by all binaries.
///
/// Production topology
/// ───────────────────
///   Engine machine  : exchange_engine  +  kline_service  +  beacon sidecar
///   Desk machine    : desk_server
///
/// Channels that cross the engine↔desk boundary use UDP so the two machines
/// can be physically separated (security isolation, independent scaling).
/// The Metrics channel stays IPC because the beacon sidecar always runs
/// co-located with the engine/desk on the same host.
///
/// For cross-machine deployment replace "localhost" with the actual engine IP
/// (e.g. ENGINE_IP=10.0.1.10) in the endpoint strings below.

pub const AERON_DIR: &str = "/tmp/aeron";

/// Desk → Engine: NewOrder / CancelOrder (SBE)
/// Per-symbol streams start at ORDERS_STREAM_BASE (10, 11, 12 …).
/// Stream 1 is kept as a legacy fallback for unknown symbols.
pub const ORDERS_CHANNEL: &str = "aeron:udp?endpoint=localhost:20121";
pub const ORDERS_STREAM: i32 = 1;          // legacy / unknown-symbol fallback
pub const ORDERS_STREAM_BASE: i32 = 10;    // per-symbol streams: base + sorted_index

/// Deterministic stream ID for a given symbol.
/// Both desk_server (publisher) and exchange_engine (subscriber) call this function
/// so they always agree on which stream carries which symbol's orders.
/// Unknown symbols fall back to ORDERS_STREAM (1) so old single-engine deploys still work.
pub fn orders_stream_for_symbol(symbol: &str) -> i32 {
    // Sorted alphabetically so the mapping is stable regardless of SYMBOLS env ordering.
    const TABLE: &[(&str, i32)] = &[
        ("BTC_USDT", ORDERS_STREAM_BASE),
        ("ETH_USDT", ORDERS_STREAM_BASE + 1),
        ("SOL_USDT", ORDERS_STREAM_BASE + 2),
    ];
    TABLE.iter().find(|(s, _)| *s == symbol).map(|(_, id)| *id).unwrap_or(ORDERS_STREAM)
}

/// Engine → Desk: OrderUpdate (SBE)
pub const ORDER_UPDATE_CHANNEL: &str = "aeron:udp?endpoint=localhost:20122";
pub const ORDER_UPDATE_STREAM: i32 = 2;

/// Engine → Desk + kline_service: TradeNotification (SBE)
/// Multiple subscribers (desk_server and kline_service) share the same Aeron media
/// driver at /tmp/aeron, so both receive every message from a single UDP socket.
pub const TRADE_CHANNEL: &str = "aeron:udp?endpoint=localhost:20123";
pub const TRADE_STREAM: i32 = 3;

/// Engine → Desk: depth snapshots (10-level / 50-level / 400-level on streams 4/5/6)
/// All three depth tiers share one UDP endpoint; stream ID distinguishes the tier.
pub const DEPTH_CHANNEL: &str = "aeron:udp?endpoint=localhost:20124";
pub const DEPTH_STREAM: i32 = 4;

pub const DEPTH50_CHANNEL: &str = "aeron:udp?endpoint=localhost:20125"; // unused in constructors — kept for documentation
pub const DEPTH50_STREAM: i32 = 5;

pub const LEVEL2_CHANNEL: &str = "aeron:udp?endpoint=localhost:20126"; // unused in constructors — kept for documentation
pub const LEVEL2_STREAM: i32 = 6;

/// Tracing checkpoint publisher → Beacon sidecar (GSL metrics)
/// Stays IPC: the beacon sidecar always runs on the same host as the publisher.
pub const METRICS_CHANNEL: &str = "aeron:ipc";
pub const METRICS_STREAM: i32 = 1001;
