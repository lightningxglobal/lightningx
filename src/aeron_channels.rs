/// Aeron channel and stream ID constants shared by all binaries.
///
/// Transport: IPC (shared memory) — all components run on the same machine during development.
/// IPC eliminates UDP protocol overhead (~200-500μs savings on loopback).
/// Channel URI is always "aeron:ipc"; stream IDs distinguish each logical channel.

pub const AERON_DIR: &str = "/tmp/aeron";

/// Desk → Engine: NewOrder / CancelOrder (SBE)
/// Per-symbol streams start at ORDERS_STREAM_BASE (10, 11, 12 …).
/// Stream 1 is kept as a legacy fallback for unknown symbols.
pub const ORDERS_CHANNEL: &str = "aeron:ipc";
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
pub const ORDER_UPDATE_CHANNEL: &str = "aeron:ipc";
pub const ORDER_UPDATE_STREAM: i32 = 2;

/// Engine → Desk + kline_service: TradeNotification (SBE)
pub const TRADE_CHANNEL: &str = "aeron:ipc";
pub const TRADE_STREAM: i32 = 3;

/// Engine → Desk: DepthSnapshot (SBE binary, 10-level)
pub const DEPTH_CHANNEL: &str = "aeron:ipc";
pub const DEPTH_STREAM: i32 = 4;

/// Engine → Desk: Depth50Snapshot (SBE binary, 50-level)
pub const DEPTH50_CHANNEL: &str = "aeron:ipc";
pub const DEPTH50_STREAM: i32 = 5;

/// Engine → Desk: Level2Snapshot (SBE binary, 400-level)
pub const LEVEL2_CHANNEL: &str = "aeron:ipc";
pub const LEVEL2_STREAM: i32 = 6;

/// Tracing checkpoint publisher → Beacon sidecar (GSL metrics)
pub const METRICS_CHANNEL: &str = "aeron:ipc";
pub const METRICS_STREAM: i32 = 1001;
