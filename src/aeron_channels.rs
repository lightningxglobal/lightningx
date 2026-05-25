/// Aeron channel and stream ID constants shared by all binaries.
///
/// Transport: IPC (shared memory) — all components run on the same machine during development.
/// IPC eliminates UDP protocol overhead (~200-500μs savings on loopback).
/// Channel URI is always "aeron:ipc"; stream IDs distinguish each logical channel.

pub const AERON_DIR: &str = "/tmp/aeron";

/// Desk → Engine: NewOrder / CancelOrder (SBE)
pub const ORDERS_CHANNEL: &str = "aeron:ipc";
pub const ORDERS_STREAM: i32 = 1;

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
