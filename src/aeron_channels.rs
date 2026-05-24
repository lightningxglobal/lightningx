/// Aeron channel and stream ID constants shared by all binaries.
///
/// Transport: UDP with conductor-invoker mode (no aeronmd required).
/// Each process embeds its own conductor; they communicate over localhost UDP.

pub const AERON_DIR: &str = "/tmp/aeron";

/// Desk → Engine: NewOrder / CancelOrder (SBE)
pub const ORDERS_CHANNEL: &str = "aeron:udp?endpoint=localhost:20121";
pub const ORDERS_STREAM: i32 = 1;

/// Engine → Desk: OrderUpdate (SBE)
pub const ORDER_UPDATE_CHANNEL: &str = "aeron:udp?endpoint=localhost:20122";
pub const ORDER_UPDATE_STREAM: i32 = 2;

/// Engine → Desk + kline_service: TradeNotification (SBE)
pub const TRADE_CHANNEL: &str = "aeron:udp?endpoint=localhost:20123";
pub const TRADE_STREAM: i32 = 3;

/// Engine → Desk: DepthSnapshot (SBE binary, 10-level)
pub const DEPTH_CHANNEL: &str = "aeron:udp?endpoint=localhost:20124";
pub const DEPTH_STREAM: i32 = 4;

/// Engine → Desk: Depth50Snapshot (SBE binary, 50-level)
pub const DEPTH50_CHANNEL: &str = "aeron:udp?endpoint=localhost:20124";
pub const DEPTH50_STREAM: i32 = 5;

/// Engine → Desk: Level2Snapshot (SBE binary, 400-level)
pub const LEVEL2_CHANNEL: &str = "aeron:udp?endpoint=localhost:20124";
pub const LEVEL2_STREAM: i32 = 6;
