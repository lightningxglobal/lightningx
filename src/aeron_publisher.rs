//! Aeron Publisher for distributing market data snapshots and trade events
//!
//! This module provides:
//! - SnapshotPublisher: Publishes PublishedSnapshot messages on dedicated stream
//! - TradePublisher: Publishes TradeEvent messages on dedicated stream
//! - Both handle back-pressure and reconnection errors gracefully
//! - Allows WebSocket servers and other consumers to subscribe independently

use aeron_wrapper::{AeronClient, Pub, Publisher, Error as AeronError};
use crate::market_data::{PublishedSnapshot, TradeEvent};

/// Aeron configuration for snapshot distribution
pub struct AeronConfig {
    /// Path to Aeron media driver shared memory
    pub aeron_dir: String,
    /// Channel string for snapshot distribution (e.g., "aeron:ipc" for IPC, "aeron:udp" for network)
    pub channel: String,
    /// Stream ID for snapshots (must be unique within the channel)
    pub stream_id: i32,
}

impl Default for AeronConfig {
    fn default() -> Self {
        Self {
            aeron_dir: "/dev/shm/aeron".to_string(),
            channel: "aeron:ipc".to_string(),
            stream_id: 10,  // Default stream ID for snapshots
        }
    }
}

/// Result type for publisher operations
pub type PublisherResult<T> = Result<T, PublisherError>;

/// Errors that can occur during snapshot publishing
#[derive(Debug, Clone)]
pub enum PublisherError {
    /// Failed to connect to Aeron media driver
    ConnectionFailed(String),
    /// Publisher not connected to subscribers
    NotConnected,
    /// Ring buffer is full, snapshot was dropped
    BackPressured,
    /// Failed to serialize snapshot
    SerializationFailed(String),
    /// Publisher or client was closed
    Closed,
    /// Unrecoverable error
    Fatal(String),
}

impl std::fmt::Display for PublisherError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PublisherError::ConnectionFailed(msg) => write!(f, "connection failed: {}", msg),
            PublisherError::NotConnected => write!(f, "publisher not connected"),
            PublisherError::BackPressured => write!(f, "back pressured"),
            PublisherError::SerializationFailed(msg) => write!(f, "serialization failed: {}", msg),
            PublisherError::Closed => write!(f, "closed"),
            PublisherError::Fatal(msg) => write!(f, "fatal: {}", msg),
        }
    }
}

impl std::error::Error for PublisherError {}

/// Converts Aeron errors to PublisherError
fn map_aeron_error(error: AeronError) -> PublisherError {
    match error {
        AeronError::NotConnected => PublisherError::NotConnected,
        AeronError::BackPressured => PublisherError::BackPressured,
        AeronError::Closed => PublisherError::Closed,
        AeronError::Registration(msg) => PublisherError::ConnectionFailed(msg),
        AeronError::Fatal(msg) => PublisherError::Fatal(msg),
        AeronError::MaxPositionExceeded => PublisherError::Fatal("max position exceeded".to_string()),
    }
}

/// Publishes market data snapshots via Aeron
///
/// SnapshotPublisher handles:
/// - Connection to Aeron media driver
/// - Serialization of PublishedSnapshot structs
/// - Back-pressure handling (drop snapshots rather than block)
/// - Lifecycle management
///
/// # Thread Safety
///
/// Publisher is Send but not Sync; intended for single-threaded use.
/// The snapshot publishing thread owns a single Publisher instance.
///
/// # Example
///
/// ```ignore
/// let config = AeronConfig::default();
/// let publisher = SnapshotPublisher::new(config)?;
///
/// let snapshot = PublishedSnapshot::default();
/// publisher.publish(&snapshot)?;
/// ```
pub struct SnapshotPublisher {
    /// Aeron client connection (kept alive for publisher)
    _client: AeronClient,
    /// Publisher handle
    publisher: Publisher,
}

impl SnapshotPublisher {
    /// Create a new snapshot publisher
    ///
    /// # Arguments
    ///
    /// * `config` - Aeron configuration (dir, channel, stream_id)
    ///
    /// # Returns
    ///
    /// Ok(SnapshotPublisher) if connection succeeds, Err otherwise.
    /// If the Aeron media driver is not running, this will return ConnectionFailed.
    pub fn new(config: AeronConfig) -> PublisherResult<Self> {
        // Connect to Aeron media driver
        let client = AeronClient::new(&config.aeron_dir)
            .map_err(|e| PublisherError::ConnectionFailed(format!("{}", e)))?;

        // Add publication on the configured channel and stream
        let publisher = client
            .add_publication(&config.channel, config.stream_id)
            .map_err(map_aeron_error)?;

        Ok(Self {
            _client: client,
            publisher,
        })
    }

    /// Publish a snapshot to Aeron subscribers
    ///
    /// # Arguments
    ///
    /// * `snapshot` - The PublishedSnapshot to send
    ///
    /// # Behavior
    ///
    /// - Serializes the snapshot to bytes (binary format)
    /// - Sends to Aeron publisher
    /// - Does NOT retry on back-pressure; the snapshot is dropped if buffer is full
    ///   (better to lose a snapshot than block the generating thread)
    /// - Returns NotConnected if no subscribers are connected
    ///
    /// # Return Value
    ///
    /// - Ok(()) on successful publish
    /// - Err(BackPressured) if ring buffer is full (snapshot is lost)
    /// - Err(NotConnected) if no subscribers are connected
    /// - Err(Closed) if publisher was closed
    /// - Err(Fatal) on unrecoverable error
    #[inline]
    pub fn publish(&self, snapshot: &PublishedSnapshot) -> PublisherResult<()> {
        // Convert snapshot to bytes for transmission
        let bytes = snapshot_to_bytes(snapshot)?;

        // Send to Aeron
        self.publisher.send(&bytes).map_err(map_aeron_error)
    }

    /// Claim a buffer region for zero-copy writing
    ///
    /// This is an advanced API for custom serialization. Most users should use publish().
    #[inline]
    pub fn try_claim(&self, length: usize) -> PublisherResult<aeron_wrapper::BufferClaim> {
        self.publisher.try_claim(length).map_err(map_aeron_error)
    }

    /// Check if the publisher is connected to at least one subscriber
    #[inline]
    pub fn is_connected(&self) -> bool {
        self.publisher.is_connected()
    }

    /// Check if the publisher has been closed
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.publisher.is_closed()
    }

    /// Get the stream ID this publisher is using
    #[inline]
    pub fn stream_id(&self) -> i32 {
        self.publisher.stream_id()
    }

    /// Maximum bytes that fit in a single message without fragmentation
    #[inline]
    pub fn max_payload_length(&self) -> usize {
        self.publisher.max_payload_length()
    }

    /// Maximum message length including fragmented delivery
    #[inline]
    pub fn max_message_length(&self) -> usize {
        self.publisher.max_message_length()
    }
}

/// Serialize a PublishedSnapshot to bytes using direct memory copy
///
/// PublishedSnapshot has stable layout with align(64), so we can safely
/// transmute it to bytes for binary transmission. This is very fast (~0 overhead).
///
/// Consumers must deserialize using the same binary format (Rust code only).
/// For cross-language support, use Option B (manual serialization) in Phase 4.
#[inline]
pub fn snapshot_to_bytes(snapshot: &PublishedSnapshot) -> PublisherResult<Vec<u8>> {
    let bytes = unsafe {
        std::slice::from_raw_parts(
            snapshot as *const _ as *const u8,
            std::mem::size_of::<PublishedSnapshot>(),
        )
    };
    Ok(bytes.to_vec())
}

/// Deserialize bytes back to PublishedSnapshot
///
/// This is the consumer-side deserialization. Used in WebSocket servers
/// and other subscribers that receive snapshots from Aeron.
///
/// # Safety
///
/// This performs a binary copy from bytes. The bytes MUST have been produced
/// by snapshot_to_bytes() and layout must match. Only safe when sender and
/// receiver are both Rust (same compiler version / target).
///
/// The bytes must be properly aligned (64-byte boundary). Aeron guarantees
/// this for messages from the publisher. For testing, use copy_from_slice instead.
#[inline]
pub fn bytes_to_snapshot(bytes: &[u8]) -> PublisherResult<PublishedSnapshot> {
    let expected_size = std::mem::size_of::<PublishedSnapshot>();
    if bytes.len() != expected_size {
        return Err(PublisherError::SerializationFailed(format!(
            "expected {} bytes, got {}",
            expected_size,
            bytes.len()
        )));
    }

    // Check alignment: PublishedSnapshot requires 64-byte alignment
    let ptr = bytes.as_ptr() as usize;
    if ptr % 64 != 0 {
        // If not properly aligned, copy to aligned buffer
        let mut snapshot = PublishedSnapshot::default();
        let snapshot_bytes = unsafe {
            std::slice::from_raw_parts_mut(
                &mut snapshot as *mut _ as *mut u8,
                expected_size,
            )
        };
        snapshot_bytes.copy_from_slice(bytes);
        return Ok(snapshot);
    }

    // Properly aligned, can dereference directly
    let snapshot = unsafe {
        *(bytes.as_ptr() as *const PublishedSnapshot)
    };

    Ok(snapshot)
}

/// Publishes trade events via Aeron
///
/// TradePublisher handles:
/// - Connection to Aeron media driver
/// - Serialization of TradeEvent structs
/// - Back-pressure handling (drop trades rather than block)
/// - Lifecycle management
///
/// # Thread Safety
///
/// Publisher is Send but not Sync; intended for single-threaded use.
/// The trade publishing thread owns a single Publisher instance.
///
/// # Example
///
/// ```ignore
/// let config = AeronConfig {
///     aeron_dir: "/dev/shm/aeron".to_string(),
///     channel: "aeron:ipc".to_string(),
///     stream_id: 11,
/// };
/// let publisher = TradePublisher::new(config)?;
///
/// let trade = TradeEvent::new(...);
/// publisher.publish(&trade)?;
/// ```
pub struct TradePublisher {
    /// Aeron client connection (kept alive for publisher)
    _client: AeronClient,
    /// Publisher handle
    publisher: Publisher,
}

impl TradePublisher {
    /// Create a new trade publisher
    ///
    /// # Arguments
    ///
    /// * `config` - Aeron configuration (dir, channel, stream_id)
    ///
    /// # Returns
    ///
    /// Ok(TradePublisher) if connection succeeds, Err otherwise.
    /// If the Aeron media driver is not running, this will return ConnectionFailed.
    pub fn new(config: AeronConfig) -> PublisherResult<Self> {
        // Connect to Aeron media driver
        let client = AeronClient::new(&config.aeron_dir)
            .map_err(|e| PublisherError::ConnectionFailed(format!("{}", e)))?;

        // Add publication on the configured channel and stream
        let publisher = client
            .add_publication(&config.channel, config.stream_id)
            .map_err(map_aeron_error)?;

        Ok(Self {
            _client: client,
            publisher,
        })
    }

    /// Publish a trade event to Aeron subscribers
    ///
    /// # Arguments
    ///
    /// * `trade` - The TradeEvent to send
    ///
    /// # Behavior
    ///
    /// - Serializes the trade to bytes (binary format)
    /// - Sends to Aeron publisher
    /// - Does NOT retry on back-pressure; the trade is dropped if buffer is full
    ///   (better to lose a single trade than block the generating thread)
    /// - Returns NotConnected if no subscribers are connected
    ///
    /// # Return Value
    ///
    /// - Ok(()) on successful publish
    /// - Err(BackPressured) if ring buffer is full (trade is lost)
    /// - Err(NotConnected) if no subscribers are connected
    /// - Err(Closed) if publisher was closed
    /// - Err(Fatal) on unrecoverable error
    #[inline]
    pub fn publish(&self, trade: &TradeEvent) -> PublisherResult<()> {
        // Convert trade to bytes for transmission
        let bytes = trade_to_bytes(trade)?;

        // Send to Aeron
        self.publisher.send(&bytes).map_err(map_aeron_error)
    }

    /// Check if the publisher is connected to at least one subscriber
    #[inline]
    pub fn is_connected(&self) -> bool {
        self.publisher.is_connected()
    }

    /// Check if the publisher has been closed
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.publisher.is_closed()
    }

    /// Get the stream ID this publisher is using
    #[inline]
    pub fn stream_id(&self) -> i32 {
        self.publisher.stream_id()
    }

    /// Maximum bytes that fit in a single message without fragmentation
    #[inline]
    pub fn max_payload_length(&self) -> usize {
        self.publisher.max_payload_length()
    }
}

/// Serialize a TradeEvent to bytes using direct memory copy
///
/// TradeEvent has stable layout with align(64), so we can safely
/// transmute it to bytes for binary transmission. This is very fast (~0 overhead).
///
/// Consumers must deserialize using the same binary format (Rust code only).
/// For cross-language support, use manual serialization in Phase 4.
#[inline]
pub fn trade_to_bytes(trade: &TradeEvent) -> PublisherResult<Vec<u8>> {
    let bytes = unsafe {
        std::slice::from_raw_parts(
            trade as *const _ as *const u8,
            std::mem::size_of::<TradeEvent>(),
        )
    };
    Ok(bytes.to_vec())
}

/// Deserialize bytes back to TradeEvent
///
/// This is the consumer-side deserialization. Used in WebSocket servers
/// and other subscribers that receive trades from Aeron.
///
/// # Safety
///
/// This performs a binary copy from bytes. The bytes MUST have been produced
/// by trade_to_bytes() and layout must match. Only safe when sender and
/// receiver are both Rust (same compiler version / target).
///
/// The bytes must be properly aligned (64-byte boundary). Aeron guarantees
/// this for messages from the publisher. For testing, use copy_from_slice instead.
#[inline]
pub fn bytes_to_trade(bytes: &[u8]) -> PublisherResult<TradeEvent> {
    let expected_size = std::mem::size_of::<TradeEvent>();
    if bytes.len() != expected_size {
        return Err(PublisherError::SerializationFailed(format!(
            "expected {} bytes, got {}",
            expected_size,
            bytes.len()
        )));
    }

    // Check alignment: TradeEvent requires 64-byte alignment
    let ptr = bytes.as_ptr() as usize;
    if ptr % 64 != 0 {
        // If not properly aligned, copy to aligned buffer
        let mut trade = TradeEvent::default();
        let trade_bytes = unsafe {
            std::slice::from_raw_parts_mut(
                &mut trade as *mut _ as *mut u8,
                expected_size,
            )
        };
        trade_bytes.copy_from_slice(bytes);
        return Ok(trade);
    }

    // Properly aligned, can dereference directly
    let trade = unsafe {
        *(bytes.as_ptr() as *const TradeEvent)
    };

    Ok(trade)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_serialization_roundtrip() {
        let snapshot = PublishedSnapshot::default();
        let bytes = snapshot_to_bytes(&snapshot).unwrap();

        assert_eq!(bytes.len(), std::mem::size_of::<PublishedSnapshot>());

        let deserialized = bytes_to_snapshot(&bytes).unwrap();
        assert_eq!(deserialized.timestamp, snapshot.timestamp);
        assert_eq!(deserialized.sequence, snapshot.sequence);
    }

    #[test]
    fn test_snapshot_bytes_wrong_size() {
        let bytes = vec![0u8; 100];
        let result = bytes_to_snapshot(&bytes);

        match result {
            Err(PublisherError::SerializationFailed(_)) => {
                // Expected
            }
            _ => panic!("Expected SerializationFailed"),
        }
    }

    #[test]
    fn test_aeron_config_default() {
        let config = AeronConfig::default();
        assert_eq!(config.aeron_dir, "/dev/shm/aeron");
        assert_eq!(config.channel, "aeron:ipc");
        assert_eq!(config.stream_id, 10);
    }

    #[test]
    fn test_trade_serialization_roundtrip() {
        use crate::order::Side;

        let trade = TradeEvent::new(
            1,           // sequence
            100,         // order_id
            99,          // maker_order_id
            1_000_000_000, // timestamp
            50000.0,     // price
            10.0,        // quantity
            Side::Buy,   // side
            1001,        // taker_id
            2001,        // maker_id
        );

        let bytes = trade_to_bytes(&trade).unwrap();
        assert_eq!(bytes.len(), std::mem::size_of::<TradeEvent>());

        let deserialized = bytes_to_trade(&bytes).unwrap();
        assert_eq!(deserialized.sequence, trade.sequence);
        assert_eq!(deserialized.order_id, trade.order_id);
        assert_eq!(deserialized.maker_order_id, trade.maker_order_id);
        assert_eq!(deserialized.timestamp, trade.timestamp);
        assert_eq!(deserialized.price, trade.price);
        assert_eq!(deserialized.quantity, trade.quantity);
        assert_eq!(deserialized.taker_id, trade.taker_id);
        assert_eq!(deserialized.maker_id, trade.maker_id);
        assert_eq!(deserialized.side, trade.side);
    }

    #[test]
    fn test_trade_bytes_wrong_size() {
        let bytes = vec![0u8; 100];
        let result = bytes_to_trade(&bytes);

        match result {
            Err(PublisherError::SerializationFailed(_)) => {
                // Expected
            }
            _ => panic!("Expected SerializationFailed"),
        }
    }
}
