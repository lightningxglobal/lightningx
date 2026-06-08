//! Engine snapshot serialization + PG persistence — T4.
//!
//! Wire format (little-endian, self-describing) — deliberately explicit
//! rather than dumping the `Order` memory layout (which has private
//! padding): a snapshot must survive struct-layout churn.
//!
//!   [u8;4] magic "ESN1"
//!   u64    next_order_id
//!   u64    trade_sequence
//!   u32    order_count
//!   order_count × {
//!     u64 id, u8 side(0=buy,1=sell), u8 tif(0..3), i64 price_ticks,
//!     i64 quantity_lots, u64 timestamp
//!   }

use crate::matching::engine::EngineSnapshot;
use crate::matching::order::{Order, Side, TimeInForce};

const MAGIC: &[u8; 4] = b"ESN1";

pub fn serialize(snap: &EngineSnapshot) -> Vec<u8> {
    let mut out = Vec::with_capacity(24 + snap.orders.len() * 34);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&snap.next_order_id.to_le_bytes());
    out.extend_from_slice(&snap.trade_sequence.to_le_bytes());
    out.extend_from_slice(&(snap.orders.len() as u32).to_le_bytes());
    for o in &snap.orders {
        out.extend_from_slice(&o.id.to_le_bytes());
        out.push(match o.side {
            Side::Buy => 0,
            Side::Sell => 1,
        });
        out.push(match o.time_in_force {
            TimeInForce::GTC => 0,
            TimeInForce::IOC => 1,
            TimeInForce::FOK => 2,
            TimeInForce::PostOnly => 3,
        });
        out.extend_from_slice(&o.price_ticks.to_le_bytes());
        out.extend_from_slice(&o.quantity_lots.to_le_bytes());
        out.extend_from_slice(&o.timestamp.to_le_bytes());
    }
    out
}

pub fn deserialize(buf: &[u8]) -> Option<EngineSnapshot> {
    if buf.len() < 24 || &buf[0..4] != MAGIC {
        return None;
    }
    let next_order_id = u64::from_le_bytes(buf[4..12].try_into().ok()?);
    let trade_sequence = u64::from_le_bytes(buf[12..20].try_into().ok()?);
    let count = u32::from_le_bytes(buf[20..24].try_into().ok()?) as usize;
    let mut orders = Vec::with_capacity(count);
    let mut p = 24;
    for _ in 0..count {
        if p + 34 > buf.len() {
            return None;
        }
        let id = u64::from_le_bytes(buf[p..p + 8].try_into().ok()?);
        let side = match buf[p + 8] {
            0 => Side::Buy,
            1 => Side::Sell,
            _ => return None,
        };
        let tif = match buf[p + 9] {
            0 => TimeInForce::GTC,
            1 => TimeInForce::IOC,
            2 => TimeInForce::FOK,
            3 => TimeInForce::PostOnly,
            _ => return None,
        };
        let price_ticks = i64::from_le_bytes(buf[p + 10..p + 18].try_into().ok()?);
        let quantity_lots = i64::from_le_bytes(buf[p + 18..p + 26].try_into().ok()?);
        let timestamp = u64::from_le_bytes(buf[p + 26..p + 34].try_into().ok()?);
        orders.push(Order::new(id, side, price_ticks, quantity_lots, tif, timestamp));
        p += 34;
    }
    Some(EngineSnapshot {
        orders,
        next_order_id,
        trade_sequence,
    })
}

/// Persist a snapshot for `symbol` at journal `position`, then prune all
/// but the latest few rows. Returns the new snapshot_seq.
pub async fn save(
    pool: &sqlx::PgPool,
    symbol: &str,
    snap: &EngineSnapshot,
    journal_recording_id: i64,
    journal_position: i64,
) -> anyhow::Result<i64> {
    let payload = serialize(snap);
    let seq: i64 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(snapshot_seq), 0) + 1 FROM engine_snapshots WHERE symbol = $1",
    )
    .bind(symbol)
    .fetch_one(pool)
    .await?;
    sqlx::query(
        "INSERT INTO engine_snapshots
            (symbol, snapshot_seq, journal_recording_id, journal_position,
             order_count, payload)
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(symbol)
    .bind(seq)
    .bind(journal_recording_id)
    .bind(journal_position)
    .bind(snap.orders.len() as i32)
    .bind(&payload)
    .execute(pool)
    .await?;
    sqlx::query(
        "DELETE FROM engine_snapshots WHERE symbol = $1 AND snapshot_seq <= $2 - 3",
    )
    .bind(symbol)
    .bind(seq)
    .execute(pool)
    .await?;
    Ok(seq)
}

/// Load the latest snapshot for `symbol`: (snapshot, journal_position).
/// (snapshot, journal_recording_id, journal_position) of the latest.
pub async fn load_latest(
    pool: &sqlx::PgPool,
    symbol: &str,
) -> anyhow::Result<Option<(EngineSnapshot, i64, i64)>> {
    let row: Option<(Vec<u8>, i64, i64)> = sqlx::query_as(
        "SELECT payload, journal_recording_id, journal_position
           FROM engine_snapshots
          WHERE symbol = $1 ORDER BY snapshot_seq DESC LIMIT 1",
    )
    .bind(symbol)
    .fetch_optional(pool)
    .await?;
    Ok(row.and_then(|(payload, rec, pos)| deserialize(&payload).map(|s| (s, rec, pos))))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_serialization() {
        let snap = EngineSnapshot {
            orders: vec![
                Order::new(1, Side::Buy, 49_000, 10, TimeInForce::GTC, 100),
                Order::new(2, Side::Sell, 50_000, 5, TimeInForce::GTC, 200),
                Order::new(3, Side::Buy, 48_500, 7, TimeInForce::PostOnly, 300),
            ],
            next_order_id: 42,
            trade_sequence: 99,
        };
        let bytes = serialize(&snap);
        let back = deserialize(&bytes).expect("round trips");
        assert_eq!(back.next_order_id, 42);
        assert_eq!(back.trade_sequence, 99);
        assert_eq!(back.orders.len(), 3);
        for (a, b) in snap.orders.iter().zip(&back.orders) {
            assert_eq!(a.id, b.id);
            assert_eq!(a.side, b.side);
            assert_eq!(a.price_ticks, b.price_ticks);
            assert_eq!(a.quantity_lots, b.quantity_lots);
            assert_eq!(a.timestamp, b.timestamp);
            assert_eq!(a.time_in_force, b.time_in_force);
        }
    }

    #[test]
    fn rejects_garbage() {
        assert!(deserialize(b"").is_none());
        assert!(deserialize(b"XXXX____________________").is_none());
        let mut b = serialize(&EngineSnapshot::default());
        b[20] = 0xE8;
        b[21] = 0x03;
        assert!(deserialize(&b).is_none());
    }
}
