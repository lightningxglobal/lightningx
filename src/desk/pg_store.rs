//! Postgres apply path for `PersistFrame`s — used by the `pg-writer` binary
//! to drain the Aeron persist stream into PG asynchronously, off the
//! desk-server hot path.
//!
//! Design notes:
//! - Idempotent: every write uses INSERT…ON CONFLICT or value-based UPDATEs
//!   so re-applying the same frame is a no-op. Safe to run alongside the
//!   existing desk-server DB worker (dual-write window).
//! - Batched: frames accumulate in `PgWriteBatch` and are flushed grouped
//!   by kind (one SQL per kind, multi-row VALUES) — the same shape the
//!   desk-server DB worker uses.
//! - No fabricated data: missing/incoherent payloads (e.g. empty asset,
//!   negative timestamp) are skipped with a warn; we never insert
//!   sentinel rows just to keep counters happy.
//!
//! Wire-format conventions inherited from `redis_store`:
//! - `OrderUpsertPayload.price == 0.0` → `orders.price = NULL` (market order).
//! - `client_order_id` empty string → NULL.
//! - `status` byte → `'PENDING' | 'TRADING' | 'COMPLETED' | 'CANCELED' | 'REJECTED'`.

use crate::desk::money::AmountAtoms;
use crate::transport::persist_event::{
    AccountSetPayload, MatchingEventPayload, OrderDeletePayload, OrderFillUpdatePayload,
    OrderUpsertPayload, PersistFrame, PersistKind, TradeInsertPayload, unpack_str,
};
use chrono::{DateTime, TimeZone, Utc};
use sqlx::{PgConnection, PgPool};

// Status encoding matches `DbOrderStatus` (0-indexed) which is what
// desk-server actually publishes on the wire — see
// crate::desk::order_state::DbOrderStatus. Earlier drafts of this module
// used a 1-indexed mapping which silently coerced everything to PENDING.
const STATUS_NAMES: [&str; 5] = ["PENDING", "TRADING", "COMPLETED", "CANCELED", "REJECTED"];

fn status_str(code: u8) -> Option<&'static str> {
    if (code as usize) < STATUS_NAMES.len() {
        Some(STATUS_NAMES[code as usize])
    } else {
        None
    }
}

fn side_str(code: u8) -> &'static str {
    if code == 0 { "buy" } else { "sell" }
}

fn from_unix_ms(ms: i64) -> Option<DateTime<Utc>> {
    Utc.timestamp_millis_opt(ms).single()
}

/// Boundary f64 → atoms. None on non-finite/out-of-range input; callers
/// skip the frame and bump `amount_atoms_invalid`.
fn to_atoms(value: f64) -> Option<i64> {
    AmountAtoms::from_f64_round(value).ok().map(|a| a.atoms())
}

/// Decoded upsert row, ready for batch SQL.
struct UpsertRow {
    id: i64,
    user_id: i64,
    symbol: String,
    side: &'static str,
    order_type: String,
    price: Option<f64>,
    quantity: f64,
    filled: f64,
    /// Fixed-point twins, derived once at row construction (the persist
    /// payload still carries f64 — carrying ticks/lots natively is part of
    /// the P2 wire upgrade). Authoritative columns in PG.
    price_atoms: Option<i64>,
    quantity_atoms: i64,
    filled_atoms: i64,
    status: &'static str,
    freeze_price: f64,
    client_order_id: Option<String>,
    created_at: DateTime<Utc>,
}

struct FillRow {
    id: i64,
    filled: f64,
    filled_atoms: i64,
    status: &'static str,
}

struct AccountRow {
    user_id: i64,
    asset: String,
    balance: f64,
    frozen: f64,
    balance_atoms: i64,
    frozen_atoms: i64,
}

struct TradeRow {
    buy_order_id: i64,
    sell_order_id: i64,
    symbol: String,
    price: f64,
    qty: f64,
    price_atoms: i64,
    qty_atoms: i64,
    created_at: DateTime<Utc>,
}

struct MatchingEventRow {
    sequence: i64,
    response_stream_id: i32,
    event_kind: i16,
    order_id: i64,
    client_order_id: i64,
    participant_id: i64,
    counterparty_order_id: i64,
    symbol: String,
    price_ticks: i64,
    quantity_lots: i64,
    remaining_lots: i64,
    ts_ns: i64,
}

/// Reason a frame was rejected by `push`, for diagnostics. Counters are
/// printed by the pg-writer stats log so we can see whether the persist
/// stream is carrying malformed frames or just unknown kinds.
#[derive(Debug, Default, Clone, Copy)]
pub struct SkipCounts {
    pub unknown_kind: u64,
    pub upsert_bad_status: u64,
    pub upsert_bad_timestamp: u64,
    pub upsert_empty_string: u64,
    pub fill_bad_status: u64,
    pub account_empty_asset: u64,
    pub trade_empty_symbol: u64,
    pub trade_bad_timestamp: u64,
    pub matching_empty_symbol: u64,
    pub matching_bad_timestamp: u64,
    pub payload_decode_failed: u64,
    /// f64 → atoms conversion failed (non-finite or beyond i64 atoms range).
    pub amount_atoms_invalid: u64,
}

impl SkipCounts {
    pub fn total(&self) -> u64 {
        self.unknown_kind
            + self.upsert_bad_status
            + self.upsert_bad_timestamp
            + self.upsert_empty_string
            + self.fill_bad_status
            + self.account_empty_asset
            + self.trade_empty_symbol
            + self.trade_bad_timestamp
            + self.matching_empty_symbol
            + self.matching_bad_timestamp
            + self.payload_decode_failed
            + self.amount_atoms_invalid
    }
}

#[derive(Default)]
pub struct PgWriteBatch {
    upserts: Vec<UpsertRow>,
    deletes: Vec<i64>,
    fills: Vec<FillRow>,
    accounts: Vec<AccountRow>,
    trades: Vec<TradeRow>,
    matching_events: Vec<MatchingEventRow>,
    skipped: u64,
    skip_counts: SkipCounts,
    /// Checkpoint floor per publisher: frames with seq <= floor were
    /// already applied in a committed transaction and are dropped as
    /// duplicates (exactly-once across restarts and Aeron replays).
    applied_seq: std::collections::HashMap<u16, u64>,
    /// Max seq per publisher accumulated in the CURRENT batch; written to
    /// persist_checkpoints inside the same flush transaction, promoted to
    /// applied_seq only after COMMIT succeeds.
    pending_max_seq: std::collections::HashMap<u16, u64>,
    /// Duplicate frames discarded by the checkpoint (replay/restart).
    pub duplicate_seq_frames: u64,
    /// Sequence holes observed (frames lost upstream of this consumer).
    pub seq_gap_frames: u64,
    /// Huge forward jumps (publisher restarted with a fresh clock-seeded
    /// sequence) — informational, not data loss.
    pub publisher_restarts: u64,
}

impl PgWriteBatch {
    pub fn new() -> Self {
        Self::default()
    }

    /// Seed the checkpoint floors from persist_checkpoints (call once at
    /// startup, after migrations).
    pub fn set_applied_floors(&mut self, floors: std::collections::HashMap<u16, u64>) {
        self.applied_seq = floors;
    }

    /// Frame admission control: returns false if the frame is a duplicate
    /// (seq at or below the committed checkpoint / already pending).
    /// Sequence accounting for gap/restart diagnostics happens here too.
    /// Unsequenced frames (seq == 0) always pass.
    fn admit_seq(&mut self, publisher_id: u16, seq: u64) -> bool {
        if seq == 0 {
            return true;
        }
        let floor = self.applied_seq.get(&publisher_id).copied().unwrap_or(0);
        let pending = self
            .pending_max_seq
            .get(&publisher_id)
            .copied()
            .unwrap_or(floor);
        if seq <= pending {
            self.duplicate_seq_frames += 1;
            return false;
        }
        let jump = seq - pending;
        if pending > 0 && jump > 1 {
            // Clock-seeded restart sequences jump by ~nanoseconds-of-downtime;
            // anything that large is a restart, not frame loss.
            const RESTART_JUMP: u64 = 1 << 40;
            if jump > RESTART_JUMP {
                self.publisher_restarts += 1;
            } else {
                self.seq_gap_frames += jump - 1;
            }
        }
        self.pending_max_seq.insert(publisher_id, seq);
        true
    }

    pub fn len(&self) -> usize {
        self.upserts.len()
            + self.deletes.len()
            + self.fills.len()
            + self.accounts.len()
            + self.trades.len()
            + self.matching_events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn skipped(&self) -> u64 {
        self.skipped
    }

    pub fn skip_counts(&self) -> SkipCounts {
        self.skip_counts
    }

    /// Drop any buffered rows but preserve the `skipped` counter. Used by
    /// the writer loop after a flush error: we discard the in-flight batch
    /// (to avoid an infinite retry loop on a poison frame) without losing
    /// the lifetime stats.
    pub fn clear_payloads(&mut self) {
        self.upserts.clear();
        self.deletes.clear();
        self.fills.clear();
        self.accounts.clear();
        self.trades.clear();
        self.matching_events.clear();
        // Dropped frames were never applied — the checkpoint must not
        // advance past them (redelivery will re-admit; at-least-once on
        // the explicit poison-drop path).
        self.pending_max_seq.clear();
    }

    /// Accept one frame into the batch. Returns false when the frame is
    /// malformed and was skipped (still safe to keep batching).
    pub fn push(&mut self, frame: &PersistFrame) -> bool {
        if !self.admit_seq(frame.publisher_id, frame.seq) {
            return false;
        }
        match frame.kind() {
            Some(PersistKind::OrderUpsert) => match frame.as_order_upsert() {
                Some(p) => self.push_upsert(&p),
                None => {
                    self.skipped += 1;
                    self.skip_counts.payload_decode_failed += 1;
                    false
                }
            },
            Some(PersistKind::OrderDelete) => match frame.as_order_delete() {
                Some(p) => {
                    self.push_delete(&p);
                    true
                }
                None => {
                    self.skipped += 1;
                    self.skip_counts.payload_decode_failed += 1;
                    false
                }
            },
            Some(PersistKind::OrderFillUpdate) => match frame.as_order_fill_update() {
                Some(p) => self.push_fill(&p),
                None => {
                    self.skipped += 1;
                    self.skip_counts.payload_decode_failed += 1;
                    false
                }
            },
            Some(PersistKind::AccountSet) => match frame.as_account_set() {
                Some(p) => self.push_account(&p),
                None => {
                    self.skipped += 1;
                    self.skip_counts.payload_decode_failed += 1;
                    false
                }
            },
            Some(PersistKind::TradeInsert) => match frame.as_trade_insert() {
                Some(p) => self.push_trade(&p),
                None => {
                    self.skipped += 1;
                    self.skip_counts.payload_decode_failed += 1;
                    false
                }
            },
            Some(PersistKind::MatchingEvent) => match frame.as_matching_event() {
                Some(p) => self.push_matching_event(&p),
                None => {
                    self.skipped += 1;
                    self.skip_counts.payload_decode_failed += 1;
                    false
                }
            },
            None => {
                self.skipped += 1;
                self.skip_counts.unknown_kind += 1;
                false
            }
        }
    }

    fn push_upsert(&mut self, p: &OrderUpsertPayload) -> bool {
        let id: i64 = p.id;
        let user_id: i64 = p.user_id;
        let side: u8 = p.side;
        let status_code: u8 = p.status;
        let price: f64 = p.price;
        let qty: f64 = p.qty;
        let filled: f64 = p.filled;
        let freeze_price: f64 = p.freeze_price;
        let created_at_ms: i64 = p.created_at_ms;
        let Some(status) = status_str(status_code) else {
            self.skipped += 1;
            self.skip_counts.upsert_bad_status += 1;
            return false;
        };
        let Some(created_at) = from_unix_ms(created_at_ms) else {
            self.skipped += 1;
            self.skip_counts.upsert_bad_timestamp += 1;
            return false;
        };
        let symbol = unpack_str(&p.symbol).to_owned();
        let order_type = unpack_str(&p.order_type).to_owned();
        let coid = unpack_str(&p.client_order_id).to_owned();
        if symbol.is_empty() || order_type.is_empty() {
            self.skipped += 1;
            self.skip_counts.upsert_empty_string += 1;
            return false;
        }
        // price=0 is the documented sentinel for "market order" (NULL in PG).
        let pg_price = if price == 0.0 { None } else { Some(price) };
        let coid_opt = if coid.is_empty() { None } else { Some(coid) };
        let (Some(quantity_atoms), Some(filled_atoms)) = (to_atoms(qty), to_atoms(filled)) else {
            self.skipped += 1;
            self.skip_counts.amount_atoms_invalid += 1;
            return false;
        };
        let price_atoms = match pg_price {
            None => None,
            Some(v) => match to_atoms(v) {
                Some(a) => Some(a),
                None => {
                    self.skipped += 1;
                    self.skip_counts.amount_atoms_invalid += 1;
                    return false;
                }
            },
        };
        self.upserts.push(UpsertRow {
            id,
            user_id,
            symbol,
            side: side_str(side),
            order_type,
            price: pg_price,
            quantity: qty,
            filled,
            price_atoms,
            quantity_atoms,
            filled_atoms,
            status,
            freeze_price,
            client_order_id: coid_opt,
            created_at,
        });
        true
    }

    fn push_delete(&mut self, p: &OrderDeletePayload) {
        let id: i64 = p.id;
        self.deletes.push(id);
    }

    fn push_fill(&mut self, p: &OrderFillUpdatePayload) -> bool {
        let id: i64 = p.id;
        let filled: f64 = p.filled;
        let Some(status) = status_str(p.status) else {
            self.skipped += 1;
            self.skip_counts.fill_bad_status += 1;
            return false;
        };
        let Some(filled_atoms) = to_atoms(filled) else {
            self.skipped += 1;
            self.skip_counts.amount_atoms_invalid += 1;
            return false;
        };
        self.fills.push(FillRow {
            id,
            filled,
            filled_atoms,
            status,
        });
        true
    }

    fn push_account(&mut self, p: &AccountSetPayload) -> bool {
        let user_id: i64 = p.user_id;
        let balance: f64 = p.balance;
        let frozen: f64 = p.frozen;
        let mut balance_atoms: i64 = p.balance_atoms;
        let mut frozen_atoms: i64 = p.frozen_atoms;
        let asset = unpack_str(&p.asset).to_owned();
        if asset.is_empty() {
            self.skipped += 1;
            self.skip_counts.account_empty_asset += 1;
            return false;
        }
        if balance_atoms == 0 && balance != 0.0 {
            balance_atoms = match AmountAtoms::from_f64_round(balance) {
                Ok(v) => v.atoms(),
                Err(_) => {
                    self.skipped += 1;
                    self.skip_counts.payload_decode_failed += 1;
                    return false;
                }
            };
        }
        if frozen_atoms == 0 && frozen != 0.0 {
            frozen_atoms = match AmountAtoms::from_f64_round(frozen) {
                Ok(v) => v.atoms(),
                Err(_) => {
                    self.skipped += 1;
                    self.skip_counts.payload_decode_failed += 1;
                    return false;
                }
            };
        }
        self.accounts.push(AccountRow {
            user_id,
            asset,
            balance,
            frozen,
            balance_atoms,
            frozen_atoms,
        });
        true
    }

    fn push_trade(&mut self, p: &TradeInsertPayload) -> bool {
        let buy_order_id: i64 = p.buy_order_id;
        let sell_order_id: i64 = p.sell_order_id;
        let price: f64 = p.price;
        let qty: f64 = p.qty;
        let ts_ms: i64 = p.ts_ms;
        let symbol = unpack_str(&p.symbol).to_owned();
        if symbol.is_empty() {
            self.skipped += 1;
            self.skip_counts.trade_empty_symbol += 1;
            return false;
        }
        let Some(created_at) = from_unix_ms(ts_ms) else {
            self.skipped += 1;
            self.skip_counts.trade_bad_timestamp += 1;
            return false;
        };
        let (Some(price_atoms), Some(qty_atoms)) = (to_atoms(price), to_atoms(qty)) else {
            self.skipped += 1;
            self.skip_counts.amount_atoms_invalid += 1;
            return false;
        };
        self.trades.push(TradeRow {
            buy_order_id,
            sell_order_id,
            symbol,
            price,
            qty,
            price_atoms,
            qty_atoms,
            created_at,
        });
        true
    }

    fn push_matching_event(&mut self, p: &MatchingEventPayload) -> bool {
        let sequence = p.sequence;
        let response_stream_id = p.response_stream_id;
        let event_kind = p.event_kind;
        let order_id = p.order_id;
        let client_order_id = p.client_order_id;
        let participant_id = p.participant_id;
        let counterparty_order_id = p.counterparty_order_id;
        let price_ticks = p.price_ticks;
        let quantity_lots = p.quantity_lots;
        let remaining_lots = p.remaining_lots;
        let ts_ns = p.ts_ns;
        let symbol = unpack_str(&p.symbol).to_owned();
        if symbol.is_empty() {
            self.skipped += 1;
            self.skip_counts.matching_empty_symbol += 1;
            return false;
        }
        if sequence > i64::MAX as u64 || ts_ns > i64::MAX as u64 {
            self.skipped += 1;
            self.skip_counts.matching_bad_timestamp += 1;
            return false;
        }
        self.matching_events.push(MatchingEventRow {
            sequence: sequence as i64,
            response_stream_id,
            event_kind: event_kind as i16,
            order_id,
            client_order_id,
            participant_id,
            counterparty_order_id,
            symbol,
            price_ticks,
            quantity_lots,
            remaining_lots,
            ts_ns: ts_ns as i64,
        });
        true
    }

    /// Public hook for backfill: stage a fully-decoded row from Redis
    /// into the upsert buffer without going through PersistFrame parsing.
    /// Used by `backfill_from_redis` below.
    #[allow(clippy::too_many_arguments)]
    pub fn push_upsert_row(
        &mut self,
        id: i64,
        user_id: i64,
        symbol: String,
        side: &'static str,
        order_type: String,
        price: Option<f64>,
        quantity: f64,
        filled: f64,
        status: &'static str,
        freeze_price: f64,
        client_order_id: Option<String>,
        created_at: DateTime<Utc>,
    ) {
        let price_atoms = price.and_then(to_atoms);
        self.upserts.push(UpsertRow {
            id,
            user_id,
            symbol,
            side,
            order_type,
            price,
            quantity,
            filled,
            price_atoms,
            quantity_atoms: to_atoms(quantity).unwrap_or(0),
            filled_atoms: to_atoms(filled).unwrap_or(0),
            status,
            freeze_price,
            client_order_id,
            created_at,
        });
    }

    /// Apply everything to PG inside one transaction. Returns total rows
    /// written (sum of affected per kind). Rows are removed from the batch
    /// only after the transaction commits, so a failed flush can be retried
    /// without losing accepted frames or landing half a settlement.
    pub async fn flush(&mut self, pool: &PgPool) -> anyhow::Result<usize> {
        let mut tx = pool.begin().await?;
        let mut total = 0;

        if !self.upserts.is_empty() {
            total += flush_upserts(&mut *tx, &self.upserts).await?;
        }
        if !self.deletes.is_empty() {
            total += flush_deletes(&mut *tx, &self.deletes).await?;
        }
        if !self.fills.is_empty() {
            total += flush_fills(&mut *tx, &self.fills).await?;
        }
        if !self.accounts.is_empty() {
            total += flush_accounts(&mut *tx, &self.accounts).await?;
        }
        if !self.trades.is_empty() {
            total += flush_trades(&mut *tx, &self.trades).await?;
        }
        if !self.matching_events.is_empty() {
            total += flush_matching_events(&mut *tx, &self.matching_events).await?;
        }
        if !self.pending_max_seq.is_empty() {
            flush_checkpoints(&mut tx, &self.pending_max_seq).await?;
        }
        tx.commit().await?;

        // COMMIT succeeded → promote the batch's checkpoint into the floor.
        for (publisher_id, seq) in self.pending_max_seq.drain() {
            let floor = self.applied_seq.entry(publisher_id).or_insert(0);
            if seq > *floor {
                *floor = seq;
            }
        }

        self.upserts.clear();
        self.deletes.clear();
        self.fills.clear();
        self.accounts.clear();
        self.trades.clear();
        self.matching_events.clear();

        Ok(total)
    }
}

/// In-place "keep last by key" dedup. Preserves order of the surviving rows
/// (the position of the LAST occurrence). Used because PG's ON CONFLICT DO
/// UPDATE refuses batches where the same target row appears twice.
#[cfg(test)]
fn dedup_keep_last_by_id<T, F: Fn(&T) -> i64>(rows: Vec<T>, key: F) -> Vec<T> {
    use std::collections::HashMap;
    let mut keep: HashMap<i64, usize> = HashMap::with_capacity(rows.len());
    for (i, r) in rows.iter().enumerate() {
        keep.insert(key(r), i);
    }
    let mut keep_idx: Vec<usize> = keep.into_values().collect();
    keep_idx.sort_unstable();
    let mut out = Vec::with_capacity(keep_idx.len());
    for (i, r) in rows.into_iter().enumerate() {
        if keep_idx.binary_search(&i).is_ok() {
            out.push(r);
        }
    }
    out
}

#[cfg(test)]
fn dedup_accounts_keep_last(rows: Vec<AccountRow>) -> Vec<AccountRow> {
    use std::collections::HashMap;
    let mut keep: HashMap<(i64, String), usize> = HashMap::with_capacity(rows.len());
    for (i, r) in rows.iter().enumerate() {
        keep.insert((r.user_id, r.asset.clone()), i);
    }
    let mut keep_idx: Vec<usize> = keep.into_values().collect();
    keep_idx.sort_unstable();
    let mut out = Vec::with_capacity(keep_idx.len());
    for (i, r) in rows.into_iter().enumerate() {
        if keep_idx.binary_search(&i).is_ok() {
            out.push(r);
        }
    }
    out
}

fn dedup_keep_last_indices_by_id<T, F: Fn(&T) -> i64>(rows: &[T], key: F) -> Vec<usize> {
    use std::collections::HashMap;
    let mut keep: HashMap<i64, usize> = HashMap::with_capacity(rows.len());
    for (i, r) in rows.iter().enumerate() {
        keep.insert(key(r), i);
    }
    let mut keep_idx: Vec<usize> = keep.into_values().collect();
    keep_idx.sort_unstable();
    keep_idx
}

fn dedup_account_indices_keep_last(rows: &[AccountRow]) -> Vec<usize> {
    use std::collections::HashMap;
    let mut keep: HashMap<(i64, &str), usize> = HashMap::with_capacity(rows.len());
    for (i, r) in rows.iter().enumerate() {
        keep.insert((r.user_id, r.asset.as_str()), i);
    }
    let mut keep_idx: Vec<usize> = keep.into_values().collect();
    keep_idx.sort_unstable();
    keep_idx
}

async fn flush_upserts(conn: &mut PgConnection, rows: &[UpsertRow]) -> anyhow::Result<usize> {
    // Dedup by id, keeping the LAST occurrence — PG's ON CONFLICT DO UPDATE
    // rejects batches that touch the same row twice. "Last wins" is the
    // semantics we want anyway: the final state per id reflects the
    // newest event in the batch.
    let keep_idx = dedup_keep_last_indices_by_id(rows, |r| r.id);
    // Pivot to column arrays for UNNEST — single SQL, server-side join.
    let n = keep_idx.len();
    let mut ids = Vec::with_capacity(n);
    let mut user_ids = Vec::with_capacity(n);
    let mut symbols = Vec::with_capacity(n);
    let mut sides = Vec::with_capacity(n);
    let mut order_types = Vec::with_capacity(n);
    let mut prices: Vec<Option<f64>> = Vec::with_capacity(n);
    let mut qtys = Vec::with_capacity(n);
    let mut filleds = Vec::with_capacity(n);
    let mut price_atoms: Vec<Option<i64>> = Vec::with_capacity(n);
    let mut qty_atoms = Vec::with_capacity(n);
    let mut filled_atoms = Vec::with_capacity(n);
    let mut statuses = Vec::with_capacity(n);
    let mut freeze_prices = Vec::with_capacity(n);
    let mut coids: Vec<Option<String>> = Vec::with_capacity(n);
    let mut createds = Vec::with_capacity(n);

    for i in keep_idx {
        let r = &rows[i];
        ids.push(r.id);
        user_ids.push(r.user_id);
        symbols.push(r.symbol.clone());
        sides.push(r.side.to_string());
        order_types.push(r.order_type.clone());
        prices.push(r.price);
        qtys.push(r.quantity);
        filleds.push(r.filled);
        price_atoms.push(r.price_atoms);
        qty_atoms.push(r.quantity_atoms);
        filled_atoms.push(r.filled_atoms);
        statuses.push(r.status.to_string());
        freeze_prices.push(r.freeze_price);
        coids.push(r.client_order_id.clone());
        createds.push(r.created_at);
    }

    let res = sqlx::query(
        r#"
        INSERT INTO orders (id, user_id, symbol, side, order_type, price, quantity, filled,
                            price_atoms, quantity_atoms, filled_atoms,
                            status, freeze_price, client_order_id, created_at, updated_at)
        SELECT * FROM UNNEST(
            $1::bigint[],
            $2::bigint[],
            $3::varchar[],
            $4::varchar[],
            $5::varchar[],
            $6::float8[],
            $7::float8[],
            $8::float8[],
            $9::bigint[],
            $10::bigint[],
            $11::bigint[],
            $12::varchar[],
            $13::float8[],
            $14::varchar[],
            $15::timestamptz[]
        ) AS t(id, user_id, symbol, side, order_type, price, quantity, filled,
               price_atoms, quantity_atoms, filled_atoms, status,
               freeze_price, client_order_id, created_at)
        CROSS JOIN LATERAL (SELECT NOW() AS updated_at) u
        ON CONFLICT (id) DO UPDATE SET
            status       = EXCLUDED.status,
            filled       = EXCLUDED.filled,
            filled_atoms = EXCLUDED.filled_atoms,
            updated_at   = NOW()
        "#,
    )
    .bind(&ids)
    .bind(&user_ids)
    .bind(&symbols)
    .bind(&sides)
    .bind(&order_types)
    .bind(&prices)
    .bind(&qtys)
    .bind(&filleds)
    .bind(&price_atoms)
    .bind(&qty_atoms)
    .bind(&filled_atoms)
    .bind(&statuses)
    .bind(&freeze_prices)
    .bind(&coids)
    .bind(&createds)
    .execute(&mut *conn)
    .await?;
    Ok(res.rows_affected() as usize)
}

async fn flush_deletes(conn: &mut PgConnection, ids: &[i64]) -> anyhow::Result<usize> {
    // DELETE WHERE ANY tolerates duplicate ids, but trimming saves bytes.
    let mut ids = ids.to_vec();
    ids.sort_unstable();
    ids.dedup();
    let res = sqlx::query("DELETE FROM orders WHERE id = ANY($1::bigint[])")
        .bind(&ids)
        .execute(&mut *conn)
        .await?;
    Ok(res.rows_affected() as usize)
}

async fn flush_fills(conn: &mut PgConnection, rows: &[FillRow]) -> anyhow::Result<usize> {
    // UPDATE FROM (UNNEST...) silently joins one VALUES row to the same
    // target row twice when duplicates exist; result is "one of the values
    // wins, undefined which." We force last-wins explicitly.
    let keep_idx = dedup_keep_last_indices_by_id(rows, |r| r.id);
    let mut ids = Vec::with_capacity(keep_idx.len());
    let mut filleds = Vec::with_capacity(keep_idx.len());
    let mut filled_atoms = Vec::with_capacity(keep_idx.len());
    let mut statuses = Vec::with_capacity(keep_idx.len());
    for i in keep_idx {
        let r = &rows[i];
        ids.push(r.id);
        filleds.push(r.filled);
        filled_atoms.push(r.filled_atoms);
        statuses.push(r.status.to_string());
    }
    let res = sqlx::query(
        r#"
        UPDATE orders AS o SET
            filled       = v.filled,
            filled_atoms = v.filled_atoms,
            status       = v.status,
            updated_at   = NOW()
        FROM UNNEST($1::bigint[], $2::float8[], $3::bigint[], $4::varchar[])
            AS v(id, filled, filled_atoms, status)
        WHERE o.id = v.id
        "#,
    )
    .bind(&ids)
    .bind(&filleds)
    .bind(&filled_atoms)
    .bind(&statuses)
    .execute(&mut *conn)
    .await?;
    Ok(res.rows_affected() as usize)
}

async fn flush_accounts(conn: &mut PgConnection, rows: &[AccountRow]) -> anyhow::Result<usize> {
    // Dedup by (user_id, asset) keeping the last value — same ON CONFLICT
    // constraint as orders.
    let keep_idx = dedup_account_indices_keep_last(rows);
    let mut user_ids = Vec::with_capacity(keep_idx.len());
    let mut assets = Vec::with_capacity(keep_idx.len());
    let mut balances = Vec::with_capacity(keep_idx.len());
    let mut frozens = Vec::with_capacity(keep_idx.len());
    let mut balance_atoms = Vec::with_capacity(keep_idx.len());
    let mut frozen_atoms = Vec::with_capacity(keep_idx.len());
    for i in keep_idx {
        let r = &rows[i];
        user_ids.push(r.user_id);
        assets.push(r.asset.clone());
        balances.push(r.balance);
        frozens.push(r.frozen);
        balance_atoms.push(r.balance_atoms);
        frozen_atoms.push(r.frozen_atoms);
    }
    let res = sqlx::query(
        r#"
        INSERT INTO accounts (user_id, asset, balance, frozen, balance_atoms, frozen_atoms, updated_at)
        SELECT * FROM UNNEST(
            $1::bigint[],
            $2::varchar[],
            $3::float8[],
            $4::float8[],
            $5::bigint[],
            $6::bigint[]
        ) AS t(user_id, asset, balance, frozen, balance_atoms, frozen_atoms)
        CROSS JOIN LATERAL (SELECT NOW() AS updated_at) u
        ON CONFLICT (user_id, asset) DO UPDATE SET
            balance     = EXCLUDED.balance,
            frozen      = EXCLUDED.frozen,
            balance_atoms = EXCLUDED.balance_atoms,
            frozen_atoms  = EXCLUDED.frozen_atoms,
            updated_at  = NOW()
        "#,
    )
    .bind(&user_ids)
    .bind(&assets)
    .bind(&balances)
    .bind(&frozens)
    .bind(&balance_atoms)
    .bind(&frozen_atoms)
    .execute(&mut *conn)
    .await?;
    Ok(res.rows_affected() as usize)
}

async fn flush_trades(conn: &mut PgConnection, rows: &[TradeRow]) -> anyhow::Result<usize> {
    let mut buys = Vec::with_capacity(rows.len());
    let mut sells = Vec::with_capacity(rows.len());
    let mut syms = Vec::with_capacity(rows.len());
    let mut prices = Vec::with_capacity(rows.len());
    let mut qtys = Vec::with_capacity(rows.len());
    let mut price_atoms = Vec::with_capacity(rows.len());
    let mut qty_atoms = Vec::with_capacity(rows.len());
    let mut createds = Vec::with_capacity(rows.len());
    for r in rows {
        buys.push(r.buy_order_id);
        sells.push(r.sell_order_id);
        syms.push(r.symbol.clone());
        prices.push(r.price);
        qtys.push(r.qty);
        price_atoms.push(r.price_atoms);
        qty_atoms.push(r.qty_atoms);
        createds.push(r.created_at);
    }
    let res = sqlx::query(
        r#"
        INSERT INTO trades (symbol, buy_order_id, sell_order_id, price, quantity,
                            price_atoms, quantity_atoms, created_at)
        SELECT * FROM UNNEST(
            $1::varchar[],
            $2::bigint[],
            $3::bigint[],
            $4::float8[],
            $5::float8[],
            $6::bigint[],
            $7::bigint[],
            $8::timestamptz[]
        ) AS t(symbol, buy_order_id, sell_order_id, price, quantity,
               price_atoms, quantity_atoms, created_at)
        "#,
    )
    .bind(&syms)
    .bind(&buys)
    .bind(&sells)
    .bind(&prices)
    .bind(&qtys)
    .bind(&price_atoms)
    .bind(&qty_atoms)
    .bind(&createds)
    .execute(&mut *conn)
    .await?;
    Ok(res.rows_affected() as usize)
}

/// Upsert per-publisher checkpoints inside the data transaction.
/// GREATEST guards against another writer instance having advanced further.
async fn flush_checkpoints(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    pending: &std::collections::HashMap<u16, u64>,
) -> anyhow::Result<()> {
    let mut ids = Vec::with_capacity(pending.len());
    let mut seqs = Vec::with_capacity(pending.len());
    for (&publisher_id, &seq) in pending {
        ids.push(publisher_id as i32);
        seqs.push(seq as i64);
    }
    sqlx::query(
        r#"
        INSERT INTO persist_checkpoints (publisher_id, last_seq, updated_at)
        SELECT t.publisher_id, t.last_seq, NOW()
          FROM UNNEST($1::int[], $2::bigint[]) AS t(publisher_id, last_seq)
        ON CONFLICT (publisher_id) DO UPDATE SET
            last_seq   = GREATEST(persist_checkpoints.last_seq, EXCLUDED.last_seq),
            updated_at = NOW()
        "#,
    )
    .bind(&ids)
    .bind(&seqs)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Load committed checkpoint floors (startup).
pub async fn load_checkpoints(
    pool: &sqlx::PgPool,
) -> anyhow::Result<std::collections::HashMap<u16, u64>> {
    let rows: Vec<(i32, i64)> =
        sqlx::query_as("SELECT publisher_id, last_seq FROM persist_checkpoints")
            .fetch_all(pool)
            .await?;
    Ok(rows
        .into_iter()
        .map(|(id, seq)| (id as u16, seq.max(0) as u64))
        .collect())
}

async fn flush_matching_events(
    conn: &mut PgConnection,
    rows: &[MatchingEventRow],
) -> anyhow::Result<usize> {
    let mut sequences = Vec::with_capacity(rows.len());
    let mut response_stream_ids = Vec::with_capacity(rows.len());
    let mut event_kinds = Vec::with_capacity(rows.len());
    let mut order_ids = Vec::with_capacity(rows.len());
    let mut client_order_ids = Vec::with_capacity(rows.len());
    let mut participant_ids = Vec::with_capacity(rows.len());
    let mut counterparty_order_ids = Vec::with_capacity(rows.len());
    let mut symbols = Vec::with_capacity(rows.len());
    let mut price_ticks = Vec::with_capacity(rows.len());
    let mut quantity_lots = Vec::with_capacity(rows.len());
    let mut remaining_lots = Vec::with_capacity(rows.len());
    let mut ts_ns = Vec::with_capacity(rows.len());
    for r in rows {
        sequences.push(r.sequence);
        response_stream_ids.push(r.response_stream_id);
        event_kinds.push(r.event_kind);
        order_ids.push(r.order_id);
        client_order_ids.push(r.client_order_id);
        participant_ids.push(r.participant_id);
        counterparty_order_ids.push(r.counterparty_order_id);
        symbols.push(r.symbol.clone());
        price_ticks.push(r.price_ticks);
        quantity_lots.push(r.quantity_lots);
        remaining_lots.push(r.remaining_lots);
        ts_ns.push(r.ts_ns);
    }
    let res = sqlx::query(
        r#"
        INSERT INTO matching_events (
            sequence, response_stream_id, event_kind, order_id, client_order_id,
            participant_id, counterparty_order_id, symbol, price_ticks,
            quantity_lots, remaining_lots, ts_ns
        )
        SELECT * FROM UNNEST(
            $1::bigint[],
            $2::integer[],
            $3::smallint[],
            $4::bigint[],
            $5::bigint[],
            $6::bigint[],
            $7::bigint[],
            $8::varchar[],
            $9::bigint[],
            $10::bigint[],
            $11::bigint[],
            $12::bigint[]
        ) AS t(
            sequence, response_stream_id, event_kind, order_id, client_order_id,
            participant_id, counterparty_order_id, symbol, price_ticks,
            quantity_lots, remaining_lots, ts_ns
        )
        ON CONFLICT (response_stream_id, sequence) DO NOTHING
        "#,
    )
    .bind(&sequences)
    .bind(&response_stream_ids)
    .bind(&event_kinds)
    .bind(&order_ids)
    .bind(&client_order_ids)
    .bind(&participant_ids)
    .bind(&counterparty_order_ids)
    .bind(&symbols)
    .bind(&price_ticks)
    .bind(&quantity_lots)
    .bind(&remaining_lots)
    .bind(&ts_ns)
    .execute(&mut *conn)
    .await?;
    Ok(res.rows_affected() as usize)
}

/// Stats from `backfill_from_redis` — how many ids were checked, how
/// many were missing in PG, how many we successfully INSERTed.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BackfillStats {
    pub redis_orders_scanned: u64,
    pub missing_in_pg: u64,
    pub backfilled: u64,
    /// Could not decode the Redis HASH (incomplete fields, bad parse).
    /// These are dropped rather than inserted with sentinels.
    pub skipped_decode: u64,
}

/// Recover PG state for orders that exist in Redis but not in PG. This
/// happens after a pg-writer restart that left a gap (the Aeron persist
/// stream is not replayable — frames published before the subscriber
/// joins are lost).
///
/// Approach:
/// 1. Read Redis `active_orders` SMEMBERS.
/// 2. Ask PG which of those ids already exist.
/// 3. For each missing id, HGETALL `order:{id}` → INSERT into PG using
///    UNNEST batching. Same code path as flush_upserts.
///
/// Idempotent — safe to call alongside live writes; the INSERT uses
/// ON CONFLICT DO UPDATE so concurrent pg-writer flushes don't conflict.
pub async fn backfill_from_redis(
    pg: &PgPool,
    conn: &mut redis::aio::MultiplexedConnection,
) -> anyhow::Result<BackfillStats> {
    use redis::AsyncCommands;
    use std::collections::HashSet;

    let mut stats = BackfillStats::default();

    let redis_ids: Vec<i64> = conn
        .smembers(crate::desk::redis_store::KEY_ACTIVE_ORDERS)
        .await?;
    stats.redis_orders_scanned = redis_ids.len() as u64;
    if redis_ids.is_empty() {
        return Ok(stats);
    }

    let pg_existing: Vec<(i64,)> =
        sqlx::query_as("SELECT id FROM orders WHERE id = ANY($1::bigint[])")
            .bind(&redis_ids)
            .fetch_all(pg)
            .await?;
    let pg_set: HashSet<i64> = pg_existing.into_iter().map(|(id,)| id).collect();

    let missing: Vec<i64> = redis_ids
        .into_iter()
        .filter(|id| !pg_set.contains(id))
        .collect();
    stats.missing_in_pg = missing.len() as u64;
    if missing.is_empty() {
        return Ok(stats);
    }

    // Pull HGETALL for every missing id in one pipelined burst.
    let mut pipe = redis::pipe();
    for id in &missing {
        pipe.hgetall(crate::desk::redis_store::key_order(*id));
    }
    let hashes: Vec<std::collections::HashMap<String, String>> = pipe.query_async(conn).await?;

    let mut batch = PgWriteBatch::new();
    for (id, h) in missing.iter().zip(hashes.into_iter()) {
        // Decode the HASH back into UpsertRow fields. Mirror of
        // redis_store::decode_hash but adapted for the pg_store side
        // (string-typed status/side rather than serde DbOrder).
        let user_id: Option<i64> = h.get("user_id").and_then(|s| s.parse().ok());
        let symbol = h.get("symbol").cloned();
        let side = h.get("side").cloned();
        let order_type = h.get("order_type").cloned();
        let status = h.get("status").cloned();
        let qty: Option<f64> = h.get("qty").and_then(|s| s.parse().ok());
        let filled: Option<f64> = h.get("filled").and_then(|s| s.parse().ok());
        let freeze_price: Option<f64> = h.get("freeze_price").and_then(|s| s.parse().ok());
        let price_raw: Option<f64> = h.get("price").and_then(|s| s.parse().ok());
        let created_at_ms: Option<i64> = h.get("created_at_ms").and_then(|s| s.parse().ok());

        let (user_id, symbol, side, order_type, status, qty, filled, freeze_price, created_at_ms) =
            match (
                user_id,
                symbol,
                side,
                order_type,
                status,
                qty,
                filled,
                freeze_price,
                created_at_ms,
            ) {
                (
                    Some(a),
                    Some(b),
                    Some(c),
                    Some(d),
                    Some(e),
                    Some(f),
                    Some(g),
                    Some(h),
                    Some(i),
                ) => (a, b, c, d, e, f, g, h, i),
                _ => {
                    stats.skipped_decode += 1;
                    continue;
                }
            };

        let side_static: &'static str = match side.as_str() {
            "buy" => "buy",
            "sell" => "sell",
            _ => {
                stats.skipped_decode += 1;
                continue;
            }
        };
        let status_static: &'static str = match status.as_str() {
            "PENDING" => "PENDING",
            "TRADING" => "TRADING",
            "COMPLETED" => "COMPLETED",
            "CANCELED" => "CANCELED",
            "REJECTED" => "REJECTED",
            _ => {
                stats.skipped_decode += 1;
                continue;
            }
        };
        let price = if let Some(p) = price_raw {
            if p == 0.0 { None } else { Some(p) }
        } else {
            None
        };
        let client_order_id = match h.get("client_order_id") {
            Some(s) if !s.is_empty() => Some(s.clone()),
            _ => None,
        };
        let Some(created_at) = from_unix_ms(created_at_ms) else {
            stats.skipped_decode += 1;
            continue;
        };

        batch.push_upsert_row(
            *id,
            user_id,
            symbol,
            side_static,
            order_type,
            price,
            qty,
            filled,
            status_static,
            freeze_price,
            client_order_id,
            created_at,
        );
    }

    let written = batch.flush(pg).await?;
    stats.backfilled = written as u64;
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::persist_event::pack_str;

    fn upsert_frame(id: i64) -> PersistFrame {
        PersistFrame::order_upsert(OrderUpsertPayload {
            id,
            user_id: 7,
            symbol: pack_str("BTC_USDT"),
            side: 0,
            status: 0, // PENDING (DbOrderStatus::Pending = 0)
            _pad: [0; 6],
            order_type: pack_str("limit"),
            price: 70000.0,
            qty: 0.1,
            filled: 0.0,
            freeze_price: 70000.0,
            client_order_id: pack_str(""),
            created_at_ms: 1_700_000_000_000,
        })
    }

    #[test]
    fn push_decodes_upsert_into_row() {
        let mut b = PgWriteBatch::new();
        assert!(b.push(&upsert_frame(42)));
        assert_eq!(b.upserts.len(), 1);
        assert_eq!(b.upserts[0].id, 42);
        assert_eq!(b.upserts[0].symbol, "BTC_USDT");
        assert_eq!(b.upserts[0].status, "PENDING");
        assert_eq!(b.upserts[0].price, Some(70000.0));
    }

    #[test]
    fn upsert_with_zero_price_becomes_null() {
        let mut f = upsert_frame(99);
        if let Some(mut p) = f.as_order_upsert() {
            p.price = 0.0;
            f = PersistFrame::order_upsert(p);
        }
        let mut b = PgWriteBatch::new();
        assert!(b.push(&f));
        assert_eq!(b.upserts[0].price, None, "zero sentinel → NULL");
    }

    #[test]
    fn upsert_with_unknown_status_is_skipped() {
        let mut f = upsert_frame(1);
        if let Some(mut p) = f.as_order_upsert() {
            p.status = 99;
            f = PersistFrame::order_upsert(p);
        }
        let mut b = PgWriteBatch::new();
        assert!(!b.push(&f));
        assert_eq!(b.skipped(), 1);
        assert!(b.is_empty());
    }

    #[test]
    fn empty_asset_account_is_skipped() {
        let f = PersistFrame::account_set(AccountSetPayload {
            user_id: 3,
            asset: pack_str(""),
            balance: 1.0,
            frozen: 0.0,
            balance_atoms: 100_000_000,
            frozen_atoms: 0,
        });
        let mut b = PgWriteBatch::new();
        assert!(!b.push(&f));
        assert_eq!(b.skipped(), 1);
    }

    #[test]
    fn delete_pushes_id_only() {
        let f = PersistFrame::order_delete(OrderDeletePayload { id: 17 });
        let mut b = PgWriteBatch::new();
        assert!(b.push(&f));
        assert_eq!(b.deletes, vec![17]);
    }

    #[test]
    fn dedup_keep_last_keeps_last_occurrence() {
        #[derive(Clone)]
        struct R {
            id: i64,
            tag: u8,
        }
        let rows = vec![
            R { id: 1, tag: 10 },
            R { id: 2, tag: 20 },
            R { id: 1, tag: 11 }, // overrides id=1
            R { id: 3, tag: 30 },
            R { id: 2, tag: 21 }, // overrides id=2
        ];
        let out = dedup_keep_last_by_id(rows, |r| r.id);
        // Order: positions kept = indices 2,3,4 ⇒ ids [1,3,2] with last tags.
        let ids: Vec<i64> = out.iter().map(|r| r.id).collect();
        let tags: Vec<u8> = out.iter().map(|r| r.tag).collect();
        assert_eq!(ids, vec![1, 3, 2]);
        assert_eq!(tags, vec![11, 30, 21]);
    }

    #[test]
    fn dedup_accounts_keeps_last_per_user_asset() {
        fn account_row(user_id: i64, asset: &str, balance: f64, frozen: f64) -> AccountRow {
            AccountRow {
                user_id,
                asset: asset.into(),
                balance,
                frozen,
                balance_atoms: AmountAtoms::from_f64_round(balance).unwrap().atoms(),
                frozen_atoms: AmountAtoms::from_f64_round(frozen).unwrap().atoms(),
            }
        }

        let rows = vec![
            account_row(1, "USDT", 100.0, 0.0),
            account_row(2, "USDT", 200.0, 0.0),
            account_row(1, "USDT", 150.0, 5.0), // overrides user=1,USDT
            account_row(1, "BTC", 0.1, 0.0),
        ];
        let out = dedup_accounts_keep_last(rows);
        assert_eq!(out.len(), 3);
        // The "last (user=1, USDT)" row must have balance=150 not 100.
        let usdt_user1 = out
            .iter()
            .find(|r| r.user_id == 1 && r.asset == "USDT")
            .expect("user=1 USDT present");
        assert!((usdt_user1.balance - 150.0).abs() < 1e-9);
        assert!((usdt_user1.frozen - 5.0).abs() < 1e-9);
    }

    #[test]
    fn fill_with_unknown_status_skipped() {
        // Wire encoding is 0..=4 (DbOrderStatus). 99 is out-of-range.
        let f = PersistFrame::order_fill_update(OrderFillUpdatePayload {
            id: 5,
            filled: 0.1,
            status: 99,
            _pad: [0; 7],
        });
        let mut b = PgWriteBatch::new();
        assert!(!b.push(&f));
        assert_eq!(b.skipped(), 1);
    }
}
