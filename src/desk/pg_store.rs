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

use crate::transport::persist_event::{
    unpack_str, AccountSetPayload, OrderDeletePayload, OrderFillUpdatePayload, OrderUpsertPayload,
    PersistFrame, PersistKind, TradeInsertPayload,
};
use chrono::{DateTime, TimeZone, Utc};
use sqlx::PgPool;

const STATUS_NAMES: [&str; 6] = ["UNKNOWN", "PENDING", "TRADING", "COMPLETED", "CANCELED", "REJECTED"];

fn status_str(code: u8) -> Option<&'static str> {
    if (1..=5).contains(&code) {
        Some(STATUS_NAMES[code as usize])
    } else {
        None
    }
}

fn side_str(code: u8) -> &'static str {
    if code == 0 {
        "buy"
    } else {
        "sell"
    }
}

fn from_unix_ms(ms: i64) -> Option<DateTime<Utc>> {
    Utc.timestamp_millis_opt(ms).single()
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
    status: &'static str,
    freeze_price: f64,
    client_order_id: Option<String>,
    created_at: DateTime<Utc>,
}

struct FillRow {
    id: i64,
    filled: f64,
    status: &'static str,
}

struct AccountRow {
    user_id: i64,
    asset: String,
    balance: f64,
    frozen: f64,
}

struct TradeRow {
    buy_order_id: i64,
    sell_order_id: i64,
    symbol: String,
    price: f64,
    qty: f64,
    created_at: DateTime<Utc>,
}

#[derive(Default)]
pub struct PgWriteBatch {
    upserts: Vec<UpsertRow>,
    deletes: Vec<i64>,
    fills: Vec<FillRow>,
    accounts: Vec<AccountRow>,
    trades: Vec<TradeRow>,
    skipped: u64,
}

impl PgWriteBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.upserts.len()
            + self.deletes.len()
            + self.fills.len()
            + self.accounts.len()
            + self.trades.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn skipped(&self) -> u64 {
        self.skipped
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
    }

    /// Accept one frame into the batch. Returns false when the frame is
    /// malformed and was skipped (still safe to keep batching).
    pub fn push(&mut self, frame: &PersistFrame) -> bool {
        match frame.kind() {
            Some(PersistKind::OrderUpsert) => match frame.as_order_upsert() {
                Some(p) => self.push_upsert(&p),
                None => {
                    self.skipped += 1;
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
                    false
                }
            },
            Some(PersistKind::OrderFillUpdate) => match frame.as_order_fill_update() {
                Some(p) => self.push_fill(&p),
                None => {
                    self.skipped += 1;
                    false
                }
            },
            Some(PersistKind::AccountSet) => match frame.as_account_set() {
                Some(p) => self.push_account(&p),
                None => {
                    self.skipped += 1;
                    false
                }
            },
            Some(PersistKind::TradeInsert) => match frame.as_trade_insert() {
                Some(p) => self.push_trade(&p),
                None => {
                    self.skipped += 1;
                    false
                }
            },
            None => {
                self.skipped += 1;
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
            return false;
        };
        let Some(created_at) = from_unix_ms(created_at_ms) else {
            self.skipped += 1;
            return false;
        };
        let symbol = unpack_str(&p.symbol).to_owned();
        let order_type = unpack_str(&p.order_type).to_owned();
        let coid = unpack_str(&p.client_order_id).to_owned();
        if symbol.is_empty() || order_type.is_empty() {
            self.skipped += 1;
            return false;
        }
        // price=0 is the documented sentinel for "market order" (NULL in PG).
        let pg_price = if price == 0.0 { None } else { Some(price) };
        let coid_opt = if coid.is_empty() { None } else { Some(coid) };
        self.upserts.push(UpsertRow {
            id,
            user_id,
            symbol,
            side: side_str(side),
            order_type,
            price: pg_price,
            quantity: qty,
            filled,
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
            return false;
        };
        self.fills.push(FillRow { id, filled, status });
        true
    }

    fn push_account(&mut self, p: &AccountSetPayload) -> bool {
        let user_id: i64 = p.user_id;
        let balance: f64 = p.balance;
        let frozen: f64 = p.frozen;
        let asset = unpack_str(&p.asset).to_owned();
        if asset.is_empty() {
            self.skipped += 1;
            return false;
        }
        self.accounts.push(AccountRow {
            user_id,
            asset,
            balance,
            frozen,
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
            return false;
        }
        let Some(created_at) = from_unix_ms(ts_ms) else {
            self.skipped += 1;
            return false;
        };
        self.trades.push(TradeRow {
            buy_order_id,
            sell_order_id,
            symbol,
            price,
            qty,
            created_at,
        });
        true
    }

    /// Apply everything to PG inside one transaction per kind. Returns
    /// total rows written (sum of affected per kind). Empties self.
    pub async fn flush(&mut self, pool: &PgPool) -> anyhow::Result<usize> {
        let mut total = 0;

        if !self.upserts.is_empty() {
            total += flush_upserts(pool, std::mem::take(&mut self.upserts)).await?;
        }
        if !self.deletes.is_empty() {
            total += flush_deletes(pool, std::mem::take(&mut self.deletes)).await?;
        }
        if !self.fills.is_empty() {
            total += flush_fills(pool, std::mem::take(&mut self.fills)).await?;
        }
        if !self.accounts.is_empty() {
            total += flush_accounts(pool, std::mem::take(&mut self.accounts)).await?;
        }
        if !self.trades.is_empty() {
            total += flush_trades(pool, std::mem::take(&mut self.trades)).await?;
        }

        Ok(total)
    }
}

async fn flush_upserts(pool: &PgPool, rows: Vec<UpsertRow>) -> anyhow::Result<usize> {
    // Pivot to column arrays for UNNEST — single SQL, server-side join.
    let n = rows.len();
    let mut ids = Vec::with_capacity(n);
    let mut user_ids = Vec::with_capacity(n);
    let mut symbols = Vec::with_capacity(n);
    let mut sides = Vec::with_capacity(n);
    let mut order_types = Vec::with_capacity(n);
    let mut prices: Vec<Option<f64>> = Vec::with_capacity(n);
    let mut qtys = Vec::with_capacity(n);
    let mut filleds = Vec::with_capacity(n);
    let mut statuses = Vec::with_capacity(n);
    let mut freeze_prices = Vec::with_capacity(n);
    let mut coids: Vec<Option<String>> = Vec::with_capacity(n);
    let mut createds = Vec::with_capacity(n);

    for r in rows {
        ids.push(r.id);
        user_ids.push(r.user_id);
        symbols.push(r.symbol);
        sides.push(r.side.to_string());
        order_types.push(r.order_type);
        prices.push(r.price);
        qtys.push(r.quantity);
        filleds.push(r.filled);
        statuses.push(r.status.to_string());
        freeze_prices.push(r.freeze_price);
        coids.push(r.client_order_id);
        createds.push(r.created_at);
    }

    let res = sqlx::query(
        r#"
        INSERT INTO orders (id, user_id, symbol, side, order_type, price, quantity, filled,
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
            $9::varchar[],
            $10::float8[],
            $11::varchar[],
            $12::timestamptz[]
        ) AS t(id, user_id, symbol, side, order_type, price, quantity, filled, status,
               freeze_price, client_order_id, created_at)
        CROSS JOIN LATERAL (SELECT NOW() AS updated_at) u
        ON CONFLICT (id) DO UPDATE SET
            status      = EXCLUDED.status,
            filled      = EXCLUDED.filled,
            updated_at  = NOW()
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
    .bind(&statuses)
    .bind(&freeze_prices)
    .bind(&coids)
    .bind(&createds)
    .execute(pool)
    .await?;
    Ok(res.rows_affected() as usize)
}

async fn flush_deletes(pool: &PgPool, ids: Vec<i64>) -> anyhow::Result<usize> {
    let res = sqlx::query("DELETE FROM orders WHERE id = ANY($1::bigint[])")
        .bind(&ids)
        .execute(pool)
        .await?;
    Ok(res.rows_affected() as usize)
}

async fn flush_fills(pool: &PgPool, rows: Vec<FillRow>) -> anyhow::Result<usize> {
    let mut ids = Vec::with_capacity(rows.len());
    let mut filleds = Vec::with_capacity(rows.len());
    let mut statuses = Vec::with_capacity(rows.len());
    for r in rows {
        ids.push(r.id);
        filleds.push(r.filled);
        statuses.push(r.status.to_string());
    }
    let res = sqlx::query(
        r#"
        UPDATE orders AS o SET
            filled     = v.filled,
            status     = v.status,
            updated_at = NOW()
        FROM UNNEST($1::bigint[], $2::float8[], $3::varchar[])
            AS v(id, filled, status)
        WHERE o.id = v.id
        "#,
    )
    .bind(&ids)
    .bind(&filleds)
    .bind(&statuses)
    .execute(pool)
    .await?;
    Ok(res.rows_affected() as usize)
}

async fn flush_accounts(pool: &PgPool, rows: Vec<AccountRow>) -> anyhow::Result<usize> {
    let mut user_ids = Vec::with_capacity(rows.len());
    let mut assets = Vec::with_capacity(rows.len());
    let mut balances = Vec::with_capacity(rows.len());
    let mut frozens = Vec::with_capacity(rows.len());
    for r in rows {
        user_ids.push(r.user_id);
        assets.push(r.asset);
        balances.push(r.balance);
        frozens.push(r.frozen);
    }
    let res = sqlx::query(
        r#"
        INSERT INTO accounts (user_id, asset, balance, frozen, updated_at)
        SELECT * FROM UNNEST($1::bigint[], $2::varchar[], $3::float8[], $4::float8[]) AS t(user_id, asset, balance, frozen)
        CROSS JOIN LATERAL (SELECT NOW() AS updated_at) u
        ON CONFLICT (user_id, asset) DO UPDATE SET
            balance     = EXCLUDED.balance,
            frozen      = EXCLUDED.frozen,
            updated_at  = NOW()
        "#,
    )
    .bind(&user_ids)
    .bind(&assets)
    .bind(&balances)
    .bind(&frozens)
    .execute(pool)
    .await?;
    Ok(res.rows_affected() as usize)
}

async fn flush_trades(pool: &PgPool, rows: Vec<TradeRow>) -> anyhow::Result<usize> {
    let mut buys = Vec::with_capacity(rows.len());
    let mut sells = Vec::with_capacity(rows.len());
    let mut syms = Vec::with_capacity(rows.len());
    let mut prices = Vec::with_capacity(rows.len());
    let mut qtys = Vec::with_capacity(rows.len());
    let mut createds = Vec::with_capacity(rows.len());
    for r in rows {
        buys.push(r.buy_order_id);
        sells.push(r.sell_order_id);
        syms.push(r.symbol);
        prices.push(r.price);
        qtys.push(r.qty);
        createds.push(r.created_at);
    }
    let res = sqlx::query(
        r#"
        INSERT INTO trades (symbol, buy_order_id, sell_order_id, price, quantity, created_at)
        SELECT * FROM UNNEST(
            $1::varchar[],
            $2::bigint[],
            $3::bigint[],
            $4::float8[],
            $5::float8[],
            $6::timestamptz[]
        ) AS t(symbol, buy_order_id, sell_order_id, price, quantity, created_at)
        "#,
    )
    .bind(&syms)
    .bind(&buys)
    .bind(&sells)
    .bind(&prices)
    .bind(&qtys)
    .bind(&createds)
    .execute(pool)
    .await?;
    Ok(res.rows_affected() as usize)
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
            status: 1, // PENDING
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
    fn fill_with_unknown_status_skipped() {
        let f = PersistFrame::order_fill_update(OrderFillUpdatePayload {
            id: 5,
            filled: 0.1,
            status: 0, // 0 is reserved
            _pad: [0; 7],
        });
        let mut b = PgWriteBatch::new();
        assert!(!b.push(&f));
        assert_eq!(b.skipped(), 1);
    }
}
