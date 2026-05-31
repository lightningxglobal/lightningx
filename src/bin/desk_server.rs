use aeron_wrapper::AeronClient;
use dashmap::DashMap;
use lightning_exchange::{
    aeron_channels::{
        aeron_dir, depth_channel, order_update_channel, orders_channel, orders_stream_for_symbol,
        trade_channel, DEPTH50_STREAM, DEPTH_STREAM, LEVEL2_STREAM, METRICS_CHANNEL,
        METRICS_STREAM, ORDER_UPDATE_STREAM, PERSIST_CHANNEL, PERSIST_STREAM, TRADE_STREAM,
    },
    aeron_transport::{
        DeskDepthSubscriber, DeskOrderPublisher, DeskOrderUpdateSubscriber, DeskTradeSubscriber,
        PersistPublisher,
    },
    transport::persist_event::{
        pack_str, AccountSetPayload, OrderDeletePayload, OrderFillUpdatePayload,
        OrderUpsertPayload, PersistFrame, TradeInsertPayload,
    },
    api::{router, AccountCache, AppState},
    db,
    order_state::{
        db_status_from_update_kind, maker_ws_status_from_db_status, ws_status_from_update_kind,
        DbOrderStatus,
    },
    tracer::{
        spawn_tracer, DESK_INSTANCE_ID, MS_AERON_ORDER_SEND, MS_AERON_UPDATE_RECV,
        MS_WS_UPDATE_SEND,
    },
    transport::AeronCmd,
    ws_handler::market_data_broadcaster,
};
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tower_http::cors::{Any, CorsLayer};

// ── DB status byte constants ──────────────────────────────────────────────────
mod db_cmd {
    use lightning_exchange::DbOrderStatus;

    pub fn status_str(status: u8) -> &'static str {
        match status {
            0 => DbOrderStatus::Pending.as_str(),
            1 => DbOrderStatus::Trading.as_str(),
            2 => DbOrderStatus::Completed.as_str(),
            3 => DbOrderStatus::Canceled.as_str(),
            _ => DbOrderStatus::Rejected.as_str(),
        }
    }

    pub fn str_bytes<const N: usize>(s: &str) -> [u8; N] {
        let mut buf = [0u8; N];
        let b = s.as_bytes();
        let n = b.len().min(N);
        buf[..n].copy_from_slice(&b[..n]);
        buf
    }
}

// ── DbCmd: fixed-size Copy type for rtrb spin thread → DB worker ─────────────
/// No heap allocation; zero Arc clones on the hot path.

/// One fill carried in a batched settlement cmd. POD/Copy for rtrb.
#[derive(Clone, Copy)]
struct SettleTradeEntry {
    taker_id: i64,
    maker_id: i64,
    taker_uid: i64, // 0 → DB worker resolves via SELECT
    maker_uid: i64,
    price: f64,
    qty: f64,
    side: u8, // 0=buy taker, 1=sell taker
    symbol: [u8; 16],
}

/// One row in a batched `INSERT INTO orders` cmd. Kept POD/Copy for rtrb.
#[derive(Clone, Copy)]
struct OrderInsertEntry {
    id: i64,
    user_id: i64,
    symbol: [u8; 16],
    side: u8, // 0=buy 1=sell
    order_type: [u8; 16],
    price: f64,
    qty: f64,
    filled: f64,
    status: u8, // DbOrderStatus as u8
    freeze_price: f64,
    do_freeze: bool,
    client_order_id: [u8; 32], // null-padded, max 31 chars
}

#[derive(Clone, Copy)]
enum DbCmd {
    /// INSERT INTO orders … ON CONFLICT DO UPDATE.
    /// do_freeze=true → also call freeze_for_buy/sell after INSERT (GTC ACCEPTED path).
    UpsertOrder {
        id: i64,
        user_id: i64,
        symbol: [u8; 16],
        side: u8, // 0=buy 1=sell
        order_type: [u8; 16],
        price: f64,
        qty: f64,
        filled: f64,
        status: u8, // DbOrderStatus as u8
        freeze_price: f64,
        do_freeze: bool,
        client_order_id: [u8; 32], // null-padded, max 31 chars
    },
    /// Engine-confirmed ACCEPTEDs (batched). Single multi-row INSERT +
    /// grouped UPDATE accounts per (user_id, asset). ~11× faster locally
    /// than per-id sequential INSERT + freeze (examples/bench_upsert_order).
    BatchUpsertOrder { entries: [OrderInsertEntry; 64], count: u8 },
    /// UPDATE orders SET status, filled WHERE id  (REST / subsequent update path).
    UpdateStatus { id: i64, status: u8, filled: f64 },
    /// Engine-confirmed cancels (batched). Single SQL DELETE ... RETURNING +
    /// grouped UPDATE accounts per (user_id, asset). ~17× faster on EC2
    /// than per-id sequential SELECT/UPDATE/DELETE on a 20-id MM cycle
    /// (examples/bench_cancel_confirm). Works equally for count=1.
    /// Fixed-array + count to keep DbCmd `Copy` (rtrb requirement).
    BatchCancelConfirmed { ids: [i64; 64], count: u8 },
    /// Release a pre-engine reservation for an order that never became active.
    ReleaseReservation {
        user_id: i64,
        symbol: [u8; 16],
        side: u8,
        qty: f64,
        freeze_price: f64,
    },
    /// Batched settlement for N fills emitted in the same engine burst.
    /// One txn: multi-row INSERT trades + UPDATE orders FROM (VALUES) +
    /// grouped UPDATE accounts. ~5× faster locally at N=5; ~10× at N=20
    /// (examples/bench_settle_trade).
    BatchSettleTrade {
        entries: [SettleTradeEntry; 64],
        count: u8,
    },
    /// Batched DeleteOrder — collapses N FILLED/REJECTED events from one
    /// engine burst into a single `DELETE … WHERE id = ANY($1)`. ~9×
    /// faster locally (examples/bench_delete_order). Works equally for
    /// count=1. Fixed-array + count to keep DbCmd `Copy`.
    BatchDeleteOrder { ids: [i64; 64], count: u8 },
}

#[derive(Clone, Copy)]
struct OrderRuntimeMeta {
    user_id: i64,
}

fn remember_runtime_order(cache: &mut HashMap<u64, OrderRuntimeMeta>, order_id: u64, user_id: i64) {
    cache.insert(order_id, OrderRuntimeMeta { user_id });
}

fn remap_runtime_order_id(cache: &mut HashMap<u64, OrderRuntimeMeta>, from: u64, to: u64) {
    if from != to {
        if let Some(meta) = cache.remove(&from) {
            cache.insert(to, meta);
        }
    }
}

fn runtime_user_id(cache: &HashMap<u64, OrderRuntimeMeta>, order_id: u64) -> i64 {
    cache.get(&order_id).map(|m| m.user_id).unwrap_or(0)
}

fn remove_runtime_order(cache: &mut HashMap<u64, OrderRuntimeMeta>, order_id: u64, client_id: u64) {
    cache.remove(&order_id);
    if order_id != client_id {
        cache.remove(&client_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_meta_survives_until_terminal_remove() {
        let mut cache = HashMap::new();
        remember_runtime_order(&mut cache, 10, 7);

        assert_eq!(runtime_user_id(&cache, 10), 7);
        assert_eq!(runtime_user_id(&cache, 11), 0);

        remove_runtime_order(&mut cache, 10, 10);
        assert_eq!(runtime_user_id(&cache, 10), 0);
    }

    #[test]
    fn runtime_meta_remaps_client_id_to_engine_order_id() {
        let mut cache = HashMap::new();
        remember_runtime_order(&mut cache, 10, 7);

        remap_runtime_order_id(&mut cache, 10, 99);

        assert_eq!(runtime_user_id(&cache, 10), 0);
        assert_eq!(runtime_user_id(&cache, 99), 7);

        remove_runtime_order(&mut cache, 99, 10);
        assert_eq!(runtime_user_id(&cache, 99), 0);
    }
}

fn push_db_cmd(db_tx: &mut rtrb::Producer<DbCmd>, mut cmd: DbCmd, context: &'static str) {
    let mut attempts: u64 = 0;
    loop {
        match db_tx.push(cmd) {
            Ok(()) => return,
            Err(rtrb::PushError::Full(returned)) => {
                cmd = returned;
                attempts += 1;
                if attempts == 1 || attempts % 10_000 == 0 {
                    tracing::warn!(
                        "DB command ring full while pushing {context}; retrying (attempt={attempts})"
                    );
                }
                if attempts % 1_024 == 0 {
                    std::thread::yield_now();
                } else {
                    std::hint::spin_loop();
                }
            }
        }
    }
}

// ── process_db_cmd: runs in the tokio runtime, off the spin thread ────────────
/// Lock-and-publish helper for PersistFrames. Lock is held for the duration
/// of a single Aeron `publish` (~µs).
fn publish_frame(
    pp: &std::sync::Arc<parking_lot::Mutex<PersistPublisher>>,
    frame: &PersistFrame,
) {
    let _ = pp.lock().publish(frame);
}

async fn process_db_cmd(
    cmd: DbCmd,
    db: std::sync::Arc<sqlx::PgPool>,
    account_cache: lightning_exchange::api::AccountCache,
    user_tx: std::sync::Arc<dashmap::DashMap<i64, tokio::sync::mpsc::Sender<String>>>,
    // Trade WS broadcast + last_trade_price update moved to the spin thread
    // for lower latency. DB worker no longer reads either; kept to avoid a
    // wider signature refactor.
    _market_tx: std::sync::Arc<tokio::sync::broadcast::Sender<String>>,
    _last_trade_price: std::sync::Arc<dashmap::DashMap<String, f64>>,
    persist_pub: std::sync::Arc<parking_lot::Mutex<PersistPublisher>>,
    aeron_cancel_tx: Option<tokio::sync::mpsc::UnboundedSender<AeronCmd>>,
) {
    use lightning_exchange::account_repository::AccountRepository;
    match cmd {
        DbCmd::UpsertOrder {
            id,
            user_id,
            symbol,
            side,
            order_type,
            price,
            qty,
            filled,
            status,
            freeze_price,
            do_freeze,
            client_order_id,
        } => {
            let sym_end = symbol.iter().position(|&b| b == 0).unwrap_or(16);
            let ot_end = order_type.iter().position(|&b| b == 0).unwrap_or(16);
            let coid_end = client_order_id.iter().position(|&b| b == 0).unwrap_or(32);
            let sym_str = std::str::from_utf8(&symbol[..sym_end]).unwrap_or("BTC_USDT");
            let ot_str = std::str::from_utf8(&order_type[..ot_end]).unwrap_or("limit");
            let coid_str = std::str::from_utf8(&client_order_id[..coid_end])
                .ok()
                .filter(|s| !s.is_empty())
                .map(|s| s.to_owned());
            let side_str = if side == 0 { "buy" } else { "sell" };
            let status_str = db_cmd::status_str(status);

            // INSERT (or UPDATE on id conflict). If the caller-supplied
            // client_order_id collides with another row of the same user
            // (UNIQUE (user_id, client_order_id) WHERE client_order_id IS NOT NULL),
            // retry once with client_order_id=NULL — losing the coid annotation
            // is acceptable; silently dropping the entire order row is not.
            if let Err(e) = sqlx::query(
                "INSERT INTO orders \
                 (id, user_id, symbol, side, order_type, price, quantity, filled, status, freeze_price, client_order_id) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) \
                 ON CONFLICT (id) DO UPDATE SET status=$9, filled=$8, updated_at=NOW()",
            )
            .bind(id).bind(user_id).bind(sym_str)
            .bind(side_str).bind(ot_str)
            .bind(price).bind(qty).bind(filled).bind(status_str).bind(freeze_price)
            .bind(coid_str.clone())
            .execute(db.as_ref()).await
            {
                let is_coid_conflict = e.as_database_error()
                    .and_then(|de| de.constraint())
                    .map(|c| c.contains("client_order_id"))
                    .unwrap_or(false);
                if is_coid_conflict {
                    if let Err(e2) = sqlx::query(
                        "INSERT INTO orders \
                         (id, user_id, symbol, side, order_type, price, quantity, filled, status, freeze_price, client_order_id) \
                         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NULL) \
                         ON CONFLICT (id) DO UPDATE SET status=$9, filled=$8, updated_at=NOW()",
                    )
                    .bind(id).bind(user_id).bind(sym_str)
                    .bind(side_str).bind(ot_str)
                    .bind(price).bind(qty).bind(filled).bind(status_str).bind(freeze_price)
                    .execute(db.as_ref()).await
                    {
                        tracing::error!("orders insert failed (retry without coid) id={id} user={user_id}: {e2}");
                    }
                } else {
                    tracing::error!("orders insert failed id={id} user={user_id}: {e}");
                }
            }

            if do_freeze {
                let repo = AccountRepository::new(&db);
                let parts: Vec<&str> = sym_str.splitn(2, '_').collect();
                let base = parts.first().copied().unwrap_or("BTC");
                let quote = parts.last().copied().unwrap_or("USDT");
                let freeze_ok = if side == 0 {
                    // buy: freeze quote
                    let amount = freeze_price * qty;
                    if amount > 0.0 {
                        match repo.freeze_for_buy(user_id, quote, amount).await {
                            Ok((bal, frz)) => {
                                account_cache
                                    .entry(user_id)
                                    .or_insert_with(HashMap::new)
                                    .insert(quote.to_string(), (bal, frz));
                                true
                            }
                            Err(_) => false,
                        }
                    } else {
                        true
                    }
                } else {
                    // sell: freeze base
                    match repo.freeze_for_sell(user_id, base, qty).await {
                        Ok((bal, frz)) => {
                            account_cache
                                .entry(user_id)
                                .or_insert_with(HashMap::new)
                                .insert(base.to_string(), (bal, frz));
                            true
                        }
                        Err(_) => false,
                    }
                };

                if !freeze_ok {
                    // Insufficient funds: cancel the resting order so it doesn't
                    // fill without backing funds. Update DB, notify user, and tell
                    // the engine to remove it from the book.
                    tracing::warn!("freeze failed for order {id} user {user_id} — cancelling");
                    let _ = sqlx::query(
                        "UPDATE orders SET status='CANCELED', updated_at=NOW() WHERE id=$1",
                    )
                    .bind(id)
                    .execute(db.as_ref())
                    .await;
                    let ts = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_micros() as u64)
                        .unwrap_or(0);
                    if let Some(tx) = user_tx.get(&(user_id as u64 as i64)) {
                        let _ = tx.try_send(
                            serde_json::json!({
                                "type": "order_update",
                                "order_id": id,
                                "status": "CANCELED",
                                "reason": "insufficient_balance",
                                "ts": ts,
                            })
                            .to_string(),
                        );
                    }
                    if let Some(ref tx) = aeron_cancel_tx {
                        let _ = tx.send(AeronCmd::Cancel(
                            lightning_exchange::sbe::CancelOrderRequest {
                                order_id: id as u64,
                                participant_id: user_id as u64,
                            },
                        ));
                    }
                }
            }
        }

        DbCmd::BatchUpsertOrder { entries, count } => {
            // Multi-row INSERT + grouped UPDATE accounts. ~11× local /
            // expected ~15-17× on EC2 vs per-id UpsertOrder (see
            // examples/bench_upsert_order). All entries assumed do_freeze=true
            // (the only caller is the MM-batch ACCEPTED path).
            let count = count as usize;
            if count == 0 {
                return;
            }
            let entries = &entries[..count];

            // 1) Multi-row INSERT.
            let mut sql = String::with_capacity(256 + count * 80);
            sql.push_str(
                "INSERT INTO orders \
                 (id, user_id, symbol, side, order_type, price, quantity, filled, status, freeze_price, client_order_id) \
                 VALUES ",
            );
            for i in 0..count {
                let b = i * 11;
                if i > 0 {
                    sql.push(',');
                }
                use std::fmt::Write;
                let _ = write!(
                    sql,
                    "(${},${},${},${},${},${},${},${},${},${},${})",
                    b + 1, b + 2, b + 3, b + 4, b + 5, b + 6, b + 7, b + 8, b + 9, b + 10, b + 11,
                );
            }
            sql.push_str(
                " ON CONFLICT (id) DO UPDATE \
                 SET status=EXCLUDED.status, filled=EXCLUDED.filled, updated_at=NOW()",
            );

            let mut q = sqlx::query(&sql);
            // Hold owned strings alive until the query is executed.
            let mut owned_syms: smallvec::SmallVec<[String; 64]> =
                smallvec::SmallVec::with_capacity(count);
            let mut owned_ots: smallvec::SmallVec<[String; 64]> =
                smallvec::SmallVec::with_capacity(count);
            let mut owned_coids: smallvec::SmallVec<[Option<String>; 64]> =
                smallvec::SmallVec::with_capacity(count);
            for e in entries {
                let sym_end = e.symbol.iter().position(|&b| b == 0).unwrap_or(16);
                let ot_end = e.order_type.iter().position(|&b| b == 0).unwrap_or(16);
                let coid_end = e.client_order_id.iter().position(|&b| b == 0).unwrap_or(32);
                owned_syms.push(
                    std::str::from_utf8(&e.symbol[..sym_end])
                        .unwrap_or("BTC_USDT")
                        .to_owned(),
                );
                owned_ots.push(
                    std::str::from_utf8(&e.order_type[..ot_end])
                        .unwrap_or("limit")
                        .to_owned(),
                );
                let coid = std::str::from_utf8(&e.client_order_id[..coid_end])
                    .ok()
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_owned());
                owned_coids.push(coid);
            }
            for (i, e) in entries.iter().enumerate() {
                q = q
                    .bind(e.id)
                    .bind(e.user_id)
                    .bind(&owned_syms[i])
                    .bind(if e.side == 0 { "buy" } else { "sell" })
                    .bind(&owned_ots[i])
                    .bind(e.price)
                    .bind(e.qty)
                    .bind(e.filled)
                    .bind(db_cmd::status_str(e.status))
                    .bind(e.freeze_price)
                    .bind(owned_coids[i].clone());
            }
            if let Err(err) = q.execute(db.as_ref()).await {
                tracing::error!("batch orders INSERT failed (count={count}): {err}");
                // Fallback: try per-id INSERTs so a single bad row doesn't
                // sink the whole batch (e.g. coid UNIQUE conflict from a
                // legacy reused client_order_id).
                for e in entries {
                    let _ = sqlx::query(
                        "INSERT INTO orders \
                         (id, user_id, symbol, side, order_type, price, quantity, filled, status, freeze_price, client_order_id) \
                         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NULL) \
                         ON CONFLICT (id) DO UPDATE SET status=$9, filled=$8, updated_at=NOW()",
                    )
                    .bind(e.id)
                    .bind(e.user_id)
                    .bind(
                        std::str::from_utf8(
                            &e.symbol[..e.symbol.iter().position(|&b| b == 0).unwrap_or(16)],
                        )
                        .unwrap_or("BTC_USDT"),
                    )
                    .bind(if e.side == 0 { "buy" } else { "sell" })
                    .bind(
                        std::str::from_utf8(
                            &e.order_type[..e.order_type.iter().position(|&b| b == 0).unwrap_or(16)],
                        )
                        .unwrap_or("limit"),
                    )
                    .bind(e.price)
                    .bind(e.qty)
                    .bind(e.filled)
                    .bind(db_cmd::status_str(e.status))
                    .bind(e.freeze_price)
                    .execute(db.as_ref())
                    .await;
                }
            }

            // 2) Group freezes by (user_id, asset) and UPDATE accounts per group.
            // All entries from MM ACCEPTED carry do_freeze=true.
            let mut freezes: HashMap<(i64, String), f64> = HashMap::new();
            for e in entries {
                if !e.do_freeze {
                    continue;
                }
                let sym_end = e.symbol.iter().position(|&b| b == 0).unwrap_or(16);
                let sym = std::str::from_utf8(&e.symbol[..sym_end]).unwrap_or("BTC_USDT");
                let parts: Vec<&str> = sym.splitn(2, '_').collect();
                let base = parts.first().copied().unwrap_or("BTC");
                let quote = parts.last().copied().unwrap_or("USDT");
                let (asset, amount) = if e.side == 0 {
                    (quote.to_string(), e.freeze_price * e.qty)
                } else {
                    (base.to_string(), e.qty)
                };
                if amount > 0.0 {
                    *freezes.entry((e.user_id, asset)).or_insert(0.0) += amount;
                }
            }
            for ((uid, asset), amount) in freezes {
                let updated: Option<(f64, f64)> = sqlx::query_as(
                    "UPDATE accounts \
                     SET frozen = frozen + $1, updated_at = NOW() \
                     WHERE user_id=$2 AND asset=$3 \
                     RETURNING balance, frozen",
                )
                .bind(amount)
                .bind(uid)
                .bind(&asset)
                .fetch_optional(db.as_ref())
                .await
                .unwrap_or(None);
                if let Some((bal, frz)) = updated {
                    account_cache
                        .entry(uid)
                        .or_insert_with(HashMap::new)
                        .insert(asset.clone(), (bal, frz));
                    // PR2 dual-write: publish AccountSet for redis-writer.
                    publish_frame(
                        &persist_pub,
                        &PersistFrame::account_set(AccountSetPayload {
                            user_id: uid,
                            asset: pack_str(&asset),
                            balance: bal,
                            frozen: frz,
                        }),
                    );
                    if let Some(tx) = user_tx.get(&uid) {
                        let _ = tx.try_send(
                            serde_json::json!({
                                "type": "balance_update",
                                "asset": asset,
                                "balance": bal,
                                "available": bal - frz,
                                "frozen": frz,
                            })
                            .to_string(),
                        );
                    }
                }
            }

            // PR2 dual-write: publish OrderUpsert per row so redis-writer
            // sees the same active orders that just landed in PG.
            for e in entries {
                publish_frame(
                    &persist_pub,
                    &PersistFrame::order_upsert(OrderUpsertPayload {
                        id: e.id,
                        user_id: e.user_id,
                        symbol: e.symbol,
                        side: e.side,
                        status: e.status,
                        _pad: [0; 6],
                        order_type: e.order_type,
                        price: e.price,
                        qty: e.qty,
                        filled: e.filled,
                        freeze_price: e.freeze_price,
                        client_order_id: e.client_order_id,
                        created_at_ms: chrono::Utc::now().timestamp_millis(),
                    }),
                );
            }
        }

        DbCmd::UpdateStatus { id, status, filled } => {
            let _ =
                sqlx::query("UPDATE orders SET status=$1, filled=$2, updated_at=NOW() WHERE id=$3")
                    .bind(db_cmd::status_str(status))
                    .bind(filled)
                    .bind(id)
                    .execute(db.as_ref())
                    .await;
            // PR2 dual-write: mirror to Redis so REST reads see the updated
            // filled/status instead of the stale PENDING the OrderUpsert
            // first published. OrderFillUpdate preserves price / qty / freeze.
            publish_frame(
                &persist_pub,
                &PersistFrame::order_fill_update(OrderFillUpdatePayload {
                    id,
                    filled,
                    status,
                    _pad: [0; 7],
                }),
            );
        }

        DbCmd::BatchCancelConfirmed { ids, count } => {
            // Single-shot batched cancel-confirm: DELETE ... RETURNING gives
            // back every row's user_id/symbol/side/qty/filled/freeze_price
            // in one round trip, then a single grouped UPDATE per
            // (user_id, asset) releases the frozen funds. ~17× faster on
            // EC2 vs per-id sequential (examples/bench_cancel_confirm).
            let ids_vec: Vec<i64> = ids[..count as usize].to_vec();
            let rows: Vec<(i64, i64, String, String, f64, f64, f64)> = sqlx::query_as(
                "DELETE FROM orders
                 WHERE id = ANY($1) AND status IN ('PENDING','TRADING')
                 RETURNING id, user_id, symbol, side, quantity, filled,
                           COALESCE(freeze_price, COALESCE(price, 0.0))",
            )
            .bind(&ids_vec)
            .fetch_all(db.as_ref())
            .await
            .unwrap_or_default();

            // Group by (user_id, asset). Typical MM bid+ask cancel hits two
            // entries (USDT for bids, base for asks).
            let mut releases: HashMap<(i64, String), f64> = HashMap::new();
            for (_id, uid, symbol, side, qty, filled, freeze_price) in &rows {
                let release_qty = (qty - filled).max(0.0);
                if release_qty <= 0.0 {
                    continue;
                }
                let parts: Vec<&str> = symbol.splitn(2, '_').collect();
                let base = parts.first().copied().unwrap_or("BTC");
                let quote = parts.last().copied().unwrap_or("USDT");
                let (asset, amount) = if side == "sell" {
                    (base.to_string(), release_qty)
                } else {
                    (quote.to_string(), freeze_price * release_qty)
                };
                if amount > 0.0 {
                    *releases.entry((*uid, asset)).or_insert(0.0) += amount;
                }
            }
            for ((uid, asset), amount) in releases {
                let updated: Option<(f64, f64)> = sqlx::query_as(
                    "UPDATE accounts
                     SET balance = balance + $1,
                         frozen  = GREATEST(frozen - $1, 0),
                         updated_at = NOW()
                     WHERE user_id=$2 AND asset=$3
                     RETURNING balance, frozen",
                )
                .bind(amount)
                .bind(uid)
                .bind(&asset)
                .fetch_optional(db.as_ref())
                .await
                .unwrap_or(None);
                if let Some((bal, frz)) = updated {
                    account_cache
                        .entry(uid)
                        .or_insert_with(HashMap::new)
                        .insert(asset.clone(), (bal, frz));
                    // PR2 dual-write: account changed → tell redis-writer.
                    publish_frame(
                        &persist_pub,
                        &PersistFrame::account_set(AccountSetPayload {
                            user_id: uid,
                            asset: pack_str(&asset),
                            balance: bal,
                            frozen: frz,
                        }),
                    );
                    if let Some(tx) = user_tx.get(&uid) {
                        let _ = tx.try_send(
                            serde_json::json!({
                                "type": "balance_update",
                                "asset": asset,
                                "balance": bal,
                                "available": bal - frz,
                                "frozen": frz,
                            })
                            .to_string(),
                        );
                    }
                }
            }
            // PR2 dual-write: orders dropped from PG → drop from Redis too.
            // Publish for EVERY requested id, not only RETURNING rows. When an
            // id was already removed by another path (e.g. BatchSettleTrade's
            // maker UPDATE → COMPLETED → BatchDeleteOrder) the RETURNING set
            // is empty for that id, but the id may still be in Redis from an
            // earlier OrderUpsert. OrderDelete is idempotent in apply_frame,
            // so publishing once per requested id is safe and prevents
            // user_orders / active_orders orphans from accumulating. Without
            // this loop we measured 2000+ orphans after a few hours of
            // continuous MM trading, which inflated REST latency 8×.
            for &id in &ids_vec {
                publish_frame(
                    &persist_pub,
                    &PersistFrame::order_delete(OrderDeletePayload { id }),
                );
            }
        }

        DbCmd::ReleaseReservation {
            user_id,
            symbol,
            side,
            qty,
            freeze_price,
        } => {
            let sym_end = symbol.iter().position(|&b| b == 0).unwrap_or(16);
            let sym_str = std::str::from_utf8(&symbol[..sym_end]).unwrap_or("BTC_USDT");
            let parts: Vec<&str> = sym_str.splitn(2, '_').collect();
            let base = parts.first().copied().unwrap_or("BTC");
            let quote = parts.last().copied().unwrap_or("USDT");
            let repo = AccountRepository::new(&db);
            let release_result = if side == 0 {
                repo.release_frozen(user_id, quote, freeze_price * qty)
                    .await
                    .map(|row| (quote, row))
            } else {
                repo.release_frozen(user_id, base, qty)
                    .await
                    .map(|row| (base, row))
            };
            if let Ok((asset, (bal, frz))) = release_result {
                account_cache
                    .entry(user_id)
                    .or_insert_with(HashMap::new)
                    .insert(asset.to_string(), (bal, frz));
                if let Some(tx) = user_tx.get(&user_id) {
                    let _ = tx.try_send(
                        serde_json::json!({
                            "type": "balance_update",
                            "asset": asset,
                            "balance": bal,
                            "available": bal - frz,
                            "frozen": frz,
                        })
                        .to_string(),
                    );
                }
            }
        }


        DbCmd::BatchDeleteOrder { ids, count } => {
            let count = count as usize;
            if count == 0 {
                return;
            }
            let ids_vec: Vec<i64> = ids[..count].to_vec();
            let _ = sqlx::query("DELETE FROM orders WHERE id = ANY($1)")
                .bind(&ids_vec)
                .execute(db.as_ref())
                .await;
            // PR2 dual-write: tell redis-writer to drop the same ids.
            for &id in &ids_vec {
                publish_frame(
                    &persist_pub,
                    &PersistFrame::order_delete(OrderDeletePayload { id }),
                );
            }
        }

        DbCmd::BatchSettleTrade { entries, count } => {
            let count = count as usize;
            if count == 0 {
                return;
            }
            let entries = &entries[..count];

            // Resolve any missing taker/maker uids in one round-trip.
            let mut need_lookup: Vec<i64> = Vec::new();
            for e in entries {
                if e.taker_uid == 0 { need_lookup.push(e.taker_id); }
                if e.maker_uid == 0 { need_lookup.push(e.maker_id); }
            }
            let mut uid_by_id: HashMap<i64, i64> = HashMap::new();
            if !need_lookup.is_empty() {
                let rows: Vec<(i64, i64)> = sqlx::query_as(
                    "SELECT id, user_id FROM orders WHERE id = ANY($1)",
                )
                .bind(&need_lookup)
                .fetch_all(db.as_ref())
                .await
                .unwrap_or_default();
                for (id, uid) in rows {
                    uid_by_id.insert(id, uid);
                }
            }
            // Apply lookups + build per-fill resolved tuple.
            #[derive(Clone)]
            struct Resolved {
                taker_id: i64,
                maker_id: i64,
                taker_uid: i64,
                maker_uid: i64,
                price: f64,
                qty: f64,
                side: u8,
                symbol: String,
            }
            let mut resolved: Vec<Resolved> = Vec::with_capacity(entries.len());
            for e in entries {
                let taker_uid = if e.taker_uid != 0 {
                    e.taker_uid
                } else {
                    *uid_by_id.get(&e.taker_id).unwrap_or(&0)
                };
                let maker_uid = if e.maker_uid != 0 {
                    e.maker_uid
                } else {
                    *uid_by_id.get(&e.maker_id).unwrap_or(&0)
                };
                if taker_uid == 0 || maker_uid == 0 {
                    // Skip un-resolvable fills — same behaviour as the single
                    // SettleTrade handler, which returns early on uid=0.
                    continue;
                }
                let sym_end = e.symbol.iter().position(|&b| b == 0).unwrap_or(16);
                let symbol = std::str::from_utf8(&e.symbol[..sym_end])
                    .unwrap_or("BTC_USDT")
                    .to_owned();
                resolved.push(Resolved {
                    taker_id: e.taker_id,
                    maker_id: e.maker_id,
                    taker_uid,
                    maker_uid,
                    price: e.price,
                    qty: e.qty,
                    side: e.side,
                    symbol,
                });
            }
            if resolved.is_empty() {
                return;
            }

            // NOTE: trade WS broadcast + last_trade_price update now happen
            // on the SPIN thread, immediately when each trade pops out of
            // trade_sub.poll() — see the trade loop in the Aeron spin
            // thread. Doing it here would duplicate every WS trade event
            // and add ms-scale cross-thread latency to clients.
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_micros() as u64)
                .unwrap_or(0);

            // Single txn for the whole batch.
            let mut txn = match db.begin().await {
                Ok(t) => t,
                Err(e) => {
                    tracing::error!("batch settle txn begin: {e}");
                    return;
                }
            };

            // 1) Multi-row INSERT trades.
            {
                use std::fmt::Write;
                let mut sql = String::from(
                    "INSERT INTO trades (symbol, buy_order_id, sell_order_id, price, quantity, created_at) VALUES ",
                );
                for (i, _r) in resolved.iter().enumerate() {
                    if i > 0 { sql.push(','); }
                    let n = i * 5;
                    let _ = write!(
                        sql,
                        "(${},${},${},${},${},NOW())",
                        n + 1, n + 2, n + 3, n + 4, n + 5
                    );
                }
                let mut q = sqlx::query(&sql);
                for r in &resolved {
                    let (b, s) = if r.side == 0 {
                        (r.taker_id, r.maker_id)
                    } else {
                        (r.maker_id, r.taker_id)
                    };
                    q = q.bind(&r.symbol).bind(b).bind(s).bind(r.price).bind(r.qty);
                }
                if let Err(e) = q.execute(&mut *txn).await {
                    tracing::error!("batch trades INSERT failed (count={}): {e}", resolved.len());
                    let _ = txn.rollback().await;
                    return;
                }
            }

            // 2) Multi-row UPDATE orders for maker fills. Each maker order's
            //    filled grows by qty; status flips to COMPLETED when fully
            //    filled. RETURNING gives us per-id status/filled for the WS
            //    push pass below.
            let maker_updates: Vec<(i64, i64, String, f64)>;
            {
                use std::fmt::Write;
                let mut sql = String::from(
                    "UPDATE orders SET
                       filled = orders.filled + u.delta_qty,
                       status = CASE
                         WHEN orders.quantity - (orders.filled + u.delta_qty) < 1e-9
                         THEN 'COMPLETED' ELSE 'TRADING'
                       END,
                       updated_at = NOW()
                     FROM (VALUES ",
                );
                for (i, _r) in resolved.iter().enumerate() {
                    if i > 0 { sql.push(','); }
                    let n = i * 2;
                    let _ = write!(sql, "(${}::bigint, ${}::float8)", n + 1, n + 2);
                }
                sql.push_str(
                    ") AS u(maker_id, delta_qty) WHERE orders.id = u.maker_id
                     RETURNING orders.id, orders.user_id, orders.status, orders.filled",
                );
                let mut q = sqlx::query_as::<_, (i64, i64, String, f64)>(&sql);
                for r in &resolved {
                    q = q.bind(r.maker_id).bind(r.qty);
                }
                maker_updates = match q.fetch_all(&mut *txn).await {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::error!("batch maker UPDATE failed: {e}");
                        let _ = txn.rollback().await;
                        return;
                    }
                };
            }

            // 3) Group net (balance_delta, frozen_release) per (user_id, asset).
            //   buyer  quote: -cost, release cost
            //   buyer  base : +qty
            //   seller base : -qty,  release qty
            //   seller quote: +cost
            #[derive(Default, Clone, Copy)]
            struct Delta {
                balance: f64,
                frozen_release: f64,
            }
            let mut deltas: HashMap<(i64, String), Delta> = HashMap::new();
            for r in &resolved {
                let parts: Vec<&str> = r.symbol.splitn(2, '_').collect();
                let base = parts.first().copied().unwrap_or("BTC").to_string();
                let quote = parts.last().copied().unwrap_or("USDT").to_string();
                let cost = r.price * r.qty;
                let (buyer_uid, seller_uid) = if r.side == 0 {
                    (r.taker_uid, r.maker_uid)
                } else {
                    (r.maker_uid, r.taker_uid)
                };
                let e = deltas.entry((buyer_uid, quote.clone())).or_default();
                e.balance -= cost;
                e.frozen_release += cost;

                let e = deltas.entry((buyer_uid, base.clone())).or_default();
                e.balance += r.qty;

                let e = deltas.entry((seller_uid, base.clone())).or_default();
                e.balance -= r.qty;
                e.frozen_release += r.qty;

                let e = deltas.entry((seller_uid, quote.clone())).or_default();
                e.balance += cost;
            }
            // 4) Apply each group as a single UPDATE.
            //    Positive balance_delta entries need an UPSERT in case the
            //    user has never held this asset before; negative ones must
            //    plain-UPDATE so the CHECK (balance >= 0) doesn't trip
            //    against a freshly-inserted negative balance.
            let mut updated_accounts: Vec<(i64, String, f64, f64)> =
                Vec::with_capacity(deltas.len());
            for ((uid, asset), d) in deltas {
                let row: Option<(f64, f64)> = if d.balance >= 0.0 {
                    sqlx::query_as(
                        "INSERT INTO accounts (user_id, asset, balance, frozen)
                         VALUES ($1, $2, $3, 0)
                         ON CONFLICT (user_id, asset) DO UPDATE SET
                           balance = accounts.balance + $3,
                           frozen  = GREATEST(accounts.frozen - $4, 0),
                           updated_at = NOW()
                         RETURNING balance, frozen",
                    )
                    .bind(uid)
                    .bind(&asset)
                    .bind(d.balance)
                    .bind(d.frozen_release)
                    .fetch_optional(&mut *txn)
                    .await
                    .unwrap_or(None)
                } else {
                    sqlx::query_as(
                        "UPDATE accounts SET
                           balance = balance + $1,
                           frozen  = GREATEST(frozen - $2, 0),
                           updated_at = NOW()
                         WHERE user_id = $3 AND asset = $4
                         RETURNING balance, frozen",
                    )
                    .bind(d.balance)
                    .bind(d.frozen_release)
                    .bind(uid)
                    .bind(&asset)
                    .fetch_optional(&mut *txn)
                    .await
                    .unwrap_or(None)
                };
                if let Some((bal, frz)) = row {
                    updated_accounts.push((uid, asset, bal, frz));
                }
            }

            if let Err(e) = txn.commit().await {
                tracing::error!("batch settle txn commit: {e}");
                return;
            }

            // 5) Fan-out: WS order_update for each maker, balance_update for
            //    each (uid, asset). Cache mirror update too.
            for (maker_id, maker_uid, new_status, new_filled) in &maker_updates {
                if let Some(tx) = user_tx.get(maker_uid) {
                    let ws_status = maker_ws_status_from_db_status(new_status).as_str();
                    let _ = tx.try_send(
                        serde_json::json!({
                            "type": "order_update",
                            "order_id": maker_id,
                            "status": ws_status,
                            "filled_qty": new_filled,
                            "ts": ts,
                        })
                        .to_string(),
                    );
                }
                // PR2 dual-write: maker order changed (filled grew, maybe
                // completed). Push OrderDelete if completed, else upsert the
                // updated filled value via a partial OrderUpsert.
                if new_status == "COMPLETED" {
                    publish_frame(
                        &persist_pub,
                        &PersistFrame::order_delete(OrderDeletePayload { id: *maker_id }),
                    );
                } else {
                    // Partial fill of an already-known order: only filled
                    // and status change. Use OrderFillUpdate so the writer
                    // doesn't overwrite price/qty/etc with sentinels.
                    publish_frame(
                        &persist_pub,
                        &PersistFrame::order_fill_update(OrderFillUpdatePayload {
                            id: *maker_id,
                            filled: *new_filled,
                            status: DbOrderStatus::Trading.as_u8(),
                            _pad: [0; 7],
                        }),
                    );
                }
            }
            for (uid, asset, bal, frz) in updated_accounts {
                account_cache
                    .entry(uid)
                    .or_insert_with(HashMap::new)
                    .insert(asset.clone(), (bal, frz));
                publish_frame(
                    &persist_pub,
                    &PersistFrame::account_set(AccountSetPayload {
                        user_id: uid,
                        asset: pack_str(&asset),
                        balance: bal,
                        frozen: frz,
                    }),
                );
                if let Some(tx) = user_tx.get(&uid) {
                    let _ = tx.try_send(
                        serde_json::json!({
                            "type": "balance_update",
                            "asset": asset,
                            "balance": bal,
                            "available": bal - frz,
                            "frozen": frz,
                        })
                        .to_string(),
                    );
                }
            }
            // PR2 dual-write: emit TradeInsert for each fill. Redis ignores;
            // pg-writer (future PR) will consume to write the trades table.
            for r in &resolved {
                publish_frame(
                    &persist_pub,
                    &PersistFrame::trade_insert(TradeInsertPayload {
                        buy_order_id: if r.side == 0 { r.taker_id } else { r.maker_id },
                        sell_order_id: if r.side == 0 { r.maker_id } else { r.taker_id },
                        symbol: pack_str(&r.symbol),
                        price: r.price,
                        qty: r.qty,
                        ts_ms: (ts / 1000) as i64,
                    }),
                );
            }
        }
    }
}

// ── spawn_db_worker: drain rtrb DbCmd ring buffer off the spin thread ─────────
fn spawn_db_worker(
    mut db_rx: rtrb::Consumer<DbCmd>,
    rt: tokio::runtime::Handle,
    db: std::sync::Arc<sqlx::PgPool>,
    account_cache: lightning_exchange::api::AccountCache,
    user_tx: std::sync::Arc<dashmap::DashMap<i64, tokio::sync::mpsc::Sender<String>>>,
    market_tx: std::sync::Arc<tokio::sync::broadcast::Sender<String>>,
    last_trade_price: std::sync::Arc<dashmap::DashMap<String, f64>>,
    persist_pub: std::sync::Arc<parking_lot::Mutex<PersistPublisher>>,
    aeron_cancel_tx: Option<tokio::sync::mpsc::UnboundedSender<AeronCmd>>,
) {
    std::thread::Builder::new()
        .name("db-worker".to_string())
        .spawn(move || {
            let mut idle_us: u64 = 0;
            loop {
                let mut did_work = false;
                while let Ok(cmd) = db_rx.pop() {
                    did_work = true;
                    idle_us = 0;
                    let db2 = db.clone();
                    let ac2 = account_cache.clone();
                    let ut2 = user_tx.clone();
                    let mt2 = market_tx.clone();
                    let ltp2 = last_trade_price.clone();
                    let pp2 = persist_pub.clone();
                    let at2 = aeron_cancel_tx.clone();
                    rt.spawn(process_db_cmd(cmd, db2, ac2, ut2, mt2, ltp2, pp2, at2));
                }
                if !did_work {
                    idle_us = (idle_us * 2 + 10).min(200);
                    std::thread::sleep(std::time::Duration::from_micros(idle_us));
                }
            }
        })
        .unwrap();
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://user:password@localhost:5432/mydb".to_string());
    let port = std::env::var("DESK_PORT").unwrap_or_else(|_| "4003".to_string());

    tracing::info!("Connecting to database…");
    let pool = db::create_pool(&database_url).await?;
    db::run_migrations(&pool).await?;
    tracing::info!("Migrations applied");

    // Ensure the market-maker robot account and its API key exist.
    let robot_api_key = std::env::var("ROBOT_API_KEY")
        .unwrap_or_else(|_| "robot_ak_2026_lightningx".to_string());
    match lightning_exchange::desk::user_service::ensure_robot_api_key(
        &pool,
        "robot@lightningx.exchange",
        "robot_secret_2026",
        &robot_api_key,
        "Market Maker Robot",
    ).await {
        Ok(uid) => tracing::info!("Robot account ready (user_id={uid}, api_key={robot_api_key})"),
        Err(e) => tracing::warn!("Robot account setup failed: {e}"),
    }

    // Pre-load all account balances into memory so GET /api/balances never touches DB.
    let account_cache: AccountCache = AccountCache::default();
    {
        let rows: Vec<(i64, String, f64, f64)> =
            sqlx::query_as("SELECT user_id, asset, balance, frozen FROM accounts")
                .fetch_all(&pool)
                .await
                .unwrap_or_default();
        for (uid, asset, bal, frz) in rows {
            account_cache
                .entry(uid)
                .or_insert_with(HashMap::new)
                .insert(asset, (bal, frz));
        }
        tracing::info!("Account cache loaded ({} rows)", account_cache.len());
    }

    let mut open_order_meta: HashMap<u64, OrderRuntimeMeta> = HashMap::new();
    {
        let rows: Vec<(i64, i64)> =
            sqlx::query_as("SELECT id, user_id FROM orders WHERE status IN ('PENDING','TRADING')")
                .fetch_all(&pool)
                .await
                .unwrap_or_default();
        for (id, user_id) in rows {
            open_order_meta.insert(id as u64, OrderRuntimeMeta { user_id });
        }
        tracing::info!(
            "Order runtime cache preloaded ({} open orders)",
            open_order_meta.len()
        );
    }

    // ── Aeron setup ───────────────────────────────────────────────────────────
    let client = Arc::new(
        AeronClient::new(&aeron_dir())
            .map_err(|e| anyhow::anyhow!("Aeron init failed: {:?}", e))?,
    );

    // PR2 dual-write: every DB-worker mutation also publishes the
    // corresponding PersistEvent to the persist Aeron stream, so the
    // (independent) redis-writer consumer keeps Redis L1 in sync.
    let persist_pub = Arc::new(parking_lot::Mutex::new(
        PersistPublisher::new(client.clone(), PERSIST_CHANNEL, PERSIST_STREAM)
            .map_err(|e| anyhow::anyhow!("PersistPublisher: {}", e))?,
    ));

    // Per-symbol order publishers: each symbol routes to its own Aeron stream so the
    // matching threads never share a stream and there is zero HOL blocking between symbols.
    let symbols_env =
        std::env::var("SYMBOLS").unwrap_or_else(|_| "ETH_USDT,BTC_USDT,SOL_USDT".to_string());
    let mut order_pubs: std::collections::HashMap<String, DeskOrderPublisher> = symbols_env
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .map(|sym| {
            let stream = orders_stream_for_symbol(&sym);
            let pub_ = DeskOrderPublisher::new(client.clone(), &orders_channel(), stream)
                .unwrap_or_else(|e| panic!("DeskOrderPublisher({sym}): {e}"));
            (sym, pub_)
        })
        .collect();

    let mut order_update_sub = DeskOrderUpdateSubscriber::new(
        client.clone(),
        &order_update_channel(),
        ORDER_UPDATE_STREAM,
    )
    .map_err(|e| anyhow::anyhow!("DeskOrderUpdateSubscriber: {}", e))?;

    let mut trade_sub = DeskTradeSubscriber::new(client.clone(), &trade_channel(), TRADE_STREAM)
        .map_err(|e| anyhow::anyhow!("DeskTradeSubscriber: {}", e))?;

    let mut depth_sub = DeskDepthSubscriber::new(
        client.clone(),
        &depth_channel(),
        DEPTH_STREAM,
        DEPTH50_STREAM,
        LEVEL2_STREAM,
    )
    .map_err(|e| anyhow::anyhow!("DeskDepthSubscriber: {}", e))?;

    tracing::info!("Aeron subscribers and publisher created");

    // ── Latency tracer (optional — disabled if is not running) ────────
    let tracer = spawn_tracer(
        &aeron_dir(),
        METRICS_CHANNEL,
        METRICS_STREAM,
        DESK_INSTANCE_ID,
    )
    .map(Arc::new);
    if tracer.is_some() {
        tracing::info!(
            "Exchange tracer connected (instance_id={})",
            DESK_INSTANCE_ID
        );
    }

    // ── Command channel: async WS handlers → Aeron spin thread ───────────────
    let (aeron_cmd_tx, mut aeron_cmd_rx) = tokio::sync::mpsc::unbounded_channel::<AeronCmd>();
    // Clone for DB worker so it can cancel orders when fund freeze fails post-ACCEPTED.
    let db_worker_aeron_tx = aeron_cmd_tx.clone();

    // ── Sync next_order_id from DB so WS atomic IDs don't collide ─────────────
    let max_db_id: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(id), 0) FROM orders")
        .fetch_one(&pool)
        .await
        .unwrap_or(0);
    let initial_id = (max_db_id as u64) + 1;

    // ── Shared state ──────────────────────────────────────────────────────────
    let (market_tx, _) = broadcast::channel::<String>(1024);

    let valid_symbols: std::collections::HashSet<String> = order_pubs.keys().cloned().collect();

    // Redis L1: cheap multiplexed conn for REST read fallback. If REDIS_URL is
    // unreachable at boot, leave it as None — handlers transparently fall
    // back to the existing PG path. We do NOT block startup on Redis.
    let redis_conn: Option<redis::aio::MultiplexedConnection> = {
        let url = std::env::var("REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379/0".to_string());
        match redis::Client::open(url.clone()) {
            Ok(client) => match client.get_multiplexed_async_connection().await {
                Ok(mut c) => {
                    match redis::cmd("PING").query_async::<String>(&mut c).await {
                        Ok(_) => {
                            tracing::info!("Redis L1 reader attached: {url}");
                            Some(c)
                        }
                        Err(e) => {
                            tracing::warn!("Redis PING failed at {url}: {e} — REST will fall back to PG");
                            None
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("Redis connect failed at {url}: {e} — REST will fall back to PG");
                    None
                }
            },
            Err(e) => {
                tracing::warn!("Invalid REDIS_URL '{url}': {e} — REST will fall back to PG");
                None
            }
        }
    };

    let state = AppState {
        db: Arc::new(pool),
        engines: None,
        market_tx: Arc::new(market_tx),
        user_tx: Arc::new(DashMap::new()),
        next_order_id: Arc::new(AtomicU64::new(initial_id)),
        aeron_cmd_tx: Some(aeron_cmd_tx),
        pending_meta: Arc::new(DashMap::new()),
        pending_orders: Arc::new(DashMap::new()),
        last_depth: Arc::new(DashMap::new()),
        last_ticker: Arc::new(DashMap::new()),
        last_trade_price: Arc::new(DashMap::new()),
        tracer: tracer.clone(),
        account_cache: account_cache.clone(),
        valid_symbols: Arc::new(valid_symbols),
        redis: redis_conn,
        persist_pub: Some(persist_pub.clone()),
    };

    // ── DB worker: rtrb ring buffer spin thread → DB worker thread ───────────
    let (mut db_tx, db_rx) = rtrb::RingBuffer::<DbCmd>::new(8 * 1024);
    spawn_db_worker(
        db_rx,
        tokio::runtime::Handle::current(),
        state.db.clone(),
        account_cache.clone(),
        state.user_tx.clone(),
        state.market_tx.clone(),
        state.last_trade_price.clone(),
        persist_pub.clone(),
        Some(db_worker_aeron_tx),
    );

    // ── Aeron spin thread: WS command drain + inbound event loop ─────────────
    // order_pub lives here exclusively — no mutex needed.
    {
        let market_tx = state.market_tx.clone();
        let pending_orders = state.pending_orders.clone();
        let pending_meta = state.pending_meta.clone();
        let user_tx = state.user_tx.clone();
        let account_cache = account_cache.clone();
        let last_depth = state.last_depth.clone();
        let last_trade_price = state.last_trade_price.clone();
        let rt = tokio::runtime::Handle::current();
        let spin_tracer = tracer.clone();
        let mut order_meta_cache = open_order_meta;
        // DESK_SPIN=false → exponential backoff (EC2/CPU-constrained hosts).
        // Default: spin_loop() for lowest latency on dedicated cores.
        let use_spin = std::env::var("DESK_SPIN")
            .map(|v| v != "false")
            .unwrap_or(true);

        std::thread::Builder::new()
            .name("aeron-event-loop".to_string())
            .spawn(move || {
                let mut idle_us: u64 = 0;
                // Accumulator for CANCELLED ids observed in one poll burst.
                // Flushed below as one DbCmd::BatchCancelConfirmed — ~17×
                // faster on EC2 than N individual CancelConfirmed for a 20-id
                // MM cycle (examples/bench_cancel_confirm). Fixed array +
                // count to keep DbCmd `Copy` (rtrb requirement).
                let mut cancel_batch: [i64; 64] = [0; 64];
                let mut cancel_count: usize = 0;

                // Same pattern for ACCEPTED — flushed as one
                // DbCmd::BatchUpsertOrder (~11× faster than per-id INSERT;
                // see examples/bench_upsert_order).
                let mut accepted_batch: [OrderInsertEntry; 64] = [OrderInsertEntry {
                    id: 0, user_id: 0, symbol: [0; 16], side: 0,
                    order_type: [0; 16], price: 0.0, qty: 0.0, filled: 0.0,
                    status: 0, freeze_price: 0.0, do_freeze: false,
                    client_order_id: [0; 32],
                }; 64];
                let mut accepted_count: usize = 0;

                // And SettleTrade — flushed as one DbCmd::BatchSettleTrade.
                // Multi-row INSERT + UPDATE orders FROM (VALUES) + grouped
                // UPDATE accounts in a single txn (~5× at N=5, see
                // examples/bench_settle_trade).
                let mut settle_batch: [SettleTradeEntry; 64] = [SettleTradeEntry {
                    taker_id: 0, maker_id: 0, taker_uid: 0, maker_uid: 0,
                    price: 0.0, qty: 0.0, side: 0, symbol: [0; 16],
                }; 64];
                let mut settle_count: usize = 0;

                // FILLED / REJECTED rows deleted in one DELETE…ANY per burst
                // (~9× faster locally; see examples/bench_delete_order).
                let mut delete_batch: [i64; 64] = [0; 64];
                let mut delete_count: usize = 0;
                loop {
                    let mut did_work = false;
                    // Drain outbound commands (WS/REST → engine) without blocking.
                    while let Ok(cmd) = aeron_cmd_rx.try_recv() {
                        did_work = true;
                        match cmd {
                            AeronCmd::NewOrder(req) => {
                                let sym = std::str::from_utf8(&req.symbol)
                                    .unwrap_or("").trim_end_matches('\0');
                                remember_runtime_order(
                                    &mut order_meta_cache,
                                    req.client_order_id,
                                    req.participant_id as i64,
                                );
                                if let Some(pub_) = order_pubs.get_mut(sym) {
                                    let _ = pub_.publish_new_order(&req);
                                } else {
                                    // No engine for this symbol — reject immediately so the
                                    // client doesn't time out and clean up pending_meta.
                                    let coid: u64 = req.client_order_id;
                                    let uid: u64  = req.participant_id;
                                    let ws_meta = pending_meta.remove(&coid).map(|(_, m)| m);
                                    remove_runtime_order(&mut order_meta_cache, coid, coid);
                                    let client_oid = ws_meta.as_ref().map(|m| m.client_order_id.as_str()).unwrap_or("");
                                    if let Some(tx) = user_tx.get(&(uid as i64)) {
                                        let msg = serde_json::json!({
                                            "type": "order_rejected",
                                            "client_order_id": client_oid,
                                            "reason": format!("No engine for symbol: {}", sym),
                                        }).to_string();
                                        let _ = tx.try_send(msg);
                                    }
                                }
                                if let Some(ref t) = spin_tracer {
                                    t.record_sym(MS_AERON_ORDER_SEND, req.client_order_id, &req.symbol);
                                }
                            }
                            AeronCmd::Cancel(req) => {
                                // Cancel doesn't carry a symbol — broadcast to all streams.
                                // Only the engine that owns the order will find it and confirm.
                                for pub_ in order_pubs.values_mut() {
                                    let _ = pub_.publish_cancel(&req);
                                }
                            }
                            AeronCmd::BatchCancel(reqs) => {
                                // Publish all cancels in one tight inner loop per publisher,
                                // avoiding per-cancel channel overhead.
                                for req in &reqs {
                                    for pub_ in order_pubs.values_mut() {
                                        let _ = pub_.publish_cancel(req);
                                    }
                                }
                            }
                            AeronCmd::BatchNewOrder(reqs) => {
                                // Publish all new orders in one tight inner loop so the engine
                                // sees the entire batch before the next depth snapshot (10ms).
                                for req in &reqs {
                                    let sym = std::str::from_utf8(&req.symbol)
                                        .unwrap_or("").trim_end_matches('\0');
                                    remember_runtime_order(
                                        &mut order_meta_cache,
                                        req.client_order_id,
                                        req.participant_id as i64,
                                    );
                                    if let Some(pub_) = order_pubs.get_mut(sym) {
                                        let _ = pub_.publish_new_order(req);
                                    }
                                    if let Some(ref t) = spin_tracer {
                                        t.record_sym(MS_AERON_ORDER_SEND, req.client_order_id, &req.symbol);
                                    }
                                }
                            }
                        }
                    }

                    order_update_sub.do_work();
                    trade_sub.do_work();
                    depth_sub.do_work();

                    // Process trade notifications before terminal order updates remove
                    // runtime metadata. Engine publishes trades before the corresponding
                    // update, and this keeps settlement off the DB lookup path.
                    while let Some(trade) = trade_sub.poll() {
                        did_work = true;
                        let price: f64 = trade.price;
                        let qty: f64 = trade.quantity;
                        let side: u8 = trade.side;
                        let taker_id = trade.taker_order_id as i64;
                        let maker_id = trade.maker_order_id as i64;

                        let taker_uid = runtime_user_id(&order_meta_cache, taker_id as u64);
                        let maker_uid = runtime_user_id(&order_meta_cache, maker_id as u64);

                        let mut sym = [0u8; 16];
                        sym.copy_from_slice(&trade.symbol[..16]);

                        // Broadcast trade WS event + bump last_trade_price right
                        // here on the spin thread, BEFORE handing settlement off
                        // to the DB worker. Previously this lived inside
                        // BatchSettleTrade in the DB worker — which meant trade
                        // WS latency = spin → rtrb → tokio scheduling → DB worker
                        // dispatch (~ms scale). Direct broadcast::Sender::send
                        // is lock-free and doesn't need a tokio runtime, so we
                        // can fan out from the spin thread with a few µs of
                        // overhead. PG/Redis writes still run async in the
                        // DB worker via the settle_batch below.
                        {
                            let sym_end = sym.iter().position(|&b| b == 0).unwrap_or(16);
                            let symbol_str = std::str::from_utf8(&sym[..sym_end])
                                .unwrap_or("BTC_USDT");
                            let side_str = if side == 0 { "buy" } else { "sell" };
                            let ts = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.as_micros() as u64)
                                .unwrap_or(0);
                            // Hand-written JSON; serde_json::json! macro costs
                            // ~600ns per call vs format!'s ~150ns for this shape.
                            let msg = format!(
                                r#"{{"type":"trade","symbol":"{}","price":{},"qty":{},"side":"{}","ts":{}}}"#,
                                symbol_str, price, qty, side_str, ts,
                            );
                            let _ = market_tx.send(msg);
                            last_trade_price.insert(symbol_str.to_string(), price);
                        }

                        // Accumulate into the batched settlement buffer.
                        // Flushed below as a single DbCmd::BatchSettleTrade
                        // after all bursts (trade + order_update) drain.
                        if settle_count >= 64 {
                            push_db_cmd(
                                &mut db_tx,
                                DbCmd::BatchSettleTrade {
                                    entries: settle_batch,
                                    count: settle_count as u8,
                                },
                                "batch settle trade (overflow)",
                            );
                            settle_count = 0;
                        }
                        settle_batch[settle_count] = SettleTradeEntry {
                            taker_id, maker_id, taker_uid, maker_uid,
                            price, qty, side, symbol: sym,
                        };
                        settle_count += 1;
                    }

                    // Process order updates — complete pending REST/WS requests.
                    while let Some(msg) = order_update_sub.poll() {
                        did_work = true;
                        use lightning_exchange::transport::order_update_kind;
                        // Copy packed struct fields to locals to avoid misaligned refs.
                        let order_id: u64 = msg.order_id;
                        let client_order_id: u64 = msg.client_order_id;
                        let participant_id: u64 = msg.participant_id;
                        let fill_qty: f64 = msg.fill_qty;
                        let fill_price: f64 = msg.fill_price;
                        let kind: u8 = msg.kind;
                        if let Some(ref t) = spin_tracer {
                            t.record(MS_AERON_UPDATE_RECV, client_order_id);
                        }

                        // For WS fast-path orders, pending_meta holds the order
                        // details. On ACCEPTED we INSERT the DB row + freeze funds.
                        // On REJECTED/CANCELLED we just drop the meta (no freeze happened).
                        // Cache user_id synchronously before removing from pending_meta
                        // so the trade handler can resolve UIDs without any DB round-trip.
                        //
                        // REJECTED messages have order_id=0 (engine never assigned one);
                        // use client_order_id (= desk's internal order_id) for the lookup.
                        let lookup_id = if kind == order_update_kind::REJECTED { client_order_id } else { order_id };
                        // Route to waiting REST request (pending_orders) if any.
                        if let Some((_, tx)) = pending_orders.remove(&lookup_id) {
                            let _ = tx.send(msg);
                        }
                        if kind == order_update_kind::ACCEPTED {
                            remap_runtime_order_id(
                                &mut order_meta_cache,
                                client_order_id,
                                order_id,
                            );
                        }
                        if let Some(meta_ref) = pending_meta.get(&lookup_id) {
                            if kind != order_update_kind::REJECTED {
                                remember_runtime_order(
                                    &mut order_meta_cache,
                                    order_id,
                                    meta_ref.user_id,
                                );
                            }
                        }
                        let ws_meta = pending_meta.remove(&lookup_id).map(|(_, m)| m);
                        // client_order_id is only available on the first event (ACCEPTED).
                        let ws_client_oid = ws_meta.as_ref().map(|m| m.client_order_id.clone());

                        if let Some(meta) = ws_meta {
                            if kind == order_update_kind::ACCEPTED {
                                // Accumulate into the batched-INSERT buffer.
                                // Flushed below as a single DbCmd::BatchUpsertOrder
                                // after the poll burst ends (~11× faster than
                                // per-id UpsertOrder; see bench_upsert_order).
                                if accepted_count >= 64 {
                                    push_db_cmd(
                                        &mut db_tx,
                                        DbCmd::BatchUpsertOrder {
                                            entries: accepted_batch,
                                            count: accepted_count as u8,
                                        },
                                        "batch upsert order (overflow)",
                                    );
                                    accepted_count = 0;
                                }
                                accepted_batch[accepted_count] = OrderInsertEntry {
                                    id:              order_id as i64,
                                    user_id:         meta.user_id,
                                    symbol:          db_cmd::str_bytes(&meta.symbol),
                                    side:            if meta.side == "buy" { 0 } else { 1 },
                                    order_type:      db_cmd::str_bytes(&meta.order_type),
                                    price:           meta.price.unwrap_or(0.0),
                                    qty:             meta.qty,
                                    filled:          0.0,
                                    status:          DbOrderStatus::Pending.as_u8(),
                                    freeze_price:    meta.freeze_price,
                                    do_freeze:       true,
                                    client_order_id: db_cmd::str_bytes(ws_client_oid.as_deref().unwrap_or("")),
                                };
                                accepted_count += 1;
                            } else if kind == order_update_kind::PARTIAL_FILL {
                                // Order is still resting — UpsertOrder writes the live row.
                                push_db_cmd(&mut db_tx, DbCmd::UpsertOrder {
                                    id:              order_id as i64,
                                    user_id:         meta.user_id,
                                    symbol:          db_cmd::str_bytes(&meta.symbol),
                                    side:            if meta.side == "buy" { 0 } else { 1 },
                                    order_type:      db_cmd::str_bytes(&meta.order_type),
                                    price:           meta.price.unwrap_or(0.0),
                                    qty:             meta.qty,
                                    filled:          fill_qty,
                                    status:          DbOrderStatus::Trading.as_u8(),
                                    freeze_price:    meta.freeze_price,
                                    do_freeze:       false,
                                    client_order_id: db_cmd::str_bytes(ws_client_oid.as_deref().unwrap_or("")),
                                }, "upsert partially-filled order");
                            }
                            // kind == FILLED here means "first event was a full fill"
                            // (market / IOC). Skip INSERT — the order is already terminal,
                            // and trades has no FK to orders so settle is unaffected.
                            if kind == order_update_kind::REJECTED {
                                // Freeze was in-memory only (hot path never hit DB).
                                // Revert the cache and notify; no DB write needed.
                                let sym_parts: Vec<&str> = meta.symbol.splitn(2, '_').collect();
                                let base = sym_parts.first().copied().unwrap_or("BTC");
                                let quote = sym_parts.last().copied().unwrap_or("USDT");
                                let (asset, rel_amount) = if meta.side == "buy" {
                                    (quote.to_string(), meta.freeze_price * meta.qty)
                                } else {
                                    (base.to_string(), meta.qty)
                                };
                                let new_vals = account_cache.get_mut(&meta.user_id).and_then(|mut e| {
                                    let kv = e.get_mut(asset.as_str())?;
                                    kv.1 = (kv.1 - rel_amount).max(0.0);
                                    Some((kv.0, kv.1))
                                });
                                if let Some((bal, frz)) = new_vals {
                                    if let Some(tx) = user_tx.get(&meta.user_id) {
                                        // Hand-written JSON for the spin-thread WS push.
                                        // asset is a short alphanumeric symbol (BTC, USDT, …) —
                                        // no JSON escaping needed.
                                        let _ = tx.try_send(format!(
                                            r#"{{"type":"balance_update","asset":"{}","balance":{},"available":{},"frozen":{}}}"#,
                                            asset, bal, bal - frz, frz,
                                        ));
                                    }
                                }
                            } else if kind == order_update_kind::CANCELLED {
                                push_db_cmd(&mut db_tx, DbCmd::ReleaseReservation {
                                    user_id: meta.user_id,
                                    symbol: db_cmd::str_bytes(&meta.symbol),
                                    side: if meta.side == "buy" { 0 } else { 1 },
                                    qty: meta.qty,
                                    freeze_price: meta.freeze_price,
                                }, "release cancelled reservation");
                            }
                        } else {
                            // REST-path order OR subsequent WS update (row already exists).
                            // Terminal states get dropped from the orders table; only
                            // PARTIAL_FILL (still resting) updates filled/status.
                            if kind == order_update_kind::CANCELLED {
                                // Batched — flushed after this poll burst ends.
                                // If the burst exceeds 64 (one full MM cycle is
                                // 20), flush early and start a new batch.
                                if cancel_count >= 64 {
                                    push_db_cmd(
                                        &mut db_tx,
                                        DbCmd::BatchCancelConfirmed {
                                            ids: cancel_batch,
                                            count: cancel_count as u8,
                                        },
                                        "batch cancel confirmed (overflow)",
                                    );
                                    cancel_count = 0;
                                }
                                cancel_batch[cancel_count] = order_id as i64;
                                cancel_count += 1;
                            } else if kind == order_update_kind::FILLED
                                || kind == order_update_kind::REJECTED
                            {
                                // Batched DELETE — flushed after the burst.
                                if delete_count >= 64 {
                                    push_db_cmd(
                                        &mut db_tx,
                                        DbCmd::BatchDeleteOrder {
                                            ids: delete_batch,
                                            count: delete_count as u8,
                                        },
                                        "batch delete order (overflow)",
                                    );
                                    delete_count = 0;
                                }
                                delete_batch[delete_count] = order_id as i64;
                                delete_count += 1;
                            } else {
                                let status = db_status_from_update_kind(kind).as_u8();
                                push_db_cmd(&mut db_tx, DbCmd::UpdateStatus {
                                    id:     order_id as i64,
                                    status,
                                    filled: fill_qty,
                                }, "update order status");
                            }
                        }

                        // Push order_update to user's personal WS channel.
                        let user_id = participant_id as i64;
                        // Record latency milestone unconditionally — measures when
                        // the desk-server is ready to forward the update, regardless
                        // of whether a WS connection exists for this user.
                        if let Some(ref t) = spin_tracer {
                            t.record(MS_WS_UPDATE_SEND, client_order_id);
                        }
                        // Single DashMap lookup — was previously two (is_none() +
                        // if let Some()), each taking a shard lock.
                        let maybe_tx = user_tx.get(&user_id);
                        if maybe_tx.is_none() {
                            tracing::warn!("no WS channel for user {user_id}, order_update {order_id} lost");
                        }
                        if let Some(tx) = maybe_tx {
                            let ws_status = ws_status_from_update_kind(kind).as_str();
                            let ts = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.as_micros() as u64)
                                .unwrap_or(0);
                            // Hand-written JSON — serde_json::json! macro
                            // measures ~600ns per call; format! is ~150ns
                            // for this fixed shape and runs at every
                            // order_update event (≈ 400/s steady-state).
                            // client_order_id is JSON-escape-safe here: it's
                            // a server-generated session prefix + counter
                            // (see make_place_msg → "{prefix}-{N}"), so no
                            // escaping needed.
                            let upd = match ws_client_oid.as_deref() {
                                Some(coid) => format!(
                                    r#"{{"type":"order_update","order_id":{},"status":"{}","filled_qty":{},"avg_price":{},"ts":{},"client_order_id":"{}"}}"#,
                                    order_id, ws_status, fill_qty, fill_price, ts, coid,
                                ),
                                None => format!(
                                    r#"{{"type":"order_update","order_id":{},"status":"{}","filled_qty":{},"avg_price":{},"ts":{}}}"#,
                                    order_id, ws_status, fill_qty, fill_price, ts,
                                ),
                            };
                            if tx.try_send(upd).is_err() {
                                tracing::warn!("personal channel full for user {user_id}, dropping order_update {order_id}");
                            }
                        }

                        if kind == order_update_kind::FILLED
                            || kind == order_update_kind::CANCELLED
                            || kind == order_update_kind::REJECTED
                        {
                            remove_runtime_order(&mut order_meta_cache, order_id, client_order_id);
                        }
                    }

                    // Flush per-burst trade accumulator as one batched settle.
                    // Multi-row INSERT trades + multi-row UPDATE orders +
                    // grouped UPDATE accounts in one txn. ~5× faster at N=5,
                    // ~10× at N=20 (examples/bench_settle_trade).
                    if settle_count > 0 {
                        push_db_cmd(
                            &mut db_tx,
                            DbCmd::BatchSettleTrade {
                                entries: settle_batch,
                                count: settle_count as u8,
                            },
                            "batch settle trade",
                        );
                        settle_count = 0;
                    }

                    // Flush per-burst ACCEPTED accumulator as one batched
                    // multi-row INSERT + grouped freeze UPDATE. Cuts work for
                    // an MM 20-id place cycle from 40 round-trips (2 per id)
                    // to ~3 total.
                    if accepted_count > 0 {
                        push_db_cmd(
                            &mut db_tx,
                            DbCmd::BatchUpsertOrder {
                                entries: accepted_batch,
                                count: accepted_count as u8,
                            },
                            "batch upsert order",
                        );
                        accepted_count = 0;
                    }

                    // Flush per-burst FILLED/REJECTED DELETE accumulator.
                    // ~9× faster than per-id DELETE (examples/bench_delete_order).
                    if delete_count > 0 {
                        push_db_cmd(
                            &mut db_tx,
                            DbCmd::BatchDeleteOrder {
                                ids: delete_batch,
                                count: delete_count as u8,
                            },
                            "batch delete order",
                        );
                        delete_count = 0;
                    }

                    // Flush the per-burst CANCELLED accumulator as one batched
                    // DB cmd. Cuts DB work for an MM 20-id cancel cycle from
                    // 60 round-trips (3 per id) to ~3 total.
                    if cancel_count > 0 {
                        push_db_cmd(
                            &mut db_tx,
                            DbCmd::BatchCancelConfirmed {
                                ids: cancel_batch,
                                count: cancel_count as u8,
                            },
                            "batch cancel confirmed",
                        );
                        cancel_count = 0;
                    }

                    // Process depth snapshots — spin thread copies raw arrays (~320B memcpy),
                    // offloads JSON build + DashMap + broadcast to tokio (10-30μs saved).
                    while let Some(depth_msg) = depth_sub.poll() {
                        did_work = true;
                        use lightning_exchange::aeron_transport::DeskDepthMsg;
                        match depth_msg {
                            DeskDepthMsg::Depth(evt) => {
                                let nb = evt.num_bids as usize;
                                let na = evt.num_asks as usize;
                                let mut bids_raw = [(0.0f64, 0.0f64); 20];
                                let mut asks_raw = [(0.0f64, 0.0f64); 20];
                                bids_raw[..nb].copy_from_slice(&evt.bids[..nb]);
                                asks_raw[..na].copy_from_slice(&evt.asks[..na]);
                                let end = evt.symbol.iter().position(|&b| b == 0).unwrap_or(16);
                                let symbol = std::str::from_utf8(&evt.symbol[..end])
                                    .unwrap_or("BTC_USDT").to_string();
                                let symbol = if symbol.is_empty() { "BTC_USDT".to_string() } else { symbol };

                                // Empty or one-sided snapshot: silently drop.
                                // The frontend keeps its last rendered state — no need to
                                // broadcast anything. Never send empty depth to clients.
                                if nb == 0 || na == 0 {
                                    continue;
                                }

                                let market_tx2 = market_tx.clone();
                                let last_depth2 = last_depth.clone();
                                rt.spawn(async move {
                                    let bids: Vec<[f64; 2]> = bids_raw[..nb]
                                        .iter().filter(|(_, q)| *q > 0.0)
                                        .map(|&(p, q)| [p, q]).collect();
                                    let asks: Vec<[f64; 2]> = asks_raw[..na]
                                        .iter().filter(|(_, q)| *q > 0.0)
                                        .map(|&(p, q)| [p, q]).collect();
                                    let ts = std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .map(|d| d.as_micros() as u64).unwrap_or(0);
                                    let depth_json = serde_json::json!({
                                        "type": "depth", "symbol": symbol,
                                        "bids": bids, "asks": asks, "ts": ts,
                                    });
                                    last_depth2.insert(symbol, depth_json.clone());
                                    let _ = market_tx2.send(depth_json.to_string());
                                });
                            }
                            DeskDepthMsg::Depth50(_) | DeskDepthMsg::Level2(_) => {}
                        }
                    }

                    if !did_work {
                        if use_spin {
                            std::hint::spin_loop();
                        } else {
                            idle_us = (idle_us * 2 + 10).min(500);
                            std::thread::sleep(std::time::Duration::from_micros(idle_us));
                        }
                    } else {
                        idle_us = 0;
                    }
                }
            })?;
    }

    // ── Periodic market data broadcaster (tickers, klines from DB) ────────────
    tokio::spawn(market_data_broadcaster(state.clone()));

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = router(state).layer(cors);

    let addr = format!("0.0.0.0:{}", port);
    tracing::info!("Desk Server (Aeron) listening on {}", addr);
    let listener = TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
