use aeron_wrapper::AeronClient;
use dashmap::DashMap;
use lightning_exchange::{
    aeron_channels::{
        aeron_dir, depth_channel, order_update_channel, orders_channel, orders_stream_for_symbol,
        trade_channel, DEPTH50_STREAM, DEPTH_STREAM, LEVEL2_STREAM, METRICS_CHANNEL,
        METRICS_STREAM, ORDER_UPDATE_STREAM, TRADE_STREAM,
    },
    aeron_transport::{
        DeskDepthSubscriber, DeskOrderPublisher, DeskOrderUpdateSubscriber, DeskTradeSubscriber,
    },
    api::{router, AccountCache, AppState},
    db,
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
    pub const STATUS_PENDING: u8 = 0;
    pub const STATUS_TRADING: u8 = 1;
    pub const STATUS_COMPLETED: u8 = 2;
    pub const STATUS_CANCELED: u8 = 3;
    pub const STATUS_REJECTED: u8 = 4;

    pub fn status_str(s: u8) -> &'static str {
        match s {
            0 => "PENDING",
            1 => "TRADING",
            2 => "COMPLETED",
            3 => "CANCELED",
            _ => "REJECTED",
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
        status: u8, // db_cmd::STATUS_*
        freeze_price: f64,
        do_freeze: bool,
        client_order_id: [u8; 32], // null-padded, max 31 chars
    },
    /// UPDATE orders SET status, filled WHERE id  (REST / subsequent update path).
    UpdateStatus { id: i64, status: u8, filled: f64 },
    /// Engine-confirmed cancel. Releases frozen funds for the cancelled
    /// unfilled quantity, then marks the order canceled.
    CancelConfirmed { id: i64, cancelled_qty: f64 },
    /// Release a pre-engine reservation for an order that never became active.
    ReleaseReservation {
        user_id: i64,
        symbol: [u8; 16],
        side: u8,
        qty: f64,
        freeze_price: f64,
    },
    /// INSERT trade + settle accounts + push maker WS.
    SettleTrade {
        taker_id: i64,
        maker_id: i64,
        taker_uid: i64, // 0 → DB worker resolves via SELECT
        maker_uid: i64, // 0 → DB worker resolves via SELECT
        price: f64,
        qty: f64,
        side: u8, // 0=buy taker, 1=sell taker
        symbol: [u8; 16],
    },
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
async fn process_db_cmd(
    cmd: DbCmd,
    db: std::sync::Arc<sqlx::PgPool>,
    account_cache: lightning_exchange::api::AccountCache,
    user_tx: std::sync::Arc<dashmap::DashMap<i64, tokio::sync::mpsc::Sender<String>>>,
    market_tx: std::sync::Arc<tokio::sync::broadcast::Sender<String>>,
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

            let _ = sqlx::query(
                "INSERT INTO orders \
                 (id, user_id, symbol, side, order_type, price, quantity, filled, status, freeze_price, client_order_id) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) \
                 ON CONFLICT (id) DO UPDATE SET status=$9, filled=$8, updated_at=NOW()",
            )
            .bind(id).bind(user_id).bind(sym_str)
            .bind(side_str).bind(ot_str)
            .bind(price).bind(qty).bind(filled).bind(status_str).bind(freeze_price)
            .bind(coid_str)
            .execute(db.as_ref()).await;

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
                            },
                        ));
                    }
                }
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
        }

        DbCmd::CancelConfirmed { id, cancelled_qty } => {
            let order: Option<(i64, String, String, f64, f64, f64)> = sqlx::query_as(
                "SELECT user_id, symbol, side, quantity, filled,
                        COALESCE(freeze_price, COALESCE(price, 0.0))
                 FROM orders
                 WHERE id=$1 AND status IN ('PENDING','TRADING')",
            )
            .bind(id)
            .fetch_optional(db.as_ref())
            .await
            .unwrap_or(None);

            if let Some((user_id, symbol, side, quantity, filled, freeze_price)) = order {
                let release_qty = if cancelled_qty > 0.0 {
                    cancelled_qty
                } else {
                    (quantity - filled).max(0.0)
                };
                let repo = AccountRepository::new(&db);
                let parts: Vec<&str> = symbol.splitn(2, '_').collect();
                let base = parts.first().copied().unwrap_or("BTC");
                let quote = parts.last().copied().unwrap_or("USDT");
                let release_result = if side == "sell" {
                    repo.release_frozen(user_id, base, release_qty)
                        .await
                        .map(|row| (base, row))
                } else {
                    repo.release_frozen(user_id, quote, freeze_price * release_qty)
                        .await
                        .map(|row| (quote, row))
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

            let _ = sqlx::query(
                "UPDATE orders SET status='CANCELED', updated_at=NOW()
                 WHERE id=$1 AND status IN ('PENDING','TRADING')",
            )
            .bind(id)
            .execute(db.as_ref())
            .await;
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

        DbCmd::SettleTrade {
            taker_id,
            maker_id,
            mut taker_uid,
            mut maker_uid,
            price,
            qty,
            side,
            symbol,
        } => {
            let sym_end = symbol.iter().position(|&b| b == 0).unwrap_or(16);
            let symbol = std::str::from_utf8(&symbol[..sym_end])
                .unwrap_or("BTC_USDT")
                .to_owned();
            let side_str = if side == 0 { "buy" } else { "sell" };
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_micros() as u64)
                .unwrap_or(0);

            let trade_msg = format!(
                r#"{{"type":"trade","symbol":"{symbol}","price":{price},"qty":{qty},"side":"{side_str}","ts":{ts}}}"#
            );
            let _ = market_tx.send(trade_msg);

            // Resolve UIDs if cache missed.
            if taker_uid == 0 {
                taker_uid = sqlx::query_scalar("SELECT user_id FROM orders WHERE id = $1")
                    .bind(taker_id)
                    .fetch_optional(db.as_ref())
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or(0);
            }
            if maker_uid == 0 {
                maker_uid = sqlx::query_scalar("SELECT user_id FROM orders WHERE id = $1")
                    .bind(maker_id)
                    .fetch_optional(db.as_ref())
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or(0);
            }

            let (buy_oid, sell_oid) = if side == 0 {
                (taker_id, maker_id)
            } else {
                (maker_id, taker_id)
            };

            if taker_uid == 0 || maker_uid == 0 {
                return;
            }

            let parts: Vec<&str> = symbol.splitn(2, '_').collect();
            let base = *parts.first().unwrap_or(&"BTC");
            let quote = *parts.last().unwrap_or(&"USDT");
            let cost = price * qty;
            let (buyer_id, seller_id) = if side == 0 {
                (taker_uid, maker_uid)
            } else {
                (maker_uid, taker_uid)
            };

            // All settlement mutations in one transaction: trade insert + maker order
            // update + all 4 balance changes. Rolls back entirely if any step fails.
            let mut txn = match db.begin().await {
                Ok(t) => t,
                Err(e) => {
                    tracing::error!("settle txn begin: {e}");
                    return;
                }
            };

            // INSERT trade — retry once to handle FK race with taker's UpsertOrder.
            let trade_ok = sqlx::query(
                "INSERT INTO trades (symbol, buy_order_id, sell_order_id, price, quantity, created_at) \
                 VALUES ($1,$2,$3,$4,$5,NOW())",
            ).bind(&symbol).bind(buy_oid).bind(sell_oid).bind(price).bind(qty)
            .execute(&mut *txn).await.is_ok();
            if !trade_ok {
                let _ = txn.rollback().await;
                tokio::time::sleep(tokio::time::Duration::from_millis(2)).await;
                txn = match db.begin().await {
                    Ok(t) => t,
                    Err(e) => {
                        tracing::error!("settle txn retry begin: {e}");
                        return;
                    }
                };
                if let Err(e) = sqlx::query(
                    "INSERT INTO trades (symbol, buy_order_id, sell_order_id, price, quantity, created_at) \
                     VALUES ($1,$2,$3,$4,$5,NOW())",
                ).bind(&symbol).bind(buy_oid).bind(sell_oid).bind(price).bind(qty)
                .execute(&mut *txn).await {
                    tracing::error!("trade insert failed: {e}");
                    let _ = txn.rollback().await;
                    return;
                }
            }

            // UPDATE maker order filled/status.
            let maker_row: Option<(String, f64)> = sqlx::query_as(
                "UPDATE orders SET filled = filled + $1, \
                 status = CASE WHEN quantity - (filled + $1) < 1e-9 THEN 'COMPLETED' ELSE 'TRADING' END, \
                 updated_at = NOW() \
                 WHERE id = $2 \
                 RETURNING status, filled",
            ).bind(qty).bind(maker_id)
            .fetch_optional(&mut *txn).await.unwrap_or(None);

            // Buyer: debit quote (decrement both balance AND frozen), credit base.
            let bq: Option<(f64, f64)> = sqlx::query_as(
                "UPDATE accounts SET balance = balance - $1, \
                 frozen = GREATEST(frozen - $1, 0), updated_at = NOW() \
                 WHERE user_id = $2 AND asset = $3 RETURNING balance, frozen",
            )
            .bind(cost)
            .bind(buyer_id)
            .bind(quote)
            .fetch_optional(&mut *txn)
            .await
            .unwrap_or(None);

            let bb: Option<(f64, f64)> = sqlx::query_as(
                "INSERT INTO accounts (user_id, asset, balance, frozen) VALUES ($1,$2,$3,0) \
                 ON CONFLICT (user_id, asset) DO UPDATE \
                 SET balance = accounts.balance + $3, updated_at = NOW() \
                 RETURNING balance, frozen",
            )
            .bind(buyer_id)
            .bind(base)
            .bind(qty)
            .fetch_optional(&mut *txn)
            .await
            .unwrap_or(None);

            // Seller: debit base (decrement both balance AND frozen), credit quote.
            let sb: Option<(f64, f64)> = sqlx::query_as(
                "UPDATE accounts SET balance = balance - $1, \
                 frozen = GREATEST(frozen - $1, 0), updated_at = NOW() \
                 WHERE user_id = $2 AND asset = $3 RETURNING balance, frozen",
            )
            .bind(qty)
            .bind(seller_id)
            .bind(base)
            .fetch_optional(&mut *txn)
            .await
            .unwrap_or(None);

            let sq: Option<(f64, f64)> = sqlx::query_as(
                "INSERT INTO accounts (user_id, asset, balance, frozen) VALUES ($1,$2,$3,0) \
                 ON CONFLICT (user_id, asset) DO UPDATE \
                 SET balance = accounts.balance + $3, updated_at = NOW() \
                 RETURNING balance, frozen",
            )
            .bind(seller_id)
            .bind(quote)
            .bind(cost)
            .fetch_optional(&mut *txn)
            .await
            .unwrap_or(None);

            if let Err(e) = txn.commit().await {
                tracing::error!("settle txn commit: {e}");
                return;
            }

            // WS push: maker order update.
            if let (Some((ref new_status, new_filled)), uid) = (&maker_row, maker_uid) {
                if uid != 0 {
                    let ws_status = if new_status == "COMPLETED" {
                        "FILLED"
                    } else {
                        "PARTIAL_FILL"
                    };
                    if let Some(tx) = user_tx.get(&uid) {
                        let upd = serde_json::json!({
                            "type": "order_update", "order_id": maker_id,
                            "status": ws_status, "filled_qty": new_filled,
                            "avg_price": price, "ts": ts,
                        })
                        .to_string();
                        let _ = tx.try_send(upd);
                    }
                }
            }

            // WS push: balance updates for both sides.
            for (uid, updates) in [
                (buyer_id, [(quote, bq), (base, bb)]),
                (seller_id, [(base, sb), (quote, sq)]),
            ] {
                for (asset, row) in updates {
                    if let Some((bal, frz)) = row {
                        account_cache
                            .entry(uid)
                            .or_insert_with(HashMap::new)
                            .insert(asset.to_string(), (bal, frz));
                        if let Some(tx) = user_tx.get(&uid) {
                            let msg = serde_json::json!({
                                "type": "balance_update", "asset": asset,
                                "balance": bal, "available": bal - frz, "frozen": frz,
                            })
                            .to_string();
                            let _ = tx.try_send(msg);
                        }
                    }
                }
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
                    let at2 = aeron_cancel_tx.clone();
                    rt.spawn(process_db_cmd(cmd, db2, ac2, ut2, mt2, at2));
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

    // ── Aeron setup ───────────────────────────────────────────────────────────
    let client = Arc::new(
        AeronClient::new(&aeron_dir())
            .map_err(|e| anyhow::anyhow!("Aeron init failed: {:?}", e))?,
    );

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
        tracer: tracer.clone(),
        account_cache: account_cache.clone(),
        valid_symbols: Arc::new(valid_symbols),
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
        Some(db_worker_aeron_tx),
    );

    // ── Aeron spin thread: WS command drain + inbound event loop ─────────────
    // order_pub lives here exclusively — no mutex needed.
    {
        let market_tx = state.market_tx.clone();
        let pending_orders = state.pending_orders.clone();
        let pending_meta = state.pending_meta.clone();
        let user_tx = state.user_tx.clone();
        let last_depth = state.last_depth.clone();
        let rt = tokio::runtime::Handle::current();
        let spin_tracer = tracer.clone();
        // order_id → user_id: populated synchronously in the spin thread when
        // each order is accepted so the trade handler never needs a DB lookup
        // for user_id (eliminates the 20ms retry-sleep entirely).
        let order_uid_cache: Arc<DashMap<u64, i64>> = Arc::new(DashMap::new());
        // DESK_SPIN=false → exponential backoff (EC2/CPU-constrained hosts).
        // Default: spin_loop() for lowest latency on dedicated cores.
        let use_spin = std::env::var("DESK_SPIN")
            .map(|v| v != "false")
            .unwrap_or(true);

        std::thread::Builder::new()
            .name("aeron-event-loop".to_string())
            .spawn(move || {
                let mut idle_us: u64 = 0;
                // Per-symbol timestamp of the last non-empty depth snapshot.
                // Used to suppress empty snapshots during the cancel-all phase of
                // market-maker refresh cycles (which momentarily empties the book).
                let mut last_nonempty_depth: std::collections::HashMap<String, std::time::Instant> =
                    std::collections::HashMap::new();
                loop {
                    let mut did_work = false;
                    // Drain outbound commands (WS/REST → engine) without blocking.
                    while let Ok(cmd) = aeron_cmd_rx.try_recv() {
                        did_work = true;
                        match cmd {
                            AeronCmd::NewOrder(req) => {
                                let sym = std::str::from_utf8(&req.symbol)
                                    .unwrap_or("").trim_end_matches('\0');
                                if let Some(pub_) = order_pubs.get_mut(sym) {
                                    let _ = pub_.publish_new_order(&req);
                                } else {
                                    // No engine for this symbol — reject immediately so the
                                    // client doesn't time out and clean up pending_meta.
                                    let coid: u64 = req.client_order_id;
                                    let uid: u64  = req.participant_id;
                                    let ws_meta = pending_meta.remove(&coid).map(|(_, m)| m);
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
                        }
                    }

                    order_update_sub.do_work();
                    trade_sub.do_work();
                    depth_sub.do_work();

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
                        if let Some(meta_ref) = pending_meta.get(&lookup_id) {
                            if kind != order_update_kind::REJECTED {
                                order_uid_cache.insert(order_id, meta_ref.user_id);
                            }
                        }
                        if kind == order_update_kind::FILLED
                            || kind == order_update_kind::CANCELLED
                            || kind == order_update_kind::REJECTED
                        {
                            order_uid_cache.remove(&order_id);
                        }
                        let ws_meta = pending_meta.remove(&lookup_id).map(|(_, m)| m);
                        // client_order_id is only available on the first event (ACCEPTED).
                        let ws_client_oid = ws_meta.as_ref().map(|m| m.client_order_id.clone());

                        if let Some(meta) = ws_meta {
                            if kind == order_update_kind::ACCEPTED {
                                // Upsert: covers the race where REST path also ran.
                                push_db_cmd(&mut db_tx, DbCmd::UpsertOrder {
                                    id:              order_id as i64,
                                    user_id:         meta.user_id,
                                    symbol:          db_cmd::str_bytes(&meta.symbol),
                                    side:            if meta.side == "buy" { 0 } else { 1 },
                                    order_type:      db_cmd::str_bytes(&meta.order_type),
                                    price:           meta.price.unwrap_or(0.0),
                                    qty:             meta.qty,
                                    filled:          0.0,
                                    status:          db_cmd::STATUS_PENDING,
                                    freeze_price:    meta.freeze_price,
                                    do_freeze:       false,
                                    client_order_id: db_cmd::str_bytes(ws_client_oid.as_deref().unwrap_or("")),
                                }, "upsert accepted order");
                            } else if kind == order_update_kind::FILLED || kind == order_update_kind::PARTIAL_FILL {
                                // Market / IOC order filled immediately — no ACCEPTED was sent.
                                // Insert the order row now so the fills JOIN and order history work.
                                let status = if kind == order_update_kind::FILLED {
                                    db_cmd::STATUS_COMPLETED
                                } else {
                                    db_cmd::STATUS_TRADING
                                };
                                push_db_cmd(&mut db_tx, DbCmd::UpsertOrder {
                                    id:              order_id as i64,
                                    user_id:         meta.user_id,
                                    symbol:          db_cmd::str_bytes(&meta.symbol),
                                    side:            if meta.side == "buy" { 0 } else { 1 },
                                    order_type:      db_cmd::str_bytes(&meta.order_type),
                                    price:           meta.price.unwrap_or(0.0),
                                    qty:             meta.qty,
                                    filled:          fill_qty,
                                    status,
                                    freeze_price:    meta.freeze_price,
                                    do_freeze:       false,
                                    client_order_id: db_cmd::str_bytes(ws_client_oid.as_deref().unwrap_or("")),
                                }, "upsert filled order");
                            }
                            if kind == order_update_kind::REJECTED
                                || kind == order_update_kind::CANCELLED
                            {
                                push_db_cmd(&mut db_tx, DbCmd::ReleaseReservation {
                                    user_id: meta.user_id,
                                    symbol: db_cmd::str_bytes(&meta.symbol),
                                    side: if meta.side == "buy" { 0 } else { 1 },
                                    qty: meta.qty,
                                    freeze_price: meta.freeze_price,
                                }, "release rejected reservation");
                            }
                        } else {
                            // REST-path order OR subsequent WS update (row already exists).
                            let status = match kind {
                                k if k == order_update_kind::ACCEPTED     => db_cmd::STATUS_PENDING,
                                k if k == order_update_kind::PARTIAL_FILL => db_cmd::STATUS_TRADING,
                                k if k == order_update_kind::FILLED       => db_cmd::STATUS_COMPLETED,
                                k if k == order_update_kind::CANCELLED    => db_cmd::STATUS_CANCELED,
                                _                                          => db_cmd::STATUS_REJECTED,
                            };
                            if kind == order_update_kind::CANCELLED {
                                push_db_cmd(&mut db_tx, DbCmd::CancelConfirmed {
                                    id: order_id as i64,
                                    cancelled_qty: fill_qty,
                                }, "confirm cancelled order");
                            } else {
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
                        if user_tx.get(&user_id).is_none() {
                            tracing::warn!("no WS channel for user {user_id}, order_update {order_id} lost");
                        }
                        if let Some(tx) = user_tx.get(&user_id) {
                            let ws_status = match kind {
                                k if k == order_update_kind::ACCEPTED     => "OPEN",
                                k if k == order_update_kind::PARTIAL_FILL => "PARTIAL",
                                k if k == order_update_kind::FILLED       => "FILLED",
                                k if k == order_update_kind::CANCELLED    => "CANCELED",
                                _                                         => "REJECTED",
                            };
                            let ts = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.as_micros() as u64)
                                .unwrap_or(0);
                            let mut upd = serde_json::json!({
                                "type": "order_update",
                                "order_id": order_id,
                                "status": ws_status,
                                "filled_qty": fill_qty,
                                "avg_price": fill_price,
                                "ts": ts,
                            });
                            // Echo client_order_id back on ACCEPTED so client can correlate
                            // without a separate lookup (only present in fast-path WS orders).
                            if let Some(ref coid) = ws_client_oid {
                                upd["client_order_id"] = serde_json::Value::String(coid.clone());
                            }
                            if tx.try_send(upd.to_string()).is_err() {
                                tracing::warn!("personal channel full for user {user_id}, dropping order_update {order_id}");
                            }
                        }
                    }

                    // Process trade notifications — push a fixed-size DbCmd to the
                    // DB worker ring buffer (~10ns, no heap alloc, no Arc clone here).
                    while let Some(trade) = trade_sub.poll() {
                        did_work = true;
                        let price: f64   = trade.price;
                        let qty: f64     = trade.quantity;
                        let side: u8     = trade.side;
                        let taker_id     = trade.taker_order_id as i64;
                        let maker_id     = trade.maker_order_id as i64;

                        let taker_uid = order_uid_cache.get(&(taker_id as u64)).map(|v| *v).unwrap_or(0);
                        let maker_uid = order_uid_cache.get(&(maker_id as u64)).map(|v| *v).unwrap_or(0);

                        let mut sym = [0u8; 16];
                        sym.copy_from_slice(&trade.symbol[..16]);

                        push_db_cmd(&mut db_tx, DbCmd::SettleTrade {
                            taker_id, maker_id, taker_uid, maker_uid,
                            price, qty, side, symbol: sym,
                        }, "settle trade");
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

                                // Suppress empty snapshots that arrive within 2s of
                                // a non-empty one — they are artifacts of the market-maker
                                // cancel-all cycle, not a genuinely empty book.
                                if nb == 0 && na == 0 {
                                    if let Some(&t) = last_nonempty_depth.get(&symbol) {
                                        if t.elapsed() < std::time::Duration::from_secs(2) {
                                            continue; // skip this empty snapshot
                                        }
                                    }
                                } else {
                                    last_nonempty_depth.insert(symbol.clone(), std::time::Instant::now());
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
