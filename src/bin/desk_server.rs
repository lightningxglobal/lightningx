use dashmap::DashMap;
use lightning_exchange::{
    aeron_channels::{
        aeron_dir,
        orders_channel, orders_stream_for_symbol,
        order_update_channel, ORDER_UPDATE_STREAM,
        trade_channel, TRADE_STREAM,
        depth_channel, DEPTH_STREAM, DEPTH50_STREAM, LEVEL2_STREAM,
        METRICS_CHANNEL, METRICS_STREAM,
    },
    aeron_transport::{DeskOrderPublisher, DeskOrderUpdateSubscriber, DeskTradeSubscriber, DeskDepthSubscriber},
    api::{router, AppState, AccountCache},
    db,
    tracer::{spawn_tracer, MS_AERON_ORDER_SEND, MS_AERON_UPDATE_RECV, MS_WS_UPDATE_SEND, DESK_INSTANCE_ID},
    transport::AeronCmd,
    ws_handler::market_data_broadcaster,
};
use aeron_wrapper::AeronClient;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tower_http::cors::{Any, CorsLayer};

// ── DB status byte constants ──────────────────────────────────────────────────
mod db_cmd {
    pub const STATUS_PENDING:   u8 = 0;
    pub const STATUS_TRADING:   u8 = 1;
    pub const STATUS_COMPLETED: u8 = 2;
    pub const STATUS_CANCELED:  u8 = 3;
    pub const STATUS_REJECTED:  u8 = 4;

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
        id:              i64,
        user_id:         i64,
        symbol:          [u8; 16],
        side:            u8,          // 0=buy 1=sell
        order_type:      [u8; 16],
        price:           f64,
        qty:             f64,
        filled:          f64,
        status:          u8,          // db_cmd::STATUS_*
        freeze_price:    f64,
        do_freeze:       bool,
        client_order_id: [u8; 32],    // null-padded, max 31 chars
    },
    /// UPDATE orders SET status, filled WHERE id  (REST / subsequent update path).
    UpdateStatus {
        id:     i64,
        status: u8,
        filled: f64,
    },
    /// INSERT trade + settle accounts + push maker WS.
    SettleTrade {
        taker_id:  i64,
        maker_id:  i64,
        taker_uid: i64,  // 0 → DB worker resolves via SELECT
        maker_uid: i64,  // 0 → DB worker resolves via SELECT
        price:     f64,
        qty:       f64,
        side:      u8,   // 0=buy taker, 1=sell taker
        symbol:    [u8; 16],
    },
}

// ── process_db_cmd: runs in the tokio runtime, off the spin thread ────────────
async fn process_db_cmd(
    cmd: DbCmd,
    db: std::sync::Arc<sqlx::PgPool>,
    account_cache: lightning_exchange::api::AccountCache,
    user_tx: std::sync::Arc<dashmap::DashMap<i64, tokio::sync::mpsc::Sender<String>>>,
    market_tx: std::sync::Arc<tokio::sync::broadcast::Sender<String>>,
) {
    use lightning_exchange::account_repository::AccountRepository;
    match cmd {
        DbCmd::UpsertOrder {
            id, user_id, symbol, side, order_type, price, qty, filled,
            status, freeze_price, do_freeze, client_order_id,
        } => {
            let sym_end  = symbol.iter().position(|&b| b == 0).unwrap_or(16);
            let ot_end   = order_type.iter().position(|&b| b == 0).unwrap_or(16);
            let coid_end = client_order_id.iter().position(|&b| b == 0).unwrap_or(32);
            let sym_str  = std::str::from_utf8(&symbol[..sym_end]).unwrap_or("BTC_USDT");
            let ot_str   = std::str::from_utf8(&order_type[..ot_end]).unwrap_or("limit");
            let coid_str = std::str::from_utf8(&client_order_id[..coid_end])
                .ok().filter(|s| !s.is_empty()).map(|s| s.to_owned());
            let side_str   = if side == 0 { "buy" } else { "sell" };
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
                let base  = parts.first().copied().unwrap_or("BTC");
                let quote = parts.last().copied().unwrap_or("USDT");
                if side == 0 {
                    // buy: freeze quote
                    let amount = freeze_price * qty;
                    if amount > 0.0 {
                        if let Ok((bal, frz)) = repo.freeze_for_buy(user_id, quote, amount).await {
                            account_cache.entry(user_id).or_insert_with(HashMap::new)
                                .insert(quote.to_string(), (bal, frz));
                        }
                    }
                } else {
                    // sell: freeze base
                    if let Ok((bal, frz)) = repo.freeze_for_sell(user_id, base, qty).await {
                        account_cache.entry(user_id).or_insert_with(HashMap::new)
                            .insert(base.to_string(), (bal, frz));
                    }
                }
            }
        }

        DbCmd::UpdateStatus { id, status, filled } => {
            let _ = sqlx::query(
                "UPDATE orders SET status=$1, filled=$2, updated_at=NOW() WHERE id=$3",
            )
            .bind(db_cmd::status_str(status)).bind(filled).bind(id)
            .execute(db.as_ref()).await;
        }

        DbCmd::SettleTrade {
            taker_id, maker_id, mut taker_uid, mut maker_uid,
            price, qty, side, symbol,
        } => {
            let sym_end  = symbol.iter().position(|&b| b == 0).unwrap_or(16);
            let symbol   = std::str::from_utf8(&symbol[..sym_end]).unwrap_or("BTC_USDT").to_owned();
            let side_str = if side == 0 { "buy" } else { "sell" };
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_micros() as u64).unwrap_or(0);

            let trade_msg = format!(
                r#"{{"type":"trade","symbol":"{symbol}","price":{price},"qty":{qty},"side":"{side_str}","ts":{ts}}}"#
            );
            let _ = market_tx.send(trade_msg);

            // Resolve UIDs if cache missed.
            if taker_uid == 0 {
                taker_uid = sqlx::query_scalar("SELECT user_id FROM orders WHERE id = $1")
                    .bind(taker_id).fetch_optional(db.as_ref()).await
                    .ok().flatten().unwrap_or(0);
            }
            if maker_uid == 0 {
                maker_uid = sqlx::query_scalar("SELECT user_id FROM orders WHERE id = $1")
                    .bind(maker_id).fetch_optional(db.as_ref()).await
                    .ok().flatten().unwrap_or(0);
            }

            let (buy_oid, sell_oid) = if side == 0 { (taker_id, maker_id) } else { (maker_id, taker_id) };

            // INSERT trade (retry once on FK race with taker's UpsertOrder).
            let trade_res = sqlx::query(
                "INSERT INTO trades (symbol, buy_order_id, sell_order_id, price, quantity, created_at) \
                 VALUES ($1,$2,$3,$4,$5,NOW())",
            ).bind(&symbol).bind(buy_oid).bind(sell_oid).bind(price).bind(qty)
            .execute(db.as_ref()).await;
            if trade_res.is_err() {
                tokio::time::sleep(tokio::time::Duration::from_millis(2)).await;
                let _ = sqlx::query(
                    "INSERT INTO trades (symbol, buy_order_id, sell_order_id, price, quantity, created_at) \
                     VALUES ($1,$2,$3,$4,$5,NOW())",
                ).bind(&symbol).bind(buy_oid).bind(sell_oid).bind(price).bind(qty)
                .execute(db.as_ref()).await;
            }

            // UPDATE maker order filled/status and capture new state for WS push.
            let maker_row: Option<(String, f64)> = sqlx::query_as(
                "UPDATE orders SET filled = filled + $1, \
                 status = CASE WHEN quantity - (filled + $1) < 1e-9 THEN 'COMPLETED' ELSE 'TRADING' END, \
                 updated_at = NOW() \
                 WHERE id = $2 \
                 RETURNING status, filled",
            ).bind(qty).bind(maker_id)
            .fetch_optional(db.as_ref()).await.unwrap_or(None);

            if let (Some((ref new_status, new_filled)), uid) = (&maker_row, maker_uid) {
                if uid != 0 {
                    let ws_status = if new_status == "COMPLETED" { "FILLED" } else { "PARTIAL_FILL" };
                    if let Some(tx) = user_tx.get(&uid) {
                        let upd = serde_json::json!({
                            "type": "order_update", "order_id": maker_id,
                            "status": ws_status, "filled_qty": new_filled,
                            "avg_price": price, "ts": ts,
                        }).to_string();
                        let _ = tx.try_send(upd);
                    }
                }
            }

            if taker_uid == 0 || maker_uid == 0 { return; }

            let parts: Vec<&str> = symbol.splitn(2, '_').collect();
            let base  = *parts.first().unwrap_or(&"BTC");
            let quote = *parts.last().unwrap_or(&"USDT");
            let cost = price * qty;
            let (buyer_id, seller_id) = if side == 0 { (taker_uid, maker_uid) } else { (maker_uid, taker_uid) };

            let ((bq, bb), (sb, sq)) = tokio::join!(
                async {
                    let q: Option<(f64, f64)> = sqlx::query_as(
                        "UPDATE accounts SET balance = balance - $1, updated_at = NOW() \
                         WHERE user_id = $2 AND asset = $3 RETURNING balance, frozen",
                    ).bind(cost).bind(buyer_id).bind(quote)
                    .fetch_optional(db.as_ref()).await.unwrap_or(None);
                    let b: Option<(f64, f64)> = sqlx::query_as(
                        "INSERT INTO accounts (user_id, asset, balance, frozen) VALUES ($1,$2,$3,0) \
                         ON CONFLICT (user_id, asset) DO UPDATE \
                         SET balance = accounts.balance + $3, updated_at = NOW() \
                         RETURNING balance, frozen",
                    ).bind(buyer_id).bind(base).bind(qty)
                    .fetch_optional(db.as_ref()).await.unwrap_or(None);
                    (q, b)
                },
                async {
                    let b: Option<(f64, f64)> = sqlx::query_as(
                        "UPDATE accounts SET balance = balance - $1, \
                         frozen = GREATEST(frozen - $1, 0), updated_at = NOW() \
                         WHERE user_id = $2 AND asset = $3 RETURNING balance, frozen",
                    ).bind(qty).bind(seller_id).bind(base)
                    .fetch_optional(db.as_ref()).await.unwrap_or(None);
                    let q: Option<(f64, f64)> = sqlx::query_as(
                        "INSERT INTO accounts (user_id, asset, balance, frozen) VALUES ($1,$2,$3,0) \
                         ON CONFLICT (user_id, asset) DO UPDATE \
                         SET balance = accounts.balance + $3, updated_at = NOW() \
                         RETURNING balance, frozen",
                    ).bind(seller_id).bind(quote).bind(cost)
                    .fetch_optional(db.as_ref()).await.unwrap_or(None);
                    (b, q)
                }
            );

            for (uid, updates) in [
                (buyer_id,  [(quote, bq), (base,  bb)]),
                (seller_id, [(base,  sb), (quote, sq)]),
            ] {
                for (asset, row) in updates {
                    if let Some((bal, frz)) = row {
                        account_cache.entry(uid).or_insert_with(HashMap::new)
                            .insert(asset.to_string(), (bal, frz));
                        if let Some(tx) = user_tx.get(&uid) {
                            let msg = serde_json::json!({
                                "type": "balance_update", "asset": asset,
                                "balance": bal, "available": bal - frz, "frozen": frz,
                            }).to_string();
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
) {
    std::thread::Builder::new().name("db-worker".to_string()).spawn(move || {
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
                rt.spawn(process_db_cmd(cmd, db2, ac2, ut2, mt2));
            }
            if !did_work {
                idle_us = (idle_us * 2 + 10).min(200);
                std::thread::sleep(std::time::Duration::from_micros(idle_us));
            }
        }
    }).unwrap();
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
        let rows: Vec<(i64, String, f64, f64)> = sqlx::query_as(
            "SELECT user_id, asset, balance, frozen FROM accounts",
        ).fetch_all(&pool).await.unwrap_or_default();
        for (uid, asset, bal, frz) in rows {
            account_cache.entry(uid).or_insert_with(HashMap::new).insert(asset, (bal, frz));
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
    let symbols_env = std::env::var("SYMBOLS")
        .unwrap_or_else(|_| "ETH_USDT,BTC_USDT,SOL_USDT".to_string());
    let mut order_pubs: std::collections::HashMap<String, DeskOrderPublisher> =
        symbols_env.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
        .map(|sym| {
            let stream = orders_stream_for_symbol(&sym);
            let pub_ = DeskOrderPublisher::new(client.clone(), &orders_channel(), stream)
                .unwrap_or_else(|e| panic!("DeskOrderPublisher({sym}): {e}"));
            (sym, pub_)
        })
        .collect();

    let mut order_update_sub = DeskOrderUpdateSubscriber::new(
        client.clone(), &order_update_channel(), ORDER_UPDATE_STREAM,
    )
    .map_err(|e| anyhow::anyhow!("DeskOrderUpdateSubscriber: {}", e))?;

    let mut trade_sub = DeskTradeSubscriber::new(client.clone(), &trade_channel(), TRADE_STREAM)
        .map_err(|e| anyhow::anyhow!("DeskTradeSubscriber: {}", e))?;

    let mut depth_sub = DeskDepthSubscriber::new(
        client.clone(), &depth_channel(), DEPTH_STREAM, DEPTH50_STREAM, LEVEL2_STREAM,
    )
    .map_err(|e| anyhow::anyhow!("DeskDepthSubscriber: {}", e))?;

    tracing::info!("Aeron subscribers and publisher created");

    // ── Latency tracer (optional — disabled if is not running) ────────
    let tracer = spawn_tracer(&aeron_dir(), METRICS_CHANNEL, METRICS_STREAM, DESK_INSTANCE_ID)
        .map(Arc::new);
    if tracer.is_some() {
        tracing::info!("Exchange tracer connected (instance_id={})", DESK_INSTANCE_ID);
    }

    // ── Command channel: async WS handlers → Aeron spin thread ───────────────
    let (aeron_cmd_tx, mut aeron_cmd_rx) = tokio::sync::mpsc::unbounded_channel::<AeronCmd>();

    // ── Sync next_order_id from DB so WS atomic IDs don't collide ─────────────
    let max_db_id: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(id), 0) FROM orders")
        .fetch_one(&pool).await.unwrap_or(0);
    let initial_id = (max_db_id as u64) + 1;

    // ── Shared state ──────────────────────────────────────────────────────────
    let (market_tx, _) = broadcast::channel::<String>(1024);

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

        std::thread::Builder::new()
            .name("aeron-event-loop".to_string())
            .spawn(move || {
                let mut idle_us: u64 = 0;
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
                                } else if let Some(pub_) = order_pubs.values_mut().next() {
                                    // unknown symbol: fall back to first available publisher
                                    let _ = pub_.publish_new_order(&req);
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

                        // Route to waiting REST request (pending_orders) if any.
                        if let Some((_, tx)) = pending_orders.remove(&order_id) {
                            let _ = tx.send(msg);
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
                        if let Some(meta_ref) = pending_meta.get(&lookup_id) {
                            if kind != order_update_kind::REJECTED {
                                order_uid_cache.insert(order_id, meta_ref.user_id);
                            }
                        }
                        let ws_meta = pending_meta.remove(&lookup_id).map(|(_, m)| m);
                        // client_order_id is only available on the first event (ACCEPTED).
                        let ws_client_oid = ws_meta.as_ref().map(|m| m.client_order_id.clone());

                        if let Some(meta) = ws_meta {
                            if kind == order_update_kind::ACCEPTED {
                                // Upsert: covers the race where REST path also ran.
                                let _ = db_tx.push(DbCmd::UpsertOrder {
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
                                    do_freeze:       true,
                                    client_order_id: db_cmd::str_bytes(ws_client_oid.as_deref().unwrap_or("")),
                                });
                            } else if kind == order_update_kind::FILLED || kind == order_update_kind::PARTIAL_FILL {
                                // Market / IOC order filled immediately — no ACCEPTED was sent.
                                // Insert the order row now so the fills JOIN and order history work.
                                let status = if kind == order_update_kind::FILLED {
                                    db_cmd::STATUS_COMPLETED
                                } else {
                                    db_cmd::STATUS_TRADING
                                };
                                let _ = db_tx.push(DbCmd::UpsertOrder {
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
                                });
                            }
                            // REJECTED / CANCELLED with meta: no DB row and no frozen
                            // funds, so nothing else to do.
                        } else {
                            // REST-path order OR subsequent WS update (row already exists).
                            let status = match kind {
                                k if k == order_update_kind::ACCEPTED     => db_cmd::STATUS_PENDING,
                                k if k == order_update_kind::PARTIAL_FILL => db_cmd::STATUS_TRADING,
                                k if k == order_update_kind::FILLED       => db_cmd::STATUS_COMPLETED,
                                k if k == order_update_kind::CANCELLED    => db_cmd::STATUS_CANCELED,
                                _                                          => db_cmd::STATUS_REJECTED,
                            };
                            let _ = db_tx.push(DbCmd::UpdateStatus {
                                id:     order_id as i64,
                                status,
                                filled: fill_qty,
                            });
                        }

                        // Push order_update to user's personal WS channel.
                        let user_id = participant_id as i64;
                        if user_tx.get(&user_id).is_none() {
                            tracing::warn!("no WS channel for user {user_id}, order_update {order_id} lost");
                        }
                        if let Some(tx) = user_tx.get(&user_id) {
                            if let Some(ref t) = spin_tracer {
                                t.record(MS_WS_UPDATE_SEND, client_order_id);
                            }
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
                        let maker_uid = order_uid_cache.remove(&(maker_id as u64)).map(|(_, v)| v).unwrap_or(0);
                        order_uid_cache.remove(&(taker_id as u64));

                        let mut sym = [0u8; 16];
                        sym.copy_from_slice(&trade.symbol[..16]);

                        let _ = db_tx.push(DbCmd::SettleTrade {
                            taker_id, maker_id, taker_uid, maker_uid,
                            price, qty, side, symbol: sym,
                        });
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

                    // Backoff when idle: exponential up to 500μs max.
                    // Keeps latency acceptable while freeing CPU on resource-constrained hosts.
                    // Remove and use spin_loop() instead if dedicated CPU cores are available.
                    if !did_work {
                        idle_us = (idle_us * 2 + 10).min(500);
                        std::thread::sleep(std::time::Duration::from_micros(idle_us));
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
