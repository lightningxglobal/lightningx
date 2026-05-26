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

    // ── Aeron spin thread: WS command drain + inbound event loop ─────────────
    // order_pub lives here exclusively — no mutex needed.
    {
        let market_tx = state.market_tx.clone();
        let pending_orders = state.pending_orders.clone();
        let pending_meta = state.pending_meta.clone();
        let user_tx = state.user_tx.clone();
        let last_depth = state.last_depth.clone();
        let db = state.db.clone();
        let account_cache = account_cache.clone();
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
                        if let Some(meta_ref) = pending_meta.get(&order_id) {
                            order_uid_cache.insert(order_id, meta_ref.user_id);
                        }
                        let ws_meta = pending_meta.remove(&order_id).map(|(_, m)| m);
                        // client_order_id is only available on the first event (ACCEPTED).
                        let ws_client_oid = ws_meta.as_ref().map(|m| m.client_order_id.clone());

                        if let Some(meta) = ws_meta {
                            if kind == order_update_kind::ACCEPTED {
                                let oid = order_id as i64;
                                let db4 = db.clone();
                                let cache4 = account_cache.clone();
                                let coid = ws_client_oid.clone();
                                rt.spawn(async move {
                                    // Upsert: covers the race where REST path also ran.
                                    let _ = sqlx::query(
                                        "INSERT INTO orders \
                                         (id, user_id, symbol, side, order_type, price, quantity, filled, status, freeze_price, client_order_id) \
                                         VALUES ($1,$2,$3,$4,$5,$6,$7,0,'PENDING',$8,$9) \
                                         ON CONFLICT (id) DO UPDATE SET status='PENDING', updated_at=NOW()"
                                    )
                                    .bind(oid).bind(meta.user_id).bind(&meta.symbol)
                                    .bind(&meta.side).bind(&meta.order_type)
                                    .bind(meta.price).bind(meta.qty).bind(meta.freeze_price)
                                    .bind(coid)
                                    .execute(&*db4).await;

                                    // Freeze funds after engine accepts; update cache with RETURNING value.
                                    let repo = lightning_exchange::account_repository::AccountRepository::new(&db4);
                                    let sym: Vec<&str> = meta.symbol.splitn(2, '_').collect();
                                    let base = sym.first().copied().unwrap_or("BTC");
                                    let quote = sym.last().copied().unwrap_or("USDT");
                                    if meta.side == "buy" {
                                        let amount = meta.freeze_price * meta.qty;
                                        if amount > 0.0 {
                                            if let Ok((bal, frz)) = repo.freeze_for_buy(meta.user_id, quote, amount).await {
                                                cache4.entry(meta.user_id).or_insert_with(HashMap::new).insert(quote.to_string(), (bal, frz));
                                            }
                                        }
                                    } else if let Ok((bal, frz)) = repo.freeze_for_sell(meta.user_id, base, meta.qty).await {
                                        cache4.entry(meta.user_id).or_insert_with(HashMap::new).insert(base.to_string(), (bal, frz));
                                    }
                                });
                            } else if kind == order_update_kind::FILLED || kind == order_update_kind::PARTIAL_FILL {
                                // Market / IOC order filled immediately — no ACCEPTED was sent.
                                // Insert the order row now so the fills JOIN and order history work.
                                let db_status = if kind == order_update_kind::FILLED { "COMPLETED" } else { "TRADING" };
                                let oid = order_id as i64;
                                let db4 = db.clone();
                                let coid = ws_client_oid.clone();
                                rt.spawn(async move {
                                    let _ = sqlx::query(
                                        "INSERT INTO orders \
                                         (id, user_id, symbol, side, order_type, price, quantity, filled, status, freeze_price, client_order_id) \
                                         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) \
                                         ON CONFLICT (id) DO UPDATE SET status=$9, filled=$8, updated_at=NOW()"
                                    )
                                    .bind(oid).bind(meta.user_id).bind(&meta.symbol)
                                    .bind(&meta.side).bind(&meta.order_type)
                                    .bind(meta.price).bind(meta.qty)
                                    .bind(fill_qty).bind(db_status).bind(meta.freeze_price)
                                    .bind(coid)
                                    .execute(&*db4).await;
                                });
                            }
                            // REJECTED / CANCELLED with meta: no DB row and no frozen
                            // funds, so nothing else to do.
                        } else {
                            // REST-path order OR subsequent WS update (row already exists).
                            let db_status = match kind {
                                k if k == order_update_kind::ACCEPTED     => "PENDING",
                                k if k == order_update_kind::PARTIAL_FILL => "TRADING",
                                k if k == order_update_kind::FILLED       => "COMPLETED",
                                k if k == order_update_kind::CANCELLED    => "CANCELED",
                                _                                         => "REJECTED",
                            };
                            let oid = order_id as i64;
                            let db3 = db.clone();
                            rt.spawn(async move {
                                let _ = sqlx::query(
                                    "UPDATE orders SET status=$1, filled=$2, updated_at=NOW() WHERE id=$3",
                                )
                                .bind(db_status).bind(fill_qty).bind(oid)
                                .execute(&*db3).await;
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

                    // Process trade notifications — offload JSON + broadcast + DB to tokio.
                    // Spin thread only extracts raw fields (~100ns); heavy work runs async.
                    while let Some(trade) = trade_sub.poll() {
                        did_work = true;
                        let end = trade.symbol.iter().position(|&b| b == 0).unwrap_or(16);
                        let symbol: String = String::from_utf8_lossy(&trade.symbol[..end]).into_owned();
                        let price: f64 = trade.price;
                        let quantity: f64 = trade.quantity;
                        let side: u8 = trade.side;
                        let taker_id: u64 = trade.taker_order_id;
                        let maker_id: u64 = trade.maker_order_id;

                        // Capture UIDs synchronously from the in-memory cache so
                        // the spawned task never needs a DB lookup for user_id.
                        // Maker orders are resting in the book and were inserted in
                        // a prior cycle so their cache entry may not exist — fall
                        // back to a single DB SELECT (no retry, no sleep).
                        let taker_uid_cached = order_uid_cache.get(&taker_id).map(|v| *v);
                        let maker_uid_cached = order_uid_cache.remove(&maker_id).map(|(_, v)| v);
                        // Taker entry is consumed once settlement runs.
                        order_uid_cache.remove(&taker_id);

                        let market_tx2 = market_tx.clone();
                        let db2 = db.clone();
                        let user_tx2 = user_tx.clone();
                        let account_cache2 = account_cache.clone();
                        rt.spawn(async move {
                            let side_str = if side == 0 { "buy" } else { "sell" };
                            let ts = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.as_micros() as u64)
                                .unwrap_or(0);
                            let trade_msg = format!(
                                r#"{{"type":"trade","symbol":"{symbol}","price":{price},"qty":{quantity},"side":"{side_str}","ts":{ts}}}"#
                            );
                            let _ = market_tx2.send(trade_msg);

                            let taker_oid = taker_id as i64;
                            let maker_oid = maker_id as i64;

                            // Resolve UIDs: use cache for taker (just placed this cycle),
                            // DB for maker (resting order from a prior cycle).
                            let (taker_uid, maker_uid) = match (taker_uid_cached, maker_uid_cached) {
                                (Some(t), Some(m)) => (Some(t), Some(m)),
                                (t_opt, m_opt) => {
                                    let t = if t_opt.is_some() { t_opt } else {
                                        sqlx::query_scalar("SELECT user_id FROM orders WHERE id = $1")
                                            .bind(taker_oid).fetch_optional(db2.as_ref()).await.unwrap_or(None)
                                    };
                                    let m = if m_opt.is_some() { m_opt } else {
                                        sqlx::query_scalar("SELECT user_id FROM orders WHERE id = $1")
                                            .bind(maker_oid).fetch_optional(db2.as_ref()).await.unwrap_or(None)
                                    };
                                    (t, m)
                                }
                            };

                            let (buy_oid, sell_oid): (i64, i64) = if side == 0 {
                                (taker_oid, maker_oid)
                            } else {
                                (maker_oid, taker_oid)
                            };
                            // FK on trades(buy_order_id, sell_order_id) requires both order
                            // rows to exist. The taker's INSERT runs in a concurrent task so
                            // there is a small race; retry once with a 2ms pause if needed.
                            let trade_res = sqlx::query(
                                "INSERT INTO trades (symbol, buy_order_id, sell_order_id, price, quantity, created_at)
                                 VALUES ($1, $2, $3, $4, $5, NOW())"
                            )
                            .bind(&symbol).bind(buy_oid).bind(sell_oid).bind(price).bind(quantity)
                            .execute(db2.as_ref()).await;
                            if trade_res.is_err() {
                                tokio::time::sleep(tokio::time::Duration::from_millis(2)).await;
                                let _ = sqlx::query(
                                    "INSERT INTO trades (symbol, buy_order_id, sell_order_id, price, quantity, created_at)
                                     VALUES ($1, $2, $3, $4, $5, NOW())"
                                )
                                .bind(&symbol).bind(buy_oid).bind(sell_oid).bind(price).bind(quantity)
                                .execute(db2.as_ref()).await;
                            }

                            // Update maker order's filled qty and status — the engine only
                            // sends order_update for the taker; maker updates must come from here.
                            let maker_row: Option<(String, f64)> = sqlx::query_as(
                                "UPDATE orders SET filled = filled + $1, \
                                 status = CASE WHEN quantity - (filled + $1) < 1e-9 THEN 'COMPLETED' ELSE 'TRADING' END, \
                                 updated_at = NOW() \
                                 WHERE id = $2 \
                                 RETURNING status, filled"
                            )
                            .bind(quantity).bind(maker_oid)
                            .fetch_optional(db2.as_ref()).await.unwrap_or(None);

                            // Push order_update WS event to the maker's user so the UI updates.
                            if let (Some((ref new_status, new_filled)), Some(maker_uid)) = (&maker_row, maker_uid) {
                                let ws_maker_status = if new_status == "COMPLETED" { "FILLED" } else { "PARTIAL_FILL" };
                                if let Some(tx) = user_tx2.get(&maker_uid) {
                                    let upd = serde_json::json!({
                                        "type": "order_update",
                                        "order_id": maker_oid,
                                        "status": ws_maker_status,
                                        "filled_qty": new_filled,
                                        "avg_price": price,
                                        "ts": ts,
                                    }).to_string();
                                    let _ = tx.try_send(upd);
                                }
                            }

                            if let (Some(taker_uid), Some(maker_uid)) = (taker_uid, maker_uid) {
                                let cost = price * quantity;
                                let sym_parts: Vec<&str> = symbol.splitn(2, '_').collect();
                                let base = *sym_parts.first().unwrap_or(&"BTC");
                                let quote = *sym_parts.last().unwrap_or(&"USDT");
                                let (buyer_id, seller_id) = if side == 0 {
                                    (taker_uid, maker_uid)
                                } else {
                                    (maker_uid, taker_uid)
                                };

                                // Settle buyer and seller in parallel; use RETURNING to get
                                // the new balance without a separate SELECT.
                                let ((bq_row, bb_row), (sb_row, sq_row)) = tokio::join!(
                                    async {
                                        // Buyer: debit quote, credit base
                                        let q: Option<(f64, f64)> = sqlx::query_as(
                                            "UPDATE accounts SET balance = balance - $1, updated_at = NOW() \
                                             WHERE user_id = $2 AND asset = $3 \
                                             RETURNING balance, frozen"
                                        ).bind(cost).bind(buyer_id).bind(quote)
                                        .fetch_optional(db2.as_ref()).await.unwrap_or(None);
                                        let b: Option<(f64, f64)> = sqlx::query_as(
                                            "INSERT INTO accounts (user_id, asset, balance, frozen) VALUES ($1, $2, $3, 0) \
                                             ON CONFLICT (user_id, asset) DO UPDATE \
                                             SET balance = accounts.balance + $3, updated_at = NOW() \
                                             RETURNING balance, frozen"
                                        ).bind(buyer_id).bind(base).bind(quantity)
                                        .fetch_optional(db2.as_ref()).await.unwrap_or(None);
                                        (q, b)
                                    },
                                    async {
                                        // Seller: debit base (release frozen), credit quote
                                        let b: Option<(f64, f64)> = sqlx::query_as(
                                            "UPDATE accounts SET balance = balance - $1, \
                                             frozen = GREATEST(frozen - $1, 0), updated_at = NOW() \
                                             WHERE user_id = $2 AND asset = $3 \
                                             RETURNING balance, frozen"
                                        ).bind(quantity).bind(seller_id).bind(base)
                                        .fetch_optional(db2.as_ref()).await.unwrap_or(None);
                                        let q: Option<(f64, f64)> = sqlx::query_as(
                                            "INSERT INTO accounts (user_id, asset, balance, frozen) VALUES ($1, $2, $3, 0) \
                                             ON CONFLICT (user_id, asset) DO UPDATE \
                                             SET balance = accounts.balance + $3, updated_at = NOW() \
                                             RETURNING balance, frozen"
                                        ).bind(seller_id).bind(quote).bind(cost)
                                        .fetch_optional(db2.as_ref()).await.unwrap_or(None);
                                        (b, q)
                                    }
                                );

                                // Update cache and push balance_update WS using RETURNING data.
                                for (uid, updates) in [
                                    (buyer_id,  [(quote, bq_row), (base,  bb_row)]),
                                    (seller_id, [(base,  sb_row), (quote, sq_row)]),
                                ] {
                                    for (asset, row) in updates {
                                        if let Some((bal, frz)) = row {
                                            // Keep in-memory cache authoritative.
                                            account_cache2.entry(uid).or_insert_with(HashMap::new).insert(asset.to_string(), (bal, frz));
                                            // Push WS balance_update if user is connected.
                                            if let Some(tx) = user_tx2.get(&uid) {
                                                let msg = serde_json::json!({
                                                    "type": "balance_update",
                                                    "asset": asset,
                                                    "balance": bal,
                                                    "available": bal - frz,
                                                    "frozen": frz,
                                                }).to_string();
                                                let _ = tx.try_send(msg);
                                            }
                                        }
                                    }
                                }
                            }
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
