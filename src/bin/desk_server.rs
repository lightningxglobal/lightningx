use dashmap::DashMap;
use lightning_exchange::{
    aeron_channels::{
        AERON_DIR,
        orders_channel, orders_stream_for_symbol,
        order_update_channel, ORDER_UPDATE_STREAM,
        trade_channel, TRADE_STREAM,
        depth_channel, DEPTH_STREAM, DEPTH50_STREAM, LEVEL2_STREAM,
        METRICS_CHANNEL, METRICS_STREAM,
    },
    aeron_transport::{DeskOrderPublisher, DeskOrderUpdateSubscriber, DeskTradeSubscriber, DeskDepthSubscriber},
    api::{router, AppState},
    db,
    tracer::{spawn_tracer, MS_AERON_ORDER_SEND, MS_AERON_UPDATE_RECV, MS_WS_UPDATE_SEND, DESK_INSTANCE_ID},
    transport::AeronCmd,
    ws_handler::market_data_broadcaster,
};
use aeron_wrapper::AeronClient;
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

    // ── Aeron setup ───────────────────────────────────────────────────────────
    let client = Arc::new(
        AeronClient::new(AERON_DIR)
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

    // ── Latency tracer (optional — disabled if sidecar is not running) ────────
    let tracer = spawn_tracer(AERON_DIR, METRICS_CHANNEL, METRICS_STREAM, DESK_INSTANCE_ID)
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
        let rt = tokio::runtime::Handle::current();
        let spin_tracer = tracer.clone();

        std::thread::Builder::new()
            .name("aeron-event-loop".to_string())
            .spawn(move || {
                loop {
                    // Drain outbound commands (WS/REST → engine) without blocking.
                    while let Ok(cmd) = aeron_cmd_rx.try_recv() {
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
                        let ws_meta = pending_meta.remove(&order_id).map(|(_, m)| m);
                        // client_order_id is only available on the first event (ACCEPTED).
                        let ws_client_oid = ws_meta.as_ref().map(|m| m.client_order_id.clone());

                        if let Some(meta) = ws_meta {
                            if kind == order_update_kind::ACCEPTED {
                                let oid = order_id as i64;
                                let db4 = db.clone();
                                rt.spawn(async move {
                                    // Upsert: covers the race where REST path also ran.
                                    let _ = sqlx::query(
                                        "INSERT INTO orders \
                                         (id, user_id, symbol, side, order_type, price, quantity, filled, status, freeze_price) \
                                         VALUES ($1,$2,$3,$4,$5,$6,$7,0,'PENDING',$8) \
                                         ON CONFLICT (id) DO UPDATE SET status='PENDING', updated_at=NOW()"
                                    )
                                    .bind(oid).bind(meta.user_id).bind(&meta.symbol)
                                    .bind(&meta.side).bind(&meta.order_type)
                                    .bind(meta.price).bind(meta.qty).bind(meta.freeze_price)
                                    .execute(&*db4).await;

                                    // Freeze funds after engine accepts.
                                    let repo = lightning_exchange::account_repository::AccountRepository::new(&db4);
                                    let sym: Vec<&str> = meta.symbol.splitn(2, '_').collect();
                                    let base = sym.first().copied().unwrap_or("BTC");
                                    let quote = sym.last().copied().unwrap_or("USDT");
                                    if meta.side == "buy" {
                                        let amount = meta.freeze_price * meta.qty;
                                        if amount > 0.0 { let _ = repo.freeze_for_buy(meta.user_id, quote, amount).await; }
                                    } else {
                                        let _ = repo.freeze_for_sell(meta.user_id, base, meta.qty).await;
                                    }
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
                        let end = trade.symbol.iter().position(|&b| b == 0).unwrap_or(16);
                        let symbol: String = String::from_utf8_lossy(&trade.symbol[..end]).into_owned();
                        let price: f64 = trade.price;
                        let quantity: f64 = trade.quantity;
                        let side: u8 = trade.side;
                        let taker_id: u64 = trade.taker_order_id;
                        let maker_id: u64 = trade.maker_order_id;

                        let market_tx2 = market_tx.clone();
                        let db2 = db.clone();
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

                            let (buy_oid, sell_oid): (i64, i64) = if side == 0 {
                                (taker_id as i64, maker_id as i64)
                            } else {
                                (maker_id as i64, taker_id as i64)
                            };
                            let _ = sqlx::query(
                                "INSERT INTO trades (symbol, buy_order_id, sell_order_id, price, quantity, created_at)
                                 VALUES ($1, $2, $3, $4, $5, NOW())"
                            )
                            .bind(&symbol)
                            .bind(buy_oid)
                            .bind(sell_oid)
                            .bind(price)
                            .bind(quantity)
                            .execute(db2.as_ref())
                            .await;
                        });
                    }

                    // Process depth snapshots — spin thread copies raw arrays (~320B memcpy),
                    // offloads JSON build + DashMap + broadcast to tokio (10-30μs saved).
                    while let Some(depth_msg) = depth_sub.poll() {
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
                                    .unwrap_or("ETH_USDT").to_string();
                                let symbol = if symbol.is_empty() { "ETH_USDT".to_string() } else { symbol };

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

                    std::hint::spin_loop();
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
