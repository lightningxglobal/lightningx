use dashmap::DashMap;
use lightning_exchange::{
    aeron_channels::{
        AERON_DIR,
        ORDERS_CHANNEL, ORDERS_STREAM,
        ORDER_UPDATE_CHANNEL, ORDER_UPDATE_STREAM,
        TRADE_CHANNEL, TRADE_STREAM,
        DEPTH_CHANNEL, DEPTH_STREAM, DEPTH50_STREAM, LEVEL2_STREAM,
    },
    aeron_transport::{DeskOrderPublisher, DeskOrderUpdateSubscriber, DeskTradeSubscriber, DeskDepthSubscriber},
    api::{router, AppState},
    db,
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

    let mut order_pub = DeskOrderPublisher::new(client.clone(), ORDERS_CHANNEL, ORDERS_STREAM)
        .map_err(|e| anyhow::anyhow!("DeskOrderPublisher: {}", e))?;

    let mut order_update_sub = DeskOrderUpdateSubscriber::new(
        client.clone(), ORDER_UPDATE_CHANNEL, ORDER_UPDATE_STREAM,
    )
    .map_err(|e| anyhow::anyhow!("DeskOrderUpdateSubscriber: {}", e))?;

    let mut trade_sub = DeskTradeSubscriber::new(client.clone(), TRADE_CHANNEL, TRADE_STREAM)
        .map_err(|e| anyhow::anyhow!("DeskTradeSubscriber: {}", e))?;

    let mut depth_sub = DeskDepthSubscriber::new(
        client.clone(), DEPTH_CHANNEL, DEPTH_STREAM, DEPTH50_STREAM, LEVEL2_STREAM,
    )
    .map_err(|e| anyhow::anyhow!("DeskDepthSubscriber: {}", e))?;

    tracing::info!("Aeron subscribers and publisher created");

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

        std::thread::Builder::new()
            .name("aeron-event-loop".to_string())
            .spawn(move || {
                loop {
                    // Drain outbound commands (WS/REST → engine) without blocking.
                    while let Ok(cmd) = aeron_cmd_rx.try_recv() {
                        match cmd {
                            AeronCmd::NewOrder(req) => { let _ = order_pub.publish_new_order(&req); }
                            AeronCmd::Cancel(req)   => { let _ = order_pub.publish_cancel(&req); }
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
                        let participant_id: u64 = msg.participant_id;
                        let fill_qty: f64 = msg.fill_qty;
                        let fill_price: f64 = msg.fill_price;
                        let kind: u8 = msg.kind;

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
                            let _ = tx.try_send(upd.to_string());
                        }
                    }

                    // Process trade notifications — broadcast to WS and persist to DB.
                    while let Some(trade) = trade_sub.poll() {
                        let symbol = {
                            let end = trade.symbol.iter().position(|&b| b == 0).unwrap_or(16);
                            String::from_utf8_lossy(&trade.symbol[..end]).to_string()
                        };
                        // Copy packed fields to locals before any use (misaligned ref safety).
                        let price: f64 = trade.price;
                        let quantity: f64 = trade.quantity;
                        let side: u8 = trade.side;
                        let taker_id: u64 = trade.taker_order_id;
                        let maker_id: u64 = trade.maker_order_id;
                        let side_str = if side == 0 { "buy" } else { "sell" };
                        let ts = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_micros() as u64)
                            .unwrap_or(0);

                        // Broadcast to WS subscribers.
                        let trade_msg = serde_json::json!({
                            "type": "trade",
                            "symbol": symbol,
                            "price": price,
                            "qty": quantity,
                            "side": side_str,
                            "ts": ts,
                        }).to_string();
                        let _ = market_tx.send(trade_msg);

                        // Persist to DB (fire-and-forget async task).
                        let (buy_oid, sell_oid): (i64, i64) = if side == 0 {
                            (taker_id as i64, maker_id as i64)
                        } else {
                            (maker_id as i64, taker_id as i64)
                        };
                        let db2 = db.clone();
                        let sym = symbol.clone();
                        rt.spawn(async move {
                            let _ = sqlx::query(
                                "INSERT INTO trades (symbol, buy_order_id, sell_order_id, price, quantity, created_at)
                                 VALUES ($1, $2, $3, $4, $5, NOW())"
                            )
                            .bind(&sym)
                            .bind(buy_oid)
                            .bind(sell_oid)
                            .bind(price)
                            .bind(quantity)
                            .execute(db2.as_ref())
                            .await;
                        });
                    }

                    // Process depth snapshots — update cache and broadcast to WS.
                    while let Some(depth_msg) = depth_sub.poll() {
                        use lightning_exchange::aeron_transport::DeskDepthMsg;
                        match depth_msg {
                            DeskDepthMsg::Depth(evt) => {
                                // Build JSON from the 20-level snapshot.
                                let ts = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .map(|d| d.as_micros() as u64)
                                    .unwrap_or(0);
                                let num_bids: u8 = evt.num_bids;
                                let num_asks: u8 = evt.num_asks;
                                tracing::debug!("depth recv: num_bids={} num_asks={}", num_bids, num_asks);
                                if num_asks > 0 {
                                    let first_ask: (f64, f64) = evt.asks[0];
                                    tracing::debug!("  first ask: price={} qty={}", first_ask.0, first_ask.1);
                                }
                                let bids: Vec<[f64; 2]> = evt.bids[..evt.num_bids as usize]
                                    .iter()
                                    .filter(|(_, q)| *q > 0.0)
                                    .map(|&(p, q)| [p, q])
                                    .collect();
                                let asks: Vec<[f64; 2]> = evt.asks[..evt.num_asks as usize]
                                    .iter()
                                    .filter(|(_, q)| *q > 0.0)
                                    .map(|&(p, q)| [p, q])
                                    .collect();
                                let end = evt.symbol.iter().position(|&b| b == 0).unwrap_or(16);
                                let symbol = std::str::from_utf8(&evt.symbol[..end])
                                    .unwrap_or("ETH_USDT")
                                    .to_string();
                                let symbol = if symbol.is_empty() { "ETH_USDT".to_string() } else { symbol };
                                let depth_json = serde_json::json!({
                                    "type": "depth",
                                    "symbol": symbol,
                                    "bids": bids,
                                    "asks": asks,
                                    "ts": ts,
                                });
                                last_depth.insert(symbol.clone(), depth_json.clone());
                                let _ = market_tx.send(depth_json.to_string());
                            }
                            DeskDepthMsg::Depth50(_) | DeskDepthMsg::Level2(_) => {
                                // Extended depth snapshots — not currently forwarded to WS clients.
                                // They update last_depth for future REST depth endpoint.
                            }
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
