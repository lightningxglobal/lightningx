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

    let order_pub = DeskOrderPublisher::new(client.clone(), ORDERS_CHANNEL, ORDERS_STREAM)
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

    // ── Shared state ──────────────────────────────────────────────────────────
    let (market_tx, _) = broadcast::channel::<String>(1024);

    let state = AppState {
        db: Arc::new(pool),
        engines: None,
        market_tx: Arc::new(market_tx),
        user_tx: Arc::new(DashMap::new()),
        next_order_id: Arc::new(AtomicU64::new(1)),
        aeron_pub: Some(Arc::new(parking_lot::Mutex::new(order_pub))),
        pending_orders: Arc::new(DashMap::new()),
        last_depth: Arc::new(DashMap::new()),
    };

    // ── Aeron event loop (dedicated OS thread) ────────────────────────────────
    // Bridge: market_tx (broadcast::Sender) and pending_orders / user_tx
    // (DashMap) are all Arc'd and thread-safe, so the OS thread can use them
    // directly without going through a tokio channel.
    {
        let market_tx = state.market_tx.clone();
        let pending_orders = state.pending_orders.clone();
        let user_tx = state.user_tx.clone();
        let last_depth = state.last_depth.clone();

        std::thread::Builder::new()
            .name("aeron-event-loop".to_string())
            .spawn(move || {
                loop {
                    order_update_sub.do_work();
                    trade_sub.do_work();
                    depth_sub.do_work();

                    // Process order updates — complete pending REST/WS requests.
                    while let Some(msg) = order_update_sub.poll() {
                        // Copy packed struct fields to locals to avoid misaligned refs.
                        let order_id: u64 = msg.order_id;
                        let participant_id: u64 = msg.participant_id;
                        let fill_qty: f64 = msg.fill_qty;
                        let fill_price: f64 = msg.fill_price;
                        let kind: u8 = msg.kind;

                        // Route to waiting request (pending_orders) if any.
                        if let Some((_, tx)) = pending_orders.remove(&order_id) {
                            let _ = tx.send(msg);
                        }

                        // Also push order_update to user's personal WS channel.
                        let user_id = participant_id as i64;
                        if let Some(tx) = user_tx.get(&user_id) {
                            use lightning_exchange::transport::order_update_kind;
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
                            let upd = serde_json::json!({
                                "type": "order_update",
                                "order_id": order_id,
                                "status": ws_status,
                                "filled_qty": fill_qty,
                                "avg_price": fill_price,
                                "ts": ts,
                            }).to_string();
                            let _ = tx.try_send(upd);
                        }
                    }

                    // Process trade notifications — broadcast to WS subscribers.
                    while let Some(trade) = trade_sub.poll() {
                        let symbol = {
                            let end = trade.symbol.iter().position(|&b| b == 0).unwrap_or(16);
                            String::from_utf8_lossy(&trade.symbol[..end]).to_string()
                        };
                        // Copy packed fields to locals.
                        let price: f64 = trade.price;
                        let quantity: f64 = trade.quantity;
                        let side: u8 = trade.side;
                        let side_str = if side == 0 { "buy" } else { "sell" };
                        let ts = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_micros() as u64)
                            .unwrap_or(0);
                        let trade_msg = serde_json::json!({
                            "type": "trade",
                            "symbol": symbol,
                            "price": price,
                            "qty": quantity,
                            "side": side_str,
                            "ts": ts,
                        }).to_string();
                        let _ = market_tx.send(trade_msg);
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
