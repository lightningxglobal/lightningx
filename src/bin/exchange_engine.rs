use lightning_exchange::{
    account_repository::AccountRepository,
    aeron_channels::{
        AERON_DIR,
        orders_channel, orders_stream_for_symbol,
        order_update_channel, ORDER_UPDATE_STREAM,
        trade_channel, TRADE_STREAM,
        depth_channel, DEPTH_STREAM, DEPTH50_STREAM, LEVEL2_STREAM,
        METRICS_CHANNEL, METRICS_STREAM,
    },
    tracer::{spawn_tracer, MS_AERON_ORDER_RECV, MS_MATCHING_DONE, MS_AERON_UPDATE_SEND, ENGINE_INSTANCE_ID},
    aeron_transport::{
        AeronOrderSubscriber, AeronOrderUpdatePublisher,
        AeronTradePublisher, AeronMarketDataPublisher,
    },
    db,
    engine::{MatchingEngine, OrderStatus, PoolConfig},
    market_data::DepthSnapshotEvent,
    models::DbOrder,
    order::{Order, Side, TimeInForce},
    sbe::TradeNotification,
    transport::{
        InboundMsg, MarketDataPublisher, OrderSubscriber, OrderUpdateMsg, OrderUpdatePublisher,
        TradePublisher,
    },
};
use aeron_wrapper::AeronClient;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn symbol_bytes(symbol: &str) -> [u8; 16] {
    let mut out = [0u8; 16];
    let bytes = symbol.as_bytes();
    let copy_len = bytes.len().min(16);
    out[..copy_len].copy_from_slice(&bytes[..copy_len]);
    out
}

/// Spawn one fully-independent matching thread for a single symbol.
///
/// Each thread creates its own AeronClient connection — this avoids the 3rd
/// publication registration hanging when all symbols share one client.
fn spawn_symbol_thread(
    symbol: String,
    engine: MatchingEngine,
    tracer: Option<lightning_exchange::tracer::ExchangeTracer>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name(format!("match-{}", symbol))
        .spawn(move || {
            let client = Arc::new(
                AeronClient::new(AERON_DIR).expect("AeronClient"),
            );
            let orders_stream = orders_stream_for_symbol(&symbol);
            let mut subscriber = AeronOrderSubscriber::new(
                client.clone(), &orders_channel(), orders_stream,
            ).unwrap_or_else(|e| panic!("[{}] subscriber: {}", symbol, e));
            let mut ou_pub = AeronOrderUpdatePublisher::new(
                client.clone(), &order_update_channel(), ORDER_UPDATE_STREAM,
            ).unwrap_or_else(|e| panic!("[{}] ou_pub: {}", symbol, e));
            let mut trade_pub = AeronTradePublisher::new(
                client.clone(), &trade_channel(), TRADE_STREAM,
            ).unwrap_or_else(|e| panic!("[{}] trade_pub: {}", symbol, e));
            let mut md_pub = AeronMarketDataPublisher::new(
                client.clone(), &depth_channel(), DEPTH_STREAM, DEPTH50_STREAM, LEVEL2_STREAM,
            ).unwrap_or_else(|e| panic!("[{}] md_pub: {}", symbol, e));

            let mut engine = engine;
            let mut trade_seq: u64 = 0;
            let mut depth_seq: u64 = 0;
            let depth_interval = Duration::from_millis(10);
            let mut last_depth = Instant::now();
            let sym_bytes_fixed = symbol_bytes(&symbol);

            tracing::info!("[{}] matching thread started", symbol);

            loop {
                // Only the subscriber needs do_work() — it processes the incoming IPC ring.
                // IPC publishers write directly to mapped memory; no do_work() needed.
                subscriber.do_work();

                while let Some(msg) = subscriber.poll() {
                    match msg {
                        InboundMsg::NewOrder(req) => {
                            if let Some(ref t) = tracer {
                                t.record_sym(MS_AERON_ORDER_RECV, req.client_order_id, &req.symbol);
                            }
                            let ts = now_ns();
                            let side = if req.side == 0 { Side::Buy } else { Side::Sell };
                            let tif = match req.time_in_force {
                                0 => TimeInForce::GTC,
                                1 => TimeInForce::IOC,
                                2 => TimeInForce::FOK,
                                3 => TimeInForce::PostOnly,
                                _ => TimeInForce::GTC,
                            };
                            let order = if req.price == 0.0 {
                                Order::new_market(req.client_order_id, side, req.quantity, ts)
                            } else {
                                Order::new(req.client_order_id, side, req.price, req.quantity, tif, ts)
                            };

                            match engine.place_order(order) {
                                Err(e) => {
                                    tracing::warn!("[{}] place_order failed: {:?}", symbol, e);
                                    if let Some(ref t) = tracer {
                                        t.record_sym(MS_MATCHING_DONE, req.client_order_id, &req.symbol);
                                    }
                                    let _ = ou_pub.publish(&OrderUpdateMsg::rejected(
                                        req.client_order_id, req.participant_id, 2, ts,
                                    ));
                                    if let Some(ref t) = tracer {
                                        t.record_sym(MS_AERON_UPDATE_SEND, req.client_order_id, &req.symbol);
                                    }
                                }
                                Ok(result) => {
                                    if let Some(ref t) = tracer {
                                        t.record_sym(MS_MATCHING_DONE, req.client_order_id, &req.symbol);
                                    }
                                    for &(maker_order_id, fill_price, fill_qty) in &result.fills {
                                        trade_seq += 1;
                                        let trade = TradeNotification {
                                            sequence: trade_seq,
                                            taker_order_id: req.client_order_id,
                                            maker_order_id,
                                            price: fill_price,
                                            quantity: fill_qty,
                                            side: req.side,
                                            _pad: [0; 7],
                                            symbol: sym_bytes_fixed,
                                        };
                                        let _ = trade_pub.publish(&trade);
                                    }
                                    let last_price = result.fills.last().map(|f| f.1).unwrap_or(req.price);
                                    let update = match result.status {
                                        OrderStatus::Accepted => OrderUpdateMsg::accepted(
                                            result.order_id, req.client_order_id, req.participant_id, ts,
                                        ),
                                        OrderStatus::Filled => OrderUpdateMsg::filled(
                                            result.order_id, req.client_order_id, req.participant_id,
                                            last_price, result.filled, ts,
                                        ),
                                        OrderStatus::PartiallyFilled => OrderUpdateMsg::partial_fill(
                                            result.order_id, req.client_order_id, req.participant_id,
                                            last_price, result.filled, req.quantity - result.filled, ts,
                                        ),
                                        OrderStatus::Cancelled => OrderUpdateMsg::cancelled(
                                            result.order_id, req.client_order_id, req.participant_id,
                                            req.quantity - result.filled, ts,
                                        ),
                                        OrderStatus::Rejected => OrderUpdateMsg::rejected(
                                            req.client_order_id, req.participant_id, 3, ts,
                                        ),
                                    };
                                    let _ = ou_pub.publish(&update);
                                    if let Some(ref t) = tracer {
                                        t.record_sym(MS_AERON_UPDATE_SEND, req.client_order_id, &req.symbol);
                                    }
                                }
                            }
                        }

                        InboundMsg::CancelOrder(req) => {
                            let ts = now_ns();
                            if let Ok(res) = engine.cancel_order(req.order_id) {
                                let _ = ou_pub.publish(&OrderUpdateMsg::cancelled(
                                    req.order_id, 0, 0, res.cancelled_quantity, ts,
                                ));
                            }
                            // If this engine doesn't own the order, cancel_order() returns Err
                            // and we publish nothing — the correct symbol thread will handle it.
                        }
                    }
                }

                // Depth snapshot every 10ms.
                let now = Instant::now();
                if now.duration_since(last_depth) >= depth_interval {
                    last_depth = now;
                    depth_seq += 1;
                    let mut snap = DepthSnapshotEvent::new(now_ns(), depth_seq);
                    snap.symbol = sym_bytes_fixed;
                    let bids = engine.get_top_levels(20, true);
                    let asks = engine.get_top_levels(20, false);
                    for (i, (p, q)) in bids.iter().take(20).enumerate() { snap.bids[i] = (*p, *q); }
                    for (i, (p, q)) in asks.iter().take(20).enumerate() { snap.asks[i] = (*p, *q); }
                    snap.num_bids = bids.len().min(20) as u8;
                    snap.num_asks = asks.len().min(20) as u8;
                    let _ = md_pub.publish_depth(&snap);
                }

                std::hint::spin_loop();
            }
        })
        .expect("failed to spawn symbol thread")
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://user:password@localhost:5432/mydb".to_string());

    let symbols: Vec<String> = std::env::var("SYMBOLS")
        .unwrap_or_else(|_| "ETH_USDT,BTC_USDT,SOL_USDT".to_string())
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();

    tracing::info!("Connecting to database...");
    let pool = db::create_pool(&database_url).await?;
    tracing::info!("DB connected");

    // Cancel stale bot orders from previous session.
    {
        let repo = AccountRepository::new(&pool);
        let bot_stale: Vec<(i64, i64, String, String, f64, f64, f64)> = sqlx::query_as(
            "SELECT o.id, o.user_id, o.symbol, o.side, o.quantity, o.filled, COALESCE(o.freeze_price, COALESCE(o.price, 0.0))
             FROM orders o
             JOIN users u ON u.id = o.user_id
             WHERE o.status IN ('PENDING','TRADING')
               AND u.email IN ('robot@lightningx.exchange', 'demo@lightning.exchange')",
        )
        .fetch_all(&pool).await.unwrap_or_default();

        // Release frozen funds per order (must be per-row: different amounts per user/asset),
        // then cancel all stale bot orders in one bulk UPDATE instead of N individual queries.
        for (_, user_id, symbol, side, quantity, filled, per_unit_price) in &bot_stale {
            let remaining = quantity - filled;
            if remaining > 0.0 {
                let sym_parts: Vec<&str> = symbol.splitn(2, '_').collect();
                let base_asset = sym_parts.first().copied().unwrap_or("BTC");
                let quote_asset = sym_parts.last().copied().unwrap_or("USDT");
                if side == "sell" {
                    let _ = repo.release_frozen(*user_id, base_asset, remaining).await;
                } else if *per_unit_price > 0.0 {
                    let _ = repo.release_frozen(*user_id, quote_asset, per_unit_price * remaining).await;
                }
            }
        }
        if !bot_stale.is_empty() {
            let ids: Vec<i64> = bot_stale.iter().map(|(id, ..)| *id).collect();
            let _ = sqlx::query(
                "UPDATE orders SET status='CANCELED', updated_at=NOW()
                 WHERE id = ANY($1)",
            )
            .bind(&ids)
            .execute(&pool).await;
            tracing::warn!("Canceled {} stale bot orders from previous session", bot_stale.len());
        }
    }

    // Restore active limit orders from DB into per-symbol engines.
    let mut engines: HashMap<String, MatchingEngine> = symbols.iter()
        .map(|s| {
            let eng = MatchingEngine::new(PoolConfig::default()).expect("engine");
            (s.clone(), eng)
        })
        .collect();

    let rows = sqlx::query_as::<_, DbOrder>(
        "SELECT * FROM orders WHERE status IN ('PENDING', 'TRADING') ORDER BY id ASC",
    )
    .fetch_all(&pool).await.unwrap_or_default();

    let mut restored = 0usize;
    let mut skipped = 0usize;
    for db_order in &rows {
        if db_order.order_type == "market" || (db_order.order_type == "ioc" && db_order.price.is_none()) {
            skipped += 1; continue;
        }
        let remaining = db_order.quantity - db_order.filled;
        if remaining <= 0.0 { continue; }
        let side = if db_order.side == "buy" { Side::Buy } else { Side::Sell };
        let order = Order::new(
            db_order.id as u64, side, db_order.price.unwrap_or(0.0),
            remaining, TimeInForce::GTC, 0,
        );
        if let Some(eng) = engines.get_mut(&db_order.symbol) {
            if eng.add_to_book(order).is_ok() { restored += 1; }
        }
    }
    tracing::info!("Restored {} active orders ({} non-restable skipped)", restored, skipped);

    // Cancel stale market orders from crash.
    {
        let repo = AccountRepository::new(&pool);
        let stale: Vec<(i64, i64, String, String, f64, f64, Option<f64>, f64)> = sqlx::query_as(
            "SELECT id, user_id, symbol, side, quantity, filled, price, freeze_price
             FROM orders WHERE status IN ('PENDING','TRADING') AND order_type='market'",
        )
        .fetch_all(&pool).await.unwrap_or_default();

        let mut cleaned = 0usize;
        for (id, user_id, symbol, side, quantity, filled, row_price, freeze_price) in stale {
            let remaining = quantity - filled;
            if remaining > 0.0 {
                let sym_parts: Vec<&str> = symbol.splitn(2, '_').collect();
                let base_asset = sym_parts.first().copied().unwrap_or("BTC");
                let quote_asset = sym_parts.last().copied().unwrap_or("USDT");
                if side == "sell" {
                    let _ = repo.release_frozen(user_id, base_asset, remaining).await;
                } else {
                    let p = if freeze_price > 0.0 { Some(freeze_price) } else { row_price.filter(|p| *p > 0.0) };
                    if let Some(p) = p {
                        let _ = repo.release_frozen(user_id, quote_asset, p * remaining).await;
                    }
                }
            }
            let _ = sqlx::query("UPDATE orders SET status='CANCELED', updated_at=NOW() WHERE id=$1")
                .bind(id).execute(&pool).await;
            cleaned += 1;
        }
        if cleaned > 0 { tracing::warn!("Cleaned up {} stale market orders", cleaned); }
    }

    drop(pool);

    tracing::info!("Exchange engine started — spawning {} symbol threads", symbols.len());

    // ----- Latency tracer (optional) -----------------------------------------
    let tracer = spawn_tracer(AERON_DIR, METRICS_CHANNEL, METRICS_STREAM, ENGINE_INSTANCE_ID);
    if tracer.is_some() {
        tracing::info!("Exchange tracer connected (instance_id={})", ENGINE_INSTANCE_ID);
    }

    // ----- Spawn one independent matching thread per symbol ------------------
    // Each thread creates its own AeronClient so publications don't contend.
    let mut handles = Vec::new();
    for symbol in &symbols {
        let engine = engines.remove(symbol).expect("engine missing");
        let handle = spawn_symbol_thread(symbol.clone(), engine, tracer.clone());
        handles.push(handle);
    }

    tokio::signal::ctrl_c().await?;
    tracing::info!("Shutting down exchange engine...");

    Ok(())
}
