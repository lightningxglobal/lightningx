use aeron_wrapper::AeronClient;
use lightning_exchange::{
    account_repository::AccountRepository,
    aeron_channels::{
        aeron_dir, depth_channel, order_update_channel, orders_channel, orders_stream_for_symbol,
        trade_channel, DEPTH50_STREAM, DEPTH_STREAM, LEVEL2_STREAM, METRICS_CHANNEL,
        METRICS_STREAM, ORDER_UPDATE_STREAM, TRADE_STREAM,
    },
    aeron_transport::{
        AeronMarketDataPublisher, AeronOrderSubscriber, AeronOrderUpdatePublisher,
        AeronTradePublisher,
    },
    db,
    engine::{MatchingEngine, OrderStatus, PoolConfig},
    market_data::DepthSnapshotEvent,
    models::DbOrder,
    order::{Order, Side, TimeInForce},
    sbe::TradeNotification,
    symbol_rules::SymbolRules,
    tracer::{
        spawn_tracer, ENGINE_INSTANCE_ID, MS_AERON_ORDER_RECV, MS_AERON_UPDATE_SEND,
        MS_MATCHING_DONE,
    },
    transport::{
        InboundMsg, MarketDataPublisher, OrderSubscriber, OrderUpdateMsg, OrderUpdatePublisher,
        TradePublisher,
    },
};
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

fn symbol_from_bytes(bytes: &[u8; 16]) -> &str {
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(16);
    std::str::from_utf8(&bytes[..end]).unwrap_or("")
}

/// Spawn one fully-independent matching thread for a single symbol.
///
/// Each thread creates its own AeronClient connection — this avoids the 3rd
/// publication registration hanging when all symbols share one client.
fn spawn_symbol_thread(
    symbol: String,
    engine: MatchingEngine,
    tracer: Option<lightning_exchange::tracer::ExchangeTracer>,
    uid_map: HashMap<u64, u64>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name(format!("match-{}", symbol))
        .spawn(move || {
            let client = Arc::new(AeronClient::new(&aeron_dir()).expect("AeronClient"));
            let orders_stream = orders_stream_for_symbol(&symbol);
            let mut subscriber =
                AeronOrderSubscriber::new(client.clone(), &orders_channel(), orders_stream)
                    .unwrap_or_else(|e| panic!("[{}] subscriber: {}", symbol, e));
            let mut ou_pub = AeronOrderUpdatePublisher::new(
                client.clone(),
                &order_update_channel(),
                ORDER_UPDATE_STREAM,
            )
            .unwrap_or_else(|e| panic!("[{}] ou_pub: {}", symbol, e));
            let mut trade_pub =
                AeronTradePublisher::new(client.clone(), &trade_channel(), TRADE_STREAM)
                    .unwrap_or_else(|e| panic!("[{}] trade_pub: {}", symbol, e));
            let mut md_pub = AeronMarketDataPublisher::new(
                client.clone(),
                &depth_channel(),
                DEPTH_STREAM,
                DEPTH50_STREAM,
                LEVEL2_STREAM,
            )
            .unwrap_or_else(|e| panic!("[{}] md_pub: {}", symbol, e));

            let mut engine = engine;
            let rules = SymbolRules::for_symbol(&symbol);
            // order_id → participant_id: populated on ACCEPTED, used on CANCEL to route
            // the update back to the correct user even when cancel doesn't carry uid.
            let mut uid_map = uid_map;
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
                            let req_symbol = symbol_from_bytes(&req.symbol);
                            if req_symbol != symbol {
                                let client_order_id = req.client_order_id;
                                let participant_id = req.participant_id;
                                tracing::warn!(
                                    "[{}] rejecting order {} for mismatched symbol {}",
                                    symbol,
                                    client_order_id,
                                    req_symbol,
                                );
                                let _ = ou_pub.publish(&OrderUpdateMsg::rejected(
                                    client_order_id,
                                    participant_id,
                                    4,
                                    now_ns(),
                                ));
                                continue;
                            }
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
                            let quantity_lots = match rules.quantity_to_lots(req.quantity) {
                                Ok(v) => v,
                                Err(_) => {
                                    let _ = ou_pub.publish(&OrderUpdateMsg::rejected(
                                        req.client_order_id,
                                        req.participant_id,
                                        2,
                                        ts,
                                    ));
                                    continue;
                                }
                            };
                            let order = if req.price == 0.0 {
                                Order::new_market(req.client_order_id, side, quantity_lots, ts)
                            } else {
                                let price_ticks = match rules.price_to_ticks(req.price) {
                                    Ok(v) => v,
                                    Err(_) => {
                                        let _ = ou_pub.publish(&OrderUpdateMsg::rejected(
                                            req.client_order_id,
                                            req.participant_id,
                                            2,
                                            ts,
                                        ));
                                        continue;
                                    }
                                };
                                Order::new(
                                    req.client_order_id,
                                    side,
                                    price_ticks,
                                    quantity_lots,
                                    tif,
                                    ts,
                                )
                            };

                            match engine.place_order(order) {
                                Err(e) => {
                                    tracing::warn!("[{}] place_order failed: {:?}", symbol, e);
                                    if let Some(ref t) = tracer {
                                        t.record_sym(
                                            MS_MATCHING_DONE,
                                            req.client_order_id,
                                            &req.symbol,
                                        );
                                    }
                                    let _ = ou_pub.publish(&OrderUpdateMsg::rejected(
                                        req.client_order_id,
                                        req.participant_id,
                                        2,
                                        ts,
                                    ));
                                    if let Some(ref t) = tracer {
                                        t.record_sym(
                                            MS_AERON_UPDATE_SEND,
                                            req.client_order_id,
                                            &req.symbol,
                                        );
                                    }
                                }
                                Ok(result) => {
                                    if let Some(ref t) = tracer {
                                        t.record_sym(
                                            MS_MATCHING_DONE,
                                            req.client_order_id,
                                            &req.symbol,
                                        );
                                    }
                                    let last_price = result
                                        .fills
                                        .last()
                                        .map(|f| rules.ticks_to_price(f.1))
                                        .unwrap_or(req.price);
                                    let filled_qty = rules.lots_to_quantity(result.filled_lots);
                                    let update = match result.status {
                                        OrderStatus::Accepted => {
                                            uid_map.insert(result.order_id, req.participant_id);
                                            OrderUpdateMsg::accepted(
                                                result.order_id,
                                                req.client_order_id,
                                                req.participant_id,
                                                ts,
                                            )
                                        }
                                        OrderStatus::Filled => {
                                            uid_map.remove(&result.order_id);
                                            OrderUpdateMsg::filled(
                                                result.order_id,
                                                req.client_order_id,
                                                req.participant_id,
                                                last_price,
                                                filled_qty,
                                                ts,
                                            )
                                        }
                                        OrderStatus::PartiallyFilled => {
                                            OrderUpdateMsg::partial_fill(
                                                result.order_id,
                                                req.client_order_id,
                                                req.participant_id,
                                                last_price,
                                                filled_qty,
                                                req.quantity - filled_qty,
                                                ts,
                                            )
                                        }
                                        OrderStatus::Cancelled => {
                                            uid_map.remove(&result.order_id);
                                            OrderUpdateMsg::cancelled(
                                                result.order_id,
                                                req.client_order_id,
                                                req.participant_id,
                                                req.quantity - filled_qty,
                                                ts,
                                            )
                                        }
                                        OrderStatus::Rejected => OrderUpdateMsg::rejected(
                                            req.client_order_id,
                                            req.participant_id,
                                            3,
                                            ts,
                                        ),
                                    };
                                    // Publish trades before the terminal order update so desk can
                                    // settle while runtime order metadata is still live.
                                    for &(maker_order_id, fill_price_ticks, fill_qty_lots) in
                                        &result.fills
                                    {
                                        trade_seq += 1;
                                        let fill_price = rules.ticks_to_price(fill_price_ticks);
                                        let fill_qty = rules.lots_to_quantity(fill_qty_lots);
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
                                    let _ = ou_pub.publish(&update);
                                    if let Some(ref t) = tracer {
                                        t.record_sym(
                                            MS_AERON_UPDATE_SEND,
                                            req.client_order_id,
                                            &req.symbol,
                                        );
                                    }
                                }
                            }
                        }

                        InboundMsg::CancelOrder(req) => {
                            let ts = now_ns();
                            let cancel_oid: u64 = req.order_id;
                            if let Ok(res) = engine.cancel_order(cancel_oid) {
                                let participant_id = uid_map.remove(&cancel_oid).unwrap_or(0);
                                let _ = ou_pub.publish(&OrderUpdateMsg::cancelled(
                                    req.order_id,
                                    0,
                                    participant_id,
                                    rules.lots_to_quantity(res.cancelled_quantity),
                                    ts,
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
                    let mut bids = [(0, 0); 20];
                    let mut asks = [(0, 0); 20];
                    let num_bids = engine.fill_top_levels(true, &mut bids);
                    let num_asks = engine.fill_top_levels(false, &mut asks);
                    for (i, (p, q)) in bids.iter().take(num_bids).enumerate() {
                        snap.bids[i] = (rules.ticks_to_price(*p), rules.lots_to_quantity(*q));
                    }
                    for (i, (p, q)) in asks.iter().take(num_asks).enumerate() {
                        snap.asks[i] = (rules.ticks_to_price(*p), rules.lots_to_quantity(*q));
                    }
                    snap.num_bids = num_bids as u8;
                    snap.num_asks = num_asks as u8;
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
                    let _ = repo
                        .release_frozen(*user_id, quote_asset, per_unit_price * remaining)
                        .await;
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
            .execute(&pool)
            .await;
            tracing::warn!(
                "Canceled {} stale bot orders from previous session",
                bot_stale.len()
            );
        }
    }

    // Restore active limit orders from DB into per-symbol engines.
    let mut engines: HashMap<String, MatchingEngine> = symbols
        .iter()
        .map(|s| {
            let eng = MatchingEngine::new(PoolConfig::default()).expect("engine");
            (s.clone(), eng)
        })
        .collect();

    // uid_maps: symbol → (order_id → participant_id), pre-seeded from restored DB orders
    // so that cancel events for restored orders carry the correct participant_id.
    let mut uid_maps: HashMap<String, HashMap<u64, u64>> = HashMap::new();

    let rows = sqlx::query_as::<_, DbOrder>(
        "SELECT * FROM orders WHERE status IN ('PENDING', 'TRADING') ORDER BY id ASC",
    )
    .fetch_all(&pool)
    .await
    .unwrap_or_default();

    let mut restored = 0usize;
    let mut skipped = 0usize;
    for db_order in &rows {
        if db_order.order_type == "market"
            || (db_order.order_type == "ioc" && db_order.price.is_none())
        {
            skipped += 1;
            continue;
        }
        let remaining = db_order.quantity - db_order.filled;
        if remaining <= 0.0 {
            continue;
        }
        let side = if db_order.side == "buy" {
            Side::Buy
        } else {
            Side::Sell
        };
        let rules = SymbolRules::for_symbol(&db_order.symbol);
        let price_ticks = match rules.price_to_ticks(db_order.price.unwrap_or(0.0)) {
            Ok(v) => v,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };
        let quantity_lots = match rules.quantity_to_lots(remaining) {
            Ok(v) => v,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };
        let order = Order::new(
            db_order.id as u64,
            side,
            price_ticks,
            quantity_lots,
            TimeInForce::GTC,
            0,
        );
        if let Some(eng) = engines.get_mut(&db_order.symbol) {
            if eng.add_to_book(order).is_ok() {
                uid_maps
                    .entry(db_order.symbol.clone())
                    .or_default()
                    .insert(db_order.id as u64, db_order.user_id as u64);
                restored += 1;
            }
        }
    }
    tracing::info!(
        "Restored {} active orders ({} non-restable skipped)",
        restored,
        skipped
    );

    // Detect crossed books after restoration: can happen if the engine crashed between
    // desk-server writing an order to DB and the engine receiving it via Aeron.
    // If bid >= ask for any symbol, cancel all orders for that symbol and rebuild fresh.
    {
        let repo = AccountRepository::new(&pool);
        let mut crossed_fixed = 0usize;
        for (sym, eng) in engines.iter_mut() {
            let bids = eng.get_top_levels(1, true);
            let asks = eng.get_top_levels(1, false);
            let crossed = matches!((bids.first(), asks.first()),
                (Some(&(bid, _)), Some(&(ask, _))) if bid >= ask);
            if !crossed {
                continue;
            }

            let rules = SymbolRules::for_symbol(sym);
            let best_bid = bids
                .first()
                .map(|&(p, _)| rules.ticks_to_price(p))
                .unwrap_or(0.0);
            let best_ask = asks
                .first()
                .map(|&(p, _)| rules.ticks_to_price(p))
                .unwrap_or(f64::MAX);
            tracing::warn!(
                "Crossed book for {} (bid={} >= ask={}) — cancelling all open orders and rebuilding",
                sym, best_bid, best_ask
            );

            #[allow(clippy::type_complexity)]
            let open_orders: Vec<(i64, i64, String, f64, f64, Option<f64>, f64)> = sqlx::query_as(
                "SELECT id, user_id, side, quantity, filled, price, freeze_price
                     FROM orders WHERE symbol=$1 AND status IN ('PENDING','TRADING')",
            )
            .bind(sym.as_str())
            .fetch_all(&pool)
            .await
            .unwrap_or_default();

            let sym_parts: Vec<&str> = sym.splitn(2, '_').collect();
            let base_asset = sym_parts.first().copied().unwrap_or("BTC");
            let quote_asset = sym_parts.last().copied().unwrap_or("USDT");

            for (id, user_id, side, quantity, filled, price, freeze_price) in &open_orders {
                let remaining = quantity - filled;
                if remaining > 0.0 {
                    if side == "sell" {
                        let _ = repo.release_frozen(*user_id, base_asset, remaining).await;
                    } else {
                        let fp = if *freeze_price > 0.0 {
                            Some(*freeze_price)
                        } else {
                            price.filter(|&p| p > 0.0)
                        };
                        if let Some(p) = fp {
                            let _ = repo
                                .release_frozen(*user_id, quote_asset, p * remaining)
                                .await;
                        }
                    }
                }
                let _ = sqlx::query(
                    "UPDATE orders SET status='CANCELED', updated_at=NOW() WHERE id=$1",
                )
                .bind(id)
                .execute(&pool)
                .await;
            }

            *eng = MatchingEngine::new(PoolConfig::default()).expect("engine");
            uid_maps.remove(sym.as_str());
            crossed_fixed += open_orders.len();
        }
        if crossed_fixed > 0 {
            tracing::warn!(
                "Fixed crossed book(s): cancelled {} orders total",
                crossed_fixed
            );
        }
    }

    // Cancel stale market orders from crash.
    {
        let repo = AccountRepository::new(&pool);
        let stale: Vec<(i64, i64, String, String, f64, f64, Option<f64>, f64)> = sqlx::query_as(
            "SELECT id, user_id, symbol, side, quantity, filled, price, freeze_price
             FROM orders WHERE status IN ('PENDING','TRADING') AND order_type='market'",
        )
        .fetch_all(&pool)
        .await
        .unwrap_or_default();

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
                    let p = if freeze_price > 0.0 {
                        Some(freeze_price)
                    } else {
                        row_price.filter(|p| *p > 0.0)
                    };
                    if let Some(p) = p {
                        let _ = repo
                            .release_frozen(user_id, quote_asset, p * remaining)
                            .await;
                    }
                }
            }
            let _ =
                sqlx::query("UPDATE orders SET status='CANCELED', updated_at=NOW() WHERE id=$1")
                    .bind(id)
                    .execute(&pool)
                    .await;
            cleaned += 1;
        }
        if cleaned > 0 {
            tracing::warn!("Cleaned up {} stale market orders", cleaned);
        }
    }

    drop(pool);

    tracing::info!(
        "Exchange engine started — spawning {} symbol threads",
        symbols.len()
    );

    // ----- Latency tracer (optional) -----------------------------------------
    let tracer = spawn_tracer(
        &aeron_dir(),
        METRICS_CHANNEL,
        METRICS_STREAM,
        ENGINE_INSTANCE_ID,
    );
    if tracer.is_some() {
        tracing::info!(
            "Exchange tracer connected (instance_id={})",
            ENGINE_INSTANCE_ID
        );
    }

    // ----- Spawn one independent matching thread per symbol ------------------
    // Each thread creates its own AeronClient so publications don't contend.
    let mut handles = Vec::new();
    for symbol in &symbols {
        let engine = engines.remove(symbol).expect("engine missing");
        let uid_map = uid_maps.remove(symbol).unwrap_or_default();
        let handle = spawn_symbol_thread(symbol.clone(), engine, tracer.clone(), uid_map);
        handles.push(handle);
    }

    tokio::signal::ctrl_c().await?;
    tracing::info!("Shutting down exchange engine...");

    Ok(())
}
