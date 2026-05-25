use lightning_exchange::{
    account_repository::AccountRepository,
    aeron_channels::{
        AERON_DIR,
        ORDERS_CHANNEL, ORDERS_STREAM,
        ORDER_UPDATE_CHANNEL, ORDER_UPDATE_STREAM,
        TRADE_CHANNEL, TRADE_STREAM,
        DEPTH_CHANNEL, DEPTH_STREAM, DEPTH50_STREAM, LEVEL2_STREAM,
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

    // Cancel stale bot orders (market-maker + demo) — they restart fresh each run.
    {
        let repo = AccountRepository::new(&pool);
        let bot_stale: Vec<(i64, i64, String, String, f64, f64, f64)> = sqlx::query_as(
            "SELECT o.id, o.user_id, o.symbol, o.side, o.quantity, o.filled, COALESCE(o.freeze_price, COALESCE(o.price, 0.0))
             FROM orders o
             JOIN users u ON u.id = o.user_id
             WHERE o.status IN ('PENDING','TRADING')
               AND u.email IN ('robot@lightningx.exchange', 'demo@lightning.exchange')",
        )
        .fetch_all(&pool)
        .await
        .unwrap_or_default();

        for (id, user_id, symbol, side, quantity, filled, per_unit_price) in &bot_stale {
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
            let _ = sqlx::query("UPDATE orders SET status='CANCELED', updated_at=NOW() WHERE id=$1")
                .bind(id)
                .execute(&pool)
                .await;
        }
        if !bot_stale.is_empty() {
            tracing::warn!("Canceled {} stale bot orders from previous session", bot_stale.len());
        }
    }

    // Restore active limit orders from DB into a per-symbol MatchingEngine.
    let mut engines: HashMap<String, MatchingEngine> = symbols
        .iter()
        .map(|s| {
            let eng = MatchingEngine::new(PoolConfig::default()).expect("failed to create engine");
            (s.clone(), eng)
        })
        .collect();

    let rows = sqlx::query_as::<_, DbOrder>(
        "SELECT * FROM orders WHERE status IN ('PENDING', 'TRADING') ORDER BY id ASC",
    )
    .fetch_all(&pool)
    .await
    .unwrap_or_default();

    let mut restored = 0usize;
    let mut skipped = 0usize;

    for db_order in &rows {
        let order_id = db_order.id as u64;

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

        let side = if db_order.side == "buy" { Side::Buy } else { Side::Sell };
        let order = Order::new(
            order_id,
            side,
            db_order.price.unwrap_or(0.0),
            remaining,
            TimeInForce::GTC,
            0,
        );

        if let Some(eng) = engines.get_mut(&db_order.symbol) {
            if eng.add_to_book(order).is_ok() {
                restored += 1;
            }
        }
    }
    tracing::info!(
        "Restored {} active orders from DB ({} non-restable skipped)",
        restored,
        skipped,
    );

    // Cancel stale PENDING/TRADING market orders that survived a crash.
    {
        let repo = AccountRepository::new(&pool);
        let stale: Vec<(i64, i64, String, String, f64, f64, Option<f64>, f64)> = sqlx::query_as(
            "SELECT id, user_id, symbol, side, quantity, filled, price, freeze_price
             FROM orders
             WHERE status IN ('PENDING','TRADING') AND order_type='market'",
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
                    let resolved_price = if freeze_price > 0.0 {
                        Some(freeze_price)
                    } else {
                        row_price.filter(|p| *p > 0.0)
                    };
                    if let Some(p) = resolved_price {
                        let _ = repo.release_frozen(user_id, quote_asset, p * remaining).await;
                    }
                }
            }
            let _ = sqlx::query(
                "UPDATE orders SET status='CANCELED', updated_at=NOW() WHERE id=$1",
            )
            .bind(id)
            .execute(&pool)
            .await;
            cleaned += 1;
        }
        if cleaned > 0 {
            tracing::warn!("Cleaned up {} stale market orders from prior runs", cleaned);
        }
    }

    // DB access ends here.
    drop(pool);

    // ----- Aeron setup ---------------------------------------------------
    let client = Arc::new(
        AeronClient::new(AERON_DIR).map_err(|e| anyhow::anyhow!("Aeron init failed: {:?}", e))?,
    );

    let mut subscriber = AeronOrderSubscriber::new(client.clone(), ORDERS_CHANNEL, ORDERS_STREAM)
        .map_err(|e| anyhow::anyhow!("subscriber: {}", e))?;
    let mut ou_pub = AeronOrderUpdatePublisher::new(
        client.clone(),
        ORDER_UPDATE_CHANNEL,
        ORDER_UPDATE_STREAM,
    )
    .map_err(|e| anyhow::anyhow!("ou_pub: {}", e))?;
    let mut trade_pub = AeronTradePublisher::new(client.clone(), TRADE_CHANNEL, TRADE_STREAM)
        .map_err(|e| anyhow::anyhow!("trade_pub: {}", e))?;
    let mut md_pub = AeronMarketDataPublisher::new(
        client.clone(),
        DEPTH_CHANNEL,
        DEPTH_STREAM,
        DEPTH50_STREAM,
        LEVEL2_STREAM,
    )
    .map_err(|e| anyhow::anyhow!("md_pub: {}", e))?;

    tracing::info!(
        "Exchange engine started (symbols={:?}, engines={})",
        symbols,
        engines.len()
    );

    // ----- Latency tracer (optional) ----------------------------------------
    let tracer = spawn_tracer(AERON_DIR, METRICS_CHANNEL, METRICS_STREAM, ENGINE_INSTANCE_ID);
    if tracer.is_some() {
        tracing::info!("Exchange tracer connected (instance_id={})", ENGINE_INSTANCE_ID);
    }

    // ----- Matching thread (dedicated OS thread, spin loop, no tokio) ----
    let symbols_for_thread = symbols.clone();
    let matching_thread = std::thread::Builder::new()
        .name("matching".to_string())
        .spawn(move || {
            let mut trade_seq: u64 = 0;
            let mut depth_seq: u64 = 0;

            let depth_interval = Duration::from_millis(10);
            let start = Instant::now();
            let mut last_depth_pub: HashMap<String, Instant> = symbols_for_thread
                .iter()
                .map(|s| (s.clone(), start))
                .collect();

            loop {
                // do_work first (per Aeron official pattern)
                subscriber.do_work();
                ou_pub.do_work();
                trade_pub.do_work();
                md_pub.do_work();

                // Drain queued inbound messages
                while let Some(msg) = subscriber.poll() {
                    match msg {
                        InboundMsg::NewOrder(req) => {
                            if let Some(ref t) = tracer {
                                t.record_sym(MS_AERON_ORDER_RECV, req.client_order_id, &req.symbol);
                            }
                            let ts = now_ns();
                            let symbol = std::str::from_utf8(&req.symbol)
                                .unwrap_or("")
                                .trim_end_matches('\0')
                                .to_string();

                            let engine = match engines.get_mut(&symbol) {
                                Some(eng) => eng,
                                None => {
                                    let _ = ou_pub.publish(&OrderUpdateMsg::rejected(
                                        req.client_order_id,
                                        req.participant_id,
                                        1, // reason: unknown symbol
                                        ts,
                                    ));
                                    continue;
                                }
                            };

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

                            let side_str = if req.side == 0 { "buy" } else { "sell" };
                            let req_price: f64 = req.price;
                            let req_qty: f64 = req.quantity;
                            let req_coid: u64 = req.client_order_id;
                            tracing::debug!("NewOrder: symbol={} side={} price={} qty={} id={}", symbol, side_str, req_price, req_qty, req_coid);

                            match engine.place_order(order) {
                                Err(e) => {
                                    tracing::warn!("place_order FAILED: symbol={} side={} price={} err={:?}", symbol, side_str, req_price, e);
                                    if let Some(ref t) = tracer {
                                        t.record_sym(MS_MATCHING_DONE, req.client_order_id, &req.symbol);
                                    }
                                    let _ = ou_pub.publish(&OrderUpdateMsg::rejected(
                                        req.client_order_id,
                                        req.participant_id,
                                        2, // reason: engine error
                                        ts,
                                    ));
                                    if let Some(ref t) = tracer {
                                        t.record_sym(MS_AERON_UPDATE_SEND, req.client_order_id, &req.symbol);
                                    }
                                }
                                Ok(result) => {
                                    if let Some(ref t) = tracer {
                                        t.record_sym(MS_MATCHING_DONE, req.client_order_id, &req.symbol);
                                    }
                                    tracing::debug!("place_order OK: symbol={} side={} status={:?} filled={}", symbol, side_str, result.status, result.filled);
                                    // Publish per-fill trade notifications.
                                    let sym_bytes = symbol_bytes(&symbol);
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
                                            symbol: sym_bytes,
                                        };
                                        let _ = trade_pub.publish(&trade);
                                    }

                                    // Publish a single OrderUpdateMsg reflecting the final status.
                                    let last_price = result
                                        .fills
                                        .last()
                                        .map(|f| f.1)
                                        .unwrap_or(req.price);
                                    let update = match result.status {
                                        OrderStatus::Accepted => OrderUpdateMsg::accepted(
                                            result.order_id,
                                            req.client_order_id,
                                            req.participant_id,
                                            ts,
                                        ),
                                        OrderStatus::Filled => OrderUpdateMsg::filled(
                                            result.order_id,
                                            req.client_order_id,
                                            req.participant_id,
                                            last_price,
                                            result.filled,
                                            ts,
                                        ),
                                        OrderStatus::PartiallyFilled => {
                                            let remaining = req.quantity - result.filled;
                                            OrderUpdateMsg::partial_fill(
                                                result.order_id,
                                                req.client_order_id,
                                                req.participant_id,
                                                last_price,
                                                result.filled,
                                                remaining,
                                                ts,
                                            )
                                        }
                                        OrderStatus::Cancelled => OrderUpdateMsg::cancelled(
                                            result.order_id,
                                            req.client_order_id,
                                            req.participant_id,
                                            req.quantity - result.filled,
                                            ts,
                                        ),
                                        OrderStatus::Rejected => OrderUpdateMsg::rejected(
                                            req.client_order_id,
                                            req.participant_id,
                                            3,
                                            ts,
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
                            let mut cancelled_qty = 0.0;
                            let mut found = false;
                            for eng in engines.values_mut() {
                                if let Ok(res) = eng.cancel_order(req.order_id) {
                                    cancelled_qty = res.cancelled_quantity;
                                    found = true;
                                    break;
                                }
                            }
                            if found {
                                let _ = ou_pub.publish(&OrderUpdateMsg::cancelled(
                                    req.order_id,
                                    0,
                                    0,
                                    cancelled_qty,
                                    ts,
                                ));
                            }
                        }
                    }
                }

                // Per-symbol depth snapshot every 10ms.
                let now_instant = Instant::now();
                for (symbol, engine) in engines.iter() {
                    let last = last_depth_pub.get(symbol).copied().unwrap_or(now_instant);
                    if now_instant.duration_since(last) >= depth_interval {
                        if let Some(slot) = last_depth_pub.get_mut(symbol) {
                            *slot = now_instant;
                        }
                        depth_seq += 1;
                        let mut snap = DepthSnapshotEvent::new(now_ns(), depth_seq);
                        snap.symbol = symbol_bytes(symbol);

                        // get_top_levels(limit, is_buy): true = buy_book (bids), false = sell_book (asks)
                        let bids = engine.get_top_levels(20, true);
                        let asks = engine.get_top_levels(20, false);
                        if symbol == "ETH_USDT" {
                            tracing::debug!("depth snap: {} bids={} asks={}", symbol, bids.len(), asks.len());
                            if let Some((p, q)) = bids.first() { tracing::debug!("  best bid: {} @ {}", q, p); }
                            if let Some((p, q)) = asks.first() { tracing::debug!("  best ask: {} @ {}", q, p); }
                        }
                        for (i, (p, q)) in bids.iter().take(20).enumerate() {
                            snap.bids[i] = (*p, *q);
                        }
                        for (i, (p, q)) in asks.iter().take(20).enumerate() {
                            snap.asks[i] = (*p, *q);
                        }
                        snap.num_bids = bids.len().min(20) as u8;
                        snap.num_asks = asks.len().min(20) as u8;

                        let _ = md_pub.publish_depth(&snap);
                    }
                }

                std::hint::spin_loop();
            }
        })?;

    tokio::signal::ctrl_c().await?;
    tracing::info!("Shutting down exchange engine...");
    matching_thread.thread().unpark();

    Ok(())
}
