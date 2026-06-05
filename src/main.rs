use dashmap::DashMap;
use lightning_exchange::{
    account_repository::AccountRepository,
    api::{router, AppState, AccountCache, UserTxRegistry},
    db,
    engine::{MatchingEngine, PoolConfig},
    models::DbOrder,
    order::{Order, Side, TimeInForce},
    symbol_rules::SymbolRules,
    ws_handler::market_data_broadcaster,
};
use sqlx::PgPool;
use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicU64;
use tokio::net::TcpListener;
use tower_http::cors::{Any, CorsLayer};

/// Seed demo K-line trade history on first startup so charts show meaningful data.
/// Idempotent: skips a symbol if it already has any trades in the DB.
/// Does NOT place limit orders — the market-maker provides all live orderbook liquidity.
async fn seed_demo_if_empty(pool: &PgPool) {
    let eth_has_trades: bool = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM trades WHERE symbol='ETH_USDT' LIMIT 1",
    )
    .fetch_one(pool)
    .await
    .unwrap_or(0) > 0;

    let btc_has_trades: bool = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM trades WHERE symbol='BTC_USDT' LIMIT 1",
    )
    .fetch_one(pool)
    .await
    .unwrap_or(0) > 0;

    if eth_has_trades && btc_has_trades {
        tracing::info!("Demo trade history already present, skipping seed");
        return;
    }

    // Insert dense seed trade history (3 hours, every 25 s) so K-line charts
    // show meaningful OHLCV variation across 1m / 5m / 15m / 1h intervals.
    if !eth_has_trades {
        let _ = sqlx::query(
            "INSERT INTO trades (symbol, price, quantity, buy_fee, sell_fee, created_at)
             SELECT 'ETH_USDT',
                    3000.0
                      + 80.0 * sin(extract(epoch from ts) / 1800.0)
                      + 15.0 * sin(extract(epoch from ts) /  300.0)
                      +  4.0 * sin(extract(epoch from ts) /   60.0),
                    0.05 + 0.08 * abs(sin(extract(epoch from ts) / 47.0)),
                    0.0, 0.0,
                    ts + (((extract(epoch from ts)::bigint * 1337 + 271828) % 9999) * interval '1 microsecond')
             FROM generate_series(
                 NOW() - INTERVAL '3 hours',
                 NOW() - INTERVAL '30 seconds',
                 '25 seconds'::interval
             ) AS gs(ts)",
        )
        .execute(pool)
        .await;

        let _ = sqlx::query(
            "INSERT INTO trades (symbol, price, quantity, buy_fee, sell_fee, created_at)
             SELECT 'SOL_USDT',
                    150.0
                      + 6.0 * sin(extract(epoch from ts) / 1800.0)
                      + 1.5 * sin(extract(epoch from ts) /  300.0)
                      + 0.4 * sin(extract(epoch from ts) /   60.0),
                    0.5 + 0.8 * abs(sin(extract(epoch from ts) / 53.0)),
                    0.0, 0.0,
                    ts + (((extract(epoch from ts)::bigint * 2311 + 161803) % 9999) * interval '1 microsecond')
             FROM generate_series(
                 NOW() - INTERVAL '3 hours',
                 NOW() - INTERVAL '30 seconds',
                 '25 seconds'::interval
             ) AS gs(ts)",
        )
        .execute(pool)
        .await;
        tracing::info!("Demo trade history seeded for ETH_USDT + SOL_USDT");
    }

    if !btc_has_trades {
        let _ = sqlx::query(
            "INSERT INTO trades (symbol, price, quantity, buy_fee, sell_fee, created_at)
             SELECT 'BTC_USDT',
                    51000.0
                      + 400.0 * sin(extract(epoch from ts) / 1800.0)
                      +  80.0 * sin(extract(epoch from ts) /  300.0)
                      +  20.0 * sin(extract(epoch from ts) /   60.0),
                    0.002 + 0.004 * abs(sin(extract(epoch from ts) / 41.0)),
                    0.0, 0.0,
                    ts + (((extract(epoch from ts)::bigint * 997 + 314159) % 9999) * interval '1 microsecond')
             FROM generate_series(
                 NOW() - INTERVAL '3 hours',
                 NOW() - INTERVAL '30 seconds',
                 '25 seconds'::interval
             ) AS gs(ts)",
        )
        .execute(pool)
        .await;
        tracing::info!("Demo trade history seeded for BTC_USDT");
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://user:password@localhost:5432/mydb".to_string());
    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".to_string());

    tracing::info!("Connecting to database…");
    let pool = db::create_pool(&database_url).await?;
    db::run_migrations(&pool).await?;
    tracing::info!("Migrations applied");

    let symbols = ["BTC_USDT", "ETH_USDT", "SOL_USDT"];
    let engines: DashMap<String, Arc<Mutex<MatchingEngine>>> = symbols
        .iter()
        .map(|s| {
            let eng = MatchingEngine::new(PoolConfig::default())
                .expect("Failed to create matching engine");
            (s.to_string(), Arc::new(Mutex::new(eng)))
        })
        .collect();

    // Upsert configured symbols into DB so data_server can discover them.
    for sym in &symbols {
        let parts: Vec<&str> = sym.split('_').collect();
        let (base, quote): (&str, &str) = if parts.len() == 2 { (parts[0], parts[1]) } else { (sym, "") };
        let _ = sqlx::query(
            "INSERT INTO symbols (symbol, base_asset, quote_asset) VALUES ($1, $2, $3)
             ON CONFLICT (symbol) DO NOTHING",
        )
        .bind(sym)
        .bind(base)
        .bind(quote)
        .execute(&pool)
        .await;
    }

    // Cancel stale orders from automated accounts (market-maker + demo seed user).
    // These accounts always restart fresh; stale orders at outdated prices would cross
    // new market-maker orders and cause a burst of trades and a CPU spike.
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
            tracing::warn!("Canceled {} stale bot orders (market-maker + demo) from previous session", bot_stale.len());
        }
    }

    // Recover active orders from DB back into engines so book state survives restarts.
    let max_order_id: u64 = {
        // Fetch global max ID across all orders (not just active ones) so the
        // next_order_id counter never reuses an ID from a previous session.
        let global_max: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(id), 0) FROM orders")
            .fetch_one(&pool)
            .await
            .unwrap_or(0);

        let rows = sqlx::query_as::<_, DbOrder>(
            "SELECT * FROM orders WHERE status IN ('PENDING', 'TRADING') ORDER BY id ASC",
        )
        .fetch_all(&pool)
        .await
        .unwrap_or_default();

        let mut max_id: u64 = global_max as u64;
        let mut restored = 0usize;
        let mut skipped = 0usize;
        for db_order in &rows {
            let order_id = db_order.id as u64;
            if order_id > max_id { max_id = order_id; }

            // Market orders (and IOC orders missing a price) can never rest in the
            // book — they were left in PENDING by a crash mid-flight. Skip them
            // here; the cleanup pass below cancels them and releases frozen funds.
            if db_order.order_type == "market"
                || (db_order.order_type == "ioc" && db_order.price.is_none())
            {
                skipped += 1;
                continue;
            }

            let remaining = db_order.quantity - db_order.filled;
            if remaining <= 0.0 { continue; }

            let side = if db_order.side == "buy" { Side::Buy } else { Side::Sell };
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
            // Only GTC orders can survive in the book; restore everything as GTC.
            let order = Order::new(
                order_id,
                side,
                price_ticks,
                quantity_lots,
                TimeInForce::GTC,
                0,
            );

            if let Some(eng_ref) = engines.get(&db_order.symbol) {
                let mut eng = eng_ref.lock().unwrap();
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
        max_id
    };

    // Cancel any stale PENDING/TRADING market orders left from prior crashes.
    // Market orders never rest in the book, so survivors are stuck; release
    // their frozen funds and flip them to CANCELED. Uses the persisted
    // freeze_price so the release amount is exact; legacy rows whose
    // freeze_price wasn't backfilled fall back to the limit `price` column
    // (correct for limit buys) or the current best ask (market buys).
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
                    } else if let Some(p) = row_price.filter(|p| *p > 0.0) {
                        Some(p)
                    } else {
                        let rules = SymbolRules::for_symbol(&symbol);
                        engines.get(&symbol).and_then(|e| {
                            let eng = e.value().lock().unwrap();
                            eng.get_top_levels(1, false)
                                .first()
                                .map(|(p, _)| rules.ticks_to_price(*p))
                        })
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

    seed_demo_if_empty(&pool).await;

    let read_pool = Arc::new(lightning_exchange::read_actor::ReadActorPool::new());
    let market_fanout = Arc::new(lightning_exchange::api::MarketFanout::new_with_actors(
        read_pool.market_senders(),
    ));

    let state = AppState {
        db: Arc::new(pool),
        engines: Some(Arc::new(engines)),
        market_fanout,
        public_market_data_enabled: true,
        response_stream_id: lightning_exchange::aeron_channels::ORDER_UPDATE_STREAM,
        user_tx: Arc::new(UserTxRegistry::new()),
        next_order_id: Arc::new(AtomicU64::new(max_order_id + 1)),
        aeron_cmd_tx: None,
        liq_cmd_tx: None,
        pending_meta: Arc::new(DashMap::new()),
        pending_orders: Arc::new(DashMap::new()),
        last_depth: Arc::new(DashMap::new()),
        last_ticker: Arc::new(DashMap::new()),
        last_trade_price: Arc::new(DashMap::new()),
        tracer: None,
        account_cache: AccountCache::default(),
        valid_symbols: Arc::new(std::collections::HashSet::new()),
        redis: None,
        persist_pub: None,
        vwap_cache: Arc::new(dashmap::DashMap::new()),
        write_pool: Arc::new(lightning_exchange::write_actor::WriteActorPool::new()),
        read_pool,
        risk_engine: lightning_exchange::desk::risk::RiskEngine::new(),
    };

    tokio::spawn(market_data_broadcaster(state.clone()));

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = router(state).layer(cors);

    let addr = format!("0.0.0.0:{}", port);
    tracing::info!("Exchange API listening on {}", addr);
    let listener = TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
