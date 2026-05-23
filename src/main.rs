use dashmap::DashMap;
use lightning_exchange::{
    account_repository::AccountRepository,
    api::{router, AppState},
    db,
    engine::{MatchingEngine, PoolConfig},
    models::DbOrder,
    order::{Order, Side, TimeInForce},
    ws_handler::market_data_broadcaster,
};
use sqlx::PgPool;
use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicU64;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tower_http::cors::{Any, CorsLayer};

/// Plant limit orders for ETH_USDT, SOL_USDT and BTC_USDT on first startup.
/// Idempotent per symbol: skips ETH/SOL if ETH_USDT already has active orders,
/// skips BTC if BTC_USDT already has active orders.
async fn seed_demo_if_empty(pool: &PgPool, engines: &DashMap<String, Arc<Mutex<MatchingEngine>>>) {
    let eth_already: Option<i64> = sqlx::query_scalar(
        "SELECT id FROM orders WHERE symbol='ETH_USDT' AND status IN ('PENDING','TRADING') LIMIT 1",
    )
    .fetch_optional(pool)
    .await
    .unwrap_or(None);

    let btc_already: Option<i64> = sqlx::query_scalar(
        "SELECT id FROM orders WHERE symbol='BTC_USDT' AND status IN ('PENDING','TRADING') LIMIT 1",
    )
    .fetch_optional(pool)
    .await
    .unwrap_or(None);

    if eth_already.is_some() && btc_already.is_some() {
        tracing::info!("Demo data already present, skipping seed");
        return;
    }

    let demo_id: i64 = match sqlx::query_scalar::<_, i64>(
        "INSERT INTO users (email, password_hash, full_name, kyc_status)
         VALUES ('demo@lightning.exchange', 'x', 'Demo User', 'verified')
         ON CONFLICT (email) DO UPDATE SET id = users.id RETURNING id",
    )
    .fetch_one(pool)
    .await
    {
        Ok(id) => id,
        Err(e) => { tracing::warn!("seed_demo: user upsert failed: {e}"); return; }
    };

    let _ = sqlx::query(
        "INSERT INTO accounts (user_id, asset, balance, frozen)
         VALUES ($1,'USDT',20000,0),($1,'ETH',5,0),($1,'SOL',100,0),($1,'BTC',5,0)
         ON CONFLICT (user_id, asset) DO UPDATE
         SET balance = GREATEST(accounts.balance, EXCLUDED.balance), updated_at=NOW()",
    )
    .bind(demo_id)
    .execute(pool)
    .await;

    let plans: &[(&str, &str, &[f64], f64, &str, &str)] = &[
        ("ETH_USDT","buy", &[2990.,2980.,2970.,2960.,2950.], 0.5, "ETH","USDT"),
        ("ETH_USDT","sell",&[3010.,3020.,3030.,3040.,3050.], 0.5, "ETH","USDT"),
        ("SOL_USDT","buy", &[149.,148.,147.,146.,145.],     10.0, "SOL","USDT"),
        ("SOL_USDT","sell",&[151.,152.,153.,154.,155.],     10.0, "SOL","USDT"),
        ("BTC_USDT","buy", &[50900.,50800.,50700.,50600.,50500.], 0.01, "BTC","USDT"),
        ("BTC_USDT","sell",&[51100.,51200.,51300.,51400.,51500.], 0.01, "BTC","USDT"),
    ];

    let repo = AccountRepository::new(pool);
    let mut placed = 0usize;

    for (symbol, side, prices, qty, base, quote) in plans {
        // Skip symbols whose demo orders are already present.
        let symbol_already = if *symbol == "BTC_USDT" { btc_already.is_some() } else { eth_already.is_some() };
        if symbol_already { continue; }

        let Some(eng_ref) = engines.get(*symbol) else { continue };
        let engine = eng_ref.value().clone();
        let engine_side = if *side == "buy" { Side::Buy } else { Side::Sell };

        for &price in *prices {
            let freeze = if *side == "buy" {
                repo.freeze_for_buy(demo_id, quote, price * qty).await
            } else {
                repo.freeze_for_sell(demo_id, base, *qty).await
            };
            if freeze.is_err() { continue; }

            // Limit buys: freeze_price = price (USDT cost backing).
            // Limit sells: 0.0 (sells freeze base_asset by quantity).
            let seed_fp = if *side == "buy" { price } else { 0.0 };
            let db_order = match sqlx::query_as::<_, DbOrder>(
                "INSERT INTO orders (id,user_id,symbol,side,order_type,price,quantity,filled,status,freeze_price)
                 VALUES (nextval('orders_id_seq'),$1,$2,$3,'limit',$4,$5,0,'PENDING',$6) RETURNING *",
            )
            .bind(demo_id).bind(*symbol).bind(*side).bind(price).bind(*qty).bind(seed_fp)
            .fetch_one(pool).await {
                Ok(o) => o,
                Err(_) => {
                    let asset = if *side == "buy" { *quote } else { *base };
                    let amt   = if *side == "buy" { price * qty } else { *qty };
                    let _ = repo.release_frozen(demo_id, asset, amt).await;
                    continue;
                }
            };

            let order = Order::new(db_order.id as u64, engine_side, price, *qty, TimeInForce::GTC, 0);
            let mut eng = engine.lock().unwrap();
            let _ = eng.add_to_book(order);
            placed += 1;
        }
    }
    tracing::info!("Demo data seeded: {} orders placed", placed);

    // Insert dense seed trade history (3 hours, every 25 s) so K-line charts
    // show meaningful OHLCV variation across 1m / 5m / 15m / 1h intervals.
    if eth_already.is_none() {
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
    }
    if btc_already.is_none() {
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

    // Recover active orders from DB back into engines so book state survives restarts.
    let max_order_id: u64 = {
        let rows = sqlx::query_as::<_, DbOrder>(
            "SELECT * FROM orders WHERE status IN ('PENDING', 'TRADING') ORDER BY id ASC",
        )
        .fetch_all(&pool)
        .await
        .unwrap_or_default();

        let mut max_id: u64 = 0;
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
            // Only GTC orders can survive in the book; restore everything as GTC.
            let order = Order::new(
                order_id,
                side,
                db_order.price.unwrap_or(0.0),
                remaining,
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
                        engines.get(&symbol).and_then(|e| {
                            let eng = e.value().lock().unwrap();
                            eng.get_top_levels(1, false).first().map(|(p, _)| *p)
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

    seed_demo_if_empty(&pool, &engines).await;

    let (market_tx, _) = broadcast::channel::<String>(1024);

    let state = AppState {
        db: Arc::new(pool),
        engines: Arc::new(engines),
        market_tx: Arc::new(market_tx),
        user_tx: Arc::new(DashMap::new()),
        next_order_id: Arc::new(AtomicU64::new(max_order_id + 1)),
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
