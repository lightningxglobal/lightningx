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

/// Plant limit orders for ETH_USDT and SOL_USDT on first startup.
/// No-ops if ETH_USDT already has active orders (idempotent).
async fn seed_demo_if_empty(pool: &PgPool, engines: &DashMap<String, Arc<Mutex<MatchingEngine>>>) {
    let already: Option<i64> = sqlx::query_scalar(
        "SELECT id FROM orders WHERE symbol='ETH_USDT' AND status IN ('PENDING','TRADING') LIMIT 1",
    )
    .fetch_optional(pool)
    .await
    .unwrap_or(None);

    if already.is_some() {
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
         VALUES ($1,'USDT',20000,0),($1,'ETH',5,0),($1,'SOL',100,0)
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
    ];

    let repo = AccountRepository::new(pool);
    let mut placed = 0usize;

    for (symbol, side, prices, qty, base, quote) in plans {
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

            let db_order = match sqlx::query_as::<_, DbOrder>(
                "INSERT INTO orders (id,user_id,symbol,side,order_type,price,quantity,filled,status)
                 VALUES (nextval('orders_id_seq'),$1,$2,$3,'limit',$4,$5,0,'PENDING') RETURNING *",
            )
            .bind(demo_id).bind(*symbol).bind(*side).bind(price).bind(*qty)
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
    tracing::info!("Demo data seeded: {} orders placed for ETH_USDT and SOL_USDT", placed);

    // Insert seed trades so tickers have a `last` price immediately.
    let _ = sqlx::query(
        "INSERT INTO trades (symbol, price, quantity, buy_fee, sell_fee)
         VALUES ('ETH_USDT', 3000.0, 0.001, 0.0, 0.0),
                ('SOL_USDT', 150.0,  0.01,  0.0, 0.0)",
    )
    .execute(pool)
    .await;
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
        for db_order in &rows {
            let order_id = db_order.id as u64;
            if order_id > max_id { max_id = order_id; }

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
                let _ = eng.add_to_book(order);
            }
        }
        tracing::info!("Restored {} active orders from DB", rows.len());
        max_id
    };

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
