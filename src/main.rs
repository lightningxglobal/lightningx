use dashmap::DashMap;
use lightning_exchange::{
    api::{router, AppState},
    db,
    engine::{MatchingEngine, PoolConfig},
    ws_handler::market_data_broadcaster,
};
use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicU64;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tower_http::cors::{Any, CorsLayer};

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

    let (market_tx, _) = broadcast::channel::<String>(1024);

    let state = AppState {
        db: Arc::new(pool),
        engines: Arc::new(engines),
        market_tx: Arc::new(market_tx),
        user_tx: Arc::new(DashMap::new()),
        next_order_id: Arc::new(AtomicU64::new(1)),
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
