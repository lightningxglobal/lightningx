use lightning_exchange::{
    db,
    desk_server::{DeskAppState, desk_market_data_broadcaster, desk_ws_handler},
    rate_limit::{RateLimiter, RateLimitPolicy},
    snowflake::SnowflakeIdGenerator,
};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tower_http::cors::{Any, CorsLayer};
use axum::{routing::get, Router};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://user:password@localhost:5432/mydb".to_string());
    let port = std::env::var("DESK_PORT").unwrap_or_else(|_| "3001".to_string());
    let desk_id: u64 = std::env::var("DESK_ID")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let upstream_ws_url = std::env::var("UPSTREAM_WS_URL")
        .unwrap_or_else(|_| "ws://127.0.0.1:3000/ws".to_string());

    tracing::info!("Connecting to database…");
    let pool = db::create_pool(&database_url).await?;
    db::run_migrations(&pool).await?;
    tracing::info!("Migrations applied");

    let (market_tx, _) = broadcast::channel::<String>(1024);

    let state = DeskAppState {
        db: Arc::new(pool),
        upstream_ws_url: Arc::new(upstream_ws_url),
        market_tx: Arc::new(market_tx),
        rate_limiter: Arc::new(parking_lot::Mutex::new(
            RateLimiter::new(RateLimitPolicy::default_trading()),
        )),
        id_gen: Arc::new(SnowflakeIdGenerator::new(desk_id)),
        last_mid_price: Arc::new(parking_lot::RwLock::new(0.0)),
    };

    tokio::spawn(desk_market_data_broadcaster(state.clone()));

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .route("/ws", get(desk_ws_handler))
        .with_state(state)
        .layer(cors);

    let addr = format!("0.0.0.0:{}", port);
    tracing::info!("Desk Server listening on {} (upstream: {})",
        addr,
        std::env::var("UPSTREAM_WS_URL").unwrap_or_else(|_| "ws://127.0.0.1:3000/ws".to_string())
    );
    let listener = TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
