use matching_engine::{
    api::{router, AppState},
    db,
};
use std::sync::Arc;
use tokio::net::TcpListener;
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

    let state = AppState { db: Arc::new(pool) };

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
