/// Lightning Data Service — read-only historical data API on port 3002.
/// Serves tickers, klines, orders, trades, and account balances from PostgreSQL.
/// No matching engine, no WebSocket, no fund mutation.
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use jsonwebtoken::{decode, DecodingKey, Validation};
use lightning_exchange::{account_repository::AccountRepository, db};
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::PgPool;
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};

#[derive(Clone)]
struct AppState {
    db: Arc<PgPool>,
    jwt_secret: Arc<String>,
}

// ─── JWT ──────────────────────────────────────────────────────────────────────

#[derive(Debug, serde::Deserialize)]
struct Claims {
    sub: i64,
}

fn auth_user(headers: &HeaderMap, secret: &str) -> Result<i64, (StatusCode, Json<Value>)> {
    let token = headers
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .ok_or_else(|| {
            (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error": "Missing token"})),
            )
        })?;

    let data = decode::<Claims>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        &Validation::default(),
    )
    .map_err(|e| {
        (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": format!("Invalid token: {}", e)})),
        )
    })?;

    Ok(data.claims.sub)
}

// ─── Handlers ─────────────────────────────────────────────────────────────────

async fn handle_tickers(State(s): State<AppState>) -> impl IntoResponse {
    let rows = sqlx::query(
        "SELECT symbol,
                MAX(price) FILTER (WHERE created_at > NOW() - INTERVAL '24 hours') AS high_24h,
                MIN(price) FILTER (WHERE created_at > NOW() - INTERVAL '24 hours') AS low_24h,
                SUM(quantity) FILTER (WHERE created_at > NOW() - INTERVAL '24 hours') AS vol_24h,
                (SELECT price FROM trades t2 WHERE t2.symbol = t.symbol ORDER BY created_at DESC LIMIT 1) AS last_price,
                (SELECT price FROM trades t3 WHERE t3.symbol = t.symbol AND t3.created_at > NOW() - INTERVAL '24 hours' ORDER BY created_at ASC LIMIT 1) AS open_24h
         FROM trades t GROUP BY symbol ORDER BY symbol",
    )
    .fetch_all(s.db.as_ref())
    .await;

    match rows {
        Ok(rows) => {
            use sqlx::Row;
            let tickers: Vec<Value> = rows
                .iter()
                .map(|r| {
                    let last: Option<f64> = r.get("last_price");
                    let open: Option<f64> = r.get("open_24h");
                    let change = match (last, open) {
                        (Some(l), Some(o)) if o != 0.0 => (l - o) / o * 100.0,
                        _ => 0.0,
                    };
                    json!({
                        "symbol": r.get::<String, _>("symbol"),
                        "last":   last,
                        "high":   r.get::<Option<f64>, _>("high_24h"),
                        "low":    r.get::<Option<f64>, _>("low_24h"),
                        "volume": r.get::<Option<f64>, _>("vol_24h"),
                        "change": change,
                    })
                })
                .collect();
            (StatusCode::OK, Json(json!(tickers))).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

#[derive(Deserialize)]
struct KlinesQuery {
    symbol: String,
    interval: Option<String>,
    limit: Option<i64>,
}

async fn handle_klines(
    State(s): State<AppState>,
    Query(params): Query<KlinesQuery>,
) -> impl IntoResponse {
    let _interval = params.interval.unwrap_or_else(|| "1m".to_string());
    let limit = params.limit.unwrap_or(200).min(1000);

    let rows = sqlx::query(
        "SELECT
           extract(epoch FROM date_trunc('minute', created_at))::bigint AS time,
           (array_agg(price ORDER BY created_at ASC))[1] AS open,
           max(price) AS high,
           min(price) AS low,
           (array_agg(price ORDER BY created_at DESC))[1] AS close,
           sum(quantity) AS volume
         FROM trades
         WHERE symbol = $1
         GROUP BY date_trunc('minute', created_at)
         ORDER BY time ASC
         LIMIT $2",
    )
    .bind(&params.symbol)
    .bind(limit)
    .fetch_all(s.db.as_ref())
    .await;

    match rows {
        Ok(rows) => {
            use sqlx::Row;
            let candles: Vec<Value> = rows
                .iter()
                .map(|r| {
                    json!({
                        "time":   r.get::<i64, _>("time"),
                        "open":   r.get::<f64, _>("open"),
                        "high":   r.get::<f64, _>("high"),
                        "low":    r.get::<f64, _>("low"),
                        "close":  r.get::<f64, _>("close"),
                        "volume": r.get::<f64, _>("volume"),
                    })
                })
                .collect();
            (StatusCode::OK, Json(json!(candles))).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

#[derive(Deserialize)]
struct OrderQuery {
    symbol: Option<String>,
    status: Option<String>,
    limit: Option<i64>,
}

async fn handle_orders(
    State(s): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<OrderQuery>,
) -> impl IntoResponse {
    let user_id = match auth_user(&headers, &s.jwt_secret) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    let limit = q.limit.unwrap_or(50).min(500);

    // Translate frontend virtual status aliases to DB status values.
    let status_filter: Option<Vec<&str>> = match q.status.as_deref() {
        Some("open")    => Some(vec!["PENDING", "TRADING"]),
        Some("history") => Some(vec!["COMPLETED", "CANCELED", "REJECTED"]),
        Some(s)         => Some(vec![s]),
        None            => None,
    };

    use lightning_exchange::models::DbOrder;
    let orders = match status_filter {
        Some(statuses) => sqlx::query_as::<_, DbOrder>(
            "SELECT * FROM orders
             WHERE user_id = $1
               AND ($2::text IS NULL OR symbol = $2)
               AND status = ANY($3)
             ORDER BY created_at DESC LIMIT $4",
        )
        .bind(user_id)
        .bind(&q.symbol)
        .bind(&statuses)
        .bind(limit)
        .fetch_all(s.db.as_ref())
        .await,
        None => sqlx::query_as::<_, DbOrder>(
            "SELECT * FROM orders
             WHERE user_id = $1
               AND ($2::text IS NULL OR symbol = $2)
             ORDER BY created_at DESC LIMIT $3",
        )
        .bind(user_id)
        .bind(&q.symbol)
        .bind(limit)
        .fetch_all(s.db.as_ref())
        .await,
    };

    match orders {
        Ok(rows) => (StatusCode::OK, Json(json!(rows))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

async fn handle_order(
    State(s): State<AppState>,
    headers: HeaderMap,
    Path(order_id): Path<i64>,
) -> impl IntoResponse {
    let user_id = match auth_user(&headers, &s.jwt_secret) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };

    use lightning_exchange::models::DbOrder;
    let row = sqlx::query_as::<_, DbOrder>(
        "SELECT * FROM orders WHERE id = $1 AND user_id = $2",
    )
    .bind(order_id)
    .bind(user_id)
    .fetch_optional(s.db.as_ref())
    .await;

    match row {
        Ok(Some(order)) => (StatusCode::OK, Json(json!(order))).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "Order not found"})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

async fn handle_accounts(State(s): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    let user_id = match auth_user(&headers, &s.jwt_secret) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    let repo = AccountRepository::new(&s.db);
    match repo.get_all_accounts(user_id).await {
        Ok(accounts) => (StatusCode::OK, Json(json!(accounts))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

#[derive(Deserialize)]
struct TradeQuery {
    symbol: Option<String>,
    limit: Option<i64>,
}

async fn handle_trades(
    State(s): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<TradeQuery>,
) -> impl IntoResponse {
    let user_id = match auth_user(&headers, &s.jwt_secret) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    let limit = q.limit.unwrap_or(50).min(500);

    // DISTINCT ON (t.id) collapses self-trades (same user on both sides) to a
    // single row; the inner ORDER BY t.id, o.id picks deterministically.
    let trades = sqlx::query(
        "SELECT * FROM (
             SELECT DISTINCT ON (t.id)
                 t.id, t.symbol, t.price, t.quantity, t.created_at,
                 CASE WHEN o.id = t.buy_order_id THEN 'buy' ELSE 'sell' END AS side
             FROM trades t
             JOIN orders o ON o.id = t.buy_order_id OR o.id = t.sell_order_id
             WHERE o.user_id = $1
               AND ($2::text IS NULL OR t.symbol = $2)
             ORDER BY t.id, o.id
         ) sub
         ORDER BY created_at DESC LIMIT $3",
    )
    .bind(user_id)
    .bind(&q.symbol)
    .bind(limit)
    .fetch_all(s.db.as_ref())
    .await;

    match trades {
        Ok(rows) => {
            use sqlx::Row;
            let out: Vec<Value> = rows
                .iter()
                .map(|r| {
                    json!({
                        "id": r.get::<i64, _>("id"),
                        "symbol": r.get::<String, _>("symbol"),
                        "price": r.get::<f64, _>("price"),
                        "quantity": r.get::<f64, _>("quantity"),
                        "created_at": r.get::<chrono::DateTime<chrono::Utc>, _>("created_at"),
                        "side": r.get::<String, _>("side"),
                    })
                })
                .collect();
            (StatusCode::OK, Json(json!(out))).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

// Returns configured trading symbols from the `symbols` table (seeded by
// exchange-server on startup), falling back to distinct symbols in `orders`.
async fn handle_symbols(State(s): State<AppState>) -> impl IntoResponse {
    // Try the canonical symbols table first.
    let rows = sqlx::query_scalar::<_, String>(
        "SELECT symbol FROM symbols WHERE active = TRUE ORDER BY symbol",
    )
    .fetch_all(s.db.as_ref())
    .await;

    let symbols = match rows {
        Ok(v) if !v.is_empty() => v,
        _ => {
            // Fall back to order history for older deployments.
            sqlx::query_scalar::<_, String>(
                "SELECT DISTINCT symbol FROM orders ORDER BY symbol",
            )
            .fetch_all(s.db.as_ref())
            .await
            .unwrap_or_default()
        }
    };

    (StatusCode::OK, Json(json!(symbols))).into_response()
}

// ─── Router ───────────────────────────────────────────────────────────────────

fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/tickers", get(handle_tickers))
        .route("/api/symbols", get(handle_symbols))
        .route("/api/klines", get(handle_klines))
        .route("/api/orders", get(handle_orders))
        .route("/api/orders/:id", get(handle_order))
        .route("/api/accounts", get(handle_accounts))
        .route("/api/trades", get(handle_trades))
        .with_state(state)
}

// ─── Main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://user:password@localhost:5432/mydb".to_string());
    let port = std::env::var("DATA_PORT").unwrap_or_else(|_| "3002".to_string());
    let jwt_secret = std::env::var("JWT_SECRET")
        .unwrap_or_else(|_| "exchange_jwt_secret_change_in_prod".to_string());

    tracing::info!("Connecting to database…");
    let pool = db::create_pool(&database_url).await?;
    tracing::info!("Database connected");

    let state = AppState {
        db: Arc::new(pool),
        jwt_secret: Arc::new(jwt_secret),
    };

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = router(state).layer(cors);

    let addr = format!("0.0.0.0:{}", port);
    tracing::info!("Lightning Data Service listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
