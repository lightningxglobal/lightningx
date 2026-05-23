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
use lightning_exchange::{account_repository::AccountRepository, db, positions};
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
        "SELECT s.symbol,
                COALESCE(
                  MAX(t.price) FILTER (WHERE t.created_at > NOW() - INTERVAL '24 hours'),
                  MAX(t.price)
                ) AS high_24h,
                COALESCE(
                  MIN(t.price) FILTER (WHERE t.created_at > NOW() - INTERVAL '24 hours'),
                  MIN(t.price)
                ) AS low_24h,
                COALESCE(
                  SUM(t.quantity) FILTER (WHERE t.created_at > NOW() - INTERVAL '24 hours'),
                  SUM(t.quantity)
                ) AS vol_24h,
                (SELECT price FROM trades t2 WHERE t2.symbol = s.symbol ORDER BY created_at DESC LIMIT 1) AS last_price,
                COALESCE(
                  (SELECT price FROM trades t3 WHERE t3.symbol = s.symbol AND t3.created_at > NOW() - INTERVAL '24 hours' ORDER BY created_at ASC LIMIT 1),
                  (SELECT price FROM trades t4 WHERE t4.symbol = s.symbol ORDER BY created_at ASC LIMIT 1)
                ) AS open_24h
         FROM symbols s
         LEFT JOIN trades t ON t.symbol = s.symbol
         WHERE s.active = TRUE
         GROUP BY s.symbol ORDER BY s.symbol",
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
    let interval = params.interval.as_deref().unwrap_or("1m");
    let limit = params.limit.unwrap_or(200).min(1000);

    // Map frontend interval codes to PostgreSQL date_bin durations (whitelist).
    let bin_width = match interval {
        "5m"  => "5 minutes",
        "15m" => "15 minutes",
        "1h"  => "1 hour",
        "4h"  => "4 hours",
        "1d"  => "1 day",
        _     => "1 minute",
    };

    let sql = format!(
        "SELECT
           extract(epoch FROM date_bin('{bin}', created_at, TIMESTAMPTZ '2000-01-01'))::bigint AS time,
           (array_agg(price ORDER BY created_at ASC))[1] AS open,
           max(price) AS high,
           min(price) AS low,
           (array_agg(price ORDER BY created_at DESC))[1] AS close,
           sum(quantity) AS volume
         FROM trades
         WHERE symbol = $1
         GROUP BY date_bin('{bin}', created_at, TIMESTAMPTZ '2000-01-01')
         ORDER BY time ASC
         LIMIT $2",
        bin = bin_width
    );

    let rows = sqlx::query(&sql)
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

async fn handle_positions(State(s): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    let user_id = match auth_user(&headers, &s.jwt_secret) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    let rows = positions::all_positions_for_user(s.db.as_ref(), user_id).await;
    (StatusCode::OK, Json(json!(rows))).into_response()
}

#[derive(Deserialize)]
struct AggTradesQuery {
    symbol: String,
    interval: Option<String>,
    limit: Option<i64>,
}

async fn handle_agg_trades(
    State(s): State<AppState>,
    Query(q): Query<AggTradesQuery>,
) -> impl IntoResponse {
    let interval = q.interval.as_deref().unwrap_or("1s");
    let limit = q.limit.unwrap_or(60).clamp(1, 300);

    // Whitelist the interval — interpolated into SQL, must not be user-controlled.
    let bin_width = match interval {
        "5s"  => "5 seconds",
        "30s" => "30 seconds",
        _     => "1 second",
    };

    let sql = format!(
        "SELECT
           extract(epoch FROM date_bin('{bin}', created_at, TIMESTAMPTZ '2000-01-01'))::bigint AS time,
           (array_agg(price ORDER BY created_at ASC))[1]  AS open,
           max(price)                                      AS high,
           min(price)                                      AS low,
           (array_agg(price ORDER BY created_at DESC))[1] AS close,
           sum(quantity)                                   AS volume,
           count(*)::bigint                                AS trade_count
         FROM trades
         WHERE symbol = $1
         GROUP BY date_bin('{bin}', created_at, TIMESTAMPTZ '2000-01-01')
         ORDER BY time DESC
         LIMIT $2",
        bin = bin_width,
    );

    let rows = sqlx::query(&sql)
        .bind(&q.symbol)
        .bind(limit)
        .fetch_all(s.db.as_ref())
        .await;

    match rows {
        Ok(rows) => {
            use sqlx::Row;
            // Returned DESC for the LIMIT; reverse so consumers get ASC.
            let mut bars: Vec<Value> = rows
                .iter()
                .map(|r| {
                    json!({
                        "time":        r.get::<i64, _>("time"),
                        "open":        r.get::<f64, _>("open"),
                        "high":        r.get::<f64, _>("high"),
                        "low":         r.get::<f64, _>("low"),
                        "close":       r.get::<f64, _>("close"),
                        "volume":      r.get::<f64, _>("volume"),
                        "trade_count": r.get::<i64, _>("trade_count"),
                    })
                })
                .collect();
            bars.reverse();
            (StatusCode::OK, Json(json!(bars))).into_response()
        }
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
                    {
                        let price = r.get::<f64, _>("price");
                        let quantity = r.get::<f64, _>("quantity");
                        json!({
                            "id": r.get::<i64, _>("id"),
                            "symbol": r.get::<String, _>("symbol"),
                            "price": price,
                            "quantity": quantity,
                            "value": price * quantity,
                            "created_at": r.get::<chrono::DateTime<chrono::Utc>, _>("created_at"),
                            "side": r.get::<String, _>("side"),
                        })
                    }
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

async fn handle_market_trades(
    State(s): State<AppState>,
    Query(q): Query<TradeQuery>,
) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(50).min(200);
    let rows = sqlx::query(
        "SELECT t.id, t.symbol, t.price, t.quantity,
                (EXTRACT(EPOCH FROM t.created_at)::bigint * 1000000
                 + EXTRACT(MICROSECONDS FROM t.created_at)::bigint % 1000000) AS ts_us,
                CASE
                    WHEN t.buy_order_id IS NULL OR t.sell_order_id IS NULL THEN 'buy'
                    WHEN t.buy_order_id > t.sell_order_id THEN 'buy'
                    ELSE 'sell'
                END AS side
         FROM trades t
         WHERE ($1::text IS NULL OR t.symbol = $1)
         ORDER BY t.created_at DESC, t.id DESC
         LIMIT $2",
    )
    .bind(&q.symbol)
    .bind(limit)
    .fetch_all(s.db.as_ref())
    .await;

    match rows {
        Ok(rows) => {
            use sqlx::Row;
            let out: Vec<Value> = rows
                .iter()
                .map(|r| json!({
                    "id":    r.get::<i64, _>("id"),
                    "price": r.get::<f64, _>("price"),
                    "qty":   r.get::<f64, _>("quantity"),
                    "side":  r.get::<String, _>("side"),
                    "ts":    r.get::<i64, _>("ts_us"),
                }))
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

// ─── Router ───────────────────────────────────────────────────────────────────

fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/tickers", get(handle_tickers))
        .route("/api/symbols", get(handle_symbols))
        .route("/api/klines", get(handle_klines))
        .route("/api/orders", get(handle_orders))
        .route("/api/orders/:id", get(handle_order))
        .route("/api/accounts", get(handle_accounts))
        .route("/api/positions", get(handle_positions))
        .route("/api/agg-trades", get(handle_agg_trades))
        .route("/api/trades", get(handle_trades))
        .route("/api/market-trades", get(handle_market_trades))
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
