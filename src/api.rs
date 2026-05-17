/// REST API layer: auth, accounts, orders, user profile, KYC
use crate::account_repository::AccountRepository;
use crate::models::{DbOrder, User};
use crate::user_service::{self, LoginRequest, RegisterRequest};
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{delete, get, patch, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::PgPool;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub db: Arc<PgPool>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        // Auth
        .route("/api/auth/register", post(handle_register))
        .route("/api/auth/login", post(handle_login))
        // User profile & KYC
        .route("/api/user/profile", get(handle_get_profile).patch(handle_update_profile))
        .route("/api/kyc", post(handle_submit_kyc))
        // Accounts / balances
        .route("/api/accounts", get(handle_accounts))
        .route("/api/balances", get(handle_accounts))
        // Orders
        .route("/api/orders", get(handle_orders).post(handle_place_order))
        .route("/api/orders/:order_id", get(handle_order).delete(handle_cancel_order))
        // Trades & tickers
        .route("/api/trades", get(handle_trades))
        .route("/api/tickers", get(handle_tickers))
        .route("/api/user/password", patch(handle_change_password))
        .with_state(state)
}

// ─── Auth ─────────────────────────────────────────────────────────────────────

async fn handle_register(
    State(s): State<AppState>,
    Json(req): Json<RegisterRequest>,
) -> impl IntoResponse {
    match user_service::register(&s.db, req).await {
        Ok(resp) => (StatusCode::CREATED, Json(json!(resp))),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({"error": e.to_string()}))),
    }
}

async fn handle_login(
    State(s): State<AppState>,
    Json(req): Json<LoginRequest>,
) -> impl IntoResponse {
    match user_service::login(&s.db, req).await {
        Ok(resp) => (StatusCode::OK, Json(json!(resp))),
        Err(e) => (StatusCode::UNAUTHORIZED, Json(json!({"error": e.to_string()}))),
    }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

fn auth_user(headers: &HeaderMap) -> Result<i64, (StatusCode, Json<Value>)> {
    let token = headers
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .ok_or_else(|| (StatusCode::UNAUTHORIZED, Json(json!({"error": "Missing token"}))))?;

    user_service::verify_token(token)
        .map(|c| c.sub)
        .map_err(|e| (StatusCode::UNAUTHORIZED, Json(json!({"error": e.to_string()}))))
}

// ─── Accounts ─────────────────────────────────────────────────────────────────

async fn handle_accounts(
    State(s): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let user_id = match auth_user(&headers) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    let repo = AccountRepository::new(&s.db);
    match repo.get_all_accounts(user_id).await {
        Ok(accounts) => (StatusCode::OK, Json(json!(accounts))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e.to_string()}))).into_response(),
    }
}

// ─── Orders ───────────────────────────────────────────────────────────────────

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
    let user_id = match auth_user(&headers) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    let limit = q.limit.unwrap_or(50).min(500);
    let orders = sqlx::query_as::<_, DbOrder>(
        "SELECT * FROM orders
         WHERE user_id = $1
           AND ($2::text IS NULL OR symbol = $2)
           AND ($3::text IS NULL OR status = $3)
         ORDER BY created_at DESC LIMIT $4",
    )
    .bind(user_id)
    .bind(&q.symbol)
    .bind(&q.status)
    .bind(limit)
    .fetch_all(s.db.as_ref())
    .await;

    match orders {
        Ok(rows) => (StatusCode::OK, Json(json!(rows))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e.to_string()}))).into_response(),
    }
}

async fn handle_order(
    State(s): State<AppState>,
    headers: HeaderMap,
    Path(order_id): Path<i64>,
) -> impl IntoResponse {
    let user_id = match auth_user(&headers) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    let row = sqlx::query_as::<_, DbOrder>(
        "SELECT * FROM orders WHERE id = $1 AND user_id = $2",
    )
    .bind(order_id)
    .bind(user_id)
    .fetch_optional(s.db.as_ref())
    .await;

    match row {
        Ok(Some(order)) => (StatusCode::OK, Json(json!(order))).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, Json(json!({"error": "Order not found"}))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e.to_string()}))).into_response(),
    }
}

// ─── Trades ───────────────────────────────────────────────────────────────────

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
    let user_id = match auth_user(&headers) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    let limit = q.limit.unwrap_or(50).min(500);
    let trades = sqlx::query(
        "SELECT t.* FROM trades t
         JOIN orders o ON o.id = t.buy_order_id OR o.id = t.sell_order_id
         WHERE o.user_id = $1
           AND ($2::text IS NULL OR t.symbol = $2)
         ORDER BY t.created_at DESC LIMIT $3",
    )
    .bind(user_id)
    .bind(&q.symbol)
    .bind(limit)
    .fetch_all(s.db.as_ref())
    .await;

    match trades {
        Ok(rows) => {
            let out: Vec<Value> = rows.iter().map(|r| {
                use sqlx::Row;
                json!({
                    "id": r.get::<i64, _>("id"),
                    "symbol": r.get::<String, _>("symbol"),
                    "price": r.get::<f64, _>("price"),
                    "quantity": r.get::<f64, _>("quantity"),
                    "created_at": r.get::<chrono::DateTime<chrono::Utc>, _>("created_at"),
                })
            }).collect();
            (StatusCode::OK, Json(json!(out))).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e.to_string()}))).into_response(),
    }
}

// ─── Order placement ──────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct PlaceOrderRequest {
    symbol: String,
    side: String,
    order_type: String,
    price: Option<f64>,
    quantity: f64,
}

async fn handle_place_order(
    State(s): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<PlaceOrderRequest>,
) -> impl IntoResponse {
    let user_id = match auth_user(&headers) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    if req.quantity <= 0.0 {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": "quantity must be > 0"}))).into_response();
    }
    if req.order_type != "market" && req.price.map(|p| p <= 0.0).unwrap_or(true) {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": "price required for non-market orders"}))).into_response();
    }
    let order = sqlx::query_as::<_, DbOrder>(
        "INSERT INTO orders (id, user_id, symbol, side, order_type, price, quantity, filled, status)
         VALUES (nextval('orders_id_seq'), $1, $2, $3, $4, $5, $6, 0, 'PENDING') RETURNING *",
    )
    .bind(user_id).bind(&req.symbol).bind(&req.side)
    .bind(&req.order_type).bind(req.price).bind(req.quantity)
    .fetch_one(s.db.as_ref()).await;

    match order {
        Ok(o) => (StatusCode::CREATED, Json(json!(o))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e.to_string()}))).into_response(),
    }
}

async fn handle_cancel_order(
    State(s): State<AppState>,
    headers: HeaderMap,
    Path(order_id): Path<i64>,
) -> impl IntoResponse {
    let user_id = match auth_user(&headers) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    let r = sqlx::query(
        "UPDATE orders SET status = 'CANCELED', updated_at = NOW()
         WHERE id = $1 AND user_id = $2 AND status IN ('PENDING','TRADING')",
    )
    .bind(order_id).bind(user_id)
    .execute(s.db.as_ref()).await;

    match r {
        Ok(r) if r.rows_affected() > 0 => (StatusCode::OK, Json(json!({"cancelled": order_id}))).into_response(),
        Ok(_) => (StatusCode::NOT_FOUND, Json(json!({"error": "Order not found or already closed"}))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e.to_string()}))).into_response(),
    }
}

// ─── Tickers ──────────────────────────────────────────────────────────────────

async fn handle_tickers(State(s): State<AppState>) -> impl IntoResponse {
    let rows = sqlx::query(
        "SELECT symbol,
                MAX(price) FILTER (WHERE created_at > NOW() - INTERVAL '24 hours') AS high_24h,
                MIN(price) FILTER (WHERE created_at > NOW() - INTERVAL '24 hours') AS low_24h,
                SUM(quantity) FILTER (WHERE created_at > NOW() - INTERVAL '24 hours') AS vol_24h,
                (SELECT price FROM trades t2 WHERE t2.symbol = t.symbol ORDER BY created_at DESC LIMIT 1) AS last_price
         FROM trades t GROUP BY symbol ORDER BY symbol",
    )
    .fetch_all(s.db.as_ref()).await;

    match rows {
        Ok(rows) => {
            use sqlx::Row;
            let tickers: Vec<Value> = rows.iter().map(|r| json!({
                "symbol": r.get::<String, _>("symbol"),
                "last":   r.get::<Option<f64>, _>("last_price"),
                "high":   r.get::<Option<f64>, _>("high_24h"),
                "low":    r.get::<Option<f64>, _>("low_24h"),
                "volume": r.get::<Option<f64>, _>("vol_24h"),
            })).collect();
            (StatusCode::OK, Json(json!(tickers))).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e.to_string()}))).into_response(),
    }
}

// ─── User profile ─────────────────────────────────────────────────────────────

async fn handle_get_profile(
    State(s): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let user_id = match auth_user(&headers) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    match sqlx::query_as::<_, User>("SELECT * FROM users WHERE id = $1")
        .bind(user_id)
        .fetch_optional(s.db.as_ref())
        .await
    {
        Ok(Some(u)) => (StatusCode::OK, Json(json!(u))).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, Json(json!({"error": "User not found"}))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e.to_string()}))).into_response(),
    }
}

#[derive(Deserialize)]
struct UpdateProfileRequest {
    full_name: Option<String>,
}

async fn handle_update_profile(
    State(s): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<UpdateProfileRequest>,
) -> impl IntoResponse {
    let user_id = match auth_user(&headers) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    match sqlx::query_as::<_, User>(
        "UPDATE users SET full_name = COALESCE($1, full_name), updated_at = NOW()
         WHERE id = $2 RETURNING *",
    )
    .bind(&req.full_name)
    .bind(user_id)
    .fetch_optional(s.db.as_ref())
    .await
    {
        Ok(Some(u)) => (StatusCode::OK, Json(json!(u))).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, Json(json!({"error": "User not found"}))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e.to_string()}))).into_response(),
    }
}

// ─── KYC ─────────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct KycRequest {
    full_name: String,
}

async fn handle_submit_kyc(
    State(s): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<KycRequest>,
) -> impl IntoResponse {
    let user_id = match auth_user(&headers) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    if req.full_name.trim().is_empty() {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": "full_name required"}))).into_response();
    }
    match sqlx::query_as::<_, User>(
        "UPDATE users SET full_name = $1, kyc_status = 'PENDING', updated_at = NOW()
         WHERE id = $2 AND kyc_status = 'NONE' RETURNING *",
    )
    .bind(req.full_name.trim())
    .bind(user_id)
    .fetch_optional(s.db.as_ref())
    .await
    {
        Ok(Some(u)) => (StatusCode::OK, Json(json!({
            "kyc_status": u.kyc_status,
            "message": "KYC submitted, pending review"
        }))).into_response(),
        Ok(None) => (StatusCode::BAD_REQUEST, Json(json!({"error": "KYC already submitted or user not found"}))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e.to_string()}))).into_response(),
    }
}

// ─── Password change ──────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct ChangePasswordRequest {
    current_password: String,
    new_password: String,
}

async fn handle_change_password(
    State(s): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<ChangePasswordRequest>,
) -> impl IntoResponse {
    let user_id = match auth_user(&headers) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    if req.new_password.len() < 8 {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": "New password must be at least 8 characters"}))).into_response();
    }
    // Fetch current hash
    let row = sqlx::query_as::<_, User>("SELECT * FROM users WHERE id = $1")
        .bind(user_id)
        .fetch_optional(s.db.as_ref())
        .await;
    let user = match row {
        Ok(Some(u)) => u,
        Ok(None) => return (StatusCode::NOT_FOUND, Json(json!({"error": "User not found"}))).into_response(),
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e.to_string()}))).into_response(),
    };
    // Verify current password
    match bcrypt::verify(&req.current_password, &user.password_hash) {
        Ok(true) => {},
        _ => return (StatusCode::UNAUTHORIZED, Json(json!({"error": "Current password incorrect"}))).into_response(),
    }
    // Hash and save new password
    let new_hash = match bcrypt::hash(&req.new_password, bcrypt::DEFAULT_COST) {
        Ok(h) => h,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e.to_string()}))).into_response(),
    };
    match sqlx::query("UPDATE users SET password_hash = $1, updated_at = NOW() WHERE id = $2")
        .bind(&new_hash)
        .bind(user_id)
        .execute(s.db.as_ref())
        .await
    {
        Ok(_) => (StatusCode::OK, Json(json!({"message": "Password updated"}))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e.to_string()}))).into_response(),
    }
}
