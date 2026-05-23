/// REST API layer: auth, accounts, orders, user profile, KYC
use crate::account_repository::AccountRepository;
use crate::engine::{MatchingEngine, OrderStatus};
use crate::models::{DbOrder, User};
use crate::order::{Order, Side, TimeInForce};
use crate::user_service::{self, LoginRequest, RegisterRequest};
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, patch, post},
    Json, Router,
};
use dashmap::DashMap;
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::PgPool;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{broadcast, mpsc};

#[derive(Clone)]
pub struct AppState {
    pub db: Arc<PgPool>,
    /// Per-symbol matching engines keyed by symbol (e.g. "BTC_USDT").
    pub engines: Arc<DashMap<String, Arc<Mutex<MatchingEngine>>>>,
    /// Broadcast market data (depth snapshots, trades) to all subscribed connections.
    pub market_tx: Arc<broadcast::Sender<String>>,
    /// Per-user personal update channel (order fills, balance changes).
    pub user_tx: Arc<DashMap<i64, mpsc::Sender<String>>>,
    /// Monotonically increasing order ID counter (separate from engine's internal counter).
    pub next_order_id: Arc<AtomicU64>,
}

pub fn router(state: AppState) -> Router {
    use crate::ws_handler::ws_handler;
    Router::new()
        // WebSocket endpoint
        .route("/ws", get(ws_handler))
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
        // K-lines
        .route("/api/klines", get(handle_klines))
        .route("/api/user/password", patch(handle_change_password))
        .route("/api/test-funds", post(handle_test_funds))
        .route("/api/seed-demo", post(handle_seed_demo))
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
    let status_filter: Option<Vec<&str>> = match q.status.as_deref() {
        Some("open")    => Some(vec!["PENDING", "TRADING"]),
        Some("history") => Some(vec!["COMPLETED", "CANCELED", "REJECTED"]),
        Some(s)         => Some(vec![s]),
        None            => None,
    };
    let orders = match status_filter {
        Some(statuses) => sqlx::query_as::<_, DbOrder>(
            "SELECT * FROM orders WHERE user_id=$1 AND ($2::text IS NULL OR symbol=$2) AND status=ANY($3) ORDER BY created_at DESC LIMIT $4",
        ).bind(user_id).bind(&q.symbol).bind(&statuses).bind(limit).fetch_all(s.db.as_ref()).await,
        None => sqlx::query_as::<_, DbOrder>(
            "SELECT * FROM orders WHERE user_id=$1 AND ($2::text IS NULL OR symbol=$2) ORDER BY created_at DESC LIMIT $4",
        ).bind(user_id).bind(&q.symbol).bind(limit).fetch_all(s.db.as_ref()).await,
    };
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

    let engine_side = match req.side.as_str() {
        "buy"  => Side::Buy,
        "sell" => Side::Sell,
        _      => return (StatusCode::BAD_REQUEST, Json(json!({"error": "invalid side"}))).into_response(),
    };
    let tif = match req.order_type.as_str() {
        "limit" | "gtc"   => TimeInForce::GTC,
        "ioc"             => TimeInForce::IOC,
        "fok"             => TimeInForce::FOK,
        "post_only"       => TimeInForce::PostOnly,
        "market"          => TimeInForce::IOC,
        _                 => return (StatusCode::BAD_REQUEST, Json(json!({"error": "unknown order_type"}))).into_response(),
    };

    let engine = match s.engines.get(&req.symbol) {
        Some(e) => e.value().clone(),
        None    => return (StatusCode::BAD_REQUEST, Json(json!({"error": format!("Unknown symbol: {}", req.symbol)}))).into_response(),
    };

    let sym_parts: Vec<&str> = req.symbol.splitn(2, '_').collect();
    let base_asset  = sym_parts.first().copied().unwrap_or("BTC");
    let quote_asset = sym_parts.last().copied().unwrap_or("USDT");

    // Best opposing price (for market orders that need a freeze estimate).
    let best_opposing = {
        let eng = engine.lock().unwrap();
        let levels = eng.get_top_levels(1, engine_side == Side::Sell);
        levels.first().map(|(p, _)| *p)
    };

    // Freeze funds before touching the engine.
    let repo = AccountRepository::new(s.db.as_ref());
    let freeze_result = if req.side == "buy" {
        let freeze_price = req.price.or(best_opposing).unwrap_or(0.0);
        if freeze_price > 0.0 {
            repo.freeze_for_buy(user_id, quote_asset, freeze_price * req.quantity).await
        } else {
            Ok(())
        }
    } else {
        repo.freeze_for_sell(user_id, base_asset, req.quantity).await
    };
    if let Err(e) = freeze_result {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": e.to_string()}))).into_response();
    }

    // Insert into DB first to get a stable ID, then use that same ID for the engine order.
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);

    let db_order = sqlx::query_as::<_, DbOrder>(
        "INSERT INTO orders (id, user_id, symbol, side, order_type, price, quantity, filled, status)
         VALUES (nextval('orders_id_seq'), $1, $2, $3, $4, $5, $6, 0, 'PENDING') RETURNING *",
    )
    .bind(user_id).bind(&req.symbol).bind(&req.side)
    .bind(&req.order_type).bind(req.price).bind(req.quantity)
    .fetch_one(s.db.as_ref()).await;

    let db_order = match db_order {
        Ok(o) => o,
        Err(e) => {
            // Roll back freeze.
            if req.side == "buy" {
                let p = req.price.or(best_opposing).unwrap_or(0.0);
                if p > 0.0 { let _ = repo.release_frozen(user_id, quote_asset, p * req.quantity).await; }
            } else {
                let _ = repo.release_frozen(user_id, base_asset, req.quantity).await;
            }
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e.to_string()}))).into_response();
        }
    };
    let db_order_id = db_order.id;

    // Build engine order using the DB-assigned ID so engine and DB stay in sync.
    let engine_order = if req.order_type == "market" {
        Order::new_market(db_order_id as u64, engine_side, req.quantity, now_ns)
    } else {
        Order::new(db_order_id as u64, engine_side, req.price.unwrap_or(0.0), req.quantity, tif, now_ns)
    };
    // Keep the atomic counter in sync so WS orders don't collide.
    s.next_order_id.fetch_max(db_order_id as u64 + 1, Ordering::Relaxed);

    // Run through matching engine.
    let engine_result = {
        let mut eng = engine.lock().unwrap();
        eng.place_order(engine_order)
    };

    let result = match engine_result {
        Ok(r) => r,
        Err(e) => {
            let _ = sqlx::query("UPDATE orders SET status='REJECTED', updated_at=NOW() WHERE id=$1")
                .bind(db_order_id).execute(s.db.as_ref()).await;
            if req.side == "buy" {
                let p = req.price.or(best_opposing).unwrap_or(0.0);
                if p > 0.0 { let _ = repo.release_frozen(user_id, quote_asset, p * req.quantity).await; }
            } else {
                let _ = repo.release_frozen(user_id, base_asset, req.quantity).await;
            }
            return (StatusCode::BAD_REQUEST, Json(json!({"error": e.to_string()}))).into_response();
        }
    };

    let db_status = match result.status {
        OrderStatus::Accepted        => "PENDING",
        OrderStatus::PartiallyFilled => "TRADING",
        OrderStatus::Filled          => "COMPLETED",
        OrderStatus::Rejected        => "REJECTED",
        OrderStatus::Cancelled       => "CANCELED",
    };

    let _ = sqlx::query("UPDATE orders SET status=$1, filled=$2, updated_at=NOW() WHERE id=$3")
        .bind(db_status).bind(result.filled).bind(db_order_id)
        .execute(s.db.as_ref()).await;

    if result.status == OrderStatus::Rejected {
        if req.side == "buy" {
            let p = req.price.or(best_opposing).unwrap_or(0.0);
            if p > 0.0 { let _ = repo.release_frozen(user_id, quote_asset, p * req.quantity).await; }
        } else {
            let _ = repo.release_frozen(user_id, base_asset, req.quantity).await;
        }
        return (StatusCode::BAD_REQUEST, Json(json!({"error": "Rejected by matching engine"}))).into_response();
    }

    // Settle fills: record trades and update maker orders.
    let mut notified_maker_uids: std::collections::HashSet<i64> = std::collections::HashSet::new();
    for &(maker_order_id, fp, fq) in &result.fills {
        if fp <= 0.0 || fq <= 0.0 { continue; }

        let maker_uid: Option<i64> = sqlx::query_scalar("SELECT user_id FROM orders WHERE id=$1")
            .bind(maker_order_id as i64).fetch_optional(s.db.as_ref()).await.unwrap_or(None);

        let (buyer_id, seller_id) = if req.side == "buy" {
            (user_id, maker_uid.unwrap_or(0))
        } else {
            (maker_uid.unwrap_or(0), user_id)
        };

        if buyer_id > 0 && seller_id > 0 {
            if req.side == "buy" {
                let over = req.price.map(|lp| (lp - fp) * fq).unwrap_or(0.0).max(0.0);
                if over > 0.0 { let _ = repo.release_frozen(user_id, quote_asset, over).await; }
            }
            let _ = repo.settle_trade(buyer_id, seller_id, base_asset, quote_asset, fp, fq, 0.0, 0.0).await;
        }

        // Update maker order status in DB.
        let _ = sqlx::query(
            "UPDATE orders SET filled = filled + $1,
             status = CASE WHEN filled + $1 >= quantity THEN 'COMPLETED' ELSE 'TRADING' END,
             updated_at = NOW() WHERE id = $2",
        )
        .bind(fq).bind(maker_order_id as i64).execute(s.db.as_ref()).await;

        // Record trade.
        let (buy_oid, sell_oid): (Option<i64>, Option<i64>) = if req.side == "buy" {
            (Some(db_order_id), Some(maker_order_id as i64))
        } else {
            (Some(maker_order_id as i64), Some(db_order_id))
        };
        let _ = sqlx::query(
            "INSERT INTO trades (symbol, buy_order_id, sell_order_id, price, quantity, created_at)
             VALUES ($1, $2, $3, $4, $5, NOW())",
        )
        .bind(&req.symbol).bind(buy_oid).bind(sell_oid).bind(fp).bind(fq)
        .execute(s.db.as_ref()).await;

        if let Some(uid) = maker_uid { notified_maker_uids.insert(uid); }
    }

    // Release frozen for IOC/market with partial or zero fill.
    if result.status == OrderStatus::Cancelled {
        let unfilled = req.quantity - result.filled;
        if unfilled > 0.0 {
            let (rel_asset, rel_amount) = if req.side == "buy" {
                (quote_asset, req.price.or(best_opposing).unwrap_or(0.0) * unfilled)
            } else {
                (base_asset, unfilled)
            };
            if rel_amount > 0.0 { let _ = repo.release_frozen(user_id, rel_asset, rel_amount).await; }
        }
    }

    // Push balance_update to taker and all makers that were involved in fills.
    let all_notify: Vec<i64> = std::iter::once(user_id)
        .chain(notified_maker_uids.into_iter())
        .collect();
    for uid in all_notify {
        if let Some(tx) = s.user_tx.get(&uid) {
            let repo2 = AccountRepository::new(s.db.as_ref());
            if let Ok(accounts) = repo2.get_all_accounts(uid).await {
                for acc in accounts {
                    let msg = serde_json::json!({
                        "type": "balance_update",
                        "asset": acc.asset,
                        "balance": acc.balance,
                        "available": acc.balance - acc.frozen,
                        "frozen": acc.frozen,
                    }).to_string();
                    let _ = tx.send(msg).await;
                }
            }
        }
    }

    // Broadcast trade event + ticker update when there were fills.
    if result.filled > 0.0 {
        let avg_fill_price = if !result.fills.is_empty() {
            let cost: f64 = result.fills.iter().map(|&(_, p, q)| p * q).sum();
            cost / result.filled
        } else {
            req.price.or(best_opposing).unwrap_or(0.0)
        };
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let trade_msg = serde_json::json!({
            "type": "trade",
            "symbol": &req.symbol,
            "price": avg_fill_price,
            "qty": result.filled,
            "side": &req.side,
            "ts": now_secs,
        }).to_string();
        let _ = s.market_tx.send(trade_msg);

        let sym_clone = req.symbol.clone();
        let db_clone = s.db.clone();
        let mtx_clone = s.market_tx.clone();
        tokio::spawn(async move {
            let open_24h: Option<f64> = sqlx::query_scalar(
                "SELECT price FROM trades WHERE symbol=$1
                 AND created_at > NOW() - INTERVAL '24 hours'
                 ORDER BY created_at ASC LIMIT 1",
            )
            .bind(&sym_clone)
            .fetch_optional(db_clone.as_ref())
            .await
            .unwrap_or(None);
            let change = match open_24h {
                Some(o) if o != 0.0 => (avg_fill_price - o) / o * 100.0,
                _ => 0.0,
            };
            let ticker = serde_json::json!({
                "type": "ticker",
                "symbol": sym_clone,
                "last": avg_fill_price,
                "change": change,
            }).to_string();
            let _ = mtx_clone.send(ticker);
        });
    }

    // Broadcast depth update and current-minute kline.
    crate::ws_handler::broadcast_depth_pub(&s, &req.symbol);
    crate::ws_handler::broadcast_kline_pub(&s, &req.symbol).await;

    // Push order_update to the user's personal WS channel so the Exchange page
    // shows a fill / partial-fill toast even for REST-placed orders.
    if result.status != OrderStatus::Accepted {
        let ws_status = match result.status {
            OrderStatus::Filled          => "FILLED",
            OrderStatus::PartiallyFilled => "PARTIAL",
            OrderStatus::Cancelled       => "CANCELED",
            _                            => db_status,
        };
        if let Some(tx) = s.user_tx.get(&user_id) {
            let msg = serde_json::json!({
                "type": "order_update",
                "order_id": db_order_id,
                "status": ws_status,
                "filled": result.filled,
                "filled_qty": result.filled,
            }).to_string();
            let _ = tx.send(msg).await;
        }
    }

    // Re-fetch final order state from DB and return it.
    match sqlx::query_as::<_, DbOrder>("SELECT * FROM orders WHERE id=$1")
        .bind(db_order_id).fetch_one(s.db.as_ref()).await
    {
        Ok(o) => (StatusCode::CREATED, Json(json!(o))).into_response(),
        Err(_) => (StatusCode::CREATED, Json(json!({"id": db_order_id}))).into_response(),
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

    // Fetch order details before cancelling so we know what to unfreeze.
    let order_row = sqlx::query(
        "SELECT symbol, side, price, quantity, filled FROM orders
         WHERE id = $1 AND user_id = $2 AND status IN ('PENDING','TRADING')",
    )
    .bind(order_id)
    .bind(user_id)
    .fetch_optional(s.db.as_ref())
    .await;

    let order_row = match order_row {
        Ok(Some(r)) => r,
        Ok(None) => return (StatusCode::NOT_FOUND, Json(json!({"error": "Order not found or already closed"}))).into_response(),
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e.to_string()}))).into_response(),
    };

    use sqlx::Row;
    let symbol: String = order_row.get("symbol");
    let side: String = order_row.get("side");
    let price: Option<f64> = order_row.get("price");
    let quantity: f64 = order_row.get("quantity");
    let filled: f64 = order_row.get("filled");
    let remaining = quantity - filled;

    let upd = sqlx::query(
        "UPDATE orders SET status = 'CANCELED', updated_at = NOW()
         WHERE id = $1 AND user_id = $2 AND status IN ('PENDING','TRADING')",
    )
    .bind(order_id).bind(user_id)
    .execute(s.db.as_ref()).await;

    match upd {
        Ok(r) if r.rows_affected() > 0 => {}
        Ok(_) => return (StatusCode::NOT_FOUND, Json(json!({"error": "Order not found or already closed"}))).into_response(),
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e.to_string()}))).into_response(),
    }

    // Best-effort cancel in engine for this symbol.
    if let Some(engine) = s.engines.get(&symbol) {
        let _ = { let mut eng = engine.lock().unwrap(); eng.cancel_order(order_id as u64) };
    }

    // Release frozen funds for the unfilled portion.
    let sym_parts: Vec<&str> = symbol.splitn(2, '_').collect();
    let base_asset = sym_parts.first().copied().unwrap_or("BTC");
    let quote_asset = sym_parts.last().copied().unwrap_or("USDT");
    let repo = AccountRepository::new(&s.db);
    if side == "buy" {
        let freeze_price = price.unwrap_or(0.0);
        if freeze_price > 0.0 && remaining > 0.0 {
            let _ = repo.release_frozen(user_id, quote_asset, freeze_price * remaining).await;
        }
    } else if remaining > 0.0 {
        let _ = repo.release_frozen(user_id, base_asset, remaining).await;
    }

    (StatusCode::OK, Json(json!({"cancelled": order_id}))).into_response()
}

// ─── Tickers ──────────────────────────────────────────────────────────────────

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
    .fetch_all(s.db.as_ref()).await;

    match rows {
        Ok(rows) => {
            use sqlx::Row;
            let tickers: Vec<Value> = rows.iter().map(|r| {
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
            }).collect();
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
        return (StatusCode::BAD_REQUEST, Json(json!({"error": "new_password must be at least 8 characters"}))).into_response();
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
        _ => return (StatusCode::UNAUTHORIZED, Json(json!({"error": "Current password is incorrect"}))).into_response(),
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

// ─── Test Funds ───────────────────────────────────────────────────────────────

/// Grant test funds to the authenticated user.
/// Only works when total USDT balance is below 100 (prevents repeated claims).
async fn handle_test_funds(
    State(s): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let user_id = match auth_user(&headers) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };

    // Check current USDT balance
    let usdt_balance: Option<f64> = sqlx::query_scalar(
        "SELECT balance FROM accounts WHERE user_id = $1 AND asset = 'USDT'",
    )
    .bind(user_id)
    .fetch_optional(s.db.as_ref())
    .await
    .unwrap_or(None);

    if usdt_balance.unwrap_or(0.0) >= 100.0 {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": "You already have funds. Test funds can only be claimed when USDT balance is below 100."}))).into_response();
    }

    // Credit 10,000 USDT, 1 BTC, 10 ETH, 100 SOL
    let result = sqlx::query(
        "INSERT INTO accounts (user_id, asset, balance, frozen)
         VALUES ($1, 'USDT', 10000, 0), ($1, 'BTC', 1, 0), ($1, 'ETH', 10, 0), ($1, 'SOL', 100, 0)
         ON CONFLICT (user_id, asset) DO UPDATE
         SET balance = accounts.balance + EXCLUDED.balance, updated_at = NOW()",
    )
    .bind(user_id)
    .execute(s.db.as_ref())
    .await;

    match result {
        Ok(_) => (StatusCode::OK, Json(json!({"message": "Test funds granted: 10,000 USDT + 1 BTC + 10 ETH + 100 SOL", "usdt": 10000, "btc": 1, "eth": 10, "sol": 100}))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e.to_string()}))).into_response(),
    }
}

// ─── K-lines ──────────────────────────────────────────────────────────────────

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
    let _interval = params.interval.unwrap_or_else(|| "1m".to_string()); // reserved for future intervals
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
            let candles: Vec<Value> = rows.iter().map(|r| json!({
                "time":   r.get::<i64, _>("time"),
                "open":   r.get::<f64, _>("open"),
                "high":   r.get::<f64, _>("high"),
                "low":    r.get::<f64, _>("low"),
                "close":  r.get::<f64, _>("close"),
                "volume": r.get::<f64, _>("volume"),
            })).collect();
            (StatusCode::OK, Json(json!(candles))).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e.to_string()}))).into_response(),
    }
}

// ─── Seed demo ────────────────────────────────────────────────────────────────

/// Plant demo limit orders for ETH_USDT, SOL_USDT, and BTC_USDT so the exchange has
/// live order books for demos. Idempotent per symbol.
async fn handle_seed_demo(State(s): State<AppState>) -> impl IntoResponse {
    // Find or create the demo user.
    let demo_user_id: i64 = match sqlx::query_scalar::<_, i64>(
        "INSERT INTO users (email, password_hash, full_name, kyc_status)
         VALUES ('demo@lightning.exchange', 'x', 'Demo User', 'verified')
         ON CONFLICT (email) DO UPDATE SET id = users.id RETURNING id",
    )
    .fetch_one(s.db.as_ref())
    .await
    {
        Ok(id) => id,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e.to_string()}))).into_response(),
    };

    // Upsert demo balances (including BTC).
    let _ = sqlx::query(
        "INSERT INTO accounts (user_id, asset, balance, frozen)
         VALUES ($1, 'USDT', 100000, 0), ($1, 'BTC', 5, 0), ($1, 'ETH', 50, 0), ($1, 'SOL', 1000, 0)
         ON CONFLICT (user_id, asset) DO UPDATE
         SET balance = GREATEST(accounts.balance, EXCLUDED.balance), updated_at = NOW()",
    )
    .bind(demo_user_id)
    .execute(s.db.as_ref())
    .await;

    // (symbol, side, prices, qty, base, quote)
    let plans: Vec<(&str, &str, Vec<f64>, f64, &str, &str)> = vec![
        ("ETH_USDT", "buy",  vec![2990.0, 2980.0, 2970.0, 2960.0, 2950.0], 0.5, "ETH", "USDT"),
        ("ETH_USDT", "sell", vec![3010.0, 3020.0, 3030.0, 3040.0, 3050.0], 0.5, "ETH", "USDT"),
        ("SOL_USDT", "buy",  vec![149.0,  148.0,  147.0,  146.0,  145.0],  10.0, "SOL", "USDT"),
        ("SOL_USDT", "sell", vec![151.0,  152.0,  153.0,  154.0,  155.0],  10.0, "SOL", "USDT"),
        ("BTC_USDT", "buy",  vec![50800.0, 50700.0, 50600.0, 50500.0, 50400.0], 0.1, "BTC", "USDT"),
        ("BTC_USDT", "sell", vec![51200.0, 51300.0, 51400.0, 51500.0, 51600.0], 0.1, "BTC", "USDT"),
    ];

    let repo = AccountRepository::new(&s.db);
    let mut placed = 0usize;

    for (symbol, side, prices, qty, base, quote) in plans {
        // Per-symbol idempotency: skip this symbol if it already has active orders.
        let already: Option<i64> = sqlx::query_scalar(
            "SELECT id FROM orders WHERE symbol = $1 AND user_id = $2 AND status IN ('PENDING','TRADING') LIMIT 1",
        )
        .bind(symbol).bind(demo_user_id)
        .fetch_optional(s.db.as_ref()).await.unwrap_or(None);
        if already.is_some() { continue; }

        let engine = match s.engines.get(symbol) {
            Some(e) => e.value().clone(),
            None => continue,
        };
        let engine_side = if side == "buy" { Side::Buy } else { Side::Sell };

        for price in prices {
            // Freeze the appropriate funds first (best-effort).
            let freeze = if side == "buy" {
                repo.freeze_for_buy(demo_user_id, quote, price * qty).await
            } else {
                repo.freeze_for_sell(demo_user_id, base, qty).await
            };
            if let Err(e) = freeze {
                tracing::warn!("seed-demo freeze failed for {} {} @ {}: {}", symbol, side, price, e);
                continue;
            }

            // Insert the order row, getting a fresh id from the sequence.
            let inserted = sqlx::query_as::<_, DbOrder>(
                "INSERT INTO orders (id, user_id, symbol, side, order_type, price, quantity, filled, status)
                 VALUES (nextval('orders_id_seq'), $1, $2, $3, 'limit', $4, $5, 0, 'PENDING') RETURNING *",
            )
            .bind(demo_user_id)
            .bind(symbol)
            .bind(side)
            .bind(price)
            .bind(qty)
            .fetch_one(s.db.as_ref())
            .await;

            let db_order = match inserted {
                Ok(o) => o,
                Err(e) => {
                    tracing::warn!("seed-demo insert failed for {} {} @ {}: {}", symbol, side, price, e);
                    // Roll back the freeze we just did.
                    let asset = if side == "buy" { quote } else { base };
                    let amount = if side == "buy" { price * qty } else { qty };
                    let _ = repo.release_frozen(demo_user_id, asset, amount).await;
                    continue;
                }
            };

            // Place into the engine book directly (no matching, no fund effects).
            let order = Order::new(db_order.id as u64, engine_side, price, qty, TimeInForce::GTC, 0);
            let mut eng = engine.lock().unwrap();
            let _ = eng.add_to_book(order);
            placed += 1;
        }
    }

    (StatusCode::OK, Json(json!({"status": "seeded", "orders_placed": placed, "demo_user_id": demo_user_id}))).into_response()
}
