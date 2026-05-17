use crate::api::AppState;
use crate::engine::OrderStatus;
use crate::order::{Order, Side, TimeInForce};
use crate::user_service;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

// ─── Client → Server message types ───────────────────────────────────────────
// Parse from raw Value to avoid serde internal-tag conflict with a field also named "type".

#[derive(Debug)]
enum ClientMsg {
    Auth { token: String },
    Subscribe { channels: Vec<String> },
    Unsubscribe { channels: Vec<String> },
    PlaceOrder {
        client_order_id: String,
        symbol: String,
        side: String,
        order_type: String,
        price: Option<f64>,
        qty: f64,
    },
    CancelOrder { order_id: i64 },
    Ping,
}

impl ClientMsg {
    fn parse(text: &str) -> Option<Self> {
        let v: Value = serde_json::from_str(text).ok()?;
        let t = v.get("type")?.as_str()?;
        match t {
            "auth" => Some(ClientMsg::Auth {
                token: v.get("token")?.as_str()?.to_owned(),
            }),
            "subscribe" => {
                let channels = v.get("channels")?
                    .as_array()?
                    .iter()
                    .filter_map(|c| c.as_str().map(|s| s.to_owned()))
                    .collect();
                Some(ClientMsg::Subscribe { channels })
            }
            "unsubscribe" => {
                let channels = v.get("channels")?
                    .as_array()?
                    .iter()
                    .filter_map(|c| c.as_str().map(|s| s.to_owned()))
                    .collect();
                Some(ClientMsg::Unsubscribe { channels })
            }
            "place_order" => Some(ClientMsg::PlaceOrder {
                client_order_id: v.get("client_order_id")?.as_str()?.to_owned(),
                symbol: v.get("symbol")?.as_str()?.to_owned(),
                side: v.get("side")?.as_str()?.to_owned(),
                // Clients send the order sub-type in the "order_type" field
                // ("limit", "market", "ioc", "fok", "post_only").
                order_type: v.get("order_type")
                    .and_then(|x| x.as_str())
                    .unwrap_or("limit")
                    .to_owned(),
                price: v.get("price").and_then(|p| p.as_f64()),
                qty: v.get("qty").or_else(|| v.get("quantity"))?.as_f64()?,
            }),
            "cancel_order" => Some(ClientMsg::CancelOrder {
                order_id: v.get("order_id")?.as_i64()?,
            }),
            "ping" => Some(ClientMsg::Ping),
            _ => None,
        }
    }
}

// ─── Per-connection session state ─────────────────────────────────────────────

struct WsSession {
    user_id: Option<i64>,
    subscribed: HashSet<String>,
}

impl WsSession {
    fn new() -> Self {
        Self {
            user_id: None,
            subscribed: HashSet::new(),
        }
    }
}

// ─── Upgrade handler ──────────────────────────────────────────────────────────

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

// ─── Socket loop ──────────────────────────────────────────────────────────────

async fn handle_socket(mut socket: WebSocket, state: AppState) {
    let mut session = WsSession::new();

    // Personal update channel for this connection.
    let (personal_tx, mut personal_rx) = mpsc::channel::<String>(64);
    // Broadcast subscriber — create before entering loop so we don't miss messages.
    let mut market_rx = state.market_tx.subscribe();

    loop {
        tokio::select! {
            // Incoming message from client
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        if let Some(reply) = handle_client_message(
                            &text, &mut session, &state, personal_tx.clone()
                        ).await {
                            if socket.send(Message::Text(reply)).await.is_err() {
                                break;
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {}
                }
            }

            // Personal order/balance update for this user
            Some(msg) = personal_rx.recv() => {
                if socket.send(Message::Text(msg)).await.is_err() {
                    break;
                }
            }

            // Broadcast market data (depth, trades)
            Ok(msg) = market_rx.recv() => {
                // Only forward if this client subscribed to a matching channel.
                // We do a quick prefix check on the JSON "type" field to avoid
                // deserializing the whole payload.
                let should_forward = if msg.contains("\"type\":\"depth\"") {
                    msg_symbol(&msg).map(|sym| {
                        session.subscribed.contains(&format!("depth.{}", sym))
                    }).unwrap_or(false)
                } else if msg.contains("\"type\":\"trade\"") {
                    msg_symbol(&msg).map(|sym| {
                        session.subscribed.contains(&format!("trades.{}", sym))
                    }).unwrap_or(false)
                } else if msg.contains("\"type\":\"ticker\"") {
                    msg_symbol(&msg).map(|sym| {
                        session.subscribed.contains(&format!("ticker.{}", sym))
                    }).unwrap_or(false)
                } else if msg.contains("\"type\":\"kline\"") {
                    msg_symbol(&msg).map(|sym| {
                        session.subscribed.contains(&format!("kline.{}", sym))
                    }).unwrap_or(false)
                } else {
                    false
                };

                if should_forward {
                    if socket.send(Message::Text(msg)).await.is_err() {
                        break;
                    }
                }
            }
        }
    }

    // Remove personal channel on disconnect.
    if let Some(uid) = session.user_id {
        state.user_tx.remove(&uid);
    }
}

/// Extract the `symbol` field from a JSON string cheaply (no full parse).
fn msg_symbol(msg: &str) -> Option<&str> {
    let key = "\"symbol\":\"";
    let start = msg.find(key)? + key.len();
    let end = msg[start..].find('"')? + start;
    Some(&msg[start..end])
}

// ─── Per-message handler ──────────────────────────────────────────────────────

async fn handle_client_message(
    text: &str,
    session: &mut WsSession,
    state: &AppState,
    personal_tx: mpsc::Sender<String>,
) -> Option<String> {
    let msg: ClientMsg = match ClientMsg::parse(text) {
        Some(m) => m,
        None => return Some(json!({"type": "error", "message": "Invalid message format"}).to_string()),
    };

    match msg {
        ClientMsg::Auth { token } => {
            match user_service::verify_token(&token) {
                Ok(claims) => {
                    session.user_id = Some(claims.sub);
                    state.user_tx.insert(claims.sub, personal_tx);
                    Some(json!({"type": "auth_ok"}).to_string())
                }
                Err(e) => Some(json!({"type": "auth_error", "message": e.to_string()}).to_string()),
            }
        }

        ClientMsg::Subscribe { channels } => {
            for ch in channels {
                session.subscribed.insert(ch);
            }
            None
        }

        ClientMsg::Unsubscribe { channels } => {
            for ch in channels {
                session.subscribed.remove(&ch);
            }
            None
        }

        ClientMsg::Ping => Some(json!({"type": "pong"}).to_string()),

        ClientMsg::PlaceOrder { client_order_id, symbol, side, order_type, price, qty } => {
            let user_id = match session.user_id {
                Some(id) => id,
                None => return Some(json!({"type": "order_rejected", "client_order_id": client_order_id, "reason": "Not authenticated"}).to_string()),
            };

            // Basic validation
            if qty <= 0.0 {
                return Some(json!({"type": "order_rejected", "client_order_id": client_order_id, "reason": "qty must be > 0"}).to_string());
            }
            if order_type != "market" && price.map(|p| p <= 0.0).unwrap_or(true) {
                return Some(json!({"type": "order_rejected", "client_order_id": client_order_id, "reason": "price required for limit orders"}).to_string());
            }

            let engine_side = match side.as_str() {
                "buy" => Side::Buy,
                "sell" => Side::Sell,
                _ => return Some(json!({"type": "order_rejected", "client_order_id": client_order_id, "reason": "Invalid side"}).to_string()),
            };

            let tif = match order_type.as_str() {
                "limit" | "gtc" => TimeInForce::GTC,
                "ioc" => TimeInForce::IOC,
                "fok" => TimeInForce::FOK,
                "post_only" => TimeInForce::PostOnly,
                "market" => TimeInForce::IOC, // market orders use IOC semantics internally
                _ => return Some(json!({"type": "order_rejected", "client_order_id": client_order_id, "reason": "Unknown order type"}).to_string()),
            };

            let now_ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0);

            // Allocate an engine-level order ID.
            let order_id = state.next_order_id.fetch_add(1, Ordering::Relaxed);

            let engine_order = if order_type == "market" {
                Order::new_market(order_id, engine_side, qty, now_ns)
            } else {
                Order::new(order_id, engine_side, price.unwrap_or(0.0), qty, tif, now_ns)
            };

            // Persist to DB as PENDING before running through engine.
            let db_result = sqlx::query_as::<_, crate::models::DbOrder>(
                "INSERT INTO orders (id, user_id, symbol, side, order_type, price, quantity, filled, status)
                 VALUES (nextval('orders_id_seq'), $1, $2, $3, $4, $5, $6, 0, 'PENDING') RETURNING *",
            )
            .bind(user_id)
            .bind(&symbol)
            .bind(&side)
            .bind(&order_type)
            .bind(price)
            .bind(qty)
            .fetch_one(state.db.as_ref())
            .await;

            let db_order_id = match db_result {
                Ok(o) => o.id,
                Err(e) => {
                    return Some(json!({
                        "type": "order_rejected",
                        "client_order_id": client_order_id,
                        "reason": format!("DB error: {}", e)
                    }).to_string());
                }
            };

            // Capture best opposing price before matching (used as fill price for market orders).
            let best_opposing_price = {
                let eng = state.engine.lock().unwrap();
                let levels = eng.get_top_levels(1, engine_side == Side::Sell); // buy side when selling
                levels.first().map(|(p, _)| *p)
            };

            // Run through matching engine — hold lock briefly.
            let engine_result = {
                let mut eng = state.engine.lock().unwrap();
                eng.place_order(engine_order)
            };

            let result = match engine_result {
                Ok(r) => r,
                Err(e) => {
                    // Update DB to REJECTED
                    let _ = sqlx::query(
                        "UPDATE orders SET status = 'REJECTED', updated_at = NOW() WHERE id = $1",
                    )
                    .bind(db_order_id)
                    .execute(state.db.as_ref())
                    .await;

                    return Some(json!({
                        "type": "order_rejected",
                        "client_order_id": client_order_id,
                        "reason": e.to_string()
                    }).to_string());
                }
            };

            // Map engine status to DB status string.
            let (db_status, ws_status) = match result.status {
                OrderStatus::Accepted => ("TRADING", "OPEN"),
                OrderStatus::PartiallyFilled => ("TRADING", "PARTIAL_FILL"),
                OrderStatus::Filled => ("COMPLETED", "FILLED"),
                OrderStatus::Rejected => ("REJECTED", "REJECTED"),
                OrderStatus::Cancelled => ("CANCELED", "CANCELED"),
            };

            // Update DB with fill info.
            let _ = sqlx::query(
                "UPDATE orders SET status = $1, filled = $2, updated_at = NOW() WHERE id = $3",
            )
            .bind(db_status)
            .bind(result.filled)
            .bind(db_order_id)
            .execute(state.db.as_ref())
            .await;

            if result.status == OrderStatus::Rejected {
                return Some(json!({
                    "type": "order_rejected",
                    "client_order_id": client_order_id,
                    "reason": "Rejected by matching engine"
                }).to_string());
            }

            let ts = unix_now();

            // Broadcast trade event if there were fills.
            if result.filled > 0.0 {
                // Fill price: limit orders use their own price; market orders use best opposing price.
                let fill_price = price.or(best_opposing_price).unwrap_or(0.0);
                let trade_msg = json!({
                    "type": "trade",
                    "symbol": symbol,
                    "price": fill_price,
                    "qty": result.filled,
                    "side": side,
                    "ts": ts
                }).to_string();
                let _ = state.market_tx.send(trade_msg);

                // Broadcast updated depth.
                broadcast_depth(state, &symbol);

                // Send order_update to this user's personal channel.
                let update_msg = json!({
                    "type": "order_update",
                    "order_id": db_order_id,
                    "status": ws_status,
                    "filled_qty": result.filled,
                    "avg_price": fill_price,
                    "ts": ts
                }).to_string();
                if let Some(tx) = state.user_tx.get(&user_id) {
                    let _ = tx.send(update_msg).await;
                }
            }

            Some(json!({
                "type": "order_accepted",
                "client_order_id": client_order_id,
                "order_id": db_order_id,
                "symbol": symbol,
                "side": side,
                "price": price,
                "qty": qty,
                "ts": ts
            }).to_string())
        }

        ClientMsg::CancelOrder { order_id } => {
            let user_id = match session.user_id {
                Some(id) => id,
                None => return Some(json!({"type": "error", "message": "Not authenticated"}).to_string()),
            };

            // Update DB first.
            let db_result = sqlx::query(
                "UPDATE orders SET status = 'CANCELED', updated_at = NOW()
                 WHERE id = $1 AND user_id = $2 AND status IN ('PENDING','TRADING')",
            )
            .bind(order_id)
            .bind(user_id)
            .execute(state.db.as_ref())
            .await;

            match db_result {
                Ok(r) if r.rows_affected() > 0 => {
                    // Best-effort cancel in engine (may not be in engine if already filled).
                    let _ = {
                        let mut eng = state.engine.lock().unwrap();
                        eng.cancel_order(order_id as u64)
                    };
                    Some(json!({"type": "order_update", "order_id": order_id, "status": "CANCELED", "ts": unix_now()}).to_string())
                }
                Ok(_) => Some(json!({"type": "error", "message": "Order not found or already closed"}).to_string()),
                Err(e) => Some(json!({"type": "error", "message": e.to_string()}).to_string()),
            }
        }
    }
}

/// Build and broadcast a depth snapshot for the given symbol.
fn broadcast_depth(state: &AppState, symbol: &str) {
    let (bids, asks) = {
        let eng = state.engine.lock().unwrap();
        (eng.get_top_levels(10, true), eng.get_top_levels(10, false))
    };
    let msg = json!({
        "type": "depth",
        "symbol": symbol,
        "bids": bids,
        "asks": asks,
        "ts": unix_now()
    }).to_string();
    let _ = state.market_tx.send(msg);
}

// ─── Background market data broadcaster ─────────────────────────────────────

pub async fn market_data_broadcaster(state: AppState) {
    let mut depth_interval = tokio::time::interval(std::time::Duration::from_secs(2));
    let mut kline_interval = tokio::time::interval(std::time::Duration::from_secs(60));
    // Track last trade price for kline generation when no trades occur.
    let mut last_price: f64 = 0.0;

    loop {
        tokio::select! {
            _ = depth_interval.tick() => {
                let (bids, asks) = {
                    let eng = state.engine.lock().unwrap();
                    (eng.get_top_levels(10, true), eng.get_top_levels(10, false))
                };
                // Update last_price from best bid.
                if let Some((p, _)) = bids.first() {
                    last_price = *p;
                }
                let msg = json!({
                    "type": "depth",
                    "symbol": "BTC_USDT",
                    "bids": bids,
                    "asks": asks,
                    "ts": unix_now()
                }).to_string();
                let _ = state.market_tx.send(msg);
            }

            _ = kline_interval.tick() => {
                // Send a synthetic 1-minute kline bar using the current price.
                // When trades occur, the WS place_order handler broadcasts individual
                // trade events; this bar covers quiet periods.
                if last_price > 0.0 {
                    // Round down to the current minute.
                    let now = unix_now();
                    let bar_time = now - (now % 60);
                    let msg = json!({
                        "type": "kline",
                        "symbol": "BTC_USDT",
                        "bar": {
                            "time": bar_time,
                            "open": last_price,
                            "high": last_price,
                            "low":  last_price,
                            "close": last_price,
                            "volume": 0.0
                        }
                    }).to_string();
                    let _ = state.market_tx.send(msg);
                }
            }
        }
    }
}
