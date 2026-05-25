use crate::account_repository::AccountRepository;
use crate::api::AppState;
use crate::engine::{MatchingEngine, OrderStatus};
use crate::order::{Order, Side, TimeInForce};
use crate::user_service;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use dashmap::DashMap;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

fn unix_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
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
    CancelSymbol { symbol: String },
    CancelAll,
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
            "cancel_symbol" => Some(ClientMsg::CancelSymbol {
                symbol: v.get("symbol")?.as_str()?.to_owned(),
            }),
            "cancel_all" => Some(ClientMsg::CancelAll),
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
    // Large capacity to absorb market-making bursts (3 symbols × 120 ops × 2 updates = 720+).
    let (personal_tx, mut personal_rx) = mpsc::channel::<String>(65536);
    // Broadcast subscriber — create before entering loop so we don't miss messages.
    let mut market_rx = state.market_tx.subscribe();

    'conn: loop {
        tokio::select! {
            // Incoming message from client
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        if let Some(reply) = handle_client_message(
                            &text, &mut session, &state, personal_tx.clone()
                        ).await {
                            if socket.send(Message::Text(reply)).await.is_err() {
                                break 'conn;
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break 'conn,
                    _ => {}
                }
            }

            // Personal order/balance update for this user
            Some(msg) = personal_rx.recv() => {
                if socket.send(Message::Text(msg)).await.is_err() {
                    break 'conn;
                }
            }

            // Broadcast market data (depth, trades)
            result = market_rx.recv() => {
                let msg = match result {
                    Ok(m) => m,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue 'conn,
                    Err(_) => break 'conn,
                };
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
                } else if msg.contains("\"type\":\"agg_trade\"") {
                    msg_symbol(&msg).map(|sym| {
                        session.subscribed.contains(&format!("agg_trades.{}", sym))
                    }).unwrap_or(false)
                } else {
                    false
                };

                if should_forward {
                    if socket.send(Message::Text(msg)).await.is_err() {
                        break 'conn;
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
            // Collect depth symbols before moving `channels` into the subscribed set,
            // so we can push an immediate snapshot for each new depth.* channel.
            let depth_symbols: Vec<String> = channels.iter()
                .filter_map(|c| c.strip_prefix("depth.").map(str::to_string))
                .collect();
            for ch in channels {
                session.subscribed.insert(ch);
            }
            for sym in depth_symbols {
                // In standalone mode: push from engine. In Aeron mode: push from last_depth cache.
                if let Some(engines) = &state.engines {
                    if let Some(engine) = engines.get(&sym) {
                        let _ = personal_tx.try_send(build_depth_json(engine.value(), &sym));
                    }
                } else if let Some(depth_json) = state.last_depth.get(&sym) {
                    let _ = personal_tx.try_send(depth_json.to_string());
                }
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

            // Look up the matching engine for this symbol; only available in standalone mode.
            let engine_opt: Option<Arc<Mutex<MatchingEngine>>> = state.engines
                .as_ref()
                .and_then(|m| m.get(&symbol).map(|e| e.value().clone()));
            // In standalone mode, reject unknown symbols up front.
            if state.engines.is_some() && engine_opt.is_none() {
                return Some(json!({
                    "type": "order_rejected",
                    "client_order_id": client_order_id,
                    "reason": format!("Unknown symbol: {}", symbol)
                }).to_string());
            }

            let now_ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0);

            // Parse base/quote assets from symbol (e.g. "BTC_USDT" → "BTC", "USDT").
            let sym_parts: Vec<&str> = symbol.splitn(2, '_').collect();
            let base_asset = sym_parts.first().copied().unwrap_or("BTC");
            let quote_asset = sym_parts.last().copied().unwrap_or("USDT");

            // Capture best opposing price before matching — used as the fill
            // price for market orders AND persisted as freeze_price so the
            // restart cleanup can release the exact amount later.
            let best_opposing_price: Option<f64> = if let Some(ref engine) = engine_opt {
                let eng = engine.lock().unwrap();
                let levels = eng.get_top_levels(1, engine_side == Side::Sell);
                levels.first().map(|(p, _)| *p)
            } else {
                None
            };

            // Price that backs the frozen quote-asset amount (buy only).
            // Sells freeze base_asset by quantity → 0.0 by convention.
            let freeze_price_val: f64 = if side == "buy" {
                price.or(best_opposing_price).unwrap_or(0.0)
            } else {
                0.0
            };

            // ── Aeron fast path: skip DB + freeze in critical path ──────────────
            // Order ID assigned atomically; DB INSERT + fund freeze happen
            // in the Aeron event loop after the engine confirms ACCEPTED.
            if let Some(aeron_cmd_tx) = &state.aeron_cmd_tx {
                use crate::sbe::NewOrderRequest as SbeNewOrder;
                use crate::tracer::MS_WS_ORDER_RECV;
                use crate::transport::{AeronCmd, OrderMeta};

                let order_id = state.next_order_id.fetch_add(1, Ordering::Relaxed);
                // Build sym_bytes first so the tracer checkpoint carries the symbol.
                let mut sym_bytes = [0u8; 16];
                let sb = symbol.as_bytes();
                sym_bytes[..sb.len().min(16)].copy_from_slice(&sb[..sb.len().min(16)]);
                if let Some(ref t) = state.tracer {
                    t.record_sym(MS_WS_ORDER_RECV, order_id, &sym_bytes);
                }
                let side_byte: u8 = if engine_side == Side::Buy { 0 } else { 1 };
                let tif_byte: u8 = match tif {
                    TimeInForce::GTC      => 0,
                    TimeInForce::IOC      => 1,
                    TimeInForce::FOK      => 2,
                    TimeInForce::PostOnly => 3,
                };

                let sbe_req = SbeNewOrder {
                    client_order_id: order_id,
                    participant_id: user_id as u64,
                    price: price.unwrap_or(0.0),
                    quantity: qty,
                    side: side_byte,
                    time_in_force: tif_byte,
                    _pad: [0; 14],
                    symbol: sym_bytes,
                };

                state.pending_meta.insert(order_id, Box::new(OrderMeta {
                    user_id,
                    symbol: symbol.clone(),
                    side: side.clone(),
                    order_type: order_type.clone(),
                    price,
                    qty,
                    client_order_id: client_order_id.clone(),
                    freeze_price: freeze_price_val,
                }));

                if aeron_cmd_tx.send(AeronCmd::NewOrder(sbe_req)).is_err() {
                    state.pending_meta.remove(&order_id);
                    return Some(json!({
                        "type": "order_rejected",
                        "client_order_id": client_order_id,
                        "reason": "Aeron channel closed"
                    }).to_string());
                }

                return Some(json!({
                    "type": "order_submitted",
                    "client_order_id": client_order_id,
                    "order_id": order_id,
                    "symbol": symbol,
                    "side": side,
                    "order_type": order_type,
                    "price": price,
                    "quantity": qty,
                    "ts": unix_now()
                }).to_string());
            }

            // ── Standalone engine path: DB + freeze + local matching ─────────────
            let engine = match engine_opt {
                Some(e) => e,
                None => return Some(json!({
                    "type": "order_rejected",
                    "client_order_id": client_order_id,
                    "reason": "No matching engine configured"
                }).to_string()),
            };

            // Persist to DB as PENDING before running through engine.
            let db_result = sqlx::query_as::<_, crate::models::DbOrder>(
                "INSERT INTO orders (id, user_id, symbol, side, order_type, price, quantity, filled, status, freeze_price)
                 VALUES (nextval('orders_id_seq'), $1, $2, $3, $4, $5, $6, 0, 'PENDING', $7) RETURNING *",
            )
            .bind(user_id)
            .bind(&symbol)
            .bind(&side)
            .bind(&order_type)
            .bind(price)
            .bind(qty)
            .bind(freeze_price_val)
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
            let engine_order = if order_type == "market" {
                Order::new_market(db_order_id as u64, engine_side, qty, now_ns)
            } else {
                Order::new(db_order_id as u64, engine_side, price.unwrap_or(0.0), qty, tif, now_ns)
            };
            state.next_order_id.fetch_max(db_order_id as u64 + 1, Ordering::Relaxed);

            // Freeze funds before sending to engine.
            let repo = AccountRepository::new(state.db.as_ref());
            let freeze_result = if side == "buy" {
                let freeze_amount = freeze_price_val * qty;
                if freeze_amount > 0.0 {
                    repo.freeze_for_buy(user_id, quote_asset, freeze_amount).await
                } else {
                    Ok(())
                }
            } else {
                repo.freeze_for_sell(user_id, base_asset, qty).await
            };
            if let Err(e) = freeze_result {
                let _ = sqlx::query("UPDATE orders SET status='REJECTED', updated_at=NOW() WHERE id=$1")
                    .bind(db_order_id).execute(state.db.as_ref()).await;
                return Some(json!({
                    "type": "order_rejected",
                    "client_order_id": client_order_id,
                    "reason": e.to_string()
                }).to_string());
            }
            {
                let frozen_asset = if side == "buy" { quote_asset } else { base_asset };
                if let Ok(acc) = repo.get_account(user_id, frozen_asset).await {
                    if let Some(tx) = state.user_tx.get(&user_id) {
                        let _ = tx.try_send(json!({
                            "type": "balance_update",
                            "asset": frozen_asset,
                            "balance": acc.balance,
                            "available": acc.balance - acc.frozen,
                            "frozen": acc.frozen,
                        }).to_string());
                    }
                }
            }

            let result = {
                let engine_result = {
                    let mut eng = engine.lock().unwrap();
                    eng.place_order(engine_order)
                };
                match engine_result {
                    Ok(r) => r,
                    Err(e) => {
                        let _ = sqlx::query(
                            "UPDATE orders SET status = 'REJECTED', updated_at = NOW() WHERE id = $1",
                        )
                        .bind(db_order_id)
                        .execute(state.db.as_ref())
                        .await;
                        if side == "buy" {
                            let p = price.or(best_opposing_price).unwrap_or(0.0);
                            if p > 0.0 { let _ = repo.release_frozen(user_id, quote_asset, p * qty).await; }
                        } else {
                            let _ = repo.release_frozen(user_id, base_asset, qty).await;
                        }
                        return Some(json!({
                            "type": "order_rejected",
                            "client_order_id": client_order_id,
                            "reason": e.to_string()
                        }).to_string());
                    }
                }
            };

            // Map engine status to (DB, WS) status strings. IOC/FOK that
            // partially fill come back as Cancelled with filled>0 — the
            // meaningful event for the trader is the partial fill, not the
            // trailing cancel of the remainder, so surface PARTIAL_FILL.
            let (db_status, ws_status) = match (result.status, result.filled > 0.0) {
                (OrderStatus::Accepted, _)          => ("TRADING",  "OPEN"),
                (OrderStatus::PartiallyFilled, _)   => ("TRADING",  "PARTIAL_FILL"),
                (OrderStatus::Filled, _)            => ("COMPLETED","FILLED"),
                (OrderStatus::Rejected, _)          => ("REJECTED", "REJECTED"),
                (OrderStatus::Cancelled, true)      => ("CANCELED", "PARTIAL_FILL"),
                (OrderStatus::Cancelled, false)     => ("CANCELED", "CANCELED"),
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
                // Release frozen funds — no fills occurred.
                if side == "buy" {
                    let p = price.or(best_opposing_price).unwrap_or(0.0);
                    if p > 0.0 { let _ = repo.release_frozen(user_id, quote_asset, p * qty).await; }
                } else {
                    let _ = repo.release_frozen(user_id, base_asset, qty).await;
                }
                return Some(json!({
                    "type": "order_rejected",
                    "client_order_id": client_order_id,
                    "reason": "Rejected by matching engine"
                }).to_string());
            }

            let ts = unix_now();

            // Settle fills and broadcast trade events.
            if result.filled > 0.0 {
                let total_filled = result.filled;
                // Weighted average price across all fills for market-event broadcasting.
                let avg_fill_price = if !result.fills.is_empty() {
                    let cost: f64 = result.fills.iter().map(|&(_, p, q)| p * q).sum();
                    cost / total_filled
                } else {
                    price.or(best_opposing_price).unwrap_or(0.0)
                };
                let fill_price = avg_fill_price;

                // Per-fill: settle both taker and maker atomically, record trade, update maker order.
                for &(maker_order_id, fp, fq) in &result.fills {
                    if fp <= 0.0 || fq <= 0.0 { continue; }

                    let maker_uid: Option<i64> = sqlx::query_scalar(
                        "SELECT user_id FROM orders WHERE id = $1",
                    )
                    .bind(maker_order_id as i64)
                    .fetch_optional(state.db.as_ref())
                    .await
                    .unwrap_or(None);

                    let (buyer_id, seller_id) = if side == "buy" {
                        (user_id, maker_uid.unwrap_or(0))
                    } else {
                        (maker_uid.unwrap_or(0), user_id)
                    };

                    if buyer_id > 0 && seller_id > 0 {
                        // Release taker's over-frozen (limit buy filled at better price).
                        if side == "buy" {
                            let over = price.map(|lp| (lp - fp) * fq).unwrap_or(0.0).max(0.0);
                            if over > 0.0 { let _ = repo.release_frozen(user_id, quote_asset, over).await; }
                        }
                        let _ = repo.settle_trade(buyer_id, seller_id, base_asset, quote_asset, fp, fq, 0.0, 0.0).await;

                        // Notify maker of balance change and position change.
                        if let Some(maker_id) = maker_uid {
                            let (m_debit, m_credit) = if side == "buy" { (base_asset, quote_asset) } else { (quote_asset, base_asset) };
                            for asset in [m_debit, m_credit] {
                                if let Ok(acc) = repo.get_account(maker_id, asset).await {
                                    let msg = json!({"type":"balance_update","asset":asset,"balance":acc.balance,"available":acc.balance-acc.frozen,"frozen":acc.frozen}).to_string();
                                    if let Some(tx) = state.user_tx.get(&maker_id) { let _ = tx.try_send(msg); }
                                }
                            }
                            if let Some(pos_msg) = crate::positions::position_update_msg(
                                state.db.as_ref(), maker_id, base_asset,
                            ).await {
                                if let Some(tx) = state.user_tx.get(&maker_id) { let _ = tx.try_send(pos_msg); }
                            }
                        }
                    }

                    // Update maker order status and filled quantity in DB; then push order_update.
                    // RETURNING `filled` gives the post-update cumulative qty so the WS payload
                    // matches taker semantics — frontend treats `filled_qty` as cumulative.
                    let maker_row: Option<(String, f64)> = sqlx::query_as(
                        "UPDATE orders SET filled = filled + $1,
                         status = CASE WHEN filled + $1 >= quantity THEN 'COMPLETED' ELSE 'TRADING' END,
                         updated_at = NOW() WHERE id = $2
                         RETURNING status, filled",
                    )
                    .bind(fq)
                    .bind(maker_order_id as i64)
                    .fetch_optional(state.db.as_ref())
                    .await
                    .unwrap_or(None);

                    // Notify maker of their order state change.
                    if let (Some(maker_id), Some((new_status, new_filled))) = (maker_uid, maker_row) {
                        let ws_maker_status = match new_status.as_str() {
                            "COMPLETED" => "FILLED",
                            "TRADING"   => "PARTIAL_FILL",
                            other        => other,
                        };
                        let upd = json!({
                            "type": "order_update",
                            "order_id": maker_order_id as i64,
                            "status": ws_maker_status,
                            "filled_qty": new_filled,
                            "fill_delta": fq,
                            "avg_price": fp,
                            "ts": ts
                        }).to_string();
                        if let Some(tx) = state.user_tx.get(&maker_id) { let _ = tx.try_send(upd); }
                    }

                    // Insert trade record with both sides.
                    let (buy_oid, sell_oid) = if side == "buy" {
                        (Some(db_order_id), Some(maker_order_id as i64))
                    } else {
                        (Some(maker_order_id as i64), Some(db_order_id))
                    };
                    let _ = sqlx::query(
                        "INSERT INTO trades (symbol, buy_order_id, sell_order_id, price, quantity, created_at)
                         VALUES ($1, $2, $3, $4, $5, NOW())",
                    )
                    .bind(&symbol).bind(buy_oid).bind(sell_oid).bind(fp).bind(fq)
                    .execute(state.db.as_ref())
                    .await;
                }

                // Release unfilled frozen amount for IOC/FOK that partially filled.
                if result.status == OrderStatus::Cancelled || result.status == OrderStatus::Rejected {
                    let unfilled = qty - total_filled;
                    if unfilled > 0.0 {
                        let (rel_asset, rel_amount) = if side == "buy" {
                            (quote_asset, price.or(best_opposing_price).unwrap_or(fill_price) * unfilled)
                        } else {
                            (base_asset, unfilled)
                        };
                        let _ = repo.release_frozen(user_id, rel_asset, rel_amount).await;
                    }
                }

                let trade_msg = json!({
                    "type": "trade",
                    "symbol": symbol,
                    "price": fill_price,
                    "qty": total_filled,
                    "side": side,
                    "ts": ts
                }).to_string();
                let _ = state.market_tx.send(trade_msg);

                // Broadcast ticker update with live last price + 24h change.
                let sym_clone = symbol.to_string();
                let db_clone = state.db.clone();
                let mtx_clone = state.market_tx.clone();
                tokio::spawn(async move {
                    let open_24h: Option<f64> = sqlx::query_scalar(
                        "SELECT price FROM trades WHERE symbol=$1 AND created_at > NOW() - INTERVAL '24 hours' ORDER BY created_at ASC LIMIT 1"
                    )
                    .bind(&sym_clone)
                    .fetch_optional(db_clone.as_ref())
                    .await
                    .unwrap_or(None);
                    let change = match open_24h {
                        Some(o) if o != 0.0 => (fill_price - o) / o * 100.0,
                        _ => 0.0,
                    };
                    let ticker = serde_json::json!({
                        "type": "ticker",
                        "symbol": sym_clone,
                        "last": fill_price,
                        "change": change,
                    }).to_string();
                    let _ = mtx_clone.send(ticker);
                });

                broadcast_depth_pub(state, &symbol);

                // Push order_update + balance_update to this user's personal channel.
                let update_msg = json!({
                    "type": "order_update",
                    "order_id": db_order_id,
                    "status": ws_status,
                    "filled_qty": total_filled,
                    "avg_price": fill_price,
                    "ts": ts
                }).to_string();
                if let Some(tx) = state.user_tx.get(&user_id) {
                    let _ = tx.try_send(update_msg);
                }

                // Send balance_update so frontend can refresh balances.
                let (debit_asset, credit_asset) = if side == "buy" { (quote_asset, base_asset) } else { (base_asset, quote_asset) };
                for asset in [debit_asset, credit_asset] {
                    if let Ok(acc) = repo.get_account(user_id, asset).await {
                        let bal_msg = json!({
                            "type": "balance_update",
                            "asset": asset,
                            "balance": acc.balance,
                            "available": acc.balance - acc.frozen,
                            "frozen": acc.frozen
                        }).to_string();
                        if let Some(tx) = state.user_tx.get(&user_id) {
                            let _ = tx.try_send(bal_msg);
                        }
                    }
                }

                // Position update for the base asset whose holdings just changed.
                if let Some(pos_msg) = crate::positions::position_update_msg(
                    state.db.as_ref(), user_id, base_asset,
                ).await {
                    if let Some(tx) = state.user_tx.get(&user_id) {
                        let _ = tx.try_send(pos_msg);
                    }
                }
            } else if result.status == OrderStatus::Accepted {
                // GTC limit order resting in the book. Frozen funds STAY frozen
                // until it fills or the user cancels — releasing here would
                // double-spend when the maker fills later.
                let open_msg = json!({
                    "type": "order_update",
                    "order_id": db_order_id,
                    "status": "OPEN",
                    "filled_qty": 0.0,
                    "ts": ts
                }).to_string();
                if let Some(tx) = state.user_tx.get(&user_id) {
                    let _ = tx.try_send(open_msg);
                }
                // New resting order changes the book — push depth so the
                // frontend reflects it without waiting for the 2s tick.
                broadcast_depth_pub(state, &symbol);
            } else {
                // No fill, not resting — IOC/FOK fully rejected, market with no
                // matchable book. Release all frozen funds and notify CANCELED.
                let (rel_asset, rel_amount) = if side == "buy" {
                    let p = price.or(best_opposing_price).unwrap_or(0.0);
                    (quote_asset, p * qty)
                } else {
                    (base_asset, qty)
                };
                if rel_amount > 0.0 {
                    let _ = repo.release_frozen(user_id, rel_asset, rel_amount).await;
                }

                // Notify user the order is CANCELED so frontend can drop it from openOrders.
                let cancel_msg = json!({
                    "type": "order_update",
                    "order_id": db_order_id,
                    "status": "CANCELED",
                    "filled_qty": 0.0,
                    "ts": ts
                }).to_string();
                if let Some(tx) = state.user_tx.get(&user_id) {
                    let _ = tx.try_send(cancel_msg);
                }

                // Refresh frontend balance for the released asset (frozen → available).
                if rel_amount > 0.0 {
                    if let Ok(acc) = repo.get_account(user_id, rel_asset).await {
                        let bal_msg = json!({
                            "type": "balance_update",
                            "asset": rel_asset,
                            "balance": acc.balance,
                            "available": acc.balance - acc.frozen,
                            "frozen": acc.frozen
                        }).to_string();
                        if let Some(tx) = state.user_tx.get(&user_id) {
                            let _ = tx.try_send(bal_msg);
                        }
                    }
                }
            }

            Some(json!({
                "type": "order_accepted",
                "client_order_id": client_order_id,
                "order_id": db_order_id,
                "symbol": symbol,
                "side": side,
                "order_type": order_type,
                "price": price,
                "quantity": qty,
                "ts": ts
            }).to_string())
        }

        ClientMsg::CancelSymbol { symbol } => {
            let user_id = match session.user_id {
                Some(id) => id,
                None => return Some(json!({"type": "error", "message": "Not authenticated"}).to_string()),
            };
            let n = bulk_cancel(user_id, Some(&symbol), state, &personal_tx).await;
            Some(json!({"type": "cancel_all_ok", "symbol": symbol, "cancelled": n}).to_string())
        }

        ClientMsg::CancelAll => {
            let user_id = match session.user_id {
                Some(id) => id,
                None => return Some(json!({"type": "error", "message": "Not authenticated"}).to_string()),
            };
            let n = bulk_cancel(user_id, None, state, &personal_tx).await;
            Some(json!({"type": "cancel_all_ok", "cancelled": n}).to_string())
        }

        ClientMsg::CancelOrder { order_id } => {
            let user_id = match session.user_id {
                Some(id) => id,
                None => return Some(json!({"type": "error", "message": "Not authenticated"}).to_string()),
            };

            // Fetch order details before cancelling so we know what to unfreeze.
            let order_row = sqlx::query(
                "SELECT symbol, side, price, quantity, filled FROM orders
                 WHERE id = $1 AND user_id = $2 AND status IN ('PENDING','TRADING')",
            )
            .bind(order_id)
            .bind(user_id)
            .fetch_optional(state.db.as_ref())
            .await;

            let order_row = match order_row {
                Ok(Some(r)) => r,
                Ok(None) => return Some(json!({"type": "error", "message": "Order not found or already closed"}).to_string()),
                Err(e) => return Some(json!({"type": "error", "message": e.to_string()}).to_string()),
            };

            use sqlx::Row;
            let symbol: String = order_row.get("symbol");
            let side: String = order_row.get("side");
            let price: Option<f64> = order_row.get("price");
            let quantity: f64 = order_row.get("quantity");
            let filled: f64 = order_row.get("filled");
            let remaining = quantity - filled;

            // Update DB status.
            let db_result = sqlx::query(
                "UPDATE orders SET status = 'CANCELED', updated_at = NOW() WHERE id = $1",
            )
            .bind(order_id)
            .execute(state.db.as_ref())
            .await;

            if let Err(e) = db_result {
                return Some(json!({"type": "error", "message": e.to_string()}).to_string());
            }

            // Best-effort cancel in engine (standalone) or via Aeron (desk mode).
            if let Some(engines) = &state.engines {
                if let Some(engine) = engines.get(&symbol) {
                    let _ = { let mut eng = engine.lock().unwrap(); eng.cancel_order(order_id as u64) };
                }
            } else if let Some(aeron_cmd_tx) = &state.aeron_cmd_tx {
                let cancel_req = crate::sbe::CancelOrderRequest { order_id: order_id as u64 };
                let _ = aeron_cmd_tx.send(crate::transport::AeronCmd::Cancel(cancel_req));
            }

            // Release frozen funds for the unfilled portion.
            let sym_parts: Vec<&str> = symbol.splitn(2, '_').collect();
            let base_asset = sym_parts.first().copied().unwrap_or("BTC");
            let quote_asset = sym_parts.last().copied().unwrap_or("USDT");
            let repo = AccountRepository::new(state.db.as_ref());
            let released_asset = if side == "buy" {
                let freeze_price = price.unwrap_or(0.0);
                if freeze_price > 0.0 && remaining > 0.0 {
                    let _ = repo.release_frozen(user_id, quote_asset, freeze_price * remaining).await;
                }
                quote_asset
            } else {
                if remaining > 0.0 {
                    let _ = repo.release_frozen(user_id, base_asset, remaining).await;
                }
                base_asset
            };

            // Push balance_update so the frontend OrderForm reflects freed-up funds.
            if let Some(tx) = state.user_tx.get(&user_id) {
                for asset in [released_asset, if released_asset == base_asset { quote_asset } else { base_asset }] {
                    if let Ok(acc) = repo.get_account(user_id, asset).await {
                        let _ = tx.try_send(json!({
                            "type": "balance_update",
                            "asset": acc.asset,
                            "balance": acc.balance,
                            "available": acc.balance - acc.frozen,
                            "frozen": acc.frozen,
                        }).to_string());
                    }
                }
            }

            Some(json!({"type": "order_update", "order_id": order_id, "status": "CANCELED", "ts": unix_now()}).to_string())
        }
    }
}

/// Cancel all open orders for `user_id`, optionally filtered to a single symbol.
/// Cancels each order in the engine, updates DB, releases frozen funds, and
/// pushes an `order_update CANCELED` event to the user's personal WS channel.
/// Returns the number of orders cancelled.
async fn bulk_cancel(
    user_id: i64,
    symbol: Option<&str>,
    state: &AppState,
    personal_tx: &tokio::sync::mpsc::Sender<String>,
) -> usize {
    #[derive(sqlx::FromRow)]
    struct OpenOrder { id: i64, symbol: String, side: String, price: Option<f64>, quantity: f64, filled: f64 }

    let rows: Vec<OpenOrder> = match symbol {
        Some(sym) => sqlx::query_as(
            "SELECT id, symbol, side, price, quantity, filled FROM orders
             WHERE user_id=$1 AND symbol=$2 AND status IN ('PENDING','TRADING')",
        ).bind(user_id).bind(sym),
        None => sqlx::query_as(
            "SELECT id, symbol, side, price, quantity, filled FROM orders
             WHERE user_id=$1 AND status IN ('PENDING','TRADING')",
        ).bind(user_id),
    }
    .fetch_all(state.db.as_ref())
    .await
    .unwrap_or_default();

    if rows.is_empty() { return 0; }

    let repo = crate::account_repository::AccountRepository::new(state.db.as_ref());
    let ts = unix_now();
    let mut count = 0usize;

    for order in rows {
        let remaining = order.quantity - order.filled;

        // Cancel in matching engine (best-effort).
        if let Some(engines) = &state.engines {
            if let Some(engine) = engines.get(&order.symbol) {
                let _ = { let mut eng = engine.lock().unwrap(); eng.cancel_order(order.id as u64) };
            }
        } else if let Some(aeron_cmd_tx) = &state.aeron_cmd_tx {
            let req = crate::sbe::CancelOrderRequest { order_id: order.id as u64 };
            let _ = aeron_cmd_tx.send(crate::transport::AeronCmd::Cancel(req));
        }

        // Update DB.
        let _ = sqlx::query(
            "UPDATE orders SET status='CANCELED', updated_at=NOW() WHERE id=$1",
        )
        .bind(order.id)
        .execute(state.db.as_ref())
        .await;

        // Release frozen funds for the unfilled portion.
        let sym_parts: Vec<&str> = order.symbol.splitn(2, '_').collect();
        let base_asset = sym_parts.first().copied().unwrap_or("BTC");
        let quote_asset = sym_parts.last().copied().unwrap_or("USDT");

        if order.side == "buy" {
            let fp = order.price.unwrap_or(0.0);
            if fp > 0.0 && remaining > 0.0 {
                let _ = repo.release_frozen(user_id, quote_asset, fp * remaining).await;
            }
        } else if remaining > 0.0 {
            let _ = repo.release_frozen(user_id, base_asset, remaining).await;
        }

        // Push order_update to the caller's personal channel.
        let _ = personal_tx.try_send(json!({
            "type": "order_update",
            "order_id": order.id,
            "status": "CANCELED",
            "filled_qty": order.filled,
            "ts": ts,
        }).to_string());

        count += 1;
    }

    count
}

/// Build a depth snapshot JSON for the given symbol from the matching engine.
fn build_depth_json(engine: &Mutex<MatchingEngine>, symbol: &str) -> String {
    let (bids, asks) = {
        let eng = engine.lock().unwrap();
        (eng.get_top_levels(10, true), eng.get_top_levels(10, false))
    };
    let bids: Vec<_> = bids.into_iter().filter(|(_, q)| *q > 0.0).collect();
    let asks: Vec<_> = asks.into_iter().filter(|(_, q)| *q > 0.0).collect();
    json!({
        "type": "depth",
        "symbol": symbol,
        "bids": bids,
        "asks": asks,
        "ts": unix_now()
    }).to_string()
}

/// Build and broadcast a depth snapshot for the given symbol.
pub fn broadcast_depth_pub(state: &AppState, symbol: &str) {
    if let Some(engines) = &state.engines {
        if let Some(engine) = engines.get(symbol) {
            let _ = state.market_tx.send(build_depth_json(engine.value(), symbol));
        }
    } else if let Some(depth_json) = state.last_depth.get(symbol) {
        let _ = state.market_tx.send(depth_json.to_string());
    }
}

/// Build and broadcast the current-minute k-line for `symbol` after a REST trade.
pub async fn broadcast_kline_pub(state: &AppState, symbol: &str) {
    let row = sqlx::query(
        "SELECT
           extract(epoch FROM date_trunc('minute', created_at))::bigint AS time,
           (array_agg(price ORDER BY created_at ASC))[1]  AS open,
           max(price)  AS high,
           min(price)  AS low,
           (array_agg(price ORDER BY created_at DESC))[1] AS close,
           sum(quantity) AS volume
         FROM trades
         WHERE symbol = $1
           AND created_at >= date_trunc('minute', NOW())
         GROUP BY date_trunc('minute', created_at)
         LIMIT 1",
    )
    .bind(symbol)
    .fetch_optional(state.db.as_ref())
    .await;

    if let Ok(Some(row)) = row {
        use sqlx::Row;
        let msg = serde_json::json!({
            "type": "kline",
            "symbol": symbol,
            "bar": {
                "time":   row.get::<i64, _>("time"),
                "open":   row.get::<f64, _>("open"),
                "high":   row.get::<f64, _>("high"),
                "low":    row.get::<f64, _>("low"),
                "close":  row.get::<f64, _>("close"),
                "volume": row.get::<f64, _>("volume"),
            }
        })
        .to_string();
        let _ = state.market_tx.send(msg);
    }
}

// ─── Background market data broadcaster ─────────────────────────────────────

/// Snapshot every engine's top-of-book under brief per-engine locks.
/// Returned as owned data so the caller can build/send messages without
/// holding any DashMap shard or engine lock.
fn snapshot_all_engines(
    engines: &DashMap<String, Arc<Mutex<MatchingEngine>>>,
) -> Vec<(String, Vec<(f64, f64)>, Vec<(f64, f64)>)> {
    engines
        .iter()
        .map(|entry| {
            let symbol = entry.key().clone();
            let eng = entry.value().lock().unwrap();
            let bids: Vec<_> = eng.get_top_levels(10, true).into_iter().filter(|(_, q)| *q > 0.0).collect();
            let asks: Vec<_> = eng.get_top_levels(10, false).into_iter().filter(|(_, q)| *q > 0.0).collect();
            (symbol, bids, asks)
        })
        .collect()
}

pub async fn market_data_broadcaster(state: AppState) {
    let mut depth_interval = tokio::time::interval(std::time::Duration::from_secs(2));
    depth_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut kline_interval = tokio::time::interval(std::time::Duration::from_secs(10));
    kline_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut ticker_interval = tokio::time::interval(std::time::Duration::from_secs(5));
    ticker_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut agg_interval = tokio::time::interval(std::time::Duration::from_secs(1));
    agg_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Last-seen top-bid price per symbol — drives ticker broadcasts and the
    // 24h-change calc. Keyed by symbol so a new symbol picked up at runtime
    // gets its own entry on first non-empty depth tick.
    let mut last_prices: HashMap<String, f64> = HashMap::new();

    loop {
        tokio::select! {
            _ = depth_interval.tick() => {
                // Only broadcast depth from local engines in standalone mode.
                // In Aeron mode, depth is pushed by exchange_engine via Aeron.
                if let Some(ref engines) = state.engines {
                    for (symbol, bids, asks) in snapshot_all_engines(engines) {
                        if let Some((p, _)) = bids.first() {
                            last_prices.insert(symbol.clone(), *p);
                        }
                        let msg = json!({
                            "type": "depth",
                            "symbol": symbol,
                            "bids": bids,
                            "asks": asks,
                            "ts": unix_now()
                        }).to_string();
                        let _ = state.market_tx.send(msg);
                    }
                } else {
                    // In Aeron mode, collect last prices from last_depth cache.
                    for entry in state.last_depth.iter() {
                        let sym = entry.key().clone();
                        let depth_val = entry.value().clone();
                        if let Some(bids) = depth_val.get("bids").and_then(|b| b.as_array()) {
                            if let Some(first_bid) = bids.first() {
                                if let Some(p) = first_bid.get(0).and_then(|v| v.as_f64()) {
                                    last_prices.insert(sym, p);
                                }
                            }
                        }
                    }
                }
            }

            _ = ticker_interval.tick() => {
                // Periodic ticker per symbol that has a known last price.
                for (symbol, &last_price) in last_prices.iter() {
                    if last_price <= 0.0 { continue; }
                    let symbol = symbol.clone();
                    let db = state.db.clone();
                    let mtx = state.market_tx.clone();
                    tokio::spawn(async move {
                        // Single query: 24h open + high + low + volume.
                        let row: Option<(Option<f64>, Option<f64>, Option<f64>, Option<f64>)> = sqlx::query_as(
                            "SELECT
                               (SELECT price FROM trades
                                WHERE symbol=$1 AND created_at > NOW() - INTERVAL '24 hours'
                                ORDER BY created_at ASC LIMIT 1) AS open_24h,
                               MAX(price)      AS high,
                               MIN(price)      AS low,
                               SUM(quantity)   AS volume
                             FROM trades
                             WHERE symbol=$1 AND created_at > NOW() - INTERVAL '24 hours'"
                        )
                        .bind(&symbol)
                        .fetch_optional(db.as_ref())
                        .await
                        .unwrap_or(None);
                        let (open_24h, high, low, volume) = row.unwrap_or_default();
                        let change = match open_24h {
                            Some(o) if o != 0.0 => (last_price - o) / o * 100.0,
                            _ => 0.0,
                        };
                        let msg = serde_json::json!({
                            "type": "ticker",
                            "symbol": symbol,
                            "last": last_price,
                            "change": change,
                            "high": high,
                            "low": low,
                            "volume": volume,
                        }).to_string();
                        let _ = mtx.send(msg);
                    });
                }
            }

            _ = kline_interval.tick() => {
                // Fetch real OHLCV from the trades table for the last 60 seconds,
                // per active symbol. Skip when the bar has no trades so we don't
                // corrupt the chart with a fake flat candle.
                let now = unix_secs();
                let bar_time = now - (now % 60);
                let symbols: Vec<String> = if let Some(ref engines) = state.engines {
                    engines.iter().map(|e| e.key().clone()).collect()
                } else {
                    last_prices.keys().cloned().collect()
                };
                for symbol in symbols {
                    let db = state.db.clone();
                    let mtx = state.market_tx.clone();
                    tokio::spawn(async move {
                        let row: Result<(Option<f64>, Option<f64>, Option<f64>, Option<f64>, f64), _> = sqlx::query_as(
                            "SELECT
                               (array_agg(price ORDER BY created_at ASC))[1]  AS open,
                               MAX(price)                                      AS high,
                               MIN(price)                                      AS low,
                               (array_agg(price ORDER BY created_at DESC))[1] AS close,
                               COALESCE(SUM(quantity), 0.0)                    AS volume
                             FROM trades
                             WHERE symbol = $1
                               AND created_at >= to_timestamp($2)
                               AND created_at <  to_timestamp($3)"
                        )
                        .bind(&symbol)
                        .bind(bar_time as f64)
                        .bind((bar_time + 60) as f64)
                        .fetch_one(db.as_ref())
                        .await;

                        match row {
                            Ok((Some(open), Some(high), Some(low), Some(close), volume)) if volume > 0.0 => {
                                let msg = json!({
                                    "type": "kline",
                                    "symbol": symbol,
                                    "bar": {
                                        "time": bar_time,
                                        "open": open,
                                        "high": high,
                                        "low":  low,
                                        "close": close,
                                        "volume": volume
                                    }
                                }).to_string();
                                let _ = mtx.send(msg);
                            }
                            Ok(_) => {
                                // No trades in the last 60s — skip sending kline.
                            }
                            Err(e) => {
                                eprintln!("kline DB query failed for {}: {}", symbol, e);
                            }
                        }
                    });
                }
            }

            _ = agg_interval.tick() => {
                // Per-second OHLCV+count for the active second and the active
                // 5s window. Only query symbols that have seen at least one
                // trade (last_prices entry) to keep DB load bounded.
                let now = unix_secs();
                let bucket_1s = now;
                let bucket_5s = now - (now % 5);
                let symbols: Vec<String> = last_prices.keys().cloned().collect();
                for symbol in symbols {
                    for &(interval_label, bin_w, bucket_start) in &[
                        ("1s", "1 second",  bucket_1s),
                        ("5s", "5 seconds", bucket_5s),
                    ] {
                        let db = state.db.clone();
                        let mtx = state.market_tx.clone();
                        let sym = symbol.clone();
                        let sql = format!(
                            "SELECT
                               (array_agg(price ORDER BY created_at ASC))[1]  AS open,
                               MAX(price)                                      AS high,
                               MIN(price)                                      AS low,
                               (array_agg(price ORDER BY created_at DESC))[1] AS close,
                               COALESCE(SUM(quantity), 0.0)                    AS volume,
                               COUNT(*)::bigint                                AS trade_count
                             FROM trades
                             WHERE symbol = $1
                               AND created_at >= date_bin('{bin}', to_timestamp($2), TIMESTAMPTZ '2000-01-01')
                               AND created_at <  date_bin('{bin}', to_timestamp($2), TIMESTAMPTZ '2000-01-01') + INTERVAL '{bin}'",
                            bin = bin_w,
                        );
                        tokio::spawn(async move {
                            let row: Result<(Option<f64>, Option<f64>, Option<f64>, Option<f64>, f64, i64), _> =
                                sqlx::query_as(&sql)
                                    .bind(&sym)
                                    .bind(bucket_start as f64)
                                    .fetch_one(db.as_ref())
                                    .await;
                            if let Ok((Some(open), Some(high), Some(low), Some(close), volume, trade_count)) = row {
                                if trade_count == 0 { return; }
                                let msg = json!({
                                    "type": "agg_trade",
                                    "symbol": sym,
                                    "interval": interval_label,
                                    "time": bucket_start,
                                    "open": open,
                                    "high": high,
                                    "low": low,
                                    "close": close,
                                    "volume": volume,
                                    "trade_count": trade_count,
                                }).to_string();
                                let _ = mtx.send(msg);
                            }
                        });
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{build_depth_json, snapshot_all_engines};
    use crate::{MatchingEngine, Order, PoolConfig, Side, TimeInForce};
    use dashmap::DashMap;
    use serde_json::Value;
    use std::sync::{Arc, Mutex};

    fn empty_engine() -> Mutex<MatchingEngine> {
        Mutex::new(MatchingEngine::new(PoolConfig::default()).unwrap())
    }

    fn engine_arc() -> Arc<Mutex<MatchingEngine>> {
        Arc::new(Mutex::new(MatchingEngine::new(PoolConfig::default()).unwrap()))
    }

    #[test]
    fn build_depth_json_empty_book_has_empty_sides() {
        let engine = empty_engine();
        let v: Value = serde_json::from_str(&build_depth_json(&engine, "BTC_USDT")).unwrap();

        assert_eq!(v["type"], "depth");
        assert_eq!(v["symbol"], "BTC_USDT");
        assert!(v["bids"].as_array().unwrap().is_empty());
        assert!(v["asks"].as_array().unwrap().is_empty());
        assert!(v["ts"].as_u64().unwrap() > 0);
    }

    #[test]
    fn build_depth_json_includes_resting_orders() {
        let engine = empty_engine();
        {
            let mut eng = engine.lock().unwrap();
            // Resting bid at 100, resting ask at 101 — won't cross.
            eng.place_order(Order::new(1, Side::Buy, 100.0, 2.0, TimeInForce::GTC, 0)).unwrap();
            eng.place_order(Order::new(2, Side::Sell, 101.0, 3.0, TimeInForce::GTC, 0)).unwrap();
        }

        let v: Value = serde_json::from_str(&build_depth_json(&engine, "BTC_USDT")).unwrap();
        let bids = v["bids"].as_array().unwrap();
        let asks = v["asks"].as_array().unwrap();

        assert_eq!(bids.len(), 1);
        assert_eq!(asks.len(), 1);
        // Each level is serialized as a [price, qty] tuple.
        assert_eq!(bids[0][0].as_f64().unwrap(), 100.0);
        assert_eq!(bids[0][1].as_f64().unwrap(), 2.0);
        assert_eq!(asks[0][0].as_f64().unwrap(), 101.0);
        assert_eq!(asks[0][1].as_f64().unwrap(), 3.0);
    }

    #[test]
    fn build_depth_json_propagates_symbol_argument() {
        let engine = empty_engine();
        let v: Value = serde_json::from_str(&build_depth_json(&engine, "ETH_USDT")).unwrap();
        assert_eq!(v["symbol"], "ETH_USDT");
    }
}
