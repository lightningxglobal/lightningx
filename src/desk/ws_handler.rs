use crate::account_repository::AccountRepository;
use crate::api::AppState;
use crate::engine::{MatchingEngine, OrderStatus};
use crate::order::{Order, Side, TimeInForce};
use crate::order_state::{
    db_status_from_engine, maker_ws_status_from_db_status, ws_status_from_engine,
};
use crate::user_service;
use axum::extract::{Request, State};
use axum::response::Response;
use dashmap::DashMap;
use fastwebsockets::{upgrade, Frame, OpCode, Payload, WebSocket};
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

fn try_freeze_cache(cache: &crate::api::AccountCache, user_id: i64, asset: &str, amount: f64) -> bool {
    if amount <= 0.0 { return true; }
    let Some(mut entry) = cache.get_mut(&user_id) else { return false };
    let Some(kv) = entry.get_mut(asset) else { return false };
    if kv.0 - kv.1 >= amount { kv.1 += amount; true } else { false }
}

fn release_cache_frozen(cache: &crate::api::AccountCache, user_id: i64, asset: &str, amount: f64) {
    if amount <= 0.0 { return; }
    if let Some(mut entry) = cache.get_mut(&user_id) {
        if let Some(kv) = entry.get_mut(asset) {
            kv.1 = (kv.1 - amount).max(0.0);
        }
    }
}

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
    Auth {
        token: String,
    },
    ApiKeyAuth {
        api_key: String,
    },
    Subscribe {
        channels: Vec<String>,
    },
    Unsubscribe {
        channels: Vec<String>,
    },
    PlaceOrder {
        client_order_id: String,
        symbol: String,
        side: String,
        order_type: String,
        time_in_force: Option<String>,
        price: Option<f64>,
        qty: f64,
    },
    PlaceOrders {
        batch_id: String,
        orders: Vec<serde_json::Value>,
    },
    CancelOrder {
        order_id: i64,
    },
    BatchCancel {
        order_ids: Vec<i64>,
    },
    CancelSymbol {
        symbol: String,
    },
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
            "auth_key" => Some(ClientMsg::ApiKeyAuth {
                api_key: v.get("api_key")?.as_str()?.to_owned(),
            }),
            "subscribe" => {
                let channels = v
                    .get("channels")?
                    .as_array()?
                    .iter()
                    .filter_map(|c| c.as_str().map(|s| s.to_owned()))
                    .collect();
                Some(ClientMsg::Subscribe { channels })
            }
            "unsubscribe" => {
                let channels = v
                    .get("channels")?
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
                order_type: v
                    .get("order_type")
                    .and_then(|x| x.as_str())
                    .unwrap_or("limit")
                    .to_owned(),
                // Optional separate time_in_force field (e.g. "GTC", "IOC", "FOK").
                // Takes effect only when order_type is "limit" or "gtc".
                time_in_force: v
                    .get("time_in_force")
                    .and_then(|x| x.as_str())
                    .map(|s| s.to_ascii_lowercase()),
                price: v.get("price").and_then(|p| p.as_f64()),
                qty: v.get("qty").or_else(|| v.get("quantity"))?.as_f64()?,
            }),
            "place_orders" => {
                let batch_id = v.get("batch_id").and_then(|b| b.as_str()).unwrap_or("").to_owned();
                let orders = v.get("orders").and_then(|a| a.as_array()).cloned().unwrap_or_default();
                Some(ClientMsg::PlaceOrders { batch_id, orders })
            }
            "cancel_order" => Some(ClientMsg::CancelOrder {
                order_id: v.get("order_id")?.as_i64()?,
            }),
            "batch_cancel" => {
                let ids = v
                    .get("order_ids")?
                    .as_array()?
                    .iter()
                    .filter_map(|x| x.as_i64())
                    .collect();
                Some(ClientMsg::BatchCancel { order_ids: ids })
            }
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

pub async fn ws_handler(State(state): State<AppState>, mut req: Request) -> Response {
    let (response, fut) = upgrade::upgrade(&mut req).expect("WS upgrade failed");
    tokio::spawn(async move {
        match fut.await {
            Ok(ws) => handle_socket(ws, state).await,
            Err(e) => tracing::error!("WS upgrade error: {e}"),
        }
    });
    response.map(|_| axum::body::Body::empty())
}

// ─── Socket loop ──────────────────────────────────────────────────────────────

async fn handle_socket(mut socket: WebSocket<TokioIo<Upgraded>>, state: AppState) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static CONN_ID_GEN: AtomicU64 = AtomicU64::new(1);
    let conn_id = CONN_ID_GEN.fetch_add(1, Ordering::Relaxed);

    // writev=true → single writev() syscall combines WS header + payload
    // instead of two write() calls per frame. On loopback this halves the
    // syscall count per outgoing frame and shaves ~50-100µs off RTT.
    socket.set_writev(true);
    socket.set_auto_close(true);
    socket.set_auto_pong(true);
    let mut session = WsSession::new();

    // Personal update channel for this connection (order/balance events).
    let (personal_tx, mut personal_rx) = mpsc::channel::<String>(65536);
    // Market data channel: registered LAZILY with state.market_fanout when
    // the client first sends a Subscribe message. Connections that never
    // subscribe (load tests, trading-only bots) never get registered, so
    // they impose zero fanout overhead. Eliminates the broadcast::Receiver
    // lock contention that pushed p50 to 2s at 40K conns.
    let (market_tx, mut market_rx) = mpsc::channel::<Arc<str>>(8192);
    let mut market_registered = false;

    'conn: loop {
        tokio::select! {
            // Incoming message from client
            frame = socket.read_frame() => {
                match frame {
                    Ok(frame) => match frame.opcode {
                        OpCode::Text => {
                            let text = match std::str::from_utf8(&frame.payload) {
                                Ok(t) => t,
                                Err(_) => continue 'conn,
                            };
                            // Lazy register with market_fanout on first
                            // Subscribe-like message — quick substring check
                            // avoids a full JSON parse just to detect intent.
                            if !market_registered && text.contains("\"subscribe\"") {
                                state.market_fanout.register(conn_id, market_tx.clone());
                                market_registered = true;
                            }
                            if let Some(reply) = handle_client_message(
                                text, &mut session, &state, personal_tx.clone()
                            ).await {
                                if socket.write_frame(Frame::text(Payload::Owned(reply.into_bytes()))).await.is_err() {
                                    break 'conn;
                                }
                            }
                        }
                        OpCode::Close => break 'conn,
                        _ => {}
                    }
                    Err(_) => break 'conn,
                }
            }

            // Personal order/balance update for this user
            Some(msg) = personal_rx.recv() => {
                if socket.write_frame(Frame::text(Payload::Owned(msg.into_bytes()))).await.is_err() {
                    break 'conn;
                }
            }

            // Market data (depth, trades, ticker, kline) — only flows
            // here for subscribers.
            Some(msg) = market_rx.recv() => {
                // Only forward if this client subscribed to a matching channel.
                // Quick prefix check on the JSON "type" field to avoid
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
                    if socket.write_frame(Frame::text(Payload::Owned(msg.as_bytes().to_vec()))).await.is_err() {
                        break 'conn;
                    }
                }
            }
        }
    }
    if market_registered {
        state.market_fanout.unregister(conn_id);
    }

    // Remove personal channel on disconnect.
    if let Some(uid) = session.user_id {
        state.user_tx.unregister(uid);
    }
}

/// Extract the `symbol` field from a JSON string cheaply (no full parse).
fn msg_symbol(msg: &str) -> Option<&str> {
    let key = "\"symbol\":\"";
    let start = msg.find(key)? + key.len();
    let end = msg[start..].find('"')? + start;
    Some(&msg[start..end])
}

fn best_opposing_from_depth(state: &AppState, symbol: &str, side: &str) -> Option<f64> {
    let depth = state.last_depth.get(symbol)?;
    let levels_key = if side == "buy" { "asks" } else { "bids" };
    depth.get(levels_key)?.as_array()?.first()?.get(0)?.as_f64()
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
        None => {
            return Some(json!({"type": "error", "message": "Invalid message format"}).to_string())
        }
    };

    match msg {
        ClientMsg::Auth { token } => match user_service::verify_token(&token) {
            Ok(claims) => {
                session.user_id = Some(claims.sub);
                state.user_tx.register(claims.sub, personal_tx);
                Some(json!({"type": "auth_ok"}).to_string())
            }
            Err(e) => Some(json!({"type": "auth_error", "message": e.to_string()}).to_string()),
        },

        ClientMsg::ApiKeyAuth { api_key } => {
            match user_service::verify_api_key(&state.db, &api_key).await {
                Ok(user_id) => {
                    session.user_id = Some(user_id);
                    state.user_tx.register(user_id, personal_tx);
                    Some(json!({"type": "auth_ok", "user_id": user_id}).to_string())
                }
                Err(e) => Some(json!({"type": "auth_error", "message": e.to_string()}).to_string()),
            }
        }

        ClientMsg::Subscribe { channels } => {
            // Collect depth + ticker symbols before moving `channels` into the
            // subscribed set, so we can push an immediate snapshot for each.
            let depth_symbols: Vec<String> = channels
                .iter()
                .filter_map(|c| c.strip_prefix("depth.").map(str::to_string))
                .collect();
            let ticker_symbols: Vec<String> = channels
                .iter()
                .filter_map(|c| c.strip_prefix("ticker.").map(str::to_string))
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
            for sym in ticker_symbols {
                if let Some(payload) = state.last_ticker.get(&sym) {
                    let _ = personal_tx.try_send(payload.clone());
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

        ClientMsg::PlaceOrder {
            client_order_id,
            symbol,
            side,
            order_type,
            time_in_force,
            price,
            qty,
        } => {
            let user_id = match session.user_id {
                Some(id) => id,
                None => return Some(json!({"type": "order_rejected", "client_order_id": client_order_id, "reason": "Not authenticated"}).to_string()),
            };

            // Client is responsible for ensuring client_order_id uniqueness
            // — no server-side idempotency check. The DB UNIQUE constraint
            // on (user_id, client_order_id) is the only safety net; if a
            // duplicate slips through, the pg-writer INSERT fails and the
            // duplicate is logged. Removing this check eliminates the
            // Redis HGET (~50µs) + PG SELECT fallback (~ms at saturation)
            // on the WS place_order hot path, which was the dominant
            // bottleneck at 400K conns (609 sqlx pool slow-acquire warnings,
            // place avg 6 s).

            let _fixed_shape = match crate::symbol_rules::normalize_order_shape(
                &symbol,
                &order_type,
                price,
                qty,
            ) {
                Ok(shape) => shape,
                Err(reason) => {
                    return Some(
                        json!({
                            "type": "order_rejected",
                            "client_order_id": client_order_id,
                            "reason": reason
                        })
                        .to_string(),
                    )
                }
            };

            let engine_side = match side.as_str() {
                "buy" => Side::Buy,
                "sell" => Side::Sell,
                _ => return Some(json!({"type": "order_rejected", "client_order_id": client_order_id, "reason": "Invalid side"}).to_string()),
            };

            let tif = match order_type.as_str() {
                "ioc" => TimeInForce::IOC,
                "fok" => TimeInForce::FOK,
                "post_only" => TimeInForce::PostOnly,
                "market" => TimeInForce::IOC,
                "limit" | "gtc" => {
                    // Honour separate time_in_force field when order_type is generic "limit".
                    match time_in_force.as_deref() {
                        Some("ioc") => TimeInForce::IOC,
                        Some("fok") => TimeInForce::FOK,
                        Some("post_only") => TimeInForce::PostOnly,
                        _ => TimeInForce::GTC,
                    }
                }
                _ => return Some(json!({"type": "order_rejected", "client_order_id": client_order_id, "reason": "Unknown order type"}).to_string()),
            };

            // Look up the matching engine for this symbol; only available in standalone mode.
            let engine_opt: Option<Arc<Mutex<MatchingEngine>>> = state
                .engines
                .as_ref()
                .and_then(|m| m.get(&symbol).map(|e| e.value().clone()));
            // In standalone mode, reject unknown symbols up front.
            if state.engines.is_some() && engine_opt.is_none() {
                return Some(
                    json!({
                        "type": "order_rejected",
                        "client_order_id": client_order_id,
                        "reason": format!("Unknown symbol: {}", symbol)
                    })
                    .to_string(),
                );
            }

            let now_ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0);

            // Parse base/quote assets from symbol (e.g. "BTC_USDT" → "BTC", "USDT").
            let sym_parts: Vec<&str> = symbol.splitn(2, '_').collect();
            let base_asset = sym_parts.first().copied().unwrap_or("BTC");
            let quote_asset = sym_parts.last().copied().unwrap_or("USDT");
            let rules = crate::symbol_rules::SymbolRules::for_symbol(&symbol);

            // Capture best opposing price before matching — used as the fill
            // price for market orders AND persisted as freeze_price so the
            // restart cleanup can release the exact amount later.
            let best_opposing_price: Option<f64> = if let Some(ref engine) = engine_opt {
                let eng = engine.lock().unwrap();
                let levels = eng.get_top_levels(1, engine_side == Side::Sell);
                levels.first().map(|(p, _)| rules.ticks_to_price(*p))
            } else if state.aeron_cmd_tx.is_some() {
                best_opposing_from_depth(state, &symbol, &side)
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

                // PERF DIAG (off by default): sample ~1/256 PlaceOrders per-section timing.
                // Enable via WS_PLACE_PROFILE=1 env on desk-server.
                let dbg = std::env::var("WS_PLACE_PROFILE").map(|v| v == "1").unwrap_or(false);
                let t0 = if dbg { Some(std::time::Instant::now()) } else { None };
                // Skip the per-place balance_update push by default — it adds
                // a second WS frame per place to the client, which would
                // double the queued-frame drain cost on the client's NEXT
                // place read. Keep gated for backwards-compat clients that
                // expect the snapshot.
                let push_bal = std::env::var("WS_PUSH_BAL").map(|v| v == "1").unwrap_or(false);

                // Reject unknown symbols immediately — before sending order_submitted —
                // so clients don't track phantom orders waiting for a response that
                // arrives as order_rejected after the pending entry is gone.
                if !state.valid_symbols.is_empty() && !state.valid_symbols.contains(&symbol) {
                    return Some(
                        json!({
                            "type": "order_rejected",
                            "client_order_id": client_order_id,
                            "reason": format!("No engine for symbol: {}", symbol)
                        })
                        .to_string(),
                    );
                }

                let (freeze_asset, freeze_amount): (&str, f64) = if side == "buy" {
                    let amount = freeze_price_val * qty;
                    if amount <= 0.0 {
                        return Some(
                            json!({
                                "type": "order_rejected",
                                "client_order_id": client_order_id,
                                "reason": "Unable to determine buy reservation price"
                            })
                            .to_string(),
                        );
                    }
                    (quote_asset, amount)
                } else {
                    (base_asset, qty)
                };
                let t_pre_freeze = t0.map(|_| std::time::Instant::now());
                if !try_freeze_cache(&state.account_cache, user_id, freeze_asset, freeze_amount) {
                    return Some(
                        json!({
                            "type": "order_rejected",
                            "client_order_id": client_order_id,
                            "reason": "Insufficient balance"
                        })
                        .to_string(),
                    );
                }
                let t_post_freeze = t0.map(|_| std::time::Instant::now());
                if push_bal {
                    if let Some(user_assets) = state.account_cache.get(&user_id) {
                        if let Some(&(bal, frz)) = user_assets.get(freeze_asset) {
                            if let Some(tx) = state.user_tx.get(user_id) {
                                let _ = tx.try_send(format!(
                                    r#"{{"type":"balance_update","asset":"{}","balance":{},"available":{},"frozen":{}}}"#,
                                    freeze_asset, bal, bal - frz, frz,
                                ));
                            }
                        }
                    }
                }
                let t_post_balupd = t0.map(|_| std::time::Instant::now());

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
                    TimeInForce::GTC => 0,
                    TimeInForce::IOC => 1,
                    TimeInForce::FOK => 2,
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

                state.pending_meta.insert(
                    order_id,
                    Box::new(OrderMeta {
                        user_id,
                        symbol: symbol.clone(),
                        side: side.clone(),
                        order_type: order_type.clone(),
                        price,
                        qty,
                        client_order_id: client_order_id.clone(),
                        freeze_price: freeze_price_val,
                    }),
                );

                if aeron_cmd_tx.send(AeronCmd::NewOrder(sbe_req)).is_err() {
                    state.pending_meta.remove(&order_id);
                    release_cache_frozen(&state.account_cache, user_id, freeze_asset, freeze_amount);
                    return Some(
                        json!({
                            "type": "order_rejected",
                            "client_order_id": client_order_id,
                            "reason": "Aeron channel closed"
                        })
                        .to_string(),
                    );
                }
                let t_post_aeron = t0.map(|_| std::time::Instant::now());

                let reply = json!({
                    "type": "order_submitted",
                    "client_order_id": client_order_id,
                    "order_id": order_id,
                    "symbol": symbol,
                    "side": side,
                    "order_type": order_type,
                    "price": price,
                    "quantity": qty,
                    "ts": unix_now()
                })
                .to_string();
                if let (Some(start), Some(pre_freeze), Some(post_freeze), Some(post_bal), Some(post_aeron)) =
                    (t0, t_pre_freeze, t_post_freeze, t_post_balupd, t_post_aeron)
                {
                    let t_done = std::time::Instant::now();
                    use std::sync::atomic::{AtomicU64, Ordering};
                    static SAMPLE_CTR: AtomicU64 = AtomicU64::new(0);
                    let n = SAMPLE_CTR.fetch_add(1, Ordering::Relaxed);
                    if n % 256 == 0 {
                        tracing::info!(
                            "place_profile #{}  pre_freeze={}µs  freeze={}µs  balance_push={}µs  aeron_send={}µs  reply_build={}µs  total={}µs",
                            n,
                            pre_freeze.duration_since(start).as_micros(),
                            post_freeze.duration_since(pre_freeze).as_micros(),
                            post_bal.duration_since(post_freeze).as_micros(),
                            post_aeron.duration_since(post_bal).as_micros(),
                            t_done.duration_since(post_aeron).as_micros(),
                            t_done.duration_since(start).as_micros(),
                        );
                    }
                }
                return Some(reply);
            }

            // ── Standalone engine path: DB + freeze + local matching ─────────────
            let engine = match engine_opt {
                Some(e) => e,
                None => {
                    return Some(
                        json!({
                            "type": "order_rejected",
                            "client_order_id": client_order_id,
                            "reason": "No matching engine configured"
                        })
                        .to_string(),
                    )
                }
            };

            // Persist to DB as PENDING before running through engine.
            // Use the shared AtomicU64 counter (same as REST path) to avoid
            // depending on a PostgreSQL sequence that doesn't exist for BIGINT PKs.
            let db_order_id = state.next_order_id.fetch_add(1, Ordering::Relaxed) as i64;
            let db_result = sqlx::query_as::<_, crate::models::DbOrder>(
                "INSERT INTO orders (id, user_id, symbol, side, order_type, price, quantity, filled, status, freeze_price)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, 0, 'PENDING', $8) RETURNING *",
            )
            .bind(db_order_id)
            .bind(user_id)
            .bind(&symbol)
            .bind(&side)
            .bind(&order_type)
            .bind(price)
            .bind(qty)
            .bind(freeze_price_val)
            .fetch_one(state.db.as_ref())
            .await;

            if let Err(e) = db_result {
                return Some(
                    json!({
                        "type": "order_rejected",
                        "client_order_id": client_order_id,
                        "reason": format!("DB error: {}", e)
                    })
                    .to_string(),
                );
            }
            let fixed_order = match crate::symbol_rules::FixedOrderInput::from_order_fields(
                db_order_id as u64,
                &symbol,
                engine_side,
                &order_type,
                price,
                qty,
                tif,
                now_ns,
            ) {
                Ok(order) => order,
                Err(reason) => {
                    return Some(
                        json!({
                            "type": "order_rejected",
                            "client_order_id": client_order_id,
                            "reason": reason
                        })
                        .to_string(),
                    )
                }
            };
            let engine_order = if fixed_order.is_market {
                Order::new_market(
                    fixed_order.id,
                    fixed_order.side,
                    fixed_order.quantity_lots,
                    fixed_order.timestamp,
                )
            } else {
                Order::new(
                    fixed_order.id,
                    fixed_order.side,
                    fixed_order.price_ticks,
                    fixed_order.quantity_lots,
                    fixed_order.time_in_force,
                    fixed_order.timestamp,
                )
            };

            // Freeze funds before sending to engine.
            let repo = AccountRepository::new(state.db.as_ref());
            let freeze_result = if side == "buy" {
                let freeze_amount = freeze_price_val * qty;
                if freeze_amount > 0.0 {
                    repo.freeze_for_buy(user_id, quote_asset, freeze_amount)
                        .await
                } else {
                    Ok((0.0, 0.0))
                }
            } else {
                repo.freeze_for_sell(user_id, base_asset, qty).await
            };
            {
                let frozen_asset = if side == "buy" {
                    quote_asset
                } else {
                    base_asset
                };
                match freeze_result {
                    Err(e) => {
                        let _ = sqlx::query(
                            "DELETE FROM orders WHERE id=$1",
                        )
                        .bind(db_order_id)
                        .execute(state.db.as_ref())
                        .await;
                        return Some(
                            json!({
                                "type": "order_rejected",
                                "client_order_id": client_order_id,
                                "reason": e.to_string()
                            })
                            .to_string(),
                        );
                    }
                    Ok((bal, frz)) if bal > 0.0 || frz >= 0.0 => {
                        // Update cache and push WS balance_update from RETURNING values.
                        state
                            .account_cache
                            .entry(user_id)
                            .or_insert_with(std::collections::HashMap::new)
                            .insert(frozen_asset.to_string(), (bal, frz));
                        if let Some(tx) = state.user_tx.get(user_id) {
                            let _ = tx.try_send(
                                json!({
                                    "type": "balance_update",
                                    "asset": frozen_asset,
                                    "balance": bal,
                                    "available": bal - frz,
                                    "frozen": frz,
                                })
                                .to_string(),
                            );
                        }
                    }
                    Ok(_) => {} // no-op freeze (zero amount)
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
                            "DELETE FROM orders WHERE id = $1",
                        )
                        .bind(db_order_id)
                        .execute(state.db.as_ref())
                        .await;
                        if side == "buy" {
                            let p = price.or(best_opposing_price).unwrap_or(0.0);
                            if p > 0.0 {
                                let _ = repo.release_frozen(user_id, quote_asset, p * qty).await;
                            }
                        } else {
                            let _ = repo.release_frozen(user_id, base_asset, qty).await;
                        }
                        return Some(
                            json!({
                                "type": "order_rejected",
                                "client_order_id": client_order_id,
                                "reason": e.to_string()
                            })
                            .to_string(),
                        );
                    }
                }
            };

            // Map engine status to (DB, WS) status strings. IOC/FOK that
            // partially fill come back as Cancelled with filled>0 — the
            // meaningful event for the trader is the partial fill, not the
            // trailing cancel of the remainder, so surface PARTIAL_FILL.
            let db_status = db_status_from_engine(result.status).as_str();
            let filled_qty = rules.lots_to_quantity(result.filled_lots);
            let ws_status = ws_status_from_engine(result.status, filled_qty > 0.0).as_str();

            // Update DB with fill info.
            let _ = sqlx::query(
                "UPDATE orders SET status = $1, filled = $2, updated_at = NOW() WHERE id = $3",
            )
            .bind(db_status)
            .bind(filled_qty)
            .bind(db_order_id)
            .execute(state.db.as_ref())
            .await;

            if result.status == OrderStatus::Rejected {
                // Release frozen funds — no fills occurred.
                if side == "buy" {
                    let p = price.or(best_opposing_price).unwrap_or(0.0);
                    if p > 0.0 {
                        let _ = repo.release_frozen(user_id, quote_asset, p * qty).await;
                    }
                } else {
                    let _ = repo.release_frozen(user_id, base_asset, qty).await;
                }
                return Some(
                    json!({
                        "type": "order_rejected",
                        "client_order_id": client_order_id,
                        "reason": "Rejected by matching engine"
                    })
                    .to_string(),
                );
            }

            let ts = unix_now();

            // Settle fills and broadcast trade events.
            if filled_qty > 0.0 {
                let total_filled = filled_qty;
                // Weighted average price across all fills for market-event broadcasting.
                let avg_fill_price = if !result.fills.is_empty() {
                    let cost: f64 = result
                        .fills
                        .iter()
                        .map(|&(_, p, q)| rules.ticks_to_price(p) * rules.lots_to_quantity(q))
                        .sum();
                    cost / total_filled
                } else {
                    price.or(best_opposing_price).unwrap_or(0.0)
                };
                let fill_price = avg_fill_price;

                // Per-fill: settle both taker and maker atomically, record trade, update maker order.
                for &(maker_order_id, fp_ticks, fq_lots) in &result.fills {
                    if fp_ticks <= 0 || fq_lots <= 0 {
                        continue;
                    }
                    let fp = rules.ticks_to_price(fp_ticks);
                    let fq = rules.lots_to_quantity(fq_lots);

                    let maker_uid: Option<i64> =
                        sqlx::query_scalar("SELECT user_id FROM orders WHERE id = $1")
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
                            if over > 0.0 {
                                let _ = repo.release_frozen(user_id, quote_asset, over).await;
                            }
                        }
                        let _ = repo
                            .settle_trade(
                                buyer_id,
                                seller_id,
                                base_asset,
                                quote_asset,
                                fp,
                                fq,
                                0.0,
                                0.0,
                            )
                            .await;

                        // Notify maker of balance change and position change.
                        if let Some(maker_id) = maker_uid {
                            let (m_debit, m_credit) = if side == "buy" {
                                (base_asset, quote_asset)
                            } else {
                                (quote_asset, base_asset)
                            };
                            for asset in [m_debit, m_credit] {
                                if let Ok(acc) = repo.get_account(maker_id, asset).await {
                                    let msg = json!({"type":"balance_update","asset":asset,"balance":acc.balance,"available":acc.balance-acc.frozen,"frozen":acc.frozen}).to_string();
                                    if let Some(tx) = state.user_tx.get(maker_id) {
                                        let _ = tx.try_send(msg);
                                    }
                                }
                            }
                            if let Some(pos_msg) = crate::positions::position_update_msg(
                                state.db.as_ref(),
                                maker_id,
                                base_asset,
                            )
                            .await
                            {
                                if let Some(tx) = state.user_tx.get(maker_id) {
                                    let _ = tx.try_send(pos_msg);
                                }
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
                    if let (Some(maker_id), Some((new_status, new_filled))) = (maker_uid, maker_row)
                    {
                        let ws_maker_status =
                            maker_ws_status_from_db_status(new_status.as_str()).as_str();
                        let upd = json!({
                            "type": "order_update",
                            "order_id": maker_order_id as i64,
                            "status": ws_maker_status,
                            "filled_qty": new_filled,
                            "fill_delta": fq,
                            "avg_price": fp,
                            "ts": ts
                        })
                        .to_string();
                        if let Some(tx) = state.user_tx.get(maker_id) {
                            let _ = tx.try_send(upd);
                        }
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
                if result.status == OrderStatus::Cancelled || result.status == OrderStatus::Rejected
                {
                    let unfilled = qty - total_filled;
                    if unfilled > 0.0 {
                        let (rel_asset, rel_amount) = if side == "buy" {
                            (
                                quote_asset,
                                price.or(best_opposing_price).unwrap_or(fill_price) * unfilled,
                            )
                        } else {
                            (base_asset, unfilled)
                        };
                        if let Ok((bal, frz)) =
                            repo.release_frozen(user_id, rel_asset, rel_amount).await
                        {
                            if bal > 0.0 || frz >= 0.0 {
                                state
                                    .account_cache
                                    .entry(user_id)
                                    .or_insert_with(std::collections::HashMap::new)
                                    .insert(rel_asset.to_string(), (bal, frz));
                            }
                        }
                    }
                }

                let trade_msg = json!({
                    "type": "trade",
                    "symbol": symbol,
                    "price": fill_price,
                    "qty": total_filled,
                    "side": side,
                    "ts": ts
                })
                .to_string();
                let _ = state.market_fanout.send_owned(trade_msg);

                // Ticker updates are handled by the periodic market_data_broadcaster;
                // spawning a DB query here on every fill piles up and exhausts the pool.

                broadcast_depth_pub(state, &symbol);

                // Push order_update + balance_update to this user's personal channel.
                let update_msg = json!({
                    "type": "order_update",
                    "order_id": db_order_id,
                    "status": ws_status,
                    "filled_qty": total_filled,
                    "avg_price": fill_price,
                    "ts": ts
                })
                .to_string();
                if let Some(tx) = state.user_tx.get(user_id) {
                    let _ = tx.try_send(update_msg);
                }

                // Send balance_update so frontend can refresh balances.
                let (debit_asset, credit_asset) = if side == "buy" {
                    (quote_asset, base_asset)
                } else {
                    (base_asset, quote_asset)
                };
                for asset in [debit_asset, credit_asset] {
                    if let Ok(acc) = repo.get_account(user_id, asset).await {
                        let bal_msg = json!({
                            "type": "balance_update",
                            "asset": asset,
                            "balance": acc.balance,
                            "available": acc.balance - acc.frozen,
                            "frozen": acc.frozen
                        })
                        .to_string();
                        if let Some(tx) = state.user_tx.get(user_id) {
                            let _ = tx.try_send(bal_msg);
                        }
                    }
                }

                // Position update for the base asset whose holdings just changed.
                if let Some(pos_msg) =
                    crate::positions::position_update_msg(state.db.as_ref(), user_id, base_asset)
                        .await
                {
                    if let Some(tx) = state.user_tx.get(user_id) {
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
                })
                .to_string();
                if let Some(tx) = state.user_tx.get(user_id) {
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
                })
                .to_string();
                if let Some(tx) = state.user_tx.get(user_id) {
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
                        })
                        .to_string();
                        if let Some(tx) = state.user_tx.get(user_id) {
                            let _ = tx.try_send(bal_msg);
                        }
                    }
                }
            }

            Some(
                json!({
                    "type": "order_accepted",
                    "client_order_id": client_order_id,
                    "order_id": db_order_id,
                    "symbol": symbol,
                    "side": side,
                    "order_type": order_type,
                    "price": price,
                    "quantity": qty,
                    "ts": ts
                })
                .to_string(),
            )
        }

        ClientMsg::CancelSymbol { symbol } => {
            let user_id = match session.user_id {
                Some(id) => id,
                None => {
                    return Some(
                        json!({"type": "error", "message": "Not authenticated"}).to_string(),
                    )
                }
            };
            // Run in background so the WS handler is not blocked on DB operations.
            // This lets queued place_order messages be processed immediately.
            let state2 = state.clone();
            let sym2 = symbol.clone();
            let ptx2 = personal_tx.clone();
            tokio::spawn(async move {
                bulk_cancel(user_id, Some(&sym2), &state2, &ptx2).await;
            });
            Some(json!({"type": "cancel_all_ok", "symbol": symbol, "cancelled": 0}).to_string())
        }

        ClientMsg::CancelAll => {
            let user_id = match session.user_id {
                Some(id) => id,
                None => {
                    return Some(
                        json!({"type": "error", "message": "Not authenticated"}).to_string(),
                    )
                }
            };
            let n = bulk_cancel(user_id, None, state, &personal_tx).await;
            Some(json!({"type": "cancel_all_ok", "cancelled": n}).to_string())
        }

        ClientMsg::CancelOrder { order_id } => {
            let user_id = match session.user_id {
                Some(id) => id,
                None => {
                    return Some(
                        json!({"type": "error", "message": "Not authenticated"}).to_string(),
                    )
                }
            };

            // ── Aeron fast path: forward cancel immediately, no DB on the hot
            // path. Fund release + PG DELETE happen when engine's CANCELED
            // event lands in the spin thread → BatchCancelConfirmed (already
            // covered there). The old "SELECT then send" sequence added a
            // ~1ms PG round-trip per single-order cancel for nothing — the
            // engine doesn't need the metadata to cancel.
            if state.engines.is_none() {
                if let Some(aeron_cmd_tx) = &state.aeron_cmd_tx {
                    let cancel_req = crate::sbe::CancelOrderRequest {
                        order_id: order_id as u64,
                        participant_id: user_id as u64,
                    };
                    if aeron_cmd_tx
                        .send(crate::transport::AeronCmd::Cancel(cancel_req))
                        .is_err()
                    {
                        return Some(
                            json!({"type": "error", "message": "Aeron channel closed"}).to_string(),
                        );
                    }
                    return Some(
                        json!({
                            "type": "cancel_submitted",
                            "order_id": order_id,
                            "ts": unix_now()
                        })
                        .to_string(),
                    );
                }
            }

            // ── Standalone path: still needs the SELECT + freeze release
            // because there's no spin thread to do it. Kept inline below.
            let order_row = sqlx::query(
                "SELECT symbol, side, price, quantity, filled FROM orders
                 WHERE id = $1 AND user_id = $2 AND status IN ('PENDING','TRADING')",
            )
            .bind(order_id)
            .bind(user_id)
            .fetch_optional(state.db.as_ref())
            .await;

            let order_row =
                match order_row {
                    Ok(Some(r)) => r,
                    Ok(None) => return Some(
                        json!({"type": "error", "message": "Order not found or already closed"})
                            .to_string(),
                    ),
                    Err(e) => {
                        return Some(json!({"type": "error", "message": e.to_string()}).to_string())
                    }
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
                "DELETE FROM orders WHERE id = $1",
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
                    let _ = {
                        let mut eng = engine.lock().unwrap();
                        eng.cancel_order(order_id as u64)
                    };
                }
            } else if let Some(aeron_cmd_tx) = &state.aeron_cmd_tx {
                let cancel_req = crate::sbe::CancelOrderRequest {
                    order_id: order_id as u64,
                    participant_id: user_id as u64,
                };
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
                    let _ = repo
                        .release_frozen(user_id, quote_asset, freeze_price * remaining)
                        .await;
                }
                quote_asset
            } else {
                if remaining > 0.0 {
                    let _ = repo.release_frozen(user_id, base_asset, remaining).await;
                }
                base_asset
            };

            // Push balance_update so the frontend OrderForm reflects freed-up funds.
            if let Some(tx) = state.user_tx.get(user_id) {
                for asset in [
                    released_asset,
                    if released_asset == base_asset {
                        quote_asset
                    } else {
                        base_asset
                    },
                ] {
                    if let Ok(acc) = repo.get_account(user_id, asset).await {
                        let _ = tx.try_send(
                            json!({
                                "type": "balance_update",
                                "asset": acc.asset,
                                "balance": acc.balance,
                                "available": acc.balance - acc.frozen,
                                "frozen": acc.frozen,
                            })
                            .to_string(),
                        );
                    }
                }
            }

            Some(json!({"type": "order_update", "order_id": order_id, "status": "CANCELED", "ts": unix_now()}).to_string())
        }

        ClientMsg::PlaceOrders { batch_id, orders } => {
            let user_id = match session.user_id {
                Some(id) => id,
                None => {
                    return Some(
                        json!({"type": "error", "message": "Not authenticated"}).to_string(),
                    )
                }
            };

            // Cap batch at 40 orders.
            let orders = if orders.len() > 40 { &orders[..40] } else { &orders[..] };

            // ── Aeron fast path: parse all → parallel DB freeze → batch-publish ──────────
            // All DB freezes run concurrently (bids freeze USDT, asks freeze BTC — different
            // rows so they don't contend). Then all N SBE requests are published in one Aeron
            // write so the engine sees the full batch before the next 10ms depth snapshot.
            if state.aeron_cmd_tx.is_some() {
                use crate::sbe::NewOrderRequest as SbeNewOrder;
                use crate::transport::OrderMeta;

                struct V {
                    idx: usize,
                    coid: String,
                    order_id: u64,
                    freeze_asset: String,
                    freeze_amount: f64,
                    sbe_req: SbeNewOrder,
                    meta: OrderMeta,
                }

                let mut par_results = vec![serde_json::Value::Null; orders.len()];
                let mut validated: Vec<V> = Vec::with_capacity(orders.len());

                for (idx, order_val) in orders.iter().enumerate() {
                    let coid = order_val.get("client_order_id").and_then(|v| v.as_str()).unwrap_or("").to_owned();

                    macro_rules! rej {
                        ($r:expr) => {{ par_results[idx] = json!({"client_order_id": &coid, "accepted": false, "reason": $r}); continue; }};
                    }

                    let symbol = match order_val.get("symbol").and_then(|v| v.as_str()) {
                        Some(s) => s.to_owned(), None => rej!("Missing symbol"),
                    };
                    let side = match order_val.get("side").and_then(|v| v.as_str()) {
                        Some(s) => s.to_owned(), None => rej!("Missing side"),
                    };
                    let order_type = order_val.get("order_type").and_then(|v| v.as_str()).unwrap_or("limit").to_owned();
                    let time_in_force: Option<String> = order_val.get("time_in_force").and_then(|v| v.as_str()).map(|s| s.to_ascii_lowercase());
                    let price: Option<f64> = order_val.get("price").and_then(|v| v.as_f64());
                    let qty = match order_val.get("qty").or_else(|| order_val.get("quantity")).and_then(|v| v.as_f64()) {
                        Some(q) => q, None => rej!("Missing qty"),
                    };
                    let engine_side = match side.as_str() {
                        "buy" => Side::Buy, "sell" => Side::Sell, _ => rej!("Invalid side"),
                    };
                    let tif = match order_type.as_str() {
                        "ioc" => TimeInForce::IOC,
                        "fok" => TimeInForce::FOK,
                        "post_only" => TimeInForce::PostOnly,
                        "market" => TimeInForce::IOC,
                        "limit" | "gtc" => match time_in_force.as_deref() {
                            Some("ioc") => TimeInForce::IOC,
                            Some("fok") => TimeInForce::FOK,
                            Some("post_only") => TimeInForce::PostOnly,
                            _ => TimeInForce::GTC,
                        },
                        _ => rej!("Unknown order type"),
                    };

                    if !state.valid_symbols.is_empty() && !state.valid_symbols.contains(&symbol) {
                        rej!(format!("No engine for symbol: {}", symbol));
                    }

                    let sym_parts: Vec<&str> = symbol.splitn(2, '_').collect();
                    let base_asset = sym_parts.first().copied().unwrap_or("BTC");
                    let quote_asset = sym_parts.last().copied().unwrap_or("USDT");

                    let best_opposing_price = best_opposing_from_depth(state, &symbol, &side);
                    let freeze_price_val: f64 = if side == "buy" { price.or(best_opposing_price).unwrap_or(0.0) } else { 0.0 };

                    let (freeze_asset, freeze_amount) = if side == "buy" {
                        let amount = freeze_price_val * qty;
                        if amount <= 0.0 { rej!("Unable to determine buy reservation price"); }
                        (quote_asset.to_owned(), amount)
                    } else {
                        (base_asset.to_owned(), qty)
                    };

                    let order_id = state.next_order_id.fetch_add(1, Ordering::Relaxed);
                    let mut sym_bytes = [0u8; 16];
                    let sb = symbol.as_bytes();
                    sym_bytes[..sb.len().min(16)].copy_from_slice(&sb[..sb.len().min(16)]);
                    let side_byte: u8 = if engine_side == Side::Buy { 0 } else { 1 };
                    let tif_byte: u8 = match tif { TimeInForce::GTC => 0, TimeInForce::IOC => 1, TimeInForce::FOK => 2, TimeInForce::PostOnly => 3 };

                    let meta = OrderMeta {
                        user_id, symbol: symbol.clone(), side: side.clone(),
                        order_type: order_type.clone(), price, qty,
                        client_order_id: coid.clone(), freeze_price: freeze_price_val,
                    };
                    let sbe_req = SbeNewOrder {
                        client_order_id: order_id, participant_id: user_id as u64,
                        price: price.unwrap_or(0.0), quantity: qty,
                        side: side_byte, time_in_force: tif_byte,
                        _pad: [0; 14], symbol: sym_bytes,
                    };
                    validated.push(V { idx, coid, order_id, freeze_asset, freeze_amount, sbe_req, meta });
                }

                // Phase 2 + 3: in-memory freeze check + build aeron_batch (no DB on hot path).
                let mut aeron_batch: smallvec::SmallVec<[SbeNewOrder; 32]> = smallvec::SmallVec::new();
                for v in validated {
                    if try_freeze_cache(&state.account_cache, user_id, &v.freeze_asset, v.freeze_amount) {
                        if let Some(user_assets) = state.account_cache.get(&user_id) {
                            if let Some(&(bal, frz)) = user_assets.get(v.freeze_asset.as_str()) {
                                if let Some(tx) = state.user_tx.get(user_id) {
                                    let _ = tx.try_send(json!({
                                        "type": "balance_update", "asset": &v.freeze_asset,
                                        "balance": bal, "available": bal - frz, "frozen": frz,
                                    }).to_string());
                                }
                            }
                        }
                        state.pending_meta.insert(v.order_id, Box::new(v.meta));
                        aeron_batch.push(v.sbe_req);
                        par_results[v.idx] = json!({"client_order_id": v.coid, "order_id": v.order_id, "accepted": true});
                    } else {
                        par_results[v.idx] = json!({"client_order_id": v.coid, "accepted": false, "reason": "Insufficient balance"});
                    }
                }

                if !aeron_batch.is_empty() {
                    if let Some(ref tx) = state.aeron_cmd_tx {
                        let _ = tx.send(crate::transport::AeronCmd::BatchNewOrder(aeron_batch));
                    }
                }

                let results: Vec<_> = par_results.into_iter().filter(|v| !v.is_null()).collect();
                return Some(json!({"type": "orders_placed", "batch_id": batch_id, "results": results}).to_string());
            }

            // ── Standalone engine path ─────────────────────────────────────────────────────
            let mut results: Vec<serde_json::Value> = Vec::with_capacity(orders.len());

            for order_val in orders {
                let coid = order_val
                    .get("client_order_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_owned();
                let symbol = match order_val.get("symbol").and_then(|v| v.as_str()) {
                    Some(s) => s.to_owned(),
                    None => {
                        results.push(json!({"client_order_id": coid, "accepted": false, "reason": "Missing symbol"}));
                        continue;
                    }
                };
                let side = match order_val.get("side").and_then(|v| v.as_str()) {
                    Some(s) => s.to_owned(),
                    None => {
                        results.push(json!({"client_order_id": coid, "accepted": false, "reason": "Missing side"}));
                        continue;
                    }
                };
                let order_type = order_val
                    .get("order_type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("limit")
                    .to_owned();
                let time_in_force: Option<String> = order_val
                    .get("time_in_force")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_ascii_lowercase());
                let price: Option<f64> = order_val.get("price").and_then(|v| v.as_f64());
                let qty = match order_val
                    .get("qty")
                    .or_else(|| order_val.get("quantity"))
                    .and_then(|v| v.as_f64())
                {
                    Some(q) => q,
                    None => {
                        results.push(json!({"client_order_id": coid, "accepted": false, "reason": "Missing qty"}));
                        continue;
                    }
                };

                let engine_side = match side.as_str() {
                    "buy" => Side::Buy,
                    "sell" => Side::Sell,
                    _ => {
                        results.push(json!({"client_order_id": coid, "accepted": false, "reason": "Invalid side"}));
                        continue;
                    }
                };

                let tif = match order_type.as_str() {
                    "ioc" => TimeInForce::IOC,
                    "fok" => TimeInForce::FOK,
                    "post_only" => TimeInForce::PostOnly,
                    "market" => TimeInForce::IOC,
                    "limit" | "gtc" => match time_in_force.as_deref() {
                        Some("ioc") => TimeInForce::IOC,
                        Some("fok") => TimeInForce::FOK,
                        Some("post_only") => TimeInForce::PostOnly,
                        _ => TimeInForce::GTC,
                    },
                    _ => {
                        results.push(json!({"client_order_id": coid, "accepted": false, "reason": "Unknown order type"}));
                        continue;
                    }
                };

                // ── Standalone engine path ───────────────────────────────────
                let engine_opt: Option<Arc<Mutex<MatchingEngine>>> = state
                    .engines
                    .as_ref()
                    .and_then(|m| m.get(&symbol).map(|e| e.value().clone()));
                if state.engines.is_some() && engine_opt.is_none() {
                    results.push(json!({"client_order_id": coid, "accepted": false, "reason": format!("Unknown symbol: {}", symbol)}));
                    continue;
                }
                let engine = match engine_opt {
                    Some(e) => e,
                    None => {
                        results.push(json!({"client_order_id": coid, "accepted": false, "reason": "No matching engine configured"}));
                        continue;
                    }
                };

                let now_ns = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos() as u64)
                    .unwrap_or(0);

                let sym_parts: Vec<&str> = symbol.splitn(2, '_').collect();
                let base_asset = sym_parts.first().copied().unwrap_or("BTC");
                let quote_asset = sym_parts.last().copied().unwrap_or("USDT");
                let rules = crate::symbol_rules::SymbolRules::for_symbol(&symbol);

                let best_opposing_price: Option<f64> = {
                    let eng = engine.lock().unwrap();
                    let levels = eng.get_top_levels(1, engine_side == Side::Sell);
                    levels.first().map(|(p, _)| rules.ticks_to_price(*p))
                };

                let freeze_price_val: f64 = if side == "buy" {
                    price.or(best_opposing_price).unwrap_or(0.0)
                } else {
                    0.0
                };

                let db_order_id = state.next_order_id.fetch_add(1, Ordering::Relaxed) as i64;
                let db_result = sqlx::query_as::<_, crate::models::DbOrder>(
                    "INSERT INTO orders (id, user_id, symbol, side, order_type, price, quantity, filled, status, freeze_price)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, 0, 'PENDING', $8) RETURNING *",
                )
                .bind(db_order_id)
                .bind(user_id)
                .bind(&symbol)
                .bind(&side)
                .bind(&order_type)
                .bind(price)
                .bind(qty)
                .bind(freeze_price_val)
                .fetch_one(state.db.as_ref())
                .await;

                if let Err(e) = db_result {
                    results.push(json!({"client_order_id": coid, "accepted": false, "reason": format!("DB error: {}", e)}));
                    continue;
                }

                let fixed_order = match crate::symbol_rules::FixedOrderInput::from_order_fields(
                    db_order_id as u64,
                    &symbol,
                    engine_side,
                    &order_type,
                    price,
                    qty,
                    tif,
                    now_ns,
                ) {
                    Ok(o) => o,
                    Err(reason) => {
                        results.push(json!({"client_order_id": coid, "accepted": false, "reason": reason}));
                        continue;
                    }
                };

                let engine_order = if fixed_order.is_market {
                    Order::new_market(
                        fixed_order.id,
                        fixed_order.side,
                        fixed_order.quantity_lots,
                        fixed_order.timestamp,
                    )
                } else {
                    Order::new(
                        fixed_order.id,
                        fixed_order.side,
                        fixed_order.price_ticks,
                        fixed_order.quantity_lots,
                        fixed_order.time_in_force,
                        fixed_order.timestamp,
                    )
                };

                // Freeze funds.
                let repo = AccountRepository::new(state.db.as_ref());
                let freeze_result = if side == "buy" {
                    let freeze_amount = freeze_price_val * qty;
                    if freeze_amount > 0.0 {
                        repo.freeze_for_buy(user_id, quote_asset, freeze_amount).await
                    } else {
                        Ok((0.0, 0.0))
                    }
                } else {
                    repo.freeze_for_sell(user_id, base_asset, qty).await
                };

                let frozen_asset = if side == "buy" { quote_asset } else { base_asset };
                match freeze_result {
                    Err(e) => {
                        let _ = sqlx::query(
                            "DELETE FROM orders WHERE id=$1",
                        )
                        .bind(db_order_id)
                        .execute(state.db.as_ref())
                        .await;
                        results.push(json!({"client_order_id": coid, "accepted": false, "reason": e.to_string()}));
                        continue;
                    }
                    Ok((bal, frz)) if bal > 0.0 || frz >= 0.0 => {
                        state
                            .account_cache
                            .entry(user_id)
                            .or_insert_with(std::collections::HashMap::new)
                            .insert(frozen_asset.to_string(), (bal, frz));
                        if let Some(tx) = state.user_tx.get(user_id) {
                            let _ = tx.try_send(
                                json!({
                                    "type": "balance_update",
                                    "asset": frozen_asset,
                                    "balance": bal,
                                    "available": bal - frz,
                                    "frozen": frz,
                                })
                                .to_string(),
                            );
                        }
                    }
                    Ok(_) => {}
                }

                let engine_result = {
                    let mut eng = engine.lock().unwrap();
                    eng.place_order(engine_order)
                };
                let result = match engine_result {
                    Ok(r) => r,
                    Err(e) => {
                        let _ = sqlx::query(
                            "DELETE FROM orders WHERE id = $1",
                        )
                        .bind(db_order_id)
                        .execute(state.db.as_ref())
                        .await;
                        if side == "buy" {
                            let p = price.or(best_opposing_price).unwrap_or(0.0);
                            if p > 0.0 {
                                let _ = repo.release_frozen(user_id, quote_asset, p * qty).await;
                            }
                        } else {
                            let _ = repo.release_frozen(user_id, base_asset, qty).await;
                        }
                        results.push(json!({"client_order_id": coid, "accepted": false, "reason": e.to_string()}));
                        continue;
                    }
                };

                let db_status = db_status_from_engine(result.status).as_str();
                let filled_qty = rules.lots_to_quantity(result.filled_lots);
                let _ = sqlx::query(
                    "UPDATE orders SET status = $1, filled = $2, updated_at = NOW() WHERE id = $3",
                )
                .bind(db_status)
                .bind(filled_qty)
                .bind(db_order_id)
                .execute(state.db.as_ref())
                .await;

                if result.status == OrderStatus::Rejected {
                    if side == "buy" {
                        let p = price.or(best_opposing_price).unwrap_or(0.0);
                        if p > 0.0 {
                            let _ = repo.release_frozen(user_id, quote_asset, p * qty).await;
                        }
                    } else {
                        let _ = repo.release_frozen(user_id, base_asset, qty).await;
                    }
                    results.push(json!({"client_order_id": coid, "accepted": false, "reason": "Rejected by matching engine"}));
                    continue;
                }

                results.push(json!({"client_order_id": coid, "order_id": db_order_id, "accepted": true}));
            }

            Some(
                json!({
                    "type": "orders_placed",
                    "batch_id": batch_id,
                    "results": results,
                })
                .to_string(),
            )
        }

        ClientMsg::BatchCancel { order_ids } => {
            let user_id = match session.user_id {
                Some(id) => id,
                None => {
                    return Some(json!({"type":"error","message":"Not authenticated"}).to_string())
                }
            };
            if order_ids.is_empty() {
                return Some(json!({"type":"batch_cancel_ok","cancelled":0}).to_string());
            }
            let n = batch_cancel_by_ids(user_id, &order_ids, state, &personal_tx).await;
            Some(json!({"type":"batch_cancel_ok","cancelled":n}).to_string())
        }
    }
}

/// Cancel a specific list of orders by ID for `user_id`.
/// Sends all cancel requests to Aeron in one BatchCancel command. In Aeron mode,
/// DB/funds/WS updates wait for engine-confirmed CANCELLED events.
/// Returns the number of orders cancelled.
async fn batch_cancel_by_ids(
    user_id: i64,
    order_ids: &[i64],
    state: &AppState,
    personal_tx: &tokio::sync::mpsc::Sender<String>,
) -> usize {
    if order_ids.is_empty() {
        return 0;
    }

    // Aeron mode: send cancels directly to the engine without a DB round-trip.
    // The DB query was the dominant latency source (>500ms under load), causing
    // the market-maker's cancel confirmation timeout to fire before the cancel
    // even reached the engine.  Fund release / DB update happen via CancelConfirmed
    // when the engine's CANCELLED event arrives back through the Aeron spin thread.
    if state.engines.is_none() {
        if let Some(aeron_cmd_tx) = &state.aeron_cmd_tx {
            let reqs: smallvec::SmallVec<[crate::sbe::CancelOrderRequest; 64]> = order_ids
                .iter()
                .map(|&id| crate::sbe::CancelOrderRequest {
                    order_id: id as u64,
                    participant_id: user_id as u64,
                })
                .collect();
            return if aeron_cmd_tx
                .send(crate::transport::AeronCmd::BatchCancel(reqs))
                .is_ok()
            {
                order_ids.len()
            } else {
                0
            };
        }
    }

    // Standalone mode: query DB to get order details for synchronous fund release.
    #[derive(sqlx::FromRow)]
    struct OpenOrder {
        id: i64,
        symbol: String,
        side: String,
        freeze_price: f64,
        quantity: f64,
        filled: f64,
    }

    let rows: Vec<OpenOrder> = sqlx::query_as(
        "SELECT id, symbol, side,
                COALESCE(freeze_price, COALESCE(price, 0.0)) as freeze_price,
                quantity, filled FROM orders
         WHERE id = ANY($1) AND user_id = $2 AND status IN ('PENDING','TRADING')",
    )
    .bind(order_ids)
    .bind(user_id)
    .fetch_all(state.db.as_ref())
    .await
    .unwrap_or_default();

    if rows.is_empty() {
        return 0;
    }

    if let Some(engines) = &state.engines {
        for order in &rows {
            if let Some(engine) = engines.get(&order.symbol) {
                let _ = {
                    let mut eng = engine.lock().unwrap();
                    eng.cancel_order(order.id as u64)
                };
            }
        }
    }

    // Batch UPDATE all matching orders in one query.
    let ids: Vec<i64> = rows.iter().map(|r| r.id).collect();
    let _ = sqlx::query(
        "DELETE FROM orders WHERE id = ANY($1) AND status IN ('PENDING','TRADING')",
    )
    .bind(&ids[..])
    .execute(state.db.as_ref())
    .await;

    let repo = crate::account_repository::AccountRepository::new(state.db.as_ref());
    let ts = unix_now();
    let count = rows.len();

    for order in rows {
        let remaining = (order.quantity - order.filled).max(0.0);
        let sym_parts: Vec<&str> = order.symbol.splitn(2, '_').collect();
        let base_asset = sym_parts.first().copied().unwrap_or("BTC");
        let quote_asset = sym_parts.last().copied().unwrap_or("USDT");

        if order.side == "buy" {
            if order.freeze_price > 0.0 && remaining > 0.0 {
                let _ = repo
                    .release_frozen(user_id, quote_asset, order.freeze_price * remaining)
                    .await;
            }
        } else if remaining > 0.0 {
            let _ = repo.release_frozen(user_id, base_asset, remaining).await;
        }

        let _ = personal_tx.try_send(
            json!({
                "type": "order_update",
                "order_id": order.id,
                "status": "CANCELED",
                "filled_qty": order.filled,
                "ts": ts,
            })
            .to_string(),
        );
    }

    count
}

/// Cancel all open orders for `user_id`, optionally filtered to a single symbol.
/// Cancels each order. In Aeron mode, DB/funds/WS updates wait for
/// engine-confirmed CANCELLED events.
/// Returns the number of orders cancelled.
async fn bulk_cancel(
    user_id: i64,
    symbol: Option<&str>,
    state: &AppState,
    personal_tx: &tokio::sync::mpsc::Sender<String>,
) -> usize {
    #[derive(sqlx::FromRow)]
    struct OpenOrder {
        id: i64,
        symbol: String,
        side: String,
        freeze_price: f64,
        quantity: f64,
        filled: f64,
    }

    let rows: Vec<OpenOrder> = match symbol {
        Some(sym) => sqlx::query_as(
            "SELECT id, symbol, side,
                    COALESCE(freeze_price, COALESCE(price, 0.0)) as freeze_price,
                    quantity, filled FROM orders
             WHERE user_id=$1 AND symbol=$2 AND status IN ('PENDING','TRADING')",
        )
        .bind(user_id)
        .bind(sym),
        None => sqlx::query_as(
            "SELECT id, symbol, side,
                    COALESCE(freeze_price, COALESCE(price, 0.0)) as freeze_price,
                    quantity, filled FROM orders
             WHERE user_id=$1 AND status IN ('PENDING','TRADING')",
        )
        .bind(user_id),
    }
    .fetch_all(state.db.as_ref())
    .await
    .unwrap_or_default();

    if rows.is_empty() {
        return 0;
    }

    if state.engines.is_none() {
        if let Some(aeron_cmd_tx) = &state.aeron_cmd_tx {
            let mut reqs = smallvec::SmallVec::<[crate::sbe::CancelOrderRequest; 64]>::new();
            for order in &rows {
                reqs.push(crate::sbe::CancelOrderRequest {
                    order_id: order.id as u64,
                    participant_id: user_id as u64,
                });
            }
            return if aeron_cmd_tx
                .send(crate::transport::AeronCmd::BatchCancel(reqs))
                .is_ok()
            {
                rows.len()
            } else {
                0
            };
        }
    }

    if let Some(engines) = &state.engines {
        for order in &rows {
            if let Some(engine) = engines.get(&order.symbol) {
                let _ = {
                    let mut eng = engine.lock().unwrap();
                    eng.cancel_order(order.id as u64)
                };
            }
        }
    }

    // Batch UPDATE all matching orders in one query.
    let ids: Vec<i64> = rows.iter().map(|r| r.id).collect();
    let _ = sqlx::query(
        "DELETE FROM orders WHERE id = ANY($1) AND status IN ('PENDING','TRADING')",
    )
    .bind(&ids[..])
    .execute(state.db.as_ref())
    .await;

    let repo = crate::account_repository::AccountRepository::new(state.db.as_ref());
    let ts = unix_now();
    let count = rows.len();

    for order in rows {
        let remaining = (order.quantity - order.filled).max(0.0);

        // Release frozen funds for the unfilled portion.
        let sym_parts: Vec<&str> = order.symbol.splitn(2, '_').collect();
        let base_asset = sym_parts.first().copied().unwrap_or("BTC");
        let quote_asset = sym_parts.last().copied().unwrap_or("USDT");

        if order.side == "buy" {
            if order.freeze_price > 0.0 && remaining > 0.0 {
                let _ = repo
                    .release_frozen(user_id, quote_asset, order.freeze_price * remaining)
                    .await;
            }
        } else if remaining > 0.0 {
            let _ = repo.release_frozen(user_id, base_asset, remaining).await;
        }

        // Push order_update to the caller's personal channel.
        let _ = personal_tx.try_send(
            json!({
                "type": "order_update",
                "order_id": order.id,
                "status": "CANCELED",
                "filled_qty": order.filled,
                "ts": ts,
            })
            .to_string(),
        );
    }

    count
}

/// Build a depth snapshot JSON for the given symbol from the matching engine.
fn build_depth_json(engine: &Mutex<MatchingEngine>, symbol: &str) -> String {
    let rules = crate::symbol_rules::SymbolRules::for_symbol(symbol);
    let (bids, asks) = {
        let eng = engine.lock().unwrap();
        (eng.get_top_levels(10, true), eng.get_top_levels(10, false))
    };
    let bids: Vec<_> = bids
        .into_iter()
        .filter(|(_, q)| *q > 0)
        .map(|(p, q)| (rules.ticks_to_price(p), rules.lots_to_quantity(q)))
        .collect();
    let asks: Vec<_> = asks
        .into_iter()
        .filter(|(_, q)| *q > 0)
        .map(|(p, q)| (rules.ticks_to_price(p), rules.lots_to_quantity(q)))
        .collect();
    json!({
        "type": "depth",
        "symbol": symbol,
        "bids": bids,
        "asks": asks,
        "ts": unix_now()
    })
    .to_string()
}

/// Build and broadcast a depth snapshot for the given symbol.
pub fn broadcast_depth_pub(state: &AppState, symbol: &str) {
    if let Some(engines) = &state.engines {
        if let Some(engine) = engines.get(symbol) {
            state
                .market_fanout
                .send_owned(build_depth_json(engine.value(), symbol));
        }
    } else if let Some(depth_json) = state.last_depth.get(symbol) {
        state.market_fanout.send_owned(depth_json.to_string());
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
        let _ = state.market_fanout.send_owned(msg);
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
            let rules = crate::symbol_rules::SymbolRules::for_symbol(&symbol);
            let eng = entry.value().lock().unwrap();
            let bids: Vec<_> = eng
                .get_top_levels(10, true)
                .into_iter()
                .filter(|(_, q)| *q > 0)
                .map(|(p, q)| (rules.ticks_to_price(p), rules.lots_to_quantity(q)))
                .collect();
            let asks: Vec<_> = eng
                .get_top_levels(10, false)
                .into_iter()
                .filter(|(_, q)| *q > 0)
                .map(|(p, q)| (rules.ticks_to_price(p), rules.lots_to_quantity(q)))
                .collect();
            (symbol, bids, asks)
        })
        .collect()
}

pub async fn market_data_broadcaster(state: AppState) {
    let mut depth_interval = tokio::time::interval(std::time::Duration::from_millis(200));
    depth_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut kline_interval = tokio::time::interval(std::time::Duration::from_secs(10));
    kline_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut ticker_interval = tokio::time::interval(std::time::Duration::from_millis(100));
    ticker_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut agg_interval = tokio::time::interval(std::time::Duration::from_secs(1));
    agg_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Last-seen top-bid price per symbol — drives ticker broadcasts.
    let mut last_prices: HashMap<String, f64> = HashMap::new();
    // 24h stats cache: symbol → (open_24h, high, low, volume, last_queried_secs).
    // Shared with spawned tasks so they can write results back.
    // Only re-query the DB every 60 seconds to avoid pool exhaustion on a full table.
    let ticker_cache: Arc<std::sync::Mutex<HashMap<String, (f64, f64, f64, f64, i64)>>> =
        Arc::new(std::sync::Mutex::new(HashMap::new()));

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
                        let _ = state.market_fanout.send_owned(msg);
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
                // DB is queried at most once every 60s per symbol (cached in ticker_cache).
                // Between DB refreshes we broadcast cached 24h stats with live last price.
                //
                // `last` = most recent trade price (from state.last_trade_price, written
                // by SettleTrade). Fallback to best bid (last_prices) only for symbols
                // that haven't traded yet this session.
                let now_secs = unix_secs();
                let symbols: Vec<(String, f64)> = last_prices.iter()
                    .filter(|(_, &p)| p > 0.0)
                    .map(|(s, &bid)| {
                        let last = state.last_trade_price.get(s).map(|v| *v).unwrap_or(bid);
                        (s.clone(), last)
                    })
                    .collect();
                for (symbol, last_price) in symbols {
                    let cached = ticker_cache.lock().ok()
                        .and_then(|c| c.get(&symbol).copied());
                    let needs_refresh = cached.map(|(_, _, _, _, t)| now_secs - t >= 60).unwrap_or(true);

                    if needs_refresh {
                        // Mark updated time immediately so concurrent ticks don't pile up queries.
                        if let Ok(mut c) = ticker_cache.lock() {
                            c.entry(symbol.clone()).or_insert((0.0, 0.0, 0.0, 0.0, 0_i64)).4 = now_secs;
                        }
                        let db = state.db.clone();
                        let mtx = state.market_fanout.clone();
                        let last_ticker = state.last_ticker.clone();
                        let tc2 = ticker_cache.clone();
                        let sym2 = symbol.clone();
                        tokio::spawn(async move {
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
                            .bind(&sym2)
                            .fetch_optional(db.as_ref())
                            .await
                            .unwrap_or(None);
                            let (open_24h, high, low, volume) = row.unwrap_or_default();
                            let open = open_24h.unwrap_or(0.0);
                            let h = high.unwrap_or(last_price);
                            let l = low.unwrap_or(last_price);
                            let v = volume.unwrap_or(0.0);
                            // Write results back into the shared cache.
                            let ts = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.as_secs() as i64).unwrap_or(0);
                            if let Ok(mut c) = tc2.lock() {
                                c.insert(sym2.clone(), (open, h, l, v, ts));
                            }
                            let change = if open != 0.0 { (last_price - open) / open * 100.0 } else { 0.0 };
                            let payload = serde_json::json!({
                                "type": "ticker",
                                "symbol": sym2,
                                "last": last_price,
                                "change": change,
                                "high": h,
                                "low": l,
                                "volume": v,
                            }).to_string();
                            last_ticker.insert(sym2.clone(), payload.clone());
                            mtx.send_owned(payload);
                        });
                    } else if let Some((open, high, low, volume, _)) = cached {
                        // Broadcast cached stats with current last price — no DB hit.
                        let change = if open != 0.0 { (last_price - open) / open * 100.0 } else { 0.0 };
                        let payload = serde_json::json!({
                            "type": "ticker",
                            "symbol": symbol,
                            "last": last_price,
                            "change": change,
                            "high": high,
                            "low": low,
                            "volume": volume,
                        }).to_string();
                        state.last_ticker.insert(symbol.clone(), payload.clone());
                        let _ = state.market_fanout.send_owned(payload);
                    }
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
                    let mtx = state.market_fanout.clone();
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
                                mtx.send_owned(msg);
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
                        let mtx = state.market_fanout.clone();
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
                                mtx.send_owned(msg);
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
    use super::build_depth_json;
    use crate::{MatchingEngine, Order, PoolConfig, Side, TimeInForce};
    use serde_json::Value;
    use std::sync::Mutex;

    fn empty_engine() -> Mutex<MatchingEngine> {
        Mutex::new(MatchingEngine::new(PoolConfig::default()).unwrap())
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
            eng.place_order(Order::new(1, Side::Buy, 100, 2, TimeInForce::GTC, 0))
                .unwrap();
            eng.place_order(Order::new(2, Side::Sell, 101, 3, TimeInForce::GTC, 0))
                .unwrap();
        }

        let v: Value = serde_json::from_str(&build_depth_json(&engine, "BTC_USDT")).unwrap();
        let bids = v["bids"].as_array().unwrap();
        let asks = v["asks"].as_array().unwrap();

        assert_eq!(bids.len(), 1);
        assert_eq!(asks.len(), 1);
        // Each level is serialized as a [price, qty] tuple.
        assert_eq!(bids[0][0].as_f64().unwrap(), 1.0);
        assert_eq!(bids[0][1].as_f64().unwrap(), 0.000002);
        assert_eq!(asks[0][0].as_f64().unwrap(), 1.01);
        assert_eq!(asks[0][1].as_f64().unwrap(), 0.000003);
    }

    #[test]
    fn build_depth_json_propagates_symbol_argument() {
        let engine = empty_engine();
        let v: Value = serde_json::from_str(&build_depth_json(&engine, "ETH_USDT")).unwrap();
        assert_eq!(v["symbol"], "ETH_USDT");
    }
}
