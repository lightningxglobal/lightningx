use crate::account_repository::AccountRepository;
use crate::api::AppState;
use crate::desk::read_actor::{ConnMarketInfo, ReadConn};
use dashmap::DashMap as ActorConnMap;
use crate::engine::{MatchingEngine, OrderStatus};
use crate::order::{Order, Side, TimeInForce};
use crate::order_state::{
    db_status_from_engine, maker_ws_status_from_db_status, ws_status_from_engine,
};
use crate::user_service;
use crate::ws_sbe;
use axum::extract::{Request, State};
use axum::response::Response;
use dashmap::DashMap;
use fastwebsockets::{upgrade, Frame, OpCode};
use std::time::Duration;
use tokio::io as tio;
use serde_json::Value;
use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

fn try_freeze_cache(
    cache: &crate::api::AccountCache,
    user_id: i64,
    asset: &str,
    amount: f64,
) -> bool {
    if amount <= 0.0 {
        return true;
    }
    let Some(mut entry) = cache.get_mut(&user_id) else {
        return false;
    };
    let Some(kv) = entry.get_mut(asset) else {
        return false;
    };
    if kv.0 - kv.1 >= amount {
        kv.1 += amount;
        true
    } else {
        false
    }
}

fn release_cache_frozen(cache: &crate::api::AccountCache, user_id: i64, asset: &str, amount: f64) {
    if amount <= 0.0 {
        return;
    }
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

#[inline]
fn ws_status_to_byte(status: crate::order_state::WsOrderStatus) -> u8 {
    use crate::order_state::WsOrderStatus;
    match status {
        WsOrderStatus::Open => ws_sbe::WS_STATUS_OPEN,
        WsOrderStatus::Partial | WsOrderStatus::PartialFill => ws_sbe::WS_STATUS_PARTIAL_FILL,
        WsOrderStatus::Filled => ws_sbe::WS_STATUS_FILLED,
        WsOrderStatus::Canceled => ws_sbe::WS_STATUS_CANCELED,
        WsOrderStatus::Rejected => ws_sbe::WS_STATUS_REJECTED,
    }
}

fn ws_place_profile_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("WS_PLACE_PROFILE")
            .map(|v| v == "1")
            .unwrap_or(false)
    })
}

fn ws_push_bal_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("WS_PUSH_BAL")
            .map(|v| v == "1")
            .unwrap_or(false)
    })
}

fn ws_inline_order_submitted_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("WS_INLINE_ORDER_SUBMITTED")
            .map(|v| v == "1")
            .unwrap_or(false)
    })
}

fn ws_personal_queue_cap() -> usize {
    static CAP: OnceLock<usize> = OnceLock::new();
    *CAP.get_or_init(|| {
        std::env::var("WS_PERSONAL_QUEUE_CAP")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(4096)
    })
}

fn aeron_cmd_latency_cap() -> Option<usize> {
    static CAP: OnceLock<Option<usize>> = OnceLock::new();
    *CAP.get_or_init(|| {
        std::env::var("AERON_CMD_LATENCY_CAP")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|&v| v > 0)
    })
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
        if matches!(json_str_field(text, "type"), Some("place_order")) {
            if let Some(msg) = parse_place_order_fast(text) {
                return Some(msg);
            }
        }
        if matches!(json_str_field(text, "type"), Some("cancel_order")) {
            if let Some(order_id) = json_i64_field(text, "order_id") {
                return Some(ClientMsg::CancelOrder { order_id });
            }
        }

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
                let batch_id = v
                    .get("batch_id")
                    .and_then(|b| b.as_str())
                    .unwrap_or("")
                    .to_owned();
                let orders = v
                    .get("orders")
                    .and_then(|a| a.as_array())
                    .cloned()
                    .unwrap_or_default();
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

fn parse_place_order_fast(text: &str) -> Option<ClientMsg> {
    Some(ClientMsg::PlaceOrder {
        client_order_id: json_str_field(text, "client_order_id")?.to_owned(),
        symbol: json_str_field(text, "symbol")?.to_owned(),
        side: json_str_field(text, "side")?.to_owned(),
        order_type: json_str_field(text, "order_type")
            .unwrap_or("limit")
            .to_owned(),
        time_in_force: json_str_field(text, "time_in_force").map(|s| s.to_ascii_lowercase()),
        price: json_f64_field(text, "price"),
        qty: json_f64_field(text, "qty").or_else(|| json_f64_field(text, "quantity"))?,
    })
}

fn json_str_field<'a>(text: &'a str, key: &str) -> Option<&'a str> {
    let key_pos = find_json_key(text, key)?;
    let mut rest = &text[key_pos + key.len() + 2..];
    rest = rest.trim_start();
    rest = rest.strip_prefix(':')?.trim_start();
    rest = rest.strip_prefix('"')?;
    let end = rest.find('"')?;
    Some(&rest[..end])
}

fn json_f64_field(text: &str, key: &str) -> Option<f64> {
    let key_pos = find_json_key(text, key)?;
    let mut rest = &text[key_pos + key.len() + 2..];
    rest = rest.trim_start();
    rest = rest.strip_prefix(':')?.trim_start();
    let end = rest
        .find(|c: char| !(c.is_ascii_digit() || matches!(c, '.' | '-' | '+' | 'e' | 'E')))
        .unwrap_or(rest.len());
    if end == 0 {
        return None;
    }
    rest[..end].parse().ok()
}

fn json_i64_field(text: &str, key: &str) -> Option<i64> {
    let key_pos = find_json_key(text, key)?;
    let mut rest = &text[key_pos + key.len() + 2..];
    rest = rest.trim_start();
    rest = rest.strip_prefix(':')?.trim_start();
    let end = rest
        .find(|c: char| !(c.is_ascii_digit() || c == '-'))
        .unwrap_or(rest.len());
    if end == 0 {
        return None;
    }
    rest[..end].parse().ok()
}

fn find_json_key(text: &str, key: &str) -> Option<usize> {
    let mut start = 0;
    loop {
        let pos = text[start..].find('"')? + start;
        let after_quote = pos + 1;
        let after_key = after_quote + key.len();
        if text[after_quote..].starts_with(key) && text[after_key..].starts_with('"') {
            return Some(pos);
        }
        start = after_quote;
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
    // A non-WS GET (e.g. curl, health probe) hits the same /ws route — never
    // panic on bad-shape requests. Return 400 instead so a stray client can't
    // take down the entire desk-server (we measured one curl request crashing
    // the whole process via this expect()).
    let (response, fut) = match upgrade::upgrade(&mut req) {
        Ok(v) => v,
        Err(e) => {
            tracing::debug!("WS upgrade rejected: {e}");
            return Response::builder()
                .status(400)
                .body(axum::body::Body::from(format!("WS upgrade rejected: {e}")))
                .unwrap();
        }
    };
    tokio::spawn(async move {
        match fut.await {
            Ok(mut socket) => {
                use std::sync::atomic::{AtomicU64, Ordering};
                static CONN_ID_GEN: AtomicU64 = AtomicU64::new(1);
                let conn_id = CONN_ID_GEN.fetch_add(1, Ordering::Relaxed);

                socket.set_writev(true);
                socket.set_auto_close(false);
                socket.set_auto_pong(false);

                let (ws_read, ws_write) = socket.split(tio::split);
                let (personal_tx, ctrl_tx) =
                    match state.write_pool.register(ws_write, ws_personal_queue_cap(), state.tracer.clone()) {
                        Some(v) => v,
                        None => return,
                    };
                let subscriptions = Arc::new(RwLock::new(HashSet::new()));
                state.read_pool.register(ReadConn {
                    conn_id,
                    ws_read,
                    state: state.clone(),
                    personal_tx,
                    ctrl_tx,
                    subscriptions,
                });
            }
            Err(e) => tracing::error!("WS upgrade error: {e}"),
        }
    });
    response.map(|_| axum::body::Body::empty())
}

// ─── Socket loop ──────────────────────────────────────────────────────────────

pub async fn read_conn_loop(
    conn: ReadConn,
    actor_conns: Arc<ActorConnMap<u64, ConnMarketInfo>>,
    actor_sub_count: Arc<std::sync::atomic::AtomicUsize>,
) {
    let ReadConn { conn_id, mut ws_read, state, personal_tx, ctrl_tx, subscriptions } = conn;

    // send_fn: required by WebSocketRead::read_frame API but never called
    // because auto_close=false and auto_pong=false suppress all auto-sends.
    let mut noop_send =
        |_frame: Frame<'static>| std::future::ready(Ok::<(), fastwebsockets::WebSocketError>(()));

    let mut session = WsSession::new();

    let mut market_registered = false;

    // ── Heartbeat ────────────────────────────────────────────────────────────
    // Send a WS-level Ping every 30s; kick if no Pong received by the next
    // interval. Jitter based on conn_id spreads 20K timers evenly across
    // the full 30s window, preventing thundering-herd wakeups.
    const PING_INTERVAL: Duration = Duration::from_secs(30);
    let jitter = Duration::from_millis(conn_id % 30_000);
    let mut ping_interval = {
        let start = tokio::time::Instant::now() + jitter;
        tokio::time::interval_at(start, PING_INTERVAL)
    };
    let mut waiting_pong = false;

    'conn: loop {
        tokio::select! {
            biased;

            // Incoming message from client — reads drive all control flow.
            frame = ws_read.read_frame(&mut noop_send) => {
                match frame {
                    Ok(frame) => match frame.opcode {
                        OpCode::Text => {
                            let text = match std::str::from_utf8(&frame.payload) {
                                Ok(t) => t,
                                Err(_) => continue 'conn,
                            };
                            let was_subscribed = !session.subscribed.is_empty();
                            if let Some(reply) = handle_client_message(
                                text, &mut session, &state, &personal_tx,
                            ).await {
                                let _ = personal_tx.try_send((reply, 0));
                            }
                            if let Ok(mut subs) = subscriptions.write() {
                                *subs = session.subscribed.clone();
                            }
                            let now_subscribed = !session.subscribed.is_empty();
                            if !was_subscribed && now_subscribed {
                                actor_sub_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                state.market_fanout.increment_subscriber();
                                market_registered = true;
                            } else if was_subscribed && !now_subscribed {
                                actor_sub_count.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                                state.market_fanout.decrement_subscriber();
                                market_registered = false;
                            }
                        }
                        OpCode::Ping => {
                            // Forward Pong via write actor channel.
                            let _ = ctrl_tx.try_send(crate::desk::write_actor::WsCtrl::Pong(
                                frame.payload.to_vec(),
                            ));
                        }
                        // Client's Pong in response to our heartbeat Ping.
                        OpCode::Pong => {
                            waiting_pong = false;
                        }
                        OpCode::Close => break 'conn,
                        _ => {}
                    }
                    Err(_) => break 'conn,
                }
            }

            // Heartbeat tick: send Ping; kick if previous Ping was not answered.
            _ = ping_interval.tick() => {
                if waiting_pong {
                    // Client missed the previous ping — consider it dead.
                    tracing::debug!(conn_id, "heartbeat timeout, closing connection");
                    break 'conn;
                }
                let _ = ctrl_tx.try_send(crate::desk::write_actor::WsCtrl::Ping);
                waiting_pong = true;
            }

        }
    }

    // Dropping personal_tx + ctrl_tx signals write_conn_loop to exit cleanly.

    // Remove from actor's market registry so the actor stops sending to this conn.
    actor_conns.remove(&conn_id);
    if market_registered {
        actor_sub_count.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        state.market_fanout.decrement_subscriber();
    }
    if let Some(uid) = session.user_id {
        state.user_tx.unregister(uid);
    }
}

fn best_opposing_from_depth(state: &AppState, symbol: &str, side: &str) -> Option<f64> {
    let buf = state.last_depth.get(symbol)?;
    let want_ask = side == "buy";
    ws_sbe::decode_best_price(&buf, want_ask)
}

// ─── Per-message handler ──────────────────────────────────────────────────────

async fn handle_client_message(
    text: &str,
    session: &mut WsSession,
    state: &AppState,
    personal_tx: &mpsc::Sender<(Vec<u8>, u64)>,
) -> Option<Vec<u8>> {
    let msg: ClientMsg = match ClientMsg::parse(text) {
        Some(m) => m,
        None => {
            return Some(ws_sbe::encode_error("Invalid message format"))
        }
    };

    match msg {
        ClientMsg::Auth { token } => match user_service::verify_token(&token) {
            Ok(claims) => {
                session.user_id = Some(claims.sub);
                state.user_tx.register(claims.sub, personal_tx.clone());
                Some(ws_sbe::encode_auth_ok(claims.sub))
            }
            Err(e) => Some(ws_sbe::encode_auth_error(&e.to_string())),
        },

        ClientMsg::ApiKeyAuth { api_key } => {
            match user_service::verify_api_key(&state.db, &api_key).await {
                Ok(user_id) => {
                    session.user_id = Some(user_id);
                    state.user_tx.register(user_id, personal_tx.clone());
                    Some(ws_sbe::encode_auth_ok(user_id))
                }
                Err(e) => Some(ws_sbe::encode_auth_error(&e.to_string())),
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
                        let _ = personal_tx.try_send((build_depth_sbe(engine.value(), &sym), 0));
                    }
                } else if let Some(depth_bytes) = state.last_depth.get(&sym) {
                    // last_depth now stores SBE bytes — clone and send directly.
                    let _ = personal_tx.try_send((depth_bytes.to_vec(), 0));
                }
            }
            for sym in ticker_symbols {
                if let Some(ticker_bytes) = state.last_ticker.get(&sym) {
                    // last_ticker now stores SBE bytes — clone and send directly.
                    let _ = personal_tx.try_send((ticker_bytes.to_vec(), 0));
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

        ClientMsg::Ping => Some(ws_sbe::encode_pong()),

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
                None => return Some(ws_sbe::encode_order_rejected(0, "Not authenticated")),
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
                    return Some(ws_sbe::encode_order_rejected(0, &reason))
                }
            };

            let engine_side = match side.as_str() {
                "buy" => Side::Buy,
                "sell" => Side::Sell,
                _ => return Some(ws_sbe::encode_order_rejected(0, "Invalid side")),
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
                _ => return Some(ws_sbe::encode_order_rejected(0, "Unknown order type")),
            };

            // Look up the matching engine for this symbol; only available in standalone mode.
            let engine_opt: Option<Arc<Mutex<MatchingEngine>>> = state
                .engines
                .as_ref()
                .and_then(|m| m.get(&symbol).map(|e| e.value().clone()));
            // In standalone mode, reject unknown symbols up front.
            if state.engines.is_some() && engine_opt.is_none() {
                return Some(ws_sbe::encode_order_rejected(0, &format!("Unknown symbol: {}", symbol)));
            }

            // Parse base/quote assets from symbol (e.g. "BTC_USDT" → "BTC", "USDT").
            // split_once skips the Vec heap allocation that splitn().collect() did.
            let (base_asset, quote_asset) = symbol.split_once('_').unwrap_or(("BTC", "USDT"));
            let rules = crate::symbol_rules::SymbolRules::for_symbol(&symbol);

            // Capture best opposing price before matching — used as the fill
            // price for market orders AND persisted as freeze_price so the
            // restart cleanup can release the exact amount later.
            let needs_opposing_price = side == "buy" && price.is_none();
            let best_opposing_price: Option<f64> = if needs_opposing_price {
                if let Some(ref engine) = engine_opt {
                    let eng = engine.lock().unwrap();
                    let levels = eng.get_top_levels(1, engine_side == Side::Sell);
                    levels.first().map(|(p, _)| rules.ticks_to_price(*p))
                } else if state.aeron_cmd_tx.is_some() {
                    best_opposing_from_depth(state, &symbol, &side)
                } else {
                    None
                }
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
                use crate::transport::{pack_str16, AeronCmd, OrderMeta};

                // PERF DIAG (off by default): sample ~1/256 PlaceOrders per-section timing.
                // Enable via WS_PLACE_PROFILE=1 env on desk-server.
                let dbg = ws_place_profile_enabled();
                let t0 = if dbg {
                    Some(std::time::Instant::now())
                } else {
                    None
                };
                // Skip the per-place balance_update push by default — it adds
                // a second WS frame per place to the client, which would
                // double the queued-frame drain cost on the client's NEXT
                // place read. Keep gated for backwards-compat clients that
                // expect the snapshot.
                let push_bal = ws_push_bal_enabled();

                // Reject unknown symbols immediately — before sending order_submitted —
                // so clients don't track phantom orders waiting for a response that
                // arrives as order_rejected after the pending entry is gone.
                if !state.valid_symbols.is_empty() && !state.valid_symbols.contains(&symbol) {
                    return Some(ws_sbe::encode_order_rejected(0, &format!("No engine for symbol: {}", symbol)));
                }

                let (freeze_asset, freeze_amount): (&str, f64) = if side == "buy" {
                    let amount = freeze_price_val * qty;
                    if amount <= 0.0 {
                        return Some(ws_sbe::encode_order_rejected(0, "Unable to determine buy reservation price"));
                    }
                    (quote_asset, amount)
                } else {
                    (base_asset, qty)
                };
                let t_pre_freeze = t0.map(|_| std::time::Instant::now());
                if !try_freeze_cache(&state.account_cache, user_id, freeze_asset, freeze_amount) {
                    return Some(ws_sbe::encode_order_rejected(0, "Insufficient balance"));
                }
                let t_post_freeze = t0.map(|_| std::time::Instant::now());
                if push_bal {
                    if let Some(user_assets) = state.account_cache.get(&user_id) {
                        if let Some(&(bal, frz)) = user_assets.get(freeze_asset) {
                            if let Some(tx) = state.user_tx.get(user_id) {
                                let _ = tx.try_send((ws_sbe::encode_balance_update(
                                    freeze_asset, bal, bal - frz, frz,
                                ), 0));
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
                    OrderMeta {
                        user_id,
                        symbol: sym_bytes,
                        side: side_byte,
                        order_type: pack_str16(&order_type),
                        price,
                        qty,
                        client_order_id: client_order_id.clone(),
                        freeze_price: freeze_price_val,
                    },
                );

                let queue_too_deep = aeron_cmd_latency_cap()
                    .map(|cap| aeron_cmd_tx.len() >= cap)
                    .unwrap_or(false);
                if queue_too_deep || aeron_cmd_tx.push(AeronCmd::NewOrder(sbe_req)).is_err() {
                    // ArrayQueue full — apply backpressure to the client.
                    state.pending_meta.remove(&order_id);
                    release_cache_frozen(
                        &state.account_cache,
                        user_id,
                        freeze_asset,
                        freeze_amount,
                    );
                    return Some(ws_sbe::encode_order_rejected(0, "system busy"));
                }
                // if let Some(ref t) = state.tracer {
                //     t.record_sym(MS_CMD_RING_PUSHED, order_id, &sym_bytes);
                // }
                let t_post_aeron = t0.map(|_| std::time::Instant::now());

                let reply = if ws_inline_order_submitted_enabled() {
                    Some(ws_sbe::encode_order_submitted(order_id, order_id, unix_now()))
                } else {
                    None
                };
                if let (
                    Some(start),
                    Some(pre_freeze),
                    Some(post_freeze),
                    Some(post_bal),
                    Some(post_aeron),
                ) = (t0, t_pre_freeze, t_post_freeze, t_post_balupd, t_post_aeron)
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
                return reply;
            }

            // ── Standalone engine path: DB + freeze + local matching ─────────────
            let now_ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0);

            let engine = match engine_opt {
                Some(e) => e,
                None => {
                    return Some(ws_sbe::encode_order_rejected(0, "No matching engine configured"))
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
                return Some(ws_sbe::encode_order_rejected(0, &format!("DB error: {}", e)));
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
                    return Some(ws_sbe::encode_order_rejected(0, &reason))
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
                        let _ = sqlx::query("DELETE FROM orders WHERE id=$1")
                            .bind(db_order_id)
                            .execute(state.db.as_ref())
                            .await;
                        return Some(ws_sbe::encode_order_rejected(0, &e.to_string()));
                    }
                    Ok((bal, frz)) if bal > 0.0 || frz >= 0.0 => {
                        // Update cache and push WS balance_update from RETURNING values.
                        state
                            .account_cache
                            .entry(user_id)
                            .or_insert_with(std::collections::HashMap::new)
                            .insert(frozen_asset.to_string(), (bal, frz));
                        if let Some(tx) = state.user_tx.get(user_id) {
                            let _ = tx.try_send((ws_sbe::encode_balance_update(
                                frozen_asset, bal, bal - frz, frz,
                            ), 0));
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
                        let _ = sqlx::query("DELETE FROM orders WHERE id = $1")
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
                        return Some(ws_sbe::encode_order_rejected(0, &e.to_string()));
                    }
                }
            };

            // Map engine status to (DB, WS) status strings. IOC/FOK that
            // partially fill come back as Cancelled with filled>0 — the
            // meaningful event for the trader is the partial fill, not the
            // trailing cancel of the remainder, so surface PARTIAL_FILL.
            let db_status = db_status_from_engine(result.status).as_str();
            let filled_qty = rules.lots_to_quantity(result.filled_lots);

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
                return Some(ws_sbe::encode_order_rejected(0, "Rejected by matching engine"));
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
                                    if let Some(tx) = state.user_tx.get(maker_id) {
                                        let _ = tx.try_send((ws_sbe::encode_balance_update(
                                            asset, acc.balance, acc.balance - acc.frozen, acc.frozen,
                                        ), 0));
                                    }
                                }
                            }
                            if let Some(pos) = crate::positions::position_for_user_asset(
                                state.db.as_ref(),
                                maker_id,
                                base_asset,
                            )
                            .await
                            {
                                if let Some(tx) = state.user_tx.get(maker_id) {
                                    let _ = tx.try_send((ws_sbe::encode_position_update(
                                        base_asset, pos.quantity, pos.entry_price,
                                    ), 0));
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
                        let ws_maker_status = maker_ws_status_from_db_status(new_status.as_str());
                        let status_byte = ws_status_to_byte(ws_maker_status);
                        if let Some(tx) = state.user_tx.get(maker_id) {
                            let _ = tx.try_send((ws_sbe::encode_order_update(
                                maker_order_id as u64, 0, status_byte, new_filled, fp, ts,
                            ), 0));
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

                let side_byte: u8 = if side == "buy" { 0 } else { 1 };
                let _ = state.market_fanout.send_owned(ws_sbe::encode_trade(
                    fill_price, total_filled, side_byte, ts, &symbol,
                ));

                // Live ticker/kline/agg updates are produced from trade events,
                // not PostgreSQL.

                broadcast_depth_pub(state, &symbol);

                // Push order_update + balance_update to this user's personal channel.
                let ws_status_byte = ws_status_to_byte(ws_status_from_engine(result.status, filled_qty > 0.0));
                if let Some(tx) = state.user_tx.get(user_id) {
                    let _ = tx.try_send((ws_sbe::encode_order_update(
                        db_order_id as u64, 0, ws_status_byte, total_filled, fill_price, ts,
                    ), 0));
                }

                // Send balance_update so frontend can refresh balances.
                let (debit_asset, credit_asset) = if side == "buy" {
                    (quote_asset, base_asset)
                } else {
                    (base_asset, quote_asset)
                };
                for asset in [debit_asset, credit_asset] {
                    if let Ok(acc) = repo.get_account(user_id, asset).await {
                        if let Some(tx) = state.user_tx.get(user_id) {
                            let _ = tx.try_send((ws_sbe::encode_balance_update(
                                asset, acc.balance, acc.balance - acc.frozen, acc.frozen,
                            ), 0));
                        }
                    }
                }

                // Position update for the base asset whose holdings just changed.
                if let Some(pos) = crate::positions::position_for_user_asset(
                    state.db.as_ref(), user_id, base_asset,
                ).await {
                    if let Some(tx) = state.user_tx.get(user_id) {
                        let _ = tx.try_send((ws_sbe::encode_position_update(
                            base_asset, pos.quantity, pos.entry_price,
                        ), 0));
                    }
                }
            } else if result.status == OrderStatus::Accepted {
                // GTC limit order resting in the book. Frozen funds STAY frozen
                // until it fills or the user cancels — releasing here would
                // double-spend when the maker fills later.
                if let Some(tx) = state.user_tx.get(user_id) {
                    let _ = tx.try_send((ws_sbe::encode_order_update(
                        db_order_id as u64, 0, ws_sbe::WS_STATUS_OPEN, 0.0, 0.0, ts,
                    ), 0));
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
                if let Some(tx) = state.user_tx.get(user_id) {
                    let _ = tx.try_send((ws_sbe::encode_order_update(
                        db_order_id as u64, 0, ws_sbe::WS_STATUS_CANCELED, 0.0, 0.0, ts,
                    ), 0));
                }

                // Refresh frontend balance for the released asset (frozen → available).
                if rel_amount > 0.0 {
                    if let Ok(acc) = repo.get_account(user_id, rel_asset).await {
                        if let Some(tx) = state.user_tx.get(user_id) {
                            let _ = tx.try_send((ws_sbe::encode_balance_update(
                                rel_asset, acc.balance, acc.balance - acc.frozen, acc.frozen,
                            ), 0));
                        }
                    }
                }
            }

            let side_byte: u8 = if side == "buy" { 0 } else { 1 };
            Some(ws_sbe::encode_order_accepted(
                db_order_id as u64, 0, price.unwrap_or(0.0), qty, side_byte, &symbol, ts,
            ))
        }

        ClientMsg::CancelSymbol { symbol } => {
            let user_id = match session.user_id {
                Some(id) => id,
                None => return Some(ws_sbe::encode_error("Not authenticated")),
            };
            // Run in background so the WS handler is not blocked on DB operations.
            // This lets queued place_order messages be processed immediately.
            let state2 = state.clone();
            let sym2 = symbol.clone();
            let ptx2 = personal_tx.clone();
            tokio::spawn(async move {
                bulk_cancel(user_id, Some(&sym2), &state2, &ptx2).await;
            });
            Some(ws_sbe::encode_cancel_all_ok(0))
        }

        ClientMsg::CancelAll => {
            let user_id = match session.user_id {
                Some(id) => id,
                None => return Some(ws_sbe::encode_error("Not authenticated")),
            };
            let n = bulk_cancel(user_id, None, state, &personal_tx).await;
            Some(ws_sbe::encode_cancel_all_ok(n as u32))
        }

        ClientMsg::CancelOrder { order_id } => {
            let user_id = match session.user_id {
                Some(id) => id,
                None => return Some(ws_sbe::encode_error("Not authenticated")),
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
                        .push(crate::transport::AeronCmd::Cancel(cancel_req))
                        .is_err()
                    {
                        return Some(ws_sbe::encode_error("system busy"));
                    }
                    return Some(ws_sbe::encode_cancel_submitted(order_id as u64, unix_now()));
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
                    Ok(None) => return Some(ws_sbe::encode_error("Order not found or already closed")),
                    Err(e) => return Some(ws_sbe::encode_error(&e.to_string())),
                };

            use sqlx::Row;
            let symbol: String = order_row.get("symbol");
            let side: String = order_row.get("side");
            let price: Option<f64> = order_row.get("price");
            let quantity: f64 = order_row.get("quantity");
            let filled: f64 = order_row.get("filled");
            let remaining = quantity - filled;

            // Update DB status.
            let db_result = sqlx::query("DELETE FROM orders WHERE id = $1")
                .bind(order_id)
                .execute(state.db.as_ref())
                .await;

            if let Err(e) = db_result {
                return Some(ws_sbe::encode_error(&e.to_string()));
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
                let _ = aeron_cmd_tx.push(crate::transport::AeronCmd::Cancel(cancel_req));
            }

            // Release frozen funds for the unfilled portion.
            let (base_asset, quote_asset) = symbol.split_once('_').unwrap_or(("BTC", "USDT"));
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
            for asset in [
                released_asset,
                if released_asset == base_asset { quote_asset } else { base_asset },
            ] {
                if let Ok(acc) = repo.get_account(user_id, asset).await {
                    if let Some(tx) = state.user_tx.get(user_id) {
                        let _ = tx.try_send((ws_sbe::encode_balance_update(
                            &acc.asset, acc.balance, acc.balance - acc.frozen, acc.frozen,
                        ), 0));
                    }
                }
            }

            Some(ws_sbe::encode_order_update(order_id as u64, 0, ws_sbe::WS_STATUS_CANCELED, 0.0, 0.0, unix_now()))
        }

        ClientMsg::PlaceOrders { batch_id, orders } => {
            let user_id = match session.user_id {
                Some(id) => id,
                None => return Some(ws_sbe::encode_error("Not authenticated")),
            };

            // Cap batch at 40 orders.
            let orders = if orders.len() > 40 {
                &orders[..40]
            } else {
                &orders[..]
            };

            // ── Aeron fast path: parse all → parallel DB freeze → batch-publish ──────────
            // All DB freezes run concurrently (bids freeze USDT, asks freeze BTC — different
            // rows so they don't contend). Then all N SBE requests are published in one Aeron
            // write so the engine sees the full batch before the next 10ms depth snapshot.
            if state.aeron_cmd_tx.is_some() {
                use crate::sbe::NewOrderRequest as SbeNewOrder;
                use crate::transport::{pack_str16, OrderMeta};

                struct V {
                    order_id: u64,
                    freeze_asset: String,
                    freeze_amount: f64,
                    sbe_req: SbeNewOrder,
                    meta: OrderMeta,  // used for pending_meta.insert on freeze success
                }

                // (client_order_id_num, order_id, status_byte): collected in order for encode_orders_placed
                let mut sbe_results: Vec<(u64, u64, u8)> = Vec::with_capacity(orders.len());
                let mut validated: Vec<V> = Vec::with_capacity(orders.len());

                for order_val in orders.iter() {
                    let coid = order_val
                        .get("client_order_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_owned();

                    macro_rules! rej {
                        ($r:expr) => {{ let _ = $r; sbe_results.push((0, 0, ws_sbe::WS_STATUS_REJECTED)); continue; }};
                    }

                    let symbol = match order_val.get("symbol").and_then(|v| v.as_str()) {
                        Some(s) => s.to_owned(),
                        None => rej!("Missing symbol"),
                    };
                    let side = match order_val.get("side").and_then(|v| v.as_str()) {
                        Some(s) => s.to_owned(),
                        None => rej!("Missing side"),
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
                        None => rej!("Missing qty"),
                    };
                    let engine_side = match side.as_str() {
                        "buy" => Side::Buy,
                        "sell" => Side::Sell,
                        _ => rej!("Invalid side"),
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

                    let (base_asset, quote_asset) = symbol.split_once('_').unwrap_or(("BTC", "USDT"));

                    let best_opposing_price = best_opposing_from_depth(state, &symbol, &side);
                    let freeze_price_val: f64 = if side == "buy" {
                        price.or(best_opposing_price).unwrap_or(0.0)
                    } else {
                        0.0
                    };

                    let (freeze_asset, freeze_amount) = if side == "buy" {
                        let amount = freeze_price_val * qty;
                        if amount <= 0.0 {
                            rej!("Unable to determine buy reservation price");
                        }
                        (quote_asset.to_owned(), amount)
                    } else {
                        (base_asset.to_owned(), qty)
                    };

                    let order_id = state.next_order_id.fetch_add(1, Ordering::Relaxed);
                    let mut sym_bytes = [0u8; 16];
                    let sb = symbol.as_bytes();
                    sym_bytes[..sb.len().min(16)].copy_from_slice(&sb[..sb.len().min(16)]);
                    let side_byte: u8 = if engine_side == Side::Buy { 0 } else { 1 };
                    let tif_byte: u8 = match tif {
                        TimeInForce::GTC => 0,
                        TimeInForce::IOC => 1,
                        TimeInForce::FOK => 2,
                        TimeInForce::PostOnly => 3,
                    };

                    let meta = OrderMeta {
                        user_id,
                        symbol: sym_bytes,
                        side: side_byte,
                        order_type: pack_str16(&order_type),
                        price,
                        qty,
                        client_order_id: coid.clone(),
                        freeze_price: freeze_price_val,
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
                    validated.push(V {
                        order_id,
                        freeze_asset,
                        freeze_amount,
                        sbe_req,
                        meta,
                    });
                }

                // Phase 2 + 3: in-memory freeze check + build aeron_batch (no DB on hot path).
                let mut aeron_batch: smallvec::SmallVec<[SbeNewOrder; 32]> =
                    smallvec::SmallVec::new();
                for v in validated {
                    if try_freeze_cache(
                        &state.account_cache,
                        user_id,
                        &v.freeze_asset,
                        v.freeze_amount,
                    ) {
                        if let Some(user_assets) = state.account_cache.get(&user_id) {
                            if let Some(&(bal, frz)) = user_assets.get(v.freeze_asset.as_str()) {
                                if let Some(tx) = state.user_tx.get(user_id) {
                                    let _ = tx.try_send((ws_sbe::encode_balance_update(
                                        &v.freeze_asset, bal, bal - frz, frz,
                                    ), 0));
                                }
                            }
                        }
                        let order_id = v.order_id;
                        sbe_results.push((order_id, order_id, ws_sbe::WS_STATUS_OPEN));
                        state.pending_meta.insert(v.order_id, v.meta);
                        aeron_batch.push(v.sbe_req);
                    } else {
                        sbe_results.push((0, 0, ws_sbe::WS_STATUS_REJECTED));
                    }
                }

                if !aeron_batch.is_empty() {
                    if let Some(ref tx) = state.aeron_cmd_tx {
                        if tx
                            .push(crate::transport::AeronCmd::BatchNewOrder(aeron_batch))
                            .is_err()
                        {
                            // Bounded mpsc full → reject the whole batch.
                            // Better to refuse here than to drop silently
                            // and leave the client expecting acks.
                            return Some(ws_sbe::encode_error("system busy — aeron command queue full"));
                        }
                    }
                }

                return Some(ws_sbe::encode_orders_placed(&batch_id, &sbe_results));
            }

            // ── Standalone engine path ─────────────────────────────────────────────────────
            let mut sa_results: Vec<(u64, u64, u8)> = Vec::with_capacity(orders.len());

            for order_val in orders {
                let coid = order_val
                    .get("client_order_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_owned();
                macro_rules! sa_rej {
                    ($r:expr) => {{ let _ = ($r, &coid); sa_results.push((0, 0, ws_sbe::WS_STATUS_REJECTED)); continue; }};
                }
                let symbol = match order_val.get("symbol").and_then(|v| v.as_str()) {
                    Some(s) => s.to_owned(),
                    None => sa_rej!("Missing symbol"),
                };
                let side = match order_val.get("side").and_then(|v| v.as_str()) {
                    Some(s) => s.to_owned(),
                    None => sa_rej!("Missing side"),
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
                    None => sa_rej!("Missing qty"),
                };

                let engine_side = match side.as_str() {
                    "buy" => Side::Buy,
                    "sell" => Side::Sell,
                    _ => sa_rej!("Invalid side"),
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
                    _ => sa_rej!("Unknown order type"),
                };

                // ── Standalone engine path ───────────────────────────────────
                let engine_opt: Option<Arc<Mutex<MatchingEngine>>> = state
                    .engines
                    .as_ref()
                    .and_then(|m| m.get(&symbol).map(|e| e.value().clone()));
                if state.engines.is_some() && engine_opt.is_none() {
                    sa_rej!(format!("Unknown symbol: {}", symbol));
                }
                let engine = match engine_opt {
                    Some(e) => e,
                    None => sa_rej!("No matching engine configured"),
                };

                let now_ns = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos() as u64)
                    .unwrap_or(0);

                let (base_asset, quote_asset) = symbol.split_once('_').unwrap_or(("BTC", "USDT"));
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
                    let _ = e;
                    sa_results.push((0, 0, ws_sbe::WS_STATUS_REJECTED));
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
                    Err(_reason) => {
                        sa_results.push((0, 0, ws_sbe::WS_STATUS_REJECTED));
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
                        repo.freeze_for_buy(user_id, quote_asset, freeze_amount)
                            .await
                    } else {
                        Ok((0.0, 0.0))
                    }
                } else {
                    repo.freeze_for_sell(user_id, base_asset, qty).await
                };

                let frozen_asset = if side == "buy" { quote_asset } else { base_asset };
                match freeze_result {
                    Err(_e) => {
                        let _ = sqlx::query("DELETE FROM orders WHERE id=$1")
                            .bind(db_order_id)
                            .execute(state.db.as_ref())
                            .await;
                        sa_results.push((0, 0, ws_sbe::WS_STATUS_REJECTED));
                        continue;
                    }
                    Ok((bal, frz)) if bal > 0.0 || frz >= 0.0 => {
                        state
                            .account_cache
                            .entry(user_id)
                            .or_insert_with(std::collections::HashMap::new)
                            .insert(frozen_asset.to_string(), (bal, frz));
                        if let Some(tx) = state.user_tx.get(user_id) {
                            let _ = tx.try_send((ws_sbe::encode_balance_update(
                                frozen_asset, bal, bal - frz, frz,
                            ), 0));
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
                    Err(_e) => {
                        let _ = sqlx::query("DELETE FROM orders WHERE id = $1")
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
                        sa_results.push((0, 0, ws_sbe::WS_STATUS_REJECTED));
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
                    sa_results.push((0, 0, ws_sbe::WS_STATUS_REJECTED));
                    continue;
                }

                sa_results.push((db_order_id as u64, db_order_id as u64, ws_sbe::WS_STATUS_OPEN));
            }

            Some(ws_sbe::encode_orders_placed(&batch_id, &sa_results))
        }

        ClientMsg::BatchCancel { order_ids } => {
            let user_id = match session.user_id {
                Some(id) => id,
                None => return Some(ws_sbe::encode_error("Not authenticated")),
            };
            if order_ids.is_empty() {
                return Some(ws_sbe::encode_batch_cancel_ok(0));
            }
            let n = batch_cancel_by_ids(user_id, &order_ids, state, &personal_tx).await;
            Some(ws_sbe::encode_batch_cancel_ok(n as u32))
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
    personal_tx: &tokio::sync::mpsc::Sender<(Vec<u8>, u64)>,
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
                .push(crate::transport::AeronCmd::BatchCancel(reqs))
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
    let _ =
        sqlx::query("DELETE FROM orders WHERE id = ANY($1) AND status IN ('PENDING','TRADING')")
            .bind(&ids[..])
            .execute(state.db.as_ref())
            .await;

    let repo = crate::account_repository::AccountRepository::new(state.db.as_ref());
    let ts = unix_now();
    let count = rows.len();

    for order in rows {
        let remaining = (order.quantity - order.filled).max(0.0);
        let (base_asset, quote_asset) = order.symbol.split_once('_').unwrap_or(("BTC", "USDT"));

        if order.side == "buy" {
            if order.freeze_price > 0.0 && remaining > 0.0 {
                let _ = repo
                    .release_frozen(user_id, quote_asset, order.freeze_price * remaining)
                    .await;
            }
        } else if remaining > 0.0 {
            let _ = repo.release_frozen(user_id, base_asset, remaining).await;
        }

        let _ = personal_tx.try_send((ws_sbe::encode_order_update(
            order.id as u64, 0, ws_sbe::WS_STATUS_CANCELED, order.filled, 0.0, ts,
        ), 0));
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
    personal_tx: &tokio::sync::mpsc::Sender<(Vec<u8>, u64)>,
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
                .push(crate::transport::AeronCmd::BatchCancel(reqs))
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
    let _ =
        sqlx::query("DELETE FROM orders WHERE id = ANY($1) AND status IN ('PENDING','TRADING')")
            .bind(&ids[..])
            .execute(state.db.as_ref())
            .await;

    let repo = crate::account_repository::AccountRepository::new(state.db.as_ref());
    let ts = unix_now();
    let count = rows.len();

    for order in rows {
        let remaining = (order.quantity - order.filled).max(0.0);

        // Release frozen funds for the unfilled portion.
        let (base_asset, quote_asset) = order.symbol.split_once('_').unwrap_or(("BTC", "USDT"));

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
        let _ = personal_tx.try_send((ws_sbe::encode_order_update(
            order.id as u64, 0, ws_sbe::WS_STATUS_CANCELED, order.filled, 0.0, ts,
        ), 0));
    }

    count
}

/// Build a depth snapshot SBE frame for the given symbol from the matching engine.
fn build_depth_sbe(engine: &Mutex<MatchingEngine>, symbol: &str) -> Vec<u8> {
    let rules = crate::symbol_rules::SymbolRules::for_symbol(symbol);
    let (raw_bids, raw_asks) = {
        let eng = engine.lock().unwrap();
        (eng.get_top_levels(10, true), eng.get_top_levels(10, false))
    };
    let bids: Vec<(f64, f64)> = raw_bids
        .into_iter()
        .filter(|(_, q)| *q > 0)
        .map(|(p, q)| (rules.ticks_to_price(p), rules.lots_to_quantity(q)))
        .collect();
    let asks: Vec<(f64, f64)> = raw_asks
        .into_iter()
        .filter(|(_, q)| *q > 0)
        .map(|(p, q)| (rules.ticks_to_price(p), rules.lots_to_quantity(q)))
        .collect();
    ws_sbe::encode_depth(unix_now(), symbol, &bids, &asks)
}

/// Build a depth snapshot JSON for the given symbol from the matching engine.
/// Kept for unit tests only.
#[cfg(test)]
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
    serde_json::json!({
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
                .send_owned(build_depth_sbe(engine.value(), symbol));
        }
    } else if let Some(depth_bytes) = state.last_depth.get(symbol) {
        // last_depth stores SBE bytes — send directly.
        state.market_fanout.send_bytes(depth_bytes.clone());
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

    loop {
        tokio::select! {
            _ = depth_interval.tick() => {
                if state.market_fanout.subscriber_count() == 0 {
                    continue;
                }
                // Only broadcast depth from local engines in standalone mode.
                // In Aeron mode, depth is pushed by exchange_engine via Aeron.
                if let Some(ref engines) = state.engines {
                    for (symbol, bids, asks) in snapshot_all_engines(engines) {
                        let msg = ws_sbe::encode_depth(unix_now(), &symbol, &bids, &asks);
                        let _ = state.market_fanout.send_owned(msg);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{build_depth_json, ClientMsg};
    use crate::{MatchingEngine, Order, PoolConfig, Side, TimeInForce};
    use serde_json::Value;
    use std::sync::Mutex;

    fn empty_engine() -> Mutex<MatchingEngine> {
        Mutex::new(MatchingEngine::new(PoolConfig::default()).unwrap())
    }

    #[test]
    fn parse_place_order_fast_handles_pressure_shape() {
        let msg = ClientMsg::parse(
            r#"{"type":"place_order","client_order_id":"p7-11","symbol":"BTC_USDT","side":"buy","order_type":"limit","price":5000.0,"qty":0.001}"#,
        )
        .unwrap();
        match msg {
            ClientMsg::PlaceOrder {
                client_order_id,
                symbol,
                side,
                order_type,
                price,
                qty,
                ..
            } => {
                assert_eq!(client_order_id, "p7-11");
                assert_eq!(symbol, "BTC_USDT");
                assert_eq!(side, "buy");
                assert_eq!(order_type, "limit");
                assert_eq!(price, Some(5000.0));
                assert_eq!(qty, 0.001);
            }
            _ => panic!("expected place_order"),
        }
    }

    #[test]
    fn parse_cancel_order_fast_handles_pressure_shape() {
        let msg = ClientMsg::parse(r#"{"type":"cancel_order","order_id":12345}"#).unwrap();
        match msg {
            ClientMsg::CancelOrder { order_id } => assert_eq!(order_id, 12345),
            _ => panic!("expected cancel_order"),
        }
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
