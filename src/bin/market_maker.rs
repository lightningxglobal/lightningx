/// market-maker: professional two-sided market maker for LightningX.
///
/// Fair value: Binance real-time depth snapshot (best bid/ask mid-price).
/// Quoting:    N bid + N ask levels, computed from mid with configurable spread.
/// Risk:       inventory skew shifts quotes to reduce position; hard limit stops quoting.
/// Lifecycle:  cancel-ALL → wait confirm → place-ALL on each re-quote cycle.
///             Fills (full and partial) tracked; P&L reported on each cycle.
/// Auth:       static API key sent over WebSocket — no REST login required.
use std::collections::{HashMap, HashSet};
use std::time::Duration;

use fastwebsockets::{handshake, Frame, OpCode, Payload, WebSocket};
use http_body_util::Empty;
use hyper::{
    body::Bytes,
    header::{CONNECTION, UPGRADE},
    Request,
};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tracing::{info, warn};

// ── WS connection helpers ─────────────────────────────────────────────────────

struct SpawnExecutor;
impl<Fut> hyper::rt::Executor<Fut> for SpawnExecutor
where
    Fut: std::future::Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    fn execute(&self, fut: Fut) {
        tokio::spawn(fut);
    }
}

fn parse_ws_url(url: &str) -> (bool, String, u16, String) {
    let tls = url.starts_with("wss://");
    let rest = if tls { &url[6..] } else { &url[5..] };
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], rest[i..].to_string()),
        None => (rest, "/".to_string()),
    };
    let (host, port) = if let Some(colon) = authority.rfind(':') {
        let port = authority[colon + 1..].parse().unwrap_or(if tls { 443 } else { 80 });
        (authority[..colon].to_string(), port)
    } else {
        (authority.to_string(), if tls { 443 } else { 80 })
    };
    (tls, host, port, path)
}

fn ws_upgrade_request(host: &str, path: &str) -> anyhow::Result<Request<Empty<Bytes>>> {
    Ok(Request::builder()
        .method("GET")
        .uri(path)
        .header("Host", host)
        .header(CONNECTION, "upgrade")
        .header(UPGRADE, "websocket")
        .header("Sec-WebSocket-Key", handshake::generate_key())
        .header("Sec-WebSocket-Version", "13")
        .body(Empty::new())?)
}

async fn ws_connect_plain(url: &str) -> anyhow::Result<WebSocket<TokioIo<hyper::upgrade::Upgraded>>> {
    let (_, host, port, path) = parse_ws_url(url);
    let tcp = TcpStream::connect(format!("{host}:{port}")).await?;
    let req = ws_upgrade_request(&host, &path)?;
    let (ws, _) = handshake::client(&SpawnExecutor, req, tcp).await?;
    Ok(ws)
}

async fn ws_connect_tls(url: &str) -> anyhow::Result<WebSocket<TokioIo<hyper::upgrade::Upgraded>>> {
    use std::sync::Arc;
    use tokio_rustls::rustls;
    let _ = rustls::crypto::ring::default_provider().install_default();
    let (_, host, port, path) = parse_ws_url(url);
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_native_certs::load_native_certs().unwrap_or_default() {
        let _ = roots.add(cert);
    }
    let config = Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    );
    let connector = tokio_rustls::TlsConnector::from(config);
    let server_name = rustls::pki_types::ServerName::try_from(host.clone())
        .map_err(|e| anyhow::anyhow!("invalid server name: {e}"))?;
    let tcp = TcpStream::connect(format!("{host}:{port}")).await?;
    let tls = connector.connect(server_name, tcp).await?;
    let req = ws_upgrade_request(&host, &path)?;
    let (ws, _) = handshake::client(&SpawnExecutor, req, tls).await?;
    Ok(ws)
}

// ── Configuration ─────────────────────────────────────────────────────────────

fn exchange_url() -> String {
    std::env::var("EXCHANGE_URL").unwrap_or_else(|_| "http://localhost:4003".to_string())
}

fn exchange_ws_url() -> String {
    let base = exchange_url();
    if base.starts_with("https://") {
        format!("{}/ws", base.replacen("https://", "wss://", 1))
    } else {
        format!("{}/ws", base.replacen("http://", "ws://", 1))
    }
}

const BINANCE_WS_BASE: &str = "wss://fstream.binance.com/ws";
/// API key sent to the exchange WS on every connect. Configure via ROBOT_API_KEY env var.
fn robot_api_key() -> String {
    std::env::var("ROBOT_API_KEY").unwrap_or_else(|_| "robot_ak_2026_lightningx".to_string())
}
/// How long to wait for cancel confirmations before forcing placement (engine-restart safety).
const CANCEL_TIMEOUT: Duration = Duration::from_millis(800);

/// Per-symbol quoting parameters — all tunable without code changes.
struct SymbolConfig {
    /// Our exchange symbol (e.g. "BTC_USDT").
    symbol: &'static str,
    /// Binance futures stream.  bookTicker gives real-time best bid/ask.
    binance_stream: &'static str,
    /// Number of bid + ask levels to maintain simultaneously.
    num_levels: usize,
    /// Base quantity per level (in base currency).
    qty_per_level: f64,
    /// Minimum allowed order quantity.
    min_qty: f64,
    /// Price rounding tick (e.g. 0.1 for BTC/USDT).
    price_tick: f64,
    /// Half-spread for the innermost (tightest) level, in basis points.
    /// Inner bid = mid * (1 - inner_half_spread), inner ask = mid * (1 + inner_half_spread).
    inner_half_spread_bps: f64,
    /// Additional bps widening per level further from mid.
    level_spacing_bps: f64,
    /// Hard position limit (base currency, both long and short).
    max_position: f64,
    /// Inventory skew: shifts the entire spread by this many bps per unit of
    /// net long inventory.  Long → shift down (buy less, sell more).
    skew_bps_per_unit: f64,
    /// Re-quote threshold: only trigger cancel-replace when any quote price
    /// has drifted by more than this many bps from its target.
    requote_threshold_bps: f64,
}

const SYMBOLS: &[SymbolConfig] = &[SymbolConfig {
    symbol: "BTC_USDT",
    binance_stream: "btcusdt@depth10@100ms",
    num_levels: 10,
    qty_per_level: 0.001,
    min_qty: 0.0001,
    price_tick: 0.1,
    inner_half_spread_bps: 0.1,
    level_spacing_bps: 0.5,
    max_position: 5.0,
    skew_bps_per_unit: 1.0,
    requote_threshold_bps: 0.05,
}];

// ── Exchange message types ────────────────────────────────────────────────────

#[derive(Deserialize)]
struct DepthUpdate {
    #[serde(rename = "b")]
    bids: Vec<[String; 2]>,
    #[serde(rename = "a")]
    asks: Vec<[String; 2]>,
}

// ── Quote state machine ───────────────────────────────────────────────────────

enum QuoteState {
    Idle,
    /// Cancel requests sent; waiting for all CANCELED acks before placing.
    Cancelling {
        to_confirm: HashSet<i64>,
        next_targets: Vec<TargetLevel>,
        started_at: std::time::Instant,
    },
    /// New batch sent; tracking individual OPEN (ACCEPTED) events per client_order_id.
    /// Binance depth events that arrive while placing are ignored — the next depth
    /// after transitioning back to Idle will re-evaluate via within_threshold.
    Placing {
        /// client_order_id → (side, price, qty) for each unconfirmed order.
        awaiting: HashMap<String, (&'static str, f64, f64)>,
    },
}

fn make_cancel_msg(ids: &[i64]) -> String {
    json!({"type": "batch_cancel", "order_ids": ids}).to_string()
}

/// Process-start nanoseconds used as a session prefix on every client_order_id
/// so coids stay globally unique across MM restarts. The DB has a UNIQUE
/// constraint on (user_id, client_order_id); without this prefix, a fresh MM
/// session reusing "0", "1", "2"... silently fails to INSERT the order row,
/// which later breaks the trades FK.
static SESSION_PREFIX: std::sync::OnceLock<String> = std::sync::OnceLock::new();

fn session_prefix() -> &'static str {
    SESSION_PREFIX.get_or_init(|| {
        let ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        format!("mm{}", ns)
    })
}

/// Returns the WS message string and a map of client_order_id → (side, price, qty)
/// used to correlate individual ACCEPTED ("OPEN") events back to our quotes.
fn make_place_msg(
    symbol: &str,
    targets: &[TargetLevel],
    order_counter: &mut u64,
) -> (String, HashMap<String, (&'static str, f64, f64)>) {
    let prefix = session_prefix();
    let mut orders = Vec::with_capacity(targets.len() * 2);
    let mut awaiting: HashMap<String, (&'static str, f64, f64)> =
        HashMap::with_capacity(targets.len() * 2);
    for t in targets {
        let bid_coid = format!("{prefix}-{}", *order_counter);
        *order_counter += 1;
        let ask_coid = format!("{prefix}-{}", *order_counter);
        *order_counter += 1;
        orders.push(json!({
            "client_order_id": bid_coid,
            "symbol": symbol,
            "side": "buy",
            "order_type": "limit",
            "price": t.bid_price,
            "qty": t.bid_qty,
        }));
        awaiting.insert(bid_coid, ("buy", t.bid_price, t.bid_qty));
        orders.push(json!({
            "client_order_id": ask_coid,
            "symbol": symbol,
            "side": "sell",
            "order_type": "limit",
            "price": t.ask_price,
            "qty": t.ask_qty,
        }));
        awaiting.insert(ask_coid, ("sell", t.ask_price, t.ask_qty));
    }
    (json!({"type": "place_orders", "orders": orders}).to_string(), awaiting)
}

// ── Exchange WS message handler ───────────────────────────────────────────────

/// Called when an order is definitively gone (CANCELED/FILLED/REJECTED).
/// Removes it from the Cancelling to_confirm set; when the set empties, places next batch.
fn confirm_cancel(
    order_id: i64,
    state: &mut QuoteState,
    book: &mut QuoteBook,
    cfg: &'static SymbolConfig,
    order_counter: &mut u64,
) -> Option<String> {
    let done = if let QuoteState::Cancelling { to_confirm, .. } = state {
        to_confirm.remove(&order_id);
        to_confirm.is_empty()
    } else {
        false
    };
    if !done {
        return None;
    }
    let targets = match std::mem::replace(state, QuoteState::Idle) {
        QuoteState::Cancelling { next_targets, .. } => next_targets,
        _ => return None,
    };
    if targets.is_empty() {
        return None;
    }
    trigger_requote(targets, book, state, cfg, order_counter)
}

/// Returns `(continue, message_to_send)`.  `continue=false` drops the connection.
fn on_exch_msg(
    text: &str,
    book: &mut QuoteBook,
    inv: &mut Inventory,
    balance: &mut HashMap<String, f64>,
    state: &mut QuoteState,
    cfg: &'static SymbolConfig,
    order_counter: &mut u64,
) -> (bool, Option<String>) {
    let v: Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => return (true, None),
    };
    let msg_type = match v.get("type").and_then(|t| t.as_str()) {
        Some(t) => t,
        None => return (true, None),
    };
    match msg_type {
        "order_update" => {
            let status = match v.get("status").and_then(|s| s.as_str()) {
                Some(s) => s,
                None => { warn!("[{}] order_update missing status", cfg.symbol); return (true, None); }
            };
            let order_id = match v.get("order_id").and_then(|i| i.as_i64()) {
                Some(id) => id,
                None => { warn!("[{}] order_update missing order_id", cfg.symbol); return (true, None); }
            };
            let client_order_id = v.get("client_order_id").and_then(|c| c.as_str()).map(|s| s.to_owned());
            match status {
                "OPEN" => {
                    // ACCEPTED: move from awaiting → active book.
                    let coid = match client_order_id {
                        Some(c) => c,
                        None => { warn!("[{}] OPEN missing client_order_id for order {}", cfg.symbol, order_id); return (true, None); }
                    };
                    if let QuoteState::Placing { awaiting } = state {
                        if let Some((side, price, qty)) = awaiting.remove(&coid) {
                            if side == "buy" {
                                book.bids.push(Quote { order_id, side: "buy", price, qty, filled: 0.0 });
                                book.bids.sort_by(|a, b| b.price.total_cmp(&a.price));
                            } else {
                                book.asks.push(Quote { order_id, side: "sell", price, qty, filled: 0.0 });
                                book.asks.sort_by(|a, b| a.price.total_cmp(&b.price));
                            }
                        }
                        if awaiting.is_empty() {
                            *state = QuoteState::Idle;
                            info!("[{}] all quotes active: {}b {}a", cfg.symbol, book.bids.len(), book.asks.len());
                        }
                    }
                    (true, None)
                }
                "PARTIALLY_FILLED" => {
                    // Server does not send `side` — look it up from the book (order must be there).
                    let side_str = match book.bids.iter().find(|q| q.order_id == order_id).map(|q| q.side)
                        .or_else(|| book.asks.iter().find(|q| q.order_id == order_id).map(|q| q.side))
                    {
                        Some(s) => s,
                        None => { warn!("[{}] PARTIALLY_FILLED order {} not in book", cfg.symbol, order_id); return (true, None); }
                    };
                    let filled_qty = match v.get("filled_qty").and_then(|f| f.as_f64()) {
                        Some(f) => f,
                        None => { warn!("[{}] PARTIALLY_FILLED missing filled_qty for order {}", cfg.symbol, order_id); return (true, None); }
                    };
                    let avg_price = match v.get("avg_price").and_then(|p| p.as_f64()) {
                        Some(p) => p,
                        None => { warn!("[{}] PARTIALLY_FILLED missing avg_price for order {}", cfg.symbol, order_id); return (true, None); }
                    };
                    inv.apply_fill(order_id, side_str, avg_price, filled_qty, false);
                    if let Some(q) = book.find_mut(order_id) { q.filled = filled_qty; }
                    (true, None)
                }
                "FILLED" => {
                    // Server does not send `side` — look up from book, then awaiting (immediate fill).
                    let side_str = book.bids.iter().find(|q| q.order_id == order_id).map(|q| q.side)
                        .or_else(|| book.asks.iter().find(|q| q.order_id == order_id).map(|q| q.side))
                        .or_else(|| {
                            if let QuoteState::Placing { awaiting } = state {
                                client_order_id.as_deref()
                                    .and_then(|coid| awaiting.get(coid))
                                    .map(|(side, _, _)| *side)
                            } else { None }
                        });
                    let side_str = match side_str {
                        Some(s) => s,
                        None => { warn!("[{}] FILLED order {} unknown (not in book or awaiting)", cfg.symbol, order_id); return (true, None); }
                    };
                    let filled_qty = match v.get("filled_qty").and_then(|f| f.as_f64()) {
                        Some(f) => f,
                        None => { warn!("[{}] FILLED missing filled_qty for order {}", cfg.symbol, order_id); return (true, None); }
                    };
                    let avg_price = match v.get("avg_price").and_then(|p| p.as_f64()) {
                        Some(p) => p,
                        None => { warn!("[{}] FILLED missing avg_price for order {}", cfg.symbol, order_id); return (true, None); }
                    };
                    inv.apply_fill(order_id, side_str, avg_price, filled_qty, true);
                    // Remove from awaiting if this order filled before we saw its OPEN ack.
                    let placing_done = if let QuoteState::Placing { awaiting } = state {
                        if let Some(coid) = &client_order_id {
                            awaiting.remove(coid);
                        }
                        awaiting.is_empty()
                    } else { false };
                    book.remove(order_id);
                    if placing_done {
                        // All pending acks resolved — go Idle; next depth will re-evaluate.
                        *state = QuoteState::Idle;
                        return (true, None);
                    }
                    // Filled while cancel was in flight — counts as a cancel confirmation.
                    (true, confirm_cancel(order_id, state, book, cfg, order_counter))
                }
                "CANCELED" => {
                    book.remove(order_id);
                    inv.order_fill_tracker.remove(&order_id);
                    (true, confirm_cancel(order_id, state, book, cfg, order_counter))
                }
                "REJECTED" => {
                    warn!("[{}] order {} REJECTED", cfg.symbol, order_id);
                    book.remove(order_id);
                    inv.order_fill_tracker.remove(&order_id);
                    // New-order rejected: remove from awaiting, check if entire batch is resolved.
                    let placing_done = if let QuoteState::Placing { awaiting } = state {
                        if let Some(coid) = &client_order_id {
                            awaiting.remove(coid);
                        }
                        awaiting.is_empty()
                    } else { false };
                    if placing_done {
                        *state = QuoteState::Idle;
                        return (true, None);
                    }
                    // Cancel-REJECTED (order_id may be 0 on engine restart) — try confirm anyway.
                    (true, confirm_cancel(order_id, state, book, cfg, order_counter))
                }
                _ => (true, None),
            }
        }
        "orders_placed" => {
            // Drain per-order rejections from the Placing awaiting map.
            // Accepted orders will later arrive as individual OPEN (order_update) events.
            let results = match v.get("results").and_then(|r| r.as_array()) {
                Some(r) => r,
                None => return (true, None),
            };
            let placing_done = if let QuoteState::Placing { awaiting } = state {
                for result in results {
                    let accepted = match result.get("accepted").and_then(|a| a.as_bool()) {
                        Some(b) => b,
                        None => continue,
                    };
                    if !accepted {
                        if let Some(coid) = result.get("client_order_id").and_then(|c| c.as_str()) {
                            if awaiting.remove(coid).is_some() {
                                let reason = match result.get("reason").and_then(|r| r.as_str()) {
                                    Some(r) => r,
                                    None => "unknown",
                                };
                                warn!("[{}] order coid={} rejected at placement: {}", cfg.symbol, coid, reason);
                            }
                        }
                    }
                }
                awaiting.is_empty()
            } else { false };
            if placing_done {
                // Every order in the batch was rejected — go Idle; next depth event re-evaluates.
                *state = QuoteState::Idle;
                warn!("[{}] entire placement batch rejected, going idle", cfg.symbol);
            }
            (true, None)
        }
        "balance_update" => {
            let asset = match v.get("asset").and_then(|a| a.as_str()) {
                Some(a) => a.to_owned(),
                None => { warn!("[{}] balance_update missing asset", cfg.symbol); return (true, None); }
            };
            let available = match v.get("available").and_then(|a| a.as_f64()) {
                Some(a) => a,
                None => { warn!("[{}] balance_update missing available", cfg.symbol); return (true, None); }
            };
            balance.insert(asset, available);
            (true, None)
        }
        "auth_error" => {
            match v.get("message").and_then(|m| m.as_str()) {
                Some(msg) => warn!("[{}] WS auth failed: {msg} — reconnecting", cfg.symbol),
                None => warn!("[{}] WS auth failed — reconnecting", cfg.symbol),
            }
            (false, None)
        }
        _ => (true, None),
    }
}

// ── Binance depth handler ─────────────────────────────────────────────────────

fn on_binance_depth(
    text: &str,
    book: &mut QuoteBook,
    inv: &mut Inventory,
    balance: &HashMap<String, f64>,
    state: &mut QuoteState,
    cfg: &'static SymbolConfig,
    order_counter: &mut u64,
) -> Option<String> {
    let new_targets = match parse_depth_snapshot(text) {
        Some(t) => t,
        None => return None,
    };
    match state {
        QuoteState::Idle => {
            let (base_asset, quote_asset) = match cfg.symbol.split_once('_') {
                Some(parts) => parts,
                None => { warn!("[{}] invalid symbol format", cfg.symbol); return None; }
            };
            let first = match new_targets.first() {
                Some(t) => t,
                None => return None,
            };
            let mid = (first.bid_price + first.ask_price) / 2.0;
            if inv.position >= cfg.max_position || inv.position <= -cfg.max_position {
                let our_targets = compute_target(mid, inv, cfg);
                if within_threshold(book, &our_targets, cfg.requote_threshold_bps) {
                    return None;
                }
                warn!("[{}] pos={:+.4} hit limit, cancelling all", cfg.symbol, inv.position);
                return trigger_requote(vec![], book, state, cfg, order_counter);
            }
            let our_targets = compute_target(mid, inv, cfg);
            if within_threshold(book, &our_targets, cfg.requote_threshold_bps) {
                return None;
            }
            let avail_base = match balance.get(base_asset) {
                Some(v) => format!("{v:.4}"),
                None => "?".to_string(),
            };
            let avail_quote = match balance.get(quote_asset) {
                Some(v) => format!("{v:.2}"),
                None => "?".to_string(),
            };
            info!(
                "[{}] bid={:.1} ask={:.1}  pos={:+.4}  rpnl={:+.2}  upnl={:+.2}  book={}b/{}a  avail={}{}/{}{} → re-quote",
                cfg.symbol, first.bid_price, first.ask_price,
                inv.position, inv.realized_pnl, inv.unrealized_pnl(mid),
                book.bids.len(), book.asks.len(),
                avail_base, base_asset, avail_quote, quote_asset,
            );
            trigger_requote(our_targets, book, state, cfg, order_counter)
        }
        QuoteState::Cancelling { .. } => {
            // Refresh next_targets with the latest computed quote while waiting for cancel acks.
            // Respect position limit: if over limit, keep next_targets empty (cancel without re-quote).
            let mid = match new_targets.first() {
                Some(t) => (t.bid_price + t.ask_price) / 2.0,
                None => return None,
            };
            let fresh = if inv.position >= cfg.max_position || inv.position <= -cfg.max_position {
                vec![]
            } else {
                compute_target(mid, inv, cfg)
            };
            let timed_out = if let QuoteState::Cancelling { next_targets, started_at, to_confirm } = state {
                *next_targets = fresh;
                if started_at.elapsed() >= CANCEL_TIMEOUT {
                    warn!("[{}] cancel timeout ({} pending), forcing place", cfg.symbol, to_confirm.len());
                    true
                } else {
                    false
                }
            } else { false };
            if timed_out {
                let targets = match std::mem::replace(state, QuoteState::Idle) {
                    QuoteState::Cancelling { next_targets, .. } => next_targets,
                    _ => return None,
                };
                if targets.is_empty() {
                    return None;
                }
                trigger_requote(targets, book, state, cfg, order_counter)
            } else {
                None
            }
        }
        QuoteState::Placing { .. } => {
            // Batch in flight — ignore; next depth after transitioning to Idle will re-evaluate.
            None
        }
    }
}

/// Returns the WS message to send (cancel batch or place batch), or None if nothing to do.
/// Empty `targets` means cancel-all without subsequent re-quote (position limit hit).
fn trigger_requote(
    targets: Vec<TargetLevel>,
    book: &mut QuoteBook,
    state: &mut QuoteState,
    cfg: &'static SymbolConfig,
    order_counter: &mut u64,
) -> Option<String> {
    let ids = book.all_ids();
    if ids.is_empty() {
        if targets.is_empty() {
            *state = QuoteState::Idle;
            return None;
        }
        let (msg, awaiting) = make_place_msg(cfg.symbol, &targets, order_counter);
        *state = QuoteState::Placing { awaiting };
        Some(msg)
    } else {
        *state = QuoteState::Cancelling {
            to_confirm: ids.iter().cloned().collect(),
            next_targets: targets,
            started_at: std::time::Instant::now(),
        };
        Some(make_cancel_msg(&ids))
    }
}


// ── Inventory ─────────────────────────────────────────────────────────────────

#[derive(Default)]
struct Inventory {
    /// Net position in base currency: positive = long, negative = short.
    position: f64,
    /// Cumulative realized P&L in quote currency.
    realized_pnl: f64,
    /// Weighted average cost of current long inventory (for PnL calculation).
    avg_cost: f64,
    /// Per-order cumulative fill tracker: order_id → total_filled so far.
    /// Needed to compute incremental fill from cumulative order_update values.
    order_fill_tracker: HashMap<i64, f64>,
}

impl Inventory {
    /// Apply an incremental fill event and update position + PnL.
    fn apply_fill(&mut self, order_id: i64, side: &str, fill_price: f64, cumulative_filled: f64, is_final: bool) {
        let prev = self.order_fill_tracker.get(&order_id).copied().unwrap_or(0.0);
        let delta = (cumulative_filled - prev).max(0.0);
        if delta <= 1e-10 {
            return;
        }

        match side {
            "buy" => {
                // Update average cost using running weighted average.
                let total = self.position + delta;
                if total > 0.0 {
                    self.avg_cost = (self.avg_cost * self.position + fill_price * delta) / total;
                }
                self.position += delta;
            }
            "sell" => {
                let realized = delta * (fill_price - self.avg_cost);
                self.realized_pnl += realized;
                self.position -= delta;
                // If position flipped to short, reset avg_cost.
                if self.position < 0.0 {
                    self.avg_cost = fill_price;
                }
            }
            _ => {}
        }

        if is_final {
            self.order_fill_tracker.remove(&order_id);
        } else {
            self.order_fill_tracker.insert(order_id, cumulative_filled);
        }
    }

    fn unrealized_pnl(&self, current_price: f64) -> f64 {
        if self.position > 0.0 {
            self.position * (current_price - self.avg_cost)
        } else if self.position < 0.0 {
            self.position.abs() * (self.avg_cost - current_price)
        } else {
            0.0
        }
    }
}

// ── Active quote book ─────────────────────────────────────────────────────────

struct Quote {
    order_id: i64,
    side: &'static str,
    price: f64,
    #[allow(dead_code)]
    qty: f64,
    /// Filled quantity so far (updated from PartialFill events).
    filled: f64,
}

#[derive(Default)]
struct QuoteBook {
    bids: Vec<Quote>,
    asks: Vec<Quote>,
}

impl QuoteBook {
    fn all_ids(&self) -> Vec<i64> {
        self.bids.iter().chain(self.asks.iter()).map(|q| q.order_id).collect()
    }

    fn remove(&mut self, order_id: i64) {
        self.bids.retain(|q| q.order_id != order_id);
        self.asks.retain(|q| q.order_id != order_id);
    }

    fn is_empty(&self) -> bool {
        self.bids.is_empty() && self.asks.is_empty()
    }

    fn find_mut(&mut self, order_id: i64) -> Option<&mut Quote> {
        self.bids
            .iter_mut()
            .chain(self.asks.iter_mut())
            .find(|q| q.order_id == order_id)
    }
}

// ── Quote computation ─────────────────────────────────────────────────────────

#[derive(Clone)]
struct TargetLevel {
    bid_price: f64,
    bid_qty: f64,
    ask_price: f64,
    ask_qty: f64,
}

fn compute_target(mid: f64, inv: &Inventory, cfg: &SymbolConfig) -> Vec<TargetLevel> {
    let skew = inv.position * cfg.skew_bps_per_unit / 10_000.0 * mid;
    let mut levels = Vec::with_capacity(cfg.num_levels);
    for i in 0..cfg.num_levels {
        let half_spread = (cfg.inner_half_spread_bps + i as f64 * cfg.level_spacing_bps) / 10_000.0;
        let bid = round_tick(mid * (1.0 - half_spread) - skew, cfg.price_tick);
        let ask = round_tick(mid * (1.0 + half_spread) - skew, cfg.price_tick);
        if bid >= ask || bid <= 0.0 {
            continue;
        }
        let q = cfg.qty_per_level.max(cfg.min_qty);
        levels.push(TargetLevel { bid_price: bid, bid_qty: q, ask_price: ask, ask_qty: q });
    }
    levels
}

fn round_tick(price: f64, tick: f64) -> f64 {
    (price / tick).round() * tick
}

/// True if all current quotes are within threshold of their targets.
fn within_threshold(book: &QuoteBook, targets: &[TargetLevel], threshold_bps: f64) -> bool {
    if book.bids.len() != targets.len() || book.asks.len() != targets.len() {
        return false;
    }
    let tol = threshold_bps / 10_000.0;
    for (q, t) in book.bids.iter().zip(targets) {
        if (q.price - t.bid_price).abs() / t.bid_price > tol {
            return false;
        }
    }
    for (q, t) in book.asks.iter().zip(targets) {
        if (q.price - t.ask_price).abs() / t.ask_price > tol {
            return false;
        }
    }
    true
}

// ── Binance depth snapshot parser ────────────────────────────────────────────

/// Parse Binance `@depth10@100ms` snapshot into TargetLevels.
/// Bids are sorted descending, asks ascending — pair by index directly.
fn parse_depth_snapshot(text: &str) -> Option<Vec<TargetLevel>> {
    let d: DepthUpdate = serde_json::from_str(text).ok()?;
    if d.bids.is_empty() || d.asks.is_empty() {
        return None;
    }
    let n = d.bids.len().min(d.asks.len());
    let mut levels = Vec::with_capacity(n);
    for i in 0..n {
        let bid_price: f64 = d.bids[i][0].parse().ok()?;
        let bid_qty: f64 = d.bids[i][1].parse().ok()?;
        let ask_price: f64 = d.asks[i][0].parse().ok()?;
        let ask_qty: f64 = d.asks[i][1].parse().ok()?;
        if bid_price <= 0.0 || ask_price <= bid_price || bid_qty <= 0.0 || ask_qty <= 0.0 {
            continue;
        }
        levels.push(TargetLevel { bid_price, bid_qty, ask_price, ask_qty });
    }
    if levels.is_empty() { None } else { Some(levels) }
}

// ── Per-symbol quoting loop (dual-WS: Binance depth + Exchange) ───────────────

async fn run_symbol(cfg: &'static SymbolConfig, exchange_ws_url: String) {
    let mut inv = Inventory::default();
    let mut book = QuoteBook::default();
    let mut balance: HashMap<String, f64> = HashMap::new();
    let mut order_counter: u64 = 0;

    info!("[{}] market-maker started", cfg.symbol);

    let mut backoff = Duration::from_secs(1);
    loop {
        // ── Connect Exchange WS ──────────────────────────────────────────────
        let mut exch_ws = loop {
            match ws_connect_plain(&exchange_ws_url).await {
                Ok(ws) => { backoff = Duration::from_secs(1); break ws; }
                Err(e) => {
                    warn!("[{}] Exchange WS failed: {e}", cfg.symbol);
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                }
            }
        };
        exch_ws.set_writev(false);
        exch_ws.set_auto_close(true);
        exch_ws.set_auto_pong(true);
        // Authenticate with API key — no REST login needed.
        let auth = json!({"type":"auth_key","api_key": robot_api_key()}).to_string();
        if exch_ws.write_frame(Frame::text(Payload::Owned(auth.into_bytes()))).await.is_err() {
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(30));
            continue;
        }
        // Reset local book and state; cancel stale engine orders from last session.
        book.bids.clear();
        book.asks.clear();
        let mut state = QuoteState::Idle;
        let cancel_sym = json!({"type":"cancel_symbol","symbol":cfg.symbol}).to_string();
        let _ = exch_ws.write_frame(Frame::text(Payload::Owned(cancel_sym.into_bytes()))).await;

        // ── Connect Binance depth WS ─────────────────────────────────────────
        let binance_url = format!("{BINANCE_WS_BASE}/{}", cfg.binance_stream);
        let mut binance_ws = loop {
            match ws_connect_tls(&binance_url).await {
                Ok(ws) => break ws,
                Err(e) => {
                    warn!("[{}] Binance WS failed: {e}", cfg.symbol);
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
            }
        };
        binance_ws.set_auto_close(true);
        binance_ws.set_auto_pong(true);
        info!("[{}] Binance connected — dual-WS loop active", cfg.symbol);

        'conn: loop {
            tokio::select! {
                biased;

                // ── Exchange WS: inbound ─────────────────────────────────────
                frame = exch_ws.read_frame() => {
                    match frame {
                        Ok(f) if f.opcode == OpCode::Close => { warn!("[{}] Exchange WS closed", cfg.symbol); break 'conn; }
                        Err(e) => { warn!("[{}] Exchange WS error: {e}", cfg.symbol); break 'conn; }
                        Ok(f) if f.opcode == OpCode::Text => {
                            if let Ok(text) = std::str::from_utf8(&f.payload) {
                                let (cont, msg) = on_exch_msg(text, &mut book, &mut inv, &mut balance, &mut state, cfg, &mut order_counter);
                                if !cont { break 'conn; }
                                if let Some(m) = msg {
                                    if exch_ws.write_frame(Frame::text(Payload::Owned(m.into_bytes()))).await.is_err() {
                                        warn!("[{}] Exchange WS write error", cfg.symbol);
                                        break 'conn;
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }

                // ── Binance: depth inbound ───────────────────────────────────
                frame = binance_ws.read_frame() => {
                    match frame {
                        Ok(f) if f.opcode == OpCode::Close => { warn!("[{}] Binance WS closed", cfg.symbol); break 'conn; }
                        Err(e) => { warn!("[{}] Binance error: {e}", cfg.symbol); break 'conn; }
                        Ok(f) if f.opcode == OpCode::Text => {
                            if let Ok(text) = std::str::from_utf8(&f.payload) {
                                let msg = on_binance_depth(text, &mut book, &mut inv, &balance, &mut state, cfg, &mut order_counter);
                                if let Some(m) = msg {
                                    if exch_ws.write_frame(Frame::text(Payload::Owned(m.into_bytes()))).await.is_err() {
                                        warn!("[{}] Exchange WS write error", cfg.symbol);
                                        break 'conn;
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        if !book.is_empty() {
            warn!("[{}] WS dropped with {} live quotes; will cancel on reconnect", cfg.symbol, book.all_ids().len());
            book.bids.clear();
            book.asks.clear();
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    info!("LightningX Market Maker starting… api_key={}", robot_api_key());

    let ws_url = exchange_ws_url();
    let handles: Vec<_> = SYMBOLS
        .iter()
        .map(|cfg| {
            let url = ws_url.clone();
            tokio::spawn(async move { run_symbol(cfg, url).await })
        })
        .collect();

    tokio::signal::ctrl_c().await?;
    info!("Shutting down…");
    for h in &handles { h.abort(); }
    tokio::time::sleep(Duration::from_secs(1)).await;
    info!("Done.");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── compute_target tests ──────────────────────────────────────────────────

    fn default_cfg() -> &'static SymbolConfig {
        &SYMBOLS[0]
    }

    fn zero_inv() -> Inventory {
        Inventory::default()
    }

    #[test]
    fn test_compute_target_bid_below_ask() {
        let cfg = default_cfg();
        let inv = zero_inv();
        let levels = compute_target(50000.0, &inv, cfg);
        for t in &levels {
            assert!(t.bid_price < t.ask_price, "bid must be below ask");
        }
    }

    #[test]
    fn test_compute_target_num_levels() {
        let cfg = default_cfg();
        let inv = zero_inv();
        let levels = compute_target(50000.0, &inv, cfg);
        // All levels should be present (mid is high enough that none are dropped).
        assert_eq!(levels.len(), cfg.num_levels);
    }

    #[test]
    fn test_compute_target_skew_shifts_down_when_long() {
        let cfg = default_cfg();
        let mut inv_long = Inventory::default();
        inv_long.position = 0.1; // long position
        let inv_flat = zero_inv();
        let mid = 50000.0;
        let levels_long = compute_target(mid, &inv_long, cfg);
        let levels_flat = compute_target(mid, &inv_flat, cfg);
        // Long inventory shifts bids downward.
        assert!(
            levels_long[0].bid_price < levels_flat[0].bid_price,
            "long position should shift bid down"
        );
    }

    #[test]
    fn test_compute_target_skew_shifts_up_when_short() {
        let cfg = default_cfg();
        let mut inv_short = Inventory::default();
        inv_short.position = -0.1; // short position
        let inv_flat = zero_inv();
        let mid = 50000.0;
        let levels_short = compute_target(mid, &inv_short, cfg);
        let levels_flat = compute_target(mid, &inv_flat, cfg);
        // Short inventory shifts asks upward (skew is negative, so -skew pushes ask up).
        assert!(
            levels_short[0].ask_price > levels_flat[0].ask_price,
            "short position should shift ask up"
        );
    }

    #[test]
    fn test_compute_target_level_spacing() {
        let cfg = default_cfg();
        let inv = zero_inv();
        let levels = compute_target(50000.0, &inv, cfg);
        // Each successive level should have a wider spread than the previous.
        for i in 1..levels.len() {
            let spread_inner = levels[i - 1].ask_price - levels[i - 1].bid_price;
            let spread_outer = levels[i].ask_price - levels[i].bid_price;
            assert!(
                spread_outer >= spread_inner,
                "outer levels must be wider than inner"
            );
        }
    }

    // ── round_tick tests ──────────────────────────────────────────────────────

    #[test]
    fn test_round_tick_rounds_to_nearest() {
        assert!((round_tick(100.15, 0.1) - 100.2).abs() < 1e-9);
        assert!((round_tick(100.14, 0.1) - 100.1).abs() < 1e-9);
        assert!((round_tick(50001.3, 0.5) - 50001.5).abs() < 1e-9);
    }

    #[test]
    fn test_round_tick_zero_tick() {
        // tick=0 should not panic; result is NaN or inf but no panic.
        let _ = round_tick(100.0, 0.0);
    }

    // ── parse_depth_snapshot tests ────────────────────────────────────────────

    #[test]
    fn test_parse_depth_snapshot_valid() {
        let json = r#"{"b":[["50000.5","0.1"],["50000.0","0.2"]],"a":[["50001.5","0.1"],["50002.0","0.3"]]}"#;
        let levels = parse_depth_snapshot(json).expect("should parse");
        assert_eq!(levels.len(), 2);
        assert!((levels[0].bid_price - 50000.5).abs() < 1e-9);
        assert!((levels[0].ask_price - 50001.5).abs() < 1e-9);
        assert!((levels[0].bid_qty - 0.1).abs() < 1e-9);
    }

    #[test]
    fn test_parse_depth_snapshot_invalid_json() {
        assert!(parse_depth_snapshot("not json").is_none());
        assert!(parse_depth_snapshot("{}").is_none());
    }

    #[test]
    fn test_parse_depth_snapshot_crossed_rejected() {
        // bid >= ask should be filtered out
        let json = r#"{"b":[["50002.0","0.1"]],"a":[["50001.0","0.1"]]}"#;
        let result = parse_depth_snapshot(json);
        assert!(result.is_none());
    }

    // ── Inventory tests ───────────────────────────────────────────────────────

    #[test]
    fn test_inventory_buy_fill_increases_position() {
        let mut inv = Inventory::default();
        inv.apply_fill(1, "buy", 50000.0, 0.01, true);
        assert!((inv.position - 0.01).abs() < 1e-10);
    }

    #[test]
    fn test_inventory_sell_fill_decreases_position() {
        let mut inv = Inventory::default();
        // First establish a position.
        inv.apply_fill(1, "buy", 50000.0, 0.01, true);
        inv.apply_fill(2, "sell", 51000.0, 0.01, true);
        assert!(inv.position.abs() < 1e-10);
    }

    #[test]
    fn test_inventory_partial_fill_tracked_incrementally() {
        let mut inv = Inventory::default();
        // First partial fill of 0.005.
        inv.apply_fill(1, "buy", 50000.0, 0.005, false);
        assert!((inv.position - 0.005).abs() < 1e-10);
        // Second cumulative fill brings total to 0.01.
        inv.apply_fill(1, "buy", 50000.0, 0.01, true);
        assert!((inv.position - 0.01).abs() < 1e-10);
    }

    #[test]
    fn test_inventory_realized_pnl_on_sell() {
        let mut inv = Inventory::default();
        inv.apply_fill(1, "buy", 50000.0, 0.01, true);
        // Sell at 51000 → realized PnL = 0.01 * (51000 - 50000) = 10.
        inv.apply_fill(2, "sell", 51000.0, 0.01, true);
        assert!((inv.realized_pnl - 10.0).abs() < 1e-6);
    }

    #[test]
    fn test_inventory_avg_cost_running_weighted_average() {
        let mut inv = Inventory::default();
        // Buy 0.01 at 50000 and 0.01 at 52000 → avg = 51000.
        inv.apply_fill(1, "buy", 50000.0, 0.01, true);
        inv.apply_fill(2, "buy", 52000.0, 0.01, true);
        assert!((inv.avg_cost - 51000.0).abs() < 1e-6);
    }

    #[test]
    fn test_inventory_final_fill_removes_tracker() {
        let mut inv = Inventory::default();
        inv.apply_fill(1, "buy", 50000.0, 0.005, false);
        assert!(inv.order_fill_tracker.contains_key(&1));
        inv.apply_fill(1, "buy", 50000.0, 0.01, true);
        // Final fill removes the tracker entry.
        assert!(!inv.order_fill_tracker.contains_key(&1));
    }

    // ── QuoteBook tests ───────────────────────────────────────────────────────

    fn make_quote(id: i64, side: &'static str, price: f64) -> Quote {
        Quote { order_id: id, side, price, qty: 0.001, filled: 0.0 }
    }

    #[test]
    fn test_quotebook_all_ids() {
        let mut book = QuoteBook::default();
        book.bids.push(make_quote(1, "buy", 100.0));
        book.asks.push(make_quote(2, "sell", 101.0));
        let ids = book.all_ids();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&1));
        assert!(ids.contains(&2));
    }

    #[test]
    fn test_quotebook_remove() {
        let mut book = QuoteBook::default();
        book.bids.push(make_quote(1, "buy", 100.0));
        book.asks.push(make_quote(2, "sell", 101.0));
        book.remove(1);
        assert_eq!(book.bids.len(), 0);
        assert_eq!(book.asks.len(), 1);
    }

    #[test]
    fn test_quotebook_is_empty() {
        let mut book = QuoteBook::default();
        assert!(book.is_empty());
        book.bids.push(make_quote(1, "buy", 100.0));
        assert!(!book.is_empty());
    }

    #[test]
    fn test_quotebook_find_mut() {
        let mut book = QuoteBook::default();
        book.bids.push(make_quote(1, "buy", 100.0));
        let q = book.find_mut(1).expect("should find");
        q.filled = 0.001;
        assert!((book.bids[0].filled - 0.001).abs() < 1e-10);
        assert!(book.find_mut(99).is_none());
    }

    // ── within_threshold tests ────────────────────────────────────────────────

    fn make_targets(bid: f64, ask: f64) -> Vec<TargetLevel> {
        vec![TargetLevel { bid_price: bid, bid_qty: 0.001, ask_price: ask, ask_qty: 0.001 }]
    }

    #[test]
    fn test_within_threshold_true_when_close() {
        let mut book = QuoteBook::default();
        book.bids.push(make_quote(1, "buy", 100.0));
        book.asks.push(make_quote(2, "sell", 101.0));
        let targets = make_targets(100.0, 101.0);
        assert!(within_threshold(&book, &targets, 1.0));
    }

    #[test]
    fn test_within_threshold_false_when_far() {
        let mut book = QuoteBook::default();
        book.bids.push(make_quote(1, "buy", 99.0));
        book.asks.push(make_quote(2, "sell", 102.0));
        let targets = make_targets(100.0, 101.0);
        // 1 bps threshold, price moved by 100 bps → should be outside.
        assert!(!within_threshold(&book, &targets, 1.0));
    }

    #[test]
    fn test_within_threshold_false_when_wrong_count() {
        let book = QuoteBook::default();
        let targets = make_targets(100.0, 101.0);
        // Book has 0 orders but targets has 1 level → mismatch.
        assert!(!within_threshold(&book, &targets, 100.0));
    }
}
