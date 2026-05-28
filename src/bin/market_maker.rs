/// market-maker: professional two-sided market maker for LightningX.
///
/// Fair value: Binance real-time bookTicker (best bid/ask mid-price).
/// Quoting:    N bid + N ask levels, computed from mid with configurable spread.
/// Risk:       inventory skew shifts quotes to reduce position; hard limit stops quoting.
/// Lifecycle:  cancel-ALL → wait confirm → place-ALL on each re-quote cycle.
///             Fills (full and partial) tracked; P&L reported on each cycle.
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use fastwebsockets::{handshake, Frame, OpCode, Payload, WebSocket};
use http_body_util::Empty;
use hyper::{
    body::Bytes,
    header::{CONNECTION, UPGRADE},
    Request,
};
use hyper_util::rt::TokioIo;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
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

const ROBOT_EMAIL: &str = "robot@lightningx.exchange";
const ROBOT_PASSWORD: &str = "robot_secret_2026";
const BINANCE_WS_BASE: &str = "wss://fstream.binance.com/ws";
/// How long to wait for cancel confirmations before giving up on this cycle.
const CANCEL_TIMEOUT: Duration = Duration::from_millis(3000);

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
    binance_stream: "btcusdt@depth5@100ms",
    num_levels: 5,
    qty_per_level: 0.001,
    min_qty: 0.0001,
    price_tick: 0.1,
    inner_half_spread_bps: 0.1,
    level_spacing_bps: 0.5,
    max_position: 0.05,
    skew_bps_per_unit: 1.0,
    requote_threshold_bps: 0.05,
}];

// ── REST helpers ──────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct LoginResponse {
    token: String,
}
#[derive(Serialize)]
struct LoginRequest<'a> {
    email: &'a str,
    password: &'a str,
}
#[derive(Serialize)]
struct RegisterRequest<'a> {
    email: &'a str,
    password: &'a str,
    full_name: &'a str,
}
#[derive(Deserialize)]
struct RestOrder {
    id: i64,
}

async fn rest_login(http: &Client, base: &str) -> anyhow::Result<String> {
    let resp = http
        .post(format!("{base}/api/auth/login"))
        .json(&LoginRequest { email: ROBOT_EMAIL, password: ROBOT_PASSWORD })
        .send()
        .await?;
    if resp.status().is_success() {
        return Ok(resp.json::<LoginResponse>().await?.token);
    }
    if resp.status() == 401 {
        let reg = http
            .post(format!("{base}/api/auth/register"))
            .json(&RegisterRequest {
                email: ROBOT_EMAIL,
                password: ROBOT_PASSWORD,
                full_name: "Market Maker Robot",
            })
            .send()
            .await?;
        if !reg.status().is_success() && reg.status().as_u16() != 409 {
            anyhow::bail!("register failed: {}", reg.status());
        }
        let resp2 = http
            .post(format!("{base}/api/auth/login"))
            .json(&LoginRequest { email: ROBOT_EMAIL, password: ROBOT_PASSWORD })
            .send()
            .await?;
        return Ok(resp2.json::<LoginResponse>().await?.token);
    }
    anyhow::bail!("login failed: {}", resp.status())
}

async fn rest_ensure_funds(http: &Client, base: &str, token: &str) {
    match http
        .post(format!("{base}/api/robot-funds"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => info!("Robot inventory topped up"),
        Ok(r) => warn!("robot-funds: {}", r.status()),
        Err(e) => warn!("robot-funds error: {e}"),
    }
}

async fn rest_open_order_ids(http: &Client, base: &str, token: &str, symbol: &str) -> Vec<i64> {
    match http
        .get(format!("{base}/api/orders"))
        .query(&[("symbol", symbol), ("status", "open"), ("limit", "500")])
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .and_then(|r| Ok(r.error_for_status()?))
    {
        Ok(r) => r.json::<Vec<RestOrder>>().await.map(|v| v.into_iter().map(|o| o.id).collect()).unwrap_or_default(),
        Err(e) => { warn!("open-order query failed: {e}"); vec![] }
    }
}

// ── Exchange events ───────────────────────────────────────────────────────────

/// Events flowing from the desk-server WS to the symbol task.
#[derive(Debug)]
enum ExchangeEvent {
    /// Engine confirmed order open; coid used to correlate with place_order().
    Accepted { coid: u64, order_id: i64 },
    /// Order rejected by engine.
    Rejected { coid: u64 },
    /// Partial fill: some quantity executed, order still resting.
    PartialFill {
        order_id: i64,
        side: String,
        fill_price: f64,
        /// Total cumulative filled qty for this order.
        cumulative_filled: f64,
    },
    /// Full fill: order completely executed.
    FullFill {
        order_id: i64,
        side: String,
        fill_price: f64,
        qty: f64,
    },
    /// Order cancelled (by us or engine).
    Cancelled { order_id: i64 },
    /// Balance update from the exchange (after freeze/release).
    BalanceUpdate { asset: String, balance: f64, available: f64, frozen: f64 },
}

// ── Batch order result ────────────────────────────────────────────────────────

struct BatchOrderResult {
    client_order_id: String,
    order_id: Option<i64>,
    accepted: bool,
}

// ── WS Exchange Client ─────────────────────────────────────────────────────────

#[derive(Clone)]
struct WsClient {
    /// Outbound messages to the desk-server WS.
    ws_tx: mpsc::Sender<String>,
    /// Pending place_order calls, keyed by client_order_id.
    pending: Arc<DashMap<u64, oneshot::Sender<Option<i64>>>>,
    /// Order IDs confirmed dead (FILLED or CANCELLED), used by wait_dead().
    dead: Arc<DashMap<i64, ()>>,
    counter: Arc<AtomicU64>,
    /// Counter for batch_id generation.
    batch_counter: Arc<AtomicU64>,
    /// Pending place_orders_batch calls, keyed by batch_id (numeric).
    pending_batch: Arc<DashMap<u64, oneshot::Sender<Vec<BatchOrderResult>>>>,
}

impl WsClient {
    /// Place a limit order; returns exchange order_id or None on rejection/timeout.
    async fn place_order(&self, symbol: &str, side: &str, price: f64, qty: f64) -> Option<i64> {
        let coid = self.counter.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.insert(coid, tx);
        let msg = json!({
            "type": "place_order",
            "client_order_id": coid.to_string(),
            "symbol": symbol,
            "side": side,
            "order_type": "limit",
            "price": price,
            "qty": qty,
        })
        .to_string();
        if self.ws_tx.send(msg).await.is_err() {
            self.pending.remove(&coid);
            return None;
        }
        match tokio::time::timeout(Duration::from_secs(5), rx).await {
            Ok(Ok(id)) => id,
            _ => {
                self.pending.remove(&coid);
                None
            }
        }
    }

    /// Place multiple orders in a single WS message; returns results for all orders.
    async fn place_orders_batch(
        &self,
        symbol: &str,
        orders: &[(String, &str, f64, f64)],
    ) -> Vec<BatchOrderResult> {
        let batch_num = self.batch_counter.fetch_add(1, Ordering::Relaxed);
        let batch_id = batch_num.to_string();
        let (tx, rx) = oneshot::channel();
        self.pending_batch.insert(batch_num, tx);

        let order_vals: Vec<Value> = orders
            .iter()
            .map(|(coid, side, price, qty)| {
                json!({
                    "client_order_id": coid,
                    "symbol": symbol,
                    "side": side,
                    "order_type": "limit",
                    "price": price,
                    "qty": qty,
                })
            })
            .collect();

        let msg = json!({
            "type": "place_orders",
            "batch_id": batch_id,
            "orders": order_vals,
        })
        .to_string();

        if self.ws_tx.send(msg).await.is_err() {
            self.pending_batch.remove(&batch_num);
            return orders
                .iter()
                .map(|(coid, _, _, _)| BatchOrderResult {
                    client_order_id: coid.clone(),
                    order_id: None,
                    accepted: false,
                })
                .collect();
        }

        match tokio::time::timeout(Duration::from_secs(5), rx).await {
            Ok(Ok(results)) => results,
            _ => {
                self.pending_batch.remove(&batch_num);
                orders
                    .iter()
                    .map(|(coid, _, _, _)| BatchOrderResult {
                        client_order_id: coid.clone(),
                        order_id: None,
                        accepted: false,
                    })
                    .collect()
            }
        }
    }

    /// Batch cancel; fire-and-forget — confirmations arrive as ExchangeEvent::Cancelled.
    async fn batch_cancel(&self, ids: &[i64]) {
        if ids.is_empty() {
            return;
        }
        let msg = json!({"type": "batch_cancel", "order_ids": ids}).to_string();
        let _ = self.ws_tx.send(msg).await;
    }

    /// Cancel all orders for a symbol (engine-side); fire-and-forget.
    async fn cancel_symbol(&self, symbol: &str) {
        let msg = json!({"type": "cancel_symbol", "symbol": symbol}).to_string();
        let _ = self.ws_tx.send(msg).await;
    }

    /// Block until all ids appear in dead set, or timeout elapses.
    async fn wait_dead(&self, ids: &[i64], timeout: Duration) -> bool {
        if ids.is_empty() {
            return true;
        }
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if ids.iter().all(|id| self.dead.contains_key(id)) {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

// ── WS manager ───────────────────────────────────────────────────────────────

async fn ws_manager(
    ws_url: String,
    token: String,
    mut outbox: mpsc::Receiver<String>,
    pending: Arc<DashMap<u64, oneshot::Sender<Option<i64>>>>,
    pending_batch: Arc<DashMap<u64, oneshot::Sender<Vec<BatchOrderResult>>>>,
    dead: Arc<DashMap<i64, ()>>,
    event_tx: mpsc::Sender<ExchangeEvent>,
) {
    let mut backoff = Duration::from_secs(1);
    loop {
        let mut ws = match ws_connect_plain(&ws_url).await {
            Ok(ws) => {
                backoff = Duration::from_secs(1);
                ws
            }
            Err(e) => {
                warn!("Desk WS connect failed: {e}, retry in {backoff:?}");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
                continue;
            }
        };
        ws.set_writev(false);
        ws.set_auto_close(true);
        ws.set_auto_pong(true);

        let auth = json!({"type":"auth","token":&token}).to_string();
        if ws.write_frame(Frame::text(Payload::Owned(auth.into_bytes()))).await.is_err() {
            continue;
        }
        info!("Desk WS connected");

        'conn: loop {
            tokio::select! {
                biased;
                frame = ws.read_frame() => {
                    match frame {
                        Ok(f) if f.opcode == OpCode::Text => {
                            if let Ok(text) = std::str::from_utf8(&f.payload) {
                                route_msg(text, &pending, &pending_batch, &dead, &event_tx);
                            }
                        }
                        Ok(f) if f.opcode == OpCode::Close => break 'conn,
                        Err(e) => { warn!("Desk WS error: {e}"); break 'conn; }
                        _ => {}
                    }
                }
                msg = outbox.recv() => {
                    match msg {
                        Some(text) => {
                            if ws.write_frame(Frame::text(Payload::Owned(text.into_bytes()))).await.is_err() {
                                break 'conn;
                            }
                        }
                        None => return,
                    }
                }
            }
        }

        warn!("Desk WS reconnecting in {backoff:?}");
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

fn route_msg(
    text: &str,
    pending: &DashMap<u64, oneshot::Sender<Option<i64>>>,
    pending_batch: &DashMap<u64, oneshot::Sender<Vec<BatchOrderResult>>>,
    dead: &DashMap<i64, ()>,
    event_tx: &mpsc::Sender<ExchangeEvent>,
) {
    let v: Value = match serde_json::from_str(text) {
        Ok(v) => v,
        _ => return,
    };
    match v.get("type").and_then(|t| t.as_str()).unwrap_or("") {
        "order_submitted" | "order_accepted" => {
            let coid: u64 = v
                .get("client_order_id")
                .and_then(|c| c.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(u64::MAX);
            let order_id = v.get("order_id").and_then(|i| i.as_i64());
            if let Some((_, tx)) = pending.remove(&coid) {
                let _ = tx.send(order_id);
            }
            if let (Some(coid_val), Some(oid)) = (
                v.get("client_order_id").and_then(|c| c.as_str()).and_then(|s| s.parse::<u64>().ok()),
                order_id,
            ) {
                let _ = event_tx.try_send(ExchangeEvent::Accepted { coid: coid_val, order_id: oid });
            }
        }
        "order_rejected" => {
            let coid: u64 = v
                .get("client_order_id")
                .and_then(|c| c.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(u64::MAX);
            if let Some((_, tx)) = pending.remove(&coid) {
                let _ = tx.send(None);
            }
            let _ = event_tx.try_send(ExchangeEvent::Rejected { coid });
        }
        "order_update" => {
            let status = v.get("status").and_then(|s| s.as_str()).unwrap_or("");
            let order_id = match v.get("order_id").and_then(|i| i.as_i64()) {
                Some(id) => id,
                None => return,
            };
            let side = v.get("side").and_then(|s| s.as_str()).unwrap_or("").to_string();
            let price = v.get("price").and_then(|p| p.as_f64()).unwrap_or(0.0);
            let filled = v.get("filled").and_then(|f| f.as_f64()).unwrap_or(0.0);
            let quantity = v.get("quantity").and_then(|q| q.as_f64()).unwrap_or(0.0);

            match status {
                "PARTIALLY_FILLED" => {
                    let _ = event_tx.try_send(ExchangeEvent::PartialFill {
                        order_id,
                        side,
                        fill_price: price,
                        cumulative_filled: filled,
                    });
                }
                "FILLED" => {
                    dead.insert(order_id, ());
                    let _ = event_tx.try_send(ExchangeEvent::FullFill {
                        order_id,
                        side,
                        fill_price: price,
                        qty: quantity,
                    });
                }
                "CANCELED" => {
                    dead.insert(order_id, ());
                    let _ = event_tx.try_send(ExchangeEvent::Cancelled { order_id });
                }
                _ => {}
            }
        }
        "orders_placed" => {
            let batch_id_str = v.get("batch_id").and_then(|b| b.as_str()).unwrap_or("");
            let batch_num: u64 = match batch_id_str.parse() {
                Ok(n) => n,
                Err(_) => return,
            };
            let results: Vec<BatchOrderResult> = v
                .get("results")
                .and_then(|r| r.as_array())
                .map(|arr| {
                    arr.iter()
                        .map(|item| BatchOrderResult {
                            client_order_id: item
                                .get("client_order_id")
                                .and_then(|c| c.as_str())
                                .unwrap_or("")
                                .to_owned(),
                            order_id: item.get("order_id").and_then(|i| i.as_i64()),
                            accepted: item
                                .get("accepted")
                                .and_then(|a| a.as_bool())
                                .unwrap_or(false),
                        })
                        .collect()
                })
                .unwrap_or_default();
            if let Some((_, tx)) = pending_batch.remove(&batch_num) {
                let _ = tx.send(results);
            }
        }
        "balance_update" => {
            let asset = v.get("asset").and_then(|a| a.as_str()).unwrap_or("").to_owned();
            let balance = v.get("balance").and_then(|b| b.as_f64()).unwrap_or(0.0);
            let available = v.get("available").and_then(|a| a.as_f64()).unwrap_or(0.0);
            let frozen = v.get("frozen").and_then(|f| f.as_f64()).unwrap_or(0.0);
            if !asset.is_empty() {
                let _ = event_tx.try_send(ExchangeEvent::BalanceUpdate { asset, balance, available, frozen });
            }
        }
        _ => {}
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

#[derive(Deserialize)]
struct DepthUpdate {
    #[serde(rename = "b")]
    bids: Vec<[String; 2]>,
    #[serde(rename = "a")]
    asks: Vec<[String; 2]>,
}

/// Parse Binance `@depth5@100ms` snapshot into TargetLevels.
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

// ── Apply exchange event ──────────────────────────────────────────────────────

fn apply_event(event: ExchangeEvent, inv: &mut Inventory, book: &mut QuoteBook) {
    match event {
        ExchangeEvent::PartialFill { order_id, side, fill_price, cumulative_filled } => {
            inv.apply_fill(order_id, &side, fill_price, cumulative_filled, false);
            if let Some(q) = book.find_mut(order_id) {
                q.filled = cumulative_filled;
            }
        }
        ExchangeEvent::FullFill { order_id, side, fill_price, qty } => {
            inv.apply_fill(order_id, &side, fill_price, qty, true);
            book.remove(order_id);
        }
        ExchangeEvent::Cancelled { order_id } => {
            book.remove(order_id);
        }
        // Accepted is handled by place_order() oneshot; Rejected likewise.
        ExchangeEvent::Accepted { .. } | ExchangeEvent::Rejected { .. } => {}
        // BalanceUpdate is handled by run_symbol's event loop.
        ExchangeEvent::BalanceUpdate { .. } => {}
    }
}

// ── Per-symbol quoting loop ───────────────────────────────────────────────────

async fn run_symbol(
    cfg: &'static SymbolConfig,
    client: WsClient,
    mut event_rx: mpsc::Receiver<ExchangeEvent>,
) {
    let mut inv = Inventory::default();
    let mut book = QuoteBook::default();
    let mut reconnect_delay = Duration::from_secs(1);
    // Available balance per asset (updated from BalanceUpdate events).
    let mut balance: HashMap<String, f64> = HashMap::new();

    info!("[{}] market-maker started", cfg.symbol);

    loop {
        let url = format!("{BINANCE_WS_BASE}/{}", cfg.binance_stream);
        let mut ws = match ws_connect_tls(&url).await {
            Ok(ws) => {
                reconnect_delay = Duration::from_secs(1);
                ws
            }
            Err(e) => {
                warn!("[{}] Binance connect failed: {e}", cfg.symbol);
                tokio::time::sleep(reconnect_delay).await;
                reconnect_delay = (reconnect_delay * 2).min(Duration::from_secs(30));
                continue;
            }
        };
        ws.set_auto_close(true);
        ws.set_auto_pong(true);
        info!("[{}] Binance connected", cfg.symbol);

        'feed: loop {
            // ── drain all pending exchange events (non-blocking) ──────────
            loop {
                match event_rx.try_recv() {
                    Ok(ExchangeEvent::BalanceUpdate { asset, available, .. }) => {
                        balance.insert(asset, available);
                    }
                    Ok(ev) => apply_event(ev, &mut inv, &mut book),
                    Err(_) => break,
                }
            }

            // ── wait for next Binance price ───────────────────────────────
            let frame = match ws.read_frame().await {
                Ok(f) => f,
                Err(e) => {
                    warn!("[{}] Binance read error: {e}", cfg.symbol);
                    break 'feed;
                }
            };
            let text = match frame.opcode {
                OpCode::Text => match std::str::from_utf8(&frame.payload) {
                    Ok(t) => t.to_string(),
                    Err(_) => continue,
                },
                OpCode::Close => break 'feed,
                _ => continue,
            };
            let targets = match parse_depth_snapshot(&text) {
                Some(t) => t,
                None => continue,
            };

            // Skip if all quotes are already within threshold.
            if within_threshold(&book, &targets, cfg.requote_threshold_bps) {
                continue;
            }

            let sym_parts: Vec<&str> = cfg.symbol.splitn(2, '_').collect();
            let base_asset = sym_parts.first().copied().unwrap_or("BTC");
            let quote_asset = sym_parts.last().copied().unwrap_or("USDT");
            let avail_base = balance.get(base_asset).copied().unwrap_or(f64::NAN);
            let avail_quote = balance.get(quote_asset).copied().unwrap_or(f64::NAN);
            let best_bid = targets.first().map(|t| t.bid_price).unwrap_or(0.0);
            let best_ask = targets.first().map(|t| t.ask_price).unwrap_or(0.0);
            let mid = (best_bid + best_ask) / 2.0;
            info!(
                "[{}] bid={:.1} ask={:.1}  pos={:+.4}  rpnl={:+.2}  upnl={:+.2}  book={}b/{}a  avail={:.4}{}/{:.2}{}  → re-quote",
                cfg.symbol,
                best_bid,
                best_ask,
                inv.position,
                inv.realized_pnl,
                inv.unrealized_pnl(mid),
                book.bids.len(),
                book.asks.len(),
                avail_base,
                base_asset,
                avail_quote,
                quote_asset,
            );

            // ── risk check: pause quoting at position limit ───────────────
            let at_max_long = inv.position >= cfg.max_position;
            let at_max_short = inv.position <= -cfg.max_position;
            if at_max_long || at_max_short {
                warn!(
                    "[{}] position limit hit ({:+.4}), cancelling all quotes",
                    cfg.symbol, inv.position
                );
                let ids = book.all_ids();
                if !ids.is_empty() {
                    client.batch_cancel(&ids).await;
                    if client.wait_dead(&ids, CANCEL_TIMEOUT).await {
                        book.bids.clear();
                        book.asks.clear();
                    }
                }
                continue;
            }

            // ── cancel ALL existing quotes, wait for confirmation ─────────
            let existing_ids = book.all_ids();
            if !existing_ids.is_empty() {
                client.batch_cancel(&existing_ids).await;
                let confirmed = client.wait_dead(&existing_ids, CANCEL_TIMEOUT).await;
                book.bids.clear();
                book.asks.clear();
                if !confirmed {
                    warn!("[{}] cancel timeout — skipping this cycle", cfg.symbol);
                    continue;
                }
            }

            if targets.is_empty() {
                continue;
            }

            // ── place all bids and asks in a single batch message ────────
            // Build a list of (coid, side, price, qty) for each level.
            let batch_counter_base = client.counter.fetch_add(targets.len() as u64 * 2, Ordering::Relaxed);
            let mut batch_orders: Vec<(String, &str, f64, f64)> = Vec::with_capacity(targets.len() * 2);
            for (i, t) in targets.iter().enumerate() {
                let bid_coid = format!("{}_{}", cfg.symbol, batch_counter_base + i as u64);
                let ask_coid = format!("{}_{}_a", cfg.symbol, batch_counter_base + i as u64);
                batch_orders.push((bid_coid, "buy", t.bid_price, t.bid_qty));
                batch_orders.push((ask_coid, "sell", t.ask_price, t.ask_qty));
            }

            let batch_results = client.place_orders_batch(cfg.symbol, &batch_orders).await;

            // Match results back to target levels (bids first, then asks, interleaved).
            for (i, t) in targets.iter().enumerate() {
                // bid result is at position i*2, ask result at i*2+1.
                if let Some(res) = batch_results.get(i * 2) {
                    if res.accepted {
                        if let Some(id) = res.order_id {
                            book.bids.push(Quote {
                                order_id: id,
                                side: "buy",
                                price: t.bid_price,
                                qty: t.bid_qty,
                                filled: 0.0,
                            });
                        }
                    }
                }
                if let Some(res) = batch_results.get(i * 2 + 1) {
                    if res.accepted {
                        if let Some(id) = res.order_id {
                            book.asks.push(Quote {
                                order_id: id,
                                side: "sell",
                                price: t.ask_price,
                                qty: t.ask_qty,
                                filled: 0.0,
                            });
                        }
                    }
                }
            }

            // Keep sorted: bids descending, asks ascending (innermost first).
            book.bids.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap_or(std::cmp::Ordering::Equal));
            book.asks.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap_or(std::cmp::Ordering::Equal));

            info!(
                "[{}] quotes active: {}b {}a  (inner bid={:.1} ask={:.1})",
                cfg.symbol,
                book.bids.len(),
                book.asks.len(),
                book.bids.first().map(|q| q.price).unwrap_or(0.0),
                book.asks.first().map(|q| q.price).unwrap_or(0.0),
            );
        }

        // ── Binance disconnected: cancel all to avoid stale quotes ────────
        if !book.is_empty() {
            warn!("[{}] Binance disconnected — cancelling {} quotes", cfg.symbol, book.all_ids().len());
            let ids = book.all_ids();
            client.batch_cancel(&ids).await;
            if client.wait_dead(&ids, CANCEL_TIMEOUT).await {
                book.bids.clear();
                book.asks.clear();
            } else {
                warn!("[{}] cancel timeout on disconnect; quotes may remain in engine", cfg.symbol);
            }
        }

        warn!("[{}] reconnecting in {reconnect_delay:?}", cfg.symbol);
        tokio::time::sleep(reconnect_delay).await;
        reconnect_delay = (reconnect_delay * 2).min(Duration::from_secs(30));
    }
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    info!("LightningX Market Maker starting…");

    let base = exchange_url();
    let http = Client::builder().timeout(Duration::from_secs(5)).build()?;

    let mut token = String::new();
    for attempt in 1..=5u32 {
        match rest_login(&http, &base).await {
            Ok(t) => {
                token = t;
                break;
            }
            Err(e) => {
                if attempt == 5 {
                    anyhow::bail!("cannot authenticate after 5 attempts: {e}");
                }
                warn!("auth failed ({e}), retry in 3s…");
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        }
    }
    info!("Authenticated");
    rest_ensure_funds(&http, &base, &token).await;

    // Shared WS client.
    let (ws_tx, ws_rx) = mpsc::channel::<String>(8192);
    let pending = Arc::new(DashMap::<u64, oneshot::Sender<Option<i64>>>::new());
    let pending_batch = Arc::new(DashMap::<u64, oneshot::Sender<Vec<BatchOrderResult>>>::new());
    let dead = Arc::new(DashMap::<i64, ()>::new());

    let client = WsClient {
        ws_tx,
        pending: pending.clone(),
        dead: dead.clone(),
        counter: Arc::new(AtomicU64::new(0)),
        batch_counter: Arc::new(AtomicU64::new(0)),
        pending_batch: pending_batch.clone(),
    };

    // Per-symbol event channels (one per symbol task).
    let mut event_txs: Vec<mpsc::Sender<ExchangeEvent>> = Vec::new();
    let mut event_rxs: Vec<mpsc::Receiver<ExchangeEvent>> = Vec::new();
    for _ in SYMBOLS {
        let (tx, rx) = mpsc::channel(4096);
        event_txs.push(tx);
        event_rxs.push(rx);
    }

    // WS manager: connection lifecycle + message routing.
    // For now single symbol → single event_tx.  Multi-symbol would filter by symbol field.
    let event_tx = event_txs.into_iter().next().expect("at least one symbol");
    let ws_url = exchange_ws_url();
    tokio::spawn(ws_manager(ws_url, token.clone(), ws_rx, pending, pending_batch, dead, event_tx));

    // Brief pause for the WS manager to connect and authenticate.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Cancel any leftover orders from a previous session.
    info!("Clearing leftover orders from previous session…");
    for cfg in SYMBOLS {
        client.cancel_symbol(cfg.symbol).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    for cfg in SYMBOLS {
        let leftovers = rest_open_order_ids(&http, &base, &token, cfg.symbol).await;
        if !leftovers.is_empty() {
            warn!("[{}] {} leftover orders remain after cancel_symbol", cfg.symbol, leftovers.len());
        }
    }

    // Spawn one quoting task per symbol.
    let handles: Vec<_> = SYMBOLS
        .iter()
        .zip(event_rxs)
        .map(|(cfg, rx)| {
            let c = client.clone();
            tokio::spawn(async move { run_symbol(cfg, c, rx).await })
        })
        .collect();

    tokio::signal::ctrl_c().await?;
    info!("Shutting down…");
    for h in &handles {
        h.abort();
    }
    // Give cancellations a moment to reach the engine.
    for cfg in SYMBOLS {
        client.cancel_symbol(cfg.symbol).await;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
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

    // ── parse_mid tests ───────────────────────────────────────────────────────

    #[test]
    fn test_parse_mid_valid() {
        let json = r#"{"b":"50000.5","a":"50001.5"}"#;
        let mid = parse_mid(json).expect("should parse");
        assert!((mid - 50001.0).abs() < 1e-9);
    }

    #[test]
    fn test_parse_mid_invalid_json() {
        assert!(parse_mid("not json").is_none());
        assert!(parse_mid("{}").is_none());
    }

    #[test]
    fn test_parse_mid_bid_above_ask_returns_none() {
        let json = r#"{"b":"50002.0","a":"50001.0"}"#;
        assert!(parse_mid(json).is_none());
    }

    #[test]
    fn test_parse_mid_zero_bid_returns_none() {
        let json = r#"{"b":"0.0","a":"50001.0"}"#;
        assert!(parse_mid(json).is_none());
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
