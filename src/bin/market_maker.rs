/// market-maker: mirrors Binance real-time order book depth into LightningX.
///
/// Two WebSocket connections: one to Binance for depth snapshots, one shared
/// persistent connection to the desk-server for order submission and receiving
/// real-time fill/cancel notifications.  REST is used only for the initial
/// login and robot-funds top-up.
///
/// Diff-based updates: only cancel orders at price levels that disappeared from
/// Binance, and only place orders at new levels.  Orders filled by the engine
/// are detected immediately via WS order_update events and re-queued on the
/// next diff cycle.
use dashmap::DashMap;
use futures_util::{future, SinkExt, StreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

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
const DEPTH_LEVELS: usize = 20;
const QTY_SCALE: f64 = 1.0;
const MAX_USDT_PER_SIDE: f64 = 2_000_000.0;
const BINANCE_WS_BASE: &str = "wss://fstream.binance.com/ws";

struct SymbolConfig {
    our_symbol: &'static str,
    binance_stream: &'static str,
    min_qty: f64,
}

const SYMBOLS: &[SymbolConfig] = &[
    SymbolConfig { our_symbol: "BTC_USDT", binance_stream: "btcusdt@depth20@500ms", min_qty: 0.0001 },
];

// ── REST helpers (login + robot-funds only) ────────────────────────────────────

#[derive(Deserialize)] struct LoginResponse { token: String }
#[derive(Serialize)] struct LoginRequest<'a> { email: &'a str, password: &'a str }
#[derive(Serialize)] struct RegisterRequest<'a> { email: &'a str, password: &'a str, full_name: &'a str }

async fn rest_login(http: &Client, base: &str) -> anyhow::Result<String> {
    let resp = http.post(format!("{base}/api/auth/login"))
        .json(&LoginRequest { email: ROBOT_EMAIL, password: ROBOT_PASSWORD })
        .send().await?;
    if resp.status().is_success() {
        return Ok(resp.json::<LoginResponse>().await?.token);
    }
    if resp.status() == 401 {
        let reg = http.post(format!("{base}/api/auth/register"))
            .json(&RegisterRequest { email: ROBOT_EMAIL, password: ROBOT_PASSWORD, full_name: "Market Maker Robot" })
            .send().await?;
        // 409 = already exists; any other non-2xx is a real error.
        if !reg.status().is_success() && reg.status().as_u16() != 409 {
            anyhow::bail!("register failed: {}", reg.status());
        }
        let resp2 = http.post(format!("{base}/api/auth/login"))
            .json(&LoginRequest { email: ROBOT_EMAIL, password: ROBOT_PASSWORD })
            .send().await?;
        return Ok(resp2.json::<LoginResponse>().await?.token);
    }
    anyhow::bail!("login failed: {}", resp.status())
}

async fn rest_ensure_funds(http: &Client, base: &str, token: &str) {
    match http.post(format!("{base}/api/robot-funds"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await
    {
        Ok(r) if r.status().is_success() => info!("Robot inventory topped up"),
        Ok(r) => warn!("robot-funds returned {}", r.status()),
        Err(e) => warn!("robot-funds error: {e}"),
    }
}

// ── WS Exchange Client ─────────────────────────────────────────────────────────
//
// Clone-friendly handle to the shared desk-server WebSocket connection.
//
//  place_order  — sends a WS place_order message and awaits the order_accepted
//                 reply, correlated by a monotonic client_order_id.
//  cancel_order — fire-and-forget: sends the cancel and returns immediately.
//                 The server's CANCELED reply arrives as an order_update event
//                 which the recv task records in dead_orders.
//  dead_orders  — set of order IDs that have been FILLED or CANCELED by the
//                 engine; symbol tasks drain this at the start of each refresh.

#[derive(Clone)]
struct WsExchangeClient {
    ws_tx: mpsc::Sender<String>,
    /// Pending order submissions: coid → resolve with exchange order_id on order_submitted.
    pending: Arc<DashMap<u64, oneshot::Sender<Option<i64>>>>,
    dead_orders: Arc<DashMap<i64, ()>>,
    order_counter: Arc<AtomicU64>,
    /// Latency probes: coid → resolve with recv_us when order_update OPEN arrives.
    latency_pending: Arc<DashMap<u64, oneshot::Sender<u64>>>,
}

fn now_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

impl WsExchangeClient {
    async fn place_order(&self, symbol: &str, side: &str, price: f64, qty: f64) -> Option<i64> {
        let coid = self.order_counter.fetch_add(1, Ordering::Relaxed);
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
        }).to_string();

        if self.ws_tx.send(msg).await.is_err() {
            self.pending.remove(&coid);
            return None;
        }

        match tokio::time::timeout(Duration::from_secs(5), rx).await {
            Ok(Ok(id)) => id,
            _ => { self.pending.remove(&coid); None }
        }
    }

    /// Like place_order but also measures end-to-end latency from send to
    /// order_update OPEN (engine confirmed ACCEPTED).
    /// Returns (order_id, latency_us).
    async fn place_order_timed(
        &self,
        symbol: &str, side: &str, price: f64, qty: f64,
    ) -> Option<(i64, u64)> {
        let coid = self.order_counter.fetch_add(1, Ordering::Relaxed);
        let (sub_tx, mut sub_rx) = oneshot::channel::<Option<i64>>();
        let (lat_tx, lat_rx) = oneshot::channel::<u64>();
        self.pending.insert(coid, sub_tx);
        self.latency_pending.insert(coid, lat_tx);

        let send_ts = now_us();

        let msg = json!({
            "type": "place_order",
            "client_order_id": coid.to_string(),
            "symbol": symbol,
            "side": side,
            "order_type": "limit",
            "price": price,
            "qty": qty,
        }).to_string();

        if self.ws_tx.send(msg).await.is_err() {
            self.pending.remove(&coid);
            self.latency_pending.remove(&coid);
            return None;
        }

        // Wait for order_update OPEN (engine confirmed ACCEPTED).
        let recv_ts = match tokio::time::timeout(Duration::from_secs(5), lat_rx).await {
            Ok(Ok(ts)) => ts,
            _ => {
                self.latency_pending.remove(&coid);
                return None;
            }
        };
        // order_submitted should have already arrived; get order_id via try_recv.
        let order_id = sub_rx.try_recv().ok().flatten().unwrap_or(0);
        Some((order_id, recv_ts.saturating_sub(send_ts)))
    }

    async fn cancel_order(&self, order_id: i64) {
        let msg = json!({"type": "cancel_order", "order_id": order_id}).to_string();
        let _ = self.ws_tx.send(msg).await;
    }

    async fn cancel_symbol(&self, symbol: &str) {
        let msg = json!({"type": "cancel_symbol", "symbol": symbol}).to_string();
        let _ = self.ws_tx.send(msg).await;
    }
}

// ── WS connection manager ──────────────────────────────────────────────────────
//
// Owns the desk-server WS connection.  Reconnects automatically on drop.
// Routes incoming order_accepted / order_rejected → pending oneshots.
// Routes order_update{FILLED|CANCELED} → dead_orders for symbol tasks to pick up.

async fn ws_manager(
    ws_url: String,
    token: String,
    mut outbox: mpsc::Receiver<String>,
    pending: Arc<DashMap<u64, oneshot::Sender<Option<i64>>>>,
    dead_orders: Arc<DashMap<i64, ()>>,
    latency_pending: Arc<DashMap<u64, oneshot::Sender<u64>>>,
) {
    let mut backoff = Duration::from_secs(1);
    loop {
        let ws = match connect_async(&ws_url).await {
            Ok((ws, _)) => { backoff = Duration::from_secs(1); ws }
            Err(e) => {
                warn!("Desk WS connect failed: {e}, retry in {backoff:?}");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
                continue;
            }
        };
        let (mut sink, mut stream) = ws.split();

        // Authenticate immediately on connect.
        if sink.send(Message::Text(json!({"type":"auth","token":&token}).to_string())).await.is_err() {
            warn!("Desk WS auth send failed, reconnecting…");
            continue;
        }
        info!("Desk WS connected and authenticated");

        // Fair select: process one stream message OR send one outbox message per
        // iteration.  No drain loop — draining blocks the read side and causes
        // a TCP deadlock when the peer's recv buffer fills (200 concurrent orders
        // generate ~80KB of responses which exceeds the 64KB socket buffer).
        'conn: loop {
            tokio::select! {
                msg = stream.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            route_desk_msg(&text, &pending, &dead_orders, &latency_pending);
                        }
                        Some(Ok(Message::Close(_))) | None => {
                            warn!("Desk WS closed by server");
                            break 'conn;
                        }
                        Some(Err(e)) => {
                            warn!("Desk WS error: {e}");
                            break 'conn;
                        }
                        _ => {}
                    }
                }

                msg = outbox.recv() => {
                    match msg {
                        Some(text) => {
                            if sink.send(Message::Text(text)).await.is_err() {
                                warn!("Desk WS send error");
                                break 'conn;
                            }
                        }
                        None => return, // all clients dropped — shutting down
                    }
                }
            }
        }

        warn!("Desk WS reconnecting in {backoff:?}…");
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

/// Route a message received from the desk-server WS.
fn route_desk_msg(
    text: &str,
    pending: &DashMap<u64, oneshot::Sender<Option<i64>>>,
    dead_orders: &DashMap<i64, ()>,
    latency_pending: &DashMap<u64, oneshot::Sender<u64>>,
) {
    let v: Value = match serde_json::from_str(text) { Ok(v) => v, _ => return };
    let msg_type = v.get("type").and_then(|t| t.as_str()).unwrap_or("?");
    tracing::debug!("desk← type={msg_type} raw={}", &text[..text.len().min(120)]);
    match v.get("type").and_then(|t| t.as_str()).unwrap_or("") {
        "order_submitted" | "order_accepted" => {
            // Correlate by client_order_id and resolve the waiting place_order future.
            let coid: u64 = v.get("client_order_id")
                .and_then(|c| c.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(u64::MAX);
            let order_id = v.get("order_id").and_then(|i| i.as_i64());
            if let Some((_, tx)) = pending.remove(&coid) {
                let _ = tx.send(order_id);
            }
        }
        "order_rejected" => {
            let coid: u64 = v.get("client_order_id")
                .and_then(|c| c.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(u64::MAX);
            if let Some((_, tx)) = pending.remove(&coid) {
                let _ = tx.send(None);
            }
            // Also clean up any latency probe for this coid.
            latency_pending.remove(&coid);
        }
        "order_update" => {
            let status = v.get("status").and_then(|s| s.as_str()).unwrap_or("");

            // Resolve latency probe on OPEN (engine confirmed ACCEPTED).
            // client_order_id is only included for WS fast-path ACCEPTED events.
            if status == "OPEN" {
                let coid: u64 = v.get("client_order_id")
                    .and_then(|c| c.as_str())
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(u64::MAX);
                if coid != u64::MAX {
                    if let Some((_, tx)) = latency_pending.remove(&coid) {
                        let _ = tx.send(now_us());
                    }
                }
            }

            // FILLED/CANCELED: order is gone; symbol tasks need to re-place the level.
            if matches!(status, "FILLED" | "CANCELED") {
                if let Some(oid) = v.get("order_id").and_then(|i| i.as_i64()) {
                    dead_orders.insert(oid, ());
                }
            }
        }
        _ => {}
    }
}

// ── Binance depth snapshot ────────────────────────────────────────────────────

#[derive(Deserialize)]
struct BinanceDepthMsg {
    #[serde(rename = "b")] bids: Vec<[String; 2]>,
    #[serde(rename = "a")] asks: Vec<[String; 2]>,
}

fn parse_levels(raw: &[[String; 2]]) -> Vec<(f64, f64)> {
    raw.iter()
        .filter_map(|pair| Some((pair[0].parse::<f64>().ok()?, pair[1].parse::<f64>().ok()?)))
        .collect()
}

// ── Diff-based book refresh ────────────────────────────────────────────────────
//
// bid_book / ask_book: f64::to_bits(price) → order_id on LightningX.
//
// At the start of each refresh, orders marked dead (filled or canceled by the
// engine) are pruned so the diff re-places those levels.
//
// Then: cancel only levels not in the new Binance snapshot; place only levels
// that are new.  Everything else is left untouched.

async fn refresh_book(
    cfg: &SymbolConfig,
    client: &WsExchangeClient,
    bids: &[(f64, f64)],
    asks: &[(f64, f64)],
    bid_book: &mut HashMap<u64, (i64, f64)>,
    ask_book: &mut HashMap<u64, (i64, f64)>,
    cycle: u32,
) {
    // Prune orders that the engine filled or canceled — they need to be re-placed.
    bid_book.retain(|_, (id, _)| client.dead_orders.remove(id).is_none());
    ask_book.retain(|_, (id, _)| client.dead_orders.remove(id).is_none());

    // Periodically clear any CANCELED echoes we already removed from bid/ask_book
    // (cancel_order is fire-and-forget; the CANCELED event still arrives for it).
    if cycle % 200 == 0 {
        client.dead_orders.clear();
    }

    // Build desired orders (USDT budget cap on bids).
    let mut desired_bids: Vec<(f64, f64)> = Vec::new();
    let mut usdt_spent = 0.0_f64;
    for &(price, binance_qty) in bids.iter().take(DEPTH_LEVELS) {
        let qty = round_qty((binance_qty * QTY_SCALE).max(cfg.min_qty), cfg.min_qty);
        let cost = price * qty;
        if usdt_spent + cost > MAX_USDT_PER_SIDE { break; }
        usdt_spent += cost;
        desired_bids.push((price, qty));
    }
    let desired_asks: Vec<(f64, f64)> = asks.iter().take(DEPTH_LEVELS)
        .map(|&(p, q)| (p, round_qty((q * QTY_SCALE).max(cfg.min_qty), cfg.min_qty)))
        .collect();

    // Diff: cancel levels absent from desired, or whose qty drifted >15%.
    const QTY_DRIFT: f64 = 0.15;

    let desired_bid_map: HashMap<u64, f64> = desired_bids.iter().map(|(p, q)| (p.to_bits(), *q)).collect();
    let desired_ask_map: HashMap<u64, f64> = desired_asks.iter().map(|(p, q)| (p.to_bits(), *q)).collect();

    let bid_cancels: Vec<i64> = bid_book.iter()
        .filter(|(k, (_, pq))| match desired_bid_map.get(k) {
            None => true,
            Some(dq) => (dq - pq).abs() / pq > QTY_DRIFT,
        })
        .map(|(_, (id, _))| *id).collect();
    let ask_cancels: Vec<i64> = ask_book.iter()
        .filter(|(k, (_, pq))| match desired_ask_map.get(k) {
            None => true,
            Some(dq) => (dq - pq).abs() / pq > QTY_DRIFT,
        })
        .map(|(_, (id, _))| *id).collect();

    // Prices being cancelled so they can be re-placed with updated qty.
    let bid_cancel_prices: HashSet<u64> = bid_book.iter()
        .filter(|(_, (id, _))| bid_cancels.contains(id))
        .map(|(k, _)| *k).collect();
    let ask_cancel_prices: HashSet<u64> = ask_book.iter()
        .filter(|(_, (id, _))| ask_cancels.contains(id))
        .map(|(k, _)| *k).collect();

    let bid_adds: Vec<(f64, f64)> = desired_bids.iter()
        .filter(|(p, _)| !bid_book.contains_key(&p.to_bits()) || bid_cancel_prices.contains(&p.to_bits()))
        .copied().collect();
    let ask_adds: Vec<(f64, f64)> = desired_asks.iter()
        .filter(|(p, _)| !ask_book.contains_key(&p.to_bits()) || ask_cancel_prices.contains(&p.to_bits()))
        .copied().collect();

    if bid_cancels.is_empty() && ask_cancels.is_empty()
        && bid_adds.is_empty() && ask_adds.is_empty() {
        return;
    }

    // Fire cancels (fire-and-forget) + places (await order_id) all simultaneously.
    let mut cancel_futs = Vec::with_capacity(bid_cancels.len() + ask_cancels.len());
    for &id in &bid_cancels { cancel_futs.push(client.cancel_order(id)); }
    for &id in &ask_cancels { cancel_futs.push(client.cancel_order(id)); }

    let bid_place_futs: Vec<_> = bid_adds.iter()
        .map(|(p, q)| client.place_order(cfg.our_symbol, "buy",  *p, *q)).collect();
    let ask_place_futs: Vec<_> = ask_adds.iter()
        .map(|(p, q)| client.place_order(cfg.our_symbol, "sell", *p, *q)).collect();

    let (_, bid_results, ask_results) = tokio::join!(
        future::join_all(cancel_futs),
        future::join_all(bid_place_futs),
        future::join_all(ask_place_futs),
    );

    // Update local books.
    bid_book.retain(|_, (id, _)| !bid_cancels.contains(id));
    ask_book.retain(|_, (id, _)| !ask_cancels.contains(id));

    for ((p, q), result) in bid_adds.iter().zip(bid_results) {
        if let Some(id) = result { bid_book.insert(p.to_bits(), (id, *q)); }
    }
    for ((p, q), result) in ask_adds.iter().zip(ask_results) {
        if let Some(id) = result { ask_book.insert(p.to_bits(), (id, *q)); }
    }

    info!(
        "[{}] diff  −{}bid −{}ask  +{}bid +{}ask  book {}b {}a",
        cfg.our_symbol,
        bid_cancels.len(), ask_cancels.len(),
        bid_adds.len(), ask_adds.len(),
        bid_book.len(), ask_book.len(),
    );
}

// ── Per-symbol WebSocket loop ─────────────────────────────────────────────────

async fn run_symbol(cfg: &'static SymbolConfig, client: WsExchangeClient) {
    let mut bid_book: HashMap<u64, (i64, f64)> = HashMap::new();
    let mut ask_book: HashMap<u64, (i64, f64)> = HashMap::new();
    let mut cycle: u32 = 0;
    let mut reconnect_delay = Duration::from_secs(1);

    info!("[{}] market-making started (stream: {})", cfg.our_symbol, cfg.binance_stream);

    loop {
        let url = format!("{BINANCE_WS_BASE}/{}", cfg.binance_stream);
        let ws = match connect_async(&url).await {
            Ok((ws, _)) => { reconnect_delay = Duration::from_secs(1); ws }
            Err(e) => {
                warn!("[{}] Binance WS connect failed: {e}, retry in {reconnect_delay:?}", cfg.our_symbol);
                tokio::time::sleep(reconnect_delay).await;
                reconnect_delay = (reconnect_delay * 2).min(Duration::from_secs(30));
                continue;
            }
        };
        info!("[{}] Binance WS connected", cfg.our_symbol);
        let (_, mut read) = ws.split();

        while let Some(msg) = read.next().await {
            let text = match msg {
                Ok(Message::Text(t)) => t,
                Ok(Message::Close(_)) => break,
                Err(e) => { warn!("[{}] Binance WS error: {e}", cfg.our_symbol); break; }
                _ => continue,
            };

            let depth: BinanceDepthMsg = match serde_json::from_str(&text) {
                Ok(d) => d,
                Err(_) => continue,
            };

            let bids = parse_levels(&depth.bids);
            let asks = parse_levels(&depth.asks);
            if bids.is_empty() || asks.is_empty() { continue; }

            cycle = cycle.wrapping_add(1);

            refresh_book(cfg, &client, &bids, &asks, &mut bid_book, &mut ask_book, cycle).await;
        }

        // On disconnect, cancel all open orders for this symbol — prices may
        // have moved while we were offline and the stale book would mislead the diff.
        let n = bid_book.len() + ask_book.len();
        if n > 0 {
            warn!("[{}] Binance WS disconnected, cancelling {} stale orders…", cfg.our_symbol, n);
            client.cancel_symbol(cfg.our_symbol).await;
            bid_book.clear();
            ask_book.clear();
        } else {
            warn!("[{}] Binance WS disconnected, reconnecting in {reconnect_delay:?}…", cfg.our_symbol);
        }
        tokio::time::sleep(reconnect_delay).await;
        reconnect_delay = (reconnect_delay * 2).min(Duration::from_secs(30));
    }
}

fn round_qty(qty: f64, min_qty: f64) -> f64 {
    if min_qty <= 0.0 { return qty; }
    (((qty / min_qty).floor() * min_qty) * 1e8).round() / 1e8
}

// ── End-to-end latency probe ──────────────────────────────────────────────────
//
// Sends probe orders far off-market (won't fill) at a steady rate, measures
// send→order_update_OPEN latency (the full round-trip through desk-server,
// Aeron, engine, Aeron back, and WS push), then reports percentiles.

async fn run_latency_probe(client: WsExchangeClient) {
    // Brief warm-up so market-making is stable before probing.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Sequential zero-sleep probe: fire the next order as soon as the previous
    // OPEN is confirmed.  This measures true system latency without queuing
    // distortion — the single shared WS connection is never overloaded,
    // and throughput equals 1 / p50_latency (~6k orders/sec).
    const PROBE_COUNT: usize = 200_000;

    info!("[latency] Starting probe — {PROBE_COUNT} samples, sequential, no sleep");

    let mut samples: Vec<u64> = Vec::with_capacity(PROBE_COUNT);
    let mut timeouts: usize = 0;
    let start = std::time::Instant::now();

    for i in 0..PROBE_COUNT {
        // Far-off-market bid: $1 for BTC — accepted but never fills.
        match client.place_order_timed("BTC_USDT", "buy", 1.0, 0.0001).await {
            Some((_oid, us)) => samples.push(us),
            None             => timeouts += 1,
        }

        if i > 0 && i % 20_000 == 0 {
            let n = samples.len();
            let rate = n as f64 / start.elapsed().as_secs_f64();
            info!("[latency] #{i}: {n} samples, {timeouts} timeouts, {rate:.0} orders/sec");
        }
    }

    let elapsed = start.elapsed();

    // Clean up resting probe orders with a single cancel-all.
    client.cancel_symbol("BTC_USDT").await;

    let n = samples.len();
    if n < 10 {
        warn!("[latency] Too few samples ({n}) for meaningful stats");
        return;
    }

    samples.sort_unstable();
    let p50   = samples[n * 50 / 100];
    let p90   = samples[n * 90 / 100];
    let p95   = samples[n * 95 / 100];
    let p99   = samples[n * 99 / 100];
    let p999  = samples[(n * 999 / 1000).min(n - 1)];
    let min   = samples[0];
    let max   = samples[n - 1];
    let mean  = samples.iter().sum::<u64>() / n as u64;
    let throughput = n as f64 / elapsed.as_secs_f64();
    info!(
        "[latency] RESULTS ({n} samples, {timeouts} timeouts, {throughput:.0} orders/sec, elapsed={:.1}s): \
         min={min}μs  mean={mean}μs  p50={p50}μs  p90={p90}μs  p95={p95}μs  p99={p99}μs  p999={p999}μs  max={max}μs",
        elapsed.as_secs_f64()
    );
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    info!("LightningX Market Maker starting…");

    let base = exchange_url();
    let http = Client::builder().timeout(Duration::from_secs(5)).build()?;

    // REST login — only network call needed before switching to WS.
    let mut token = String::new();
    for attempt in 1..=5u32 {
        match rest_login(&http, &base).await {
            Ok(t) => { token = t; break; }
            Err(e) => {
                if attempt == 5 { anyhow::bail!("cannot authenticate after 5 attempts: {e}"); }
                warn!("auth failed ({e}), retry in 3s…");
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        }
    }
    info!("Robot authenticated");
    rest_ensure_funds(&http, &base, &token).await;

    // Shared WS client: one TCP connection, shared by all symbol tasks.
    let (ws_tx, ws_rx) = mpsc::channel::<String>(8192);
    let pending        = Arc::new(DashMap::<u64, oneshot::Sender<Option<i64>>>::new());
    let dead_orders    = Arc::new(DashMap::<i64, ()>::new());
    let latency_pending = Arc::new(DashMap::<u64, oneshot::Sender<u64>>::new());

    let client = WsExchangeClient {
        ws_tx,
        pending: pending.clone(),
        dead_orders: dead_orders.clone(),
        order_counter: Arc::new(AtomicU64::new(0)),
        latency_pending: latency_pending.clone(),
    };

    // WS manager: handles connection lifecycle, auth, send/recv, reconnect.
    let ws_url = exchange_ws_url();
    tokio::spawn(ws_manager(ws_url, token, ws_rx, pending, dead_orders, latency_pending));

    // Brief pause for the WS manager to connect and authenticate.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Cancel any leftover orders from a previous session before starting.
    info!("Clearing leftover orders…");
    for cfg in SYMBOLS {
        client.cancel_symbol(cfg.our_symbol).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    let probe_only = std::env::var("PROBE_ONLY").is_ok();
    let run_probe  = probe_only || std::env::var("RUN_PROBE").is_ok();
    let mut handles: Vec<_> = if probe_only {
        info!("PROBE_ONLY mode — skipping market-making");
        vec![]
    } else {
        info!("Starting market-making on {} symbols", SYMBOLS.len());
        SYMBOLS.iter()
            .map(|cfg| {
                let c = client.clone();
                tokio::spawn(async move { run_symbol(cfg, c).await })
            })
            .collect()
    };

    // Latency probe: only run when explicitly requested via RUN_PROBE or PROBE_ONLY.
    if run_probe {
        info!("RUN_PROBE enabled — starting latency probe");
        handles.push(tokio::spawn(run_latency_probe(client.clone())));
    }

    tokio::signal::ctrl_c().await?;
    info!("Shutting down…");
    for h in handles { h.abort(); }
    tokio::time::sleep(Duration::from_secs(2)).await;
    info!("Done.");
    Ok(())
}
