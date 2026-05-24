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
const QTY_SCALE: f64 = 0.02;
const MAX_USDT_PER_SIDE: f64 = 40000.0;
const BINANCE_WS_BASE: &str = "wss://fstream.binance.com/ws";

struct SymbolConfig {
    our_symbol: &'static str,
    binance_stream: &'static str,
    min_qty: f64,
}

const SYMBOLS: &[SymbolConfig] = &[
    SymbolConfig { our_symbol: "ETH_USDT", binance_stream: "ethusdt@depth20@100ms", min_qty: 0.001 },
    SymbolConfig { our_symbol: "BTC_USDT", binance_stream: "btcusdt@depth20@100ms", min_qty: 0.0001 },
    SymbolConfig { our_symbol: "SOL_USDT", binance_stream: "solusdt@depth20@100ms", min_qty: 0.01 },
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
    pending: Arc<DashMap<u64, oneshot::Sender<Option<i64>>>>,
    dead_orders: Arc<DashMap<i64, ()>>,
    order_counter: Arc<AtomicU64>,
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

        // Drive outgoing + incoming until the connection drops.
        loop {
            tokio::select! {
                msg = outbox.recv() => {
                    match msg {
                        Some(text) => {
                            if sink.send(Message::Text(text)).await.is_err() {
                                warn!("Desk WS send error");
                                break;
                            }
                        }
                        None => return, // all clients dropped — shutting down
                    }
                }

                msg = stream.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            route_desk_msg(&text, &pending, &dead_orders);
                        }
                        Some(Ok(Message::Close(_))) | None => {
                            warn!("Desk WS closed by server");
                            break;
                        }
                        Some(Err(e)) => {
                            warn!("Desk WS error: {e}");
                            break;
                        }
                        _ => {}
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
) {
    let v: Value = match serde_json::from_str(text) { Ok(v) => v, _ => return };
    match v.get("type").and_then(|t| t.as_str()).unwrap_or("") {
        "order_accepted" => {
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
        }
        "order_update" => {
            // FILLED = maker order consumed by taker. CANCELED = cancel confirmed.
            // Both mean the order is gone; symbol tasks need to re-place the level.
            let status = v.get("status").and_then(|s| s.as_str()).unwrap_or("");
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
    bid_book: &mut HashMap<u64, i64>,
    ask_book: &mut HashMap<u64, i64>,
    cycle: u32,
) {
    // Prune orders that the engine filled or canceled — they need to be re-placed.
    bid_book.retain(|_, id| client.dead_orders.remove(id).is_none());
    ask_book.retain(|_, id| client.dead_orders.remove(id).is_none());

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

    // Diff.
    let desired_bid_keys: HashSet<u64> = desired_bids.iter().map(|(p, _)| p.to_bits()).collect();
    let desired_ask_keys: HashSet<u64> = desired_asks.iter().map(|(p, _)| p.to_bits()).collect();

    let bid_cancels: Vec<i64> = bid_book.iter()
        .filter(|(k, _)| !desired_bid_keys.contains(k))
        .map(|(_, &id)| id).collect();
    let ask_cancels: Vec<i64> = ask_book.iter()
        .filter(|(k, _)| !desired_ask_keys.contains(k))
        .map(|(_, &id)| id).collect();

    let bid_adds: Vec<(f64, f64)> = desired_bids.iter()
        .filter(|(p, _)| !bid_book.contains_key(&p.to_bits()))
        .copied().collect();
    let ask_adds: Vec<(f64, f64)> = desired_asks.iter()
        .filter(|(p, _)| !ask_book.contains_key(&p.to_bits()))
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
    bid_book.retain(|_, id| !bid_cancels.contains(id));
    ask_book.retain(|_, id| !ask_cancels.contains(id));

    for ((p, _), result) in bid_adds.iter().zip(bid_results) {
        if let Some(id) = result { bid_book.insert(p.to_bits(), id); }
    }
    for ((p, _), result) in ask_adds.iter().zip(ask_results) {
        if let Some(id) = result { ask_book.insert(p.to_bits(), id); }
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
    let mut bid_book: HashMap<u64, i64> = HashMap::new();
    let mut ask_book: HashMap<u64, i64> = HashMap::new();
    let mut last_bid: f64 = 0.0;
    let mut last_ask: f64 = 0.0;
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

            let best_bid = bids[0].0;
            let best_ask = asks[0].0;

            // Only run a diff when the BBO moves — stable ticks cost nothing.
            if (best_bid - last_bid).abs() < 1e-9 && (best_ask - last_ask).abs() < 1e-9 {
                continue;
            }
            last_bid = best_bid;
            last_ask = best_ask;
            cycle = cycle.wrapping_add(1);

            refresh_book(cfg, &client, &bids, &asks, &mut bid_book, &mut ask_book, cycle).await;
        }

        warn!("[{}] Binance WS disconnected, reconnecting in {reconnect_delay:?}…", cfg.our_symbol);
        tokio::time::sleep(reconnect_delay).await;
        reconnect_delay = (reconnect_delay * 2).min(Duration::from_secs(30));
    }
}

fn round_qty(qty: f64, min_qty: f64) -> f64 {
    if min_qty <= 0.0 { return qty; }
    (((qty / min_qty).floor() * min_qty) * 1e8).round() / 1e8
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
    let (ws_tx, ws_rx) = mpsc::channel::<String>(512);
    let pending   = Arc::new(DashMap::<u64, oneshot::Sender<Option<i64>>>::new());
    let dead_orders = Arc::new(DashMap::<i64, ()>::new());

    let client = WsExchangeClient {
        ws_tx,
        pending: pending.clone(),
        dead_orders: dead_orders.clone(),
        order_counter: Arc::new(AtomicU64::new(0)),
    };

    // WS manager: handles connection lifecycle, auth, send/recv, reconnect.
    let ws_url = exchange_ws_url();
    tokio::spawn(ws_manager(ws_url, token, ws_rx, pending, dead_orders));

    // Brief pause for the WS manager to connect and authenticate.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Cancel any leftover orders from a previous session before starting.
    info!("Clearing leftover orders…");
    for cfg in SYMBOLS {
        client.cancel_symbol(cfg.our_symbol).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    info!("Starting market-making on {} symbols", SYMBOLS.len());
    let handles: Vec<_> = SYMBOLS.iter()
        .map(|cfg| {
            let c = client.clone();
            tokio::spawn(async move { run_symbol(cfg, c).await })
        })
        .collect();

    tokio::signal::ctrl_c().await?;
    info!("Shutting down…");
    for h in handles { h.abort(); }
    tokio::time::sleep(Duration::from_secs(2)).await;
    info!("Done.");
    Ok(())
}
