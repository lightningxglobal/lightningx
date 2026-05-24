/// market-maker: mirrors Binance real-time order book depth into LightningX.
///
/// Each symbol runs in its own tokio task connected to Binance's WebSocket
/// partial-book stream (`@depth20@100ms`).  Diff-based updates: only cancel
/// orders at price levels that disappeared from Binance, and only place orders
/// at price levels that are new — unchanged levels are left untouched, cutting
/// typical HTTP traffic from ~80 requests/cycle to 1–5.
use futures_util::{future, StreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_tungstenite::connect_async;
use tracing::{info, warn};

// ── Configuration ─────────────────────────────────────────────────────────────

fn exchange_url() -> String {
    std::env::var("EXCHANGE_URL").unwrap_or_else(|_| "http://localhost:4003".to_string())
}
const ROBOT_EMAIL: &str = "robot@lightningx.exchange";
const ROBOT_PASSWORD: &str = "robot_secret_2026";

const DEPTH_LEVELS: usize = 20;
const QTY_SCALE: f64 = 0.02;
const MAX_USDT_PER_SIDE: f64 = 40000.0;

// Binance USDT-M perpetual WebSocket endpoint.
// Stream: {symbol}@depth20@100ms — full 20-level snapshots every 100ms.
const BINANCE_WS_BASE: &str = "wss://fstream.binance.com/ws";

struct SymbolConfig {
    our_symbol: &'static str,
    binance_stream: &'static str, // e.g. "btcusdt@depth20@100ms"
    min_qty: f64,
}

const SYMBOLS: &[SymbolConfig] = &[
    SymbolConfig { our_symbol: "ETH_USDT", binance_stream: "ethusdt@depth20@100ms", min_qty: 0.001 },
    SymbolConfig { our_symbol: "BTC_USDT", binance_stream: "btcusdt@depth20@100ms", min_qty: 0.0001 },
    SymbolConfig { our_symbol: "SOL_USDT", binance_stream: "solusdt@depth20@100ms", min_qty: 0.01 },
];

// ── Exchange REST client ───────────────────────────────────────────────────────

#[derive(Clone)]
struct ExchangeClient {
    http: Client,
    base: String,
    token: Arc<Mutex<String>>,
}

#[derive(Deserialize)]
struct LoginResponse { token: String }

#[derive(Serialize)]
struct LoginRequest<'a> { email: &'a str, password: &'a str }

#[derive(Serialize)]
struct RegisterRequest<'a> { email: &'a str, password: &'a str, full_name: &'a str }

#[derive(Serialize)]
struct PlaceOrderRequest<'a> {
    symbol: &'a str, side: &'a str, order_type: &'a str, price: f64, quantity: f64,
}

#[derive(Deserialize)]
struct PlaceOrderResponse { id: Option<i64> }

impl ExchangeClient {
    fn new(base: &str) -> Self {
        Self {
            http: Client::builder().timeout(Duration::from_secs(5)).build().unwrap(),
            base: base.to_string(),
            token: Arc::new(Mutex::new(String::new())),
        }
    }

    async fn login(&self) -> anyhow::Result<()> {
        let resp = self.http
            .post(format!("{}/api/auth/login", self.base))
            .json(&LoginRequest { email: ROBOT_EMAIL, password: ROBOT_PASSWORD })
            .send().await?;

        if resp.status().is_success() {
            *self.token.lock().await = resp.json::<LoginResponse>().await?.token;
            info!("Robot authenticated");
            return Ok(());
        }
        if resp.status() == 401 {
            let reg = self.http
                .post(format!("{}/api/auth/register", self.base))
                .json(&RegisterRequest { email: ROBOT_EMAIL, password: ROBOT_PASSWORD, full_name: "Market Maker Robot" })
                .send().await?;
            if !reg.status().is_success() {
                anyhow::bail!("register failed: {}", reg.text().await.unwrap_or_default());
            }
            let resp2 = self.http
                .post(format!("{}/api/auth/login", self.base))
                .json(&LoginRequest { email: ROBOT_EMAIL, password: ROBOT_PASSWORD })
                .send().await?;
            *self.token.lock().await = resp2.json::<LoginResponse>().await?.token;
            info!("Robot registered and authenticated");
            return Ok(());
        }
        anyhow::bail!("login failed: {}", resp.status())
    }

    async fn ensure_funds(&self) -> anyhow::Result<()> {
        let token = self.token.lock().await.clone();
        let resp = self.http
            .post(format!("{}/api/robot-funds", self.base))
            .header("Authorization", format!("Bearer {token}"))
            .send().await?;
        if resp.status().is_success() { info!("Robot inventory topped up"); }
        else { warn!("robot-funds returned {}", resp.status()); }
        Ok(())
    }

    async fn cancel_order(&self, order_id: i64) -> bool {
        let token = self.token.lock().await.clone();
        match self.http
            .delete(format!("{}/api/orders/{order_id}", self.base))
            .header("Authorization", format!("Bearer {token}"))
            .send().await
        {
            Ok(r) => r.status().is_success() || r.status() == 404,
            Err(e) => { warn!("cancel {order_id}: {e}"); false }
        }
    }

    async fn open_order_ids(&self, symbol: &str) -> Vec<i64> {
        let token = self.token.lock().await.clone();
        #[derive(Deserialize)] struct OId { id: i64 }
        match self.http
            .get(format!("{}/api/orders?status=open&symbol={symbol}&limit=500", self.base))
            .header("Authorization", format!("Bearer {token}"))
            .send().await
        {
            Ok(r) if r.status().is_success() =>
                r.json::<Vec<OId>>().await.unwrap_or_default().into_iter().map(|o| o.id).collect(),
            _ => vec![],
        }
    }

    async fn place_order(&self, symbol: &str, side: &str, price: f64, qty: f64) -> Option<i64> {
        let token = self.token.lock().await.clone();
        match self.http
            .post(format!("{}/api/orders", self.base))
            .header("Authorization", format!("Bearer {token}"))
            .json(&PlaceOrderRequest { symbol, side, order_type: "limit", price, quantity: qty })
            .send().await
        {
            Ok(r) if r.status().is_success() =>
                r.json::<PlaceOrderResponse>().await.ok().and_then(|p| p.id),
            Ok(r) => {
                let code = r.status();
                if code != 400 { warn!("place {side} {symbol} @ {price:.2} x {qty:.6} → {code}"); }
                None
            }
            Err(e) => { warn!("place order: {e}"); None }
        }
    }
}

// ── Binance depth snapshot (WS message) ───────────────────────────────────────

#[derive(Deserialize)]
struct BinanceDepthMsg {
    #[serde(rename = "b")]
    bids: Vec<[String; 2]>,
    #[serde(rename = "a")]
    asks: Vec<[String; 2]>,
}

fn parse_levels(raw: &[[String; 2]]) -> Vec<(f64, f64)> {
    raw.iter()
        .filter_map(|pair| {
            let price = pair[0].parse::<f64>().ok()?;
            let qty   = pair[1].parse::<f64>().ok()?;
            Some((price, qty))
        })
        .collect()
}

// ── Diff-based book refresh ────────────────────────────────────────────────────
//
// bid_book / ask_book: f64::to_bits(price) → order_id on LightningX.
//
// On each Binance snapshot we:
//   1. Compute the desired set of orders (with USDT budget cap on bids).
//   2. Cancel only orders whose price level is no longer desired.
//   3. Place only orders for price levels not yet in our book.
//   4. Leave everything else untouched.
//
// Every ORPHAN_CLEANUP_CYCLES BBO-change cycles we reconcile with the server:
//   - Drop book entries for orders that were filled / lost (server says they're gone).
//   - Cancel any server orders we lost track of (orphans).
// After cleanup the next diff naturally re-places any missing levels.
const ORPHAN_CLEANUP_CYCLES: u32 = 50;

async fn refresh_book(
    cfg: &SymbolConfig,
    client: &ExchangeClient,
    bids: &[(f64, f64)],
    asks: &[(f64, f64)],
    bid_book: &mut HashMap<u64, i64>,
    ask_book: &mut HashMap<u64, i64>,
    cycle: u32,
) {
    // Build desired orders (USDT budget cap applies to bids only).
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

    // Orphan cleanup: reconcile local books with actual server state.
    if cycle % ORPHAN_CLEANUP_CYCLES == 0 {
        let open_ids: HashSet<i64> = client.open_order_ids(cfg.our_symbol).await
            .into_iter().collect();
        // Remove filled/rejected entries — they'll be re-placed in the diff below.
        bid_book.retain(|_, id| open_ids.contains(id));
        ask_book.retain(|_, id| open_ids.contains(id));
        // Cancel server orders we lost track of.
        let our_ids: HashSet<i64> = bid_book.values().chain(ask_book.values()).copied().collect();
        let orphans: Vec<i64> = open_ids.into_iter().filter(|id| !our_ids.contains(id)).collect();
        if !orphans.is_empty() {
            warn!("[{}] sweeping {} orphan orders", cfg.our_symbol, orphans.len());
            future::join_all(orphans.iter().map(|&id| client.cancel_order(id))).await;
        }
    }

    // Diff: what to cancel, what to add.
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

    // Fire cancels + places all at the same time.
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

async fn run_symbol(cfg: &'static SymbolConfig, client: ExchangeClient) {
    let mut bid_book: HashMap<u64, i64> = HashMap::new();
    let mut ask_book: HashMap<u64, i64> = HashMap::new();
    let mut last_bid: f64 = 0.0;
    let mut last_ask: f64 = 0.0;
    let mut cycle: u32 = 0;
    let mut reconnect_delay = Duration::from_secs(1);

    info!("[{}] market-making started (Binance WS {})", cfg.our_symbol, cfg.binance_stream);

    loop {
        let url = format!("{BINANCE_WS_BASE}/{}", cfg.binance_stream);
        let ws = match connect_async(&url).await {
            Ok((ws, _)) => { reconnect_delay = Duration::from_secs(1); ws }
            Err(e) => {
                warn!("[{}] WS connect failed: {e}, retry in {reconnect_delay:?}", cfg.our_symbol);
                tokio::time::sleep(reconnect_delay).await;
                reconnect_delay = (reconnect_delay * 2).min(Duration::from_secs(30));
                continue;
            }
        };
        info!("[{}] WS connected", cfg.our_symbol);

        let (_, mut read) = ws.split();

        while let Some(msg) = read.next().await {
            let text = match msg {
                Ok(tokio_tungstenite::tungstenite::Message::Text(t)) => t,
                Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => break,
                Err(e) => { warn!("[{}] WS error: {e}", cfg.our_symbol); break; }
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

        warn!("[{}] WS disconnected, reconnecting in {reconnect_delay:?}…", cfg.our_symbol);
        tokio::time::sleep(reconnect_delay).await;
        reconnect_delay = (reconnect_delay * 2).min(Duration::from_secs(30));
    }
}

/// Round qty down to the nearest multiple of min_qty.
fn round_qty(qty: f64, min_qty: f64) -> f64 {
    if min_qty <= 0.0 { return qty; }
    (((qty / min_qty).floor() * min_qty) * 1e8).round() / 1e8
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    info!("LightningX Market Maker starting…");

    let client = ExchangeClient::new(&exchange_url());

    let mut attempts = 0u32;
    loop {
        match client.login().await {
            Ok(_) => break,
            Err(e) => {
                attempts += 1;
                if attempts >= 5 { anyhow::bail!("cannot authenticate after 5 attempts: {e}"); }
                warn!("auth failed ({e}), retry in 3s…");
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        }
    }

    client.ensure_funds().await?;
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
