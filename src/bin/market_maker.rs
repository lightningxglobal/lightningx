/// market-maker: mirrors Binance real-time order book depth into LightningX.
///
/// Each symbol runs in its own tokio task connected to Binance's WebSocket
/// partial-book stream (`@depth20@100ms`).  Cancel+replace fires only when
/// the best bid or ask price changes, so we react as fast as Binance pushes
/// without flooding the exchange with no-op cycles.
use futures_util::StreamExt;
use reqwest::Client;
use serde::{Deserialize, Serialize};
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

// ── Cancel all open + place fresh book ────────────────────────────────────────

async fn refresh_book(
    cfg: &SymbolConfig,
    client: &ExchangeClient,
    bids: &[(f64, f64)],
    asks: &[(f64, f64)],
    tracked: &mut Vec<i64>,
) {
    tracked.clear();
    for id in client.open_order_ids(cfg.our_symbol).await {
        client.cancel_order(id).await;
    }

    let mut usdt_spent = 0.0_f64;
    for (price, binance_qty) in bids.iter().take(DEPTH_LEVELS) {
        let qty = round_qty((binance_qty * QTY_SCALE).max(cfg.min_qty), cfg.min_qty);
        let cost = price * qty;
        if usdt_spent + cost > MAX_USDT_PER_SIDE { break; }
        usdt_spent += cost;
        if let Some(id) = client.place_order(cfg.our_symbol, "buy", *price, qty).await {
            tracked.push(id);
        }
    }
    for (price, binance_qty) in asks.iter().take(DEPTH_LEVELS) {
        let qty = round_qty((binance_qty * QTY_SCALE).max(cfg.min_qty), cfg.min_qty);
        if let Some(id) = client.place_order(cfg.our_symbol, "sell", *price, qty).await {
            tracked.push(id);
        }
    }
}

// ── Per-symbol WebSocket loop ─────────────────────────────────────────────────

async fn run_symbol(cfg: &'static SymbolConfig, client: ExchangeClient) {
    let mut tracked: Vec<i64> = Vec::with_capacity(DEPTH_LEVELS * 2);
    let mut last_bid: f64 = 0.0;
    let mut last_ask: f64 = 0.0;
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

            // Only refresh when the BBO moves — no-op ticks cost nothing.
            if (best_bid - last_bid).abs() < 1e-9 && (best_ask - last_ask).abs() < 1e-9 {
                continue;
            }
            last_bid = best_bid;
            last_ask = best_ask;

            refresh_book(cfg, &client, &bids, &asks, &mut tracked).await;

            info!(
                "[{}] refreshed {} orders  bbo {:.2}/{:.2}",
                cfg.our_symbol, tracked.len(), best_bid, best_ask,
            );
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
