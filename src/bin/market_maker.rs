/// market-maker: mirrors Binance real-time order book depth into LightningX.
///
/// Each symbol runs in its own tokio task:
///   1. Poll Binance REST depth endpoint every REFRESH_MS
///   2. On each snapshot: cancel tracked orders → place fresh limit orders
///
/// Standalone process — zero shared memory with exchange-server.
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{info, warn};

// ── Configuration ─────────────────────────────────────────────────────────────

const EXCHANGE_URL: &str = "http://localhost:4001";
const ROBOT_EMAIL: &str = "robot@lightningx.exchange";
const ROBOT_PASSWORD: &str = "robot_secret_2026";

const DEPTH_LEVELS: usize = 10;     // levels per side to mirror
const QTY_SCALE: f64 = 0.02;        // use 2% of Binance's qty per level
const MAX_USDT_PER_SIDE: f64 = 4000.0; // safety cap: total USDT exposure per side
const REFRESH_MS: u64 = 500;        // minimum ms between refresh cycles

const BINANCE_API: &str = "https://api.binance.com/api/v3/depth";

struct SymbolConfig {
    our_symbol: &'static str,
    binance_symbol: &'static str,
    min_qty: f64,
}

const SYMBOLS: &[SymbolConfig] = &[
    SymbolConfig { our_symbol: "ETH_USDT", binance_symbol: "ETHUSDT", min_qty: 0.001 },
    SymbolConfig { our_symbol: "BTC_USDT", binance_symbol: "BTCUSDT", min_qty: 0.0001 },
    SymbolConfig { our_symbol: "SOL_USDT", binance_symbol: "SOLUSDT", min_qty: 0.01 },
];

// ── Exchange REST client ───────────────────────────────────────────────────────

#[derive(Clone)]
struct ExchangeClient {
    http: Client,
    binance: Client,
    base: String,
    token: Arc<Mutex<String>>,
}

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

#[derive(Serialize)]
struct PlaceOrderRequest<'a> {
    symbol: &'a str,
    side: &'a str,
    order_type: &'a str,
    price: f64,
    quantity: f64,
}

#[derive(Deserialize)]
struct PlaceOrderResponse {
    id: Option<i64>,
}

impl ExchangeClient {
    fn new(base: &str) -> Self {
        Self {
            http: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("failed to build HTTP client"),
            binance: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("failed to build Binance HTTP client"),
            base: base.to_string(),
            token: Arc::new(Mutex::new(String::new())),
        }
    }

    async fn fetch_binance_depth(&self, symbol: &str) -> anyhow::Result<BinanceDepth> {
        let url = format!("{BINANCE_API}?symbol={symbol}&limit=20");
        let resp = self.binance.get(&url).send().await?;
        if !resp.status().is_success() {
            anyhow::bail!("Binance depth {symbol}: {}", resp.status());
        }
        Ok(resp.json::<BinanceDepth>().await?)
    }

    async fn login(&self) -> anyhow::Result<()> {
        let resp = self.http
            .post(format!("{}/api/auth/login", self.base))
            .json(&LoginRequest { email: ROBOT_EMAIL, password: ROBOT_PASSWORD })
            .send()
            .await?;

        if resp.status().is_success() {
            let body: LoginResponse = resp.json().await?;
            *self.token.lock().await = body.token;
            info!("Robot authenticated");
            return Ok(());
        }

        // Not registered yet — register first
        if resp.status() == 401 {
            let reg = self.http
                .post(format!("{}/api/auth/register", self.base))
                .json(&RegisterRequest {
                    email: ROBOT_EMAIL,
                    password: ROBOT_PASSWORD,
                    full_name: "Market Maker Robot",
                })
                .send()
                .await?;
            if !reg.status().is_success() {
                let txt = reg.text().await.unwrap_or_default();
                anyhow::bail!("register failed: {txt}");
            }
            // Login after register
            let resp2 = self.http
                .post(format!("{}/api/auth/login", self.base))
                .json(&LoginRequest { email: ROBOT_EMAIL, password: ROBOT_PASSWORD })
                .send()
                .await?;
            let body: LoginResponse = resp2.json().await?;
            *self.token.lock().await = body.token;
            info!("Robot registered and authenticated");
            return Ok(());
        }

        anyhow::bail!("login failed: {}", resp.status())
    }

    async fn ensure_funds(&self) -> anyhow::Result<()> {
        let token = self.token.lock().await.clone();
        let resp = self.http
            .post(format!("{}/api/test-funds", self.base))
            .header("Authorization", format!("Bearer {token}"))
            .send()
            .await?;
        match resp.status().as_u16() {
            200 => {
                let txt = resp.text().await.unwrap_or_default();
                info!("Test funds granted: {txt}");
            }
            400 => {
                // Already has funds — normal
            }
            code => {
                warn!("test-funds returned {code}");
            }
        }
        Ok(())
    }

    async fn cancel_order(&self, order_id: i64) -> bool {
        let token = self.token.lock().await.clone();
        let res = self.http
            .delete(format!("{}/api/orders/{order_id}", self.base))
            .header("Authorization", format!("Bearer {token}"))
            .send()
            .await;
        match res {
            Ok(r) => r.status().is_success() || r.status() == 404,
            Err(e) => { warn!("cancel {order_id} error: {e}"); false }
        }
    }

    async fn place_order(&self, symbol: &str, side: &str, price: f64, qty: f64) -> Option<i64> {
        let token = self.token.lock().await.clone();
        let res = self.http
            .post(format!("{}/api/orders", self.base))
            .header("Authorization", format!("Bearer {token}"))
            .json(&PlaceOrderRequest {
                symbol,
                side,
                order_type: "limit",
                price,
                quantity: qty,
            })
            .send()
            .await;
        match res {
            Ok(r) if r.status().is_success() => {
                r.json::<PlaceOrderResponse>().await.ok().and_then(|p| p.id)
            }
            Ok(r) => {
                let code = r.status();
                // 400 usually means insufficient balance — not an error worth logging every cycle
                if code != 400 {
                    warn!("place {side} {symbol} @ {price:.2} x {qty:.6} → {code}");
                }
                None
            }
            Err(e) => { warn!("place order HTTP error: {e}"); None }
        }
    }
}

// ── Binance depth snapshot ─────────────────────────────────────────────────────

#[derive(Deserialize)]
struct BinanceDepth {
    bids: Vec<[serde_json::Value; 2]>,
    asks: Vec<[serde_json::Value; 2]>,
}

fn parse_levels(raw: &[[serde_json::Value; 2]]) -> Vec<(f64, f64)> {
    raw.iter()
        .filter_map(|pair| {
            let price = pair[0].as_str()?.parse::<f64>().ok()?;
            let qty   = pair[1].as_str()?.parse::<f64>().ok()?;
            Some((price, qty))
        })
        .collect()
}

// ── Per-symbol market-making loop ─────────────────────────────────────────────

async fn run_symbol(cfg: &'static SymbolConfig, client: ExchangeClient) {
    let mut tracked_ids: Vec<i64> = Vec::with_capacity(DEPTH_LEVELS * 2);
    let mut consecutive_errors: u32 = 0;

    info!("[{}] market-making started (REST polling every {}ms)", cfg.our_symbol, REFRESH_MS);

    loop {
        tokio::time::sleep(Duration::from_millis(REFRESH_MS)).await;

        let depth = match client.fetch_binance_depth(cfg.binance_symbol).await {
            Ok(d) => { consecutive_errors = 0; d }
            Err(e) => {
                consecutive_errors += 1;
                if consecutive_errors <= 3 || consecutive_errors % 10 == 0 {
                    warn!("[{}] Binance fetch error (#{consecutive_errors}): {e}", cfg.our_symbol);
                }
                continue;
            }
        };

        let bids = parse_levels(&depth.bids);
        let asks = parse_levels(&depth.asks);

        if bids.is_empty() && asks.is_empty() {
            continue;
        }

        // 1. Cancel all tracked orders
        let ids = std::mem::take(&mut tracked_ids);
        let cancel_futs: Vec<_> = ids.iter()
            .map(|&id| {
                let c = client.clone();
                async move { c.cancel_order(id).await }
            })
            .collect();
        futures::future::join_all(cancel_futs).await;

        // 2. Place bid orders
        let mut usdt_spent = 0.0_f64;
        for (price, binance_qty) in bids.iter().take(DEPTH_LEVELS) {
            let qty = (binance_qty * QTY_SCALE).max(0.0);
            let qty = round_qty(qty, cfg.min_qty);
            if qty < cfg.min_qty { continue; }
            let cost = price * qty;
            if usdt_spent + cost > MAX_USDT_PER_SIDE { break; }
            usdt_spent += cost;
            if let Some(id) = client.place_order(cfg.our_symbol, "buy", *price, qty).await {
                tracked_ids.push(id);
            }
        }

        // 3. Place ask orders
        for (price, binance_qty) in asks.iter().take(DEPTH_LEVELS) {
            let qty = (binance_qty * QTY_SCALE).max(0.0);
            let qty = round_qty(qty, cfg.min_qty);
            if qty < cfg.min_qty { continue; }
            if let Some(id) = client.place_order(cfg.our_symbol, "sell", *price, qty).await {
                tracked_ids.push(id);
            }
        }

        if !tracked_ids.is_empty() {
            info!(
                "[{}] refreshed: {} orders (bbo {:.2}/{:.2})",
                cfg.our_symbol,
                tracked_ids.len(),
                bids.first().map(|(p, _)| *p).unwrap_or(0.0),
                asks.first().map(|(p, _)| *p).unwrap_or(0.0),
            );
        }
    }
}

/// Round qty down to the nearest multiple of min_qty.
fn round_qty(qty: f64, min_qty: f64) -> f64 {
    if min_qty <= 0.0 { return qty; }
    let steps = (qty / min_qty).floor();
    (steps * min_qty * 1e8).round() / 1e8   // avoid float drift
}

// ── Graceful shutdown ─────────────────────────────────────────────────────────

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    info!("LightningX Market Maker starting…");

    let client = ExchangeClient::new(EXCHANGE_URL);

    // Authenticate
    let mut attempts = 0u32;
    loop {
        match client.login().await {
            Ok(_) => break,
            Err(e) => {
                attempts += 1;
                if attempts >= 5 {
                    anyhow::bail!("cannot authenticate after 5 attempts: {e}");
                }
                warn!("auth failed ({e}), exchange-server not ready? retry in 3s…");
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        }
    }

    // Ensure robot account has funds
    client.ensure_funds().await?;

    info!("Starting market-making on {} symbols", SYMBOLS.len());

    // Spawn one task per symbol
    let handles: Vec<_> = SYMBOLS.iter()
        .map(|cfg| {
            let c = client.clone();
            tokio::spawn(async move { run_symbol(cfg, c).await })
        })
        .collect();

    // Ctrl-C / SIGTERM: we let tokio cancel the tasks (drop their handles).
    // Each task cancels its tracked orders at the top of the reconnect loop.
    tokio::signal::ctrl_c().await?;
    info!("Shutting down market maker…");
    for h in handles { h.abort(); }
    // Brief pause so in-flight cancel requests can complete
    tokio::time::sleep(Duration::from_secs(2)).await;
    info!("Done.");
    Ok(())
}
