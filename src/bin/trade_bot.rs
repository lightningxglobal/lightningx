/// trade-bot: places small market orders periodically to generate live trade flow.
///
/// Cycles through all symbols round-robin, alternating buy/sell each cycle,
/// so Recent Trades always has fresh entries without drifting inventory too far.
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{info, warn};

fn exchange_url() -> String {
    std::env::var("EXCHANGE_URL").unwrap_or_else(|_| "http://localhost:4003".to_string())
}

const ROBOT_EMAIL: &str = "robot@lightningx.exchange";
const ROBOT_PASSWORD: &str = "robot_secret_2026";
const TRADE_INTERVAL_MS: u64 = 3000;

struct SymbolConfig {
    symbol: &'static str,
    qty: f64,
}

const SYMBOLS: &[SymbolConfig] = &[
    SymbolConfig { symbol: "ETH_USDT", qty: 0.01 },
    SymbolConfig { symbol: "BTC_USDT", qty: 0.0001 },
    SymbolConfig { symbol: "SOL_USDT", qty: 0.1 },
];

#[derive(Serialize)]
struct LoginRequest<'a> { email: &'a str, password: &'a str }

#[derive(Deserialize)]
struct LoginResponse { token: String }

#[derive(Serialize)]
struct PlaceOrderRequest<'a> {
    symbol: &'a str,
    side: &'a str,
    order_type: &'a str,
    quantity: f64,
}

#[derive(Deserialize)]
struct PlaceOrderResponse { id: Option<i64> }

async fn login(http: &Client, base: &str) -> anyhow::Result<String> {
    let resp = http
        .post(format!("{base}/api/auth/login"))
        .json(&LoginRequest { email: ROBOT_EMAIL, password: ROBOT_PASSWORD })
        .send().await?;
    if !resp.status().is_success() {
        anyhow::bail!("login failed: {}", resp.status());
    }
    Ok(resp.json::<LoginResponse>().await?.token)
}

async fn place_market(http: &Client, base: &str, token: &str, symbol: &str, side: &str, qty: f64) {
    let res = http
        .post(format!("{base}/api/orders"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&PlaceOrderRequest { symbol, side, order_type: "market", quantity: qty })
        .send().await;
    match res {
        Ok(r) if r.status().is_success() => {
            if let Ok(p) = r.json::<PlaceOrderResponse>().await {
                info!("[{symbol}] market {side} {qty} → id={:?}", p.id);
            }
        }
        Ok(r) => warn!("[{symbol}] market {side} → {}", r.status()),
        Err(e) => warn!("[{symbol}] market {side} error: {e}"),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let base = exchange_url();
    let http = Client::builder().timeout(Duration::from_secs(5)).build()?;

    info!("Trade bot starting…");

    // Retry login until desk-server is ready.
    let mut token = String::new();
    for attempt in 1..=10 {
        match login(&http, &base).await {
            Ok(t) => { token = t; break; }
            Err(e) => {
                if attempt == 10 { anyhow::bail!("cannot login: {e}"); }
                warn!("login failed ({e}), retry in 3s…");
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        }
    }
    info!("Authenticated");

    // Ensure funds
    let _ = http.post(format!("{base}/api/robot-funds"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await;

    info!("Starting trade loop ({TRADE_INTERVAL_MS}ms interval, {} symbols)", SYMBOLS.len());

    let mut cycle: u64 = 0;
    loop {
        tokio::time::sleep(Duration::from_millis(TRADE_INTERVAL_MS)).await;

        let cfg = &SYMBOLS[cycle as usize % SYMBOLS.len()];
        let side = if cycle % 2 == 0 { "buy" } else { "sell" };
        cycle += 1;

        place_market(&http, &base, &token, cfg.symbol, side, cfg.qty).await;
    }
}
