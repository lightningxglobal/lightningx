/// trade-bot: places market orders continuously to generate live trade flow.
///
/// Fires all symbols in parallel every TRADE_INTERVAL_MS ms (not round-robin),
/// alternating buy/sell each cycle with random quantity jitter.
/// Result: ~5 trades/sec per symbol = 15 trades/sec across 3 symbols.
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{info, warn};

fn exchange_url() -> String {
    std::env::var("EXCHANGE_URL").unwrap_or_else(|_| "http://localhost:4003".to_string())
}

// Recommended: trade-bot runs as its OWN account so its order_updates
// don't fan out to the market-maker's WS channel (desk-server's user_tx
// is keyed by user_id; two clients sharing a user share one WS event
// stream — see shared_user_ws_fanout memory). Falls back to the shared
// robot account when TRADE_BOT_EMAIL is unset so single-user demo
// setups still work.
const DEFAULT_TRADE_BOT_EMAIL: &str = "tradebot@lightningx.exchange";
const DEFAULT_TRADE_BOT_PASSWORD: &str = "tradebot_secret_2026";
const ROBOT_EMAIL: &str = "robot@lightningx.exchange";
const ROBOT_PASSWORD: &str = "robot_secret_2026";
const TRADE_INTERVAL_MS: u64 = 3000;

fn trade_bot_email() -> String {
    std::env::var("TRADE_BOT_EMAIL").unwrap_or_else(|_| DEFAULT_TRADE_BOT_EMAIL.to_string())
}
fn trade_bot_password() -> String {
    std::env::var("TRADE_BOT_PASSWORD")
        .unwrap_or_else(|_| DEFAULT_TRADE_BOT_PASSWORD.to_string())
}

struct SymbolConfig {
    symbol: &'static str,
    base_qty: f64,
}

const SYMBOLS: &[SymbolConfig] = &[
    SymbolConfig { symbol: "BTC_USDT", base_qty: 0.003 },
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

async fn login_with(
    http: &Client,
    base: &str,
    email: &str,
    password: &str,
) -> anyhow::Result<String> {
    let resp = http
        .post(format!("{base}/api/auth/login"))
        .json(&LoginRequest { email, password })
        .send()
        .await?;
    if !resp.status().is_success() {
        anyhow::bail!("login failed: {}", resp.status());
    }
    Ok(resp.json::<LoginResponse>().await?.token)
}

#[derive(Serialize)]
struct RegisterRequest<'a> {
    email: &'a str,
    password: &'a str,
}

async fn register(http: &Client, base: &str, email: &str, password: &str) -> anyhow::Result<()> {
    let resp = http
        .post(format!("{base}/api/auth/register"))
        .json(&RegisterRequest { email, password })
        .send()
        .await?;
    if !resp.status().is_success() {
        // 409 / already-exists is acceptable.
        let s = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !body.contains("already") && s.as_u16() != 409 {
            anyhow::bail!("register failed: {s} body={body}");
        }
    }
    Ok(())
}

/// Login as the trade-bot's own account; fall back to the shared robot
/// account when registration/login both fail (single-user demo).
async fn login(http: &Client, base: &str) -> anyhow::Result<String> {
    let bot_email = trade_bot_email();
    let bot_pass = trade_bot_password();

    // Try the trade-bot account first.
    if let Ok(token) = login_with(http, base, &bot_email, &bot_pass).await {
        info!("trade-bot logged in as {bot_email}");
        return Ok(token);
    }
    // Auto-register on first run.
    if register(http, base, &bot_email, &bot_pass).await.is_ok() {
        if let Ok(token) = login_with(http, base, &bot_email, &bot_pass).await {
            info!("trade-bot registered + logged in as {bot_email}");
            // Note: new accounts have 0 balance. Operator must fund via
            // /api/test-funds before trade-bot can place orders.
            warn!(
                "trade-bot account {bot_email} just registered; needs funding via /api/test-funds"
            );
            return Ok(token);
        }
    }

    // Fall back to the shared robot account so single-user demos keep
    // working out of the box. Expected to share fanout with MM.
    warn!("falling back to shared robot account — MM will see trade-bot fills (cosmetic noise)");
    login_with(http, base, ROBOT_EMAIL, ROBOT_PASSWORD).await
}

/// Return a pseudo-random multiplier in [1.0, 2.0] based on seed.
/// Uses a simple LCG to avoid pulling in a rand crate.
fn qty_jitter(seed: u64) -> f64 {
    let x = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
    1.0 + (x >> 60) as f64 / 15.0   // 1.0 + [0..15]/15 = [1.0, 2.0]
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
                info!("[{symbol}] market {side} {qty:.4} → id={:?}", p.id);
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

    let _ = http.post(format!("{base}/api/robot-funds"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await;

    info!(
        "Starting trade loop ({}ms interval, {} symbols in parallel)",
        TRADE_INTERVAL_MS, SYMBOLS.len()
    );

    let mut cycle: u64 = 0;
    loop {
        tokio::time::sleep(Duration::from_millis(TRADE_INTERVAL_MS)).await;

        let side = if cycle % 2 == 0 { "buy" } else { "sell" };

        // Fire all symbols in parallel with jittered quantities.
        let futs: Vec<_> = SYMBOLS.iter().enumerate().map(|(i, cfg)| {
            let jitter = qty_jitter(cycle.wrapping_add(i as u64 * 31337));
            let qty = (cfg.base_qty * jitter * 1e4).round() / 1e4;
            let qty = qty.max(cfg.base_qty);  // floor at base_qty
            place_market(&http, &base, &token, cfg.symbol, side, qty)
        }).collect();

        futures::future::join_all(futs).await;

        cycle += 1;
    }
}
