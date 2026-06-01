//! pressure_client: WebSocket load generator against desk-server.
//!
//! Goal: see how many WS connections + ops/s the system holds.
//!
//! Per env vars:
//!   PRESSURE_USERS         # of test users to register/login   (default 10000)
//!   PRESSURE_CONNS         # of WS connections to open         (default 5000)
//!   PRESSURE_OPS_PER_SEC   per-connection place/cancel rate    (default 0.2 = 1 cycle per 5s)
//!   PRESSURE_DURATION_S    test duration in seconds            (default 60)
//!   PRESSURE_RAMP_S        seconds to ramp connections up      (default 30)
//!   PRESSURE_BASE_URL      desk-server base url                (default http://localhost:4003)
//!   PRESSURE_SYMBOL        symbol to trade                     (default BTC_USDT)
//!   PRESSURE_PRICE         limit price (far from market)       (default 1.0  — won't fill)
//!   PRESSURE_QTY           order quantity                      (default 0.0001)
//!
//! Workload per connection (single repeating cycle):
//!   place_order → wait WAIT_AFTER_PLACE → cancel_order → wait remainder.
//!   Cycle period is `1 / PRESSURE_OPS_PER_SEC` seconds.
//!
//! Setup phase:
//!   Registers/logs in PRESSURE_USERS accounts (`pressure_{i}@stress.test`)
//!   sequentially up-front. Each gets test-funds. Tokens reused across
//!   connections.
//!
//! Connect phase:
//!   Connections opened over `PRESSURE_RAMP_S` seconds. Each is assigned
//!   a user round-robin from the pool.
//!
//! Reporting:
//!   Per-second snapshot of [conn_open, conn_failed, place_ok, place_fail,
//!   cancel_ok, cancel_fail, place_p50/p99, cancel_p50/p99] printed to
//!   stdout. Final summary at the end.

use anyhow::Context;
use fastwebsockets::{handshake, Frame, OpCode, Payload, WebSocket};
use http_body_util::Empty;
use hyper::{
    body::Bytes,
    header::{CONNECTION, UPGRADE},
    Request,
};
use hyper_util::rt::TokioIo;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tracing::{info, warn};

// ── Configuration via env ────────────────────────────────────────────────────

fn env_usize(k: &str, default: usize) -> usize {
    std::env::var(k)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}
fn env_f64(k: &str, default: f64) -> f64 {
    std::env::var(k)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}
fn env_string(k: &str, default: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| default.to_string())
}

fn default_run_id() -> String {
    let micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0);
    let mut buf = [0u8; 13];
    let mut n = micros;
    let mut i = buf.len();
    while n > 0 {
        i -= 1;
        let digit = (n % 36) as u8;
        buf[i] = if digit < 10 {
            b'0' + digit
        } else {
            b'a' + (digit - 10)
        };
        n /= 36;
    }
    std::str::from_utf8(&buf[i..]).unwrap_or("0").to_string()
}

struct Config {
    users: usize,
    conns: usize,
    ops_per_sec: f64,
    duration_s: u64,
    ramp_s: u64,
    base_url: String,
    symbol: String,
    price: f64,
    qty: f64,
    run_id: String,
    /// Comma-separated list of source IPs to bind each connection's
    /// socket to. With multiple loopback aliases (127.0.0.1, 127.0.0.2,
    /// …) we can scale past the single-IP ~16K ephemeral-port cap on
    /// macOS. Empty → kernel picks (default 127.0.0.1).
    source_ips: Vec<std::net::IpAddr>,
}

impl Config {
    fn from_env() -> Self {
        Self {
            users: env_usize("PRESSURE_USERS", 10_000),
            conns: env_usize("PRESSURE_CONNS", 5_000),
            ops_per_sec: env_f64("PRESSURE_OPS_PER_SEC", 0.2),
            duration_s: env_usize("PRESSURE_DURATION_S", 60) as u64,
            ramp_s: env_usize("PRESSURE_RAMP_S", 30) as u64,
            base_url: env_string("PRESSURE_BASE_URL", "http://localhost:4003"),
            symbol: env_string("PRESSURE_SYMBOL", "BTC_USDT"),
            // Default price 5000 × qty 0.001 = $5 notional (BTC_USDT
            // min_notional is $5). Far below market (~74k) → orders rest
            // without filling, won't affect MM quotes.
            price: env_f64("PRESSURE_PRICE", 5000.0),
            qty: env_f64("PRESSURE_QTY", 0.001),
            run_id: std::env::var("PRESSURE_RUN_ID").unwrap_or_else(|_| default_run_id()),
            source_ips: env_string("PRESSURE_SOURCE_IPS", "")
                .split(',')
                .filter_map(|s| {
                    let s = s.trim();
                    if s.is_empty() {
                        None
                    } else {
                        s.parse::<std::net::IpAddr>().ok()
                    }
                })
                .collect(),
        }
    }
}

// ── Metrics ──────────────────────────────────────────────────────────────────

#[derive(Default)]
struct Metrics {
    // Connection lifecycle.
    conn_attempted: AtomicU64,
    conn_open: AtomicUsize, // gauge
    conn_failed: AtomicU64,
    conn_closed: AtomicU64,

    // Order flow (cumulative counters).
    place_sent: AtomicU64,
    place_ok: AtomicU64,
    place_fail: AtomicU64,
    cancel_sent: AtomicU64,
    cancel_ok: AtomicU64,
    cancel_fail: AtomicU64,

    // Latency accumulators (sum µs + count → can derive mean; not perfect
    // p50/p99, but enough for a load test). HdrHistogram is heavier than
    // this tool wants for now.
    place_latency_us_sum: AtomicU64,
    place_latency_us_max: AtomicU64,
    cancel_latency_us_sum: AtomicU64,
    cancel_latency_us_max: AtomicU64,
    place_latency_hist: LatencyHist,
    cancel_latency_hist: LatencyHist,
}

// Keep 1µs precision around the sub-ms target while still diagnosing
// overload runs that drift into hundreds of milliseconds.
const LATENCY_HIST_MAX_US: usize = 1_000_000;

struct LatencyHist {
    buckets: Vec<AtomicU64>,
}

impl Default for LatencyHist {
    fn default() -> Self {
        Self {
            buckets: (0..=LATENCY_HIST_MAX_US)
                .map(|_| AtomicU64::new(0))
                .collect(),
        }
    }
}

impl LatencyHist {
    fn record(&self, value_us: u64) {
        let idx = (value_us as usize).min(LATENCY_HIST_MAX_US);
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
    }

    fn percentile(&self, pct: f64) -> u64 {
        let total: u64 = self.buckets.iter().map(|b| b.load(Ordering::Relaxed)).sum();
        if total == 0 {
            return 0;
        }
        let rank = ((total as f64 * pct).ceil() as u64).max(1);
        let mut seen = 0u64;
        for (idx, bucket) in self.buckets.iter().enumerate() {
            seen += bucket.load(Ordering::Relaxed);
            if seen >= rank {
                return idx as u64;
            }
        }
        LATENCY_HIST_MAX_US as u64
    }
}

impl Metrics {
    fn snapshot(&self) -> MetricsSnap {
        MetricsSnap {
            conn_attempted: self.conn_attempted.load(Ordering::Relaxed),
            conn_open: self.conn_open.load(Ordering::Relaxed),
            conn_failed: self.conn_failed.load(Ordering::Relaxed),
            conn_closed: self.conn_closed.load(Ordering::Relaxed),
            place_sent: self.place_sent.load(Ordering::Relaxed),
            place_ok: self.place_ok.load(Ordering::Relaxed),
            place_fail: self.place_fail.load(Ordering::Relaxed),
            cancel_sent: self.cancel_sent.load(Ordering::Relaxed),
            cancel_ok: self.cancel_ok.load(Ordering::Relaxed),
            cancel_fail: self.cancel_fail.load(Ordering::Relaxed),
            place_lat_sum_us: self.place_latency_us_sum.load(Ordering::Relaxed),
            place_lat_max_us: self.place_latency_us_max.load(Ordering::Relaxed),
            cancel_lat_sum_us: self.cancel_latency_us_sum.load(Ordering::Relaxed),
            cancel_lat_max_us: self.cancel_latency_us_max.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Copy)]
struct MetricsSnap {
    conn_attempted: u64,
    conn_open: usize,
    conn_failed: u64,
    conn_closed: u64,
    place_sent: u64,
    place_ok: u64,
    place_fail: u64,
    cancel_sent: u64,
    cancel_ok: u64,
    cancel_fail: u64,
    place_lat_sum_us: u64,
    place_lat_max_us: u64,
    cancel_lat_sum_us: u64,
    cancel_lat_max_us: u64,
}

fn record_max(slot: &AtomicU64, v: u64) {
    let mut cur = slot.load(Ordering::Relaxed);
    while v > cur {
        match slot.compare_exchange_weak(cur, v, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(actual) => cur = actual,
        }
    }
}

// ── WS connection helpers (cribbed from market_maker.rs) ─────────────────────

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

fn parse_ws_url(url: &str) -> (String, u16, String) {
    let rest = url.strip_prefix("ws://").unwrap_or(url);
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], rest[i..].to_string()),
        None => (rest, "/".to_string()),
    };
    let (host, port) = if let Some(colon) = authority.rfind(':') {
        let port = authority[colon + 1..].parse().unwrap_or(80);
        (authority[..colon].to_string(), port)
    } else {
        (authority.to_string(), 80)
    };
    (host, port, path)
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

async fn ws_connect_plain_from(
    url: &str,
    bind_ip: Option<std::net::IpAddr>,
) -> anyhow::Result<WebSocket<TokioIo<hyper::upgrade::Upgraded>>> {
    let (host, port, path) = parse_ws_url(url);
    let _ = &host; // silence unused warning when bind_ip path skips host lookup
    let tcp = if let Some(ip) = bind_ip {
        // Bind to a specific loopback alias so we can spread connections
        // across multiple source IPs and bypass the single-IP ephemeral
        // port cap (~16K on macOS / ~28K on Linux).
        let socket = tokio::net::TcpSocket::new_v4()?;
        socket.bind(std::net::SocketAddr::new(ip, 0))?;
        // Properly await DNS resolution and filter to IPv4 — `localhost`
        // typically resolves to both `[::1]` (v6) and `127.0.0.1` (v4),
        // and the v6 entry first won't connect from an v4-bound socket
        // ("Address family not supported by protocol family", errno 47).
        let dst = match format!("{host}:{port}").parse::<std::net::SocketAddr>() {
            Ok(a) if a.is_ipv4() => a,
            Ok(_) => {
                return Err(anyhow::anyhow!(
                    "bind_ip only supports v4 destinations; got {host}"
                ))
            }
            Err(_) => {
                let addrs = tokio::net::lookup_host(format!("{host}:{port}")).await?;
                addrs.filter(|a| a.is_ipv4()).next().ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, "no ipv4 addr resolved")
                })?
            }
        };
        socket.connect(dst).await?
    } else {
        TcpStream::connect(format!("{host}:{port}")).await?
    };
    tcp.set_nodelay(true).ok();
    let req = ws_upgrade_request(&host, &path)?;
    let (ws, _) = handshake::client(&SpawnExecutor, req, tcp).await?;
    Ok(ws)
}

// ── Setup phase ──────────────────────────────────────────────────────────────

#[derive(Clone)]
struct UserToken {
    user_id: i64,
    token: String,
}

async fn ensure_user(
    http: &reqwest::Client,
    base_url: &str,
    idx: usize,
) -> anyhow::Result<UserToken> {
    let email = format!("pressure_{idx}@stress.test");
    let password = "pressure-secret-2026";

    // Try login first.
    let login = http
        .post(format!("{base_url}/api/auth/login"))
        .json(&json!({"email": &email, "password": password}))
        .send()
        .await?;
    let token: String;
    let user_id: i64;
    if login.status().is_success() {
        let body: Value = login.json().await?;
        token = body["token"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("login: no token"))?
            .to_string();
        user_id = body["user"]["id"]
            .as_i64()
            .ok_or_else(|| anyhow::anyhow!("login: no user.id"))?;
    } else {
        // Register on cold pool.
        let reg = http
            .post(format!("{base_url}/api/auth/register"))
            .json(&json!({"email": &email, "password": password}))
            .send()
            .await?;
        if !reg.status().is_success() {
            anyhow::bail!(
                "register failed: {} {}",
                reg.status(),
                reg.text().await.unwrap_or_default()
            );
        }
        let body: Value = reg.json().await?;
        token = body["token"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("register: no token"))?
            .to_string();
        user_id = body["user"]["id"]
            .as_i64()
            .ok_or_else(|| anyhow::anyhow!("register: no user.id"))?;
        // Fresh accounts: fund via /api/test-funds (10000 USDT + 1 BTC).
        let _ = http
            .post(format!("{base_url}/api/test-funds"))
            .bearer_auth(&token)
            .send()
            .await;
    }
    Ok(UserToken { user_id, token })
}

/// Read PRESSURE_TOKENS_CSV (lines `user_id,idx`), self-sign JWT for each
/// using the desk-server's hardcoded JWT_SECRET — bypasses HTTP login
/// entirely. Drops setup for 400K users from ~10 min (bcrypt-bound) to
/// ~10 s (local HMAC-SHA256 signing).
fn setup_users_from_csv(cfg: &Config, path: &str) -> anyhow::Result<Arc<Vec<UserToken>>> {
    use jsonwebtoken::{encode, EncodingKey, Header};
    use serde::Serialize;
    #[derive(Serialize)]
    struct Claims {
        sub: i64,
        email: String,
        exp: usize,
    }
    // Matches src/desk/user_service.rs::JWT_SECRET — hardcoded there, not
    // env-derived, so the stress client must use the same bytes.
    const JWT_SECRET: &[u8] = b"exchange_jwt_secret_change_in_prod";
    let exp = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs()
        + 86400) as usize;
    let key = EncodingKey::from_secret(JWT_SECRET);
    let header = Header::default();

    info!(
        "setup: loading user→id map from {path}, self-signing tokens for {} users…",
        cfg.users
    );
    let started = Instant::now();
    let raw = std::fs::read_to_string(path)?;
    // Build idx → user_id index for O(1) lookup. CSV is `user_id,idx`.
    let mut by_idx: std::collections::HashMap<usize, i64> =
        std::collections::HashMap::with_capacity(raw.lines().count());
    for line in raw.lines() {
        let mut it = line.split(',');
        let uid: i64 = it
            .next()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| anyhow::anyhow!("bad CSV row: {line}"))?;
        let idx: usize = it
            .next()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| anyhow::anyhow!("bad CSV row: {line}"))?;
        by_idx.insert(idx, uid);
    }
    let mut users = Vec::with_capacity(cfg.users);
    for i in 0..cfg.users {
        let user_id = *by_idx.get(&i).ok_or_else(|| {
            anyhow::anyhow!("missing user idx {i} in CSV — pre-create users first")
        })?;
        let email = format!("pressure_{i}@stress.test");
        let claims = Claims {
            sub: user_id,
            email,
            exp,
        };
        let token = encode(&header, &claims, &key)?;
        users.push(UserToken { user_id, token });
    }
    info!(
        "setup: {} users via CSV+self-sign in {:?}",
        users.len(),
        started.elapsed()
    );
    Ok(Arc::new(users))
}

async fn setup_users(cfg: &Config) -> anyhow::Result<Arc<Vec<UserToken>>> {
    // Fast path: pre-built CSV + local JWT sign. Used for 400K+ load tests
    // where HTTP register/login would dominate setup time.
    if let Ok(path) = std::env::var("PRESSURE_TOKENS_CSV") {
        return setup_users_from_csv(cfg, &path);
    }

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .pool_max_idle_per_host(64)
        .build()?;

    info!("setup: ensuring {} test users…", cfg.users);
    let started = Instant::now();
    let mut users = Vec::with_capacity(cfg.users);

    // Sequential is slow for 10K but predictable. Run in small parallel
    // batches to speed up while staying gentle on the auth/register
    // endpoints.
    use futures::stream::{FuturesUnordered, StreamExt};
    const PARALLEL: usize = 32;
    let mut pending: FuturesUnordered<_> = (0..PARALLEL.min(cfg.users))
        .map(|i| ensure_user(&http, &cfg.base_url, i))
        .collect();
    let mut next_idx = PARALLEL.min(cfg.users);

    while let Some(res) = pending.next().await {
        match res {
            Ok(u) => users.push(u),
            Err(e) => warn!("ensure_user failed: {e}"),
        }
        if next_idx < cfg.users {
            pending.push(ensure_user(&http, &cfg.base_url, next_idx));
            next_idx += 1;
        }
        if users.len() % 1000 == 0 && users.len() > 0 {
            info!("  setup: {} / {} users ready", users.len(), cfg.users);
        }
    }
    info!(
        "setup: {} users in {:?} ({:.0} users/s)",
        users.len(),
        started.elapsed(),
        users.len() as f64 / started.elapsed().as_secs_f64()
    );
    Ok(Arc::new(users))
}

// ── Per-connection driver ────────────────────────────────────────────────────

async fn driver(
    cfg: Arc<Config>,
    users: Arc<Vec<UserToken>>,
    metrics: Arc<Metrics>,
    conn_idx: usize,
    deadline: Instant,
) {
    metrics.conn_attempted.fetch_add(1, Ordering::Relaxed);
    let user = &users[conn_idx % users.len()];
    let ws_url = format!("{}/ws", cfg.base_url.replacen("http://", "ws://", 1));
    let bind_ip = if cfg.source_ips.is_empty() {
        None
    } else {
        Some(cfg.source_ips[conn_idx % cfg.source_ips.len()])
    };

    let mut ws = match ws_connect_plain_from(&ws_url, bind_ip).await {
        Ok(w) => w,
        Err(e) => {
            metrics.conn_failed.fetch_add(1, Ordering::Relaxed);
            tracing::debug!("conn {conn_idx} failed: {e}");
            return;
        }
    };
    ws.set_writev(true);
    ws.set_auto_close(true);
    ws.set_auto_pong(true);
    metrics.conn_open.fetch_add(1, Ordering::Relaxed);

    // Authenticate via WS message — first frame after handshake.
    let auth_msg = format!(r#"{{"type":"auth","token":"{}"}}"#, user.token);
    if ws
        .write_frame(Frame::text(Payload::Owned(auth_msg.into_bytes())))
        .await
        .is_err()
    {
        metrics.conn_open.fetch_sub(1, Ordering::Relaxed);
        metrics.conn_failed.fetch_add(1, Ordering::Relaxed);
        return;
    }

    let cycle_period = Duration::from_secs_f64(1.0 / cfg.ops_per_sec.max(0.0001));
    let wait_after_place = cycle_period / 2;
    let mut counter: u64 = 0;
    let mut current_order_id: Option<i64> = None;

    loop {
        if Instant::now() >= deadline {
            break;
        }

        // Send place_order.
        counter += 1;
        let coid = format!("p{}-{conn_idx}-{counter}", cfg.run_id);
        let coid_match = format!(r#""client_order_id":"{}""#, coid);
        let mut order_id_match: Option<String> = None;
        let place_msg = format!(
            r#"{{"type":"place_order","client_order_id":"{}","symbol":"{}","side":"buy","order_type":"limit","price":{},"qty":{}}}"#,
            coid, cfg.symbol, cfg.price, cfg.qty,
        );
        let t0 = Instant::now();
        metrics.place_sent.fetch_add(1, Ordering::Relaxed);
        if ws
            .write_frame(Frame::text(Payload::Owned(place_msg.into_bytes())))
            .await
            .is_err()
        {
            metrics.conn_open.fetch_sub(1, Ordering::Relaxed);
            metrics.conn_closed.fetch_add(1, Ordering::Relaxed);
            return;
        }

        // Drain frames until we get the engine OrderUpdate (status OPEN) for
        // our coid. order_submitted is the desk inline ack — it carries
        // order_id, so capture it from there, but DON'T break yet: we want to
        // measure full e2e RTT (client → desk → engine → desk → client). The
        // engine ack arrives as `order_update` with status=OPEN. Anything else
        // (balance_update, market data, stale frames) we drain silently — never
        // leaving an unread order_update in the recv buffer.
        let place_deadline = t0 + wait_after_place;
        let mut placed_ok = false;
        let dbg = std::env::var("PRESSURE_DEBUG_READS")
            .map(|v| v == "1")
            .unwrap_or(false);
        let mut frames_read: u32 = 0;
        let mut place_us: u64 = 0;
        loop {
            let now = Instant::now();
            if now >= place_deadline {
                break;
            }
            let remaining = place_deadline - now;
            let frame = match tokio::time::timeout(remaining, ws.read_frame()).await {
                Ok(Ok(f)) => f,
                Ok(Err(_)) => {
                    metrics.conn_open.fetch_sub(1, Ordering::Relaxed);
                    metrics.conn_closed.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                Err(_) => break, // timeout
            };
            if frame.opcode != OpCode::Text {
                continue;
            }
            frames_read += 1;
            let Ok(text) = std::str::from_utf8(&frame.payload) else {
                continue;
            };
            if dbg && counter <= 5 {
                let preview = &text[..text.len().min(140)];
                eprintln!("[conn{conn_idx} place#{counter} read#{frames_read}] {preview}");
            }
            // Cheap substring checks — avoid full JSON parse hot path.
            if text.contains("\"order_submitted\"") {
                // Desk inline ack — capture order_id, but keep waiting for
                // the engine `order_update` so place_us reflects e2e RTT.
                if let Some(id) = extract_order_id(text) {
                    current_order_id = Some(id);
                    order_id_match = Some(format!(r#""order_id":{},"#, id));
                }
                continue;
            }
            if text.contains("\"order_rejected\"") {
                placed_ok = false;
                place_us = t0.elapsed().as_micros() as u64;
                break;
            }
            if text.contains("\"order_update\"") {
                // Match by client_order_id if present (engine→desk forward
                // includes it), else fall back to order_id match.
                let mine = text.contains(&coid_match)
                    || order_id_match
                        .as_ref()
                        .map(|needle| text.contains(needle))
                        .unwrap_or(false);
                if !mine {
                    // Stale order_update from a prior cycle, or somehow not
                    // ours — drain and keep reading.
                    continue;
                }
                if text.contains("\"status\":\"OPEN\"") {
                    if current_order_id.is_none() {
                        current_order_id = extract_order_id(text);
                    }
                    placed_ok = true;
                    place_us = t0.elapsed().as_micros() as u64;
                    break;
                }
                if text.contains("\"status\":\"REJECTED\"") {
                    placed_ok = false;
                    place_us = t0.elapsed().as_micros() as u64;
                    break;
                }
                // PARTIAL/FILLED on a resting GTC @ 5000 is unexpected for
                // this test — record as ok and move on.
                if text.contains("\"status\":\"FILLED\"")
                    || text.contains("\"status\":\"PARTIAL\"")
                    || text.contains("\"status\":\"PARTIAL_FILL\"")
                {
                    placed_ok = true;
                    place_us = t0.elapsed().as_micros() as u64;
                    break;
                }
            }
            // balance_update, trade, depth, ticker, etc. — silently drained.
        }
        if place_us == 0 {
            // Loop fell out via deadline without ever finding our OPEN.
            place_us = t0.elapsed().as_micros() as u64;
        }
        if placed_ok {
            metrics.place_ok.fetch_add(1, Ordering::Relaxed);
        } else {
            metrics.place_fail.fetch_add(1, Ordering::Relaxed);
        }
        metrics
            .place_latency_us_sum
            .fetch_add(place_us, Ordering::Relaxed);
        record_max(&metrics.place_latency_us_max, place_us);
        metrics.place_latency_hist.record(place_us);

        // Drain the place→cancel gap. Any late order_update / balance_update
        // / market frame queued in the TCP buffer is read & discarded here, so
        // the next cancel measurement starts with an empty pipe.
        let gap_deadline = t0 + wait_after_place;
        while Instant::now() < gap_deadline {
            let remaining = gap_deadline - Instant::now();
            match tokio::time::timeout(remaining, ws.read_frame()).await {
                Ok(Ok(_)) => continue,
                Ok(Err(_)) => {
                    metrics.conn_open.fetch_sub(1, Ordering::Relaxed);
                    metrics.conn_closed.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                Err(_) => break, // gap deadline reached
            }
        }

        // Cancel.
        if let Some(id) = current_order_id.take() {
            let cancel_msg = format!(r#"{{"type":"cancel_order","order_id":{}}}"#, id);
            let t1 = Instant::now();
            metrics.cancel_sent.fetch_add(1, Ordering::Relaxed);
            if ws
                .write_frame(Frame::text(Payload::Owned(cancel_msg.into_bytes())))
                .await
                .is_err()
            {
                metrics.conn_open.fetch_sub(1, Ordering::Relaxed);
                metrics.conn_closed.fetch_add(1, Ordering::Relaxed);
                return;
            }
            // Wait for the engine-emitted CANCELED order_update (not just
            // desk-server's inline cancel_submitted ack). If we broke on
            // cancel_submitted, the engine's CANCELED would arrive during the
            // next cycle's sleep gap and linger in the TCP buffer — making
            // the next place's first read return that stale CANCELED,
            // doubling place latency. Reading through to CANCELED keeps the
            // stream clean.
            let cancel_deadline = t1 + (cycle_period - wait_after_place);
            let mut cancel_ok = false;
            let mut saw_inline_ack = false;
            let id_match = format!("\"order_id\":{id},");
            loop {
                let now = Instant::now();
                if now >= cancel_deadline {
                    break;
                }
                let frame = match tokio::time::timeout(cancel_deadline - now, ws.read_frame()).await
                {
                    Ok(Ok(f)) => f,
                    Ok(Err(_)) => {
                        metrics.conn_open.fetch_sub(1, Ordering::Relaxed);
                        metrics.conn_closed.fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                    Err(_) => break,
                };
                if frame.opcode != OpCode::Text {
                    continue;
                }
                let Ok(text) = std::str::from_utf8(&frame.payload) else {
                    continue;
                };
                if text.contains("\"cancel_submitted\"") || text.contains("\"cancel_all_ok\"") {
                    saw_inline_ack = true;
                    // Don't break — keep reading until the engine CANCELED
                    // arrives so we don't leave it queued.
                    continue;
                }
                if text.contains("\"order_update\"")
                    && text.contains("\"status\":\"CANCELED\"")
                    && text.contains(&id_match)
                {
                    cancel_ok = true;
                    break;
                }
                // Other order_update events (FILLED on a fill that beat the
                // cancel, balance_update, market data) silently drained.
            }
            // If we got the inline ack but timed out waiting for engine
            // CANCELED, still count as ok (rare — only if engine is slow).
            if !cancel_ok && saw_inline_ack {
                cancel_ok = true;
            }
            let cancel_us = t1.elapsed().as_micros() as u64;
            if cancel_ok {
                metrics.cancel_ok.fetch_add(1, Ordering::Relaxed);
            } else {
                metrics.cancel_fail.fetch_add(1, Ordering::Relaxed);
            }
            metrics
                .cancel_latency_us_sum
                .fetch_add(cancel_us, Ordering::Relaxed);
            record_max(&metrics.cancel_latency_us_max, cancel_us);
            metrics.cancel_latency_hist.record(cancel_us);
        }

        // Sleep until the next cycle, draining any trailing frames so the
        // next place starts with an empty buffer.
        let cycle_deadline = t0 + cycle_period;
        while Instant::now() < cycle_deadline {
            let remaining = cycle_deadline - Instant::now();
            match tokio::time::timeout(remaining, ws.read_frame()).await {
                Ok(Ok(_)) => continue,
                Ok(Err(_)) => {
                    metrics.conn_open.fetch_sub(1, Ordering::Relaxed);
                    metrics.conn_closed.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                Err(_) => break,
            }
        }
    }

    // Clean close.
    let _ = ws.write_frame(Frame::close(1000, b"")).await;
    metrics.conn_open.fetch_sub(1, Ordering::Relaxed);
    metrics.conn_closed.fetch_add(1, Ordering::Relaxed);
}

fn extract_order_id(text: &str) -> Option<i64> {
    // Find `"order_id":<digits>` without full JSON parse.
    let key = "\"order_id\":";
    let idx = text.find(key)?;
    let rest = &text[idx + key.len()..];
    let mut end = 0;
    for (i, c) in rest.char_indices() {
        if c.is_ascii_digit() || c == '-' {
            end = i + 1;
        } else {
            break;
        }
    }
    rest[..end].parse().ok()
}

// ── Reporter ─────────────────────────────────────────────────────────────────

async fn reporter(metrics: Arc<Metrics>, deadline: Instant) {
    let mut prev = metrics.snapshot();
    let mut last = Instant::now();
    println!(
        "{:>5} {:>7} {:>7} {:>7} {:>9} {:>8} {:>9} {:>8} {:>10} {:>10} {:>10} {:>10}",
        "t",
        "conns",
        "open",
        "failed",
        "place/s",
        "p_ok%",
        "cancel/s",
        "c_ok%",
        "p_avg_us",
        "p_max_us",
        "c_avg_us",
        "c_max_us"
    );
    let start = Instant::now();
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let now = Instant::now();
        let cur = metrics.snapshot();
        let dt = now.duration_since(last).as_secs_f64();
        last = now;

        let dpsent = cur.place_sent.saturating_sub(prev.place_sent);
        let dpok = cur.place_ok.saturating_sub(prev.place_ok);
        let dcsent = cur.cancel_sent.saturating_sub(prev.cancel_sent);
        let dcok = cur.cancel_ok.saturating_sub(prev.cancel_ok);
        let dpls = cur.place_lat_sum_us.saturating_sub(prev.place_lat_sum_us);
        let dcls = cur.cancel_lat_sum_us.saturating_sub(prev.cancel_lat_sum_us);

        let psent_rate = (dpsent as f64 / dt) as u64;
        let csent_rate = (dcsent as f64 / dt) as u64;
        let pok_pct = if dpsent > 0 {
            100.0 * dpok as f64 / dpsent as f64
        } else {
            0.0
        };
        let cok_pct = if dcsent > 0 {
            100.0 * dcok as f64 / dcsent as f64
        } else {
            0.0
        };
        let pavg = if dpsent > 0 { dpls / dpsent.max(1) } else { 0 };
        let cavg = if dcsent > 0 { dcls / dcsent.max(1) } else { 0 };

        println!(
            "{:>5}s {:>7} {:>7} {:>7} {:>9} {:>7.1}% {:>9} {:>7.1}% {:>10} {:>10} {:>10} {:>10}",
            start.elapsed().as_secs(),
            cur.conn_attempted,
            cur.conn_open,
            cur.conn_failed,
            psent_rate,
            pok_pct,
            csent_rate,
            cok_pct,
            pavg,
            cur.place_lat_max_us,
            cavg,
            cur.cancel_lat_max_us,
        );

        prev = cur;
        if Instant::now() >= deadline {
            break;
        }
    }
}

// ── Main ─────────────────────────────────────────────────────────────────────

fn worker_threads() -> usize {
    std::env::var("PRESSURE_WORKERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(8)
        })
}

fn main() -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads())
        .enable_all()
        .build()?;
    runtime.block_on(async_main())
}

async fn async_main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let cfg = Arc::new(Config::from_env());
    info!(
        "pressure_client: users={} conns={} ops/s/conn={} duration={}s ramp={}s symbol={} price={} qty={}",
        cfg.users, cfg.conns, cfg.ops_per_sec, cfg.duration_s, cfg.ramp_s,
        cfg.symbol, cfg.price, cfg.qty,
    );

    let users = setup_users(&cfg).await.context("setup users")?;
    if users.is_empty() {
        anyhow::bail!("no users were set up — abort");
    }

    let metrics = Arc::new(Metrics::default());

    let test_started = Instant::now();
    let deadline = test_started + Duration::from_secs(cfg.duration_s + cfg.ramp_s);

    // Reporter.
    let reporter_m = metrics.clone();
    let reporter_deadline = deadline;
    let reporter_handle = tokio::spawn(async move {
        reporter(reporter_m, reporter_deadline).await;
    });

    // Connection ramp: spawn cfg.conns drivers over cfg.ramp_s seconds.
    let conns = cfg.conns;
    let ramp_step = if conns > 0 && cfg.ramp_s > 0 {
        Duration::from_secs_f64(cfg.ramp_s as f64 / conns as f64)
    } else {
        Duration::from_millis(0)
    };
    info!(
        "ramping {} connections over {}s ({} µs/conn)",
        conns,
        cfg.ramp_s,
        ramp_step.as_micros()
    );

    let mut handles = Vec::with_capacity(conns);
    for i in 0..conns {
        let cfg2 = cfg.clone();
        let users2 = users.clone();
        let m2 = metrics.clone();
        let conn_deadline = deadline;
        handles.push(tokio::spawn(async move {
            driver(cfg2, users2, m2, i, conn_deadline).await;
        }));
        if i % 100 == 0 {
            info!("spawned {} / {}", i, conns);
        }
        if !ramp_step.is_zero() {
            tokio::time::sleep(ramp_step).await;
        }
    }

    // Wait for everyone.
    for h in handles {
        let _ = h.await;
    }
    let _ = reporter_handle.await;

    // Final summary.
    let cur = metrics.snapshot();
    let elapsed = test_started.elapsed().as_secs_f64();
    println!();
    println!("─── final summary ─────────────────────────────────────────");
    println!("  duration:            {:.1}s", elapsed);
    println!(
        "  conn attempted/open/failed/closed:  {} / {} / {} / {}",
        cur.conn_attempted, cur.conn_open, cur.conn_failed, cur.conn_closed
    );
    let place_pct = if cur.place_sent > 0 {
        100.0 * cur.place_ok as f64 / cur.place_sent as f64
    } else {
        0.0
    };
    let cancel_pct = if cur.cancel_sent > 0 {
        100.0 * cur.cancel_ok as f64 / cur.cancel_sent as f64
    } else {
        0.0
    };
    println!(
        "  place sent/ok/fail/ok%:             {} / {} / {} / {:.1}%",
        cur.place_sent, cur.place_ok, cur.place_fail, place_pct
    );
    println!(
        "  cancel sent/ok/fail/ok%:            {} / {} / {} / {:.1}%",
        cur.cancel_sent, cur.cancel_ok, cur.cancel_fail, cancel_pct
    );
    let p_avg_us = if cur.place_sent > 0 {
        cur.place_lat_sum_us / cur.place_sent
    } else {
        0
    };
    let c_avg_us = if cur.cancel_sent > 0 {
        cur.cancel_lat_sum_us / cur.cancel_sent
    } else {
        0
    };
    println!(
        "  place  latency avg/max (µs):        {} / {}",
        p_avg_us, cur.place_lat_max_us
    );
    println!(
        "  place  latency p50/p95 (µs):        {} / {}",
        metrics.place_latency_hist.percentile(0.50),
        metrics.place_latency_hist.percentile(0.95),
    );
    println!(
        "  cancel latency avg/max (µs):        {} / {}",
        c_avg_us, cur.cancel_lat_max_us
    );
    println!(
        "  cancel latency p50/p95 (µs):        {} / {}",
        metrics.cancel_latency_hist.percentile(0.50),
        metrics.cancel_latency_hist.percentile(0.95),
    );
    let total_ops_per_s = (cur.place_sent + cur.cancel_sent) as f64 / elapsed;
    println!(
        "  total ops/s:                        {:.0}",
        total_ops_per_s
    );
    Ok(())
}
