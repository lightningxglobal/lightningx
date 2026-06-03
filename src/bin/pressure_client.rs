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
//!   PRESSURE_USER_OFFSET   first pressure user index            (default 0)
//!   PRESSURE_SETUP_ONLY    create/load users then exit          (default false)
//!   PRESSURE_EXPORT_USERS_CSV write user_id,idx rows after setup
//!   PRESSURE_SYMBOL        symbol to trade                     (default BTC_USDT)
//!   PRESSURE_PRICE         limit price (far from market)       (default 1.0  — won't fill)
//!   PRESSURE_QTY           order quantity                      (default 0.0001)
//!   PRESSURE_SETUP_PARALLEL concurrent setup HTTP requests      (default 32)
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
use lightning_exchange::ws_sbe;
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
    user_offset: usize,
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
            user_offset: env_usize("PRESSURE_USER_OFFSET", 0),
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
    conn_fail_addr_unavailable: AtomicU64,
    conn_fail_refused: AtomicU64,
    conn_fail_timeout: AtomicU64,
    conn_fail_auth: AtomicU64,
    conn_fail_other: AtomicU64,

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
    place_ok_latency_us_sum: AtomicU64,
    place_ok_latency_us_max: AtomicU64,
    cancel_latency_us_sum: AtomicU64,
    cancel_latency_us_max: AtomicU64,
    place_latency_hist: LatencyHist,
    place_ok_latency_hist: LatencyHist,
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
            conn_fail_addr_unavailable: self.conn_fail_addr_unavailable.load(Ordering::Relaxed),
            conn_fail_refused: self.conn_fail_refused.load(Ordering::Relaxed),
            conn_fail_timeout: self.conn_fail_timeout.load(Ordering::Relaxed),
            conn_fail_auth: self.conn_fail_auth.load(Ordering::Relaxed),
            conn_fail_other: self.conn_fail_other.load(Ordering::Relaxed),
            place_sent: self.place_sent.load(Ordering::Relaxed),
            place_ok: self.place_ok.load(Ordering::Relaxed),
            place_fail: self.place_fail.load(Ordering::Relaxed),
            cancel_sent: self.cancel_sent.load(Ordering::Relaxed),
            cancel_ok: self.cancel_ok.load(Ordering::Relaxed),
            cancel_fail: self.cancel_fail.load(Ordering::Relaxed),
            place_lat_sum_us: self.place_latency_us_sum.load(Ordering::Relaxed),
            place_lat_max_us: self.place_latency_us_max.load(Ordering::Relaxed),
            place_ok_lat_sum_us: self.place_ok_latency_us_sum.load(Ordering::Relaxed),
            place_ok_lat_max_us: self.place_ok_latency_us_max.load(Ordering::Relaxed),
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
    conn_fail_addr_unavailable: u64,
    conn_fail_refused: u64,
    conn_fail_timeout: u64,
    conn_fail_auth: u64,
    conn_fail_other: u64,
    place_sent: u64,
    place_ok: u64,
    place_fail: u64,
    cancel_sent: u64,
    cancel_ok: u64,
    cancel_fail: u64,
    place_lat_sum_us: u64,
    place_lat_max_us: u64,
    place_ok_lat_sum_us: u64,
    place_ok_lat_max_us: u64,
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

fn record_conn_error(metrics: &Metrics, err: &anyhow::Error) {
    metrics.conn_failed.fetch_add(1, Ordering::Relaxed);
    let text = err.to_string();
    if text.contains("Can't assign requested address")
        || text.contains("Cannot assign requested address")
        || text.contains("EADDRNOTAVAIL")
    {
        metrics
            .conn_fail_addr_unavailable
            .fetch_add(1, Ordering::Relaxed);
    } else if text.contains("Connection refused") || text.contains("ECONNREFUSED") {
        metrics.conn_fail_refused.fetch_add(1, Ordering::Relaxed);
    } else if text.contains("timed out") || text.contains("deadline") || text.contains("timeout") {
        metrics.conn_fail_timeout.fetch_add(1, Ordering::Relaxed);
    } else {
        metrics.conn_fail_other.fetch_add(1, Ordering::Relaxed);
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
        "setup: loading user→id map from {path}, self-signing tokens for {} users at offset {}…",
        cfg.users, cfg.user_offset,
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
        let idx = cfg.user_offset + i;
        let user_id = *by_idx.get(&idx).ok_or_else(|| {
            anyhow::anyhow!("missing user idx {idx} in CSV — pre-create users first")
        })?;
        let email = format!("pressure_{idx}@stress.test");
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

    info!(
        "setup: ensuring {} test users at offset {}…",
        cfg.users, cfg.user_offset
    );
    let started = Instant::now();
    let mut users = Vec::with_capacity(cfg.users);

    // Run setup in bounded parallel batches; this is outside the measured
    // WebSocket path.
    use futures::stream::{FuturesUnordered, StreamExt};
    let parallel = env_usize("PRESSURE_SETUP_PARALLEL", 32).max(1);
    let mut pending: FuturesUnordered<_> = (0..parallel.min(cfg.users))
        .map(|i| ensure_user(&http, &cfg.base_url, cfg.user_offset + i))
        .collect();
    let mut next_idx = parallel.min(cfg.users);

    while let Some(res) = pending.next().await {
        match res {
            Ok(u) => users.push(u),
            Err(e) => warn!("ensure_user failed: {e}"),
        }
        if next_idx < cfg.users {
            pending.push(ensure_user(
                &http,
                &cfg.base_url,
                cfg.user_offset + next_idx,
            ));
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
            record_conn_error(&metrics, &e);
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
        metrics.conn_fail_auth.fetch_add(1, Ordering::Relaxed);
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
        let _coid_match = coid.as_str(); // kept for future use
        let mut order_id_match: Option<u64> = None;
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
            if frame.opcode != OpCode::Binary {
                continue;
            }
            frames_read += 1;
            let buf = &frame.payload;
            let Some(msg_type) = ws_sbe::decode_msg_type(buf) else {
                continue;
            };
            if dbg && counter <= 5 {
                eprintln!("[conn{conn_idx} place#{counter} read#{frames_read}] msg_type={msg_type} len={}", buf.len());
            }
            match msg_type {
                ws_sbe::ORDER_SUBMITTED => {
                    // Desk inline ack — capture order_id, but keep waiting for
                    // the engine `order_update` so place_us reflects e2e RTT.
                    if let Some((id, _, _)) = ws_sbe::decode_order_submitted(buf) {
                        current_order_id = Some(id as i64);
                        order_id_match = Some(id);
                    }
                    continue;
                }
                ws_sbe::ORDER_REJECTED => {
                    placed_ok = false;
                    place_us = t0.elapsed().as_micros() as u64;
                    break;
                }
                ws_sbe::ORDER_ACCEPTED => {
                    // Standalone mode: ORDER_ACCEPTED is the immediate ack.
                    if let Some((id, _)) = ws_sbe::decode_order_accepted(buf) {
                        if current_order_id.is_none() {
                            current_order_id = Some(id as i64);
                        }
                    }
                    placed_ok = true;
                    place_us = t0.elapsed().as_micros() as u64;
                    break;
                }
                ws_sbe::ORDER_UPDATE => {
                    if let Some((order_id, _, status, _, _, _)) = ws_sbe::decode_order_update(buf) {
                        // Match by order_id if we already know it, else accept any.
                        let mine = order_id_match.map(|exp| order_id == exp).unwrap_or(true);
                        if !mine {
                            continue;
                        }
                        match status {
                            ws_sbe::WS_STATUS_OPEN => {
                                if current_order_id.is_none() {
                                    current_order_id = Some(order_id as i64);
                                }
                                placed_ok = true;
                                place_us = t0.elapsed().as_micros() as u64;
                                break;
                            }
                            ws_sbe::WS_STATUS_REJECTED => {
                                placed_ok = false;
                                place_us = t0.elapsed().as_micros() as u64;
                                break;
                            }
                            ws_sbe::WS_STATUS_PARTIAL_FILL | ws_sbe::WS_STATUS_FILLED => {
                                placed_ok = true;
                                place_us = t0.elapsed().as_micros() as u64;
                                break;
                            }
                            _ => continue,
                        }
                    }
                }
                // balance_update, trade, depth, ticker, etc. — silently drained.
                _ => {}
            }
        }
        if place_us == 0 {
            // Loop fell out via deadline without ever finding our OPEN.
            place_us = t0.elapsed().as_micros() as u64;
        }
        if placed_ok {
            metrics.place_ok.fetch_add(1, Ordering::Relaxed);
            metrics
                .place_ok_latency_us_sum
                .fetch_add(place_us, Ordering::Relaxed);
            record_max(&metrics.place_ok_latency_us_max, place_us);
            metrics.place_ok_latency_hist.record(place_us);
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
                if frame.opcode != OpCode::Binary {
                    continue;
                }
                let buf = &frame.payload;
                let Some(msg_type) = ws_sbe::decode_msg_type(buf) else {
                    continue;
                };
                match msg_type {
                    ws_sbe::CANCEL_SUBMITTED | ws_sbe::CANCEL_ALL_OK | ws_sbe::BATCH_CANCEL_OK => {
                        saw_inline_ack = true;
                        // Don't break — keep reading until the engine CANCELED
                        // arrives so we don't leave it queued.
                        continue;
                    }
                    ws_sbe::ORDER_UPDATE => {
                        if let Some((order_id, _, status, _, _, _)) = ws_sbe::decode_order_update(buf) {
                            if order_id == id as u64 && status == ws_sbe::WS_STATUS_CANCELED {
                                cancel_ok = true;
                                break;
                            }
                        }
                    }
                    // Other order_update events, balance_update, market data — silently drained.
                    _ => {}
                }
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
        .unwrap_or(2)
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    raise_nofile_limit();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads())
        .enable_all()
        .build()?;
    runtime.block_on(async_main())
}

fn raise_nofile_limit() {
    let target = std::env::var("NOFILE_LIMIT")
        .ok()
        .and_then(|s| s.parse::<libc::rlim_t>().ok())
        .unwrap_or(262_144);

    unsafe {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) != 0 {
            warn!(
                "getrlimit(RLIMIT_NOFILE) failed: {}",
                std::io::Error::last_os_error()
            );
            return;
        }

        if limit.rlim_cur >= target {
            info!(
                "RLIMIT_NOFILE already sufficient: soft={} hard={}",
                limit.rlim_cur, limit.rlim_max
            );
            return;
        }

        let new_soft = target.min(limit.rlim_max);
        let new_limit = libc::rlimit {
            rlim_cur: new_soft,
            rlim_max: limit.rlim_max,
        };
        if libc::setrlimit(libc::RLIMIT_NOFILE, &new_limit) != 0 {
            warn!(
                "setrlimit(RLIMIT_NOFILE) failed: target={} soft={} hard={} error={}",
                target,
                limit.rlim_cur,
                limit.rlim_max,
                std::io::Error::last_os_error()
            );
            return;
        }

        info!(
            "raised RLIMIT_NOFILE: soft {} -> {} hard={}",
            limit.rlim_cur, new_soft, limit.rlim_max
        );
    }
}

async fn async_main() -> anyhow::Result<()> {
    let cfg = Arc::new(Config::from_env());
    info!(
        "pressure_client: users={} user_offset={} conns={} ops/s/conn={} duration={}s ramp={}s symbol={} price={} qty={}",
        cfg.users, cfg.user_offset, cfg.conns, cfg.ops_per_sec, cfg.duration_s, cfg.ramp_s,
        cfg.symbol, cfg.price, cfg.qty,
    );

    let users = setup_users(&cfg).await.context("setup users")?;
    if users.is_empty() {
        anyhow::bail!("no users were set up — abort");
    }
    if let Ok(path) = std::env::var("PRESSURE_EXPORT_USERS_CSV") {
        let mut out = String::with_capacity(users.len() * 16);
        for (i, user) in users.iter().enumerate() {
            let idx = cfg.user_offset + i;
            out.push_str(&format!("{},{}\n", user.user_id, idx));
        }
        std::fs::write(&path, out).with_context(|| format!("write {path}"))?;
        info!("setup: exported {} user rows to {path}", users.len());
    }
    if std::env::var("PRESSURE_SETUP_ONLY")
        .map(|v| v == "1" || v == "true")
        .unwrap_or(false)
    {
        return Ok(());
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
    if cur.conn_failed > 0 {
        println!(
            "  conn failed breakdown:             addr_unavail={} refused={} timeout={} auth={} other={}",
            cur.conn_fail_addr_unavailable,
            cur.conn_fail_refused,
            cur.conn_fail_timeout,
            cur.conn_fail_auth,
            cur.conn_fail_other
        );
    }
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
        "  place  latency avg/max (µs):        {} / {}  (all sent)",
        p_avg_us, cur.place_lat_max_us
    );
    println!(
        "  place  latency p50/p90/p99 (µs):    {} / {} / {}  (all sent)",
        metrics.place_latency_hist.percentile(0.50),
        metrics.place_latency_hist.percentile(0.90),
        metrics.place_latency_hist.percentile(0.99),
    );
    let p_ok_avg_us = if cur.place_ok > 0 {
        cur.place_ok_lat_sum_us / cur.place_ok
    } else {
        0
    };
    println!(
        "  place OK latency avg/max (µs):      {} / {}  (accepted only)",
        p_ok_avg_us, cur.place_ok_lat_max_us
    );
    println!(
        "  place OK latency p50/p90/p99 (µs):  {} / {} / {}  (accepted only)",
        metrics.place_ok_latency_hist.percentile(0.50),
        metrics.place_ok_latency_hist.percentile(0.90),
        metrics.place_ok_latency_hist.percentile(0.99),
    );
    println!(
        "  cancel latency avg/max (µs):        {} / {}",
        c_avg_us, cur.cancel_lat_max_us
    );
    println!(
        "  cancel latency p50/p90/p99 (µs):    {} / {} / {}",
        metrics.cancel_latency_hist.percentile(0.50),
        metrics.cancel_latency_hist.percentile(0.90),
        metrics.cancel_latency_hist.percentile(0.99),
    );
    let total_ops_per_s = (cur.place_sent + cur.cancel_sent) as f64 / elapsed;
    println!(
        "  total ops/s:                        {:.0}",
        total_ops_per_s
    );
    Ok(())
}
