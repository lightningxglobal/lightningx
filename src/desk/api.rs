/// REST API layer: auth, accounts, orders, user profile, KYC
use crate::account_repository::AccountRepository;
use crate::desk::money::{AccountBalance, AmountAtoms};
use crate::engine::{MatchingEngine, OrderStatus};
use crate::models::{DbOrder, User};
use crate::order::{Order, Side, TimeInForce};
use crate::order_state::{
    db_status_from_engine, db_status_from_update_kind, ws_status_from_engine,
};
use crate::tracer::ExchangeTracer;
use crate::transport::{AeronCmd, OrderMeta, OrderUpdateMsg, pack_str16};
use crate::user_service::{self, LoginRequest, RegisterRequest};
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, patch, post},
};
use dashmap::DashMap;
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::PgPool;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};

/// In-memory account cache: user_id → { asset → fixed-point balance }.
/// Updated after every DB write so GET /api/balances never hits the DB.
pub type AccountCache = Arc<DashMap<i64, HashMap<String, AccountBalance>>>;

/// Running VWAP of a user's BUY fills per (user_id, base_asset).
/// Updated on every BatchSettleTrade so GET /api/positions can compute
/// entry_price = weighted_sum / qty_sum in O(1) instead of a 90ms
/// PG aggregate over the trades table.
#[derive(Default, Clone, Copy, Debug)]
pub struct VwapStats {
    pub weighted_sum: f64, // Σ price * qty
    pub qty_sum: f64,      // Σ qty
}

pub type VwapCache = Arc<DashMap<(i64, String), VwapStats>>;

/// Shared Redis L1 connection. MultiplexedConnection is cheap-to-clone and
/// safe under concurrent use, so each request handler clones its own copy
/// before issuing commands. `None` means Redis is not available (e.g.
/// standalone in-process mode, or REDIS_URL unreachable at startup) →
/// readers fall back to PG.
pub type RedisConn = redis::aio::MultiplexedConnection;

/// Market data fanout via per-conn independent mpsc channels.
///
/// Why not tokio::sync::broadcast? Internally lock-based — at 40K
/// subscribers the lock contention on `recv()` dominates and pushes
/// p50 latency from ms into seconds. With per-conn mpsc::Sender<Arc<str>>,
/// each broadcast is N atomic Arc clones + N lock-free try_send (~50 ns
/// each), no shared lock. 40K × 30 broadcasts/s ≈ 60 ms/s CPU — fine.
///
/// Subscribers register LAZILY — only when the client sends a
/// `ClientMsg::Subscribe`. Connections that never subscribe (load tests,
/// trading-only bots) impose zero broadcast overhead.
/// Market data fanout: one channel per read actor (8) instead of one per
/// connection (100K). `send_bytes` costs 8 try_sends regardless of connection
/// count; each read actor distributes to its slice of connections internally.
pub struct MarketFanout {
    /// One sender per read actor. Capacity = 128 messages per actor.
    actor_txs: Vec<mpsc::Sender<Arc<[u8]>>>,
    /// Count of connections that have subscribed to market data. Used as an
    /// early-exit guard in the broadcaster to skip encoding when nobody listens.
    sub_count: AtomicUsize,
}

impl MarketFanout {
    pub fn new_with_actors(actor_txs: Vec<mpsc::Sender<Arc<[u8]>>>) -> Self {
        Self {
            actor_txs,
            sub_count: AtomicUsize::new(0),
        }
    }
    /// Called when a connection subscribes to market data.
    pub fn increment_subscriber(&self) {
        self.sub_count.fetch_add(1, Ordering::Relaxed);
    }
    /// Called when a subscribed connection unsubscribes or closes.
    pub fn decrement_subscriber(&self) {
        self.sub_count.fetch_sub(1, Ordering::Relaxed);
    }
    pub fn subscriber_count(&self) -> usize {
        self.sub_count.load(Ordering::Relaxed)
    }
    /// Fan a pre-encoded binary frame out to all read actors.
    /// Each actor distributes to its subscribed connections.
    pub fn send_bytes(&self, msg: Arc<[u8]>) {
        for tx in &self.actor_txs {
            let _ = tx.try_send(msg.clone());
        }
    }
    /// Convenience: take ownership of a Vec<u8> and dispatch.
    pub fn send_owned(&self, msg: Vec<u8>) {
        self.send_bytes(Arc::from(msg));
    }
}

/// Per-user personal-channel registry indexed by `user_id` directly.
///
/// Replaces `DashMap<i64, mpsc::Sender<String>>`. At 400K users the DashMap
/// shard lock dominated the recv-spin's OrderUpdate dispatch (DashMap.get
/// takes a shard read lock ≈ 100-500 ns under contention). This is a
/// fixed-size slab of lock-free `ArcSwapOption` cells; load is ~10-20 ns,
/// register/unregister is a single atomic swap.
///
/// Memory: 1 048 576 cells × 16 B ≈ 16 MB upfront. Cheap relative to the
/// gain.
pub struct UserTxRegistry {
    slots: Box<[arc_swap::ArcSwapOption<mpsc::Sender<(Vec<u8>, u64)>>]>,
}

const USER_TX_CAPACITY: usize = 1 << 20; // 1 048 576

impl UserTxRegistry {
    pub fn new() -> Self {
        let mut v = Vec::with_capacity(USER_TX_CAPACITY);
        for _ in 0..USER_TX_CAPACITY {
            v.push(arc_swap::ArcSwapOption::const_empty());
        }
        Self {
            slots: v.into_boxed_slice(),
        }
    }

    #[inline]
    fn idx(&self, user_id: i64) -> Option<usize> {
        let u = user_id as usize;
        if user_id > 0 && u < self.slots.len() {
            Some(u)
        } else {
            None
        }
    }

    pub fn register(&self, user_id: i64, sender: mpsc::Sender<(Vec<u8>, u64)>) {
        if let Some(idx) = self.idx(user_id) {
            self.slots[idx].store(Some(Arc::new(sender)));
        }
    }

    pub fn unregister(&self, user_id: i64) {
        if let Some(idx) = self.idx(user_id) {
            self.slots[idx].store(None);
        }
    }

    /// Returns an `Arc<mpsc::Sender>` ready for `try_send`. The Arc clone
    /// makes the call safe against concurrent unregister mid-send.
    #[inline]
    pub fn get(&self, user_id: i64) -> Option<Arc<mpsc::Sender<(Vec<u8>, u64)>>> {
        let idx = self.idx(user_id)?;
        self.slots[idx].load_full()
    }
}

impl Default for UserTxRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared queue for PersistEvent frames. A dedicated sync thread owns the
/// Aeron publisher so REST/WS hot paths never spin on Aeron backpressure.
/// `None` in standalone mode (no Aeron).
pub type PersistPubHandle =
    std::sync::Arc<crossbeam_queue::ArrayQueue<crate::transport::persist_event::PersistFrame>>;

#[derive(Clone)]
pub struct AppState {
    pub db: Arc<PgPool>,
    /// Per-symbol matching engines keyed by symbol (e.g. "BTC_USDT").
    /// None in Aeron-connected desk_server mode.
    pub engines: Option<Arc<DashMap<String, Arc<Mutex<MatchingEngine>>>>>,
    /// Market data fanout. Subscribers register on `ClientMsg::Subscribe`
    /// (lazy — connections that never subscribe pay zero broadcast cost).
    /// Replaces tokio::sync::broadcast which lock-contended at 40K+ subs.
    pub market_fanout: Arc<MarketFanout>,
    /// Whether this process accepts public market-data subscriptions.
    /// desk-server defaults this off; market-data-gateway owns live public data.
    pub public_market_data_enabled: bool,
    /// Private order-update stream for this desk/counter. New orders and
    /// cancels carry this id so matching publishes replies only to this desk.
    pub response_stream_id: i32,
    /// Logical desk/counter id for account ownership. A desk may only mutate
    /// private user state for users whose owner shard maps to this id.
    pub desk_id: u16,
    /// Per-user personal update channel (order fills, balance changes).
    /// Lock-free slab indexed by user_id. See UserTxRegistry for why we
    /// replaced `DashMap<i64, _>` (broke down at 400K subscribers).
    pub user_tx: Arc<UserTxRegistry>,
    /// Monotonically increasing order ID counter (desk_server Aeron mode uses this directly).
    pub next_order_id: Arc<AtomicU64>,
    /// Bounded lock-free MPMC ring for WS handler → aeron-send spin.
    /// `crossbeam::queue::ArrayQueue` because the workload is N async
    /// producers (tokio worker threads) into 1 sync consumer (send-spin);
    /// rtrb is SPSC-only so doesn't fit, and tokio mpsc adds ~25–40 ns
    /// per push vs crossbeam's 5–10 ns. Bounded so a runaway pressure
    /// stream cannot OOM the process — when push returns Err(value),
    /// the WS handler must reject the client with "system busy".
    pub aeron_cmd_tx: Option<std::sync::Arc<crossbeam_queue::ArrayQueue<AeronCmd>>>,
    /// Wrong-owner WS order/cancel forwarding queue. Drained by the Aeron
    /// send-spin thread and published to the owning counter desk.
    pub counter_forward_tx: Option<
        std::sync::Arc<
            crossbeam_queue::ArrayQueue<crate::transport::counter_forward::CounterForwardMsg>,
        >,
    >,
    /// Priority ring for system-generated liquidation orders.
    /// The spin thread drains this BEFORE aeron_cmd_tx so forced-liquidation
    /// orders are always submitted to the matching engine ahead of normal flow.
    pub liq_cmd_tx: Option<std::sync::Arc<crossbeam_queue::ArrayQueue<AeronCmd>>>,
    /// Pending order metadata stored by WS fast path until Aeron event loop handles DB+freeze.
    pub pending_meta: Arc<DashMap<u64, OrderMeta>>,
    /// Pending REST order responses: order_id → oneshot sender (desk_server mode only).
    pub pending_orders: Arc<DashMap<u64, oneshot::Sender<OrderUpdateMsg>>>,
    /// Last known depth per symbol stored as SBE DEPTH_MSG bytes. REST endpoints
    /// decode on demand; WS subscribers get the Arc<[u8]> directly.
    pub last_depth: Arc<DashMap<String, Arc<[u8]>>>,
    /// Last ticker per symbol as SBE TICKER_MSG bytes. REST decodes on demand.
    pub last_ticker: Arc<DashMap<String, Arc<[u8]>>>,
    /// Last trade price per symbol — updated on every `SettleTrade`. Drives the
    /// `last` field of broadcast tickers (which used to use best bid, causing
    /// the order-book center price to lag the recent-trades feed).
    pub last_trade_price: Arc<DashMap<String, f64>>,
    /// Latency tracer checkpoints (desk-server side). None in standalone mode.
    pub tracer: Option<Arc<ExchangeTracer>>,
    /// In-memory account balances: read path never touches DB after initial load.
    pub account_cache: AccountCache,
    /// Symbols with live matching engines. Used to reject orders for unknown symbols
    /// before sending order_submitted, preventing phantom orders in clients.
    /// Empty set = all symbols allowed (standalone mode with local engines).
    pub valid_symbols: Arc<HashSet<String>>,
    /// Redis L1 read connection. None ⇒ no Redis at startup, REST falls back to PG.
    pub redis: Option<RedisConn>,
    /// Per-user order-entry rate limiter, shared across WS connections and
    /// REST handlers (see SharedRateLimiter). Disabled unless WS_RL_* env
    /// vars are set.
    pub rate_limiter: std::sync::Arc<crate::rate_limit::SharedRateLimiter>,
    /// PersistEvent publisher (Aeron IPC stream consumed by redis-writer).
    /// None ⇒ standalone mode without Aeron — REST POST will not publish.
    pub persist_pub: Option<PersistPubHandle>,
    /// Funding view for /api/funding (S3.6): symbol → (next_settlement_ms,
    /// last_rate_e9, running premium-TWAP estimate e9). Written by the
    /// desk's funding task, read-only here.
    pub funding_view: Arc<DashMap<String, (i64, i64, i64)>>,
    /// Runtime exchange controls (T2 ops console): halt/fees per symbol,
    /// mutated by admin endpoints without a restart.
    pub exchange_config: Arc<crate::desk::exchange_config::ExchangeConfig>,
    /// Per-symbol trigger books (S5). REST inserts/cancels; the desk's
    /// trigger task pops due entries on mark moves. parking_lot::Mutex —
    /// held for O(log n) operations only.
    pub trigger_books:
        Arc<DashMap<String, parking_lot::Mutex<crate::desk::trigger::TriggerBook>>>,
    /// Per (user_id, base_asset) running VWAP of BUY fills. Updated by
    /// the spin thread on every BatchSettleTrade. Read by /api/positions.
    pub vwap_cache: VwapCache,
    /// Write actor pool: N tokio tasks each driving M connections via
    /// FuturesUnordered. Handler tasks call write_pool.register() to hand
    /// off their WebSocket write half; all outgoing frames then go through
    /// the pool, keeping handler select! loops read-only.
    pub write_pool: Arc<crate::desk::write_actor::WriteActorPool>,
    /// Read actor pool: N tokio tasks each driving M connection read loops via
    /// FuturesUnordered. On accept, the ws_handler routes the read half here
    /// instead of spawning a dedicated per-connection handler task. Reduces
    /// sleeping task count from 10K to N, shrinking the tokio scheduler
    /// ready-queue on every market-data or order-update wakeup.
    pub read_pool: Arc<crate::desk::read_actor::ReadActorPool>,
    /// Margin risk engine: per-account margin state, order reservation.
    pub risk_engine: Arc<crate::desk::risk::RiskEngine>,
}

pub fn router(state: AppState) -> Router {
    use crate::ws_handler::ws_handler;
    Router::new()
        // WebSocket endpoint
        .route("/ws", get(ws_handler))
        // Auth
        .route("/api/auth/register", post(handle_register))
        .route("/api/auth/login", post(handle_login))
        .route("/api/auth/refresh", post(handle_refresh))
        .route("/metrics", get(handle_metrics))
        .route("/api/admin/revoke", post(handle_admin_revoke))
        .route("/api/admin/unrevoke", post(handle_admin_unrevoke))
        .route("/api/admin/config", get(handle_admin_get_config).post(handle_admin_set_config))
        .route("/api/admin/force-close", post(handle_admin_force_close))
        .route("/api/deposit/credit", post(handle_deposit_credit))
        .route("/api/withdrawals", get(handle_list_withdrawals).post(handle_request_withdrawal))
        .route("/api/withdrawals/:id/status", post(handle_withdrawal_status))
        // User profile & KYC
        .route(
            "/api/user/profile",
            get(handle_get_profile).patch(handle_update_profile),
        )
        .route("/api/kyc", post(handle_submit_kyc))
        // Accounts / balances
        .route("/api/accounts", get(handle_accounts))
        .route("/api/balances", get(handle_accounts))
        // Orders
        .route(
            "/api/orders",
            get(handle_orders)
                .post(handle_place_order)
                .delete(handle_cancel_all_orders),
        )
        .route(
            "/api/orders/:order_id",
            get(handle_order).delete(handle_cancel_order),
        )
        // Trades, positions & tickers
        .route("/api/trades", get(handle_trades))
        .route("/api/positions", get(handle_positions))
        .route("/api/funding", get(handle_funding))
        .route(
            "/api/trigger-orders",
            get(handle_list_triggers).post(handle_place_trigger),
        )
        .route("/api/trigger-orders/:id", axum::routing::delete(handle_cancel_trigger))
        .route("/api/tickers", get(handle_tickers))
        // K-lines
        .route("/api/klines", get(handle_klines))
        .route("/api/user/password", patch(handle_change_password))
        .route("/api/test-funds", post(handle_test_funds))
        .route("/api/robot-funds", post(handle_robot_funds))
        .route("/api/seed-demo", post(handle_seed_demo))
        // Debug endpoint: only active when TEST_ENDPOINTS=1 env var is set.
        // Forces mark price for a symbol — used by integration tests to drive
        // liquidations without relying on live market data.
        .route("/api/debug/mark-price", post(handle_debug_mark_price))
        .with_state(state)
}

// ─── Auth ─────────────────────────────────────────────────────────────────────

/// Warm account_cache for `user_id` from PG. Called on register/login so
/// subsequent order placement on the hot path never hits a cache miss
/// (which would otherwise produce "Insufficient balance" errors even when
/// the user has plenty in PG — the hot path doesn't fall back to PG).
async fn warm_account_cache_for(s: &AppState, user_id: i64) {
    let rows: Vec<(String, i64, i64)> =
        sqlx::query_as("SELECT asset, balance_atoms, frozen_atoms FROM accounts WHERE user_id=$1")
            .bind(user_id)
            .fetch_all(s.db.as_ref())
            .await
            .unwrap_or_default();
    if rows.is_empty() {
        return;
    }
    // S5 fix: users who registered AFTER desk startup had no risk
    // account (the seed loop runs once at boot), so every margin-gated
    // path — trigger firing first among them — rejected with "Account
    // not found". Initialize it here, where both register and login
    // already warm the cache.
    if !s.risk_engine.accounts.contains_key(&user_id) {
        let usdt_atoms = rows
            .iter()
            .find(|(asset, _, _)| asset == "USDT")
            .map(|(_, bal, _)| *bal)
            .unwrap_or(0);
        s.risk_engine.initialize_account(user_id, usdt_atoms);
    }
    let mut entry = s.account_cache.entry(user_id).or_insert_with(HashMap::new);
    for (asset, bal, frz) in rows {
        entry.insert(asset, AccountBalance::from_atoms(bal, frz));
    }
}

async fn handle_register(
    State(s): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<RegisterRequest>,
) -> impl IntoResponse {
    let ip = headers
        .get("x-real-ip")
        .or_else(|| headers.get("x-forwarded-for"))
        .and_then(|v| v.to_str().ok())
        .map(|s| s.split(',').next().unwrap_or(s).trim().to_string());
    match user_service::register(&s.db, req, ip.clone()).await {
        Ok(resp) => {
            user_service::audit(
                &s.db,
                Some(resp.user.id),
                "register",
                ip.as_deref(),
                json!({"email": resp.user.email}),
            )
            .await;
            // Warm account_cache so the user's first order doesn't trip
            // try_freeze_cache's cache-miss path.
            warm_account_cache_for(&s, resp.user.id).await;
            (StatusCode::CREATED, Json(json!(resp)))
        }
        Err(e) => {
            let msg = e.to_string();
            let status = if msg.contains("duplicate key") || msg.contains("unique constraint") {
                StatusCode::CONFLICT
            } else {
                StatusCode::BAD_REQUEST
            };
            (status, Json(json!({"error": msg})))
        }
    }
}

async fn handle_login(
    State(s): State<AppState>,
    Json(req): Json<LoginRequest>,
) -> impl IntoResponse {
    let email = req.email.clone();
    match user_service::login(&s.db, req).await {
        Ok(resp) => {
            user_service::audit(
                &s.db,
                Some(resp.user.id),
                "login_ok",
                None,
                json!({"email": email}),
            )
            .await;
            warm_account_cache_for(&s, resp.user.id).await;
            (StatusCode::OK, Json(json!(resp)))
        }
        Err(e) => {
            user_service::audit(&s.db, None, "login_failed", None, json!({"email": email}))
                .await;
            (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error": e.to_string()})),
            )
        }
    }
}

fn admin_authorized(headers: &HeaderMap) -> bool {
    let Ok(expected) = std::env::var("EXCHANGE_ADMIN_TOKEN") else {
        return false; // no admin token configured → admin API disabled
    };
    if expected.len() < 32 {
        return false;
    }
    headers
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .is_some_and(|t| t == expected)
}

/// Service auth for the deposit watcher: EXCHANGE_DEPOSIT_TOKEN bearer
/// (≥32 bytes). Separate from the human admin token so the on-chain
/// service has its own least-privilege credential.
fn deposit_service_authorized(headers: &HeaderMap) -> bool {
    let Ok(expected) = std::env::var("EXCHANGE_DEPOSIT_TOKEN") else {
        return false;
    };
    if expected.len() < 32 {
        return false;
    }
    headers
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .is_some_and(|t| t == expected)
}

#[derive(serde::Deserialize)]
struct DepositCreditRequest {
    chain: String,
    tx_hash: String,
    #[serde(default)]
    log_index: i32,
    user_id: i64,
    asset: String,
    /// Atoms (1e-8). The watcher converts the on-chain amount once.
    amount_atoms: i64,
    from_address: Option<String>,
    to_address: Option<String>,
}

/// The narrow deposit interface (T-deposit): the off-chain watcher calls
/// this ONCE per confirmed on-chain transfer. Idempotent by
/// (chain, tx_hash, log_index); credits the user's virtual sub-account in
/// one transaction (balance + fund_audit + deposit row) and publishes an
/// AccountSet frame so Redis + every desk converge. The matching engine
/// is never involved.
async fn handle_deposit_credit(
    State(s): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<DepositCreditRequest>,
) -> impl IntoResponse {
    if !deposit_service_authorized(&headers) {
        return (StatusCode::FORBIDDEN, Json(json!({"error": "deposit service token required"})))
            .into_response();
    }
    if req.amount_atoms <= 0 {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": "amount must be positive"})))
            .into_response();
    }
    // The crediting desk must own this user's shard (account state is
    // sharded); the watcher should route to the owning desk, but reject
    // cleanly if misrouted.
    if let Err(e) = ensure_user_owned_by_this_desk(req.user_id, s.desk_id) {
        return e.into_response();
    }
    let repo = crate::desk::account_repository::AccountRepository::new(s.db.as_ref());
    let res = repo
        .credit_chain_deposit(
            &req.chain,
            &req.tx_hash,
            req.log_index,
            req.user_id,
            &req.asset,
            req.amount_atoms,
            req.from_address.as_deref(),
            req.to_address.as_deref(),
        )
        .await;
    let (credited, new_balance) = match res {
        Ok(v) => v,
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e.to_string()})))
                .into_response();
        }
    };

    if credited {
        // Refresh L1 (Redis hydrates account_cache lazily; invalidate so
        // the next read reloads) and publish an AccountSet so other desks
        // + Redis converge. frozen unchanged (a deposit is free balance).
        s.account_cache.remove(&req.user_id);
        if let Some(pp) = s.persist_pub.as_ref() {
            let balance = new_balance as f64 / 100_000_000.0;
            match account_set_payload(req.user_id, &req.asset, balance, 0.0) {
                Ok(mut payload) => {
                    payload.balance_atoms = new_balance;
                    payload.frozen_atoms = 0; // overwritten below if frozen exists
                    // Preserve any existing frozen by reading it.
                    if let Ok(frz) = sqlx::query_scalar::<_, i64>(
                        "SELECT frozen_atoms FROM accounts WHERE user_id = $1 AND asset = $2",
                    )
                    .bind(req.user_id)
                    .bind(&req.asset)
                    .fetch_one(s.db.as_ref())
                    .await
                    {
                        payload.frozen_atoms = frz;
                        payload.frozen = frz as f64 / 100_000_000.0;
                    }
                    let _ = pp.push(crate::transport::persist_event::PersistFrame::account_set(payload));
                }
                Err(e) => tracing::warn!("deposit: skip AccountSet frame: {e}"),
            }
        }
        user_service::audit(
            &s.db,
            Some(req.user_id),
            "deposit_credit",
            None,
            json!({
                "chain": req.chain, "tx_hash": req.tx_hash, "log_index": req.log_index,
                "asset": req.asset, "amount_atoms": req.amount_atoms,
            }),
        )
        .await;
    }

    (
        StatusCode::OK,
        Json(json!({
            "credited": credited,           // false = already applied (idempotent)
            "new_balance_atoms": new_balance,
            "tx_hash": req.tx_hash,
        })),
    )
        .into_response()
}

#[derive(serde::Deserialize)]
struct WithdrawalRequest {
    asset: String,
    chain: String,
    to_address: String,
    /// Atoms (1e-8). The exchange's fee is added on top and frozen too.
    amount_atoms: i64,
}

/// Phase 1 — a USER requests a withdrawal: freezes amount + fee and
/// records a 'pending' row. The chain service later approves/broadcasts/
/// confirms. Fee from exchange_config-driven default (flat for now).
async fn handle_request_withdrawal(
    State(s): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<WithdrawalRequest>,
) -> impl IntoResponse {
    let user_id = match auth_user(&headers) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    if let Err(e) = ensure_user_owned_by_this_desk(user_id, s.desk_id) {
        return e.into_response();
    }
    if req.amount_atoms <= 0 {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": "amount must be positive"})))
            .into_response();
    }
    // Flat withdrawal fee (atoms), env-tunable; the chain service's gas
    // is covered by this. WITHDRAW_FEE_ATOMS default 1 USDT.
    let fee_atoms: i64 = std::env::var("WITHDRAW_FEE_ATOMS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100_000_000);
    let repo = crate::desk::account_repository::AccountRepository::new(s.db.as_ref());
    match repo
        .request_withdrawal(
            user_id, &req.asset, &req.chain, &req.to_address, req.amount_atoms, fee_atoms,
        )
        .await
    {
        Ok((id, bal)) => {
            // The freeze changed `frozen` — converge L1 + other desks.
            s.account_cache.remove(&user_id);
            if let Some(pp) = s.persist_pub.as_ref() {
                if let Ok(mut p) =
                    account_set_payload(user_id, &req.asset, bal.balance(), bal.frozen())
                {
                    p.balance_atoms = bal.balance_atoms;
                    p.frozen_atoms = bal.frozen_atoms;
                    let _ = pp.push(crate::transport::persist_event::PersistFrame::account_set(p));
                }
            }
            user_service::audit(
                &s.db,
                Some(user_id),
                "withdrawal_request",
                None,
                json!({"id": id, "asset": req.asset, "chain": req.chain,
                       "amount_atoms": req.amount_atoms, "fee_atoms": fee_atoms,
                       "to": req.to_address}),
            )
            .await;
            (StatusCode::OK, Json(json!({"id": id, "status": "pending",
                "fee_atoms": fee_atoms}))).into_response()
        }
        Err(e) => {
            (StatusCode::BAD_REQUEST, Json(json!({"error": e.to_string()}))).into_response()
        }
    }
}

#[derive(serde::Deserialize)]
struct WithdrawalStatusRequest {
    /// "approved" | "broadcast" | "confirmed" | "failed"
    status: String,
    tx_hash: Option<String>,
    reason: Option<String>,
}

/// Phase 2 — the chain SERVICE advances a withdrawal's state
/// (EXCHANGE_DEPOSIT_TOKEN — same least-privilege service credential).
/// confirmed → debit frozen; failed → release frozen; both idempotent.
async fn handle_withdrawal_status(
    State(s): State<AppState>,
    headers: HeaderMap,
    axum::extract::Path(id): axum::extract::Path<i64>,
    Json(req): Json<WithdrawalStatusRequest>,
) -> impl IntoResponse {
    if !deposit_service_authorized(&headers) {
        return (StatusCode::FORBIDDEN, Json(json!({"error": "service token required"})))
            .into_response();
    }
    let repo = crate::desk::account_repository::AccountRepository::new(s.db.as_ref());
    // Find the owning user for cache/frame convergence on terminal moves.
    let owner: Option<(i64, String)> = sqlx::query_as(
        "SELECT user_id, asset FROM withdrawals WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(s.db.as_ref())
    .await
    .unwrap_or(None);

    let result = match req.status.as_str() {
        "approved" => repo.set_withdrawal_status(id, "pending", "approved", None).await,
        "broadcast" => {
            repo.set_withdrawal_status(id, "approved", "broadcast", req.tx_hash.as_deref()).await
        }
        "confirmed" => {
            let tx = req.tx_hash.as_deref().unwrap_or("");
            repo.confirm_withdrawal(id, tx).await
        }
        "failed" => {
            let reason = req.reason.as_deref().unwrap_or("chain send failed");
            repo.fail_withdrawal(id, reason).await
        }
        other => {
            return (StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("unknown status '{other}'")}))).into_response();
        }
    };
    match result {
        Ok(changed) => {
            // On confirm/fail the balance/frozen moved — converge L1 +
            // publish AccountSet for the owner.
            if changed && (req.status == "confirmed" || req.status == "failed") {
                if let Some((user_id, asset)) = owner {
                    s.account_cache.remove(&user_id);
                    if let (Some(pp), Ok((bal, frz))) = (
                        s.persist_pub.as_ref(),
                        sqlx::query_as::<_, (i64, i64)>(
                            "SELECT balance_atoms, frozen_atoms FROM accounts
                              WHERE user_id = $1 AND asset = $2",
                        )
                        .bind(user_id)
                        .bind(&asset)
                        .fetch_one(s.db.as_ref())
                        .await,
                    ) {
                        if let Ok(mut p) = account_set_payload(
                            user_id, &asset, bal as f64 / 1e8, frz as f64 / 1e8,
                        ) {
                            p.balance_atoms = bal;
                            p.frozen_atoms = frz;
                            let _ = pp
                                .push(crate::transport::persist_event::PersistFrame::account_set(p));
                        }
                    }
                }
            }
            (StatusCode::OK, Json(json!({"id": id, "status": req.status, "changed": changed})))
                .into_response()
        }
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({"error": e.to_string()}))).into_response(),
    }
}

async fn handle_list_withdrawals(
    State(s): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let user_id = match auth_user(&headers) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    let rows: Vec<(i64, String, String, String, i64, i64, String, Option<String>)> =
        sqlx::query_as(
            "SELECT id, asset, chain, to_address, amount_atoms, fee_atoms, status, tx_hash
               FROM withdrawals WHERE user_id = $1 ORDER BY requested_at DESC LIMIT 200",
        )
        .bind(user_id)
        .fetch_all(s.db.as_ref())
        .await
        .unwrap_or_default();
    let out: Vec<Value> = rows
        .into_iter()
        .map(|(id, asset, chain, to, amt, fee, st, tx)| {
            json!({"id": id, "asset": asset, "chain": chain, "to_address": to,
                   "amount_atoms": amt, "fee_atoms": fee, "status": st, "tx_hash": tx})
        })
        .collect();
    (StatusCode::OK, Json(json!({"withdrawals": out}))).into_response()
}

#[derive(serde::Deserialize)]
struct SetConfigRequest {
    symbol: String,
    /// Any omitted field is left unchanged.
    trading_halted: Option<bool>,
    maker_fee_bps: Option<i64>,
    taker_fee_bps: Option<i64>,
    /// Pass null explicitly to CLEAR a fee override back to the default.
    #[serde(default)]
    clear_maker_fee: bool,
    #[serde(default)]
    clear_taker_fee: bool,
}

/// T2 — set runtime controls for a symbol (halt/resume, fee override).
/// Persists to exchange_config, refreshes the in-memory mirror, audits.
async fn handle_admin_set_config(
    State(s): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<SetConfigRequest>,
) -> impl IntoResponse {
    if !admin_authorized(&headers) {
        return (StatusCode::FORBIDDEN, Json(json!({"error": "admin only"}))).into_response();
    }
    use crate::desk::exchange_config::SymbolControl;
    let cur = s.exchange_config.get(&req.symbol);
    let next = SymbolControl {
        trading_halted: req.trading_halted.unwrap_or(cur.trading_halted),
        maker_fee_bps: if req.clear_maker_fee {
            None
        } else {
            req.maker_fee_bps.or(cur.maker_fee_bps)
        },
        taker_fee_bps: if req.clear_taker_fee {
            None
        } else {
            req.taker_fee_bps.or(cur.taker_fee_bps)
        },
    };
    if let Err(e) = sqlx::query(
        "INSERT INTO exchange_config (symbol, trading_halted, maker_fee_bps, taker_fee_bps, updated_by)
         VALUES ($1, $2, $3, $4, 'admin')
         ON CONFLICT (symbol) DO UPDATE SET
            trading_halted = EXCLUDED.trading_halted,
            maker_fee_bps = EXCLUDED.maker_fee_bps,
            taker_fee_bps = EXCLUDED.taker_fee_bps,
            updated_at = NOW(), updated_by = 'admin'",
    )
    .bind(&req.symbol)
    .bind(next.trading_halted)
    .bind(next.maker_fee_bps)
    .bind(next.taker_fee_bps)
    .execute(s.db.as_ref())
    .await
    {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e.to_string()}))).into_response();
    }
    s.exchange_config.set(&req.symbol, next);
    user_service::audit(
        &s.db,
        None,
        "admin_set_config",
        None,
        json!({
            "symbol": req.symbol, "halted": next.trading_halted,
            "maker_fee_bps": next.maker_fee_bps, "taker_fee_bps": next.taker_fee_bps,
        }),
    )
    .await;
    (StatusCode::OK, Json(json!({
        "symbol": req.symbol,
        "trading_halted": next.trading_halted,
        "maker_fee_bps": next.maker_fee_bps,
        "taker_fee_bps": next.taker_fee_bps,
    }))).into_response()
}

async fn handle_admin_get_config(
    State(s): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if !admin_authorized(&headers) {
        return (StatusCode::FORBIDDEN, Json(json!({"error": "admin only"}))).into_response();
    }
    let rows: Vec<Value> = s
        .exchange_config
        .all()
        .into_iter()
        .map(|(sym, c)| {
            json!({
                "symbol": sym, "trading_halted": c.trading_halted,
                "maker_fee_bps": c.maker_fee_bps, "taker_fee_bps": c.taker_fee_bps,
            })
        })
        .collect();
    (StatusCode::OK, Json(json!({"config": rows}))).into_response()
}

#[derive(serde::Deserialize)]
struct ForceCloseRequest {
    user_id: i64,
    symbol: String,
}

/// T2 — admin force-close: inject a reduce-only market order that flattens
/// the user's position (manual intervention). Reuses the normal fill
/// pipeline; the position must exist and be owned by this desk's shard.
async fn handle_admin_force_close(
    State(s): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<ForceCloseRequest>,
) -> impl IntoResponse {
    if !admin_authorized(&headers) {
        return (StatusCode::FORBIDDEN, Json(json!({"error": "admin only"}))).into_response();
    }
    let sym16 = crate::transport::persist_event::pack_str(&req.symbol);
    let Some(pos) = s.risk_engine.positions.get(&(req.user_id, sym16)) else {
        return (StatusCode::NOT_FOUND, Json(json!({"error": "no open position"}))).into_response();
    };
    // Closing side: opposite the position.
    let close_side: u8 = match pos.side {
        crate::desk::risk::PositionSide::Long => 1,
        crate::desk::risk::PositionSide::Short => 0,
    };
    let qty = pos.qty_lots;
    drop(pos);
    let Some(cmd_tx) = s.liq_cmd_tx.as_ref() else {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "no command queue"}))).into_response();
    };
    let rules = crate::symbol_rules::SymbolRules::for_symbol(&req.symbol);
    let mark = s.risk_engine.mark_price_ticks(&sym16).unwrap_or(0);
    if mark <= 0 {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "no mark price"}))).into_response();
    }
    // Aggressive IOC 5% through the mark so it fills.
    let exec = if close_side == 0 { mark + mark / 20 } else { mark - mark / 20 };
    let order_id = s.next_order_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let req_sbe = crate::sbe::NewOrderRequest {
        client_order_id: order_id,
        participant_id: req.user_id as u64,
        price_ticks: exec,
        quantity_lots: qty,
        side: close_side,
        time_in_force: 1, // IOC
        response_stream_id: s.response_stream_id,
        reduce_only: 1,
        _pad: [0; 9],
        symbol: sym16,
    };
    let _ = rules;
    if cmd_tx.push(crate::transport::AeronCmd::NewOrder(req_sbe)).is_err() {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "command queue full"}))).into_response();
    }
    user_service::audit(
        &s.db,
        Some(req.user_id),
        "admin_force_close",
        None,
        json!({"symbol": req.symbol, "qty_lots": qty, "order_id": order_id}),
    )
    .await;
    (StatusCode::OK, Json(json!({"order_id": order_id, "qty_lots": qty}))).into_response()
}

#[derive(serde::Deserialize)]
struct RevokeRequest {
    user_id: i64,
}

/// Revoke every outstanding token of a user. Admin-only
/// (EXCHANGE_ADMIN_TOKEN bearer; endpoint disabled when unset/short).
async fn handle_admin_revoke(
    State(s): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<RevokeRequest>,
) -> impl IntoResponse {
    // TODO(dual-control): sensitive admin ops should require two-person
    // approval — single admin token for now. Policy + approver roles are
    // a business decision; see docs/plans/deferred-todos.md §2. audit_log
    // is already the append-only ledger such a flow would build on.
    if !admin_authorized(&headers) {
        return (StatusCode::UNAUTHORIZED, Json(json!({"error": "unauthorized"})));
    }
    let mut redis = s.redis.clone();
    if let Err(e) = user_service::revoke_user(redis.as_mut(), req.user_id).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        );
    }
    user_service::audit(&s.db, None, "admin_revoke", None, json!({"user_id": req.user_id}))
        .await;
    (StatusCode::OK, Json(json!({"revoked": req.user_id})))
}

async fn handle_admin_unrevoke(
    State(s): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<RevokeRequest>,
) -> impl IntoResponse {
    if !admin_authorized(&headers) {
        return (StatusCode::UNAUTHORIZED, Json(json!({"error": "unauthorized"})));
    }
    let mut redis = s.redis.clone();
    if let Err(e) = user_service::unrevoke_user(redis.as_mut(), req.user_id).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        );
    }
    user_service::audit(&s.db, None, "admin_unrevoke", None, json!({"user_id": req.user_id}))
        .await;
    (StatusCode::OK, Json(json!({"unrevoked": req.user_id})))
}

/// Prometheus exposition endpoint (scraped by VictoriaMetrics).
async fn handle_metrics() -> impl IntoResponse {
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        crate::desk::metrics::render(),
    )
}

/// Re-issue an access token from a still-valid one (sliding expiry).
/// Production pairs this with a short EXCHANGE_JWT_ACCESS_TTL_SECS.
async fn handle_refresh(State(s): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    let claims = match auth_claims(&headers) {
        Ok(c) => c,
        Err(e) => return e.into_response(),
    };
    match user_service::refresh_token(&claims) {
        Ok(token) => {
            user_service::audit(&s.db, Some(claims.sub), "token_refresh", None, json!({}))
                .await;
            (StatusCode::OK, Json(json!({"token": token}))).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

fn auth_claims(headers: &HeaderMap) -> Result<user_service::Claims, (StatusCode, Json<Value>)> {
    let token = headers
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .ok_or_else(|| {
            (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error": "Missing token"})),
            )
        })?;
    let claims = user_service::verify_token(token).map_err(|e| {
        (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": e.to_string()})),
        )
    })?;
    // Revocation: in-memory O(1), refreshed from Redis every 30s.
    if user_service::is_revoked(claims.sub) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "token revoked"})),
        ));
    }
    Ok(claims)
}

fn auth_user(headers: &HeaderMap) -> Result<i64, (StatusCode, Json<Value>)> {
    auth_claims(headers).map(|c| c.sub)
}

fn ensure_user_owned_by_this_desk(
    user_id: i64,
    desk_id: u16,
) -> Result<(), (StatusCode, Json<Value>)> {
    if crate::desk::counter_shard::is_user_owned_by_desk(user_id, desk_id) {
        return Ok(());
    }
    let owner_shard_id = crate::desk::counter_shard::owner_shard_for_user_id(user_id);
    Err((
        StatusCode::MISDIRECTED_REQUEST,
        Json(json!({
            "error": "wrong counter shard",
            "user_id": user_id,
            "desk_id": desk_id,
            "owner_shard_id": owner_shard_id,
        })),
    ))
}

// ─── Accounts ─────────────────────────────────────────────────────────────────

/// Atomic in-memory freeze: returns true if balance had enough to cover
/// `amount` (mutates `frozen += amount`), false otherwise (no mutation).
/// Mirror of the WS-handler helper of the same name; kept local so REST
/// hot path doesn't reach across modules for it.
fn try_freeze_cache(cache: &AccountCache, user_id: i64, asset: &str, amount: f64) -> bool {
    if amount <= 0.0 {
        return true;
    }
    let Some(mut entry) = cache.get_mut(&user_id) else {
        return false;
    };
    let Some(kv) = entry.get_mut(asset) else {
        return false;
    };
    let Ok(amount_atoms) = AmountAtoms::from_f64_round(amount) else {
        return false;
    };
    kv.try_freeze_atoms(amount_atoms.atoms())
}

fn cache_set_snapshot(cache: &AccountCache, user_id: i64, asset: &str, snapshot: AccountBalance) {
    cache
        .entry(user_id)
        .or_insert_with(HashMap::new)
        .insert(asset.to_string(), snapshot);
}

/// Balance response shape. The numeric fields are kept for existing
/// clients; the `*_str` twins are exact decimal renderings of the atom
/// values (industry convention — JSON numbers lose precision past 2^53).
/// New clients should parse the strings.
fn balance_json(asset: &str, amount: AccountBalance) -> Value {
    json!({
        "asset":         asset,
        "balance":       amount.balance(),
        "frozen":        amount.frozen(),
        "available":     amount.available(),
        "balance_str":   AmountAtoms::from_atoms(amount.balance_atoms).to_decimal_string(),
        "frozen_str":    AmountAtoms::from_atoms(amount.frozen_atoms).to_decimal_string(),
        "available_str": AmountAtoms::from_atoms(amount.available_atoms()).to_decimal_string(),
    })
}

async fn handle_accounts(State(s): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    let user_id = match auth_user(&headers) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    if let Err(e) = ensure_user_owned_by_this_desk(user_id, s.desk_id) {
        return e.into_response();
    }

    // Serve from in-memory cache when available (zero DB round-trip).
    if let Some(assets) = s.account_cache.get(&user_id) {
        let accounts: Vec<Value> = assets
            .iter()
            .map(|(asset, amount)| balance_json(asset, *amount))
            .collect();
        return (StatusCode::OK, Json(json!(accounts))).into_response();
    }

    // Cold path: cache miss on first request after startup — load from DB and populate.
    let repo = AccountRepository::new(&s.db);
    match repo.get_all_accounts(user_id).await {
        Ok(accounts) => {
            let mut entry = s.account_cache.entry(user_id).or_insert_with(HashMap::new);
            let mut out = Vec::with_capacity(accounts.len());
            for a in &accounts {
                let snapshot = AccountBalance::from_atoms(a.balance_atoms, a.frozen_atoms);
                entry.insert(a.asset.clone(), snapshot);
                out.push(balance_json(&a.asset, snapshot));
            }
            (StatusCode::OK, Json(json!(out))).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

// ─── PersistEvent helpers (REST → Aeron → redis-writer) ──────────────────────

/// Publish OrderUpsert + AccountSet frames to the persist stream so Redis
/// L1 stays in sync with PG. Called by REST POST /api/orders after the
/// row lands in PG. No-op when persist_pub is None (standalone mode).
///
/// We deliberately publish from REST (not the DB worker batch path) so
/// Redis sees the new order before the engine response round-trip, which
/// previously left ~12 orders of drift between PG and Redis under load.
fn publish_rest_order_upsert(
    state: &AppState,
    db_order: &DbOrder,
    user_id: i64,
    freeze_price: f64,
    quote_asset: &str,
    base_asset: &str,
) {
    use crate::transport::persist_event::{OrderUpsertPayload, PersistFrame, pack_str};
    let Some(pp) = state.persist_pub.as_ref() else {
        return;
    };

    // OrderUpsert: encode the row exactly as INSERT…RETURNING gave it back.
    // For market orders db_order.price is None — Redis encodes that as 0
    // (documented sentinel in apply_order_upsert); this matches the wire
    // format apply_frame already understands.
    let side: u8 = if db_order.side == "buy" { 0 } else { 1 };
    let price_f: f64 = match db_order.price {
        Some(p) => p,
        None => 0.0,
    };
    let coid_str: &str = match db_order.client_order_id.as_deref() {
        Some(s) => s,
        None => "",
    };
    let upsert = OrderUpsertPayload {
        id: db_order.id,
        user_id,
        symbol: pack_str(&db_order.symbol),
        side,
        status: 1, // PENDING — matches INSERT default
        _pad: [0; 6],
        order_type: pack_str(&db_order.order_type),
        price: price_f,
        qty: db_order.quantity,
        filled: db_order.filled,
        freeze_price,
        client_order_id: pack_str(coid_str),
        created_at_ms: db_order.created_at.timestamp_millis(),
    };
    let _ = pp.push(PersistFrame::order_upsert(upsert));

    // AccountSet: only the side that was frozen. Read balance/frozen from
    // the in-memory cache which the freeze step already updated with the
    // PG-RETURNING value. Skip publishing if the cache entry is somehow
    // missing — better silent than fabricated.
    let asset = if db_order.side == "buy" {
        quote_asset
    } else {
        base_asset
    };
    let snapshot: Option<AccountBalance> = state
        .account_cache
        .get(&user_id)
        .and_then(|entry| entry.get(asset).copied());
    if let Some(snapshot) = snapshot {
        match account_set_payload(user_id, asset, snapshot.balance(), snapshot.frozen()) {
            Ok(acc) => {
                let _ = pp.push(PersistFrame::account_set(acc));
            }
            Err(e) => tracing::warn!("skip AccountSet persist frame: {e}"),
        }
    }
}

fn account_set_payload(
    user_id: i64,
    asset: &str,
    balance: f64,
    frozen: f64,
) -> anyhow::Result<crate::transport::persist_event::AccountSetPayload> {
    use crate::transport::persist_event::pack_str;

    Ok(crate::transport::persist_event::AccountSetPayload {
        user_id,
        asset: pack_str(asset),
        balance,
        frozen,
        balance_atoms: AmountAtoms::from_f64_round(balance)?.atoms(),
        frozen_atoms: AmountAtoms::from_f64_round(frozen)?.atoms(),
    })
}

// ─── Orders ───────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct OrderQuery {
    symbol: Option<String>,
    status: Option<String>,
    limit: Option<i64>,
}

async fn handle_orders(
    State(s): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<OrderQuery>,
) -> impl IntoResponse {
    let user_id = match auth_user(&headers) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    if let Err(e) = ensure_user_owned_by_this_desk(user_id, s.desk_id) {
        return e.into_response();
    }
    let limit_raw = match q.limit {
        Some(v) => v,
        None => 50,
    };
    let limit = limit_raw.min(500);

    // Fast path: status=open + Redis attached → read user_orders:{uid} (SET)
    // + pipelined HGETALLs, no PG round-trip. Falls back to PG on any error.
    let wants_open = matches!(q.status.as_deref(), Some("open"));
    if wants_open {
        if let Some(conn_template) = s.redis.as_ref() {
            let mut conn = conn_template.clone();
            match crate::desk::redis_store::list_user_open_orders(
                &mut conn,
                user_id,
                q.symbol.as_deref(),
                limit as usize,
            )
            .await
            {
                Ok(rows) => return (StatusCode::OK, Json(json!(rows))).into_response(),
                Err(e) => {
                    tracing::warn!(
                        "Redis list_user_open_orders failed for user={user_id}: {e} — falling back to PG"
                    );
                }
            }
        }
    }

    let status_filter: Option<Vec<&str>> = match q.status.as_deref() {
        Some("open") => Some(vec!["PENDING", "TRADING"]),
        Some("history") => Some(vec!["COMPLETED", "CANCELED", "REJECTED"]),
        Some(s) => Some(vec![s]),
        None => None,
    };
    let orders = match status_filter {
        Some(statuses) => sqlx::query_as::<_, DbOrder>(
            "SELECT * FROM orders WHERE user_id=$1 AND ($2::text IS NULL OR symbol=$2) AND status=ANY($3) ORDER BY created_at DESC LIMIT $4",
        ).bind(user_id).bind(&q.symbol).bind(&statuses).bind(limit).fetch_all(s.db.as_ref()).await,
        None => sqlx::query_as::<_, DbOrder>(
            "SELECT * FROM orders WHERE user_id=$1 AND ($2::text IS NULL OR symbol=$2) ORDER BY created_at DESC LIMIT $3",
        ).bind(user_id).bind(&q.symbol).bind(limit).fetch_all(s.db.as_ref()).await,
    };
    match orders {
        Ok(rows) => (StatusCode::OK, Json(json!(rows))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

async fn handle_order(
    State(s): State<AppState>,
    headers: HeaderMap,
    Path(order_id): Path<i64>,
) -> impl IntoResponse {
    let user_id = match auth_user(&headers) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    if let Err(e) = ensure_user_owned_by_this_desk(user_id, s.desk_id) {
        return e.into_response();
    }

    // Fast path: try Redis first when the order is still active. Redis only
    // holds PENDING/TRADING orders, so missing-in-Redis (Ok(None)) doesn't
    // imply 404 — we fall through to PG which has terminal-state history too.
    if let Some(conn_template) = s.redis.as_ref() {
        let mut conn = conn_template.clone();
        match crate::desk::redis_store::get_order(&mut conn, order_id, user_id).await {
            Ok(Some(order)) => return (StatusCode::OK, Json(json!(order))).into_response(),
            Ok(None) => { /* fall through to PG */ }
            Err(e) => {
                tracing::warn!(
                    "Redis get_order failed for order_id={order_id} user_id={user_id}: {e} — falling back to PG"
                );
            }
        }
    }

    let row = sqlx::query_as::<_, DbOrder>("SELECT * FROM orders WHERE id = $1 AND user_id = $2")
        .bind(order_id)
        .bind(user_id)
        .fetch_optional(s.db.as_ref())
        .await;

    match row {
        Ok(Some(order)) => (StatusCode::OK, Json(json!(order))).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "Order not found"})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

// ─── Trades ───────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct TradeQuery {
    symbol: Option<String>,
    limit: Option<i64>,
}

async fn handle_trades(
    State(s): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<TradeQuery>,
) -> impl IntoResponse {
    let user_id = match auth_user(&headers) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    if let Err(e) = ensure_user_owned_by_this_desk(user_id, s.desk_id) {
        return e.into_response();
    }
    let limit = q.limit.unwrap_or(50).min(500);
    // DISTINCT ON (t.id) collapses self-trades (same user on both sides) to a
    // single row; the inner ORDER BY t.id, o.id picks the buy side
    // deterministically. The `side` column reflects which of the user's own
    // orders matched this trade.
    let trades = sqlx::query(
        "SELECT * FROM (
             SELECT DISTINCT ON (t.id)
                 t.id, t.symbol, t.price, t.quantity, t.created_at,
                 CASE WHEN o.id = t.buy_order_id THEN 'buy' ELSE 'sell' END AS side,
                 o.client_order_id
             FROM trades t
             JOIN orders o ON o.id = t.buy_order_id OR o.id = t.sell_order_id
             WHERE o.user_id = $1
               AND ($2::text IS NULL OR t.symbol = $2)
             ORDER BY t.id, o.id
         ) sub
         ORDER BY created_at DESC LIMIT $3",
    )
    .bind(user_id)
    .bind(&q.symbol)
    .bind(limit)
    .fetch_all(s.db.as_ref())
    .await;

    match trades {
        Ok(rows) => {
            let out: Vec<Value> = rows
                .iter()
                .map(|r| {
                    use sqlx::Row;
                    let price: f64 = r.get("price");
                    let quantity: f64 = r.get("quantity");
                    let client_order_id: Option<String> = r.get("client_order_id");
                    let mut obj = json!({
                        "id":         r.get::<i64, _>("id"),
                        "symbol":     r.get::<String, _>("symbol"),
                        "price":      price,
                        "quantity":   quantity,
                        "value":      price * quantity,
                        "side":       r.get::<String, _>("side"),
                        "created_at": r.get::<chrono::DateTime<chrono::Utc>, _>("created_at"),
                    });
                    if let Some(coid) = client_order_id {
                        obj["client_order_id"] = serde_json::Value::String(coid);
                    }
                    obj
                })
                .collect();
            (StatusCode::OK, Json(json!(out))).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

#[derive(serde::Deserialize)]
struct PlaceTriggerRequest {
    symbol: String,
    side: String,       // "buy" | "sell"
    order_type: String, // "limit" | "market"
    trigger_price: f64,
    price: Option<f64>, // required for limit
    quantity: f64,
    /// "rising" | "falling"; default = stop semantics for the side.
    trigger_when: Option<String>,
}

/// S5 — park a stop/take-profit trigger. It lives at the DESK (not the
/// engine book) until the manipulation-clamped mark crosses
/// trigger_price, then converts into a regular order through the same
/// margin checks as direct order entry.
async fn handle_place_trigger(
    State(s): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<PlaceTriggerRequest>,
) -> impl IntoResponse {
    let user_id = match auth_user(&headers) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    if let Err(e) = ensure_user_owned_by_this_desk(user_id, s.desk_id) {
        return e.into_response();
    }
    let side: u8 = match req.side.as_str() {
        "buy" => 0,
        "sell" => 1,
        _ => {
            return (StatusCode::BAD_REQUEST, Json(json!({"error": "side must be buy|sell"})))
                .into_response();
        }
    };
    let is_market = match req.order_type.as_str() {
        "market" => true,
        "limit" => false,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "order_type must be limit|market"})),
            )
                .into_response();
        }
    };
    let rules = crate::desk::symbol_rules::SymbolRules::for_symbol(&req.symbol);
    let Ok(trigger_ticks) = rules.price_to_ticks(req.trigger_price) else {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": "trigger_price off tick"})))
            .into_response();
    };
    let price_ticks = match (is_market, req.price) {
        (true, _) => None,
        (false, Some(p)) => match rules.price_to_ticks(p) {
            Ok(t) => Some(t),
            Err(_) => {
                return (StatusCode::BAD_REQUEST, Json(json!({"error": "price off tick"})))
                    .into_response();
            }
        },
        (false, None) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "limit trigger requires price"})),
            )
                .into_response();
        }
    };
    let Ok(qty_lots) = rules.quantity_to_lots(req.quantity) else {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": "quantity off step"})))
            .into_response();
    };
    if qty_lots <= 0 || trigger_ticks <= 0 {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": "must be positive"})))
            .into_response();
    }
    let when = match req.trigger_when.as_deref() {
        None => crate::desk::trigger::TriggerWhen::default_stop_loss_direction(side),
        Some(w) => match crate::desk::trigger::TriggerWhen::from_db_str(w) {
            Some(w) => w,
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": "trigger_when must be rising|falling"})),
                )
                    .into_response();
            }
        },
    };

    let id = s.next_order_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed) as i64;
    if let Err(e) = sqlx::query(
        "INSERT INTO trigger_orders
            (id, user_id, symbol, side, order_type, trigger_price_ticks, trigger_when,
             price_ticks, qty_lots)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
    )
    .bind(id)
    .bind(user_id)
    .bind(&req.symbol)
    .bind(if side == 0 { "buy" } else { "sell" })
    .bind(&req.order_type)
    .bind(trigger_ticks)
    .bind(when.as_db_str())
    .bind(price_ticks)
    .bind(qty_lots)
    .execute(s.db.as_ref())
    .await
    {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e.to_string()})))
            .into_response();
    }
    // Memory AFTER durable insert: a crash in between just re-hydrates it.
    s.trigger_books
        .entry(req.symbol.clone())
        .or_default()
        .lock()
        .insert(
            when,
            crate::desk::trigger::PendingTrigger {
                id,
                user_id,
                side,
                is_market,
                trigger_price_ticks: trigger_ticks,
                price_ticks,
                qty_lots,
            },
        );
    (StatusCode::OK, Json(json!({"id": id, "status": "pending"}))).into_response()
}

async fn handle_cancel_trigger(
    State(s): State<AppState>,
    headers: HeaderMap,
    axum::extract::Path(id): axum::extract::Path<i64>,
) -> impl IntoResponse {
    let user_id = match auth_user(&headers) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    // PG decides the race against firing: only a 'pending' row cancels.
    let row: Option<(String, String, i64)> = sqlx::query_as(
        "UPDATE trigger_orders
            SET status = 'cancelled', cancel_reason = 'user'
          WHERE id = $1 AND user_id = $2 AND status = 'pending'
          RETURNING symbol, trigger_when, trigger_price_ticks",
    )
    .bind(id)
    .bind(user_id)
    .fetch_optional(s.db.as_ref())
    .await
    .unwrap_or(None);
    let Some((symbol, when_s, trig)) = row else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "not found, not yours, or already fired"})),
        )
            .into_response();
    };
    if let Some(book) = s.trigger_books.get(&symbol) {
        if let Some(when) = crate::desk::trigger::TriggerWhen::from_db_str(&when_s) {
            book.lock().remove(when, trig, id);
        }
    }
    (StatusCode::OK, Json(json!({"id": id, "status": "cancelled"}))).into_response()
}

async fn handle_list_triggers(
    State(s): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let user_id = match auth_user(&headers) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    let rows: Vec<(i64, String, String, String, i64, String, Option<i64>, i64, String)> =
        sqlx::query_as(
            "SELECT id, symbol, side, order_type, trigger_price_ticks, trigger_when,
                    price_ticks, qty_lots, status
               FROM trigger_orders
              WHERE user_id = $1
              ORDER BY created_at DESC LIMIT 200",
        )
        .bind(user_id)
        .fetch_all(s.db.as_ref())
        .await
        .unwrap_or_default();
    let out: Vec<Value> = rows
        .into_iter()
        .map(|(id, symbol, side, ot, trig, when, price, qty, status)| {
            json!({
                "id": id, "symbol": symbol, "side": side, "order_type": ot,
                "trigger_price_ticks": trig, "trigger_when": when,
                "price_ticks": price, "qty_lots": qty, "status": status,
            })
        })
        .collect();
    (StatusCode::OK, Json(json!({"triggers": out}))).into_response()
}

/// S3.6 — current funding state per symbol (public; no auth needed).
/// `?symbol=BTC_USDT` filters; otherwise every tracked symbol.
async fn handle_funding(
    State(s): State<AppState>,
    axum::extract::Query(q): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> impl IntoResponse {
    let want = q.get("symbol");
    let rows: Vec<Value> = s
        .funding_view
        .iter()
        .filter(|e| want.is_none_or(|w| w == e.key()))
        .map(|e| {
            let (next_ms, last_rate_e9, premium_twap_e9) = *e.value();
            json!({
                "symbol": e.key(),
                "next_settlement_at_ms": next_ms,
                "last_rate_e9": last_rate_e9,
                "premium_twap_e9": premium_twap_e9,
            })
        })
        .collect();
    (StatusCode::OK, Json(json!({ "funding": rows }))).into_response()
}

async fn handle_positions(State(s): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    let user_id = match auth_user(&headers) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    if let Err(e) = ensure_user_owned_by_this_desk(user_id, s.desk_id) {
        return e.into_response();
    }
    // All-in-memory path — was a 3-query PG call (90ms p50). Each piece:
    //   • (balance, frozen) from account_cache (DashMap)
    //   • current_price from last_trade_price (DashMap, spin-thread maintained)
    //   • entry_price from vwap_cache (DashMap, spin-thread maintained from
    //     BUY fills in BatchSettleTrade) — preload happens at boot from
    //     `SELECT user_id, base_asset, SUM(price*qty), SUM(qty) FROM trades`.
    let positions = crate::positions::all_positions_in_memory(
        user_id,
        &s.account_cache,
        s.last_trade_price.as_ref(),
        &s.vwap_cache,
    );
    (StatusCode::OK, Json(json!(positions))).into_response()
}

// ─── Order placement ──────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct PlaceOrderRequest {
    symbol: String,
    side: String,
    order_type: String,
    price: Option<f64>,
    quantity: f64,
    client_order_id: Option<String>,
    #[serde(default)]
    reduce_only: bool,
}

async fn handle_place_order(
    State(s): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<PlaceOrderRequest>,
) -> impl IntoResponse {
    let user_id = match auth_user(&headers) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    if let Err(reason) = s
        .rate_limiter
        .try_consume(user_id, crate::rate_limit::OpClass::Place)
    {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({"error": reason})),
        )
            .into_response();
    }
    if let Err(e) = ensure_user_owned_by_this_desk(user_id, s.desk_id) {
        return e.into_response();
    }
    // T2 halt gate: a halted symbol rejects order ENTRY at the REST edge
    // (cancels still work; reduce-only de-risking is allowed through).
    if !req.reduce_only && s.exchange_config.is_halted(&req.symbol) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "trading halted for this symbol"})),
        )
            .into_response();
    }
    let fixed_shape = match crate::symbol_rules::normalize_order_shape(
        &req.symbol,
        &req.order_type,
        req.price,
        req.quantity,
    ) {
        Ok(shape) => shape,
        Err(reason) => {
            return (StatusCode::BAD_REQUEST, Json(json!({"error": reason}))).into_response();
        }
    };

    // Idempotent check via Redis L1 (replaces PG SELECT — 100× cheaper).
    // The (user_id, client_order_id) UNIQUE constraint downstream still
    // catches actual dupes; a Redis miss falls through to placement.
    if let Some(client_order_id) = req.client_order_id.as_deref() {
        if !client_order_id.is_empty() {
            if let Some(redis_conn) = s.redis.as_ref() {
                let mut conn = redis_conn.clone();
                use redis::AsyncCommands;
                let prior: Option<i64> = conn
                    .hget(
                        crate::desk::redis_store::key_user_coid(user_id),
                        client_order_id,
                    )
                    .await
                    .ok();
                if let Some(id) = prior {
                    if let Ok(Some(order)) =
                        crate::desk::redis_store::get_order(&mut conn, id, user_id).await
                    {
                        return (StatusCode::OK, Json(json!(order))).into_response();
                    }
                }
            } else {
                // Cold path: Redis not configured. Keep PG fallback so
                // idempotency still holds for standalone deployments.
                let existing = sqlx::query_as::<_, DbOrder>(
                    "SELECT * FROM orders WHERE user_id=$1 AND client_order_id=$2",
                )
                .bind(user_id)
                .bind(client_order_id)
                .fetch_optional(s.db.as_ref())
                .await;
                match existing {
                    Ok(Some(order)) => {
                        return (StatusCode::OK, Json(json!(order))).into_response();
                    }
                    Ok(None) => {}
                    Err(e) => {
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(json!({"error": e.to_string()})),
                        )
                            .into_response();
                    }
                }
            }
        }
    }

    let engine_side = match req.side.as_str() {
        "buy" => Side::Buy,
        "sell" => Side::Sell,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "invalid side"})),
            )
                .into_response();
        }
    };
    let tif = match req.order_type.as_str() {
        "limit" | "gtc" => TimeInForce::GTC,
        "ioc" => TimeInForce::IOC,
        "fok" => TimeInForce::FOK,
        "post_only" => TimeInForce::PostOnly,
        "market" => TimeInForce::IOC,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "unknown order_type"})),
            )
                .into_response();
        }
    };

    // In Aeron mode we don't have a local engine; in standalone mode we do.
    let engine_opt: Option<Arc<Mutex<MatchingEngine>>> = s
        .engines
        .as_ref()
        .and_then(|m| m.get(&req.symbol).map(|e| e.value().clone()));

    if s.engines.is_some() && engine_opt.is_none() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": format!("Unknown symbol: {}", req.symbol)})),
        )
            .into_response();
    }

    let sym_parts: Vec<&str> = req.symbol.splitn(2, '_').collect();
    let base_asset = sym_parts.first().copied().unwrap_or("BTC");
    let quote_asset = sym_parts.last().copied().unwrap_or("USDT");
    let rules = crate::symbol_rules::SymbolRules::for_symbol(&req.symbol);

    // Best opposing price (for market orders that need a freeze estimate).
    // In Aeron mode (no local engine), read from last_depth — same source
    // ws_handler uses for the WS path. Without this REST market-buy
    // returns "Unable to determine buy reservation price" because the
    // freeze_price falls back to 0.
    let best_opposing = if let Some(ref engine) = engine_opt {
        let eng = engine.lock().unwrap();
        let levels = eng.get_top_levels(1, engine_side == Side::Sell);
        levels.first().map(|(p, _)| rules.ticks_to_price(*p))
    } else {
        // Aeron mode: read from spin-thread-maintained last_depth (SBE bytes).
        let want_ask = req.side == "buy";
        s.last_depth
            .get(&req.symbol)
            .and_then(|buf| crate::ws_sbe::decode_best_price(&buf, want_ask))
    };

    // Compute the price that actually backs the frozen quote-asset amount.
    // For sells we freeze base_asset by quantity, so freeze_price has no
    // role — persisted as 0.0 by convention.
    let freeze_price: f64 = if req.side == "buy" {
        req.price.or(best_opposing).unwrap_or(0.0)
    } else {
        0.0
    };

    // Freeze funds in-memory (account_cache) — was a PG UPDATE round-trip
    // (~3-5ms). Cache is preloaded at boot from PG; the DB worker
    // BatchUpsertOrder downstream both mirrors the freeze to PG and
    // publishes AccountSet → Redis. WS PlaceOrders uses the same pattern.
    // We keep AccountRepository handle around because the engine-rollback
    // path below still uses repo.release_frozen on Aeron send failure.
    let repo = AccountRepository::new(s.db.as_ref());
    let (freeze_asset, freeze_amount): (&str, f64) = if req.side == "buy" {
        if freeze_price > 0.0 {
            (quote_asset, freeze_price * req.quantity)
        } else {
            // Market buy with no opposing depth yet — refuse.
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "Unable to determine buy reservation price"})),
            )
                .into_response();
        }
    } else {
        (base_asset, req.quantity)
    };
    if !try_freeze_cache(&s.account_cache, user_id, freeze_asset, freeze_amount) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "Insufficient balance"})),
        )
            .into_response();
    }

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);

    // Same shared AtomicU64 counter as the WS path so REST and WS IDs never
    // collide regardless of the Postgres sequence value.
    let rest_order_id = s.next_order_id.fetch_add(1, Ordering::Relaxed) as i64;
    let db_order_id = rest_order_id;

    // Build the DbOrder shape clients expect directly from the request —
    // PG INSERT is removed from the hot path. The row will be INSERT'd
    // asynchronously by the spin thread's BatchUpsertOrder when the engine
    // returns ACCEPTED (same path WS PlaceOrders uses). Redis L1 sees the
    // OrderUpsert frame before the engine round-trip completes (via
    // publish_rest_order_upsert below), so GET /api/orders/:id can find it
    // immediately.
    let now_utc = chrono::Utc::now();
    let db_order = DbOrder {
        id: db_order_id,
        user_id,
        symbol: req.symbol.clone(),
        side: req.side.clone(),
        order_type: req.order_type.clone(),
        price: req.price,
        quantity: req.quantity,
        filled: 0.0,
        status: "PENDING".to_string(),
        freeze_price,
        client_order_id: req.client_order_id.clone(),
        created_at: now_utc,
        updated_at: now_utc,
    };

    // Mirror the new row to Redis L1 immediately, before the engine round-trip.
    // Standalone mode (persist_pub=None) skips this; Aeron mode keeps Redis
    // in sync without waiting for the DB worker's batch path.
    publish_rest_order_upsert(
        &s,
        &db_order,
        user_id,
        freeze_price,
        quote_asset,
        base_asset,
    );

    // ── Route to engine: local (standalone) vs Aeron (desk) ──────────────────
    if let Some(engine) = engine_opt {
        // Standalone mode: run through local matching engine.
        let fixed_order = match crate::symbol_rules::FixedOrderInput::from_order_fields(
            db_order_id as u64,
            &req.symbol,
            engine_side,
            &req.order_type,
            req.price,
            req.quantity,
            tif,
            now_ns,
        ) {
            Ok(order) => order,
            Err(reason) => {
                return (StatusCode::BAD_REQUEST, Json(json!({"error": reason}))).into_response();
            }
        };
        let engine_order = if fixed_order.is_market {
            Order::new_market(
                fixed_order.id,
                fixed_order.side,
                fixed_order.quantity_lots,
                fixed_order.timestamp,
            )
        } else {
            Order::new(
                fixed_order.id,
                fixed_order.side,
                fixed_order.price_ticks,
                fixed_order.quantity_lots,
                fixed_order.time_in_force,
                fixed_order.timestamp,
            )
        };

        let engine_result = {
            let mut eng = engine.lock().unwrap();
            eng.place_order(engine_order)
        };

        let result = match engine_result {
            Ok(r) => r,
            Err(e) => {
                let _ = sqlx::query("DELETE FROM orders WHERE id=$1")
                    .bind(db_order_id)
                    .execute(s.db.as_ref())
                    .await;
                if req.side == "buy" {
                    let p = req.price.or(best_opposing).unwrap_or(0.0);
                    if p > 0.0 {
                        let _ = repo
                            .release_frozen(user_id, quote_asset, p * req.quantity)
                            .await;
                    }
                } else {
                    let _ = repo.release_frozen(user_id, base_asset, req.quantity).await;
                }
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": e.to_string()})),
                )
                    .into_response();
            }
        };

        let db_status = db_status_from_engine(result.status).as_str();
        let filled_qty = rules.lots_to_quantity(result.filled_lots);

        let _ = sqlx::query("UPDATE orders SET status=$1, filled_atoms=$2, updated_at=NOW() WHERE id=$3")
            .bind(db_status)
            .bind(crate::desk::money::AmountAtoms::from_f64_round(filled_qty).map(|a| a.atoms()).unwrap_or(0))
            .bind(db_order_id)
            .execute(s.db.as_ref())
            .await;

        if result.status == OrderStatus::Rejected {
            if req.side == "buy" {
                let p = req.price.or(best_opposing).unwrap_or(0.0);
                if p > 0.0 {
                    let _ = repo
                        .release_frozen(user_id, quote_asset, p * req.quantity)
                        .await;
                }
            } else {
                let _ = repo.release_frozen(user_id, base_asset, req.quantity).await;
            }
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "Rejected by matching engine"})),
            )
                .into_response();
        }

        // ── Settle fills (standalone path) ────────────────────────────────────
        let mut notified_maker_uids: std::collections::HashSet<i64> =
            std::collections::HashSet::new();
        for &(maker_order_id, fp_ticks, fq_lots) in &result.fills {
            if fp_ticks <= 0 || fq_lots <= 0 {
                continue;
            }
            let fp = rules.ticks_to_price(fp_ticks);
            let fq = rules.lots_to_quantity(fq_lots);

            let maker_uid: Option<i64> =
                sqlx::query_scalar("SELECT user_id FROM orders WHERE id=$1")
                    .bind(maker_order_id as i64)
                    .fetch_optional(s.db.as_ref())
                    .await
                    .unwrap_or(None);

            let (buyer_id, seller_id) = if req.side == "buy" {
                (user_id, maker_uid.unwrap_or(0))
            } else {
                (maker_uid.unwrap_or(0), user_id)
            };

            if buyer_id > 0 && seller_id > 0 {
                if req.side == "buy" {
                    let over = req.price.map(|lp| (lp - fp) * fq).unwrap_or(0.0).max(0.0);
                    if over > 0.0 {
                        let _ = repo.release_frozen(user_id, quote_asset, over).await;
                    }
                }
                // Trade row + four account legs + fund audit: ONE transaction.
                let (buy_oid, sell_oid) = if req.side == "buy" {
                    (result.order_id as i64, maker_order_id as i64)
                } else {
                    (maker_order_id as i64, result.order_id as i64)
                };
                let trade_rec = crate::desk::account_repository::SettleTradeRecord {
                    symbol: req.symbol.to_string(),
                    buy_order_id: buy_oid,
                    sell_order_id: sell_oid,
                };
                let _ = repo
                    .settle_trade_atoms(
                        buyer_id,
                        seller_id,
                        base_asset,
                        quote_asset,
                        crate::desk::money::AmountAtoms::from_f64_round(fp)
                            .unwrap_or(crate::desk::money::AmountAtoms::ZERO),
                        crate::desk::money::AmountAtoms::from_f64_round(fq)
                            .unwrap_or(crate::desk::money::AmountAtoms::ZERO),
                        crate::desk::money::AmountAtoms::ZERO,
                        crate::desk::money::AmountAtoms::ZERO,
                        Some(&trade_rec),
                    )
                    .await;
            }

            let _ = sqlx::query(
                "UPDATE orders SET filled_atoms = filled_atoms + $1,
                 status = CASE WHEN filled_atoms + $1 >= quantity_atoms THEN 'COMPLETED' ELSE 'TRADING' END,
                 updated_at = NOW() WHERE id = $2",
            )
            .bind(crate::desk::money::AmountAtoms::from_f64_round(fq).map(|a| a.atoms()).unwrap_or(0))
            .bind(maker_order_id as i64)
            .execute(s.db.as_ref())
            .await;

            let (buy_oid, sell_oid): (Option<i64>, Option<i64>) = if req.side == "buy" {
                (Some(db_order_id), Some(maker_order_id as i64))
            } else {
                (Some(maker_order_id as i64), Some(db_order_id))
            };
            let _ = sqlx::query(
                "INSERT INTO trades (symbol, buy_order_id, sell_order_id, price_atoms, quantity_atoms, created_at)
                 VALUES ($1, $2, $3, $4, $5, NOW())",
            )
            .bind(&req.symbol).bind(buy_oid).bind(sell_oid)
            .bind(crate::desk::money::AmountAtoms::from_f64_round(fp).map(|a| a.atoms()).unwrap_or(0))
            .bind(crate::desk::money::AmountAtoms::from_f64_round(fq).map(|a| a.atoms()).unwrap_or(0))
            .execute(s.db.as_ref()).await;

            if let Some(uid) = maker_uid {
                notified_maker_uids.insert(uid);
            }
        }

        if result.status == OrderStatus::Cancelled {
            let unfilled = req.quantity - filled_qty;
            if unfilled > 0.0 {
                let (rel_asset, rel_amount) = if req.side == "buy" {
                    (
                        quote_asset,
                        req.price.or(best_opposing).unwrap_or(0.0) * unfilled,
                    )
                } else {
                    (base_asset, unfilled)
                };
                if rel_amount > 0.0 {
                    let _ = repo.release_frozen(user_id, rel_asset, rel_amount).await;
                }
            }
        }

        let all_notify: Vec<i64> = std::iter::once(user_id)
            .chain(notified_maker_uids.into_iter())
            .collect();
        for uid in all_notify {
            if let Some(tx) = s.user_tx.get(uid) {
                let repo2 = AccountRepository::new(s.db.as_ref());
                if let Ok(accounts) = repo2.get_all_accounts(uid).await {
                    for acc in accounts {
                        let _ = tx
                            .send((
                                crate::ws_sbe::encode_balance_update(
                                    &acc.asset,
                                    AmountAtoms::from_atoms(acc.balance_atoms).to_f64(),
                                    AmountAtoms::from_atoms(acc.balance_atoms - acc.frozen_atoms)
                                        .to_f64(),
                                    AmountAtoms::from_atoms(acc.frozen_atoms).to_f64(),
                                ),
                                0,
                            ))
                            .await;
                    }
                }
                if let Some(pos) =
                    crate::positions::position_for_user_asset(s.db.as_ref(), uid, base_asset).await
                {
                    let _ = tx
                        .send((
                            crate::ws_sbe::encode_position_update(
                                base_asset,
                                pos.quantity,
                                pos.entry_price,
                            ),
                            0,
                        ))
                        .await;
                }
            }
        }

        if filled_qty > 0.0 {
            let avg_fill_price = if !result.fills.is_empty() {
                let cost: f64 = result
                    .fills
                    .iter()
                    .map(|&(_, p, q)| rules.ticks_to_price(p) * rules.lots_to_quantity(q))
                    .sum();
                cost / filled_qty
            } else {
                req.price.or(best_opposing).unwrap_or(0.0)
            };
            let now_us = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_micros() as u64)
                .unwrap_or(0);
            let side_byte: u8 = if req.side == "buy" { 0 } else { 1 };
            let _ = s.market_fanout.send_owned(crate::ws_sbe::encode_trade(
                avg_fill_price,
                filled_qty,
                side_byte,
                now_us,
                &req.symbol,
            ));
        }

        crate::ws_handler::broadcast_depth_pub(&s, &req.symbol);

        let ws_status = ws_status_from_engine(result.status, false);
        if let Some(tx) = s.user_tx.get(user_id) {
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_micros() as u64)
                .unwrap_or(0);
            let status_byte = crate::ws_sbe::ws_status_byte_from_str(ws_status.as_str());
            let _ = tx
                .send((
                    crate::ws_sbe::encode_order_update(
                        db_order_id as u64,
                        0,
                        status_byte,
                        filled_qty,
                        0.0,
                        ts,
                    ),
                    0,
                ))
                .await;
        }

        return match sqlx::query_as::<_, DbOrder>("SELECT * FROM orders WHERE id=$1")
            .bind(db_order_id)
            .fetch_one(s.db.as_ref())
            .await
        {
            Ok(o) => (StatusCode::CREATED, Json(json!(o))).into_response(),
            Err(_) => (StatusCode::CREATED, Json(json!({"id": db_order_id}))).into_response(),
        };
    }

    // ── Aeron mode: publish order to exchange_engine, await OrderUpdate ───────
    if let Some(aeron_cmd_tx) = &s.aeron_cmd_tx {
        use crate::sbe::NewOrderRequest as SbeNewOrder;

        let side_byte: u8 = if engine_side == Side::Buy { 0 } else { 1 };
        let tif_byte: u8 = match tif {
            TimeInForce::GTC => 0,
            TimeInForce::IOC => 1,
            TimeInForce::FOK => 2,
            TimeInForce::PostOnly => 3,
        };
        let mut sym_bytes = [0u8; 16];
        let sb = req.symbol.as_bytes();
        let copy_len = sb.len().min(16);
        sym_bytes[..copy_len].copy_from_slice(&sb[..copy_len]);

        let sbe_req = SbeNewOrder {
            client_order_id: db_order_id as u64,
            participant_id: user_id as u64,
            price_ticks: fixed_shape.price_ticks.unwrap_or(0),
            quantity_lots: fixed_shape.quantity_lots,
            side: side_byte,
            time_in_force: tif_byte,
            response_stream_id: s.response_stream_id,
            reduce_only: u8::from(req.reduce_only),
            _pad: [0; 9],
            symbol: sym_bytes,
        };

        // Insert pending_meta so the spin thread's ACCEPTED handler triggers
        // BatchUpsertOrder (PG INSERT). Same pattern as WS PlaceOrders —
        // REST POST no longer does its own synchronous INSERT, the spin
        // thread is the sole writer to PG for these rows.
        s.pending_meta.insert(
            db_order_id as u64,
            OrderMeta {
                user_id,
                symbol: sym_bytes,
                side: side_byte,
                order_type: pack_str16(&req.order_type),
                price: req.price,
                qty: req.quantity,
                client_order_id: req.client_order_id.clone().unwrap_or_default(),
                freeze_price,
                initial_margin_atoms: 0,
                liq_price_ticks: 0,
            },
        );

        let (tx, rx) = oneshot::channel::<OrderUpdateMsg>();
        s.pending_orders.insert(db_order_id as u64, tx);

        if aeron_cmd_tx.push(AeronCmd::NewOrder(sbe_req)).is_err() {
            s.pending_orders.remove(&(db_order_id as u64));
            let _ = sqlx::query("DELETE FROM orders WHERE id=$1")
                .bind(db_order_id)
                .execute(s.db.as_ref())
                .await;
            if req.side == "buy" {
                let p = req.price.or(best_opposing).unwrap_or(0.0);
                if p > 0.0 {
                    let _ = repo
                        .release_frozen(user_id, quote_asset, p * req.quantity)
                        .await;
                }
            } else {
                let _ = repo.release_frozen(user_id, base_asset, req.quantity).await;
            }
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": "Aeron channel closed"})),
            )
                .into_response();
        }

        let update = match tokio::time::timeout(std::time::Duration::from_secs(5), rx).await {
            Ok(Ok(msg)) => msg,
            Ok(Err(_)) => {
                s.pending_orders.remove(&(db_order_id as u64));
                return (
                    StatusCode::GATEWAY_TIMEOUT,
                    Json(json!({"error": "Order response channel closed"})),
                )
                    .into_response();
            }
            Err(_) => {
                s.pending_orders.remove(&(db_order_id as u64));
                return (
                    StatusCode::GATEWAY_TIMEOUT,
                    Json(json!({"error": "Order response timeout"})),
                )
                    .into_response();
            }
        };

        let db_status = db_status_from_update_kind(update.kind).as_str();

        // Copy packed struct fields to locals before use (avoids misaligned ref).
        let fill_qty: f64 = update.fill_qty;

        // PG INSERT/UPDATE happen on the spin thread (BatchUpsertOrder/
        // BatchDeleteOrder) after the engine event; this REST handler no
        // longer waits for that. Build the response from in-memory state
        // (db_order assembled before Aeron send) + the engine ACK fields.
        // Redis already mirrors the row via publish_rest_order_upsert above
        // for subsequent GET /api/orders/:id reads.
        let mut response_order = db_order.clone();
        response_order.status = db_status.to_string();
        response_order.filled = fill_qty;
        response_order.updated_at = chrono::Utc::now();

        return (StatusCode::CREATED, Json(json!(response_order))).into_response();
    }

    // Neither engine nor Aeron — should not happen.
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({"error": "No matching engine configured"})),
    )
        .into_response()
}

async fn handle_cancel_order(
    State(s): State<AppState>,
    headers: HeaderMap,
    Path(order_id): Path<i64>,
) -> impl IntoResponse {
    let user_id = match auth_user(&headers) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    if let Err(reason) = s
        .rate_limiter
        .try_consume(user_id, crate::rate_limit::OpClass::Cancel)
    {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({"error": reason})),
        )
            .into_response();
    }
    if let Err(e) = ensure_user_owned_by_this_desk(user_id, s.desk_id) {
        return e.into_response();
    }

    // Fetch order details before cancelling so we know what to unfreeze.
    let order_row = sqlx::query(
        "SELECT symbol, side, price, quantity, filled FROM orders
         WHERE id = $1 AND user_id = $2 AND status IN ('PENDING','TRADING')",
    )
    .bind(order_id)
    .bind(user_id)
    .fetch_optional(s.db.as_ref())
    .await;

    let order_row = match order_row {
        Ok(Some(r)) => r,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "Order not found or already closed"})),
            )
                .into_response();
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };

    use sqlx::Row;
    let symbol: String = order_row.get("symbol");
    let side: String = order_row.get("side");
    let price: Option<f64> = order_row.get("price");
    let quantity: f64 = order_row.get("quantity");
    let filled: f64 = order_row.get("filled");
    let remaining = quantity - filled;

    if s.engines.is_none() {
        if let Some(aeron_cmd_tx) = &s.aeron_cmd_tx {
            let cancel_req = crate::sbe::CancelOrderRequest {
                order_id: order_id as u64,
                participant_id: user_id as u64,
                response_stream_id: s.response_stream_id,
                _pad: [0; 4],
            };
            if aeron_cmd_tx.push(AeronCmd::Cancel(cancel_req)).is_err() {
                return (
                    StatusCode::BAD_GATEWAY,
                    Json(json!({"error": "Aeron channel closed"})),
                )
                    .into_response();
            }
            return (
                StatusCode::ACCEPTED,
                Json(json!({
                    "cancel_submitted": order_id
                })),
            )
                .into_response();
        }
    }

    let upd = sqlx::query(
        "DELETE FROM orders WHERE id = $1 AND user_id = $2 AND status IN ('PENDING','TRADING')",
    )
    .bind(order_id)
    .bind(user_id)
    .execute(s.db.as_ref())
    .await;

    match upd {
        Ok(r) if r.rows_affected() > 0 => {}
        Ok(_) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "Order not found or already closed"})),
            )
                .into_response();
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
    }

    // Best-effort cancel in engine for this symbol (standalone mode only).
    if let Some(engines) = &s.engines {
        if let Some(engine) = engines.get(&symbol) {
            let res = {
                let mut eng = engine.lock().unwrap();
                eng.cancel_order(order_id as u64)
            };
            if let Err(e) = res {
                tracing::warn!("engine cancel_order({}) failed: {:?}", order_id, e);
            }
        }
    } else if let Some(aeron_cmd_tx) = &s.aeron_cmd_tx {
        // Aeron mode: send cancel to exchange_engine via lock-free channel.
        let cancel_req = crate::sbe::CancelOrderRequest {
            order_id: order_id as u64,
            participant_id: user_id as u64,
            response_stream_id: s.response_stream_id,
            _pad: [0; 4],
        };
        let _ = aeron_cmd_tx.push(AeronCmd::Cancel(cancel_req));
    }

    // Release frozen funds for the unfilled portion.
    let sym_parts: Vec<&str> = symbol.splitn(2, '_').collect();
    let base_asset = sym_parts.first().copied().unwrap_or("BTC");
    let quote_asset = sym_parts.last().copied().unwrap_or("USDT");
    let repo = AccountRepository::new(&s.db);
    if side == "buy" {
        let freeze_price = price.unwrap_or(0.0);
        if freeze_price > 0.0 && remaining > 0.0 {
            if let Ok(snapshot) = repo
                .release_frozen(user_id, quote_asset, freeze_price * remaining)
                .await
            {
                if snapshot.balance_atoms > 0 || snapshot.frozen_atoms >= 0 {
                    cache_set_snapshot(&s.account_cache, user_id, quote_asset, snapshot);
                }
            }
        }
    } else if remaining > 0.0 {
        if let Ok(snapshot) = repo.release_frozen(user_id, base_asset, remaining).await {
            if snapshot.balance_atoms > 0 || snapshot.frozen_atoms >= 0 {
                cache_set_snapshot(&s.account_cache, user_id, base_asset, snapshot);
            }
        }
    }

    // Notify the user's WS subscribers (other tabs, etc.) that the order is
    // gone — the WS path does this too, this brings the REST path in line.
    if let Some(tx) = s.user_tx.get(user_id) {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);
        let _ = tx
            .send((
                crate::ws_sbe::encode_order_update(
                    order_id as u64,
                    0,
                    crate::ws_sbe::WS_STATUS_CANCELED,
                    0.0,
                    0.0,
                    ts,
                ),
                0,
            ))
            .await;
    }

    (StatusCode::OK, Json(json!({"cancelled": order_id}))).into_response()
}

/// DELETE /api/orders — cancel ALL open orders for the authenticated user.
///
/// Aeron mode: submits a batch-cancel to the engine asynchronously; fund release
/// arrives via the CancelConfirmed path when each CANCELLED event comes back.
/// Standalone mode: cancels each order synchronously and releases frozen funds.
async fn handle_cancel_all_orders(
    State(s): State<AppState>,
    headers: HeaderMap,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> impl IntoResponse {
    let user_id = match auth_user(&headers) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    if let Err(e) = ensure_user_owned_by_this_desk(user_id, s.desk_id) {
        return e.into_response();
    }

    // Optional ?symbol= filter.
    let symbol_filter: Option<String> = params.get("symbol").cloned();

    #[derive(sqlx::FromRow)]
    struct OpenOrder {
        id: i64,
        symbol: String,
        side: String,
        price: Option<f64>,
        quantity: f64,
        filled: f64,
    }

    let rows: Vec<OpenOrder> = {
        let query_str = if symbol_filter.is_some() {
            "SELECT id, symbol, side, price, quantity, filled FROM orders
             WHERE user_id = $1 AND symbol = $2 AND status IN ('PENDING','TRADING')"
        } else {
            "SELECT id, symbol, side, price, quantity, filled FROM orders
             WHERE user_id = $1 AND status IN ('PENDING','TRADING')"
        };
        let mut q = sqlx::query_as::<_, OpenOrder>(query_str).bind(user_id);
        if let Some(ref sym) = symbol_filter {
            q = q.bind(sym);
        }
        match q.fetch_all(s.db.as_ref()).await {
            Ok(rows) => rows,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": e.to_string()})),
                )
                    .into_response();
            }
        }
    };

    if rows.is_empty() {
        return (StatusCode::OK, Json(json!({"cancelled": 0}))).into_response();
    }

    let order_ids: Vec<i64> = rows.iter().map(|r| r.id).collect();

    // ── Aeron mode: batch-cancel in the engine; fund release via CancelConfirmed ─
    if s.engines.is_none() {
        if let Some(aeron_cmd_tx) = &s.aeron_cmd_tx {
            let reqs: smallvec::SmallVec<[crate::sbe::CancelOrderRequest; 64]> = order_ids
                .iter()
                .map(|&id| crate::sbe::CancelOrderRequest {
                    order_id: id as u64,
                    participant_id: user_id as u64,
                    response_stream_id: s.response_stream_id,
                    _pad: [0; 4],
                })
                .collect();
            let count = reqs.len();
            if aeron_cmd_tx.push(AeronCmd::BatchCancel(reqs)).is_ok() {
                return (
                    StatusCode::ACCEPTED,
                    Json(json!({"cancel_submitted": count})),
                )
                    .into_response();
            }
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": "Aeron channel closed"})),
            )
                .into_response();
        }
    }

    // ── Standalone mode: cancel in engine + DB + release frozen funds ─────────
    let mut cancelled = 0usize;
    let repo = AccountRepository::new(&s.db);

    for row in &rows {
        // Cancel in matching engine.
        if let Some(engines) = &s.engines {
            if let Some(engine) = engines.get(&row.symbol) {
                let _ = engine.lock().unwrap().cancel_order(row.id as u64);
            }
        }

        // Mark as CANCELLED in DB.
        let upd = sqlx::query(
            "DELETE FROM orders WHERE id = $1 AND user_id = $2 AND status IN ('PENDING','TRADING')",
        )
        .bind(row.id)
        .bind(user_id)
        .execute(s.db.as_ref())
        .await;

        if upd.map(|r| r.rows_affected() > 0).unwrap_or(false) {
            cancelled += 1;
        }

        // Release frozen funds for the unfilled remainder.
        let remaining = row.quantity - row.filled;
        if remaining > 0.0 {
            let sym_parts: Vec<&str> = row.symbol.splitn(2, '_').collect();
            let base_asset = sym_parts.first().copied().unwrap_or("BTC");
            let quote_asset = sym_parts.last().copied().unwrap_or("USDT");
            if row.side == "buy" {
                if let Some(fp) = row.price.filter(|&p| p > 0.0) {
                    if let Ok(snapshot) = repo
                        .release_frozen(user_id, quote_asset, fp * remaining)
                        .await
                    {
                        cache_set_snapshot(&s.account_cache, user_id, quote_asset, snapshot);
                    }
                }
            } else if let Ok(snapshot) = repo.release_frozen(user_id, base_asset, remaining).await
            {
                cache_set_snapshot(&s.account_cache, user_id, base_asset, snapshot);
            }
        }

        // Notify open WS connections for this user.
        if let Some(tx) = s.user_tx.get(user_id) {
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_micros() as u64)
                .unwrap_or(0);
            let _ = tx
                .send((
                    crate::ws_sbe::encode_order_update(
                        row.id as u64,
                        0,
                        crate::ws_sbe::WS_STATUS_CANCELED,
                        0.0,
                        0.0,
                        ts,
                    ),
                    0,
                ))
                .await;
        }
    }

    (StatusCode::OK, Json(json!({"cancelled": cancelled}))).into_response()
}

// ─── Tickers ──────────────────────────────────────────────────────────────────

async fn handle_tickers(State(s): State<AppState>) -> impl IntoResponse {
    // Fast path: serve from in-memory `last_ticker` DashMap, populated by
    // live trade-event aggregation. Each entry is the already-stringified WS
    // ticker payload,
    // so we just collect-and-strip the WS envelope to match the REST
    // response shape. p50 here used to be 243ms (PG aggregate over the
    // trades table) — this path is sub-ms.
    //
    // Cold-start fallback: if last_ticker is empty (process just booted
    // and no trade has fired the broadcast yet), run the old PG query so
    // the first call after startup still returns something useful.
    if !s.last_ticker.is_empty() {
        let mut tickers: Vec<Value> = Vec::with_capacity(s.last_ticker.len());
        for entry in s.last_ticker.iter() {
            // Each cached value is SBE TICKER_MSG bytes. Decode on demand for REST.
            let Some(json) = crate::ws_sbe::decode_ticker_for_rest(entry.value()) else {
                continue;
            };
            tickers.push(json);
        }
        tickers.sort_by(|a, b| {
            let ks = a.get("symbol").and_then(|v| v.as_str()).unwrap_or("");
            let kt = b.get("symbol").and_then(|v| v.as_str()).unwrap_or("");
            ks.cmp(kt)
        });
        return (StatusCode::OK, Json(json!(tickers))).into_response();
    }

    let rows = sqlx::query(
        "SELECT symbol,
                MAX(price) FILTER (WHERE created_at > NOW() - INTERVAL '24 hours') AS high_24h,
                MIN(price) FILTER (WHERE created_at > NOW() - INTERVAL '24 hours') AS low_24h,
                SUM(quantity) FILTER (WHERE created_at > NOW() - INTERVAL '24 hours') AS vol_24h,
                (SELECT price FROM trades t2 WHERE t2.symbol = t.symbol ORDER BY created_at DESC LIMIT 1) AS last_price,
                (SELECT price FROM trades t3 WHERE t3.symbol = t.symbol AND t3.created_at > NOW() - INTERVAL '24 hours' ORDER BY created_at ASC LIMIT 1) AS open_24h
         FROM trades t GROUP BY symbol ORDER BY symbol",
    )
    .fetch_all(s.db.as_ref()).await;

    match rows {
        Ok(rows) => {
            use sqlx::Row;
            let tickers: Vec<Value> = rows
                .iter()
                .map(|r| {
                    let last: Option<f64> = r.get("last_price");
                    let open: Option<f64> = r.get("open_24h");
                    let change = match (last, open) {
                        (Some(l), Some(o)) if o != 0.0 => (l - o) / o * 100.0,
                        _ => 0.0,
                    };
                    json!({
                        "symbol": r.get::<String, _>("symbol"),
                        "last":   last,
                        "high":   r.get::<Option<f64>, _>("high_24h"),
                        "low":    r.get::<Option<f64>, _>("low_24h"),
                        "volume": r.get::<Option<f64>, _>("vol_24h"),
                        "change": change,
                    })
                })
                .collect();
            (StatusCode::OK, Json(json!(tickers))).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

// ─── User profile ─────────────────────────────────────────────────────────────

async fn handle_get_profile(State(s): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    let user_id = match auth_user(&headers) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    if let Err(e) = ensure_user_owned_by_this_desk(user_id, s.desk_id) {
        return e.into_response();
    }
    match sqlx::query_as::<_, User>("SELECT * FROM users WHERE id = $1")
        .bind(user_id)
        .fetch_optional(s.db.as_ref())
        .await
    {
        Ok(Some(u)) => (StatusCode::OK, Json(json!(u))).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "User not found"})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

#[derive(Deserialize)]
struct UpdateProfileRequest {
    full_name: Option<String>,
}

async fn handle_update_profile(
    State(s): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<UpdateProfileRequest>,
) -> impl IntoResponse {
    let user_id = match auth_user(&headers) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    if let Err(e) = ensure_user_owned_by_this_desk(user_id, s.desk_id) {
        return e.into_response();
    }
    match sqlx::query_as::<_, User>(
        "UPDATE users SET full_name = COALESCE($1, full_name), updated_at = NOW()
         WHERE id = $2 RETURNING *",
    )
    .bind(&req.full_name)
    .bind(user_id)
    .fetch_optional(s.db.as_ref())
    .await
    {
        Ok(Some(u)) => (StatusCode::OK, Json(json!(u))).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "User not found"})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

// ─── KYC ─────────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct KycRequest {
    full_name: String,
}

// TODO(compliance): real KYC/AML integration (vendor + jurisdiction +
// address screening) is deferred — this endpoint only flags kyc_status.
// See docs/plans/deferred-todos.md §1. Not needed at the current stage.
async fn handle_submit_kyc(
    State(s): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<KycRequest>,
) -> impl IntoResponse {
    let user_id = match auth_user(&headers) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    if let Err(e) = ensure_user_owned_by_this_desk(user_id, s.desk_id) {
        return e.into_response();
    }
    if req.full_name.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "full_name required"})),
        )
            .into_response();
    }
    match sqlx::query_as::<_, User>(
        "UPDATE users SET full_name = $1, kyc_status = 'PENDING', updated_at = NOW()
         WHERE id = $2 AND kyc_status = 'NONE' RETURNING *",
    )
    .bind(req.full_name.trim())
    .bind(user_id)
    .fetch_optional(s.db.as_ref())
    .await
    {
        Ok(Some(u)) => (
            StatusCode::OK,
            Json(json!({
                "kyc_status": u.kyc_status,
                "message": "KYC submitted, pending review"
            })),
        )
            .into_response(),
        Ok(None) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "KYC already submitted or user not found"})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

// ─── Password change ──────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct ChangePasswordRequest {
    current_password: String,
    new_password: String,
}

async fn handle_change_password(
    State(s): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<ChangePasswordRequest>,
) -> impl IntoResponse {
    let user_id = match auth_user(&headers) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    if let Err(e) = ensure_user_owned_by_this_desk(user_id, s.desk_id) {
        return e.into_response();
    }
    if req.new_password.len() < 8 {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "new_password must be at least 8 characters"})),
        )
            .into_response();
    }
    // Fetch current hash
    let row = sqlx::query_as::<_, User>("SELECT * FROM users WHERE id = $1")
        .bind(user_id)
        .fetch_optional(s.db.as_ref())
        .await;
    let user = match row {
        Ok(Some(u)) => u,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "User not found"})),
            )
                .into_response();
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };
    // Verify current password
    match bcrypt::verify(&req.current_password, &user.password_hash) {
        Ok(true) => {}
        _ => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error": "Current password is incorrect"})),
            )
                .into_response();
        }
    }
    // Hash and save new password
    let new_hash = match bcrypt::hash(&req.new_password, bcrypt::DEFAULT_COST) {
        Ok(h) => h,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };
    match sqlx::query("UPDATE users SET password_hash = $1, updated_at = NOW() WHERE id = $2")
        .bind(&new_hash)
        .bind(user_id)
        .execute(s.db.as_ref())
        .await
    {
        Ok(_) => (StatusCode::OK, Json(json!({"message": "Password updated"}))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

// ─── Test Funds ───────────────────────────────────────────────────────────────

/// Grant test funds to the authenticated user.
/// Only works when total USDT balance is below 100 (prevents repeated claims).
async fn handle_test_funds(State(s): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    let user_id = match auth_user(&headers) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    if let Err(e) = ensure_user_owned_by_this_desk(user_id, s.desk_id) {
        return e.into_response();
    }

    // Check current USDT balance. atoms column — the float8 twin was
    // dropped by migration 018; this endpoint had silently broken then
    // (caught by the S1.6 chaos drill setting up funds via REST).
    let usdt_balance_atoms: Option<i64> = sqlx::query_scalar(
        "SELECT balance_atoms FROM accounts WHERE user_id = $1 AND asset = 'USDT'",
    )
    .bind(user_id)
    .fetch_optional(s.db.as_ref())
    .await
    .unwrap_or(None);

    if usdt_balance_atoms.unwrap_or(0) >= 100 * 100_000_000 {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": "You already have funds. Test funds can only be claimed when USDT balance is below 100."}))).into_response();
    }

    // Credit 10,000 USDT, 1 BTC, 10 ETH, 100 SOL
    let result = sqlx::query(
        "INSERT INTO accounts (user_id, asset, balance_atoms, frozen_atoms)
         VALUES
            ($1, 'USDT', 1000000000000, 0),
            ($1, 'BTC', 100000000, 0),
            ($1, 'ETH', 1000000000, 0),
            ($1, 'SOL', 10000000000, 0)
         ON CONFLICT (user_id, asset) DO UPDATE
         SET balance_atoms = accounts.balance_atoms + EXCLUDED.balance_atoms,
             updated_at = NOW()",
    )
    .bind(user_id)
    .execute(s.db.as_ref())
    .await;

    match result {
        Ok(_) => {
            s.account_cache.remove(&user_id); // reload on next read
            (StatusCode::OK, Json(json!({"message": "Test funds granted: 10,000 USDT + 1 BTC + 10 ETH + 100 SOL", "usdt": 10000, "btc": 1, "eth": 10, "sol": 100}))).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// Top up robot account to maintain sufficient inventory for market-making.
/// Restricted to the robot service account — rejects all other callers.
async fn handle_robot_funds(State(s): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    let claims = match auth_claims(&headers) {
        Ok(c) => c,
        Err(e) => return e.into_response(),
    };
    if claims.email != "robot@lightningx.exchange" {
        return (StatusCode::FORBIDDEN, Json(json!({"error": "Forbidden"}))).into_response();
    }
    let user_id = claims.sub;
    if let Err(e) = ensure_user_owned_by_this_desk(user_id, s.desk_id) {
        return e.into_response();
    }

    let result = sqlx::query(
        "INSERT INTO accounts (user_id, asset, balance_atoms, frozen_atoms)
         VALUES
            ($1, 'USDT', 1000000000000000, 0),
            ($1, 'BTC', 1000000000000, 0),
            ($1, 'ETH', 100000000000000, 0),
            ($1, 'SOL', 1000000000000000, 0)
         ON CONFLICT (user_id, asset) DO UPDATE
         SET balance_atoms = GREATEST(accounts.balance_atoms, EXCLUDED.balance_atoms),
             frozen_atoms = 0,
             updated_at = NOW()",
    )
    .bind(user_id)
    .execute(s.db.as_ref())
    .await;

    match result {
        Ok(_) => {
            s.account_cache.remove(&user_id); // reload on next read
            (StatusCode::OK, Json(json!({"ok": true}))).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

// ─── K-lines ──────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct KlinesQuery {
    symbol: String,
    interval: Option<String>,
    limit: Option<i64>,
}

async fn handle_klines(
    State(s): State<AppState>,
    Query(params): Query<KlinesQuery>,
) -> impl IntoResponse {
    let interval = params.interval.unwrap_or_else(|| "1m".to_string());
    let limit = params.limit.unwrap_or(200).min(1000);
    // Read from klines table (written by kline_service). Return the most recent
    // N bars in ascending time order so the frontend always sees current data.
    let rows = sqlx::query(
        "SELECT open_time AS time, open, high, low, close, volume
         FROM (
           SELECT open_time, open, high, low, close, volume
           FROM klines
           WHERE symbol = $1 AND interval = $2
           ORDER BY open_time DESC
           LIMIT $3
         ) sub
         ORDER BY time ASC",
    )
    .bind(&params.symbol)
    .bind(&interval)
    .bind(limit)
    .fetch_all(s.db.as_ref())
    .await;

    match rows {
        Ok(rows) => {
            use sqlx::Row;
            let candles: Vec<Value> = rows
                .iter()
                .map(|r| {
                    json!({
                        "time":   r.get::<i64, _>("time"),
                        "open":   r.get::<f64, _>("open"),
                        "high":   r.get::<f64, _>("high"),
                        "low":    r.get::<f64, _>("low"),
                        "close":  r.get::<f64, _>("close"),
                        "volume": r.get::<f64, _>("volume"),
                    })
                })
                .collect();
            (StatusCode::OK, Json(json!(candles))).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

// ─── Seed demo ────────────────────────────────────────────────────────────────

/// Plant demo limit orders for ETH_USDT, SOL_USDT, and BTC_USDT so the exchange has
/// live order books for demos. Idempotent per symbol.
async fn handle_seed_demo(State(s): State<AppState>) -> impl IntoResponse {
    // Find or create the demo user.
    let demo_user_id: i64 = match sqlx::query_scalar::<_, i64>(
        "INSERT INTO users (email, password_hash, full_name, kyc_status)
         VALUES ('demo@lightning.exchange', 'x', 'Demo User', 'verified')
         ON CONFLICT (email) DO UPDATE SET id = users.id RETURNING id",
    )
    .fetch_one(s.db.as_ref())
    .await
    {
        Ok(id) => id,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };

    // Upsert demo balances (including BTC).
    let _ = sqlx::query(
        "INSERT INTO accounts (user_id, asset, balance_atoms, frozen_atoms)
         VALUES
            ($1, 'USDT', 10000000000000, 0),
            ($1, 'BTC', 500000000, 0),
            ($1, 'ETH', 5000000000, 0),
            ($1, 'SOL', 100000000000, 0)
         ON CONFLICT (user_id, asset) DO UPDATE
         SET balance_atoms = GREATEST(accounts.balance_atoms, EXCLUDED.balance_atoms),
             updated_at = NOW()",
    )
    .bind(demo_user_id)
    .execute(s.db.as_ref())
    .await;

    // (symbol, side, prices, qty, base, quote)
    let plans: Vec<(&str, &str, Vec<f64>, f64, &str, &str)> = vec![
        (
            "ETH_USDT",
            "buy",
            vec![2990.0, 2980.0, 2970.0, 2960.0, 2950.0],
            0.5,
            "ETH",
            "USDT",
        ),
        (
            "ETH_USDT",
            "sell",
            vec![3010.0, 3020.0, 3030.0, 3040.0, 3050.0],
            0.5,
            "ETH",
            "USDT",
        ),
        (
            "SOL_USDT",
            "buy",
            vec![149.0, 148.0, 147.0, 146.0, 145.0],
            10.0,
            "SOL",
            "USDT",
        ),
        (
            "SOL_USDT",
            "sell",
            vec![151.0, 152.0, 153.0, 154.0, 155.0],
            10.0,
            "SOL",
            "USDT",
        ),
        (
            "BTC_USDT",
            "buy",
            vec![50800.0, 50700.0, 50600.0, 50500.0, 50400.0],
            0.1,
            "BTC",
            "USDT",
        ),
        (
            "BTC_USDT",
            "sell",
            vec![51200.0, 51300.0, 51400.0, 51500.0, 51600.0],
            0.1,
            "BTC",
            "USDT",
        ),
    ];

    let repo = AccountRepository::new(&s.db);
    let mut placed = 0usize;

    for (symbol, side, prices, qty, base, quote) in plans {
        // Per-symbol idempotency: skip this symbol if it already has active orders.
        let already: Option<i64> = sqlx::query_scalar(
            "SELECT id FROM orders WHERE symbol = $1 AND user_id = $2 AND status IN ('PENDING','TRADING') LIMIT 1",
        )
        .bind(symbol).bind(demo_user_id)
        .fetch_optional(s.db.as_ref()).await.unwrap_or(None);
        if already.is_some() {
            continue;
        }

        let engine_opt: Option<Arc<Mutex<MatchingEngine>>> = s
            .engines
            .as_ref()
            .and_then(|m| m.get(symbol).map(|e| e.value().clone()));
        // In Aeron mode there is no local engine; seed is DB-only (no orderbook).
        let engine_side = if side == "buy" { Side::Buy } else { Side::Sell };

        for price in prices {
            // Freeze the appropriate funds first (best-effort).
            let freeze = if side == "buy" {
                repo.freeze_for_buy(demo_user_id, quote, price * qty).await
            } else {
                repo.freeze_for_sell(demo_user_id, base, qty).await
            };
            if let Err(e) = freeze {
                tracing::warn!(
                    "seed-demo freeze failed for {} {} @ {}: {}",
                    symbol,
                    side,
                    price,
                    e
                );
                continue;
            }

            // Insert the order row, getting a fresh id from the sequence.
            // Limit buys persist `price` as freeze_price (USDT cost backing);
            // sells persist 0 since freezing is by base-asset quantity.
            let seed_freeze_price = if side == "buy" { price } else { 0.0 };
            let inserted = sqlx::query_as::<_, DbOrder>(
                "INSERT INTO orders (id, user_id, symbol, side, order_type, price_atoms, quantity_atoms, filled_atoms, status, freeze_price_atoms)
                 VALUES (nextval('orders_id_seq'), $1, $2, $3, 'limit', $4, $5, 0, 'PENDING', $6) RETURNING *",
            )
            .bind(demo_user_id)
            .bind(symbol)
            .bind(side)
            .bind(crate::desk::money::AmountAtoms::from_f64_round(price).map(|a| a.atoms()).unwrap_or(0))
            .bind(crate::desk::money::AmountAtoms::from_f64_round(qty).map(|a| a.atoms()).unwrap_or(0))
            .bind(crate::desk::money::AmountAtoms::from_f64_round(seed_freeze_price).map(|a| a.atoms()).unwrap_or(0))
            .fetch_one(s.db.as_ref())
            .await;

            let db_order = match inserted {
                Ok(o) => o,
                Err(e) => {
                    tracing::warn!(
                        "seed-demo insert failed for {} {} @ {}: {}",
                        symbol,
                        side,
                        price,
                        e
                    );
                    // Roll back the freeze we just did.
                    let asset = if side == "buy" { quote } else { base };
                    let amount = if side == "buy" { price * qty } else { qty };
                    let _ = repo.release_frozen(demo_user_id, asset, amount).await;
                    continue;
                }
            };

            // Place into the engine book directly (no matching, no fund effects).
            // In Aeron mode we skip local book insertion; the exchange_engine manages its own book.
            if let Some(ref engine) = engine_opt {
                let rules = crate::symbol_rules::SymbolRules::for_symbol(symbol);
                let price_ticks = match rules.price_to_ticks(price) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let quantity_lots = match rules.quantity_to_lots(qty) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let order = Order::new(
                    db_order.id as u64,
                    engine_side,
                    price_ticks,
                    quantity_lots,
                    TimeInForce::GTC,
                    0,
                );
                let mut eng = engine.lock().unwrap();
                let _ = eng.add_to_book(order);
            }
            placed += 1;
        }
    }

    (
        StatusCode::OK,
        Json(json!({"status": "seeded", "orders_placed": placed, "demo_user_id": demo_user_id})),
    )
        .into_response()
}

// ─── Debug: Force mark price (TEST_ENDPOINTS=1 only) ─────────────────────────

#[derive(Deserialize)]
struct DebugMarkPriceReq {
    symbol: String,
    price: f64,
}

/// Force-set the mark price for a symbol and re-run the risk tick.
/// Only active when the `TEST_ENDPOINTS=1` environment variable is set.
/// Returns 403 otherwise.  Used by integration tests to trigger liquidations
/// without a live market data feed.
async fn handle_debug_mark_price(
    State(s): State<AppState>,
    Json(req): Json<DebugMarkPriceReq>,
) -> impl IntoResponse {
    if std::env::var("TEST_ENDPOINTS").as_deref() != Ok("1") {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "TEST_ENDPOINTS not enabled"})),
        )
            .into_response();
    }

    let rules = crate::symbol_rules::SymbolRules::for_symbol(&req.symbol);
    let price_ticks = (req.price / rules.price_tick).round() as i64;

    let mut sym_bytes = [0u8; 16];
    let b = req.symbol.as_bytes();
    sym_bytes[..b.len().min(16)].copy_from_slice(&b[..b.len().min(16)]);

    // Test-only path: force the base mark, then run one EWMA update so
    // unrealized PnL and equity are recomputed deterministically.
    s.risk_engine
        .force_mark_price_for_test(sym_bytes, price_ticks);
    // Converge the EWMA to the target in ONE call so tests are
    // deterministic (otherwise each call moves the mark only ~10% toward
    // the target and the test must spin, which flakes under load).
    for _ in 0..40 {
        s.risk_engine
            .update_mark_price(sym_bytes, price_ticks, rules.notional_scale);
    }

    let events = s.risk_engine.run_risk_tick();
    let liq_count = events.len();

    // Dispatch liquidation events to the priority ring (same as the 10ms
    // task, including the S6.1 tranche).
    let tranche_bps: i64 = std::env::var("LIQ_TRANCHE_BPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000);
    for mut evt in events {
        evt.qty_lots =
            crate::desk::risk::calc::liquidation_tranche_lots(evt.qty_lots, tranche_bps);
        use crate::desk::risk::PositionSide;
        use crate::sbe::NewOrderRequest as SbeNewOrder;
        use crate::transport::{AeronCmd, OrderMeta, pack_str16};
        use std::sync::atomic::Ordering;

        if evt.liq_price_ticks == 0 {
            continue;
        }

        s.risk_engine
            .set_account_status(evt.user_id, crate::desk::risk::RiskStatus::Liquidating);
        let Some(ref cmd_tx) = s.liq_cmd_tx else {
            continue;
        };

        let sym_str_end = evt.symbol.iter().position(|&b| b == 0).unwrap_or(16);
        let sym_str = std::str::from_utf8(&evt.symbol[..sym_str_end]).unwrap_or("BTC_USDT");
        let liq_rules = crate::symbol_rules::SymbolRules::for_symbol(sym_str);

        let liq_side: u8 = if evt.side == PositionSide::Long { 1 } else { 0 };
        let order_id = s.next_order_id.fetch_add(1, Ordering::Relaxed);

        let sbe_req = SbeNewOrder {
            client_order_id: order_id,
            participant_id: evt.user_id as u64,
            price_ticks: evt.liq_price_ticks,
            quantity_lots: evt.qty_lots,
            side: liq_side,
            time_in_force: 1,
            response_stream_id: s.response_stream_id,
            reduce_only: 0,
            _pad: [0; 9],
            symbol: evt.symbol,
        };
        let mark_ticks = s.risk_engine.mark_price_ticks(&evt.symbol).unwrap_or(0);
        let notional = if mark_ticks > 0 {
            crate::desk::risk::calc::calc_notional_atoms(
                mark_ticks,
                evt.qty_lots,
                liq_rules.notional_scale,
            )
        } else {
            0
        };
        let margin_atoms = crate::desk::risk::calc::calc_initial_margin_atoms(
            notional,
            liq_rules.default_leverage,
        );
        let order_type = if liq_side == 0 { "liq-buy" } else { "liq-sell" };
        s.pending_meta.insert(
            order_id,
            OrderMeta {
                user_id: evt.user_id,
                symbol: evt.symbol,
                side: liq_side,
                order_type: pack_str16(order_type),
                price: None,
                qty: liq_rules.lots_to_quantity(evt.qty_lots),
                client_order_id: format!("liq-{}", order_id),
                freeze_price: 0.0,
                initial_margin_atoms: margin_atoms,
                liq_price_ticks: evt.liq_price_ticks,
            },
        );
        if cmd_tx.push(AeronCmd::NewOrder(sbe_req)).is_err() {
            s.pending_meta.remove(&order_id);
        }
    }

    (
        StatusCode::OK,
        Json(json!({
            "symbol": req.symbol,
            "price": req.price,
            "price_ticks": price_ticks,
            "liquidations_triggered": liq_count,
        })),
    )
        .into_response()
}
