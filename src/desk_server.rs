use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use serde::{Deserialize, Serialize};

use crate::rate_limit::{RateLimiter, RateLimitPolicy};
use crate::snowflake::SnowflakeIdGenerator;
use crate::account::AccountManager;

/// Desk 服务器配置
#[derive(Clone, Debug)]
pub struct DeskConfig {
    /// Desk ID (0-32767)
    pub desk_id: u16,
    /// 监听地址
    pub addr: String,
    /// Rate Limit 策略
    pub rate_limit_policy: RateLimitPolicy,
}

impl Default for DeskConfig {
    fn default() -> Self {
        Self {
            desk_id: 1,
            addr: "127.0.0.1:3000".to_string(),
            rate_limit_policy: RateLimitPolicy::default_trading(),
        }
    }
}

/// 会话 ID（用户连接的唯一标识）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionId(u64);

impl SessionId {
    fn new() -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SESSION_COUNTER: AtomicU64 = AtomicU64::new(1);
        SessionId(SESSION_COUNTER.fetch_add(1, Ordering::Relaxed))
    }
}

/// 客户端委托请求
#[derive(Debug, Clone, Deserialize)]
pub struct OrderRequest {
    pub symbol: String,
    pub side: String,    // "buy" or "sell"
    pub price: f64,
    pub quantity: f64,
}

/// 取消委托请求
#[derive(Debug, Clone, Deserialize)]
pub struct CancelRequest {
    pub order_id: u64,
}

/// 客户端消息（从WebSocket接收）
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum ClientMessage {
    #[serde(rename = "new_order")]
    NewOrder(OrderRequest),
    #[serde(rename = "cancel")]
    Cancel(CancelRequest),
    #[serde(rename = "subscribe")]
    Subscribe { channels: Option<Vec<String>> },
}

/// 服务器响应消息（发送到WebSocket）
#[derive(Debug, Serialize)]
#[serde(tag = "type")]
pub enum ServerMessage {
    #[serde(rename = "error")]
    Error { message: String },
    #[serde(rename = "order_accepted")]
    OrderAccepted { order_id: u64 },
    #[serde(rename = "order_filled")]
    OrderFilled { order_id: u64, price: f64, qty: f64 },
    #[serde(rename = "order_cancelled")]
    OrderCancelled { order_id: u64 },
}

/// Desk Server 状态
pub struct DeskServer {
    pub config: DeskConfig,

    // ID 生成
    id_gen: SnowflakeIdGenerator,

    // Rate Limit（本地）
    rate_limiter: RwLock<RateLimiter>,

    // 账户管理（暂时本地，后续接 Redis）
    account_mgr: RwLock<AccountManager>,

    // 会话管理
    sessions: Arc<RwLock<HashMap<SessionId, SessionInfo>>>,

    // 订单追踪：client_order_id -> order_id
    order_mapping: Arc<RwLock<HashMap<String, u64>>>,

    // 行情广播频道
    market_broadcast_tx: broadcast::Sender<MarketDataUpdate>,
}

/// 会话信息
#[derive(Clone, Debug)]
pub struct SessionInfo {
    pub session_id: SessionId,
    pub client_id: String,
    pub account_id: u64,
    pub subscribed_channels: Vec<String>,
}

/// 行情更新（用于广播给WebSocket客户端）
#[derive(Debug, Clone)]
pub enum MarketDataUpdate {
    Depth { symbol: String, data: String },
    Trade { symbol: String, data: String },
}

impl DeskServer {
    /// 创建新的 Desk Server
    pub fn new(config: DeskConfig) -> Self {
        let id_gen = SnowflakeIdGenerator::new(config.desk_id as u64);
        let rate_limiter = RateLimiter::new(config.rate_limit_policy);
        let mut account_mgr = AccountManager::new();

        // 创建演示账户（开发用）
        let _ = account_mgr.create_account(1, 100_000.0);
        let _ = account_mgr.create_account(2, 100_000.0);

        let (_market_tx, _) = broadcast::channel(10000);

        Self {
            config,
            id_gen,
            rate_limiter: RwLock::new(rate_limiter),
            account_mgr: RwLock::new(account_mgr),
            sessions: Arc::new(RwLock::new(HashMap::new())),
            order_mapping: Arc::new(RwLock::new(HashMap::new())),
            market_broadcast_tx: _market_tx,
        }
    }

    /// Authenticate a JWT token and create a session. Returns Err if the token is invalid.
    pub async fn authenticate_and_create_session(&self, token: &str) -> Result<SessionId, String> {
        let claims = crate::user_service::verify_token(token)
            .map_err(|e| format!("Auth failed: {}", e))?;
        Ok(self.create_session(claims.email.clone(), claims.sub as u64).await)
    }

    /// 创建新会话
    pub async fn create_session(&self, client_id: String, account_id: u64) -> SessionId {
        let session_id = SessionId::new();
        let session = SessionInfo {
            session_id,
            client_id,
            account_id,
            subscribed_channels: vec![],
        };
        self.sessions.write().await.insert(session_id, session);
        session_id
    }

    /// 获取会话信息
    pub async fn get_session(&self, session_id: SessionId) -> Option<SessionInfo> {
        self.sessions.read().await.get(&session_id).cloned()
    }

    /// 处理委托请求（核心验证逻辑）
    pub async fn process_order_request(
        &self,
        session_id: SessionId,
        client_order_id: String,
        request: OrderRequest,
    ) -> Result<u64, String> {
        // 1. 获取会话
        let session = self
            .get_session(session_id)
            .await
            .ok_or_else(|| "Session not found".to_string())?;

        // 2. Rate Limit 检查
        let mut limiter = self.rate_limiter.write().await;
        limiter
            .consume(&session.client_id, 1)
            .map_err(|e| format!("Rate limit exceeded: {}", e))?;
        drop(limiter);

        // 3. 生成 Snowflake Order ID
        let order_id = self.id_gen.next_id();

        // 4. 验资验券
        let account_mgr = self.account_mgr.read().await;
        let account = account_mgr
            .get_account(session.account_id)
            .map_err(|e| format!("Account error: {}", e))?;

        if request.side == "buy" {
            let required = request.price * request.quantity;
            if account.available_balance() < required {
                return Err(format!(
                    "Insufficient balance: have {:.8}, need {:.8}",
                    account.available_balance(),
                    required
                ));
            }
        } else {
            // sell: validate position holdings
            let symbol = request.symbol.split('_').next().unwrap_or(&request.symbol);
            let position = account.get_position(symbol)
                .ok_or_else(|| format!("No position in {}", symbol))?;
            if position.available() < request.quantity {
                return Err(format!(
                    "Insufficient position: have {:.8}, need {:.8} {}",
                    position.available(),
                    request.quantity,
                    symbol
                ));
            }
        }
        drop(account_mgr);

        // 5. 记录订单映射
        self.order_mapping
            .write()
            .await
            .insert(client_order_id, order_id);

        Ok(order_id)
    }

    /// 处理取消委托请求
    pub async fn process_cancel_request(
        &self,
        session_id: SessionId,
        request: CancelRequest,
    ) -> Result<(), String> {
        // 1. 获取会话
        let session = self
            .get_session(session_id)
            .await
            .ok_or_else(|| "Session not found".to_string())?;

        // 2. Rate Limit 检查
        let mut limiter = self.rate_limiter.write().await;
        limiter
            .consume(&session.client_id, 1)
            .map_err(|e| format!("Rate limit exceeded: {}", e))?;
        drop(limiter);

        Ok(())
    }

    /// 获取行情广播发送者
    pub fn get_market_tx(&self) -> broadcast::Sender<MarketDataUpdate> {
        self.market_broadcast_tx.clone()
    }

    /// 广播行情数据
    pub async fn broadcast_market_data(&self, update: MarketDataUpdate) {
        let _ = self.market_broadcast_tx.send(update);
    }

    /// 处理订单 WebSocket 连接（简化版：返回委托回报）
    pub async fn handle_order_ws(&self, session_id: SessionId, req: OrderRequest) -> ServerMessage {
        let client_id = format!("order_{}", uuid::Uuid::new_v4());
        match self.process_order_request(session_id, client_id, req).await {
            Ok(order_id) => ServerMessage::OrderAccepted { order_id },
            Err(e) => ServerMessage::Error { message: e },
        }
    }

    /// 处理取消委托请求
    pub async fn handle_cancel_ws(&self, session_id: SessionId, req: CancelRequest) -> ServerMessage {
        match self.process_cancel_request(session_id, req.clone()).await {
            Ok(()) => ServerMessage::OrderCancelled {
                order_id: req.order_id,
            },
            Err(e) => ServerMessage::Error { message: e },
        }
    }

    /// 获取市场数据订阅接收器
    pub fn subscribe_market_data(&self) -> broadcast::Receiver<MarketDataUpdate> {
        self.market_broadcast_tx.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_desk_server_creation() {
        let config = DeskConfig::default();
        let desk = DeskServer::new(config);
        assert_eq!(desk.config.desk_id, 1);
    }

    #[tokio::test]
    async fn test_session_management() {
        let desk = DeskServer::new(DeskConfig::default());

        let session_id = desk.create_session("client1".to_string(), 1).await;
        let session = desk.get_session(session_id).await;

        assert!(session.is_some());
        let s = session.unwrap();
        assert_eq!(s.client_id, "client1");
        assert_eq!(s.account_id, 1);
    }

    #[tokio::test]
    async fn test_order_request_processing() {
        let desk = DeskServer::new(DeskConfig::default());
        let session_id = desk.create_session("client1".to_string(), 1).await;

        let request = OrderRequest {
            symbol: "BTC".to_string(),
            side: "buy".to_string(),
            price: 100.0,
            quantity: 100.0,
        };

        let result = desk
            .process_order_request(session_id, "co_123".to_string(), request)
            .await;

        assert!(result.is_ok(), "Order should be accepted");
        let order_id = result.unwrap();
        assert!(order_id > 0);
    }

    #[tokio::test]
    async fn test_rate_limit_enforcement() {
        let mut config = DeskConfig::default();
        config.rate_limit_policy = RateLimitPolicy {
            requests_per_second: 2,
            burst_capacity: 2,
        };
        let desk = DeskServer::new(config);
        let session_id = desk.create_session("client1".to_string(), 1).await;

        let request = OrderRequest {
            symbol: "BTC".to_string(),
            side: "buy".to_string(),
            price: 100.0,
            quantity: 100.0,
        };

        // 前两个请求应该成功
        let r1 = desk
            .process_order_request(session_id, "co_1".to_string(), request.clone())
            .await;
        assert!(r1.is_ok());

        let r2 = desk
            .process_order_request(session_id, "co_2".to_string(), request.clone())
            .await;
        assert!(r2.is_ok());

        // 第三个应该被限流
        let r3 = desk
            .process_order_request(session_id, "co_3".to_string(), request.clone())
            .await;
        assert!(r3.is_err(), "Should be rate limited");
    }

    #[tokio::test]
    async fn test_insufficient_balance() {
        let desk = DeskServer::new(DeskConfig::default());
        let session_id = desk.create_session("client1".to_string(), 1).await;

        let request = OrderRequest {
            symbol: "BTC".to_string(),
            side: "buy".to_string(),
            price: 100_000.0,  // 太高
            quantity: 1000.0,   // 总共需要 100,000,000
        };

        let result = desk
            .process_order_request(session_id, "co_123".to_string(), request)
            .await;

        assert!(result.is_err(), "Should fail due to insufficient balance");
    }

    #[tokio::test]
    async fn test_cancel_request() {
        let desk = DeskServer::new(DeskConfig::default());
        let session_id = desk.create_session("client1".to_string(), 1).await;

        let cancel_request = CancelRequest { order_id: 12345 };

        let result = desk
            .process_cancel_request(session_id, cancel_request)
            .await;

        assert!(result.is_ok(), "Cancel should be accepted (rate limit passes)");
    }

    #[tokio::test]
    async fn test_market_broadcast() {
        let desk = Arc::new(DeskServer::new(DeskConfig::default()));

        let tx = desk.get_market_tx();
        let mut rx = tx.subscribe();

        // 在后台发送行情
        let desk_clone = desk.clone();
        tokio::spawn(async move {
            desk_clone
                .broadcast_market_data(MarketDataUpdate::Depth {
                    symbol: "BTC".to_string(),
                    data: r#"{"bid":100.0,"ask":101.0}"#.to_string(),
                })
                .await;
        });

        // 接收广播
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            rx.recv(),
        )
        .await;

        assert!(result.is_ok(), "Should receive market data broadcast");
    }
}

// ─── DeskAppState ─────────────────────────────────────────────────────────────

use std::sync::atomic::AtomicU64;
use std::sync::Mutex;
use dashmap::DashMap;
use sqlx::PgPool;
use tokio::sync::mpsc;

use crate::engine::MatchingEngine;

#[derive(Clone)]
pub struct DeskAppState {
    pub db: Arc<PgPool>,
    pub engine: Arc<Mutex<MatchingEngine>>,
    pub market_tx: Arc<broadcast::Sender<String>>,
    pub user_tx: Arc<DashMap<i64, mpsc::Sender<String>>>,
    pub next_order_id: Arc<AtomicU64>,
    pub rate_limiter: Arc<parking_lot::Mutex<RateLimiter>>,
    pub id_gen: Arc<SnowflakeIdGenerator>,
}

// ─── Price-deviation risk check ───────────────────────────────────────────────

pub fn check_price_risk(engine: &MatchingEngine, _side: &str, price: f64) -> Result<(), String> {
    let bids = engine.get_top_levels(1, true);
    let asks = engine.get_top_levels(1, false);
    let mid = match (bids.first(), asks.first()) {
        (Some((b, _)), Some((a, _))) => (b + a) / 2.0,
        (Some((b, _)), None) => *b,
        (None, Some((a, _))) => *a,
        _ => return Ok(()),
    };
    if mid > 0.0 && (price - mid).abs() / mid > 0.10 {
        return Err(format!("Price {:.2} deviates >10% from mid {:.2}", price, mid));
    }
    Ok(())
}

// ─── WebSocket upgrade handler ────────────────────────────────────────────────

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};
use crate::order::{Order, Side, TimeInForce};
use crate::engine::OrderStatus;
use crate::user_service;

fn desk_unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[derive(Debug)]
enum DeskClientMsg {
    Auth { token: String },
    Subscribe { channels: Vec<String> },
    Unsubscribe { channels: Vec<String> },
    PlaceOrder {
        client_order_id: String,
        symbol: String,
        side: String,
        order_type: String,
        price: Option<f64>,
        qty: f64,
    },
    CancelOrder { order_id: i64 },
    Ping,
}

impl DeskClientMsg {
    fn parse(text: &str) -> Option<Self> {
        let v: Value = serde_json::from_str(text).ok()?;
        let t = v.get("type")?.as_str()?;
        match t {
            "auth" => Some(Self::Auth {
                token: v.get("token")?.as_str()?.to_owned(),
            }),
            "subscribe" => {
                let channels = v.get("channels")?
                    .as_array()?
                    .iter()
                    .filter_map(|c| c.as_str().map(|s| s.to_owned()))
                    .collect();
                Some(Self::Subscribe { channels })
            }
            "unsubscribe" => {
                let channels = v.get("channels")?
                    .as_array()?
                    .iter()
                    .filter_map(|c| c.as_str().map(|s| s.to_owned()))
                    .collect();
                Some(Self::Unsubscribe { channels })
            }
            "place_order" => Some(Self::PlaceOrder {
                client_order_id: v.get("client_order_id")?.as_str()?.to_owned(),
                symbol: v.get("symbol")?.as_str()?.to_owned(),
                side: v.get("side")?.as_str()?.to_owned(),
                order_type: v.get("order_type")
                    .and_then(|x| x.as_str())
                    .unwrap_or("limit")
                    .to_owned(),
                price: v.get("price").and_then(|p| p.as_f64()),
                qty: v.get("qty").or_else(|| v.get("quantity"))?.as_f64()?,
            }),
            "cancel_order" => Some(Self::CancelOrder {
                order_id: v.get("order_id")?.as_i64()?,
            }),
            "ping" => Some(Self::Ping),
            _ => None,
        }
    }
}

struct DeskWsSession {
    user_id: Option<i64>,
    subscribed: HashSet<String>,
}

impl DeskWsSession {
    fn new() -> Self {
        Self { user_id: None, subscribed: HashSet::new() }
    }
}

pub async fn desk_ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<DeskAppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| desk_handle_socket(socket, state))
}

async fn desk_handle_socket(mut socket: WebSocket, state: DeskAppState) {
    let mut session = DeskWsSession::new();
    let (personal_tx, mut personal_rx) = mpsc::channel::<String>(64);
    let mut market_rx = state.market_tx.subscribe();

    loop {
        tokio::select! {
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        if let Some(reply) = desk_handle_message(
                            &text, &mut session, &state, personal_tx.clone()
                        ).await {
                            if socket.send(Message::Text(reply)).await.is_err() {
                                break;
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {}
                }
            }
            Some(msg) = personal_rx.recv() => {
                if socket.send(Message::Text(msg)).await.is_err() {
                    break;
                }
            }
            Ok(msg) = market_rx.recv() => {
                let should_forward = if msg.contains("\"type\":\"depth\"") {
                    desk_msg_symbol(&msg).map(|sym| {
                        session.subscribed.contains(&format!("depth.{}", sym))
                    }).unwrap_or(false)
                } else if msg.contains("\"type\":\"trade\"") {
                    desk_msg_symbol(&msg).map(|sym| {
                        session.subscribed.contains(&format!("trades.{}", sym))
                    }).unwrap_or(false)
                } else if msg.contains("\"type\":\"ticker\"") {
                    desk_msg_symbol(&msg).map(|sym| {
                        session.subscribed.contains(&format!("ticker.{}", sym))
                    }).unwrap_or(false)
                } else if msg.contains("\"type\":\"kline\"") {
                    desk_msg_symbol(&msg).map(|sym| {
                        session.subscribed.contains(&format!("kline.{}", sym))
                    }).unwrap_or(false)
                } else {
                    false
                };
                if should_forward {
                    if socket.send(Message::Text(msg)).await.is_err() {
                        break;
                    }
                }
            }
        }
    }

    if let Some(uid) = session.user_id {
        state.user_tx.remove(&uid);
    }
}

fn desk_msg_symbol(msg: &str) -> Option<&str> {
    let key = "\"symbol\":\"";
    let start = msg.find(key)? + key.len();
    let end = msg[start..].find('"')? + start;
    Some(&msg[start..end])
}

async fn desk_handle_message(
    text: &str,
    session: &mut DeskWsSession,
    state: &DeskAppState,
    personal_tx: mpsc::Sender<String>,
) -> Option<String> {
    let msg = match DeskClientMsg::parse(text) {
        Some(m) => m,
        None => return Some(json!({"type": "error", "message": "Invalid message format"}).to_string()),
    };

    match msg {
        DeskClientMsg::Auth { token } => {
            match user_service::verify_token(&token) {
                Ok(claims) => {
                    session.user_id = Some(claims.sub);
                    state.user_tx.insert(claims.sub, personal_tx);
                    Some(json!({"type": "auth_ok"}).to_string())
                }
                Err(e) => Some(json!({"type": "auth_error", "message": e.to_string()}).to_string()),
            }
        }

        DeskClientMsg::Subscribe { channels } => {
            for ch in channels { session.subscribed.insert(ch); }
            None
        }

        DeskClientMsg::Unsubscribe { channels } => {
            for ch in channels { session.subscribed.remove(&ch); }
            None
        }

        DeskClientMsg::Ping => Some(json!({"type": "pong"}).to_string()),

        DeskClientMsg::PlaceOrder { client_order_id, symbol, side, order_type, price, qty } => {
            let user_id = match session.user_id {
                Some(id) => id,
                None => return Some(json!({"type": "order_rejected", "client_order_id": client_order_id, "reason": "Not authenticated"}).to_string()),
            };

            // Rate limit check (per user)
            {
                let mut limiter = state.rate_limiter.lock();
                if limiter.consume(&user_id.to_string(), 1).is_err() {
                    return Some(json!({"type": "order_rejected", "client_order_id": client_order_id, "reason": "Rate limit exceeded"}).to_string());
                }
            }

            if qty <= 0.0 {
                return Some(json!({"type": "order_rejected", "client_order_id": client_order_id, "reason": "qty must be > 0"}).to_string());
            }
            if order_type != "market" && price.map(|p| p <= 0.0).unwrap_or(true) {
                return Some(json!({"type": "order_rejected", "client_order_id": client_order_id, "reason": "price required for limit orders"}).to_string());
            }

            let engine_side = match side.as_str() {
                "buy" => Side::Buy,
                "sell" => Side::Sell,
                _ => return Some(json!({"type": "order_rejected", "client_order_id": client_order_id, "reason": "Invalid side"}).to_string()),
            };

            let tif = match order_type.as_str() {
                "limit" | "gtc" => TimeInForce::GTC,
                "ioc" => TimeInForce::IOC,
                "fok" => TimeInForce::FOK,
                "post_only" => TimeInForce::PostOnly,
                "market" => TimeInForce::IOC,
                _ => return Some(json!({"type": "order_rejected", "client_order_id": client_order_id, "reason": "Unknown order type"}).to_string()),
            };

            // Price deviation risk check (limit orders only)
            if order_type != "market" {
                if let Some(p) = price {
                    let eng = state.engine.lock().unwrap();
                    if let Err(e) = check_price_risk(&eng, &side, p) {
                        return Some(json!({"type": "order_rejected", "client_order_id": client_order_id, "reason": e}).to_string());
                    }
                }
            }

            let now_ns = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0);

            // Use Snowflake ID for engine-level order ID
            let order_id = state.id_gen.next_id();

            let engine_order = if order_type == "market" {
                Order::new_market(order_id, engine_side, qty, now_ns)
            } else {
                Order::new(order_id, engine_side, price.unwrap_or(0.0), qty, tif, now_ns)
            };

            // Persist to DB
            let db_result = sqlx::query_as::<_, crate::models::DbOrder>(
                "INSERT INTO orders (id, user_id, symbol, side, order_type, price, quantity, filled, status)
                 VALUES (nextval('orders_id_seq'), $1, $2, $3, $4, $5, $6, 0, 'PENDING') RETURNING *",
            )
            .bind(user_id)
            .bind(&symbol)
            .bind(&side)
            .bind(&order_type)
            .bind(price)
            .bind(qty)
            .fetch_one(state.db.as_ref())
            .await;

            let db_order_id = match db_result {
                Ok(o) => o.id,
                Err(e) => return Some(json!({"type": "order_rejected", "client_order_id": client_order_id, "reason": format!("DB error: {}", e)}).to_string()),
            };

            let best_opposing_price = {
                let eng = state.engine.lock().unwrap();
                let levels = eng.get_top_levels(1, engine_side == Side::Sell);
                levels.first().map(|(p, _)| *p)
            };

            let engine_result = {
                let mut eng = state.engine.lock().unwrap();
                eng.place_order(engine_order)
            };

            let result = match engine_result {
                Ok(r) => r,
                Err(e) => {
                    let _ = sqlx::query("UPDATE orders SET status = 'REJECTED', updated_at = NOW() WHERE id = $1")
                        .bind(db_order_id)
                        .execute(state.db.as_ref())
                        .await;
                    return Some(json!({"type": "order_rejected", "client_order_id": client_order_id, "reason": e.to_string()}).to_string());
                }
            };

            let (db_status, ws_status) = match result.status {
                OrderStatus::Accepted => ("TRADING", "OPEN"),
                OrderStatus::PartiallyFilled => ("TRADING", "PARTIAL_FILL"),
                OrderStatus::Filled => ("COMPLETED", "FILLED"),
                OrderStatus::Rejected => ("REJECTED", "REJECTED"),
                OrderStatus::Cancelled => ("CANCELED", "CANCELED"),
            };

            let _ = sqlx::query("UPDATE orders SET status = $1, filled = $2, updated_at = NOW() WHERE id = $3")
                .bind(db_status)
                .bind(result.filled)
                .bind(db_order_id)
                .execute(state.db.as_ref())
                .await;

            if result.status == OrderStatus::Rejected {
                return Some(json!({"type": "order_rejected", "client_order_id": client_order_id, "reason": "Rejected by matching engine"}).to_string());
            }

            let ts = desk_unix_now();

            if result.filled > 0.0 {
                let fill_price = price.or(best_opposing_price).unwrap_or(0.0);
                let trade_msg = json!({"type": "trade", "symbol": symbol, "price": fill_price, "qty": result.filled, "side": side, "ts": ts}).to_string();
                let _ = state.market_tx.send(trade_msg);
                desk_broadcast_depth(state, &symbol);
                let update_msg = json!({"type": "order_update", "order_id": db_order_id, "status": ws_status, "filled_qty": result.filled, "avg_price": fill_price, "ts": ts}).to_string();
                if let Some(tx) = state.user_tx.get(&user_id) {
                    let _ = tx.send(update_msg).await;
                }
            }

            Some(json!({"type": "order_accepted", "client_order_id": client_order_id, "order_id": db_order_id, "symbol": symbol, "side": side, "price": price, "qty": qty, "ts": ts}).to_string())
        }

        DeskClientMsg::CancelOrder { order_id } => {
            let user_id = match session.user_id {
                Some(id) => id,
                None => return Some(json!({"type": "error", "message": "Not authenticated"}).to_string()),
            };

            let db_result = sqlx::query(
                "UPDATE orders SET status = 'CANCELED', updated_at = NOW()
                 WHERE id = $1 AND user_id = $2 AND status IN ('PENDING','TRADING')",
            )
            .bind(order_id)
            .bind(user_id)
            .execute(state.db.as_ref())
            .await;

            match db_result {
                Ok(r) if r.rows_affected() > 0 => {
                    let _ = {
                        let mut eng = state.engine.lock().unwrap();
                        eng.cancel_order(order_id as u64)
                    };
                    Some(json!({"type": "order_update", "order_id": order_id, "status": "CANCELED", "ts": desk_unix_now()}).to_string())
                }
                Ok(_) => Some(json!({"type": "error", "message": "Order not found or already closed"}).to_string()),
                Err(e) => Some(json!({"type": "error", "message": e.to_string()}).to_string()),
            }
        }
    }
}

fn desk_broadcast_depth(state: &DeskAppState, symbol: &str) {
    let (bids, asks) = {
        let eng = state.engine.lock().unwrap();
        (eng.get_top_levels(10, true), eng.get_top_levels(10, false))
    };
    let msg = json!({"type": "depth", "symbol": symbol, "bids": bids, "asks": asks, "ts": desk_unix_now()}).to_string();
    let _ = state.market_tx.send(msg);
}

pub async fn desk_market_data_broadcaster(state: DeskAppState) {
    let mut depth_interval = tokio::time::interval(std::time::Duration::from_secs(2));
    let mut kline_interval = tokio::time::interval(std::time::Duration::from_secs(60));
    let mut last_price: f64 = 0.0;

    loop {
        tokio::select! {
            _ = depth_interval.tick() => {
                let (bids, asks) = {
                    let eng = state.engine.lock().unwrap();
                    (eng.get_top_levels(10, true), eng.get_top_levels(10, false))
                };
                if let Some((p, _)) = bids.first() { last_price = *p; }
                let msg = json!({"type": "depth", "symbol": "BTC_USDT", "bids": bids, "asks": asks, "ts": desk_unix_now()}).to_string();
                let _ = state.market_tx.send(msg);
            }
            _ = kline_interval.tick() => {
                if last_price > 0.0 {
                    let now = desk_unix_now();
                    let bar_time = now - (now % 60);
                    let msg = json!({"type": "kline", "symbol": "BTC_USDT", "bar": {"time": bar_time, "open": last_price, "high": last_price, "low": last_price, "close": last_price, "volume": 0.0}}).to_string();
                    let _ = state.market_tx.send(msg);
                }
            }
        }
    }
}
