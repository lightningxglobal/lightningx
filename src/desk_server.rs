/// Desk Server：多柜台服务器，处理客户端委托和行情推送
///
/// 职责：
/// 1. WebSocket /ws/order - 委托上报（buy/sell/cancel）
/// 2. WebSocket /ws/market - 行情推送（Depth/Trade/BBO）
/// 3. Rate Limit 检查（本地令牌桶）
/// 4. 风控检查（本地规则）
/// 5. 验资验券（Account Service）
/// 6. 生成 Snowflake Order ID
/// 7. Aeron 集成（发送委托、接收回报、接收行情）
/// 8. 会话管理和路由

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
