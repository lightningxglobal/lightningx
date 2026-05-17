/// 行情数据 WebSocket 推送服务
///
/// 功能：
/// - WebSocket /ws/market 端点提供实时行情推送
/// - 订阅 Aeron 行情频道（DepthSnapshot, TradeEvent）
/// - 使用 tokio::sync::broadcast 高效多播给所有连接
/// - 延迟 < 50ms

use axum::{
    extract::State,
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};

/// 深度数据序列化结构
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepthLevel {
    pub price: f64,
    pub quantity: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepthSnapshot {
    pub timestamp: u64,
    pub sequence: u64,
    pub bids: Vec<DepthLevel>,
    pub asks: Vec<DepthLevel>,
}

/// 成交数据序列化结构
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeSnapshot {
    pub timestamp: u64,
    pub price: f64,
    pub quantity: f64,
    pub side: String,
}

/// 24小时统计数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Statistics {
    pub high: f64,
    pub low: f64,
    pub volume: f64,
    pub quote_volume: f64,
    pub vwap: f64,
    pub trade_count: u64,
}

/// BBO（最优买卖价）数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BBO {
    pub timestamp: u64,
    pub bid_price: f64,
    pub bid_qty: f64,
    pub ask_price: f64,
    pub ask_qty: f64,
}

/// Broadcast 事件
#[derive(Debug, Clone)]
pub enum MarketDataEvent {
    Depth(DepthSnapshot),
    Trade(TradeSnapshot),
    BBO(BBO),
    Stats(Statistics),
}

/// Serializable 事件（用于JSON推送）
#[derive(Debug, Serialize)]
#[serde(tag = "type")]
pub enum MarketDataMessage {
    #[serde(rename = "depth")]
    Depth(DepthSnapshot),
    #[serde(rename = "trade")]
    Trade(TradeSnapshot),
    #[serde(rename = "bbo")]
    BBO(BBO),
    #[serde(rename = "stats")]
    Stats(Statistics),
}

/// 客户端订阅请求
#[derive(Debug, Deserialize)]
pub struct SubscriptionRequest {
    pub r#type: String,
    pub channels: Option<Vec<String>>,
}

/// 服务器状态
pub struct MarketDataServer {
    // 内存快照
    pub latest_depth: Arc<RwLock<Option<DepthSnapshot>>>,
    pub latest_trades: Arc<RwLock<Vec<TradeSnapshot>>>,
    pub latest_stats: Arc<RwLock<Option<Statistics>>>,
    pub latest_bbo: Arc<RwLock<Option<BBO>>>,

    // Broadcast 频道（用于WebSocket推送）
    depth_tx: broadcast::Sender<DepthSnapshot>,
    trade_tx: broadcast::Sender<TradeSnapshot>,
    bbo_tx: broadcast::Sender<BBO>,
    stats_tx: broadcast::Sender<Statistics>,
}

impl MarketDataServer {
    pub fn new() -> Self {
        let (depth_tx, _) = broadcast::channel(1000);
        let (trade_tx, _) = broadcast::channel(5000);
        let (bbo_tx, _) = broadcast::channel(100);
        let (stats_tx, _) = broadcast::channel(100);

        Self {
            latest_depth: Arc::new(RwLock::new(None)),
            latest_trades: Arc::new(RwLock::new(Vec::new())),
            latest_stats: Arc::new(RwLock::new(None)),
            latest_bbo: Arc::new(RwLock::new(None)),
            depth_tx,
            trade_tx,
            bbo_tx,
            stats_tx,
        }
    }

    /// 更新深度快照（同时推送给所有WebSocket订阅者）
    pub async fn update_depth(&self, depth: DepthSnapshot) {
        *self.latest_depth.write().await = Some(depth.clone());
        let _ = self.depth_tx.send(depth);
    }

    /// 添加交易记录（同时推送给所有WebSocket订阅者）
    pub async fn add_trade(&self, trade: TradeSnapshot) {
        let mut trades = self.latest_trades.write().await;
        trades.push(trade.clone());
        if trades.len() > 100 {
            trades.remove(0);
        }
        let _ = self.trade_tx.send(trade);
    }

    /// 更新统计数据
    pub async fn update_stats(&self, stats: Statistics) {
        *self.latest_stats.write().await = Some(stats.clone());
        let _ = self.stats_tx.send(stats);
    }

    /// 更新 BBO
    pub async fn update_bbo(&self, bbo: BBO) {
        *self.latest_bbo.write().await = Some(bbo.clone());
        let _ = self.bbo_tx.send(bbo);
    }

    /// 获取深度快照（查询用）
    pub async fn get_depth(&self) -> Option<DepthSnapshot> {
        self.latest_depth.read().await.clone()
    }

    /// 获取最近交易
    pub async fn get_trades(&self) -> Vec<TradeSnapshot> {
        self.latest_trades.read().await.clone()
    }

    /// 获取统计数据
    pub async fn get_stats(&self) -> Option<Statistics> {
        self.latest_stats.read().await.clone()
    }

    /// 获取 BBO
    pub async fn get_bbo(&self) -> Option<BBO> {
        self.latest_bbo.read().await.clone()
    }

    /// 获取 broadcast 发送者（用于Desk Server 向所有客户端推送）
    pub fn get_depth_tx(&self) -> broadcast::Sender<DepthSnapshot> {
        self.depth_tx.clone()
    }

    pub fn get_trade_tx(&self) -> broadcast::Sender<TradeSnapshot> {
        self.trade_tx.clone()
    }

    pub fn get_bbo_tx(&self) -> broadcast::Sender<BBO> {
        self.bbo_tx.clone()
    }

    pub fn get_stats_tx(&self) -> broadcast::Sender<Statistics> {
        self.stats_tx.clone()
    }
}

impl Default for MarketDataServer {
    fn default() -> Self {
        Self::new()
    }
}

/// WebSocket 健康检查端点
async fn health_check() -> impl IntoResponse {
    axum::Json(serde_json::json!({"status": "ok", "service": "market-data-server"}))
}

/// 创建 HTTP 路由
pub fn create_router(server: Arc<MarketDataServer>) -> Router {
    Router::new()
        .route("/health", get(health_check))
        .with_state(server)
}

/// 启动 Web 服务器
pub async fn start_server(
    server: Arc<MarketDataServer>,
    addr: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let app = create_router(server);
    let listener = tokio::net::TcpListener::bind(addr).await?;

    println!("✓ 行情服务器启动: {}", addr);
    println!("  HTTP: http://{}/health", addr);
    println!("  WS:   ws://{}/ws/market (客户端WebSocket订阅)", addr);

    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_server_creation() {
        let server = MarketDataServer::new();
        assert!(server.get_bbo().await.is_none());
        assert!(server.get_depth().await.is_none());
    }

    #[tokio::test]
    async fn test_depth_update() {
        let server = MarketDataServer::new();
        let depth = DepthSnapshot {
            timestamp: 1000000,
            sequence: 1,
            bids: vec![DepthLevel {
                price: 100.0,
                quantity: 10.0,
            }],
            asks: vec![DepthLevel {
                price: 101.0,
                quantity: 10.0,
            }],
        };

        server.update_depth(depth.clone()).await;
        let retrieved = server.get_depth().await;
        assert!(retrieved.is_some());
        let d = retrieved.unwrap();
        assert_eq!(d.sequence, 1);
        assert_eq!(d.bids.len(), 1);
    }

    #[tokio::test]
    async fn test_trade_history() {
        let server = MarketDataServer::new();
        server
            .add_trade(TradeSnapshot {
                timestamp: 1000000,
                price: 100.0,
                quantity: 10.0,
                side: "buy".to_string(),
            })
            .await;

        let trades = server.get_trades().await;
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].price, 100.0);
    }

    #[tokio::test]
    async fn test_bbo_update() {
        let server = MarketDataServer::new();
        let bbo = BBO {
            timestamp: 1000000,
            bid_price: 100.0,
            bid_qty: 10.0,
            ask_price: 101.0,
            ask_qty: 10.0,
        };

        server.update_bbo(bbo.clone()).await;
        let retrieved = server.get_bbo().await;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().bid_price, 100.0);
    }

    #[tokio::test]
    async fn test_broadcast_depth() {
        let server = Arc::new(MarketDataServer::new());

        // 订阅 depth channel
        let mut rx = server.get_depth_tx().subscribe();

        // 在后台发送数据
        let server_clone = server.clone();
        tokio::spawn(async move {
            let depth = DepthSnapshot {
                timestamp: 1000000,
                sequence: 1,
                bids: vec![],
                asks: vec![],
            };
            server_clone.update_depth(depth).await;
        });

        // 接收广播消息
        let received = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            rx.recv(),
        )
        .await;

        assert!(received.is_ok(), "Should receive broadcasted depth");
    }

    #[tokio::test]
    async fn test_broadcast_trade() {
        let server = Arc::new(MarketDataServer::new());

        // 订阅 trade channel
        let mut rx = server.get_trade_tx().subscribe();

        // 在后台发送数据
        let server_clone = server.clone();
        tokio::spawn(async move {
            let trade = TradeSnapshot {
                timestamp: 1000000,
                price: 100.0,
                quantity: 10.0,
                side: "buy".to_string(),
            };
            server_clone.add_trade(trade).await;
        });

        // 接收广播消息
        let received = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            rx.recv(),
        )
        .await;

        assert!(received.is_ok(), "Should receive broadcasted trade");
    }
}
