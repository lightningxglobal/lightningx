/// 行情数据 Web 服务：HTTP API
///
/// 功能：
/// - HTTP 查询接口（深度、交易、统计、BBO）
/// - 多客户端连接支持

use axum::{
    extract::State,
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::RwLock;

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

/// 服务器状态
pub struct MarketDataServer {
    // 最新快照（供 HTTP 查询）
    pub latest_depth: Arc<RwLock<Option<DepthSnapshot>>>,
    pub latest_trades: Arc<RwLock<Vec<TradeSnapshot>>>,
    pub latest_stats: Arc<RwLock<Option<Statistics>>>,
    pub latest_bbo: Arc<RwLock<Option<BBO>>>,
}

impl MarketDataServer {
    pub fn new() -> Self {
        Self {
            latest_depth: Arc::new(RwLock::new(None)),
            latest_trades: Arc::new(RwLock::new(Vec::new())),
            latest_stats: Arc::new(RwLock::new(None)),
            latest_bbo: Arc::new(RwLock::new(None)),
        }
    }

    /// 更新深度快照
    pub async fn update_depth(&self, depth: DepthSnapshot) {
        *self.latest_depth.write().await = Some(depth);
    }

    /// 更新交易记录
    pub async fn add_trade(&self, trade: TradeSnapshot) {
        let mut trades = self.latest_trades.write().await;
        trades.push(trade);
        // 保持最近100条交易
        if trades.len() > 100 {
            trades.remove(0);
        }
    }

    /// 更新统计数据
    pub async fn update_stats(&self, stats: Statistics) {
        *self.latest_stats.write().await = Some(stats);
    }

    /// 更新 BBO
    pub async fn update_bbo(&self, bbo: BBO) {
        *self.latest_bbo.write().await = Some(bbo);
    }

    /// 获取深度快照
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
}

impl Default for MarketDataServer {
    fn default() -> Self {
        Self::new()
    }
}

/// HTTP 路由处理函数

async fn get_health() -> Json<serde_json::Value> {
    Json(json!({"status": "ok", "service": "market-data-server"}))
}

async fn get_bbo(State(server): State<Arc<MarketDataServer>>) -> Json<serde_json::Value> {
    match server.get_bbo().await {
        Some(bbo) => Json(json!({"status": "ok", "data": bbo})),
        None => Json(json!({"status": "error", "message": "No BBO data available"})),
    }
}

async fn get_depth(State(server): State<Arc<MarketDataServer>>) -> Json<serde_json::Value> {
    match server.get_depth().await {
        Some(depth) => Json(json!({"status": "ok", "data": depth})),
        None => Json(json!({"status": "error", "message": "No depth data available"})),
    }
}

async fn get_trades(State(server): State<Arc<MarketDataServer>>) -> Json<serde_json::Value> {
    let trades = server.get_trades().await;
    Json(json!({"status": "ok", "data": trades}))
}

async fn get_stats(State(server): State<Arc<MarketDataServer>>) -> Json<serde_json::Value> {
    match server.get_stats().await {
        Some(stats) => Json(json!({"status": "ok", "data": stats})),
        None => Json(json!({"status": "error", "message": "No statistics available"})),
    }
}

/// 创建 HTTP 路由
pub fn create_router(server: Arc<MarketDataServer>) -> Router {
    Router::new()
        .route("/health", get(get_health))
        .route("/api/market/bbo", get(get_bbo))
        .route("/api/market/depth", get(get_depth))
        .route("/api/market/trades", get(get_trades))
        .route("/api/market/stats", get(get_stats))
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
    println!("  HTTP: http://{}/api/market/*", addr);
    println!("  WS:   ws://{}/ws/market", addr);

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
}
