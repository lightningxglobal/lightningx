/// Desk Server 功能演示
/// 演示柜台服务器的核心功能：
/// 1. 会话管理
/// 2. 委托处理（买卖、取消）
/// 3. Rate Limit 检查
/// 4. 行情推送

use matching_engine::{DeskConfig, DeskServer, desk_server::{OrderRequest, CancelRequest}};
use std::sync::Arc;

#[tokio::main]
async fn main() {
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║            Desk Server 功能演示                           ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    // 初始化 Desk Server
    let config = DeskConfig {
        desk_id: 1,
        addr: "127.0.0.1:3000".to_string(),
        rate_limit_policy: matching_engine::RateLimitPolicy::default_trading(),
    };

    let desk = Arc::new(DeskServer::new(config));
    println!("✓ Desk Server 初始化完成 (desk_id: 1, addr: 127.0.0.1:3000)\n");

    // 场景 1: 会话管理
    println!("【场景 1】会话管理");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    let session_id = desk.create_session("client_alice".to_string(), 1).await;
    println!("✓ 创建会话: client_alice, session_id: {:?}", session_id);

    let session = desk.get_session(session_id).await;
    match session {
        Some(s) => println!("✓ 获取会话: client_id={}, account_id={}", s.client_id, s.account_id),
        None => println!("✗ 会话不存在"),
    }
    println!();

    // 场景 2: 委托处理 - 买单
    println!("【场景 2】委托处理 - 买单");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    let buy_order = OrderRequest {
        symbol: "BTC".to_string(),
        side: "buy".to_string(),
        price: 50000.0,
        quantity: 1.0,
    };

    let response = desk.handle_order_ws(session_id, buy_order.clone()).await;
    println!("✓ 提交买单: BTC 1.0 @ 50000.0");
    println!("✓ 回报: {:?}\n", response);

    // 场景 3: Rate Limit 检查
    println!("【场景 3】Rate Limit 检查");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    let mut success_count = 0;
    for i in 0..3 {
        let response = desk.handle_order_ws(session_id, buy_order.clone()).await;
        match response {
            matching_engine::desk_server::ServerMessage::OrderAccepted { .. } => {
                success_count += 1;
                println!("  ✓ 委托 {}: 被接受", i + 1);
            }
            matching_engine::desk_server::ServerMessage::Error { message } => {
                println!("  ✗ 委托 {}: 被拒绝 - {}", i + 1, message);
            }
            _ => {}
        }
    }
    println!("✓ 成功: {}/3\n", success_count);

    // 场景 4: 行情推送
    println!("【场景 4】行情推送");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // 启动行情推送线程
    let desk_clone = desk.clone();
    tokio::spawn(async move {
        for i in 0..3 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            let depth_data = format!(
                r#"{{"bid":{},"ask":{}}}"#,
                50000.0 - i as f64,
                50000.0 + i as f64 + 1.0
            );
            desk_clone
                .broadcast_market_data(matching_engine::desk_server::MarketDataUpdate::Depth {
                    symbol: "BTC".to_string(),
                    data: depth_data,
                })
                .await;
        }
    });

    // 订阅行情
    let mut rx = desk.subscribe_market_data();
    tokio::spawn(async move {
        for _ in 0..3 {
            if let Ok(update) = tokio::time::timeout(
                std::time::Duration::from_secs(1),
                rx.recv(),
            )
            .await
            {
                match update {
                    Ok(matching_engine::desk_server::MarketDataUpdate::Depth { symbol, data }) => {
                        println!("  ✓ 行情推送: {} depth={}", symbol, data);
                    }
                    _ => {}
                }
            }
        }
    });

    // 等待行情推送完成
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    println!();

    // 场景 5: 取消委托
    println!("【场景 5】取消委托");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    let cancel_req = CancelRequest { order_id: 12345 };
    let response = desk.handle_cancel_ws(session_id, cancel_req).await;
    println!("✓ 取消委托: order_id=12345");
    println!("✓ 回报: {:?}\n", response);

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║                  ✓ 所有演示完成！                         ║");
    println!("╚════════════════════════════════════════════════════════════╝");
}
