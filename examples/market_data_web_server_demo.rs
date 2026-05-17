/// 行情数据 Web 服务演示
/// 展示如何通过 HTTP API 查询行情数据
use matching_engine::{
    MatchingEngine, PoolConfig, Order, Side, TimeInForce, MarketDataServer,
    create_router,
    market_data_server_types::{DepthSnapshot, DepthLevel, TradeSnapshot, BBO},
};
use std::sync::Arc;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔═══════════════════════════════════════════════════════════════════╗");
    println!("║           行情数据 Web 服务演示 - HTTP API                        ║");
    println!("╚═══════════════════════════════════════════════════════════════════╝\n");

    // 初始化行情服务器
    let server = Arc::new(MarketDataServer::new());
    println!("✓ 行情服务器初始化");

    // 初始化撮合引擎
    let mut engine = MatchingEngine::new(PoolConfig::default())?;
    println!("✓ 撮合引擎初始化\n");

    // 【场景1】生成初始行情数据
    {
        println!("【场景1】生成初始行情数据");
        println!("{}", "━".repeat(64));

        // 下单生成订单簿
        println!("✓ 生成订单簿...");
        let mut best_bid = 0.0;
        let mut best_ask = 0.0;

        for i in 0..5 {
            let price = 100.0 - (i as f64) * 0.5;
            let order = Order::new(100 + i, Side::Buy, price, 100.0 + (i as f64) * 10.0, TimeInForce::GTC, 0);
            engine.place_order(order)?;
            if i == 0 {
                best_bid = price;
            }
        }

        for i in 0..5 {
            let price = 100.0 + (i as f64) * 0.5;
            let order = Order::new(200 + i, Side::Sell, price, 100.0 + (i as f64) * 10.0, TimeInForce::GTC, 0);
            engine.place_order(order)?;
            if i == 0 {
                best_ask = price;
            }
        }

        // 创建 BBO 数据
        let bbo = BBO {
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            bid_price: best_bid,
            bid_qty: 100.0,
            ask_price: best_ask,
            ask_qty: 100.0,
        };
        server.update_bbo(bbo.clone()).await;
        println!("✓ BBO: bid={:.1}, ask={:.1}", best_bid, best_ask);

        // 创建深度快照
        let mut bids = Vec::new();
        let mut asks = Vec::new();
        for i in 0..5 {
            let price = 100.0 - (i as f64) * 0.5;
            bids.push(DepthLevel {
                price,
                quantity: 100.0 + (i as f64) * 10.0,
            });
        }
        for i in 0..5 {
            let price = 100.0 + (i as f64) * 0.5;
            asks.push(DepthLevel {
                price,
                quantity: 100.0 + (i as f64) * 10.0,
            });
        }

        let depth = DepthSnapshot {
            timestamp: bbo.timestamp,
            sequence: 1,
            bids,
            asks,
        };
        server.update_depth(depth).await;
        println!("✓ 深度快照: {} bids, {} asks", 5, 5);

        println!("✓ 场景1完成\n");
    }

    // 【场景2】生成成交数据
    {
        println!("【场景2】生成成交数据");
        println!("{}", "━".repeat(64));

        // 进行成交
        println!("✓ 执行成交序列...");
        let buy_order = Order::new(1000, Side::Buy, 100.0, 50.0, TimeInForce::GTC, 0);
        engine.place_order(buy_order)?;

        let sell_order = Order::new(2000, Side::Sell, 100.0, 50.0, TimeInForce::GTC, 0);
        engine.place_order(sell_order)?;

        // 添加成交记录到服务器
        for i in 0..3 {
            let trade = TradeSnapshot {
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64 + (i as u64) * 100,
                price: 100.0 - (i as f64) * 0.1,
                quantity: 50.0 - (i as f64) * 5.0,
                side: if i % 2 == 0 { "buy".to_string() } else { "sell".to_string() },
            };
            server.add_trade(trade.clone()).await;
            println!("✓ Trade #{}: price={}, qty={}, side={}", i + 1, trade.price, trade.quantity, trade.side);
        }

        println!("✓ 场景2完成\n");
    }

    // 【场景3】启动 Web 服务器
    {
        println!("【场景3】启动 Web 服务器");
        println!("{}", "━".repeat(64));

        let server_clone = server.clone();
        let addr = "127.0.0.1:8080";

        // 在后台启动服务器
        tokio::spawn(async move {
            let app = create_router(server_clone);
            let listener = TcpListener::bind(addr).await.expect("Failed to bind");
            println!("✓ Web 服务器启动: http://{}", addr);
            println!("  GET  http://{}/health", addr);
            println!("  GET  http://{}/api/market/bbo", addr);
            println!("  GET  http://{}/api/market/depth", addr);
            println!("  GET  http://{}/api/market/trades", addr);
            println!("  GET  http://{}/api/market/stats", addr);

            axum::serve(listener, app).await.expect("Server error");
        });

        println!("✓ 服务器已在后台启动");
        println!("✓ 可通过 curl 或 HTTP 客户端查询：\n");

        // 等待服务器启动
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // 模拟 HTTP 查询
        println!("【模拟客户端查询】");

        // 查询 BBO
        let bbo = server.get_bbo().await;
        if let Some(b) = bbo {
            println!("✓ BBO 查询成功:");
            println!("  bid: {:.1} × {:.1}", b.bid_price, b.bid_qty);
            println!("  ask: {:.1} × {:.1}", b.ask_price, b.ask_qty);
        }

        // 查询深度
        let depth = server.get_depth().await;
        if let Some(d) = depth {
            println!("✓ 深度查询成功:");
            println!("  买方档位: {}", d.bids.len());
            println!("  卖方档位: {}", d.asks.len());
            if !d.bids.is_empty() {
                let (price, qty) = (&d.bids[0].price, &d.bids[0].quantity);
                println!("  最优买价: {} × {}", price, qty);
            }
        }

        // 查询交易
        let trades = server.get_trades().await;
        println!("✓ 交易查询成功: {} 条记录", trades.len());
        for (i, t) in trades.iter().enumerate() {
            println!("  #{}: {}: {} × {}", i + 1, t.side, t.price, t.quantity);
        }

        println!("\n✓ 场景3完成");
    }

    println!("\n╔═══════════════════════════════════════════════════════════════════╗");
    println!("║                  ✓ Web 服务演示完成！                            ║");
    println!("║                 服务运行在后台，按 Enter 退出                    ║");
    println!("╚═══════════════════════════════════════════════════════════════════╝");

    // 保持服务运行
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;

    Ok(())
}
