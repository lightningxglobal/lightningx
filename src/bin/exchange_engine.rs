use lightning_exchange::{
    account_repository::AccountRepository,
    aeron_channels::{
        AERON_DIR,
        ORDERS_CHANNEL, ORDERS_STREAM,
        ORDER_UPDATE_CHANNEL, ORDER_UPDATE_STREAM,
        TRADE_CHANNEL, TRADE_STREAM,
        DEPTH_CHANNEL, DEPTH_STREAM, DEPTH50_STREAM, LEVEL2_STREAM,
    },
    aeron_transport::{AeronOrderSubscriber, AeronOrderUpdatePublisher, AeronTradePublisher, AeronMarketDataPublisher},
    db,
    engine::{MatchingEngine, PoolConfig},
    models::DbOrder,
    order::{Order, Side, TimeInForce},
    trading_engine::{TradingConfig, TradingEngine},
};
use aeron_wrapper::AeronClient;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://user:password@localhost:5432/mydb".to_string());

    let symbols: Vec<String> = std::env::var("SYMBOLS")
        .unwrap_or_else(|_| "ETH_USDT,BTC_USDT,SOL_USDT".to_string())
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();

    tracing::info!("Connecting to database...");
    let pool = db::create_pool(&database_url).await?;
    tracing::info!("DB connected");

    // Cancel stale bot orders (market-maker + demo) — they restart fresh each run.
    {
        let repo = AccountRepository::new(&pool);
        let bot_stale: Vec<(i64, i64, String, String, f64, f64, f64)> = sqlx::query_as(
            "SELECT o.id, o.user_id, o.symbol, o.side, o.quantity, o.filled, COALESCE(o.freeze_price, COALESCE(o.price, 0.0))
             FROM orders o
             JOIN users u ON u.id = o.user_id
             WHERE o.status IN ('PENDING','TRADING')
               AND u.email IN ('robot@lightningx.exchange', 'demo@lightning.exchange')",
        )
        .fetch_all(&pool)
        .await
        .unwrap_or_default();

        for (id, user_id, symbol, side, quantity, filled, per_unit_price) in &bot_stale {
            let remaining = quantity - filled;
            if remaining > 0.0 {
                let sym_parts: Vec<&str> = symbol.splitn(2, '_').collect();
                let base_asset = sym_parts.first().copied().unwrap_or("BTC");
                let quote_asset = sym_parts.last().copied().unwrap_or("USDT");
                if side == "sell" {
                    let _ = repo.release_frozen(*user_id, base_asset, remaining).await;
                } else if *per_unit_price > 0.0 {
                    let _ = repo.release_frozen(*user_id, quote_asset, per_unit_price * remaining).await;
                }
            }
            let _ = sqlx::query("UPDATE orders SET status='CANCELED', updated_at=NOW() WHERE id=$1")
                .bind(id)
                .execute(&pool)
                .await;
        }
        if !bot_stale.is_empty() {
            tracing::warn!("Canceled {} stale bot orders from previous session", bot_stale.len());
        }
    }

    // Restore active limit orders from DB into a single MatchingEngine per symbol.
    // For this single-engine design, all symbols share one engine; the desk_server
    // already routes by symbol at the DB/WS layer, so the matching engine itself
    // is symbol-agnostic (it holds price levels, not symbols).
    // NOTE: multi-symbol routing via Aeron is handled at the desk layer; for now
    // we start one engine instance and accept all orders on a single stream.
    let mut engines: std::collections::HashMap<String, MatchingEngine> = symbols
        .iter()
        .map(|s| {
            let eng = MatchingEngine::new(PoolConfig::default()).expect("failed to create engine");
            (s.clone(), eng)
        })
        .collect();

    let rows = sqlx::query_as::<_, DbOrder>(
        "SELECT * FROM orders WHERE status IN ('PENDING', 'TRADING') ORDER BY id ASC",
    )
    .fetch_all(&pool)
    .await
    .unwrap_or_default();

    let mut max_id: u64 = 0;
    let mut restored = 0usize;
    let mut skipped = 0usize;

    for db_order in &rows {
        let order_id = db_order.id as u64;
        if order_id > max_id {
            max_id = order_id;
        }

        if db_order.order_type == "market"
            || (db_order.order_type == "ioc" && db_order.price.is_none())
        {
            skipped += 1;
            continue;
        }

        let remaining = db_order.quantity - db_order.filled;
        if remaining <= 0.0 {
            continue;
        }

        let side = if db_order.side == "buy" { Side::Buy } else { Side::Sell };
        let order = Order::new(
            order_id,
            side,
            db_order.price.unwrap_or(0.0),
            remaining,
            TimeInForce::GTC,
            0,
        );

        if let Some(eng) = engines.get_mut(&db_order.symbol) {
            if eng.add_to_book(order).is_ok() {
                restored += 1;
            }
        }
    }
    tracing::info!(
        "Restored {} active orders from DB ({} non-restable skipped)",
        restored,
        skipped,
    );

    // Cancel stale PENDING/TRADING market orders that survived a crash.
    {
        let repo = AccountRepository::new(&pool);
        let stale: Vec<(i64, i64, String, String, f64, f64, Option<f64>, f64)> = sqlx::query_as(
            "SELECT id, user_id, symbol, side, quantity, filled, price, freeze_price
             FROM orders
             WHERE status IN ('PENDING','TRADING') AND order_type='market'",
        )
        .fetch_all(&pool)
        .await
        .unwrap_or_default();

        let mut cleaned = 0usize;
        for (id, user_id, symbol, side, quantity, filled, row_price, freeze_price) in stale {
            let remaining = quantity - filled;
            if remaining > 0.0 {
                let sym_parts: Vec<&str> = symbol.splitn(2, '_').collect();
                let base_asset = sym_parts.first().copied().unwrap_or("BTC");
                let quote_asset = sym_parts.last().copied().unwrap_or("USDT");
                if side == "sell" {
                    let _ = repo.release_frozen(user_id, base_asset, remaining).await;
                } else {
                    let resolved_price = if freeze_price > 0.0 {
                        Some(freeze_price)
                    } else {
                        row_price.filter(|p| *p > 0.0)
                    };
                    if let Some(p) = resolved_price {
                        let _ = repo.release_frozen(user_id, quote_asset, p * remaining).await;
                    }
                }
            }
            let _ = sqlx::query(
                "UPDATE orders SET status='CANCELED', updated_at=NOW() WHERE id=$1",
            )
            .bind(id)
            .execute(&pool)
            .await;
            cleaned += 1;
        }
        if cleaned > 0 {
            tracing::warn!("Cleaned up {} stale market orders from prior runs", cleaned);
        }
    }

    // DB access ends here. Engines are moved into TradingEngine below.
    drop(pool);

    // For now: start one TradingEngine per symbol, each on its own Aeron stream.
    // The desk_server must route by symbol; this is a single-symbol MVP binding.
    // Multi-symbol multiplexing can be added later without changing the engine.
    //
    // We use the first symbol's engine as the primary for the Aeron subscription.
    // TODO: per-symbol Aeron streams when desk_server supports symbol routing.
    let primary_symbol = symbols.first().map(|s| s.as_str()).unwrap_or("ETH_USDT");
    let primary_engine = engines
        .remove(primary_symbol)
        .unwrap_or_else(|| MatchingEngine::new(PoolConfig::default()).expect("engine"));

    let client = Arc::new(
        AeronClient::new(AERON_DIR).map_err(|e| anyhow::anyhow!("Aeron init failed: {:?}", e))?,
    );

    let subscriber = Box::new(
        AeronOrderSubscriber::new(client.clone(), ORDERS_CHANNEL, ORDERS_STREAM)
            .map_err(|e| anyhow::anyhow!("subscriber: {}", e))?,
    );
    let ou_pub = Box::new(
        AeronOrderUpdatePublisher::new(client.clone(), ORDER_UPDATE_CHANNEL, ORDER_UPDATE_STREAM)
            .map_err(|e| anyhow::anyhow!("ou_pub: {}", e))?,
    );
    let trade_pub = Box::new(
        AeronTradePublisher::new(client.clone(), TRADE_CHANNEL, TRADE_STREAM)
            .map_err(|e| anyhow::anyhow!("trade_pub: {}", e))?,
    );
    let md_pub = Box::new(
        AeronMarketDataPublisher::new(
            client.clone(),
            DEPTH_CHANNEL,
            DEPTH_STREAM,
            DEPTH50_STREAM,
            LEVEL2_STREAM,
        )
        .map_err(|e| anyhow::anyhow!("md_pub: {}", e))?,
    );

    let engine = TradingEngine::with_engine(primary_engine, TradingConfig::default());
    let (matching_thread, publishing_thread) = engine.run(subscriber, ou_pub, trade_pub, md_pub);

    tracing::info!("Exchange engine started (symbol={})", primary_symbol);

    tokio::signal::ctrl_c().await?;
    tracing::info!("Shutting down exchange engine...");

    matching_thread.thread().unpark();
    publishing_thread.thread().unpark();

    Ok(())
}
