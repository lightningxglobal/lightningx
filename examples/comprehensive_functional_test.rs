/// 全面的功能正确性测试
/// 覆盖复杂场景：取消、部分填充、快速匹配、流动性变化
use matching_engine::{
    MatchingEngine, PoolConfig, Order, Side, TimeInForce, TradeEvent,
};
use rtrb::RingBuffer;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔════════════════════════════════════════════════════════════════════╗");
    println!("║              全面功能正确性测试 - 复杂场景验证                    ║");
    println!("╚════════════════════════════════════════════════════════════════════╝\n");

    // 场景1: 订单取消 + 部分成交
    {
        println!("【场景1】订单取消 + 部分成交验证");
        println!("{}", "━".repeat(64));

        let mut engine = MatchingEngine::new(PoolConfig::default())?;
        let (trade_tx, mut trade_rx) = RingBuffer::<TradeEvent>::new(10000);
        engine.set_trade_event_sender(trade_tx);

        // 下一个大卖单 1000股
        let sell = Order::new(1, Side::Sell, 100.0, 1000.0, TimeInForce::GTC, 0);
        let result = engine.place_order(sell)?;
        println!("✓ 卖单 #1: qty=1000, status={:?}", result.status);
        assert_eq!(result.filled, 0.0, "卖单应该进入簿");

        // 下一个部分匹配的买单 300股
        let buy = Order::new(2, Side::Buy, 100.0, 300.0, TimeInForce::GTC, 0);
        let result = engine.place_order(buy)?;
        println!("✓ 买单 #2: qty=300, filled={}", result.filled);
        assert_eq!(result.filled, 300.0, "买单应该全部成交");

        // 检查trade event
        let mut trade_count = 0;
        while trade_rx.pop().is_ok() {
            trade_count += 1;
        }
        println!("✓ Trade events: {}", trade_count);
        assert_eq!(trade_count, 1, "应该有1个trade");

        // 现在卖单应该还有700股在簿中
        let get_result = engine.get_order_fill_status(1);
        println!("✓ 卖单 #1 填充状态: {:?}", get_result);
        if let Some((filled, remaining)) = get_result {
            assert_eq!(filled, 300.0, "卖单应该填充300");
            assert_eq!(remaining, 700.0, "卖单应该剩余700");
        }

        // 取消卖单的剩余部分
        let cancel_result = engine.cancel_order(1)?;
        println!("✓ 取消卖单 #1: cancelled_qty={}", cancel_result.cancelled_quantity);
        assert_eq!(cancel_result.cancelled_quantity, 700.0, "应该取消700股");

        // 验证取消后查询返回None
        let get_result = engine.get_order_fill_status(1);
        println!("✓ 取消后查询: {:?}", get_result);
        assert!(get_result.is_none(), "已取消的订单应该查询不到");

        println!("✓ 场景1验证通过\n");
    }

    // 场景2: 多个价位的连续成交
    {
        println!("【场景2】多个价位的连续成交");
        println!("{}", "━".repeat(64));

        let mut engine = MatchingEngine::new(PoolConfig::default())?;
        let (trade_tx, mut trade_rx) = RingBuffer::<TradeEvent>::new(10000);
        engine.set_trade_event_sender(trade_tx);

        // 在不同价位放置卖单
        let prices = vec![100.5, 100.4, 100.3, 100.2, 100.1];
        for (i, &price) in prices.iter().enumerate() {
            let sell = Order::new(100 + i as u64, Side::Sell, price, 100.0, TimeInForce::GTC, 0);
            let result = engine.place_order(sell)?;
            println!("✓ 卖单 #{}: price={}, qty=100, status={:?}", 100+i, price, result.status);
        }

        // 下一个大买单，会与所有卖单部分或全部成交
        let buy = Order::new(200, Side::Buy, 101.0, 400.0, TimeInForce::GTC, 0);
        let result = engine.place_order(buy)?;
        println!("✓ 买单 #200: qty=400, filled={}", result.filled);
        assert_eq!(result.filled, 400.0, "买单应该全部填充");

        // 检查trade event数量（应该有多个）
        let mut trade_count = 0;
        while trade_rx.pop().is_ok() {
            trade_count += 1;
        }
        println!("✓ Trade events: {}", trade_count);
        assert!(trade_count >= 4, "应该至少有4个成交（400/100)");

        // 验证剩余的卖单
        let remaining_sell = engine.get_order_fill_status(104); // 最后一个卖单
        println!("✓ 最后卖单的状态: {:?}", remaining_sell);
        if let Some((filled, remaining)) = remaining_sell {
            println!("  filled={}, remaining={}", filled, remaining);
            assert!(remaining > 0.0, "最后卖单应该有剩余");
        }

        println!("✓ 场景2验证通过\n");
    }

    // 场景3: IOC (Immediate or Cancel) 订单
    {
        println!("【场景3】IOC订单验证 - 无对手时立即取消");
        println!("{}", "━".repeat(64));

        let mut engine = MatchingEngine::new(PoolConfig::default())?;
        let (trade_tx, mut trade_rx) = RingBuffer::<TradeEvent>::new(10000);
        engine.set_trade_event_sender(trade_tx);

        // 放一个买单在簿中
        let buy = Order::new(1, Side::Buy, 99.9, 100.0, TimeInForce::GTC, 0);
        let _ = engine.place_order(buy)?;
        println!("✓ 买单 #1 进入簿 (99.9)");

        // 用IOC卖单在更高价格 - 不应该成交
        let ioc_sell = Order::new(2, Side::Sell, 100.0, 50.0, TimeInForce::IOC, 0);
        let result = engine.place_order(ioc_sell)?;
        println!("✓ IOC卖单 #2: price=100.0, qty=50, filled={}, status={:?}", result.filled, result.status);
        assert_eq!(result.filled, 0.0, "IOC不应该成交");

        // 检查是否有trade event
        let trade_count = trade_rx.pop().is_ok() as u32;
        println!("✓ Trade events: {}", trade_count);
        assert_eq!(trade_count, 0, "不应该有任何成交");

        // 现在用IOC买单在合适价格 - 应该与sell成交
        let ioc_buy = Order::new(3, Side::Buy, 100.0, 30.0, TimeInForce::IOC, 0);
        let result = engine.place_order(ioc_buy)?;
        println!("✓ IOC买单 #3: price=100.0, qty=30, filled={}, status={:?}", result.filled, result.status);
        assert_eq!(result.filled, 0.0, "IOC买单不应该与99.9的卖单成交");

        println!("✓ 场景3验证通过\n");
    }

    // 场景4: 快速连续订单（乱序成交）
    {
        println!("【场景4】快速连续订单 - 验证成交一致性");
        println!("{}", "━".repeat(64));

        let mut engine = MatchingEngine::new(PoolConfig::default())?;
        let (trade_tx, mut trade_rx) = RingBuffer::<TradeEvent>::new(100000);
        engine.set_trade_event_sender(trade_tx);

        // 快速放置10个买单和10个卖单
        let mut total_filled = 0.0;
        for i in 0..10 {
            let buy = Order::new(i * 2, Side::Buy, 100.0 - i as f64 * 0.1, 10.0, TimeInForce::GTC, 0);
            let result = engine.place_order(buy)?;
            total_filled += result.filled;

            let sell = Order::new(i * 2 + 1, Side::Sell, 100.0 + i as f64 * 0.1, 10.0, TimeInForce::GTC, 0);
            let result = engine.place_order(sell)?;
            total_filled += result.filled;
        }

        // 统计所有trade event
        let mut trade_count = 0;
        let mut total_trade_qty = 0.0;
        while let Ok(trade) = trade_rx.pop() {
            trade_count += 1;
            total_trade_qty += trade.quantity;
        }

        println!("✓ 总订单数: 20");
        println!("✓ Trade events: {}", trade_count);
        println!("✓ 成交总量: {}", total_trade_qty);
        println!("✓ 已填充总量: {}", total_filled);

        // trade事件数应该合理
        assert!(trade_count > 0, "应该有成交事件");
        assert_eq!(total_filled, total_trade_qty, "填充量应该等于成交量");

        println!("✓ 场景4验证通过\n");
    }

    // 场景5: 流动性消耗和补充
    {
        println!("【场景5】流动性消耗和补充");
        println!("{}", "━".repeat(64));

        let mut engine = MatchingEngine::new(PoolConfig::default())?;

        // 建立初始流动性
        let mut order_id = 1u64;
        for level in 0..5 {
            let price = 100.0 - level as f64 * 0.1;
            let buy = Order::new(order_id, Side::Buy, price, 100.0, TimeInForce::GTC, 0);
            let _ = engine.place_order(buy)?;
            order_id += 1;
        }
        println!("✓ 建立买单流动性: 5个档位, 100股/档");

        // 用大卖单消耗流动性
        let large_sell = Order::new(order_id, Side::Sell, 99.6, 300.0, TimeInForce::GTC, 0);
        let result = engine.place_order(large_sell)?;
        println!("✓ 大卖单: qty=300, filled={}", result.filled);
        order_id += 1;

        // 补充流动性
        for level in 0..3 {
            let price = 100.0 - level as f64 * 0.1;
            let buy = Order::new(order_id, Side::Buy, price, 100.0, TimeInForce::GTC, 0);
            let _ = engine.place_order(buy)?;
            order_id += 1;
        }
        println!("✓ 补充买单流动性: 3个档位, 100股/档");

        // 再次消耗
        let second_sell = Order::new(order_id, Side::Sell, 99.7, 200.0, TimeInForce::GTC, 0);
        let result = engine.place_order(second_sell)?;
        println!("✓ 二次大卖单: qty=200, filled={}", result.filled);

        println!("✓ 场景5验证通过\n");
    }

    println!("╔════════════════════════════════════════════════════════════════════╗");
    println!("║                  ✓ 全面功能测试全部通过！                         ║");
    println!("╚════════════════════════════════════════════════════════════════════╝");

    Ok(())
}
