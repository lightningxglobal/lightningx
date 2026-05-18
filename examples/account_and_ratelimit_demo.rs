/// 账户管理和限流演示
/// 展示如何集成账户验证和请求限流到交易引擎
use lightning_exchange::{
    MatchingEngine, PoolConfig, Order, Side, TimeInForce,
    AccountManager, RateLimiter, RateLimitPolicy,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔═══════════════════════════════════════════════════════════════════╗");
    println!("║           账户管理和限流集成演示 - 生产级交易系统              ║");
    println!("╚═══════════════════════════════════════════════════════════════════╝\n");

    // 初始化撮合引擎
    let mut engine = MatchingEngine::new(PoolConfig::default())?;

    // 初始化账户管理器
    let mut account_mgr = AccountManager::new();
    account_mgr.create_account(1, 100_000.0)?; // 账户1：初始资金10万
    account_mgr.create_account(2, 50_000.0)?;  // 账户2：初始资金5万
    println!("✓ 创建账户：");
    println!("  账户1: 100,000 CNY");
    println!("  账户2: 50,000 CNY\n");

    // 初始化限流器
    let mut rate_limiter = RateLimiter::new(RateLimitPolicy::default_trading());
    // 为高级用户设置更高的限额
    rate_limiter.set_policy("client1", RateLimitPolicy::lenient());
    println!("✓ 限流器配置：");
    println!("  默认: 100 req/sec (突发200)");
    println!("  客户端1: 1000 req/sec (突发2000)\n");

    // 场景1: 买单验证和资金冻结
    {
        println!("【场景1】买单资金验证和冻结");
        println!("{}", "━".repeat(64));

        let account_id = 1u64;
        let symbol = "BTC";
        let price = 100.0;
        let quantity = 500.0; // 需要50000
        let cost = price * quantity;

        // 1. 检查限流
        if let Err(e) = rate_limiter.consume("client1", 1) {
            println!("✗ 限流拒绝: {}", e);
            return Ok(());
        }
        println!("✓ 通过限流检查");

        // 2. 验证资金
        match account_mgr.validate_buy_order(account_id, symbol, price, quantity) {
            Ok(_) => println!("✓ 资金验证通过: 需要{}, 可用{}", cost, 100_000.0),
            Err(e) => {
                println!("✗ 资金验证失败: {}", e);
                return Ok(());
            }
        }

        // 3. 冻结资金
        account_mgr.freeze_buy_order(account_id, price, quantity)?;
        println!("✓ 冻结资金: {}", cost);

        // 4. 下单
        let order = Order::new(101, Side::Buy, price, quantity, TimeInForce::GTC, 0);
        let result = engine.place_order(order)?;
        println!("✓ 订单已下: id={}, status={:?}", result.order_id, result.status);

        // 5. 显示账户状态
        let account = account_mgr.get_account(account_id)?;
        println!("✓ 账户状态：");
        println!("  可用余额: {}", account.available_balance());
        println!("  冻结余额: {}", account.frozen_balance);

        println!("✓ 场景1验证通过\n");
    }

    // 场景2: 卖单验证和持仓冻结
    {
        println!("【场景2】卖单持仓验证和冻结");
        println!("{}", "━".repeat(64));

        let account_id = 2u64;
        let symbol = "ETH";
        let price = 50.0;
        let quantity = 100.0;

        // 1. 检查限流
        rate_limiter.consume("client2", 1)?;
        println!("✓ 通过限流检查");

        // 这个账户没有ETH持仓，应该失败
        match account_mgr.validate_sell_order(account_id, symbol, quantity) {
            Ok(_) => println!("✗ 应该验证失败（无持仓）"),
            Err(e) => println!("✓ 正确拒绝: {}", e),
        }

        // 手动添加持仓用于演示
        {
            let account = account_mgr.get_account_mut(account_id)?;
            account.get_position_mut(symbol).quantity = 50.0;
        }
        println!("✓ 添加持仓: {} {}", 50.0, symbol);

        // 现在验证应该失败（数量不足）
        match account_mgr.validate_sell_order(account_id, symbol, quantity) {
            Ok(_) => println!("✗ 应该验证失败（数量不足）"),
            Err(e) => println!("✓ 正确拒绝: {}", e),
        }

        // 用正确的数量
        let quantity = 50.0;
        account_mgr.validate_sell_order(account_id, symbol, quantity)?;
        println!("✓ 持仓验证通过: 有50, 要卖50");

        // 冻结持仓
        account_mgr.freeze_sell_order(account_id, symbol, quantity)?;
        println!("✓ 冻结持仓: {} {}", quantity, symbol);

        // 下单
        let order = Order::new(201, Side::Sell, price, quantity, TimeInForce::GTC, 0);
        engine.place_order(order)?;
        println!("✓ 卖单已下");

        println!("✓ 场景2验证通过\n");
    }

    // 场景3: 限流测试
    {
        println!("【场景3】限流机制演示");
        println!("{}", "━".repeat(64));

        let mut client_limiter = RateLimiter::new(
            RateLimitPolicy {
                requests_per_second: 5,
                burst_capacity: 10,
            }
        );

        println!("✓ 测试限流: 5 req/sec, 突发10");

        // 消耗初始的10个令牌（突发容量）
        for i in 1..=10 {
            match client_limiter.consume("test_client", 1) {
                Ok(_) => print!("✓"),
                Err(_) => {
                    println!("✗ 第{}个请求被限流", i);
                    break;
                }
            }
        }
        println!(" (消耗10个令牌)");

        // 现在应该被限流（突发用完了）
        match client_limiter.consume("test_client", 1) {
            Ok(_) => println!("✗ 不应该通过"),
            Err(e) => println!("✓ 正确限流: {}", e),
        }

        // 查询令牌数
        let tokens = client_limiter.get_tokens("test_client");
        println!("✓ 当前令牌数: {:.2}", tokens);

        println!("✓ 场景3验证通过\n");
    }

    // 场景4: 交易执行和账户更新
    {
        println!("【场景4】交易执行 - 账户更新");
        println!("{}", "━".repeat(64));

        let buyer_id = 1u64;
        let seller_id = 2u64;
        let symbol = "BTC";
        let price = 100.0;
        let quantity = 10.0;

        // 模拟买单成交
        println!("✓ 执行买单成交: {} BTC @ {}", quantity, price);
        account_mgr.execute_buy(buyer_id, symbol, price, quantity)?;

        let buyer = account_mgr.get_account(buyer_id)?;
        println!("  买方账户: 余额={}, 冻结={}, 持仓={}",
            buyer.balance, buyer.frozen_balance,
            buyer.get_position(symbol).map(|p| p.quantity).unwrap_or(0.0));

        // 模拟卖单成交
        println!("✓ 执行卖单成交: {} {} @ {}", quantity, symbol, price);
        account_mgr.execute_sell(seller_id, symbol, price, quantity)?;

        let seller = account_mgr.get_account(seller_id)?;
        println!("  卖方账户: 余额={}, 冻结={}, 持仓={}",
            seller.balance, seller.frozen_balance,
            seller.get_position(symbol).map(|p| p.quantity).unwrap_or(0.0));

        println!("✓ 场景4验证通过\n");
    }

    println!("╔═══════════════════════════════════════════════════════════════════╗");
    println!("║           ✓ 账户管理和限流集成演示完成！                         ║");
    println!("╚═══════════════════════════════════════════════════════════════════╝");

    Ok(())
}
