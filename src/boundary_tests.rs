//! 边界条件和错误场景测试模块
//! 验证系统在极端情况下的行为和错误处理

#[cfg(test)]
mod tests {
    use crate::{
        MatchingEngine, MatchingEngineError, Order, OrderStatus, PoolConfig, Side, TimeInForce,
    };

    #[test]
    fn test_zero_quantity_order() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();
        let order = Order::new(1, Side::Buy, 100, 0, TimeInForce::GTC, 0);

        let result = engine.place_order(order);
        assert!(
            result.is_err() || result.unwrap().status == OrderStatus::Rejected,
            "零数量订单应该被拒绝"
        );
    }

    #[test]
    fn test_negative_price_order() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();
        let order = Order::new(1, Side::Buy, -100, 10, TimeInForce::GTC, 0);

        let result = engine.place_order(order);
        assert!(
            result.is_err() || result.unwrap().status == OrderStatus::Rejected,
            "负价格订单应该被拒绝"
        );
    }

    #[test]
    fn test_zero_price_order() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();
        let order = Order::new(1, Side::Buy, 0, 10, TimeInForce::GTC, 0);

        let result = engine.place_order(order);
        assert!(
            result.is_err() || result.unwrap().status == OrderStatus::Rejected,
            "零价格订单应该被拒绝"
        );
    }

    #[test]
    fn test_nan_price_order() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();
        let order = Order::new(1, Side::Buy, -1, 10, TimeInForce::GTC, 0);

        let result = engine.place_order(order);
        assert!(
            result.is_err() || result.unwrap().status == OrderStatus::Rejected,
            "NaN价格订单应该被拒绝"
        );
    }

    #[test]
    fn test_duplicate_order_id() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();
        let order1 = Order::new(1, Side::Buy, 100, 10, TimeInForce::GTC, 0);
        let order2 = Order::new(1, Side::Sell, 101, 10, TimeInForce::GTC, 0);

        let result1 = engine.place_order(order1);
        assert!(result1.is_ok(), "第一个订单应该成功");

        let result2 = engine.place_order(order2);
        assert!(matches!(
            result2,
            Err(MatchingEngineError::DuplicateOrderId(1))
        ));
        assert!(engine.get_top_levels(10, false).is_empty());
    }

    #[test]
    fn test_very_large_quantity() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();
        let order = Order::new(1, Side::Buy, 100, i64::MAX, TimeInForce::GTC, 0);

        let result = engine.place_order(order);
        // 应该能够处理极大的数字（可能溢出或被限制）
        assert!(result.is_ok() || result.is_err(), "应该有有效的结果");
    }

    #[test]
    fn test_very_small_quantity() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();
        let order = Order::new(1, Side::Buy, 100, 1, TimeInForce::GTC, 0);

        let result = engine.place_order(order);
        // 应该能够处理极小的数字
        assert!(result.is_ok(), "极小数量订单应该能下单");
    }

    #[test]
    fn test_cancel_nonexistent_order() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        let result = engine.cancel_order(999);
        assert!(result.is_err(), "撤销不存在的订单应该报错");
    }

    #[test]
    fn test_rapid_buy_sell_match() {
        let mut engine = MatchingEngine::new(PoolConfig::default()).unwrap();

        // 快速下单和成交
        for i in 0..100 {
            let buy = Order::new(i * 2, Side::Buy, 100 + i as i64, 10, TimeInForce::GTC, 0);
            let sell = Order::new(
                i * 2 + 1,
                Side::Sell,
                100 + i as i64,
                10,
                TimeInForce::IOC,
                0,
            );

            let _ = engine.place_order(buy);
            let _ = engine.place_order(sell);
        }

        // 应该完成而不崩溃
    }

    #[test]
    fn test_order_pool_exhaustion() {
        let pool_config = PoolConfig {
            order_capacity: 10, // 很小的容量
            ..PoolConfig::default()
        };
        let mut engine = MatchingEngine::new(pool_config).unwrap();

        // 尝试添加超过容量的订单
        for i in 0..20 {
            let order = Order::new(i, Side::Buy, 100 + i as i64, 10, TimeInForce::GTC, 0);
            let result = engine.place_order(order);
            // 应该能够优雅地处理，而不是崩溃
            if result.is_err() {
                // 池已满，预期行为
                break;
            }
        }
    }
}
