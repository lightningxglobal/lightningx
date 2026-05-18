/// Test order lifecycle and OrderUpdateEvent generation
///
/// Tests:
/// 1. IOC order with no match → REJECTED
/// 2. IOC order with match → FILLED
/// 3. GTC order entering book → ACCEPTED
/// 4. GTC order partial fill → PARTIALLY_FILLED
/// 5. GTC order full fill → FILLED
/// 6. GTC order cancel → CANCELLED

use lightning_exchange::{
    engine::{MatchingEngine, PoolConfig, OrderStatus},
    order::{Order, Side, TimeInForce},
    orderbook_impl::OrderBookType,
};

fn main() {
    println!("Testing Order Lifecycle and OrderUpdateEvent Generation\n");

    test_ioc_no_match();
    test_ioc_with_match();
    test_gtc_accepted();
    test_gtc_partial_fill();
    test_gtc_full_fill();
    test_gtc_cancel();
}

fn test_ioc_no_match() {
    println!("=== Test 1: IOC order with no match ===");

    let config = PoolConfig {
        order_capacity: 10000,
        orderbook_type: OrderBookType::SkipList,
        queue_capacity: 100,
    };

    let mut engine = MatchingEngine::new(config).unwrap();

    // Send IOC buy order with no counterparty
    let order = Order::new(
        1,
        Side::Buy,
        100.0,
        10.0,
        TimeInForce::IOC,
        1000,
    );

    match engine.place_order(order) {
        Ok(result) => {
            println!("Order status: {:?}", result.status);
            match result.status {
                OrderStatus::Rejected => println!("✓ IOC order correctly REJECTED"),
                _ => println!("✗ Expected REJECTED, got {:?}", result.status),
            }
        }
        Err(e) => println!("✗ Order placement error: {:?}", e),
    }
    println!();
}

fn test_ioc_with_match() {
    println!("=== Test 2: IOC order with match ===");

    let config = PoolConfig {
        order_capacity: 10000,
        orderbook_type: OrderBookType::SkipList,
        queue_capacity: 100,
    };

    let mut engine = MatchingEngine::new(config).unwrap();

    // First: GTC sell order at 100.0
    let sell_order = Order::new(
        1,
        Side::Sell,
        100.0,
        10.0,
        TimeInForce::GTC,
        1000,
    );

    match engine.place_order(sell_order) {
        Ok(result) => println!("Sell order status: {:?}", result.status),
        Err(e) => println!("Sell order error: {:?}", e),
    }

    // Second: IOC buy order that matches
    let buy_order = Order::new(
        2,
        Side::Buy,
        100.0,
        10.0,
        TimeInForce::IOC,
        2000,
    );

    match engine.place_order(buy_order) {
        Ok(result) => {
            println!("Buy order status: {:?}", result.status);
            match result.status {
                OrderStatus::Filled => println!("✓ IOC order correctly FILLED"),
                _ => println!("✗ Expected FILLED, got {:?}", result.status),
            }
        }
        Err(e) => println!("✗ Order placement error: {:?}", e),
    }
    println!();
}

fn test_gtc_accepted() {
    println!("=== Test 3: GTC order entering book ===");

    let config = PoolConfig {
        order_capacity: 10000,
        orderbook_type: OrderBookType::SkipList,
        queue_capacity: 100,
    };

    let mut engine = MatchingEngine::new(config).unwrap();

    let order = Order::new(
        1,
        Side::Buy,
        100.0,
        10.0,
        TimeInForce::GTC,
        1000,
    );

    match engine.place_order(order) {
        Ok(result) => {
            println!("Order status: {:?}", result.status);
            match result.status {
                OrderStatus::Accepted => println!("✓ GTC order correctly ACCEPTED"),
                _ => println!("✗ Expected ACCEPTED, got {:?}", result.status),
            }
        }
        Err(e) => println!("✗ Order placement error: {:?}", e),
    }
    println!();
}

fn test_gtc_partial_fill() {
    println!("=== Test 4: GTC order partial fill ===");

    let config = PoolConfig {
        order_capacity: 10000,
        orderbook_type: OrderBookType::SkipList,
        queue_capacity: 100,
    };

    let mut engine = MatchingEngine::new(config).unwrap();

    // First: GTC sell order for 10.0
    let sell_order = Order::new(
        1,
        Side::Sell,
        100.0,
        10.0,
        TimeInForce::GTC,
        1000,
    );

    match engine.place_order(sell_order) {
        Ok(result) => println!("Sell order status: {:?}", result.status),
        Err(e) => println!("Sell order error: {:?}", e),
    }

    // Second: GTC buy order for 20.0 (larger than sell)
    let buy_order = Order::new(
        2,
        Side::Buy,
        100.0,
        20.0,
        TimeInForce::GTC,
        2000,
    );

    match engine.place_order(buy_order) {
        Ok(result) => {
            println!("Buy order status: {:?}", result.status);
            match result.status {
                OrderStatus::PartiallyFilled => println!("✓ Buy order correctly PARTIALLY_FILLED (filled: {}, remaining: {})", result.filled, 20.0 - result.filled),
                _ => println!("✗ Expected PARTIALLY_FILLED, got {:?}", result.status),
            }
        }
        Err(e) => println!("✗ Order placement error: {:?}", e),
    }
    println!();
}

fn test_gtc_full_fill() {
    println!("=== Test 5: GTC order full fill ===");

    let config = PoolConfig {
        order_capacity: 10000,
        orderbook_type: OrderBookType::SkipList,
        queue_capacity: 100,
    };

    let mut engine = MatchingEngine::new(config).unwrap();

    // First: GTC sell order
    let sell_order = Order::new(
        1,
        Side::Sell,
        100.0,
        10.0,
        TimeInForce::GTC,
        1000,
    );

    match engine.place_order(sell_order) {
        Ok(result) => println!("Sell order status: {:?}", result.status),
        Err(e) => println!("Sell order error: {:?}", e),
    }

    // Second: GTC buy order for same amount
    let buy_order = Order::new(
        2,
        Side::Buy,
        100.0,
        10.0,
        TimeInForce::GTC,
        2000,
    );

    match engine.place_order(buy_order) {
        Ok(result) => {
            println!("Buy order status: {:?}", result.status);
            match result.status {
                OrderStatus::Filled => println!("✓ Buy order correctly FILLED"),
                _ => println!("✗ Expected FILLED, got {:?}", result.status),
            }
        }
        Err(e) => println!("✗ Order placement error: {:?}", e),
    }
    println!();
}

fn test_gtc_cancel() {
    println!("=== Test 6: GTC order cancellation ===");

    let config = PoolConfig {
        order_capacity: 10000,
        orderbook_type: OrderBookType::SkipList,
        queue_capacity: 100,
    };

    let mut engine = MatchingEngine::new(config).unwrap();

    // First: GTC buy order
    let order = Order::new(
        1,
        Side::Buy,
        100.0,
        10.0,
        TimeInForce::GTC,
        1000,
    );

    match engine.place_order(order) {
        Ok(result) => println!("Order accepted with status: {:?}", result.status),
        Err(e) => println!("Order placement error: {:?}", e),
    }

    // Cancel the order
    match engine.cancel_order(1) {
        Ok(result) => {
            println!("Cancelled qty: {}", result.cancelled_quantity);
            if result.cancelled_quantity > 0.0 {
                println!("✓ Order correctly CANCELLED");
            } else {
                println!("✗ Expected cancellation, but qty is 0");
            }
        }
        Err(e) => println!("✗ Cancel error: {:?}", e),
    }
    println!();
}
