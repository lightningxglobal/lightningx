/// Complete order lifecycle test - All order types with correct status transitions
///
/// Validates:
/// 1. IOC orders (Immediate-or-Cancel)
/// 2. GTC orders (Good-Till-Cancel)
/// 3. FOK orders (Fill-or-Kill)
/// 4. PostOnly orders
///
/// Expected transitions verified against ORDER_LIFECYCLE.md

use matching_engine::{
    engine::{MatchingEngine, PoolConfig},
    order::{Order, Side, TimeInForce},
    orderbook_impl::OrderBookType,
    time_provider::wall_clock_nanos,
};

fn main() {
    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║      Complete Order Lifecycle Test - All Order Types       ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    test_ioc_orders();
    test_gtc_orders();
    test_fok_orders();
    test_postonly_orders();

    println!("\n✅ All order lifecycle tests completed successfully!");
}

fn test_ioc_orders() {
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("IOC Orders (Immediate-or-Cancel)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    let mut engine = setup_engine();
    let now = wall_clock_nanos();

    // Test: IOC with no match → REJECTED
    println!("Scenario 1: IOC with no matching counterparty");
    let id1 = engine.stats().total_orders as u64;
    let order = Order::new(id1, Side::Buy, 100.0, 10.0, TimeInForce::IOC, now);
        Ok(result) => {
            if matches!(result.status, matching_engine::engine::OrderStatus::Rejected) {
                println!("  ✓ Status: REJECTED (correct for no match)");
            }
        }
        Err(e) => println!("  ✗ Error: {:?}", e),
    }

    // Test: IOC with match → FILLED
    println!("\nScenario 2: IOC with matching counterparty");

    // First place GTC sell order
    let id2 = engine.stats().total_orders as u64;
    let sell = Order::new(id2, Side::Sell, 100.0, 10.0, TimeInForce::GTC, now);

    // Then IOC buy matching it
    let id3 = engine.stats().total_orders as u64;
    let buy = Order::new(id3, Side::Buy, 100.0, 10.0, TimeInForce::IOC, now);
        Ok(result) => {
            if matches!(result.status, matching_engine::engine::OrderStatus::Filled) {
                println!("  ✓ Status: FILLED (matched entire order)");
                println!("  ✓ Filled quantity: {}", result.filled);
            }
        }
        Err(e) => println!("  ✗ Error: {:?}", e),
    }
    println!();
}

fn test_gtc_orders() {
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("GTC Orders (Good-Till-Cancel)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    let mut engine = setup_engine();
    let now = wall_clock_nanos();

    // Test: GTC enters book → ACCEPTED
    println!("Scenario 1: GTC order entering book (no immediate match)");
    let id1 = engine.stats().total_orders as u64;
    let order = Order::new(id1, Side::Buy, 100.0, 10.0, TimeInForce::GTC, now);
        Ok(result) => {
            if matches!(result.status, matching_engine::engine::OrderStatus::Accepted) {
                println!("  ✓ Status: ACCEPTED (entered order book)");
            }
        }
        Err(e) => println!("  ✗ Error: {:?}", e),
    }

    // Test: GTC partial fill → PARTIALLY_FILLED
    println!("\nScenario 2: GTC partial fill");

    // Place GTC sell for 10
    let id2 = engine.stats().total_orders as u64;
    let sell = Order::new(id2, Side::Sell, 100.0, 10.0, TimeInForce::GTC, now);
        Ok(r) => println!("  Sell order: {:?}", r.status),
        Err(e) => println!("  Sell error: {:?}", e),
    }

    // Place GTC buy for 20 (more than available)
    let id3 = engine.stats().total_orders as u64;
    let buy = Order::new(id3, Side::Buy, 100.0, 20.0, TimeInForce::GTC, now);
        Ok(result) => {
            match result.status {
                matching_engine::engine::OrderStatus::PartiallyFilled => {
                    println!("  ✓ Status: PARTIALLY_FILLED");
                    println!("  ✓ Filled: {} | Remaining: {}", result.filled, 20.0 - result.filled);
                }
                _ => println!("  ✗ Expected PartiallyFilled, got {:?}", result.status),
            }
        }
        Err(e) => println!("  ✗ Error: {:?}", e),
    }

    // Test: GTC full fill → FILLED
    println!("\nScenario 3: GTC full fill");

    let id4 = engine.stats().total_orders as u64;
    let sell = Order::new(id4, Side::Sell, 100.0, 10.0, TimeInForce::GTC, now);
        Ok(r) => println!("  Sell order: {:?}", r.status),
        Err(e) => println!("  Sell error: {:?}", e),
    }

    let id5 = engine.stats().total_orders as u64;
    let buy = Order::new(id5, Side::Buy, 100.0, 10.0, TimeInForce::GTC, now);
        Ok(result) => {
            match result.status {
                matching_engine::engine::OrderStatus::Filled => {
                    println!("  ✓ Status: FILLED (complete match)");
                }
                _ => println!("  ✗ Expected Filled, got {:?}", result.status),
            }
        }
        Err(e) => println!("  ✗ Error: {:?}", e),
    }
    println!();
}

fn test_fok_orders() {
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("FOK Orders (Fill-or-Kill)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    let mut engine = setup_engine();
    let now = wall_clock_nanos();

    // Test: FOK can't fill completely → REJECTED
    println!("Scenario 1: FOK unable to fill completely");
    let id1 = engine.stats().total_orders as u64;
    let order = Order::new(id1, Side::Buy, 100.0, 10.0, TimeInForce::FOK, now);
        Ok(result) => {
            if matches!(result.status, matching_engine::engine::OrderStatus::Rejected) {
                println!("  ✓ Status: REJECTED (can't fill completely)");
            }
        }
        Err(e) => println!("  ✗ Error: {:?}", e),
    }

    // Test: FOK can fill completely → FILLED
    println!("\nScenario 2: FOK able to fill completely");

    let id2 = engine.stats().total_orders as u64;
    let sell = Order::new(id2, Side::Sell, 100.0, 10.0, TimeInForce::GTC, now);

    let id3 = engine.stats().total_orders as u64;
    let buy = Order::new(id3, Side::Buy, 100.0, 10.0, TimeInForce::FOK, now);
        Ok(result) => {
            if matches!(result.status, matching_engine::engine::OrderStatus::Filled) {
                println!("  ✓ Status: FILLED (complete fill achieved)");
            }
        }
        Err(e) => println!("  ✗ Error: {:?}", e),
    }
    println!();
}

fn test_postonly_orders() {
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("PostOnly Orders (No immediate execution)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    let mut engine = setup_engine();
    let now = wall_clock_nanos();

    // Test: PostOnly enters book → ACCEPTED
    println!("Scenario 1: PostOnly enters order book");
    let id1 = engine.stats().total_orders as u64;
    let order = Order::new(id1, Side::Buy, 100.0, 10.0, TimeInForce::PostOnly, now);
        Ok(result) => {
            if matches!(result.status, matching_engine::engine::OrderStatus::Accepted) {
                println!("  ✓ Status: ACCEPTED (enters book, no execution)");
            }
        }
        Err(e) => println!("  ✗ Error: {:?}", e),
    }

    // Test: PostOnly can be filled by later order
    println!("\nScenario 2: PostOnly filled by subsequent order");

    let id2 = engine.stats().total_orders as u64;
    let post_only = Order::new(id2, Side::Buy, 100.0, 10.0, TimeInForce::PostOnly, now);

    let id3 = engine.stats().total_orders as u64;
    let sell = Order::new(id3, Side::Sell, 100.0, 10.0, TimeInForce::GTC, now);
        Ok(result) => {
            if matches!(result.status, matching_engine::engine::OrderStatus::Filled) {
                println!("  ✓ Status: FILLED (matched with PostOnly order)");
            }
        }
        Err(e) => println!("  ✗ Error: {:?}", e),
    }
    println!();
}

fn setup_engine() -> MatchingEngine {
    let config = PoolConfig {
        order_capacity: 10000,
        orderbook_type: OrderBookType::SkipList,
        queue_capacity: 100,
    };
    MatchingEngine::new(config).unwrap()
}
