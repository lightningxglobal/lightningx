use matching_engine::skiplist::{SkipList, SortOrder};

fn main() {
    println!("Testing SkipList...\n");

    // Test 1: Insert and Find
    println!("Test 1: Insert and Find");
    let mut sl = SkipList::new_with_pool(SortOrder::Ascending, 100);
    for i in 0..10 {
        let price = 100.0 + i as f64;
        assert!(sl.insert_level(price).is_ok(), "Failed to insert {}", price);
    }
    assert_eq!(sl.count(), 10);
    println!("✓ Inserted 10 prices, count = {}", sl.count());

    for i in 0..10 {
        let price = 100.0 + i as f64;
        assert!(sl.find_node(price).is_ok(), "Failed to find {}", price);
        assert!(sl.get_node_at_price(price).is_some());
    }
    println!("✓ All 10 prices found\n");

    // Test 2: Duplicate rejection
    println!("Test 2: Duplicate rejection");
    assert!(sl.insert_level(100.0).is_err());
    println!("✓ Duplicate correctly rejected\n");

    // Test 3: Add/Remove orders
    println!("Test 3: Add/Remove orders");
    let mut sl2 = SkipList::new_with_pool(SortOrder::Ascending, 100);
    sl2.insert_level(100.0).ok();
    sl2.add_order_at_level(100.0, 1, 10.0).ok();
    sl2.add_order_at_level(100.0, 2, 20.0).ok();
    let node = sl2.get_node_at_price(100.0).unwrap();
    assert!((node.total_quantity - 30.0).abs() < 1e-10);
    println!("✓ Added 2 orders, total quantity = {}", node.total_quantity);

    sl2.remove_order_at_level(100.0, 1).ok();
    let node = sl2.get_node_at_price(100.0).unwrap();
    assert!((node.total_quantity - 20.0).abs() < 1e-10);
    println!("✓ Removed order 1, remaining quantity = {}\n", node.total_quantity);

    // Test 4: Best with orders
    println!("Test 4: Best with orders");
    let mut sl3 = SkipList::new_with_pool(SortOrder::Ascending, 100);
    sl3.insert_level(100.0).ok();
    sl3.insert_level(101.0).ok();
    sl3.insert_level(102.0).ok();
    sl3.add_order_at_level(101.0, 1, 10.0).ok();

    assert_eq!(sl3.best().unwrap().price, 100.0);
    assert_eq!(sl3.best_with_orders().unwrap().price, 101.0);
    println!("✓ best() = 100.0, best_with_orders() = 101.0\n");

    // Test 5: Clear
    println!("Test 5: Clear");
    let mut sl4 = SkipList::new_with_pool(SortOrder::Ascending, 100);
    for i in 0..5 {
        sl4.insert_level(100.0 + i as f64).ok();
    }
    assert_eq!(sl4.count(), 5);
    sl4.clear();
    assert_eq!(sl4.count(), 0);
    assert!(sl4.best().is_none());
    println!("✓ Cleared 5 prices, count now = {}\n", sl4.count());

    // Test 6: Remove level
    println!("Test 6: Remove level");
    let mut sl5 = SkipList::new_with_pool(SortOrder::Ascending, 100);
    sl5.insert_level(100.0).ok();
    sl5.insert_level(101.0).ok();
    sl5.insert_level(102.0).ok();
    assert_eq!(sl5.count(), 3);

    sl5.remove_level(101.0).ok();
    assert_eq!(sl5.count(), 2);
    assert!(sl5.find_node(101.0).is_err());
    println!("✓ Removed price 101.0, count now = {}\n", sl5.count());

    // Test 7: Ascending sort order
    println!("Test 7: Ascending sort order");
    let mut sl6 = SkipList::new_with_pool(SortOrder::Ascending, 100);
    let prices = vec![105.5, 100.2, 103.7, 102.1, 104.9, 101.3];
    for &price in &prices {
        sl6.insert_level(price).ok();
    }
    
    let top = sl6.get_top_levels(6);
    println!("Inserted (random order): {:?}", prices);
    println!("Retrieved (sorted order): {:?}", top.iter().map(|p| p.0).collect::<Vec<_>>());
    
    let mut prev = f64::NEG_INFINITY;
    for (price, _) in &top {
        assert!(price > &prev, "Level 0 should be sorted ascending");
        prev = *price;
    }
    println!("✓ All prices in ascending order\n");

    // Test 8: Get top levels
    println!("Test 8: Get top levels");
    let mut sl7 = SkipList::new_with_pool(SortOrder::Ascending, 100);
    for i in 0..5 {
        let price = 100.0 + i as f64;
        sl7.insert_level(price).ok();
        for j in 0..=i {
            sl7.add_order_at_level(price, j as u64, 1.0).ok();
        }
    }

    let top = sl7.get_top_levels(3);
    assert_eq!(top.len(), 3);
    assert_eq!(top[0].0, 100.0);
    assert_eq!(top[1].0, 101.0);
    assert_eq!(top[2].0, 102.0);
    println!("✓ Top 3 levels: {:?}\n", top);

    println!("All tests passed! ✓");
}
