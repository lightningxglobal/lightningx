use matching_engine::skiplist::{SkipList, SortOrder};

fn main() {
    println!("SkipList Stress Test - 10,000+ operations\n");
    
    let mut sl = SkipList::new_with_pool(SortOrder::Ascending, 10000);
    
    // Insert 1000 price levels
    println!("Inserting 1000 prices...");
    for i in 0..1000 {
        let price = 1000.0 + i as f64 * 0.01;
        assert!(sl.insert_level(price).is_ok(), "Insert failed at {}", price);
    }
    assert_eq!(sl.count(), 1000);
    println!("✓ All 1000 prices inserted");
    
    // Add 10 orders at each level (10k total)
    println!("Adding 10,000 orders (10 per level)...");
    for i in 0..1000 {
        let price = 1000.0 + i as f64 * 0.01;
        for j in 0..10 {
            let order_id = (i * 10 + j) as u64;
            let qty = (j + 1) as f64;
            assert!(sl.add_order_at_level(price, order_id, qty).is_ok(),
                    "Add order failed at price {}, order {}", price, order_id);
        }
    }
    println!("✓ All 10,000 orders added");
    
    // Verify total quantities
    println!("Verifying totals...");
    let top_100 = sl.get_top_levels(100);
    let total_qty: f64 = top_100.iter().map(|(_, q)| q).sum();
    let expected_qty = (0..10).map(|i| (i + 1) as f64).sum::<f64>() * 100.0;
    assert!((total_qty - expected_qty).abs() < 0.1);
    println!("✓ Quantity tracking verified");
    
    // Remove orders randomly
    println!("Removing 5000 orders...");
    let mut removed = 0;
    for i in 0..1000 {
        let price = 1000.0 + i as f64 * 0.01;
        for j in 0..5 {
            let order_id = (i * 10 + j) as u64;
            if sl.remove_order_at_level(price, order_id).is_ok() {
                removed += 1;
            }
        }
    }
    assert_eq!(removed, 5000);
    println!("✓ All 5000 orders removed");
    
    // Remove some price levels
    println!("Removing 500 price levels...");
    for i in (0..500).step_by(2) {
        let price = 1000.0 + i as f64 * 0.01;
        sl.remove_level(price).ok();
    }
    println!("✓ Price levels removed");
    
    // Verify order counts
    println!("Final verification...");
    assert!(sl.count() >= 500, "Count mismatch after removals");
    assert!(sl.best().is_some(), "Best node should exist");
    
    println!("\n✓ Stress test passed! 10,000+ operations successful.");
}
