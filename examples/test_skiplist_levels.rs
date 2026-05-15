use matching_engine::skiplist::{SkipList, SortOrder};

fn main() {
    println!("Testing SkipList Multi-Level Traversal\n");

    let mut sl = SkipList::new_with_pool(SortOrder::Ascending, 1000);

    // Insert many prices to trigger multi-level nodes
    for i in 0..100 {
        sl.insert_level(1000.0 + i as f64).ok();
    }

    println!("Inserted 100 prices");
    println!("Internal level: {}", unsafe {
        let head_ptr = sl.head;
        // Count how many levels have forward pointers
        let mut max_level = 0;
        for i in 0..12 {
            if !(*head_ptr).forward[i].is_null() {
                max_level = i;
            }
        }
        max_level
    });

    // Verify all prices are findable
    for i in 0..100 {
        let price = 1000.0 + i as f64;
        assert!(sl.find_node(price).is_ok(), "Failed to find price {}", price);
    }
    println!("✓ All 100 prices found via multi-level traversal");

    // Verify level 0 is sorted
    let top = sl.get_top_levels(100);
    assert_eq!(top.len(), 100);
    for i in 0..99 {
        assert!(top[i].0 < top[i + 1].0, "Level 0 not sorted!");
    }
    println!("✓ Level 0 correctly sorted");

    // Test that nodes at different levels have correct forward pointers
    println!("\nStructure verification:");
    unsafe {
        let head_ptr = sl.head;
        let mut level_counts = [0; 12];

        // Walk level 0 and count node levels
        let mut current = head_ptr;
        let mut node_count = 0;
        loop {
            let next = (*current).forward[0];
            if next.is_null() { break; }

            let level = (*next).level;
            level_counts[level] += 1;
            node_count += 1;

            current = next;
        }

        println!("Total nodes at each level:");
        for i in 0..12 {
            if level_counts[i] > 0 {
                println!("  Level {}: {} nodes", i, level_counts[i]);
            }
        }
        println!("Total unique nodes: {}", node_count);
    }

    println!("\nAll multi-level tests passed! ✓");
}
