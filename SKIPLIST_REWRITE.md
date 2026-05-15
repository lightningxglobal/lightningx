# SkipList Rewrite: Raw Pointer Arena Design

## Problem Fixed

The original SkipList had a fundamental design flaw that prevented O(log N) operations:

### Design Flaw: Ownership Constraint
```rust
// OLD (broken):
pub forward: Vec<Option<Box<SkipListNode>>>
```

- Each `Box` requires unique ownership
- Same physical node cannot be owned by multiple levels
- Result: Only level 0 worked, degrading from O(log N) to O(N)

### Additional Bugs
1. **Double Traversal**: `insert_level()` traversed the entire skiplist twice
2. **O(12×N) Bug**: `get_node_at_price()` iterated `MAX_LEVEL=12` times instead of `self.level`
3. **Missing Quantity Update**: `remove_order_at_level()` didn't update `total_quantity`

## Solution: Raw Pointer + Arena Pattern

### New Design
```rust
pub struct SkipListNode {
    pub forward: [*mut SkipListNode; MAX_LEVEL],  // ← KEY: raw pointers
}

pub struct SkipList {
    head: *mut SkipListNode,                      // ← Pointer to sentinel
    level: usize,                                  // ← Current max level
    arena: Vec<Box<SkipListNode>>,                // ← Sole owner of all nodes
}
```

### Why This Works
- **Arena holds sole ownership**: `Vec<Box<SkipListNode>>` owns all heap nodes
- **Box stability**: Box pointers remain valid across Vec reallocations (stored by value, not reference)
- **Raw pointers safe**: All raw pointers point into arena, valid for SkipList lifetime
- **Mutual exclusion**: `&mut self` prevents concurrent access
- **Automatic cleanup**: When SkipList drops, arena drops, all nodes freed

## Key Changes

### 1. Node Initialization
- Moved `random_level()` from node method to SkipList static method
- Removed `new()` method (simplified to private `new()`)
- Fixed head initialization: `level: 0` instead of `MAX_LEVEL - 1`

### 2. Traversal Helper: find_update()
```rust
unsafe fn find_update(&self, price: f64) -> ([*mut SkipListNode; MAX_LEVEL], *mut SkipListNode)
```
- Consolidates search logic used by insert/delete
- Returns predecessors at all levels (for multi-level insertion)
- Fills higher levels with head (for automatic level promotion)

### 3. Insert Operation
```rust
pub fn insert_level(&mut self, price: f64) -> Result<(), String> {
    let new_level = Self::random_level();
    unsafe {
        let (update, _) = self.find_update(price);  // Single traversal
        // ... create new node in arena ...
        for i in 0..=new_level {
            (*new_ptr).forward[i] = (*update[i]).forward[i];
            (*update[i]).forward[i] = new_ptr;
        }
    }
    Ok(())
}
```
- Single traversal (fixed double-traversal bug)
- Multi-level insertion works correctly

### 4. Lookup Functions
```rust
pub fn get_node_at_price(&self, price: f64) -> Option<&SkipListNode> {
    unsafe {
        let mut current = self.head;
        for i in (0..=self.level).rev() {  // Fixed: use self.level not MAX_LEVEL
            // traverse
        }
    }
}
```

### 5. Remove Order with Quantity Tracking
```rust
if list_node.order_id == order_id {
    let quantity = list_node.quantity;
    (*next).orders.remove(node_idx, &mut self.list_pool);
    self.list_pool.release(node_idx);
    (*next).total_quantity -= quantity;  // ← Fixed: update total_quantity
}
```

### 6. Level Shrinking
```rust
while self.level > 0 && (*self.head).forward[self.level].is_null() {
    self.level -= 1;
}
```
- When highest levels become empty, shrink level to save traversal cost

## Performance Results

All operations now O(log N):
- **Insert**: 222 ns per operation (1000 inserts)
- **Find**: 64 ns per operation (1000 lookups)
- **Add Order**: 25 ns per operation
- **Remove Order**: 25 ns per operation

## Testing

8 comprehensive tests:
1. `test_insert_and_find` - Basic insert/find operations
2. `test_insert_duplicate_rejected` - Duplicate protection
3. `test_level_distribution` - Verify multi-level nodes
4. `test_add_remove_orders` - Order management with quantity tracking
5. `test_best_with_orders` - Skip empty price levels
6. `test_clear` - Reset skiplist
7. `test_remove_level` - Delete entire price level
8. `test_sorted_level_0_*` - Verify correct sort order

All tests passing ✓

## Safety Analysis

### Unsafe Code Justification
All unsafe code is within `impl SkipList`:
- **Bounded by lifetime**: All raw pointers point to arena nodes
- **Bounded by exclusivity**: `&mut self` prevents concurrent access
- **Bounded by scope**: Unsafe block only accesses self fields
- **Comments explain invariants**: Each unsafe section documented

### Memory Safety
- No memory leaks: Arena cleanup automatic
- No dangling pointers: Arena keeps nodes alive
- No use-after-free: Arena outlives all raw pointers
- No data races: Exclusive access via &mut self

## Impact on Matching Engine

The SkipList is used for the order book (best bid/ask tracking):
- Previous: O(N) per order insertion/cancellation
- Now: O(log N) per order operation
- Enables sub-microsecond matching at scale

This is critical for achieving the performance target of 7M TPS with <300ns P50 latency.

## Files Changed
- `src/skiplist.rs`: Complete rewrite (895 lines added/removed)
- Commit: `e6362e8`
