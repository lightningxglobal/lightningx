# Multi-Symbol Engines — Part 1: AppState DashMap

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the single `engine: Arc<Mutex<MatchingEngine>>` in `AppState` with `engines: Arc<DashMap<String, Arc<Mutex<MatchingEngine>>>>` so the exchange-server can host multiple symbols' books concurrently, without yet rewriting the routing call sites. Part 2 (separate plan / task #25) migrates ws_handler, api, and the broadcaster to use the new map.

**Architecture:** Additive only. Part 1 introduces the `engines` DashMap, a lazy `get_or_create_engine(symbol)` helper, startup pre-seeding from the `orders` table, and a temporary `engine` legacy field that returns the BTC_USDT engine (or lazily creates it on first access) so existing call sites keep compiling unchanged. This split makes Part 1 bisectable and removable in one commit.

**Tech Stack:** Rust, `dashmap`, `parking_lot`-free (`std::sync::Mutex` to match the current type), `sqlx` for the startup symbol query, `tokio` for the runtime.

---

## Decisions baked in

- **DashMap value type:** `Arc<Mutex<MatchingEngine>>` (matches current `state.engine` shape so the legacy accessor returns the right thing without conversion).
- **Lazy creation:** Symbols not seen at startup are created on first access in Part 2 by calling `get_or_create_engine(symbol)`. Part 1 only exposes the helper; the call sites don't use it yet.
- **Startup pre-seed:** Read `SELECT DISTINCT symbol FROM orders` and instantiate one engine per row, so restarts don't lose books that already have resting orders. Fallback to `["BTC_USDT"]` when the table is empty (matches existing broadcaster fallback).
- **Symbol allowlist:** Not enforced in Part 1. Part 2 needs to gate `get_or_create_engine` against an allowlist (or DB symbol table) so a client can't DOS the server by spraying unique symbol strings — flagged for Part 2.
- **Per-symbol risk params (mid price, deviation %, etc.):** Out of scope. Currently lives in desk_server's `last_mid_price` (single value); a follow-up will turn that into per-symbol storage.
- **Recovery (replay open orders into engines):** Out of scope. We only create empty engines; restoring resting orders is a broader concern handled separately.

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `src/api.rs` | Modify (AppState struct + 2 helpers) | Add `engines` field and `get_or_create_engine` / `legacy_engine` accessors. Keep `engine` field as a deprecated alias backed by `legacy_engine()`. |
| `src/main.rs` | Modify (startup wiring) | Query DB for distinct symbols, build the DashMap, populate AppState. Keep `engine` field populated with the BTC_USDT entry for legacy callers. |
| `tests/multi_symbol_state.rs` | Create (integration tests) | Cover: empty-DB fallback to BTC_USDT, pre-seed honors DB symbols, get_or_create_engine lazily inserts new symbol, two get_or_create calls return the same Arc, legacy `engine` field equals the BTC_USDT entry. |
| `docs/superpowers/plans/2026-05-17-multi-symbol-engines-part1.md` | Create (this doc) | Design + implementation plan for the lead's review. |

Out of scope for Part 1 (handled in task #25 / Part 2):

- `src/ws_handler.rs` (handle_client_message routing by symbol, broadcast_depth per symbol, market_data_broadcaster looping over symbols)
- `src/api.rs` handler bodies that use `state.engine` (cancel-order, etc.)
- `src/desk_server.rs` (per-symbol mid-price tracking)

---

## Task 1: Add `engines` to AppState + helpers

**Files:**
- Modify: `src/api.rs` (AppState definition near L22-31, plus a new `impl AppState` block)
- Test: `tests/multi_symbol_state.rs`

- [ ] **Step 1: Write the failing tests**

```rust
// tests/multi_symbol_state.rs
use std::sync::{Arc, Mutex};
use dashmap::DashMap;
use lightning_exchange::{
    api::AppState,
    engine::{MatchingEngine, PoolConfig},
};
use sqlx::PgPool;
use std::sync::atomic::AtomicU64;
use tokio::sync::{broadcast, mpsc};

fn fake_state() -> AppState {
    let (market_tx, _) = broadcast::channel::<String>(8);
    AppState {
        // Test uses a tiny in-memory map; the PgPool field is constructed
        // with a never-used lazy connect so we don't need a real DB.
        db: Arc::new(PgPool::connect_lazy("postgres://x/x").unwrap()),
        engines: Arc::new(DashMap::new()),
        engine: Arc::new(Mutex::new(
            MatchingEngine::new(PoolConfig::default()).unwrap(),
        )),
        market_tx: Arc::new(market_tx),
        user_tx: Arc::new(DashMap::new()),
        next_order_id: Arc::new(AtomicU64::new(1)),
    }
}

#[test]
fn get_or_create_inserts_lazily() {
    let state = fake_state();
    assert_eq!(state.engines.len(), 0);
    let _eng = state.get_or_create_engine("ETH_USDT");
    assert_eq!(state.engines.len(), 1);
    assert!(state.engines.contains_key("ETH_USDT"));
}

#[test]
fn get_or_create_returns_same_arc_on_repeat() {
    let state = fake_state();
    let a = state.get_or_create_engine("SOL_USDT");
    let b = state.get_or_create_engine("SOL_USDT");
    assert!(Arc::ptr_eq(&a, &b), "second call must reuse the inserted engine");
}
```

- [ ] **Step 2: Run the tests to confirm they fail**

Run: `cargo test --test multi_symbol_state`
Expected: compile error (the `engines` field and `get_or_create_engine` method don't exist yet).

- [ ] **Step 3: Add the field and the helper**

In `src/api.rs`, change the AppState definition and add an impl block immediately after it:

```rust
#[derive(Clone)]
pub struct AppState {
    pub db: Arc<PgPool>,
    /// Per-symbol matching engines. Built at startup from `SELECT DISTINCT
    /// symbol FROM orders`; new symbols are inserted lazily by
    /// `get_or_create_engine`.
    pub engines: Arc<DashMap<String, Arc<Mutex<MatchingEngine>>>>,
    /// Legacy single-engine handle. Kept during the Part 1 → Part 2 migration
    /// so call sites in ws_handler / api compile unchanged; populated at
    /// startup with the BTC_USDT entry from `engines`. Remove in Part 2 once
    /// every reader has migrated to `engines` + `get_or_create_engine`.
    pub engine: Arc<Mutex<MatchingEngine>>,
    pub market_tx: Arc<broadcast::Sender<String>>,
    pub user_tx: Arc<DashMap<i64, mpsc::Sender<String>>>,
    pub next_order_id: Arc<AtomicU64>,
}

impl AppState {
    /// Get the engine for `symbol`, creating it on first access. Returns a
    /// clone of the `Arc` so callers can lock it without holding a DashMap
    /// shard guard across the lock.
    pub fn get_or_create_engine(&self, symbol: &str) -> Arc<Mutex<MatchingEngine>> {
        if let Some(existing) = self.engines.get(symbol) {
            return existing.clone();
        }
        let new_engine = Arc::new(Mutex::new(
            MatchingEngine::new(crate::engine::PoolConfig::default())
                .expect("MatchingEngine::new should not fail for default PoolConfig"),
        ));
        // entry().or_insert_with avoids a TOCTOU race if two tasks miss the
        // get() simultaneously.
        self.engines
            .entry(symbol.to_string())
            .or_insert_with(|| new_engine)
            .clone()
    }
}
```

- [ ] **Step 4: Run the tests to confirm they pass**

Run: `cargo test --test multi_symbol_state`
Expected: 2 passed.

- [ ] **Step 5: Run the full suite to confirm nothing regressed**

Run: `cargo build --release && cargo test --lib`
Expected: 0 warnings/errors, 199 tests pass (no new lib tests yet; integration tests are in tests/).

- [ ] **Step 6: Commit**

```bash
git add src/api.rs tests/multi_symbol_state.rs docs/superpowers/plans/2026-05-17-multi-symbol-engines-part1.md
git commit -m "feat(state): add per-symbol engines DashMap with get_or_create helper"
```

---

## Task 2: Pre-seed engines from DB at startup

**Files:**
- Modify: `src/main.rs` (startup wiring near L27-37)
- Test: `tests/multi_symbol_state.rs` (extend)

- [ ] **Step 1: Write the failing tests**

Add to `tests/multi_symbol_state.rs`:

```rust
use lightning_exchange::api::seed_engines_from_symbols;

#[test]
fn seed_falls_back_to_btc_usdt_when_empty() {
    let engines = seed_engines_from_symbols(Vec::<String>::new());
    assert_eq!(engines.len(), 1);
    assert!(engines.contains_key("BTC_USDT"));
}

#[test]
fn seed_creates_one_engine_per_input_symbol() {
    let engines = seed_engines_from_symbols(vec![
        "BTC_USDT".to_string(),
        "ETH_USDT".to_string(),
        "SOL_USDT".to_string(),
    ]);
    assert_eq!(engines.len(), 3);
    for sym in ["BTC_USDT", "ETH_USDT", "SOL_USDT"] {
        assert!(engines.contains_key(sym), "missing seeded engine: {sym}");
    }
}
```

- [ ] **Step 2: Run the tests to confirm they fail**

Run: `cargo test --test multi_symbol_state`
Expected: compile error (`seed_engines_from_symbols` doesn't exist).

- [ ] **Step 3: Add the helper to `src/api.rs`**

```rust
/// Build a fresh `engines` DashMap pre-populated with one engine per input
/// symbol. Called at startup from main.rs after querying the DB; isolated
/// here (rather than inline in main) so it's unit-testable without a DB.
/// Always returns a map with at least `BTC_USDT` so the broadcaster never
/// has an empty book to broadcast.
pub fn seed_engines_from_symbols<I, S>(symbols: I) -> DashMap<String, Arc<Mutex<MatchingEngine>>>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let map = DashMap::new();
    for sym in symbols {
        let engine = MatchingEngine::new(crate::engine::PoolConfig::default())
            .expect("MatchingEngine::new should not fail for default PoolConfig");
        map.insert(sym.into(), Arc::new(Mutex::new(engine)));
    }
    if map.is_empty() {
        let engine = MatchingEngine::new(crate::engine::PoolConfig::default())
            .expect("MatchingEngine::new should not fail for default PoolConfig");
        map.insert("BTC_USDT".to_string(), Arc::new(Mutex::new(engine)));
    }
    map
}
```

- [ ] **Step 4: Wire it into main.rs**

Replace the AppState construction block in `src/main.rs` (currently L27-37):

```rust
    // Pre-seed one engine per symbol that has ever had an order. Fresh
    // deploys without any orders fall back to BTC_USDT inside the helper.
    let known_symbols: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT symbol FROM orders ORDER BY symbol",
    )
    .fetch_all(&pool)
    .await
    .unwrap_or_default();
    tracing::info!(
        "Seeding matching engines for {} symbol(s): {:?}",
        known_symbols.len().max(1),
        if known_symbols.is_empty() { vec!["BTC_USDT".to_string()] } else { known_symbols.clone() },
    );
    let engines = Arc::new(seed_engines_from_symbols(known_symbols));

    // Legacy single-engine field for the Part 1 → Part 2 migration window.
    // Resolves to the BTC_USDT entry, which seed_engines_from_symbols
    // guarantees is present.
    let legacy_engine = engines
        .get("BTC_USDT")
        .expect("seed_engines_from_symbols guarantees BTC_USDT entry")
        .clone();

    let (market_tx, _) = broadcast::channel::<String>(1024);

    let state = AppState {
        db: Arc::new(pool),
        engines,
        engine: legacy_engine,
        market_tx: Arc::new(market_tx),
        user_tx: Arc::new(DashMap::new()),
        next_order_id: Arc::new(AtomicU64::new(1)),
    };
```

Remove the now-unused imports:

```rust
// Drop these from the `use` block at the top of main.rs:
//   engine::{MatchingEngine, PoolConfig}  — no longer constructed here
//   std::sync::{Arc, Mutex}                — only Arc is still used; drop Mutex
//
// And add:
use lightning_exchange::api::seed_engines_from_symbols;
```

- [ ] **Step 5: Export `seed_engines_from_symbols` from the lib**

In `src/lib.rs`, add to the existing re-exports block:

```rust
pub use api::seed_engines_from_symbols;
```

- [ ] **Step 6: Run the tests to confirm they pass**

Run: `cargo test --test multi_symbol_state`
Expected: 4 passed (the 2 from Task 1 plus the 2 new ones).

- [ ] **Step 7: Confirm release build and full suite**

Run: `cargo build --release && cargo test --lib`
Expected: 0 warnings/errors, 199 lib tests pass.

- [ ] **Step 8: Smoke test main.rs startup against a local DB**

Run (in a separate terminal):
```bash
DATABASE_URL=postgres://user:password@localhost:5432/mydb \
  cargo run --release --bin lightning-exchange 2>&1 | head -20
```
Expected: log line `Seeding matching engines for N symbol(s): […]` listing the symbols (or `["BTC_USDT"]` on a fresh DB), and the server binds to port 3000 without panicking. Kill with Ctrl-C.

- [ ] **Step 9: Commit**

```bash
git add src/main.rs src/api.rs src/lib.rs tests/multi_symbol_state.rs
git commit -m "feat(state): seed per-symbol engines from DB at startup"
```

---

## Verification at end of Part 1

- `cargo build --release` clean.
- `cargo test --lib` → 199 passed (unchanged — the new tests are integration tests, not lib tests).
- `cargo test --test multi_symbol_state` → 4 passed.
- A fresh `cargo run --release` boots, logs the seeded symbols, and serves on port 3000.
- Every existing reader of `state.engine` still compiles and runs against the BTC_USDT engine (no behavior change for single-symbol clients).
- `state.engines.len() == max(1, distinct(orders.symbol))` after startup.

## Hand-off to Part 2 (task #25)

Part 2 should:
1. Replace every `state.engine.lock()` call with `state.get_or_create_engine(&symbol).lock()`, threading the symbol from the inbound message / order row.
2. Make `market_data_broadcaster` iterate over `state.engines` (and refresh the iteration set periodically so newly-added symbols start broadcasting).
3. Add a symbol allowlist or DB-backed validation inside `get_or_create_engine` so an unauthenticated caller can't spray arbitrary symbol strings and exhaust memory.
4. Remove the legacy `state.engine` field once no call site reads it.
5. Update `tests/multi_symbol_state.rs` and add ws_handler integration tests covering per-symbol routing.
