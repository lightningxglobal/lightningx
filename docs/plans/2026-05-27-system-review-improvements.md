# System Review Improvement Plan

Date: 2026-05-27

This file tracks the design, architecture, correctness, and performance review items found in the current LightningX system. The goal is to turn each review finding into an executable fix with a clear acceptance check.

## Execution Order

1. Core matching correctness: deterministic behavior inside the engine first.
2. Transport encoding and allocation cleanup: remove drift between SBE tests and production paths.
3. Routing and loss prevention: make cross-process event handling fail visible instead of silently.
4. Order lifecycle consistency: make risk, cancel, persistence, and settlement follow engine-confirmed state transitions.
5. Long-running architecture work: replay/recovery, migration tracking, and broader operational hardening.

## Findings And Acceptance Criteria

### 1. Duplicate order IDs can corrupt the book

Status: Done

Problem: `MatchingEngine::add_to_book` overwrites `orders[order_id]` without rejecting duplicates, leaving stale list nodes in price levels.

Acceptance:
- Engine rejects duplicate active order IDs before mutating the book.
- A regression test proves the second order is rejected and no stale/zombie level remains.

### 2. FOK fillability check scans only 1000 levels

Status: Done

Problem: `can_fill_fok` uses `get_top_levels(1000)`, so a valid FOK can be rejected on a deep but fillable book.

Acceptance:
- FOK pre-check walks the opposite book until quantity is sufficient or price becomes unacceptable.
- A regression test covers fillability beyond 1000 price levels.

### 3. SBE order update encoding has drift

Status: Done

Problem: `OrderUpdateMsg` is encoded manually in the Aeron publisher with template id `2`, while tests use a different block length and the main SBE module does not own the type.

Acceptance:
- Production encode/decode helpers own `OrderUpdateMsg`.
- Tests call the production helper instead of reimplementing byte layout.
- Header block length matches the actual 64-byte struct.

### 4. Hot-path Aeron publishers allocate

Status: Done

Problem: order update and trade publishers allocate `Vec<u8>` for every fixed-size message.

Acceptance:
- Fixed-size outbound messages use stack arrays.
- Encoding stays covered by tests.

### 5. DB command ring buffer failures are ignored

Status: Done

Problem: `db_tx.push(...)` failures are discarded, which can silently drop order persistence, status updates, or settlement commands.

Acceptance:
- Every critical DB command push has explicit handling.
- On full ring, the system logs and retries, or marks the event as failed with a visible metric/error path.

### 6. Cancel lifecycle is desk-confirmed instead of engine-confirmed

Status: Done

Problem: REST/WS cancel paths update DB and release funds before the engine confirms cancellation, racing with fills.

Acceptance:
- Desk sends cancel requests first.
- DB status and frozen funds are updated only after a `CANCELLED` update from the engine.
- Timeout/failure paths are explicit and do not release funds prematurely.

### 7. Fast-path risk/funds reservation happens after engine acceptance

Status: Done

Problem: WS Aeron fast path can send orders into the engine before funds are frozen. Freeze failure triggers cancellation after acceptance, which leaves a window for unbacked fills.

Acceptance:
- Orders entering the engine have passed risk/funds reservation, or the engine receives a reservation token/confirmed order id.
- Freeze failure cannot occur after an order has already filled.

### 8. Symbol-to-stream routing is static and unsafe for new symbols

Status: Done

Problem: only BTC/ETH/SOL have deterministic stream ids; unknown symbols fall back to the same legacy stream. Engine threads do not reject mismatched symbols.

Acceptance:
- Desk and engine derive stream mappings from the configured `SYMBOLS`.
- Engine symbol thread validates inbound message symbol before processing.

### 9. Trade/user cache lifetime is too short

Status: Partial

Problem: `order_uid_cache` removes taker and maker entries while additional fills for the same order can still arrive.

Acceptance:
- UID cache entries live until the order receives a terminal status.
- Multi-fill settlement does not rely on retry sleeps or DB races.

### 10. Price and quantity use raw `f64`

Status: Partial

Problem: core and persistence paths use floating point values for prices and quantities, relying on epsilon guards.

Acceptance:
- API boundaries enforce per-symbol tick size, lot size, and minimum notional.
- A follow-up design decision is made for fixed-point internal representation.

### 11. SkipList top-level snapshot allocates

Status: Done

Problem: `get_top_levels` returns `Vec`, used by hot depth/FOK paths.

Acceptance:
- Hot snapshot paths can fill caller-provided buffers or `SmallVec`.
- Existing public helper can remain for non-hot/debug paths if needed.

### 11a. Partial fills leave visible depth quantities stale

Status: Done

Problem: maker partial fills updated the pooled `Order` but did not reduce the price-level list node quantity or level total.

Acceptance:
- Partial fills reduce visible depth immediately.
- FOK pre-checks use current visible quantity.
- A regression test covers a partially consumed resting order.

### 12. SkipList removed levels are not reused

Status: Done

Problem: `remove_level` unlinks nodes but leaves them in the arena until engine drop.

Acceptance:
- Removed level nodes are reusable, or a bounded slab/free-list replaces append-only arena growth.
- Market-maker churn benchmark demonstrates bounded memory growth.

### 13. Aeron backpressure spins forever

Status: Done

Problem: publishers spin indefinitely on backpressure without timeout or metrics.

Acceptance:
- Backpressure loops have a bounded retry/yield policy.
- Failures surface to callers and logs/metrics.

### 14. Aeron callbacks use `Arc<Mutex<VecDeque>>`

Status: Pending

Problem: every inbound Aeron message locks and copies through a `VecDeque`.

Acceptance:
- Subscriber path avoids mutex per message, or uses a bounded SPSC ring.
- Polling still keeps subscription handles alive and explicitly polls Aeron.

### 15. Order state mappings are duplicated

Status: Pending

Problem: engine, DB, REST, and WS state strings are mapped in several places.

Acceptance:
- State transition/mapping logic is centralized.
- Tests cover REST and WS state output for the same engine result.

### 16. `client_order_id` is not idempotent

Status: Done

Problem: migration adds `client_order_id` but does not enforce uniqueness per user.

Acceptance:
- Duplicate `(user_id, client_order_id)` returns the existing order or is rejected deterministically.
- Migration adds the appropriate partial unique index.

### 17. Recovery cancels crossed books instead of replaying deterministically

Status: Pending

Problem: startup recovery cancels all orders for a crossed symbol, which is safe but lossy.

Acceptance:
- A replayable order event log or deterministic restore process exists.
- Startup reports book checksum/validation without destructive fallback for recoverable cases.

### 18. Migration execution lacks a version table

Status: Done

Problem: app startup runs raw SQL includes without recording applied versions.

Acceptance:
- Migration application is versioned and observable.
- Concurrent and failed migrations have deterministic behavior.

### 19. Boundary tests are weak

Status: Done

Problem: several tests accept either success or failure and do not define expected behavior.

Acceptance:
- Boundary tests assert one deterministic contract per case.
- Duplicate order and pool exhaustion tests verify book state after failure.

## Verification Checklist

- `cargo fmt --check`
- `cargo test --lib`
- SBE-focused tests
- `cargo test --no-run`
- Targeted integration/manual checks for Aeron order, cancel, and settlement flow when those paths are touched.
