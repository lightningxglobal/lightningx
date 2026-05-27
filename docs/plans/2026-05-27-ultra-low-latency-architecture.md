# Ultra-Low-Latency Architecture Plan

Date: 2026-05-27

This plan covers the remaining large architecture items from the system review. The guiding constraint is that every microsecond matters: correctness work must not add locks, heap allocation, dynamic dispatch, string handling, DB waits, or network waits to the matching hot path.

## Principles

- Matching threads remain single-owner, per-symbol, deterministic loops.
- The engine hot path emits compact fixed-size events and returns; persistence, WebSocket, metrics, and recovery projections run outside the matching loop.
- Cross-thread handoff uses bounded preallocated SPSC rings or Aeron IPC. No `Mutex<VecDeque>` in message fast paths.
- Durable recovery is based on ordered engine events and snapshots, not reconstructing intent from mutable order rows.
- API JSON floats are converted at the boundary. Internal matching should move toward fixed-point integer ticks/lots.

## Phase 1: Aeron Subscriber SPSC Rings

Status: Completed in `src/transport/aeron_transport.rs`.

Goal: remove `Arc<Mutex<VecDeque<_>>` from Aeron callbacks.

Design:
- Each Aeron subscriber owns a bounded SPSC ring with a callback-side producer and poll-side consumer.
- Callback work is limited to length/template validation, `read_unaligned`, and ring `push`.
- Order inbound ring full is a hard backpressure signal; order messages must not be silently dropped.
- Market-data ring full may drop old snapshots and keep latest, but must increment counters.

Acceptance:
- No per-message mutex in order update, trade, depth, or inbound order subscribers.
- Ring-full behavior is deterministic per message class and increments dropped counters.
- `cargo test --lib` plus targeted subscriber tests pass.

## Phase 2: UID Cache Lifetime

Status: Completed in `src/bin/desk_server.rs` and `src/bin/exchange_engine.rs`.

Goal: multi-fill settlement must not depend on DB lookup races.

Design:
- Maintain compact `order_id -> OrderRuntimeMeta` in the desk Aeron event loop.
- Insert on accepted/submitted order paths.
- Do not remove on trade notification.
- Remove only on terminal order update: filled, cancelled, rejected.
- Preload open orders into the cache during desk startup.

Acceptance:
- One taker consuming multiple makers settles all fills without relying on DB lookup on the hot event path.
- Maker metadata survives partial fills until terminal state.
- Desk startup preloads open order metadata into the runtime cache.

## Phase 3: Central Order State Machine

Status: Completed for DB/WS order-status projections.

Goal: remove scattered DB/WS/engine state string mappings.

Design:
- Add compact enums for engine events, order states, and side effects.
- A pure transition function maps `(current_state, engine_event)` to `(next_state, side_effects)`.
- Side effects are fixed enums: persist order, update filled, settle fill, release frozen, push user update.
- DB and WS string conversion happens only at projection boundaries.

Acceptance:
- REST and WS use the same central mapping helpers for matching-engine and Aeron order updates.
- State transition tests cover accepted, partial fill, filled, cancelled, and rejected mappings.

## Phase 4: Fixed-Point Matching

Status: In progress. Phase 4a adds fixed-point boundary normalization. The temporary fixed-to-legacy bridge was removed because the system is still in development and the matching hot path should move directly to integer ticks/lots.

Goal: remove `f64` from engine price/quantity arithmetic.

Design:
- API receives floats but validates and converts to integer ticks/lots at the boundary.
- Engine `Order` uses `i64 price_ticks`, `i64 quantity_lots`, `i64 filled_lots`.
- SBE gets a v2 template with integer fields.
- DB keeps legacy float columns for display initially, with integer columns as source of truth.

Acceptance:
- API and WS order entry normalize price/quantity to integer ticks/lots before accepting an order.
- Engine order state, book keys, fill accounting, and matching comparisons use integer ticks/lots directly.
- Float conversion exists only at external projection boundaries such as REST/WS JSON, DB legacy display columns, and market-data publishing.
- Misaligned price/quantity inputs are rejected at the boundary.
- Matching benchmark p50/p99 does not regress for each Phase 4 step.

## Phase 5: Event Log And Replay Recovery

Status: Pending

Goal: recover from deterministic engine events instead of cancelling crossed books.

Design:
- Per-symbol monotonic `engine_seq`.
- Engine emits append-only events: accepted, rejected, trade, cancelled.
- A writer persists events sequentially outside the matching loop.
- Periodic snapshots contain active orders, top-level book state, and last sequence.
- Startup loads the last snapshot, replays events, and validates checksum.

Latency mode:
- Default low-latency mode uses asynchronous durable writing and documents a bounded RPO.
- Strong durability mode may wait for writer acknowledgement and is explicitly slower.

Acceptance:
- Random order stream replay produces the same book checksum as live execution.
- Startup no longer needs crossed-book mass cancellation for recoverable event streams.

## Deferred Tradeoff

Hard synchronous durability on every order is incompatible with the current microsecond latency target. The design keeps the matching path deterministic and fast, then gives deployments a clear choice between lowest latency and stronger persistence acknowledgement.
