# HA And Replay Model

This note defines the next production-hardening target after fixed-point accounts,
sequenced private updates, matching event persistence, transactional pg-writer
flush, STP, and price-band protection.

## Current State

- `exchange-engine` is still the live matching truth for each symbol thread.
- `OrderUpdateMsg` has a per-response-stream sequence for desk-side gap detection.
- `matching_events` is an append-only PostgreSQL audit table written asynchronously
  by `pg-writer`.
- `pg-writer` flushes buffered payloads in a single transaction and keeps the batch
  on failure.
- Aeron IPC streams are not replayable after a subscriber restart.

This is better than the old snapshot-only model, but it is not yet production HA.
The missing piece is a deterministic, sequenced event log that is committed before
external finality depends on it.

## Target Invariants

1. Every matching output event has a strictly increasing per-engine sequence.
2. Downstream services can detect any gap by sequence.
3. A process restart can rebuild matching/account/risk state from durable events.
4. No order is considered externally final unless its matching event is durable or
   is recoverable from an equivalent durable WAL segment.
5. PostgreSQL remains audit/cold storage; live hot path must not synchronously query
   PostgreSQL.

## Event Shape

Each matching event should carry:

- `engine_id`
- `symbol_id`
- `sequence`
- `event_kind`
- `order_id`
- `client_order_id`
- `participant_id`
- `response_stream_id`
- `price_ticks`
- `quantity_lots`
- `remaining_lots`
- `reject_reason`
- `event_ts_ns`

The natural key is `(engine_id, sequence)`. PostgreSQL can also keep
`client_order_id` and `order_id` indexes for audit lookup, but those are not replay
keys.

## Write Path

Phase 1 should be simple and explicit:

1. `exchange-engine` assigns `matching_sequence` before publishing any order/trade
   output.
2. It writes the event into a local append-only WAL segment.
3. It publishes the event to Aeron after the WAL append succeeds.
4. `pg-writer` consumes the same event stream and persists it into
   `matching_events` asynchronously.

This keeps PostgreSQL out of the live path while making the local WAL the immediate
durability source.

## Replay Path

On restart:

1. Load the latest durable snapshot for the engine/symbol if present.
2. Replay WAL events after the snapshot sequence.
3. Compare replayed tail sequence with `matching_events` for audit consistency.
4. Start Aeron publishers only after the in-memory book/account projection reaches
   the expected sequence.

If the local WAL is missing or corrupt, recovery may fall back to PostgreSQL
`matching_events`, but that is a cold recovery path, not a latency path.

## HA Model

The first HA version should be active/passive per symbol:

- Active engine owns matching and emits WAL + Aeron events.
- Passive engine tails the active WAL/event stream and replays deterministically.
- Passive must expose `last_replayed_sequence`.
- Failover is allowed only when passive has caught up to active's committed
  sequence.

Do not attempt active/active matching. It adds split-brain risk and does not match
the single-writer order book design.

## Implementation Order

1. Add `matching_sequence` to engine outputs and `matching_events`.
2. Add a local binary WAL writer/reader with segment rotation and fsync policy.
3. Teach `exchange-engine` startup to rebuild from WAL.
4. Add replay tests that reconstruct book state from events.
5. Add passive replay process for one symbol.
6. Add failover tooling and operator runbook.

## Open Decisions

- WAL fsync policy: every event, every batch, or time-bounded group commit.
- Snapshot cadence: sequence interval, time interval, or shutdown-only.
- Whether account/risk replay uses the same event stream or a parallel risk event
  stream linked by matching sequence.
- Whether `matching_events` stores raw SBE bytes, typed columns, or both.
