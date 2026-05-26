# AGENTS.md

## Scope

These instructions apply to this repository: `matching` / `lightning-exchange`.

## Project Summary

LightningX is a Rust 2021 crypto exchange demo. The core matching engine targets very low latency and uses a SkipList order book, preallocated pools, `SmallVec`, SBE binary messages, `rtrb` ring buffers, Aeron IPC/UDP transport, axum WebSocket/REST gateways, PostgreSQL persistence via `sqlx`, and Redis support.

Primary binaries:
- `exchange-engine`: Aeron-backed matching engine, usually one thread per symbol.
- `desk-server`: REST/WebSocket gateway, auth, order routing, balances, and real-time pushes.
- `exchange-server`: older/in-process API server from `src/main.rs`.
- `lightning-data`: data API service.
- `kline-service`: trade-to-OHLCV aggregator.
- `market-maker` and `trade-bot`: demo/market data flow helpers.

## Working Rules

- Keep changes surgical. Hot-path files such as `src/engine.rs`, `src/skiplist.rs`, `src/pools.rs`, `src/orderbook*.rs`, `src/sbe.rs`, and `src/aeron_transport.rs` should not receive broad refactors unless requested.
- Preserve the existing performance-oriented style: avoid heap allocation, locks, string work, or dynamic dispatch in matching/encoding paths unless the change explicitly requires it.
- Prefer fixed-size or stack-backed buffers already used in the project (`SmallVec`, pools, fixed SBE structs) over new allocation-heavy structures.
- Match existing module boundaries. Core matching belongs in `engine`, `order`, `trade`, `orderbook`, `skiplist`, `pools`, and `market_data`; transport/encoding belongs in `sbe`, `aeron_transport`, `aeron_channels`, `order_update`, and `transport`; persistence/API work belongs in `db`, `models`, `api`, `ws_handler`, and `src/bin/*`.
- Add or update focused tests when changing matching behavior, order lifecycle behavior, SBE encoding, balance settlement, or transport routing.
- Do not edit generated or large runtime artifacts such as `server.log`, benchmark images, or local IDE files.
- Natural-language output, comments, commit messages, and docs must be English or Simplified Chinese only.

## Commands

Basic checks:

```bash
cargo fmt --check
cargo test
cargo test --lib
cargo test --bench matching_bench
cargo bench --bench matching_bench
```

Common local services:

```bash
docker compose up -d postgres redis
DATABASE_URL=postgres://user:password@localhost:5432/mydb cargo run --bin desk-server
DATABASE_URL=postgres://user:password@localhost:5432/mydb SYMBOLS=BTC_USDT cargo run --bin exchange-engine
DATABASE_URL=postgres://user:password@localhost:5432/mydb cargo run --bin lightning-data
DATABASE_URL=postgres://user:password@localhost:5432/mydb cargo run --bin kline-service
```

Aeron demo flow:

```bash
export AERON_DIR=/tmp/aeron
aeronmd &
bash scripts/demo_trading_system.sh
bash scripts/test_aeron_message_flow.sh
```

The repository defaults to PostgreSQL at `postgres://user:password@localhost:5432/mydb`. `docker-compose.yml` also starts Redis on `localhost:6379`.

## Environment Notes

- `DATABASE_URL` is required for API/data/engine binaries that touch persistence; most code defaults to `postgres://user:password@localhost:5432/mydb`.
- `JWT_SECRET` is used by API/data server paths; local examples often use `change_me_in_production`.
- `SYMBOLS` controls multi-symbol desk/engine routing, for example `BTC_USDT,ETH_USDT,SOL_USDT`.
- `AERON_DIR` defaults to `/dev/shm/aeron` on Linux and `/tmp/aeron` elsewhere. Existing scripts assume `/tmp/aeron`.
- `AERON_TRANSPORT=ipc` is the default. Set `AERON_TRANSPORT=udp` plus `ENGINE_HOST=<host>` for UDP topology.
- Per-symbol Aeron order streams are assigned in `src/aeron_channels.rs`; keep desk and engine mappings deterministic and synchronized.

## Aeron Gotchas

- Aeron subscribers must be polled explicitly. Calling only `client.do_work()` is not enough to trigger receive callbacks.
- Keep subscription handles alive for the entire receive loop. Do not bind them to throwaway names that drop before polling.
- Allow publisher/subscriber registration time before sending messages. Existing docs and scripts use warm-up loops or sleeps because early messages can be lost before registration completes.
- For local latency measurements, prefer IPC transport. UDP loopback on macOS has scheduler/network jitter that can distort results.

## Database And Migrations

- Migrations live in `migrations/*.sql` and are applied manually by `src/db.rs` with `sqlx::raw_sql(include_str!(...))`.
- When adding a migration, update `src/db.rs::run_migrations` in order.
- Avoid introducing SQLx compile-time query macros unless the repository is also changed to support the required offline metadata or live database during builds.

## Verification Expectations

- For documentation-only changes, no runtime test is required, but mention that tests were not run.
- For core matching changes, run at least `cargo test --lib`; add targeted tests in `src/boundary_tests.rs` or relevant module tests.
- For SBE changes, run SBE-focused tests from `src/sbe_tests.rs` and any transport tests affected by message size/layout.
- For API or persistence changes, run the relevant binary or tests with Postgres available; use `docker compose up -d postgres redis` if needed.
- For performance-sensitive changes, run `cargo bench --bench matching_bench` when feasible and report any notable throughput/latency change.

