<p align="center">
  <img src="logo.svg" width="80" height="80" alt="LightningX Logo" />
</p>

# LightningX Exchange

A high-performance crypto exchange built in Rust. The matching engine sustains **6–9M orders/sec** on a single core with sub-microsecond per-order latency, and the full server-side processing path (WS frame received → Aeron IPC → matching → Aeron IPC → WS frame sent) runs at **p50 = 20 µs** at 40K concurrent connections. The production WebSocket hot path uses **SBE binary messages end-to-end** for private order flow.

Live demo with very limited resources and a very very simple market making bot: **https://www.lightningx.global**

LightningX is an experimental low-latency trading infrastructure, where I explore exchange architecture, execution engines, market data distribution, risk systems, replication, persistence, and latency optimization. It is not intended to be a production exchange, but a collection of production-inspired components and design experiments.

---

## System Capabilities

LightningX is more than a matching-engine benchmark. The repository contains a working testnet-style exchange stack with the following subsystems:

| Area | Capability |
|---|---|
| Trading | Limit, market, IOC, FOK, post-only, reduce-only, batch place, cancel, batch cancel |
| Matching | Per-symbol single-threaded matching engines, deterministic symbol routing, restored resting orders |
| Private gateway | JWT auth, owner-sharded users, private order/account/position WebSocket pushes |
| Market data | Public trades, depth, ticker, kline, aggregate trades, Binance depth mirror market maker |
| Risk | Margin reservation, leverage rules, position limits, mark-price driven liquidation checks |
| Perpetual swaps | Positions, funding schedule/view, funding settlement persistence, trigger orders |
| Funds | Test funds, account balances, deposit credit API, two-phase withdrawal state machine |
| Operations | Admin trading halt/config, admin force-close, user token revoke/unrevoke, metrics endpoint |
| Persistence | PostgreSQL migrations, async pg-writer, Redis hot cache, append-style fund audit records |
| Recovery/HA experiments | Input/output journal helpers, journal replay/audit tools, engine snapshots |
| Observability | Beacon latency tracing, HDR histograms, VictoriaMetrics/Grafana integration notes |
| Load testing | Pressure client, Aeron demo scripts, 40K/100K/200K WebSocket connection test paths |

The current implementation is still a demo/testnet system. It is useful for studying low-latency exchange design and operational tradeoffs, not for custody or real-money trading.

---

## Latency

![LightningX end-to-end latency on M4 Mac](lightningx-latency-m4.jpg)

*Internal processing latency: server receives WS frame → Aeron IPC → matching engine → Aeron IPC → result queued for sending. Measured via beacon tracing on an M4 MacBook Pro (14-core) at **40K concurrent WebSocket connections (4 desks × 10K)**. **p50 = 20 µs · p90 = 74 µs.** Network latency (Internet RTT) is not included and dominates user-perceived latency in practice. Further gains are expected on Linux with dedicated core pinning.*

---

## Architecture

```
External clients
  +-- Browser / API client
  +-- market-maker / trade-bot / pressure-client
  +-- chain watcher or admin tooling
        |
        | HTTPS / WSS
        v
nginx / load balancer
        |
        +-- private /api + /ws
        |      v
        |   desk-server
        |      +-- auth, owner-sharded user sessions, private WS pushes
        |      +-- risk engine: margin, positions, liquidations, funding view
        |      +-- trigger orders, admin controls, deposit/withdraw APIs
        |      +-- Redis hot cache writer/reader path
        |      +-- Aeron command publish
        |             |
        |             v
        |   exchange-engine
        |      +-- one spin-loop matching thread per symbol
        |      +-- SkipList order book, fixed-point ticks/lots
        |      +-- SBE command/update frames
        |      +-- depth/trade/order-update/event streams
        |             |
        |             +-- order_update -------> desk-server -------> private WS
        |             +-- depth / trades -----> market-data-gateway -> public WS
        |             +-- persist events -----> pg-writer / redis-writer
        |             +-- journal / snapshots -> replay and audit tools
        |             +-- latency beacons ----> beacon -> VictoriaMetrics -> Grafana
        |
        +-- public /ws
        |      v
        |   market-data-gateway
        |      +-- trades, depth, ticker, kline, aggregate trade fanout
        |
        +-- slow REST /api
               v
            lightning-data
               +-- historical tickers, klines, trades, orders, accounts, positions

PostgreSQL is the durable store for users, orders, balances, positions, trades,
funding, withdrawals, audits, and snapshots. Redis is the low-latency cache for
hot account/order reads and downstream convergence.
```

`desk-server` is intentionally private-only by default. Set `DESK_PUBLIC_MARKET_DATA=1`
only for legacy local runs that still need public market-data subscriptions on the desk
process.

The compact owner-shard plan uses four counters: desk 0 for BTC, desk 1 for ETH,
and desks 2/3 for the remaining smaller symbols. Private account ownership stays
deterministic via `user_id % 4`.

```
Browser / API client
    │  private order flow
    ▼
desk-server
    │
    │  Aeron IPC (lock-free ring buffer)
    ▼
exchange-engine
```

**Key design choices:**

| Layer | Technology | Why |
|---|---|---|
| Order book | Lock-free SkipList | O(log N) insert/cancel, cache-friendly, 6–9M TPS |
| Transport | Aeron IPC | Zero-copy ring buffer, sub-microsecond publish latency |
| Encoding | SBE (Simple Binary Encoding) | Fixed-size, no allocation, 16–72 bytes per message |
| API server | axum + tokio | Async, zero-cost WS fan-out to thousands of clients |
| Counter sharding | `user_id % 4` owner shard | Four compact counters: BTC, ETH, and two altcoin groups |
| Market data | diff-based Binance mirror | Only cancel/place changed price levels, ~20× less traffic |

---

## Components

| Binary | Description |
|---|---|
| `exchange-engine` | Matching engine: one spin-loop thread per symbol, restores resting orders from DB on startup |
| `desk-server` | Private counter gateway: auth, owner-sharded order routing, private order/account pushes, balance/risk state |
| `market-data-gateway` | Public live market-data WebSocket: trades, depth, ticker, kline, aggregate trade fanout |
| `lightning-data` | Slow/cold REST data service: historical tickers, klines, trades, orders, accounts, positions from PostgreSQL |
| `market-maker` | Mirrors Binance top-20 depth into LightningX via diff-based order management |
| `kline-service` | Aggregates trades from Aeron into OHLCV candles and persists to PostgreSQL |
| `trade-bot` | Demo user that places random orders to keep the book active |
| `beacon` | Reads HDR histogram latency traces from the engine, pushes `latency_us` metrics to VictoriaMetrics |
| `pg-writer` | Consumes compact persistence frames and writes orders, trades, balances, positions, funding, and risk state to PostgreSQL |
| `redis-writer` | Mirrors hot state into Redis for low-latency reads and recovery convergence |
| `pressure-client` | WebSocket load generator used for connection-count and order-ack latency tests |
| `journal-replicate` | Replicates journal streams for HA/replay experiments |
| `journal-audit` | Audits journal output for sequence and replay consistency checks |

---

## Order Types

| Type | Behaviour |
|---|---|
| `limit` (GTC) | Rests in the book until filled or cancelled |
| `market` | Fills against the best available price, IOC semantics |
| `ioc` | Fills immediately, cancels remaining |
| `fok` | Fill-or-kill: all-or-nothing |
| `post_only` | Rejected if it would immediately match (maker only) |

---

## Quick Start (local dev)

Prerequisites: Rust stable, PostgreSQL on `localhost:5432`, Aeron media driver running.

```bash
# Run database migrations and start desk-server
DATABASE_URL=postgres://user:password@localhost:5432/mydb \
cargo run --bin desk-server

# Public live market data WebSocket, default port 4010
cargo run --bin market-data-gateway

# Slow/cold REST data service, default port 4002
DATABASE_URL=postgres://user:password@localhost:5432/mydb \
cargo run --bin lightning-data

# In a separate terminal — start the matching engine
DATABASE_URL=postgres://user:password@localhost:5432/mydb \
SYMBOLS=BTC_USDT \
cargo run --bin exchange-engine
```

Frontend (separate repo — Vue 3 + Vite):

```bash
cd ../exchange-frontend
npm install
npm run dev          # http://localhost:5173
```

---

## WebSocket API

Connect to `wss://<host>/ws`.

**Production private order flow is SBE binary both ways.** Every binary WebSocket frame begins with a 1-byte `msg_type`, followed by a fixed-layout little-endian payload. Legacy JSON/text parsing may still exist for compatibility and non-hot-path local tools, but production clients should use the binary frames below.

Public market data is served by `market-data-gateway`, not by `desk-server`.

### Client → Server (SBE binary)

See `src/transport/ws_sbe.rs` for exact layouts and helpers.

| `msg_type` | Name | Size | Key fields |
|---|---|---:|---|
| 50 | `CLIENT_PLACE_ORDER` | 37 B | `client_order_id: u64`, `symbol: [u8;8]`, `side: u8`, `tif: u8`, `price_ticks: i64`, `quantity_lots: i64` |
| 51 | `CLIENT_CANCEL_ORDER` | 9 B | `order_id: i64` |
| 52 | `CLIENT_PING` | 1 B | none |

`side`: `0=Buy`, `1=Sell`. `tif`: `0=GTC`, `1=IOC`, `2=FOK`, `3=PostOnly`. `price_ticks=0` means market/IOC-style execution.

### Server → Client (SBE binary)

All frames: `[msg_type: u8][payload...]`. See `src/transport/ws_sbe.rs` for exact byte layouts.

**Unicast (sent to the requesting client only):**

| `msg_type` | Name | Key fields |
|---|---|---|
| 1 | `AUTH_OK` | `user_id: i64` |
| 2 | `AUTH_ERROR` | `reason: str` |
| 3 | `ORDER_ACCEPTED` | `order_id: u64`, `client_order_id: u64` |
| 4 | `ORDER_REJECTED` | `client_order_id: u64`, `reason: str` |
| 5 | `ORDER_SUBMITTED` | `order_id: u64`, `client_order_id: u64`, `ts: u64` |
| 6 | `ORDER_UPDATE` | `order_id`, `status: u8`, `filled_qty: f64`, `price: f64` |
| 7 | `CANCEL_SUBMITTED` | `order_id: u64`, `ts: u64` |
| 8 | `BALANCE_UPDATE` | `asset: [u8;8]`, `available: f64`, `frozen: f64` |
| 9 | `POSITION_UPDATE` | `symbol: [u8;8]`, `qty: f64`, `avg_price: f64` |
| 13 | `ERROR_MSG` | `message: str` |

**Public market-data broadcast (`market-data-gateway`):**

| `msg_type` | Name | Key fields |
|---|---|---|
| 20 | `TRADE` | `price: f64`, `qty: f64`, `side: u8`, `ts: u64`, `symbol: [u8;8]` |
| 21 | `DEPTH` | `symbol: [u8;8]`, N bid/ask levels (`price: f64`, `qty: f64`) |
| 22 | `TICKER` | `symbol: [u8;8]`, `open/high/low/close/vol: f64`, `ts: u64` |
| 23 | `KLINE` | interval, OHLCV, `ts: u64` |
| 24 | `AGG_TRADE` | interval aggregate trade bucket |

`ORDER_UPDATE` status byte: `0=OPEN  1=PARTIAL_FILL  2=FILLED  3=CANCELED  4=REJECTED`

---

## REST API

| Method | Path | Auth | Description |
|---|---|---|---|
| POST | `/api/auth/register` | — | Create account |
| POST | `/api/auth/login` | — | Login, returns JWT |
| POST | `/api/auth/refresh` | JWT | Refresh auth token |
| GET/PATCH | `/api/user/profile` | JWT | Read or update user profile |
| PATCH | `/api/user/password` | JWT | Change password |
| POST | `/api/kyc` | JWT | Submit KYC-style profile data for the demo system |
| GET | `/api/accounts` | JWT | Asset balances |
| GET | `/api/balances` | JWT | Alias for account balances |
| GET | `/api/orders` | JWT | Open and past orders |
| POST | `/api/orders` | JWT | Place order |
| DELETE | `/api/orders` | JWT | Cancel all open orders for the authenticated user |
| GET | `/api/orders/:order_id` | JWT | Get one order |
| DELETE | `/api/orders/:order_id` | JWT | Cancel one order |
| GET | `/api/trades` | JWT | User trades |
| GET | `/api/positions` | JWT | Current perpetual-swap positions |
| GET | `/api/funding` | — | Funding state for a symbol |
| GET/POST | `/api/trigger-orders` | JWT | List or create trigger orders |
| DELETE | `/api/trigger-orders/:id` | JWT | Cancel trigger order |
| GET/POST | `/api/withdrawals` | JWT | List or request withdrawals |
| POST | `/api/deposit/credit` | Service token | Idempotent deposit credit from a chain watcher |
| POST | `/api/withdrawals/:id/status` | Service token | Advance withdrawal status after chain-service actions |
| POST | `/api/admin/config` | Admin token | Halt/resume trading and update symbol config |
| GET | `/api/admin/config` | Admin token | Read symbol config |
| POST | `/api/admin/force-close` | Admin token | Force-close a user position |
| POST | `/api/admin/revoke` | Admin token | Revoke a user's outstanding tokens |
| POST | `/api/admin/unrevoke` | Admin token | Restore a user's login ability |
| GET | `/api/tickers` | — | 24h ticker stats |
| GET | `/api/klines` | — | Historical OHLCV candles |
| POST | `/api/test-funds` | JWT | Add sandbox funds to the authenticated user |
| POST | `/api/robot-funds` | Robot JWT | Fund the market-maker service account |
| POST | `/api/seed-demo` | — | Seed demo data for local/testnet runs |
| POST | `/api/debug/mark-price` | Test-only | Force mark price when `TEST_ENDPOINTS=1` |
| GET | `/metrics` | — | Process metrics |

See [`docs/api/README.md`](docs/api/README.md) and [`docs/api/openapi.yaml`](docs/api/openapi.yaml) for the testnet-oriented API guide and machine-readable schema.

---

## Performance

All benchmarks run on an Apple M4 MacBook Pro, single core, in-process (no network hop).

### Matching Engine Throughput

| Scenario | Throughput | Notes |
|---|---|---|
| Single GTC limit order | **5.6M orders/sec** | Realistic mix: ~50% match, ~50% insert |
| Batch (20 orders) | **8.6M orders/sec** | Primary path for high-frequency flow |
| Market order (IOC) | **6.1M orders/sec** | Eats all available levels, never rests |
| Deep book — single (400 levels) | **8.4M orders/sec** | O(log N) unaffected by book depth |
| Deep book — batch (400 levels) | **21M orders/sec** | Cache locality maximised in batch |

### Matching Engine Latency (per order, in-process)

| Metric | Single order | Batch (20 orders) |
|---|---|---|
| P50 | **84 ns** | **68 ns** |
| P99 | **1 042 ns** | **733 ns** |

Latency is measured from `place_order()` call to result return, including order book traversal,
price matching, and market data snapshot generation. No allocations on the hot path —
all buffers are pre-allocated (`SmallVec`, arena SkipList, rtrb ring buffers).

### Internal Processing Latency

Measured from the moment the server **receives** the client's WS frame to the moment the response WS frame **has been written to the socket** — the complete exchange-side processing, network transit excluded. Captured via beacon / HDR histogram tracing (`TRACER_ENABLED=1`) on an Apple M4 MacBook Pro at 40K concurrent WebSocket connections (4 desks × 10K, `DESK_SPIN=true`).

| Metric | P50 | P90 | P99 | P999 |
|---|---|---|---|---|
| **Apple M4 MacBook Pro (40K conns, 14-core)** | **20 µs** | **74 µs** | **709 µs** | — |
| **AMD Ryzen 9 7945HX Ubuntu (40K conns, isolated CPUs)** | **42 µs** | **68 µs** | **129 µs** | **239 µs** |

> Internet RTT (20–200 ms typical) is not included. User-perceived latency is dominated by network, not by exchange processing.
>
> AMD p50 is ~2× M4 due to Infinity Fabric overhead on Aeron IPC shared memory (~0.8 µs per hop × 2 hops). AMD p99/p999 is significantly better because dedicated isolated spin threads (`isolcpus`) eliminate OS scheduler jitter.

Stage-by-stage breakdown (steady-state):

| Stage | P50 | P90 |
|---|---|---|
| WS frame decode → Aeron publish | 2 µs | 4 µs |
| Aeron IPC transit (desk → engine) | < 1 µs | 1 µs |
| Engine matching | 1 µs | 3 µs |
| Result publish (engine → Aeron) | 1 µs | 3 µs |
| Aeron IPC transit (engine → desk) | < 1 µs | 1 µs |
| Desk recv → WS frame written to socket | 2 µs | 4 µs |

The matching step is ~1 µs; the remaining cost is the two Aeron IPC hops and WS frame handling at each end. Network transit (Internet RTT, typically 20–200 ms) is the exchange's boundary — once the frame leaves the NIC, latency is the client's network, not ours.

### Connection Scalability (Ubuntu Linux, AMD Ryzen 9 7945HX)

The table below is a **connection load test**, not a latency benchmark. Its purpose is to verify that the system handles N concurrent WebSocket connections with zero order failures. The internal processing latency (the metric that matters for exchange performance) is measured separately via beacon tracing — see the "Internal Processing Latency" section above.

Test machine: Ubuntu 26.04, AMD Ryzen 9 7945HX (16C/32T, 5.2 GHz boost, 55 W TDP), 64 GB DDR5. Loopback interface with 32 source IP aliases (`127.0.0.2`–`.33`). `isolcpus=0-11` reserved for aeronmd (CPU 0), exchange-engine (CPU 2), and desk spin threads (CPU 4, 6, 8, 10, 3, 1, 5, 7, 9, 11). 2 MB huge pages enabled (`vm.nr_hugepages=512`, 1 GB reserved) with `AERON_USE_HUGE_PAGES=1` to reduce TLB pressure on Aeron IPC shared-memory buffers.

| Connections | Desks | Conns/desk | Conn success | Order OK% | Client p50 | Client p90 | Client p99 |
|---|---|---:|---:|---:|---:|---:|---:|
| **40K**  | 4  | 10K | **100%** | **100%** | **93 µs**  | **154 µs** | **244 µs** |
| **100K** | 4  | 25K | **100%** | **100%** | **112 µs** | **205 µs** | **1.2 ms** |
| **200K** | 8  | 25K | **100%** | **100%** | **291 µs** | **947 µs** | **4.7 ms** |

Client latency = loopback RTT (place order → acknowledgement received by pressure client). Internal server-side latency is lower; see "Internal Processing Latency" above.

> **200K note:** 8 desk-servers exhaust the 10 isolated CPUs on this laptop (2 desks share SMT pairs with the engine and aeronmd cores). The higher p90/p99 reflects CPU contention on this constrained test machine, not a design ceiling. A production server with 64+ dedicated cores would scale linearly.

---

### What Makes It Fast

| Technique | Effect |
|---|---|
| **Lock-free SkipList** — O(log N) insert/cancel | No contention on the hot path; 27% faster than `BTreeMap` |
| **Arena allocation** — nodes owned by a `Vec<Box<Node>>`, linked via raw pointers | Zero heap allocation during order placement or cancellation |
| **SmallVec** everywhere — stack-allocated for ≤ N fills | Eliminates `Vec` heap churn in the matching loop |
| **rtrb ring buffer** — SPSC, cache-line aligned, zero-copy | Lock-free event fan-out to market data and order update channels |
| **SBE encoding** — fixed-size binary, 16–72 bytes/message | No serialisation overhead; direct `memcpy` into Aeron publication buffer |
| **Spin-loop matching thread** — one dedicated OS thread per symbol | No tokio scheduler jitter; sub-100 ns consistent latency |
| **Batch API** — up to 20 orders per call | Amortises function-call and cache-miss overhead across orders |
| **2 MB huge pages** — `vm.nr_hugepages=512`, `AERON_USE_HUGE_PAGES=1` | Reduces TLB misses on Aeron IPC shared-memory buffers; lowers IPC RTT on AMD |

---

## License

Non-commercial use is free under [AGPL-3.0](LICENSE) — derivative works must be open-sourced under the same terms.

Commercial use requires a separate license agreement. Contact **lightningx.global@gmail.com**.

---

## Disclaimer

> **This platform is for demonstration purposes only. Do NOT use it with real assets.**
