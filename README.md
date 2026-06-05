<p align="center">
  <img src="logo.svg" width="80" height="80" alt="LightningX Logo" />
</p>

# LightningX Exchange

A high-performance crypto exchange built in Rust. The matching engine sustains **6–9M orders/sec** on a single core, and the production WebSocket hot path uses **SBE binary messages end-to-end** for private order flow. In the latest 40K-connection local pressure run, accepted-order p50 was **172–176 µs**.

Live demo with very limited resources and a very very simple market making bot: **https://www.lightningx.global**

---

## Latency

![LightningX end-to-end latency on M4 Mac](lightningx-latency-m4.jpg)

*Internal processing latency: server receives WS frame → Aeron IPC → matching engine → Aeron IPC → result queued for sending. Measured via beacon tracing on an M4 MacBook Pro (14-core) at **40K concurrent WebSocket connections (4 desks × 10K)**. **p50 = 20 µs · p90 = 74 µs.** Network latency (Internet RTT) is not included and dominates user-perceived latency in practice. Further gains are expected on Linux with dedicated core pinning.*

---

## Architecture

```
Browser / API client
    │  HTTPS / WSS
    ▼
nginx  (TLS termination, reverse proxy)
    │
    ├─ private /api + /ws  ──►  desk-server  (counter/private gateway)
    ├─ public  /ws         ──►  market-data-gateway
    └─ slow REST /api      ──►  lightning-data
                            │
                            │  Aeron IPC (lock-free ring buffer)
                            ▼
                   exchange-engine  (matching engine, one thread per symbol)
                      SkipList order book · SBE binary messages
                            │
                            ├─ order_update  ──►  desk-server  ──►  private WS push
                            ├─ depth / trades ──►  market-data-gateway  ──►  public WS broadcast
                            └─ metrics  ──►  beacon  ──►  VictoriaMetrics
                                                                         │
                            PostgreSQL  ◄──  writers / lightning-data  Grafana
```

`desk-server` is intentionally private-only by default. Set `DESK_PUBLIC_MARKET_DATA=1`
only for legacy local runs that still need public market-data subscriptions on the desk
process.

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
| Counter sharding | `user_id % 16` owner shard | Each desk owns private state for a deterministic user shard |
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
| GET | `/api/accounts` | JWT | Asset balances |
| GET | `/api/orders` | JWT | Open and past orders |
| POST | `/api/orders` | JWT | Place order |
| DELETE | `/api/orders/:id` | JWT | Cancel order |
| GET | `/api/trades` | JWT | User trades |
| GET | `/api/tickers` | — | 24h ticker stats |

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

Measured from the moment the server receives a WS frame to the moment the result is queued for sending back — **excluding network transit in both directions**. Captured via beacon / HDR histogram tracing on an Apple M4 MacBook Pro at 40K concurrent WebSocket connections (4 desks × 10K, `DESK_SPIN=true`, `TRACER_ENABLED=1`).

| Metric | P50 | P90 | P99 |
|---|---|---|---|
| **Internal processing (40K conns, M4 14-core)** | **20 µs** | **74 µs** | **709 µs** |

> Internet RTT (20–200 ms typical) is not included. User-perceived latency is dominated by network, not by exchange processing.

Stage-by-stage breakdown (steady-state):

| Stage | P50 | P90 |
|---|---|---|
| WS frame decode → Aeron publish | 2 µs | 4 µs |
| Aeron IPC transit (desk → engine) | < 1 µs | 1 µs |
| Engine matching | 1 µs | 3 µs |
| Result publish (engine → Aeron) | 1 µs | 3 µs |
| Aeron IPC transit (engine → desk) | < 1 µs | 1 µs |
| Desk recv → result queued for sending | 2 µs | 4 µs |

The matching step is ~1 µs; the remaining cost is the two Aeron IPC hops and WS frame handling at each end.

### WebSocket Scalability (desk-server, macOS M4 Pro 14-core)

Measured with `pressure-client` using SBE binary private order frames. Runs are local macOS pressure tests, so the 100K result is mostly a scheduler/WS-queue capacity signal, not a matching-engine limit.

| Connections | Desks | Conn success | Place success | Place OK p50 | Place OK p90 | Place OK p99 |
|---|---|---:|---:|---:|---:|---:|
| **40K** | 4 × 10K | **100%** | **100%** | **172–176 µs** | **1345–1387 µs** | **8.9–9.1 ms** |
| **100K** | 3 × ~33K | ~94.7% | ~73–74% | **360–368 µs** | **161–178 ms** | 1 s cap |

40K latest run: `/tmp/lightning-40000-4desk-20260605-142427`.
100K latest run: `/tmp/lightning-100000-3desk-20260605-184213`.

At 100K on this 14-core Mac, accepted-order p50 remains below 600 µs, but success rate and tail latency collapse once the WS/command queues saturate. The matching step itself stays ~1 µs; production 100K targets require Linux core isolation/pinning and enough desk shards to keep each counter below its queueing knee.

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

---

## License

Non-commercial use is free under [AGPL-3.0](LICENSE) — derivative works must be open-sourced under the same terms.

Commercial use requires a separate license agreement. Contact **lightningx.global@gmail.com**.

---

## Disclaimer

> **This platform is for demonstration purposes only. Do NOT use it with real assets.**
