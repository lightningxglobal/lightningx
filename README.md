<p align="center">
  <img src="logo.svg" width="80" height="80" alt="LightningX Logo" />
</p>

# LightningX Exchange

A high-performance crypto exchange built in Rust. The matching engine sustains **6–9M orders/sec** on a single core with **5 µs median end-to-end latency**.

Live demo with very limited resources and a very very simple market making bot: **https://www.lightningx.global**

---

## Latency

![LightningX end-to-end latency on M4 Mac](lightningx-latency-m4.jpg)

*Exchange-layer round-trip (WS receive → Aeron IPC → matching engine → Aeron IPC → WS send-ready) measured via beacon tracing on an M4 MacBook Pro. **p50 = 5 µs · p90 = 9 µs · p99 = 15 µs.***

---

## Architecture

```
Browser / API client
    │  HTTPS / WSS
    ▼
nginx  (TLS termination, reverse proxy)
    │
    ├─ /api/*  ──►  desk-server  (axum, HTTP + WebSocket)
    └─ /ws     ──►      │
                        │  Aeron IPC (lock-free ring buffer)
                        ▼
               exchange-engine  (matching engine, one thread per symbol)
                  SkipList order book · SBE binary messages
                        │
                        ├─ order_update  ──►  desk-server  ──►  WebSocket push
                        ├─ depth / trades ──►  desk-server  ──►  WebSocket broadcast
                        └─ metrics  ──►  beacon  ──►  VictoriaMetrics
                                                                     │
                        PostgreSQL  ◄──  desk-server (persistence)  Grafana
```

**Key design choices:**

| Layer | Technology | Why |
|---|---|---|
| Order book | Lock-free SkipList | O(log N) insert/cancel, cache-friendly, 6–9M TPS |
| Transport | Aeron IPC | Zero-copy ring buffer, sub-microsecond publish latency |
| Encoding | SBE (Simple Binary Encoding) | Fixed-size, no allocation, 16–72 bytes per message |
| API server | axum + tokio | Async, zero-cost WS fan-out to thousands of clients |
| Market data | diff-based Binance mirror | Only cancel/place changed price levels, ~20× less traffic |

---

## Components

| Binary | Description |
|---|---|
| `exchange-engine` | Matching engine: one spin-loop thread per symbol, restores resting orders from DB on startup |
| `desk-server` | WebSocket + REST gateway: auth, order routing, real-time pushes, balance management |
| `market-maker` | Mirrors Binance top-20 depth into LightningX via diff-based order management |
| `kline` | Aggregates trades from Aeron into OHLCV candles and persists to PostgreSQL |
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

**Wire format is asymmetric:**
- **Client → Server**: JSON text frames
- **Server → Client**: SBE binary frames — every frame begins with a 1-byte `msg_type`, followed by a fixed-layout payload (all integers little-endian)

### Client → Server (JSON text)

```json
// Subscribe to market data (no auth required)
{ "type": "subscribe", "channels": ["depth.BTC_USDT", "trades.BTC_USDT", "ticker.BTC_USDT"] }

// Authenticate
{ "type": "auth", "token": "<JWT>" }

// Place a limit order (requires auth)
{ "type": "place_order", "symbol": "BTC_USDT", "side": "buy", "order_type": "limit", "price": 65000, "quantity": 0.01, "client_order_id": "my-id-1" }

// Cancel an order
{ "type": "cancel_order", "order_id": 12345, "client_order_id": "my-id-1" }
```

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

**Broadcast (pushed to all subscribers of the relevant channel):**

| `msg_type` | Name | Key fields |
|---|---|---|
| 20 | `TRADE` | `price: f64`, `qty: f64`, `side: u8`, `ts: u64`, `symbol: [u8;8]` |
| 21 | `DEPTH` | `symbol: [u8;8]`, N bid/ask levels (`price: f64`, `qty: f64`) |
| 22 | `TICKER` | `symbol: [u8;8]`, `open/high/low/close/vol: f64`, `ts: u64` |
| 23 | `KLINE` | interval, OHLCV, `ts: u64` |

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
| GET | `/api/trades` | — | Recent trades |
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

### Full System End-to-End Latency

Round-trip path: **WS receive → Aeron IPC → matching engine → Aeron IPC → WS send-ready**  
Measured via beacon / HDR histogram tracing on an Apple M4 MacBook Pro.

| Metric | P50 | P90 | P99 |
|---|---|---|---|
| **Full RTT** | **5 µs** | **9 µs** | **15 µs** |

Stage-by-stage breakdown (steady-state, last-observed values):

| Stage | P50 | P90 |
|---|---|---|
| WS receive → Aeron publish | 2 µs | 4 µs |
| Aeron IPC transit (desk → engine) | < 1 µs | 1 µs |
| Engine matching | 1 µs | 3 µs |
| Result publish (engine → Aeron) | 1 µs | 3 µs |
| Aeron IPC transit (engine → desk) | < 1 µs | 1 µs |
| Desk processing (Aeron recv → WS ready) | 2 µs | 4 µs |

The matching step contributes only ~1 µs end-to-end; the dominant cost is the two Aeron IPC round-trips and tokio scheduler wake-ups at each WS boundary.

### WebSocket Scalability (desk-server, macOS M4 Pro 14-core)

Measured with `pressure-client` placing limit orders at 0.2 ops/s per connection over a 30 s steady-state window after a 60 s ramp. All connections share a single `BTC_USDT` symbol; market data broadcast enabled.

| Connections | Desks | Conn success | Order success | Place OK p50 |
|---|---|---|---|---|
| **40K** | 4 × 10K | **100%** | **100%** | **~178 µs** |
| **100K** | 3 × 33K | ~95–100% | ~77–91%† | **~325–560 µs** |

† 100K order success rate varies run-to-run (77–91%) because the machine operates near its CPU ceiling: 3 Aeron recv spin threads + 6 tokio workers + 1 engine thread ≈ 10 threads competing for 14 physical cores. p90 widens to ~30–84 ms and p99 reaches the 1 s timeout cap for some orders.

**On 32+ core Linux** with `sched_setaffinity` pinning each spin thread to a dedicated core, 100K at 100% success and p99 < 10 ms is achievable with the current architecture. See `docs/benchmark_40k_baseline.md` for full results.

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
