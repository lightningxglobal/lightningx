<p align="center">
  <img src="logo.svg" width="80" height="80" alt="LightningX Logo" />
</p>

# LightningX Exchange

A high-performance crypto exchange built in Rust. The matching engine sustains **6–9M orders/sec** on a single core with sub-millisecond end-to-end latency.

Live demo: **https://www.lightningx.global**

---

## Latency

![LightningX end-to-end latency on M4 Mac](lightningx-latency-m4.jpg)

*Full system round-trip (client → nginx → desk-server → Aeron → matching engine → Aeron → desk-server → client) measured on an M4 MacBook Pro. p50 ≈ 42 μs, p99 ≈ 95 μs.*

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

### Subscribe to market data (no auth required)

```json
{ "type": "subscribe", "channels": ["depth.BTC_USDT", "trades.BTC_USDT", "ticker.BTC_USDT"] }
```

### Authenticate

```json
{ "type": "auth", "token": "<JWT>" }
```

### Place / cancel orders (requires auth)

```json
{ "type": "place_order", "symbol": "BTC_USDT", "side": "buy", "order_type": "limit", "price": 65000, "quantity": 0.01, "client_order_id": "my-id-1" }
{ "type": "cancel_order", "order_id": 12345, "client_order_id": "my-id-1" }
```

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

Round-trip path: **browser → nginx (TLS) → desk-server → Aeron IPC → matching engine → Aeron IPC → desk-server → WebSocket push → browser**

| Metric | Latency |
|---|---|
| P50 | **42 μs** |
| P99 | **95 μs** |

The dominant cost is two Aeron IPC round-trips (~5 μs each) plus two tokio scheduler wake-ups.
The matching step itself contributes only ~84 ns.

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
