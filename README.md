<p align="center">
  <img src="logo.svg" width="80" height="80" alt="LightningX Logo" />
</p>

# LightningX Exchange

A high-performance crypto exchange built in Rust. The matching engine sustains **6–9M orders/sec** on a single core with sub-millisecond end-to-end latency.

Live demo: **https://www.lightningx.global**

---

## Latency

![LightningX end-to-end latency on M4 Mac](lightningx-latency-m4.jpg)

*Full system round-trip (client → nginx → desk-server → Aeron → matching engine → Aeron → desk-server → client) measured on an M4 MacBook Pro. p50 ≈ 88 μs, p99 ≈ 215 μs.*

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
                        └─ metrics  ──►  beacon sidecar  ──►  VictoriaMetrics
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
| `beacon` | Latency sidecar: reads HDR histogram traces from engine, pushes `latency_us` to VictoriaMetrics |

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

Benchmarked on Apple M4 MacBook Pro (single core):

| Scenario | Throughput |
|---|---|
| Single limit orders (GTC) | ~5.3M orders/sec |
| Batch limit orders | ~7.3M orders/sec |
| Market orders (IOC) | ~5.1M orders/sec |
| Deep order book (400 levels) | ~21M orders/sec |

End-to-end latency (M4 localhost, p50/p99): **88 μs / 215 μs**

---

## License

Non-commercial use is free under [AGPL-3.0](LICENSE) — derivative works must be open-sourced under the same terms.

Commercial use requires a separate license agreement. Contact **lightningx.global@gmail.com**.

---

## Disclaimer

> **This platform is for demonstration purposes only. Do NOT use it with real assets.**
