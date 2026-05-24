# Market Maker — Binance Depth Mirror

**Goal**: A standalone `market-maker` binary that subscribes to Binance's real-time
depth WebSocket and continuously places / replaces limit orders on LightningX so the
order book always reflects real market prices and has visible liquidity.

---

## Architecture

```
Binance WSS (depth20@500ms)
       │  JSON snapshot every 500 ms
       ▼
┌─────────────────────────────────────────────────┐
│  market-maker  (src/bin/market_maker.rs)         │
│                                                  │
│  per symbol task:                                │
│    1. parse bids / asks from Binance snapshot    │
│    2. cancel tracked open orders via REST        │
│    3. place new limit orders via REST            │
│    4. store new order IDs                        │
└─────────────────────────────────────────────────┘
       │  REST  (localhost:4001)
       ▼
exchange-server  →  matching engine
```

The market maker is a **separate OS process**. It never touches engine memory
directly — all interaction is through the existing REST API. No TPS regression
on the matching engine.

---

## Symbols

| Our symbol   | Binance stream          | Qty scale | Depth levels |
|-------------|------------------------|-----------|--------------|
| ETH_USDT    | ethusdt@depth20@500ms  | 2%        | 10           |
| BTC_USDT    | btcusdt@depth20@500ms  | 2%        | 10           |
| SOL_USDT    | solusdt@depth20@500ms  | 2%        | 10           |

**Qty scale = 2%** of Binance quantity per level — keeps individual orders small so
real users always get filled without draining the robot's balance.

---

## Configuration (compile-time constants, easily moved to env vars later)

```
EXCHANGE_URL      = http://localhost:4001
ROBOT_EMAIL       = robot@lightningx.exchange
ROBOT_PASSWORD    = robot_secret_2026
DEPTH_LEVELS      = 10          # levels per side
QTY_SCALE         = 0.02        # fraction of Binance qty
SPREAD_EXTRA_BPS  = 5           # widen spread by +5 bps vs Binance mid
MAX_USDT_PER_SIDE = 5000.0      # safety cap: total USDT frozen per side
MIN_ORDER_QTY     = 0.0001      # skip dust orders
REFRESH_MS        = 500         # throttle: ignore Binance updates faster than this
```

---

## Implementation Steps

### Step 1 — Cargo.toml additions
```toml
reqwest = { version = "0.12", features = ["json", "rustls-tls"], default-features = false }

[[bin]]
name = "market-maker"
path = "src/bin/market_maker.rs"
```

### Step 2 — Robot account bootstrap
On startup the binary:
1. Tries `POST /api/auth/login` with robot credentials
2. If 401 (not registered): `POST /api/auth/register`, then login
3. Calls `POST /api/test-funds` to ensure robot has enough balance
4. Logs balance summary

### Step 3 — Per-symbol task
Each symbol runs in its own `tokio::spawn` task:
```
connect_binance_ws(stream_name)
  loop:
    msg = ws.next()
    depth = parse(msg)           // bids: Vec<(f64,f64)>, asks: Vec<(f64,f64)>
    cancel_all_robot_orders()    // DELETE /api/orders/:id for each tracked id
    clear tracked_ids
    place_bid_orders(depth.bids) // POST /api/orders
    place_ask_orders(depth.asks)
    store new order ids in tracked_ids
    sleep(REFRESH_MS) if needed
```

### Step 4 — Order placement logic
```
for each (price, binance_qty) in depth.bids.take(DEPTH_LEVELS):
    qty = binance_qty * QTY_SCALE
    if qty < MIN_ORDER_QTY: skip
    cost = price * qty
    if cumulative_cost > MAX_USDT_PER_SIDE: break
    POST /api/orders { symbol, side:"buy", order_type:"limit", price, quantity:qty }

for each (price, binance_qty) in depth.asks.take(DEPTH_LEVELS):
    qty = binance_qty * QTY_SCALE
    if qty < MIN_ORDER_QTY: skip
    POST /api/orders { symbol, side:"sell", order_type:"limit", price, quantity:qty }
```

### Step 5 — Error handling & reconnect
- REST 4xx on cancel (order already filled/canceled): log and continue, remove from tracked
- REST 4xx on place (insufficient balance): log warning, skip that level
- Binance WS disconnect: log, wait 5 s, reconnect (exponential backoff up to 60 s)
- Token expiry: re-login, retry

### Step 6 — Graceful shutdown
On SIGTERM / SIGINT:
1. Stop receiving new depth updates
2. Cancel all tracked orders
3. Exit

---

## Build & Run

```bash
# Build
CARGO_TARGET_DIR=/Users/alphawu/.cargo/global-target \
  cargo build --release --bin market-maker

# Run (exchange-server must be running on :4001)
CARGO_TARGET_DIR=/Users/alphawu/.cargo/global-target \
  ./target/release/market-maker

# Or with env overrides (future)
EXCHANGE_URL=http://localhost:4001 \
ROBOT_PASSWORD=custom_pass \
  ./market-maker
```

---

## Testing Checklist

- [ ] Robot registers + gets test funds on first run
- [ ] Orders appear in exchange order book (check via frontend)
- [ ] Prices match Binance current price (within 0.1%)
- [ ] On next Binance snapshot, old orders are canceled and new ones placed
- [ ] Frontend OrderBook shows 10 bid levels + 10 ask levels
- [ ] `cargo test` still passes (no regression)
- [ ] After 60 s runtime, robot balance is stable (not draining)
- [ ] Ctrl-C cancels all open robot orders cleanly

---

## Performance Guarantees

| Concern | Mitigation |
|---------|------------|
| Engine TPS | Market maker uses REST, same path as any user — no direct engine access |
| Latency spike | Cancel/place is 500 ms cadence, not hot path |
| Memory | Standalone process, zero shared memory with exchange-server |
| Balance drain | MAX_USDT_PER_SIDE cap + qty scale 2% |
| Order spam | Tracked IDs ensure old orders are always canceled before new ones placed |
