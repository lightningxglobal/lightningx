# Lightning Exchange — Backend

High-performance crypto exchange backend written in Rust. Targets 6–9M TPS on a single core using a lock-free SkipList order book, SBE binary encoding, and Aeron transport.

---

## Quick Start (dev mode)

Prerequisites: PostgreSQL on `localhost:5432`, Redis on `localhost:6379` (Docker containers already running).

```bash
# Copy environment file
cp .env.example .env

# Run database migrations and start the API server
cargo run
```

Frontend (separate terminal):

```bash
cd ../exchange-frontend
npm install
npm run dev          # Vite dev server on http://localhost:5173
```

---

## Docker Start (full stack)

```bash
# From /Users/alphawu/work/rs/matching
docker-compose up -d
```

Services started:

| Service            | URL                        |
|--------------------|----------------------------|
| exchange-api       | http://localhost:3000      |
| exchange-frontend  | http://localhost:4001       |
| PostgreSQL         | localhost:5432              |
| Redis              | localhost:6379              |

Stop everything:

```bash
docker-compose down
```

Destroy data volumes too:

```bash
docker-compose down -v
```

---

## Environment Variables

| Variable       | Default                                       | Description                        |
|----------------|-----------------------------------------------|------------------------------------|
| `DATABASE_URL` | `postgres://user:password@localhost:5432/mydb`| PostgreSQL connection string        |
| `REDIS_URL`    | `redis://localhost:6379`                      | Redis connection string             |
| `JWT_SECRET`   | `change_me_in_production`                     | Secret key for JWT signing          |
| `PORT`         | `3000`                                        | HTTP listen port                    |
| `HOST`         | `0.0.0.0`                                    | HTTP bind address                   |
| `RUST_LOG`     | `info`                                        | Log level (`trace/debug/info/warn`) |

Copy `.env.example` to `.env` for local development. In production, inject these via your secrets manager — never commit `.env`.

---

## API Endpoint Reference

Base URL: `http://localhost:3000`

### Auth

| Method | Path                   | Body                                      | Description            |
|--------|------------------------|-------------------------------------------|------------------------|
| POST   | `/api/auth/register`   | `{ username, email, password }`           | Create account         |
| POST   | `/api/auth/login`      | `{ email, password }`                     | Login, returns JWT     |

### User Profile

| Method | Path                   | Auth | Description             |
|--------|------------------------|------|-------------------------|
| GET    | `/api/user/profile`    | JWT  | Fetch own profile       |
| PATCH  | `/api/user/profile`    | JWT  | Update profile fields   |

### KYC

| Method | Path       | Auth | Description            |
|--------|------------|------|------------------------|
| POST   | `/api/kyc` | JWT  | Submit KYC documents   |

### Accounts / Balances

| Method | Path             | Auth | Description              |
|--------|------------------|------|--------------------------|
| GET    | `/api/accounts`  | JWT  | List all asset balances  |
| GET    | `/api/balances`  | JWT  | Alias for `/api/accounts`|

### Orders

| Method | Path                      | Auth | Description            |
|--------|---------------------------|------|------------------------|
| GET    | `/api/orders`             | JWT  | List open/past orders  |
| POST   | `/api/orders`             | JWT  | Place a new order      |
| GET    | `/api/orders/:order_id`   | JWT  | Get single order       |
| DELETE | `/api/orders/:order_id`   | JWT  | Cancel an order        |

Place order body:

```json
{
  "symbol": "BTC-USDT",
  "side": "buy",
  "type": "limit",
  "price": "65000.00",
  "quantity": "0.01"
}
```

Supported `type` values: `limit`, `market`, `ioc`, `fok`, `post_only`.

### Trades & Tickers

| Method | Path           | Auth | Description                 |
|--------|----------------|------|-----------------------------|
| GET    | `/api/trades`  | —    | Recent trade history        |
| GET    | `/api/tickers` | —    | 24h ticker stats per symbol |

### WebSocket

Connect to `ws://localhost:3000/ws` for real-time market data (order book depth, trades, ticker).

---

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                      Clients / Browser                   │
└────────────┬────────────────────────┬────────────────────┘
             │ REST (HTTP/1.1)         │ WebSocket
             ▼                         ▼
┌────────────────────────────────────────────────────────┐
│                   exchange-frontend                     │
│            Vue 3 + Vite → nginx (port 4001)            │
│      /api/* → proxy → exchange-api:3000                │
│      /ws    → proxy → exchange-api:3000                │
└──────────────────────┬─────────────────────────────────┘
                       │
                       ▼
┌────────────────────────────────────────────────────────┐
│                    exchange-api (Rust)                  │
│   axum HTTP + WebSocket server (port 3000)             │
│                                                        │
│  ┌──────────────┐  ┌──────────────┐  ┌─────────────┐  │
│  │  Auth / JWT  │  │  Order API   │  │ Market Data │  │
│  └──────────────┘  └──────┬───────┘  └──────┬──────┘  │
│                            │                  │         │
│              ┌─────────────▼──────────────────▼──────┐ │
│              │         Matching Engine                │ │
│              │  SkipList order book (6–9M TPS)        │ │
│              │  SBE encoding · Aeron transport        │ │
│              └───────────────────────────────────────┘ │
└───────────┬────────────────────────────────────────────┘
            │
     ┌──────┴───────┐
     │              │
     ▼              ▼
┌─────────┐   ┌──────────┐
│PostgreSQL│   │  Redis   │
│ (state)  │   │ (cache)  │
└─────────┘   └──────────┘
```
