-- Exchange core schema

CREATE TABLE IF NOT EXISTS users (
    id          BIGSERIAL PRIMARY KEY,
    email       VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    -- KYC fields
    full_name   VARCHAR(255),
    kyc_status  VARCHAR(20) NOT NULL DEFAULT 'NONE',  -- NONE, PENDING, APPROVED, REJECTED
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- One row per (user, asset). asset = 'USDT', 'BTC', 'ETH', etc.
CREATE TABLE IF NOT EXISTS accounts (
    id          BIGSERIAL PRIMARY KEY,
    user_id     BIGINT NOT NULL REFERENCES users(id),
    asset       VARCHAR(20) NOT NULL,
    balance     DOUBLE PRECISION NOT NULL DEFAULT 0,
    frozen      DOUBLE PRECISION NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(user_id, asset),
    CHECK (balance >= 0),
    CHECK (frozen >= 0),
    CHECK (frozen <= balance)
);

-- Orders. id = Snowflake ID from matching engine.
CREATE TABLE IF NOT EXISTS orders (
    id          BIGINT PRIMARY KEY,
    user_id     BIGINT NOT NULL REFERENCES users(id),
    symbol      VARCHAR(20) NOT NULL,          -- e.g. 'BTC_USDT'
    side        VARCHAR(4) NOT NULL,            -- 'buy' | 'sell'
    order_type  VARCHAR(10) NOT NULL,           -- 'limit' | 'market' | 'ioc' | 'fok' | 'post_only'
    price       DOUBLE PRECISION,               -- NULL for market orders
    quantity    DOUBLE PRECISION NOT NULL,
    filled      DOUBLE PRECISION NOT NULL DEFAULT 0,
    status      VARCHAR(20) NOT NULL DEFAULT 'PENDING',  -- PENDING|TRADING|COMPLETED|CANCELED|REJECTED
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_user_id   ON orders(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_symbol    ON orders(symbol);
CREATE INDEX IF NOT EXISTS idx_orders_status    ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_created   ON orders(created_at DESC);

-- Trades. Immutable record of every fill.
CREATE TABLE IF NOT EXISTS trades (
    id              BIGSERIAL PRIMARY KEY,
    symbol          VARCHAR(20) NOT NULL,
    buy_order_id    BIGINT NOT NULL REFERENCES orders(id),
    sell_order_id   BIGINT NOT NULL REFERENCES orders(id),
    price           DOUBLE PRECISION NOT NULL,
    quantity        DOUBLE PRECISION NOT NULL,
    buy_fee         DOUBLE PRECISION NOT NULL DEFAULT 0,
    sell_fee        DOUBLE PRECISION NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_trades_symbol    ON trades(symbol);
CREATE INDEX IF NOT EXISTS idx_trades_created   ON trades(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_trades_buy_order ON trades(buy_order_id);
CREATE INDEX IF NOT EXISTS idx_trades_sell_order ON trades(sell_order_id);
