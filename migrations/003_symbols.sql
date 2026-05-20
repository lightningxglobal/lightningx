-- Canonical list of tradeable symbols (populated by exchange-server on startup).
CREATE TABLE IF NOT EXISTS symbols (
    symbol      VARCHAR(20) PRIMARY KEY,
    base_asset  VARCHAR(10) NOT NULL,
    quote_asset VARCHAR(10) NOT NULL,
    active      BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
