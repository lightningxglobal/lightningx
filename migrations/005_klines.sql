-- K-line (OHLCV candlestick) bars produced by kline_service.
-- open_time is Unix seconds aligned to the bar's interval start.
-- close_time = open_time + interval_secs - 1.
CREATE TABLE IF NOT EXISTS klines (
    symbol        VARCHAR(20)      NOT NULL,
    interval      VARCHAR(4)       NOT NULL,  -- '1m', '5m', '15m', '1h'
    open_time     BIGINT           NOT NULL,  -- Unix seconds
    open          DOUBLE PRECISION NOT NULL,
    high          DOUBLE PRECISION NOT NULL,
    low           DOUBLE PRECISION NOT NULL,
    close         DOUBLE PRECISION NOT NULL,
    volume        DOUBLE PRECISION NOT NULL,
    close_time    BIGINT           NOT NULL,  -- Unix seconds
    trade_count   BIGINT           NOT NULL DEFAULT 0,
    PRIMARY KEY (symbol, interval, open_time)
);

CREATE INDEX IF NOT EXISTS idx_klines_symbol_interval ON klines (symbol, interval, open_time DESC);
