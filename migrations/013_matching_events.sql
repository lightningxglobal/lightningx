CREATE TABLE IF NOT EXISTS matching_events (
    sequence              BIGINT NOT NULL,
    response_stream_id    INTEGER NOT NULL DEFAULT 0,
    event_kind            SMALLINT NOT NULL,
    order_id              BIGINT NOT NULL,
    client_order_id       BIGINT NOT NULL DEFAULT 0,
    participant_id        BIGINT NOT NULL DEFAULT 0,
    counterparty_order_id BIGINT NOT NULL DEFAULT 0,
    symbol                VARCHAR(20) NOT NULL,
    price_ticks           BIGINT NOT NULL DEFAULT 0,
    quantity_lots         BIGINT NOT NULL DEFAULT 0,
    remaining_lots        BIGINT NOT NULL DEFAULT 0,
    ts_ns                 BIGINT NOT NULL,
    payload_version       SMALLINT NOT NULL DEFAULT 1,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (response_stream_id, sequence)
);

CREATE INDEX IF NOT EXISTS idx_matching_events_symbol_seq
    ON matching_events(symbol, sequence DESC);

CREATE INDEX IF NOT EXISTS idx_matching_events_order_id
    ON matching_events(order_id);
