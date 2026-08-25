CREATE TABLE market_events (
    event_id UUID PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    price NUMERIC(19,4) NOT NULL,
    volume BIGINT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_market_events_symbol
    ON market_events(symbol);

CREATE INDEX idx_market_events_timestamp
    ON market_events(timestamp);