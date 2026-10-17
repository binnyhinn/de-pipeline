CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS clean;

CREATE TABLE IF NOT EXISTS raw.crypto_prices (
    id          SERIAL PRIMARY KEY,
    fetched_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    payload     JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS clean.crypto_prices (
    coin_id              TEXT NOT NULL,
    price_usd            NUMERIC NOT NULL,
    market_cap_usd       NUMERIC,
    volume_24h_usd       NUMERIC,
    price_change_24h_pct NUMERIC,
    fetched_at           TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (coin_id, fetched_at)
);