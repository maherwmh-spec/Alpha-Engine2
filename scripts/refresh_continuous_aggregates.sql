
-- Idempotent TimescaleDB continuous aggregate refresh/bootstrap script.
-- Run after migrations or initial data load.

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

ALTER TABLE market_data.ohlcv
    ADD COLUMN IF NOT EXISTS source VARCHAR(100) DEFAULT 'unknown';

CREATE INDEX IF NOT EXISTS idx_ohlcv_source_time ON market_data.ohlcv (source, time DESC);

CREATE MATERIALIZED VIEW IF NOT EXISTS market_data.ohlcv_5m
WITH (timescaledb.continuous) AS
SELECT time_bucket('5 minutes', time) AS time, symbol, '5m'::varchar(10) AS timeframe, name,
       FIRST(open, time) AS open, MAX(high) AS high, MIN(low) AS low, LAST(close, time) AS close,
       SUM(volume) AS volume, 'aggregate'::varchar(100) AS source
FROM market_data.ohlcv WHERE timeframe = '1m'
GROUP BY time_bucket('5 minutes', time), symbol, name WITH NO DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS market_data.ohlcv_15m
WITH (timescaledb.continuous) AS
SELECT time_bucket('15 minutes', time) AS time, symbol, '15m'::varchar(10) AS timeframe, name,
       FIRST(open, time) AS open, MAX(high) AS high, MIN(low) AS low, LAST(close, time) AS close,
       SUM(volume) AS volume, 'aggregate'::varchar(100) AS source
FROM market_data.ohlcv WHERE timeframe = '1m'
GROUP BY time_bucket('15 minutes', time), symbol, name WITH NO DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS market_data.ohlcv_30m
WITH (timescaledb.continuous) AS
SELECT time_bucket('30 minutes', time) AS time, symbol, '30m'::varchar(10) AS timeframe, name,
       FIRST(open, time) AS open, MAX(high) AS high, MIN(low) AS low, LAST(close, time) AS close,
       SUM(volume) AS volume, 'aggregate'::varchar(100) AS source
FROM market_data.ohlcv WHERE timeframe = '1m'
GROUP BY time_bucket('30 minutes', time), symbol, name WITH NO DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS market_data.ohlcv_1h
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 hour', time) AS time, symbol, '1h'::varchar(10) AS timeframe, name,
       FIRST(open, time) AS open, MAX(high) AS high, MIN(low) AS low, LAST(close, time) AS close,
       SUM(volume) AS volume, 'aggregate'::varchar(100) AS source
FROM market_data.ohlcv WHERE timeframe = '1m'
GROUP BY time_bucket('1 hour', time), symbol, name WITH NO DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS market_data.ohlcv_1d
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 day', time) AS time, symbol, '1d'::varchar(10) AS timeframe, name,
       FIRST(open, time) AS open, MAX(high) AS high, MIN(low) AS low, LAST(close, time) AS close,
       SUM(volume) AS volume, 'aggregate'::varchar(100) AS source
FROM market_data.ohlcv WHERE timeframe = '1m'
GROUP BY time_bucket('1 day', time), symbol, name WITH NO DATA;

SELECT add_continuous_aggregate_policy('market_data.ohlcv_5m',  start_offset => INTERVAL '3 days', end_offset => INTERVAL '5 minutes',  schedule_interval => INTERVAL '5 minutes',  if_not_exists => TRUE);
SELECT add_continuous_aggregate_policy('market_data.ohlcv_15m', start_offset => INTERVAL '3 days', end_offset => INTERVAL '15 minutes', schedule_interval => INTERVAL '15 minutes', if_not_exists => TRUE);
SELECT add_continuous_aggregate_policy('market_data.ohlcv_30m', start_offset => INTERVAL '3 days', end_offset => INTERVAL '30 minutes', schedule_interval => INTERVAL '30 minutes', if_not_exists => TRUE);
SELECT add_continuous_aggregate_policy('market_data.ohlcv_1h',  start_offset => INTERVAL '3 days', end_offset => INTERVAL '1 hour',    schedule_interval => INTERVAL '1 hour',    if_not_exists => TRUE);
SELECT add_continuous_aggregate_policy('market_data.ohlcv_1d',  start_offset => INTERVAL '14 days', end_offset => INTERVAL '1 day',    schedule_interval => INTERVAL '1 day',     if_not_exists => TRUE);

CALL refresh_continuous_aggregate('market_data.ohlcv_5m',  NULL, now() - INTERVAL '5 minutes');
CALL refresh_continuous_aggregate('market_data.ohlcv_15m', NULL, now() - INTERVAL '15 minutes');
CALL refresh_continuous_aggregate('market_data.ohlcv_30m', NULL, now() - INTERVAL '30 minutes');
CALL refresh_continuous_aggregate('market_data.ohlcv_1h',  NULL, now() - INTERVAL '1 hour');
CALL refresh_continuous_aggregate('market_data.ohlcv_1d',  NULL, now() - INTERVAL '1 day');
