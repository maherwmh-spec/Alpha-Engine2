-- ============================================================
-- Alpha-Engine2: Sahmk API Tables
-- TimescaleDB Hypertable + Continuous Aggregates
-- ============================================================
-- Run AFTER init_db.sql:
--   docker exec -it alpha_postgres psql -U alpha_user -d alpha_engine \
--     -f /scripts/init_sahmk_tables.sql
-- ============================================================
--
-- RETENTION POLICY (updated for long-term analysis):
--   1m   → 365  days  (1 year)
--   5m   → 730  days  (2 years)
--   15m  → 730  days  (2 years)
--   30m  → 1095 days  (3 years)
--   1h   → 1825 days  (5 years)
--   1d   → UNLIMITED  (never deleted)
-- ============================================================

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- ============================================================
-- 1. RAW 1-MINUTE OHLCV TABLE (WebSocket feed)
-- ============================================================
CREATE TABLE IF NOT EXISTS market_data.ohlcv_realtime (
    time        TIMESTAMPTZ      NOT NULL,
    symbol      VARCHAR(20)      NOT NULL,
    open        DOUBLE PRECISION NOT NULL,
    high        DOUBLE PRECISION NOT NULL,
    low         DOUBLE PRECISION NOT NULL,
    close       DOUBLE PRECISION NOT NULL,
    volume      DOUBLE PRECISION NOT NULL DEFAULT 0,
    tick_count  INTEGER          NOT NULL DEFAULT 1,
    source      VARCHAR(50)      NOT NULL DEFAULT 'sahmk_websocket'
);

SELECT create_hypertable(
    'market_data.ohlcv_realtime', 'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- UPSERT support
CREATE UNIQUE INDEX IF NOT EXISTS idx_ohlcv_rt_time_symbol
    ON market_data.ohlcv_realtime (time, symbol);

CREATE INDEX IF NOT EXISTS idx_ohlcv_rt_symbol_time
    ON market_data.ohlcv_realtime (symbol, time DESC);

-- ============================================================
-- 2. CONTINUOUS AGGREGATES
-- ============================================================

-- 5-minute candles
CREATE MATERIALIZED VIEW IF NOT EXISTS market_data.ohlcv_rt_5m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 minutes', time)  AS bucket,
    symbol,
    first(open,  time)              AS open,
    max(high)                       AS high,
    min(low)                        AS low,
    last(close,  time)              AS close,
    sum(volume)                     AS volume,
    sum(tick_count)                 AS tick_count
FROM market_data.ohlcv_realtime
GROUP BY bucket, symbol
WITH NO DATA;

SELECT add_continuous_aggregate_policy(
    'market_data.ohlcv_rt_5m',
    start_offset      => INTERVAL '2 hours',
    end_offset        => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists     => TRUE
);

-- 15-minute candles
CREATE MATERIALIZED VIEW IF NOT EXISTS market_data.ohlcv_rt_15m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('15 minutes', time) AS bucket,
    symbol,
    first(open,  time)              AS open,
    max(high)                       AS high,
    min(low)                        AS low,
    last(close,  time)              AS close,
    sum(volume)                     AS volume,
    sum(tick_count)                 AS tick_count
FROM market_data.ohlcv_realtime
GROUP BY bucket, symbol
WITH NO DATA;

SELECT add_continuous_aggregate_policy(
    'market_data.ohlcv_rt_15m',
    start_offset      => INTERVAL '4 hours',
    end_offset        => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists     => TRUE
);

-- 30-minute candles
CREATE MATERIALIZED VIEW IF NOT EXISTS market_data.ohlcv_rt_30m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('30 minutes', time) AS bucket,
    symbol,
    first(open,  time)              AS open,
    max(high)                       AS high,
    min(low)                        AS low,
    last(close,  time)              AS close,
    sum(volume)                     AS volume,
    sum(tick_count)                 AS tick_count
FROM market_data.ohlcv_realtime
GROUP BY bucket, symbol
WITH NO DATA;

SELECT add_continuous_aggregate_policy(
    'market_data.ohlcv_rt_30m',
    start_offset      => INTERVAL '6 hours',
    end_offset        => INTERVAL '10 minutes',
    schedule_interval => INTERVAL '10 minutes',
    if_not_exists     => TRUE
);

-- 1-hour candles
CREATE MATERIALIZED VIEW IF NOT EXISTS market_data.ohlcv_rt_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time)     AS bucket,
    symbol,
    first(open,  time)              AS open,
    max(high)                       AS high,
    min(low)                        AS low,
    last(close,  time)              AS close,
    sum(volume)                     AS volume,
    sum(tick_count)                 AS tick_count
FROM market_data.ohlcv_realtime
GROUP BY bucket, symbol
WITH NO DATA;

SELECT add_continuous_aggregate_policy(
    'market_data.ohlcv_rt_1h',
    start_offset      => INTERVAL '2 days',
    end_offset        => INTERVAL '15 minutes',
    schedule_interval => INTERVAL '15 minutes',
    if_not_exists     => TRUE
);

-- 1-day candles
CREATE MATERIALIZED VIEW IF NOT EXISTS market_data.ohlcv_rt_1d
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', time)      AS bucket,
    symbol,
    first(open,  time)              AS open,
    max(high)                       AS high,
    min(low)                        AS low,
    last(close,  time)              AS close,
    sum(volume)                     AS volume,
    sum(tick_count)                 AS tick_count
FROM market_data.ohlcv_realtime
GROUP BY bucket, symbol
WITH NO DATA;

SELECT add_continuous_aggregate_policy(
    'market_data.ohlcv_rt_1d',
    start_offset      => INTERVAL '7 days',
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists     => TRUE
);

-- ============================================================
-- 3. COMPRESSION POLICY (save disk space)
-- ============================================================
ALTER TABLE market_data.ohlcv_realtime SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby   = 'time DESC'
);

SELECT add_compression_policy(
    'market_data.ohlcv_realtime',
    INTERVAL '7 days',
    if_not_exists => TRUE
);

-- ============================================================
-- 4. RETENTION POLICIES
-- ============================================================
-- 1m raw data: 365 days (1 year)
SELECT add_retention_policy(
    'market_data.ohlcv_realtime',
    INTERVAL '365 days',
    if_not_exists => TRUE
);

-- 5m aggregate: 730 days (2 years)
SELECT add_retention_policy(
    'market_data.ohlcv_rt_5m',
    INTERVAL '730 days',
    if_not_exists => TRUE
);

-- 15m aggregate: 730 days (2 years)
SELECT add_retention_policy(
    'market_data.ohlcv_rt_15m',
    INTERVAL '730 days',
    if_not_exists => TRUE
);

-- 30m aggregate: 1095 days (3 years)
SELECT add_retention_policy(
    'market_data.ohlcv_rt_30m',
    INTERVAL '1095 days',
    if_not_exists => TRUE
);

-- 1h aggregate: 1825 days (5 years)
SELECT add_retention_policy(
    'market_data.ohlcv_rt_1h',
    INTERVAL '1825 days',
    if_not_exists => TRUE
);

-- 1d aggregate: NO RETENTION (keep forever)
-- NOTE: No add_retention_policy for ohlcv_rt_1d → data kept indefinitely

-- ============================================================
-- 5. HELPER VIEWS
-- ============================================================
CREATE OR REPLACE VIEW market_data.latest_prices AS
SELECT DISTINCT ON (symbol)
    symbol,
    time        AS last_update,
    close       AS price,
    volume,
    open,
    high,
    low
FROM market_data.ohlcv_realtime
ORDER BY symbol, time DESC;

CREATE OR REPLACE VIEW market_data.today_summary AS
SELECT
    symbol,
    first(open,  time)                                        AS open_today,
    max(high)                                                 AS high_today,
    min(low)                                                  AS low_today,
    last(close,  time)                                        AS close_latest,
    sum(volume)                                               AS volume_today,
    round(
        ((last(close, time) - first(open, time))
          / NULLIF(first(open, time), 0) * 100)::numeric, 2
    )                                                         AS change_pct
FROM market_data.ohlcv_realtime
WHERE time >= date_trunc('day', NOW() AT TIME ZONE 'Asia/Riyadh')
GROUP BY symbol
ORDER BY change_pct DESC;

-- ============================================================
-- 6. GRANT PERMISSIONS
-- ============================================================
GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA market_data TO alpha_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA market_data TO alpha_user;

-- ============================================================
-- DONE
-- ============================================================
DO $$
BEGIN
    RAISE NOTICE '✅ Sahmk tables initialized with updated retention policies:';
    RAISE NOTICE '   ohlcv_realtime (1m) → 365 days  (1 year)';
    RAISE NOTICE '   ohlcv_rt_5m         → 730 days  (2 years)';
    RAISE NOTICE '   ohlcv_rt_15m        → 730 days  (2 years)';
    RAISE NOTICE '   ohlcv_rt_30m        → 1095 days (3 years)';
    RAISE NOTICE '   ohlcv_rt_1h         → 1825 days (5 years)';
    RAISE NOTICE '   ohlcv_rt_1d         → UNLIMITED (no retention)';
    RAISE NOTICE '   Compression: after 7 days';
END $$;
