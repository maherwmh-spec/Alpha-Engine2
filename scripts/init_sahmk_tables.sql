-- ============================================================
-- Alpha-Engine2: Sahmk API Tables (v2 - Comprehensive Collection)
-- TimescaleDB Hypertable + Continuous Aggregates
-- ============================================================
--
-- RETENTION POLICY (v2 - Long-Term Storage):
--   1m   → 365  days  (1 year)
--   5m   → 730  days  (2 years)
--   15m  → 1095 days  (3 years)
--   30m  → 1825 days  (5 years)
--   1h   → 2555 days  (7 years)
--   1d   → UNLIMITED  (never deleted)
-- ============================================================

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- ============================================================
-- 1. UNIFIED OHLCV TABLE (for both REST and WebSocket)
-- ============================================================
CREATE TABLE IF NOT EXISTS market_data.ohlcv (
    time          TIMESTAMPTZ       NOT NULL,
    symbol        VARCHAR(20)       NOT NULL,
    timeframe     VARCHAR(10)       NOT NULL, -- 1m, 5m, 1d, etc.
    name          VARCHAR(255)      NOT NULL DEFAULT 'Unknown',
    open          DOUBLE PRECISION  NOT NULL,
    high          DOUBLE PRECISION  NOT NULL,
    low           DOUBLE PRECISION  NOT NULL,
    close         DOUBLE PRECISION  NOT NULL,
    volume        BIGINT            NOT NULL DEFAULT 0,
    open_interest INTEGER           NOT NULL DEFAULT 0,
    source        VARCHAR(50)       NOT NULL
);

-- Make it a hypertable
SELECT create_hypertable(
    'market_data.ohlcv', 'time',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

-- Primary Key for UPSERT support
CREATE UNIQUE INDEX IF NOT EXISTS idx_ohlcv_time_symbol_timeframe
    ON market_data.ohlcv (time, symbol, timeframe);

-- Index for faster queries
CREATE INDEX IF NOT EXISTS idx_ohlcv_symbol_time_desc
    ON market_data.ohlcv (symbol, time DESC);

-- ============================================================
-- 2. COMPRESSION POLICY (save disk space)
-- ============================================================
ALTER TABLE market_data.ohlcv SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol, timeframe',
    timescaledb.compress_orderby   = 'time DESC'
);

SELECT add_compression_policy(
    'market_data.ohlcv',
    INTERVAL '15 days',
    if_not_exists => TRUE
);

-- ============================================================
-- 3. RETENTION POLICIES (applied per timeframe)
-- ============================================================
-- NOTE: These are applied in update_retention_policies.sql
-- This file only creates the structure.

-- ============================================================
-- 4. GRANT PERMISSIONS
-- ============================================================
GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA market_data TO alpha_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA market_data TO alpha_user;

-- ============================================================
-- DONE
-- ============================================================
DO $$
BEGIN
    RAISE NOTICE '✅ Sahmk OHLCV table initialized successfully.';
    RAISE NOTICE '   Run update_retention_policies.sql to apply retention policies.';
END $$;
