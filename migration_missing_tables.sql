-- ============================================================
-- Alpha-Engine2 — Migration: Create Missing Tables
-- Run with:
--   docker compose exec postgres psql -U alpha_user -d alpha_engine -f /migration_missing_tables.sql
-- Or copy-paste directly into psql session.
-- ============================================================

-- ─────────────────────────────────────────────────────────────
-- 1. Create schemas if not exist
-- ─────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS strategies;
CREATE SCHEMA IF NOT EXISTS bots;
CREATE SCHEMA IF NOT EXISTS trading;

-- ─────────────────────────────────────────────────────────────
-- 2. strategies.signals
--    Referenced by dashboard queries:
--    SELECT COUNT(*) FROM strategies.signals WHERE DATE(timestamp) = CURRENT_DATE
--    SELECT timestamp, strategy_name, symbol, signal_type, confidence, price ...
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS strategies.signals (
    id              BIGSERIAL       PRIMARY KEY,
    timestamp       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    strategy_name   VARCHAR(100)    NOT NULL,
    symbol          VARCHAR(20)     NOT NULL,
    signal_type     VARCHAR(10)     NOT NULL CHECK (signal_type IN ('BUY', 'SELL', 'HOLD')),
    confidence      NUMERIC(5,4)    NOT NULL DEFAULT 0.0,
    price           NUMERIC(12,4)   NOT NULL DEFAULT 0.0,
    timeframe       VARCHAR(10)     DEFAULT '1d',
    notes           TEXT,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- Index for dashboard queries (filter by date, order by timestamp)
CREATE INDEX IF NOT EXISTS idx_signals_timestamp
    ON strategies.signals (timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_signals_strategy_date
    ON strategies.signals (strategy_name, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_signals_symbol
    ON strategies.signals (symbol, timestamp DESC);

-- ─────────────────────────────────────────────────────────────
-- 3. bots.status
--    Referenced by dashboard query:
--    SELECT bot_name, status, last_run, error_message FROM bots.status ORDER BY bot_name
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bots.status (
    bot_name        VARCHAR(100)    PRIMARY KEY,
    status          VARCHAR(20)     NOT NULL DEFAULT 'stopped'
                                    CHECK (status IN ('running', 'stopped', 'error', 'idle')),
    last_run        TIMESTAMPTZ,
    error_message   TEXT,
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- Insert default rows for all known bots (ON CONFLICT → do nothing if already exists)
INSERT INTO bots.status (bot_name, status) VALUES
    ('market_reporter',    'stopped'),
    ('data_importer',      'stopped'),
    ('technical_miner',    'stopped'),
    ('consolidation_hunter','stopped'),
    ('monitor',            'stopped'),
    ('strategic_analyzer', 'stopped'),
    ('scientist',          'stopped'),
    ('telegram_bot',       'stopped')
ON CONFLICT (bot_name) DO NOTHING;

-- ─────────────────────────────────────────────────────────────
-- 4. trading.performance  (optional — for performance dashboard)
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS trading.performance (
    id              BIGSERIAL       PRIMARY KEY,
    timestamp       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    strategy_name   VARCHAR(100)    NOT NULL,
    symbol          VARCHAR(20)     NOT NULL,
    entry_price     NUMERIC(12,4),
    exit_price      NUMERIC(12,4),
    pnl             NUMERIC(12,4),
    pnl_pct         NUMERIC(8,4),
    trade_type      VARCHAR(10)     CHECK (trade_type IN ('BUY', 'SELL')),
    notes           TEXT
);

CREATE INDEX IF NOT EXISTS idx_performance_timestamp
    ON trading.performance (timestamp DESC);

-- ─────────────────────────────────────────────────────────────
-- 5. Verify
-- ─────────────────────────────────────────────────────────────
SELECT schemaname, tablename
FROM pg_tables
WHERE schemaname IN ('strategies', 'bots', 'trading', 'market_data')
ORDER BY schemaname, tablename;
