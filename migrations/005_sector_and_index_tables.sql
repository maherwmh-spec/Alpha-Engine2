-- =============================================================================
-- Migration 005: Sector and Index Tables
-- Creates: market_data.sector_performance, market_data.index_performance
-- =============================================================================

-- ── 1. جدول أداء القطاعات ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS market_data.sector_performance (
    time            TIMESTAMPTZ     NOT NULL,
    symbol          VARCHAR(10)     NOT NULL,   -- e.g. 90010, 90017
    name            VARCHAR(100),               -- اسم القطاع بالعربي
    timeframe       VARCHAR(5)      NOT NULL DEFAULT '1d',
    open            NUMERIC(18, 4)  NOT NULL,
    high            NUMERIC(18, 4)  NOT NULL,
    low             NUMERIC(18, 4)  NOT NULL,
    close           NUMERIC(18, 4)  NOT NULL,
    volume          BIGINT          NOT NULL DEFAULT 0,
    members_count   INT             DEFAULT 0,  -- عدد الأسهم المُكوِّنة للقطاع
    source          VARCHAR(50)     DEFAULT 'db_sector_calculator',
    created_at      TIMESTAMPTZ     DEFAULT NOW()
);

-- تحويل إلى hypertable (TimescaleDB)
SELECT create_hypertable(
    'market_data.sector_performance',
    'time',
    if_not_exists => TRUE
);

-- فهرس مركّب للاستعلامات الشائعة
CREATE INDEX IF NOT EXISTS idx_sector_perf_symbol_time
    ON market_data.sector_performance (symbol, time DESC);

CREATE INDEX IF NOT EXISTS idx_sector_perf_timeframe
    ON market_data.sector_performance (timeframe, time DESC);

-- UNIQUE لمنع التكرار (upsert)
CREATE UNIQUE INDEX IF NOT EXISTS idx_sector_perf_unique
    ON market_data.sector_performance (symbol, timeframe, time);

COMMENT ON TABLE market_data.sector_performance IS
    'أداء القطاعات (90010-90030) محسوبة من بيانات الأسهم';

-- ── 2. جدول أداء المؤشر العام ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS market_data.index_performance (
    time            TIMESTAMPTZ     NOT NULL,
    symbol          VARCHAR(10)     NOT NULL DEFAULT '90001',
    name            VARCHAR(100)    DEFAULT 'TASI',
    timeframe       VARCHAR(5)      NOT NULL DEFAULT '1d',
    open            NUMERIC(18, 4),
    high            NUMERIC(18, 4),
    low             NUMERIC(18, 4),
    close           NUMERIC(18, 4)  NOT NULL,
    volume          BIGINT          DEFAULT 0,
    change_pct      NUMERIC(8, 4),              -- نسبة التغيير %
    change_abs      NUMERIC(18, 4),             -- التغيير المطلق
    source          VARCHAR(50)     DEFAULT 'db_calculator',
    created_at      TIMESTAMPTZ     DEFAULT NOW()
);

-- تحويل إلى hypertable
SELECT create_hypertable(
    'market_data.index_performance',
    'time',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_index_perf_symbol_time
    ON market_data.index_performance (symbol, time DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_index_perf_unique
    ON market_data.index_performance (symbol, timeframe, time);

COMMENT ON TABLE market_data.index_performance IS
    'أداء المؤشر العام (TASI - 90001)';

\echo '✅ Migration 005 completed successfully!'
