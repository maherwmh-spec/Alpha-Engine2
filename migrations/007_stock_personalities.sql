-- =============================================================================
-- Migration 007: Stock Personalities (Phase 2)
-- Statistical (multi-window) + behavioral + phase detection
-- =============================================================================

CREATE TABLE IF NOT EXISTS market_data.stock_personalities (
    symbol                  TEXT PRIMARY KEY,

    -- Statistical (3 windows)
    beta_30d                DOUBLE PRECISION,
    beta_90d                DOUBLE PRECISION,
    beta_all                DOUBLE PRECISION,
    alpha_30d               DOUBLE PRECISION,
    alpha_90d               DOUBLE PRECISION,
    alpha_all               DOUBLE PRECISION,
    volatility_30d          DOUBLE PRECISION,
    volatility_90d          DOUBLE PRECISION,

    -- Behavioral
    accumulation_score      DOUBLE PRECISION,
    range_tightness         DOUBLE PRECISION,
    explosiveness_score     DOUBLE PRECISION,
    distribution_score      DOUBLE PRECISION,
    distribution_style      TEXT,
    cycle_length_est        INT,
    breathing_rhythm_days   DOUBLE PRECISION,

    -- Phase detection
    phase                   TEXT,
    phase_confidence        DOUBLE PRECISION,
    days_in_phase           INT,
    gain_from_base          DOUBLE PRECISION,
    drop_from_peak          DOUBLE PRECISION,

    -- Classifications
    beta_class              TEXT,
    vol_class               TEXT,

    -- Meta
    benchmark_symbol        TEXT,
    sample_size_30d         INT,
    sample_size_90d         INT,
    sample_size_all         INT,
    status                  TEXT,
    computed_at             TIMESTAMPTZ,
    updated_at              TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stock_personalities_phase
    ON market_data.stock_personalities (phase, phase_confidence DESC);

CREATE INDEX IF NOT EXISTS idx_stock_personalities_updated
    ON market_data.stock_personalities (updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_stock_personalities_status
    ON market_data.stock_personalities (status);

COMMENT ON TABLE market_data.stock_personalities IS
    'Phase 2 stock personality: multi-window beta/alpha/vol + behavioral scores + phase detection';

DO $$
BEGIN
    RAISE NOTICE 'Migration 007 complete: market_data.stock_personalities';
END $$;
