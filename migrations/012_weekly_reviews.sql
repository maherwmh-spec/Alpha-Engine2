-- =============================================================================
-- Migration 012: Phase 7 — weekly reviews
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.weekly_reviews (
    week_id                 TEXT PRIMARY KEY,
    window_start            TIMESTAMPTZ NOT NULL,
    window_end              TIMESTAMPTZ NOT NULL,
    signals_count           INT DEFAULT 0,
    closed_papers           INT DEFAULT 0,
    wins                    INT DEFAULT 0,
    losses                  INT DEFAULT 0,
    win_rate                DOUBLE PRECISION,
    avg_pnl_pct             DOUBLE PRECISION,
    best_symbols_json       JSONB,
    worst_symbols_json      JSONB,
    recommendations_json    JSONB,
    report_text             TEXT,
    notified                BOOLEAN DEFAULT FALSE,
    trainer_status          TEXT,
    computed_at             TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_weekly_reviews_computed
    ON analytics.weekly_reviews (computed_at DESC);

COMMENT ON TABLE analytics.weekly_reviews IS
    'Phase 7 weekly performance review + report snapshot';

DO $$
BEGIN
    RAISE NOTICE 'Migration 012 complete: analytics.weekly_reviews';
END $$;
