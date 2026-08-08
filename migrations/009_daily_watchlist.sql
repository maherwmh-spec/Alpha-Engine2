-- =============================================================================
-- Migration 009: Phase 4 — Daily Watchlist
-- =============================================================================

CREATE TABLE IF NOT EXISTS market_data.daily_watchlist (
    id                  BIGSERIAL PRIMARY KEY,
    trade_date          DATE NOT NULL,
    symbol              TEXT NOT NULL,
    rank                INT NOT NULL,
    score               DOUBLE PRECISION,
    bucket              TEXT,
    phase               TEXT,
    phase_confidence    DOUBLE PRECISION,
    sentiment_label     TEXT,
    genetic_fitness     DOUBLE PRECISION,
    reasons_json        JSONB,
    status              TEXT DEFAULT 'active',
    notified_at         TIMESTAMPTZ,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (trade_date, symbol)
);

CREATE INDEX IF NOT EXISTS idx_daily_watchlist_date_rank
    ON market_data.daily_watchlist (trade_date DESC, rank ASC);

CREATE INDEX IF NOT EXISTS idx_daily_watchlist_status
    ON market_data.daily_watchlist (status);

COMMENT ON TABLE market_data.daily_watchlist IS
    'Phase 4 morning candidate list built from personality + features + sentiment + genetic';

DO $$
BEGIN
    RAISE NOTICE 'Migration 009 complete: market_data.daily_watchlist';
END $$;
