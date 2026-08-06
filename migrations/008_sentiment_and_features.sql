-- =============================================================================
-- Migration 008: Phase 3 — Sentiment + Feature Engineering tables
-- =============================================================================

-- ── News sentiments (unified nightly path) ───────────────────────────────────
CREATE TABLE IF NOT EXISTS market_data.news_sentiments (
    symbol              TEXT PRIMARY KEY,
    sentiment_label     TEXT,
    sentiment_score     DOUBLE PRECISION,
    score_positive      DOUBLE PRECISION,
    score_neutral       DOUBLE PRECISION,
    score_negative      DOUBLE PRECISION,
    articles_count      INT DEFAULT 0,
    model_name          TEXT,
    status              TEXT,
    computed_at         TIMESTAMPTZ,
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_news_sentiments_label
    ON market_data.news_sentiments (sentiment_label);

CREATE INDEX IF NOT EXISTS idx_news_sentiments_updated
    ON market_data.news_sentiments (updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_news_sentiments_status
    ON market_data.news_sentiments (status);

COMMENT ON TABLE market_data.news_sentiments IS
    'Phase 3 unified nightly sentiment per symbol (FinBERT or fallback)';

-- Optional lightweight news source table if missing (safe create)
CREATE TABLE IF NOT EXISTS market_data.news (
    id              BIGSERIAL PRIMARY KEY,
    symbol          TEXT,
    title           TEXT,
    content         TEXT,
    source          TEXT,
    published_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_news_symbol_published
    ON market_data.news (symbol, published_at DESC);

-- ── Stock features (fixed list + JSON extension) ─────────────────────────────
CREATE TABLE IF NOT EXISTS market_data.stock_features (
    symbol              TEXT PRIMARY KEY,
    timeframe           TEXT DEFAULT '1d',
    ret_1d              DOUBLE PRECISION,
    ret_5d              DOUBLE PRECISION,
    ret_20d             DOUBLE PRECISION,
    vol_10d             DOUBLE PRECISION,
    vol_20d             DOUBLE PRECISION,
    rsi_14              DOUBLE PRECISION,
    roc_10              DOUBLE PRECISION,
    sma20_dist          DOUBLE PRECISION,
    sma50_dist          DOUBLE PRECISION,
    volume_ratio_20     DOUBLE PRECISION,
    range_pct           DOUBLE PRECISION,
    close_loc           DOUBLE PRECISION,
    features_json       JSONB,
    sample_size         INT,
    status              TEXT,
    computed_at         TIMESTAMPTZ,
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stock_features_status
    ON market_data.stock_features (status);

CREATE INDEX IF NOT EXISTS idx_stock_features_updated
    ON market_data.stock_features (updated_at DESC);

COMMENT ON TABLE market_data.stock_features IS
    'Phase 3 nightly fixed feature set from OHLCV (no Featuretools)';

DO $$
BEGIN
    RAISE NOTICE 'Migration 008 complete: news_sentiments + stock_features (+ news if missing)';
END $$;
