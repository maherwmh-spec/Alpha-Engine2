-- =============================================================================
-- Migration 010: Phase 5 — Active monitoring + paper trades
-- =============================================================================

CREATE TABLE IF NOT EXISTS market_data.active_monitoring (
    symbol                  TEXT PRIMARY KEY,
    source                  TEXT,
    status                  TEXT DEFAULT 'active',
    last_phase              TEXT,
    last_sentiment          TEXT,
    last_price              DOUBLE PRECISION,
    alert_on_phase_change   BOOLEAN DEFAULT TRUE,
    alert_on_sentiment      BOOLEAN DEFAULT TRUE,
    alert_on_signal         BOOLEAN DEFAULT TRUE,
    notes                   TEXT,
    added_at                TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW(),
    last_alert_at           TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_active_monitoring_status
    ON market_data.active_monitoring (status);

CREATE TABLE IF NOT EXISTS market_data.paper_trades (
    id                  BIGSERIAL PRIMARY KEY,
    symbol              TEXT NOT NULL,
    side                TEXT DEFAULT 'long',
    status              TEXT DEFAULT 'open',
    entry_price         DOUBLE PRECISION,
    stop_loss           DOUBLE PRECISION,
    take_profit         DOUBLE PRECISION,
    qty                 DOUBLE PRECISION DEFAULT 1,
    opened_at           TIMESTAMPTZ,
    closed_at           TIMESTAMPTZ,
    exit_price          DOUBLE PRECISION,
    pnl_pct             DOUBLE PRECISION,
    source              TEXT,
    strategy_ref        TEXT,
    meta_json           JSONB,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_paper_trades_status
    ON market_data.paper_trades (status);

CREATE INDEX IF NOT EXISTS idx_paper_trades_symbol
    ON market_data.paper_trades (symbol);

COMMENT ON TABLE market_data.active_monitoring IS
    'Phase 5 symbols under active user-driven monitoring';
COMMENT ON TABLE market_data.paper_trades IS
    'Phase 5 paper (simulated) trades — no live broker execution';

DO $$
BEGIN
    RAISE NOTICE 'Migration 010 complete: active_monitoring + paper_trades';
END $$;
