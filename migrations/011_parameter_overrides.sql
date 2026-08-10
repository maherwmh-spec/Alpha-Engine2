-- =============================================================================
-- Migration 011: Phase 6 — parameter overrides + audit
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS strategies;

CREATE TABLE IF NOT EXISTS strategies.parameter_overrides (
    id              BIGSERIAL PRIMARY KEY,
    scope           TEXT NOT NULL DEFAULT 'symbol',
    symbol          TEXT,
    strategy_ref    TEXT,
    param_key       TEXT NOT NULL,
    param_value     JSONB NOT NULL,
    enabled         BOOLEAN DEFAULT TRUE,
    updated_by      TEXT DEFAULT 'telegram',
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_param_overrides_key
    ON strategies.parameter_overrides (
        scope,
        COALESCE(symbol, ''),
        COALESCE(strategy_ref, ''),
        param_key
    );

CREATE INDEX IF NOT EXISTS idx_param_overrides_symbol
    ON strategies.parameter_overrides (symbol)
    WHERE symbol IS NOT NULL;

CREATE TABLE IF NOT EXISTS strategies.parameter_audit (
    id              BIGSERIAL PRIMARY KEY,
    symbol          TEXT,
    param_key       TEXT,
    old_value       JSONB,
    new_value       JSONB,
    action          TEXT,
    source          TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_param_audit_symbol_time
    ON strategies.parameter_audit (symbol, created_at DESC);

COMMENT ON TABLE strategies.parameter_overrides IS
    'Phase 6 user overrides for strategy params (symbol > global > config)';
COMMENT ON TABLE strategies.parameter_audit IS
    'Phase 6 audit log for parameter changes';

DO $$
BEGIN
    RAISE NOTICE 'Migration 011 complete: parameter_overrides + parameter_audit';
END $$;
