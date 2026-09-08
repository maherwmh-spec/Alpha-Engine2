-- Genetic persistence for Scientist / Evaluator / Watchlist.
CREATE SCHEMA IF NOT EXISTS genetic;
CREATE SCHEMA IF NOT EXISTS strategies;

CREATE TABLE IF NOT EXISTS genetic.strategies (
    id                  BIGSERIAL PRIMARY KEY,
    strategy_hash       TEXT UNIQUE,
    symbol              TEXT NOT NULL,
    profit_objective    TEXT NOT NULL DEFAULT 'short_swings',
    dna                 JSONB,
    fitness_score       DOUBLE PRECISION,
    status              TEXT NOT NULL DEFAULT 'evaluated',
    total_profit_pct    DOUBLE PRECISION,
    win_rate            DOUBLE PRECISION,
    total_trades        INTEGER,
    max_drawdown_pct    DOUBLE PRECISION,
    sharpe_ratio        DOUBLE PRECISION,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_genetic_strategies_symbol_obj
    ON genetic.strategies (symbol, profit_objective, status);

CREATE TABLE IF NOT EXISTS genetic.performance (
    id                  BIGSERIAL PRIMARY KEY,
    strategy_hash       TEXT,
    symbol              TEXT,
    profit_objective    TEXT,
    fitness_score       DOUBLE PRECISION,
    total_profit_pct    DOUBLE PRECISION,
    win_rate            DOUBLE PRECISION,
    total_trades        INTEGER,
    max_drawdown_pct    DOUBLE PRECISION,
    sharpe_ratio        DOUBLE PRECISION,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS strategies.discovered_strategies (
    id                  BIGSERIAL PRIMARY KEY,
    symbol              TEXT NOT NULL,
    fitness             DOUBLE PRECISION,
    strategy_hash       TEXT,
    profit_objective    TEXT,
    status              TEXT DEFAULT 'elite',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_discovered_strategies_symbol
    ON strategies.discovered_strategies (symbol);
