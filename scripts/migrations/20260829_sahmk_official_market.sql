-- Official SahmK TASI + sector performance snapshots.
-- Does not delete legacy 900xx sector_candles / local calculator rows.

CREATE TABLE IF NOT EXISTS market_data.sahmk_sector_performance (
    as_of              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    market_index       VARCHAR(20) NOT NULL DEFAULT 'TASI',
    sector_code        VARCHAR(20),
    sector_name_en     TEXT,
    sector_name_ar     TEXT,
    change_percent     DOUBLE PRECISION,
    avg_change_percent DOUBLE PRECISION,
    volume             DOUBLE PRECISION,
    num_stocks         INTEGER,
    source             VARCHAR(50) NOT NULL DEFAULT 'sahmk_official',
    raw                JSONB,
    PRIMARY KEY (as_of, market_index, sector_name_en)
);

CREATE INDEX IF NOT EXISTS idx_sahmk_sector_perf_latest
    ON market_data.sahmk_sector_performance (market_index, as_of DESC);

CREATE TABLE IF NOT EXISTS market_data.sahmk_index_snapshot (
    as_of                TIMESTAMPTZ PRIMARY KEY DEFAULT NOW(),
    market_index         VARCHAR(20) NOT NULL DEFAULT 'TASI',
    index_value          DOUBLE PRECISION,
    index_change         DOUBLE PRECISION,
    index_change_percent DOUBLE PRECISION,
    is_delayed           BOOLEAN,
    total_volume         DOUBLE PRECISION,
    advancing            INTEGER,
    declining            INTEGER,
    unchanged            INTEGER,
    market_mood          TEXT,
    source               VARCHAR(50) NOT NULL DEFAULT 'sahmk_official',
    raw                  JSONB
);

GRANT ALL PRIVILEGES ON TABLE market_data.sahmk_sector_performance TO alpha_user;
GRANT ALL PRIVILEGES ON TABLE market_data.sahmk_index_snapshot TO alpha_user;
