-- Alpha-Engine2 Database Initialization Script

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS market_data;
CREATE SCHEMA IF NOT EXISTS strategies;
CREATE SCHEMA IF NOT EXISTS bots;
CREATE SCHEMA IF NOT EXISTS alerts;
CREATE SCHEMA IF NOT EXISTS analytics;

-- ========================================
-- Market Data Tables
-- ========================================

-- OHLCV table: جدول موحد لجميع البيانات (أسهم + قطاعات + مؤشرات)
-- الرموز 1010-9999: أسهم تداول
-- الرموز 90001-90099: مؤشرات وقطاعات
CREATE TABLE IF NOT EXISTS market_data.ohlcv (
    time          TIMESTAMPTZ  NOT NULL,
    symbol        VARCHAR(20)  NOT NULL,
    timeframe     VARCHAR(10)  NOT NULL,
    name          VARCHAR(100),               -- اسم السهم أو القطاع
    open          DECIMAL(15, 4),
    high          DECIMAL(15, 4),
    low           DECIMAL(15, 4),
    close         DECIMAL(15, 4),
    volume        BIGINT,
    open_interest BIGINT DEFAULT 0,
    PRIMARY KEY (time, symbol, timeframe)
);

SELECT create_hypertable('market_data.ohlcv', 'time', if_not_exists => TRUE);

-- Indexes for faster queries
CREATE INDEX IF NOT EXISTS idx_ohlcv_symbol_time     ON market_data.ohlcv (symbol, time DESC);
CREATE INDEX IF NOT EXISTS idx_ohlcv_timeframe_time  ON market_data.ohlcv (timeframe, time DESC);
CREATE INDEX IF NOT EXISTS idx_ohlcv_symbol_tf_time  ON market_data.ohlcv (symbol, timeframe, time DESC);

-- ========================================
-- Continuous Aggregates (TimescaleDB)
-- تجميع تلقائي من 1m إلى 5m, 15m, 30m, 1h, 1d
-- ========================================

-- 5 دقائق من 1 دقيقة
CREATE MATERIALIZED VIEW IF NOT EXISTS market_data.ohlcv_5m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 minutes', time) AS time,
    symbol,
    timeframe,
    name,
    FIRST(open, time)  AS open,
    MAX(high)          AS high,
    MIN(low)           AS low,
    LAST(close, time)  AS close,
    SUM(volume)        AS volume
FROM market_data.ohlcv
WHERE timeframe = '1m'
GROUP BY time_bucket('5 minutes', time), symbol, timeframe, name
WITH NO DATA;

SELECT add_continuous_aggregate_policy('market_data.ohlcv_5m',
    start_offset      => INTERVAL '3 days',
    end_offset        => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists     => TRUE);

-- 15 دقيقة من 1 دقيقة
CREATE MATERIALIZED VIEW IF NOT EXISTS market_data.ohlcv_15m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('15 minutes', time) AS time,
    symbol,
    timeframe,
    name,
    FIRST(open, time)  AS open,
    MAX(high)          AS high,
    MIN(low)           AS low,
    LAST(close, time)  AS close,
    SUM(volume)        AS volume
FROM market_data.ohlcv
WHERE timeframe = '1m'
GROUP BY time_bucket('15 minutes', time), symbol, timeframe, name
WITH NO DATA;

SELECT add_continuous_aggregate_policy('market_data.ohlcv_15m',
    start_offset      => INTERVAL '3 days',
    end_offset        => INTERVAL '15 minutes',
    schedule_interval => INTERVAL '15 minutes',
    if_not_exists     => TRUE);

-- 30 دقيقة من 1 دقيقة
CREATE MATERIALIZED VIEW IF NOT EXISTS market_data.ohlcv_30m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('30 minutes', time) AS time,
    symbol,
    timeframe,
    name,
    FIRST(open, time)  AS open,
    MAX(high)          AS high,
    MIN(low)           AS low,
    LAST(close, time)  AS close,
    SUM(volume)        AS volume
FROM market_data.ohlcv
WHERE timeframe = '1m'
GROUP BY time_bucket('30 minutes', time), symbol, timeframe, name
WITH NO DATA;

SELECT add_continuous_aggregate_policy('market_data.ohlcv_30m',
    start_offset      => INTERVAL '3 days',
    end_offset        => INTERVAL '30 minutes',
    schedule_interval => INTERVAL '30 minutes',
    if_not_exists     => TRUE);

-- 1 ساعة من 1 دقيقة
CREATE MATERIALIZED VIEW IF NOT EXISTS market_data.ohlcv_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS time,
    symbol,
    timeframe,
    name,
    FIRST(open, time)  AS open,
    MAX(high)          AS high,
    MIN(low)           AS low,
    LAST(close, time)  AS close,
    SUM(volume)        AS volume
FROM market_data.ohlcv
WHERE timeframe = '1m'
GROUP BY time_bucket('1 hour', time), symbol, timeframe, name
WITH NO DATA;

SELECT add_continuous_aggregate_policy('market_data.ohlcv_1h',
    start_offset      => INTERVAL '3 days',
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists     => TRUE);

-- 1 يوم من 1 دقيقة
CREATE MATERIALIZED VIEW IF NOT EXISTS market_data.ohlcv_1d
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', time) AS time,
    symbol,
    timeframe,
    name,
    FIRST(open, time)  AS open,
    MAX(high)          AS high,
    MIN(low)           AS low,
    LAST(close, time)  AS close,
    SUM(volume)        AS volume
FROM market_data.ohlcv
WHERE timeframe = '1m'
GROUP BY time_bucket('1 day', time), symbol, timeframe, name
WITH NO DATA;

SELECT add_continuous_aggregate_policy('market_data.ohlcv_1d',
    start_offset      => INTERVAL '7 days',
    end_offset        => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists     => TRUE);

-- ========================================
-- Compression Policies (TimescaleDB)
-- ========================================

ALTER TABLE market_data.ohlcv SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol, timeframe',
    timescaledb.compress_orderby   = 'time DESC'
);

SELECT add_compression_policy('market_data.ohlcv',
    INTERVAL '30 days', if_not_exists => TRUE);

-- ========================================
-- Technical indicators table
-- ========================================

CREATE TABLE IF NOT EXISTS market_data.technical_indicators (
    time         TIMESTAMPTZ NOT NULL,
    symbol       VARCHAR(20) NOT NULL,
    timeframe    VARCHAR(10),
    rsi          DECIMAL(8, 4),
    macd         DECIMAL(12, 6),
    macd_signal  DECIMAL(12, 6),
    macd_hist    DECIMAL(12, 6),
    bb_upper     DECIMAL(12, 4),
    bb_middle    DECIMAL(12, 4),
    bb_lower     DECIMAL(12, 4),
    ema_9        DECIMAL(12, 4),
    ema_21       DECIMAL(12, 4),
    sma_50       DECIMAL(12, 4),
    sma_200      DECIMAL(12, 4),
    atr          DECIMAL(12, 4),
    stoch_k      DECIMAL(8, 4),
    stoch_d      DECIMAL(8, 4),
    adx          DECIMAL(8, 4),
    obv          BIGINT,
    PRIMARY KEY (time, symbol, timeframe)
);

SELECT create_hypertable('market_data.technical_indicators', 'time', if_not_exists => TRUE);
SELECT add_compression_policy('market_data.technical_indicators', INTERVAL '7 days', if_not_exists => TRUE);

-- ========================================
-- News and sentiment table
-- ========================================

CREATE TABLE IF NOT EXISTS market_data.news (
    id              SERIAL PRIMARY KEY,
    timestamp       TIMESTAMPTZ NOT NULL,
    symbol          VARCHAR(20),
    title           TEXT,
    content         TEXT,
    source          VARCHAR(100),
    url             TEXT,
    sentiment_score DECIMAL(5, 4),
    sentiment_label VARCHAR(20),
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_news_symbol_timestamp ON market_data.news (symbol, timestamp DESC);

-- ========================================
-- Strategies Tables
-- ========================================

CREATE TABLE IF NOT EXISTS strategies.signals (
    id            SERIAL PRIMARY KEY,
    timestamp     TIMESTAMPTZ NOT NULL,
    strategy_name VARCHAR(50) NOT NULL,
    symbol        VARCHAR(20) NOT NULL,
    signal_type   VARCHAR(10) NOT NULL,  -- BUY, SELL, HOLD
    price         DECIMAL(12, 4),
    confidence    DECIMAL(5, 4),
    timeframe     VARCHAR(10),
    metadata      JSONB,
    created_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_signals_strategy_symbol ON strategies.signals (strategy_name, symbol, timestamp DESC);

CREATE TABLE IF NOT EXISTS strategies.backtest_results (
    id               SERIAL PRIMARY KEY,
    strategy_name    VARCHAR(50) NOT NULL,
    symbol           VARCHAR(20),
    start_date       DATE,
    end_date         DATE,
    total_trades     INTEGER,
    winning_trades   INTEGER,
    losing_trades    INTEGER,
    total_profit     DECIMAL(12, 4),
    total_loss       DECIMAL(12, 4),
    net_profit       DECIMAL(12, 4),
    win_rate         DECIMAL(5, 4),
    profit_factor    DECIMAL(8, 4),
    sharpe_ratio     DECIMAL(8, 4),
    max_drawdown     DECIMAL(8, 4),
    parameters       JSONB,
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS strategies.positions (
    id                    SERIAL PRIMARY KEY,
    strategy_name         VARCHAR(50) NOT NULL,
    symbol                VARCHAR(20) NOT NULL,
    entry_time            TIMESTAMPTZ NOT NULL,
    exit_time             TIMESTAMPTZ,
    entry_price           DECIMAL(12, 4),
    exit_price            DECIMAL(12, 4),
    quantity              INTEGER,
    position_type         VARCHAR(10),   -- LONG, SHORT
    status                VARCHAR(20),   -- OPEN, CLOSED
    profit_loss           DECIMAL(12, 4),
    profit_loss_percentage DECIMAL(8, 4),
    stop_loss             DECIMAL(12, 4),
    take_profit           DECIMAL(12, 4),
    metadata              JSONB,
    created_at            TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_positions_strategy_symbol ON strategies.positions (strategy_name, symbol, status);

-- ========================================
-- Bots Tables
-- ========================================

CREATE TABLE IF NOT EXISTS bots.status (
    bot_name      VARCHAR(50) PRIMARY KEY,
    status        VARCHAR(20),  -- RUNNING, STOPPED, ERROR
    last_run      TIMESTAMPTZ,
    next_run      TIMESTAMPTZ,
    error_message TEXT,
    metadata      JSONB,
    updated_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bots.logs (
    id         SERIAL PRIMARY KEY,
    timestamp  TIMESTAMPTZ NOT NULL,
    bot_name   VARCHAR(50) NOT NULL,
    level      VARCHAR(20),  -- INFO, WARNING, ERROR, CRITICAL
    message    TEXT,
    metadata   JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bot_logs_bot_timestamp ON bots.logs (bot_name, timestamp DESC);

-- ========================================
-- Alerts Tables
-- ========================================

CREATE TABLE IF NOT EXISTS alerts.notifications (
    id            SERIAL PRIMARY KEY,
    timestamp     TIMESTAMPTZ NOT NULL,
    alert_type    VARCHAR(50),   -- SIGNAL, HEALTH, ERROR, WEEKLY_REVIEW
    priority      INTEGER,       -- 1=HIGH, 2=MEDIUM, 3=LOW
    title         VARCHAR(200),
    message       TEXT,
    symbol        VARCHAR(20),
    strategy_name VARCHAR(50),
    sent          BOOLEAN DEFAULT FALSE,
    sent_at       TIMESTAMPTZ,
    metadata      JSONB,
    created_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alerts_sent_priority ON alerts.notifications (sent, priority, timestamp DESC);

-- ========================================
-- Analytics Tables
-- ========================================

CREATE TABLE IF NOT EXISTS analytics.performance_metrics (
    id            SERIAL PRIMARY KEY,
    date          DATE NOT NULL,
    strategy_name VARCHAR(50),
    total_trades  INTEGER,
    winning_trades INTEGER,
    total_profit  DECIMAL(12, 4),
    total_loss    DECIMAL(12, 4),
    net_profit    DECIMAL(12, 4),
    win_rate      DECIMAL(5, 4),
    sharpe_ratio  DECIMAL(8, 4),
    max_drawdown  DECIMAL(8, 4),
    created_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_performance_date_strategy ON analytics.performance_metrics (date DESC, strategy_name);

CREATE TABLE IF NOT EXISTS analytics.model_performance (
    id              SERIAL PRIMARY KEY,
    model_name      VARCHAR(100) NOT NULL,
    version         VARCHAR(50),
    training_date   TIMESTAMPTZ,
    accuracy        DECIMAL(8, 6),
    precision_score DECIMAL(8, 6),
    recall          DECIMAL(8, 6),
    f1_score        DECIMAL(8, 6),
    auc_roc         DECIMAL(8, 6),
    parameters      JSONB,
    feature_importance JSONB,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS analytics.system_health (
    id               SERIAL PRIMARY KEY,
    timestamp        TIMESTAMPTZ NOT NULL,
    cpu_usage        DECIMAL(5, 2),
    memory_usage     DECIMAL(5, 2),
    disk_usage       DECIMAL(5, 2),
    database_size    BIGINT,
    active_bots      INTEGER,
    active_positions INTEGER,
    redis_memory     BIGINT,
    metadata         JSONB,
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

SELECT create_hypertable('analytics.system_health', 'timestamp', if_not_exists => TRUE);
SELECT add_compression_policy('analytics.system_health', INTERVAL '30 days', if_not_exists => TRUE);

-- ========================================
-- Configuration Tables
-- ========================================

CREATE TABLE IF NOT EXISTS bots.parameters (
    id          SERIAL PRIMARY KEY,
    category    VARCHAR(50),   -- STRATEGY, BOT, RISK, GENERAL
    key         VARCHAR(100) UNIQUE NOT NULL,
    value       TEXT,
    data_type   VARCHAR(20),   -- STRING, INTEGER, FLOAT, BOOLEAN, JSON
    description TEXT,
    editable    BOOLEAN DEFAULT TRUE,
    updated_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_by  VARCHAR(50)
);

INSERT INTO bots.parameters (category, key, value, data_type, description, editable) VALUES
('GENERAL', 'silent_mode',          'false', 'BOOLEAN', 'Silent mode (stop alerts but continue data collection)', TRUE),
('RISK',    'max_daily_loss',        '0.05',  'FLOAT',   'Maximum daily loss percentage',                         TRUE),
('RISK',    'max_drawdown',          '0.15',  'FLOAT',   'Maximum drawdown percentage',                           TRUE),
('RISK',    'max_total_positions',   '10',    'INTEGER', 'Maximum total positions',                               TRUE)
ON CONFLICT (key) DO NOTHING;

-- ========================================
-- Views
-- ========================================

CREATE OR REPLACE VIEW strategies.active_positions AS
SELECT * FROM strategies.positions
WHERE status = 'OPEN'
ORDER BY entry_time DESC;

CREATE OR REPLACE VIEW analytics.today_performance AS
SELECT
    strategy_name,
    COUNT(*) AS total_trades,
    SUM(CASE WHEN profit_loss > 0 THEN 1 ELSE 0 END) AS winning_trades,
    SUM(profit_loss) AS net_profit,
    AVG(profit_loss_percentage) AS avg_profit_percentage
FROM strategies.positions
WHERE DATE(entry_time) = CURRENT_DATE
GROUP BY strategy_name;

CREATE OR REPLACE VIEW alerts.pending_alerts AS
SELECT * FROM alerts.notifications
WHERE sent = FALSE
ORDER BY priority ASC, timestamp DESC;

-- ========================================
-- Functions
-- ========================================

CREATE OR REPLACE FUNCTION market_data.get_latest_price(p_symbol VARCHAR)
RETURNS DECIMAL(15, 4) AS $$
DECLARE
    latest_price DECIMAL(15, 4);
BEGIN
    SELECT close INTO latest_price
    FROM market_data.ohlcv
    WHERE symbol = p_symbol
    ORDER BY time DESC
    LIMIT 1;
    RETURN latest_price;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION strategies.calculate_win_rate(p_strategy_name VARCHAR)
RETURNS DECIMAL(5, 4) AS $$
DECLARE
    win_rate DECIMAL(5, 4);
BEGIN
    SELECT
        CASE
            WHEN COUNT(*) = 0 THEN 0
            ELSE CAST(SUM(CASE WHEN profit_loss > 0 THEN 1 ELSE 0 END) AS DECIMAL) / COUNT(*)
        END INTO win_rate
    FROM strategies.positions
    WHERE strategy_name = p_strategy_name
    AND status = 'CLOSED';
    RETURN win_rate;
END;
$$ LANGUAGE plpgsql;

-- ========================================
-- Triggers
-- ========================================

CREATE OR REPLACE FUNCTION bots.update_status_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_bot_status
BEFORE UPDATE ON bots.status
FOR EACH ROW
EXECUTE FUNCTION bots.update_status_timestamp();

-- ========================================
-- Initial Data
-- ========================================

INSERT INTO bots.status (bot_name, status, last_run, next_run) VALUES
('data_importer',        'STOPPED', NULL, NULL),
('technical_miner',      'STOPPED', NULL, NULL),
('market_reporter',      'STOPPED', NULL, NULL),
('scientist',            'STOPPED', NULL, NULL),
('strategic_analyzer',   'STOPPED', NULL, NULL),
('monitor',              'STOPPED', NULL, NULL),
('behavioral_analyzer',  'STOPPED', NULL, NULL),
('multiframe_confirmer', 'STOPPED', NULL, NULL),
('risk_guardian',        'STOPPED', NULL, NULL),
('consolidation_hunter', 'STOPPED', NULL, NULL),
('self_trainer',         'STOPPED', NULL, NULL),
('weekly_reviewer',      'STOPPED', NULL, NULL),
('health_monitor',       'STOPPED', NULL, NULL),
('backup_manager',       'STOPPED', NULL, NULL),
('parameter_editor',     'STOPPED', NULL, NULL),
('dashboard_service',    'STOPPED', NULL, NULL),
('freqai_manager',       'STOPPED', NULL, NULL),
('silent_mode_manager',  'STOPPED', NULL, NULL)
ON CONFLICT (bot_name) DO NOTHING;

-- ========================================
-- Indexes for Performance
-- ========================================

CREATE INDEX IF NOT EXISTS idx_technical_indicators_symbol ON market_data.technical_indicators (symbol, time DESC);
CREATE INDEX IF NOT EXISTS idx_signals_timestamp           ON strategies.signals (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_positions_entry_time        ON strategies.positions (entry_time DESC);

-- ========================================
-- Grant Permissions
-- ========================================

GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA market_data TO alpha_user;
GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA strategies  TO alpha_user;
GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA bots        TO alpha_user;
GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA alerts      TO alpha_user;
GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA analytics   TO alpha_user;

GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA market_data TO alpha_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA strategies  TO alpha_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA bots        TO alpha_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA alerts      TO alpha_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA analytics   TO alpha_user;
