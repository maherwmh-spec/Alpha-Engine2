-- ============================================================
-- Migration 001: Genetic Engine Tables
-- Alpha-Engine2 — المحرك الجيني لاكتشاف استراتيجيات التداول
-- ============================================================
-- التاريخ: 2026-04-05
-- الوصف: إنشاء جداول الشيفرات الجينية ونتائج الأداء
-- ============================================================

-- ── 1. Schema ──────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS genetic;
GRANT USAGE ON SCHEMA genetic TO alpha_user;

-- ── 2. جدول الشيفرات الجينية (genetic_strategies) ──────────
-- يخزن كل "وصفة" JSON مكتشفة مع هدفها الربحي وجيلها
CREATE TABLE IF NOT EXISTS genetic.strategies (
    id               BIGSERIAL    PRIMARY KEY,
    strategy_hash    VARCHAR(64)  NOT NULL UNIQUE,   -- SHA256 للـ JSON (يمنع التكرار)
    symbol           VARCHAR(20)  NOT NULL,           -- السهم المستهدف
    profit_objective VARCHAR(30)  NOT NULL,           -- scalping | short_swings | medium_trends | momentum
    risk_box         VARCHAR(20)  NOT NULL,           -- speculation | growth | investment | big_strategy
    generation       INTEGER      NOT NULL DEFAULT 1, -- رقم الجيل
    dna              JSONB        NOT NULL,           -- الشيفرة الجينية الكاملة (JSON)
    fitness_score    DECIMAL(10,6) DEFAULT 0.0,       -- درجة التقييم الأخيرة
    status           VARCHAR(20)  NOT NULL DEFAULT 'pending',
                                                      -- pending | evaluated | elite | retired
    parent_a_hash    VARCHAR(64),                     -- hash الأب (للتزاوج)
    parent_b_hash    VARCHAR(64),                     -- hash الأم (للتزاوج)
    mutation_count   INTEGER      DEFAULT 0,          -- عدد الطفرات المطبقة
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_gs_symbol_obj
    ON genetic.strategies (symbol, profit_objective);
CREATE INDEX IF NOT EXISTS idx_gs_fitness
    ON genetic.strategies (fitness_score DESC);
CREATE INDEX IF NOT EXISTS idx_gs_status
    ON genetic.strategies (status);
CREATE INDEX IF NOT EXISTS idx_gs_generation
    ON genetic.strategies (generation DESC);
CREATE INDEX IF NOT EXISTS idx_gs_risk_box
    ON genetic.strategies (risk_box);
CREATE INDEX IF NOT EXISTS idx_gs_dna
    ON genetic.strategies USING GIN (dna);

-- Trigger: تحديث updated_at تلقائياً
CREATE OR REPLACE FUNCTION genetic.update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_gs_updated_at ON genetic.strategies;
CREATE TRIGGER trg_gs_updated_at
    BEFORE UPDATE ON genetic.strategies
    FOR EACH ROW EXECUTE FUNCTION genetic.update_updated_at();

\echo '✅ genetic.strategies created'

-- ── 3. جدول نتائج الأداء (strategy_performance) ────────────
-- يخزن نتائج كل backtest لكل استراتيجية على كل سهم
CREATE TABLE IF NOT EXISTS genetic.performance (
    id               BIGSERIAL    PRIMARY KEY,
    strategy_hash    VARCHAR(64)  NOT NULL
                     REFERENCES genetic.strategies(strategy_hash) ON DELETE CASCADE,
    symbol           VARCHAR(20)  NOT NULL,
    profit_objective VARCHAR(30)  NOT NULL,
    -- ── مقاييس الأداء الأساسية ──
    total_profit_pct DECIMAL(10,4) DEFAULT 0.0,   -- إجمالي الربح %
    win_rate         DECIMAL(6,4)  DEFAULT 0.0,   -- نسبة الصفقات الرابحة (0-1)
    total_trades     INTEGER       DEFAULT 0,      -- إجمالي الصفقات
    avg_profit_pct   DECIMAL(10,4) DEFAULT 0.0,   -- متوسط الربح لكل صفقة %
    max_drawdown_pct DECIMAL(10,4) DEFAULT 0.0,   -- أقصى تراجع %
    sharpe_ratio     DECIMAL(10,4) DEFAULT 0.0,   -- Sharpe Ratio
    profit_factor    DECIMAL(10,4) DEFAULT 0.0,   -- Profit Factor
    avg_duration_min INTEGER       DEFAULT 0,      -- متوسط مدة الصفقة (دقائق)
    -- ── درجة التقييم المحسوبة ──
    fitness_score    DECIMAL(10,6) DEFAULT 0.0,   -- الدرجة النهائية المحسوبة
    fitness_formula  TEXT,                         -- المعادلة المستخدمة (للتوثيق)
    -- ── فترة الاختبار ──
    backtest_start   TIMESTAMPTZ,
    backtest_end     TIMESTAMPTZ,
    candles_count    INTEGER       DEFAULT 0,
    -- ── metadata ──
    evaluated_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    evaluator_version VARCHAR(10) DEFAULT '1.0'
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_gp_hash
    ON genetic.performance (strategy_hash);
CREATE INDEX IF NOT EXISTS idx_gp_symbol_obj
    ON genetic.performance (symbol, profit_objective);
CREATE INDEX IF NOT EXISTS idx_gp_fitness
    ON genetic.performance (fitness_score DESC);
CREATE INDEX IF NOT EXISTS idx_gp_evaluated_at
    ON genetic.performance (evaluated_at DESC);

\echo '✅ genetic.performance created'

-- ── 4. جدول تاريخ الأجيال (evolution_log) ──────────────────
-- يسجل ملخص كل جيل: أفضل درجة، متوسط الدرجات، عدد النخبة
CREATE TABLE IF NOT EXISTS genetic.evolution_log (
    id               BIGSERIAL    PRIMARY KEY,
    symbol           VARCHAR(20)  NOT NULL,
    profit_objective VARCHAR(30)  NOT NULL,
    generation       INTEGER      NOT NULL,
    population_size  INTEGER      NOT NULL,
    best_fitness     DECIMAL(10,6) DEFAULT 0.0,
    avg_fitness      DECIMAL(10,6) DEFAULT 0.0,
    elite_count      INTEGER       DEFAULT 0,
    best_hash        VARCHAR(64),                  -- hash أفضل استراتيجية في الجيل
    run_duration_sec INTEGER       DEFAULT 0,
    logged_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_elog_symbol_obj
    ON genetic.evolution_log (symbol, profit_objective, generation DESC);

\echo '✅ genetic.evolution_log created'

-- ── 5. Permissions ──────────────────────────────────────────
GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA genetic TO alpha_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA genetic TO alpha_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA genetic
    GRANT ALL ON TABLES    TO alpha_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA genetic
    GRANT ALL ON SEQUENCES TO alpha_user;

\echo '✅ Permissions granted on genetic schema'

-- ── 6. Verification ─────────────────────────────────────────
\echo ''
\echo '══════════════════════════════════════════════════════'
\echo '📋 Genetic Engine Tables:'
\echo '══════════════════════════════════════════════════════'
SELECT
    schemaname AS "Schema",
    tablename  AS "Table",
    tableowner AS "Owner"
FROM pg_tables
WHERE schemaname = 'genetic'
ORDER BY tablename;

\echo '✅ Migration 001 completed successfully!'
