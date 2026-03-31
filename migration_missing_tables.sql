-- ============================================================
-- Alpha-Engine2 — Migration: Missing Tables
-- ============================================================
-- الغرض:
--   إنشاء الجداول الناقصة التي يحتاجها الداشبورد ولم تُنشأ
--   في init_db.sql الأصلي، أو تعارضت بنيتها مع ما يتوقعه
--   dashboard/app.py.
--
-- الجداول المُنشأة:
--   1. strategies.signals   — إشارات التداول (BUY/SELL/HOLD)
--   2. bots.status          — حالة الـ 18 روبوت (مع بيانات افتراضية)
--   3. trading.performance  — سجل الصفقات والأداء المالي
--
-- الاستخدام:
--   # نسخ الملف إلى الـ container ثم تنفيذه:
--   docker compose cp migration_missing_tables.sql postgres:/tmp/
--   docker compose exec postgres psql \
--       -U alpha_user -d alpha_engine \
--       -f /tmp/migration_missing_tables.sql
--
-- ملاحظة: الملف آمن للتنفيذ المتكرر (IF NOT EXISTS + ON CONFLICT DO NOTHING)
-- ============================================================

\set ON_ERROR_STOP on
\echo '▶ Starting migration_missing_tables.sql ...'

-- ============================================================
-- 0. Schemas
--    (موجودة في init_db.sql لكن نُعيد إنشاءها بأمان)
-- ============================================================
CREATE SCHEMA IF NOT EXISTS strategies;
CREATE SCHEMA IF NOT EXISTS bots;
CREATE SCHEMA IF NOT EXISTS trading;
CREATE SCHEMA IF NOT EXISTS alerts;
CREATE SCHEMA IF NOT EXISTS analytics;

\echo '✅ Schemas ready'

-- ============================================================
-- 1. strategies.signals
-- ============================================================
-- الغرض: تخزين إشارات التداول الصادرة من كل الاستراتيجيات.
--
-- الأعمدة التي يستعلم عنها الداشبورد:
--   timestamp, symbol, strategy_name, signal_type, confidence, price
--
-- signal_type: BUY | SELL | HOLD
-- confidence:  قيمة بين 0.0 و 1.0 تعبّر عن ثقة الاستراتيجية بالإشارة
-- ============================================================
CREATE TABLE IF NOT EXISTS strategies.signals (
    id              BIGSERIAL       PRIMARY KEY,

    -- توقيت الإشارة (بتوقيت UTC)
    timestamp       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    -- الاستراتيجية التي أصدرت الإشارة (مثال: RSI_Breakout, MACD_Cross)
    strategy_name   VARCHAR(100)    NOT NULL,

    -- رمز السهم (مثال: 2222, 1120)
    symbol          VARCHAR(20)     NOT NULL,

    -- نوع الإشارة
    signal_type     VARCHAR(10)     NOT NULL
                    CHECK (signal_type IN ('BUY', 'SELL', 'HOLD')),

    -- درجة الثقة بالإشارة (0.0 = لا ثقة، 1.0 = ثقة تامة)
    confidence      NUMERIC(5, 4)   NOT NULL DEFAULT 0.0
                    CHECK (confidence >= 0.0 AND confidence <= 1.0),

    -- سعر السهم وقت الإشارة
    price           NUMERIC(12, 4)  NOT NULL DEFAULT 0.0,

    -- الإطار الزمني المستخدم في الإشارة (1m, 5m, 15m, 1h, 1d)
    timeframe       VARCHAR(10)     DEFAULT '1d',

    -- بيانات إضافية (مؤشرات، أسباب الإشارة، إلخ) بصيغة JSON
    metadata        JSONB,

    -- ملاحظات نصية اختيارية
    notes           TEXT,

    -- وقت إدراج السجل في DB
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- Index رئيسي: فلترة بالتاريخ (أكثر استعلامات الداشبورد)
CREATE INDEX IF NOT EXISTS idx_signals_timestamp
    ON strategies.signals (timestamp DESC);

-- Index للفلترة بالاستراتيجية + التاريخ
CREATE INDEX IF NOT EXISTS idx_signals_strategy_ts
    ON strategies.signals (strategy_name, timestamp DESC);

-- Index للفلترة بالسهم + التاريخ
CREATE INDEX IF NOT EXISTS idx_signals_symbol_ts
    ON strategies.signals (symbol, timestamp DESC);

-- Index للفلترة بنوع الإشارة
CREATE INDEX IF NOT EXISTS idx_signals_type
    ON strategies.signals (signal_type, timestamp DESC);

-- Index للفلترة بالثقة (لصفحة الإشارات: confidence >= X)
CREATE INDEX IF NOT EXISTS idx_signals_confidence
    ON strategies.signals (confidence DESC);

\echo '✅ strategies.signals created'

-- ============================================================
-- 2. bots.status
-- ============================================================
-- الغرض: تتبع حالة كل روبوت في النظام في الوقت الفعلي.
--
-- الأعمدة التي يستعلم عنها الداشبورد:
--   bot_name, status, last_run, error_message
--
-- status القيم الممكنة:
--   running  → الروبوت يعمل الآن
--   stopped  → الروبوت متوقف
--   error    → الروبوت توقف بسبب خطأ
--   idle     → الروبوت يعمل لكن في وضع الانتظار
-- ============================================================
CREATE TABLE IF NOT EXISTS bots.status (
    -- اسم الروبوت (مفتاح أساسي — كل روبوت له سجل واحد فقط)
    bot_name        VARCHAR(100)    PRIMARY KEY,

    -- الحالة الحالية
    status          VARCHAR(20)     NOT NULL DEFAULT 'stopped'
                    CHECK (status IN ('running', 'stopped', 'error', 'idle', 'RUNNING', 'STOPPED', 'ERROR')),

    -- آخر مرة نفّذ فيها الروبوت دورته
    last_run        TIMESTAMPTZ,

    -- الدورة القادمة المجدولة (اختياري)
    next_run        TIMESTAMPTZ,

    -- رسالة الخطأ الأخيرة (NULL إذا لم يكن هناك خطأ)
    error_message   TEXT,

    -- بيانات إضافية (إحصائيات، إعدادات، إلخ)
    metadata        JSONB,

    -- آخر تحديث للسجل
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- Trigger: تحديث updated_at تلقائياً عند كل UPDATE
CREATE OR REPLACE FUNCTION bots.update_status_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_bot_status ON bots.status;
CREATE TRIGGER trigger_update_bot_status
    BEFORE UPDATE ON bots.status
    FOR EACH ROW
    EXECUTE FUNCTION bots.update_status_timestamp();

-- ── إضافة الأعمدة الناقصة إذا لم تكن موجودة ────────────────────
-- (الجدول قد يكون موجوداً من init_db.sql بدون هذه الأعمدة)
DO $$
BEGIN
    -- عمود next_run
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'bots'
          AND table_name   = 'status'
          AND column_name  = 'next_run'
    ) THEN
        ALTER TABLE bots.status ADD COLUMN next_run TIMESTAMPTZ;
        RAISE NOTICE 'Added column bots.status.next_run';
    END IF;

    -- عمود metadata
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'bots'
          AND table_name   = 'status'
          AND column_name  = 'metadata'
    ) THEN
        ALTER TABLE bots.status ADD COLUMN metadata JSONB;
        RAISE NOTICE 'Added column bots.status.metadata';
    END IF;

    -- عمود updated_at (قد يكون موجوداً باسم مختلف)
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'bots'
          AND table_name   = 'status'
          AND column_name  = 'updated_at'
    ) THEN
        ALTER TABLE bots.status ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
        RAISE NOTICE 'Added column bots.status.updated_at';
    END IF;
END
$$;

-- ── البيانات الافتراضية: الـ 18 روبوت ────────────────────────
-- يُدرج كل روبوت بحالة 'stopped' إذا لم يكن موجوداً
-- ON CONFLICT DO NOTHING: لا يُعدّل الروبوتات التي تم تحديثها
INSERT INTO bots.status (bot_name, status, last_run, next_run, metadata) VALUES

    -- ── جمع البيانات ──────────────────────────────────────────
    ('market_reporter',
     'stopped', NULL, NULL,
     '{"description": "يجمع بيانات السوق اللحظية عبر WebSocket من Sahmk API ويحفظها في TimescaleDB"}'),

    ('data_importer',
     'stopped', NULL, NULL,
     '{"description": "يستورد البيانات التاريخية ويملأ الفجوات في قاعدة البيانات"}'),

    -- ── التحليل الفني ─────────────────────────────────────────
    ('technical_miner',
     'stopped', NULL, NULL,
     '{"description": "يحسب المؤشرات الفنية (RSI, MACD, Bollinger, ATR...) ويحفظها في market_data.technical_indicators"}'),

    ('multiframe_confirmer',
     'stopped', NULL, NULL,
     '{"description": "يؤكد الإشارات عبر أطر زمنية متعددة (1m, 5m, 15m, 1h, 1d)"}'),

    -- ── الاستراتيجيات والإشارات ───────────────────────────────
    ('strategic_analyzer',
     'stopped', NULL, NULL,
     '{"description": "يُشغّل الاستراتيجيات ويولّد إشارات BUY/SELL/HOLD في strategies.signals"}'),

    ('consolidation_hunter',
     'stopped', NULL, NULL,
     '{"description": "يبحث عن أنماط التماسك والاختراق في الأسهم"}'),

    ('behavioral_analyzer',
     'stopped', NULL, NULL,
     '{"description": "يحلل سلوك السوق وأنماط التداول غير الاعتيادية"}'),

    -- ── الذكاء الاصطناعي ──────────────────────────────────────
    ('scientist',
     'stopped', NULL, NULL,
     '{"description": "يُدرّب نماذج ML ويُقيّم أداءها على البيانات التاريخية"}'),

    ('self_trainer',
     'stopped', NULL, NULL,
     '{"description": "يُعيد تدريب النماذج تلقائياً بناءً على أداء الإشارات السابقة"}'),

    ('freqai_manager',
     'stopped', NULL, NULL,
     '{"description": "يُدير نماذج FreqAI ويتكامل مع Freqtrade للتداول الآلي"}'),

    -- ── إدارة المخاطر ─────────────────────────────────────────
    ('risk_guardian',
     'stopped', NULL, NULL,
     '{"description": "يراقب المخاطر ويُوقف التداول عند تجاوز حدود الخسارة اليومية أو الـ drawdown"}'),

    -- ── المراقبة والتقارير ─────────────────────────────────────
    ('monitor',
     'stopped', NULL, NULL,
     '{"description": "يراقب صحة النظام ويُرسل تنبيهات عند وجود مشاكل"}'),

    ('health_monitor',
     'stopped', NULL, NULL,
     '{"description": "يتتبع استخدام CPU/RAM/Disk ويحفظ إحصائيات الأداء في analytics.system_health"}'),

    ('weekly_reviewer',
     'stopped', NULL, NULL,
     '{"description": "يُنشئ تقرير أسبوعي شامل عن أداء الاستراتيجيات والنظام"}'),

    -- ── الخدمات المساعدة ──────────────────────────────────────
    ('backup_manager',
     'stopped', NULL, NULL,
     '{"description": "يُنفّذ نسخ احتياطية دورية لقاعدة البيانات والإعدادات"}'),

    ('parameter_editor',
     'stopped', NULL, NULL,
     '{"description": "يُدير معاملات النظام القابلة للتعديل في bots.parameters"}'),

    ('dashboard_service',
     'stopped', NULL, NULL,
     '{"description": "خدمة Streamlit Dashboard — لوحة التحكم الرئيسية"}'),

    ('silent_mode_manager',
     'stopped', NULL, NULL,
     '{"description": "يُدير وضع الصمت: يوقف التنبيهات مع الاستمرار في جمع البيانات"}')

ON CONFLICT (bot_name) DO NOTHING;

\echo '✅ bots.status created and seeded with 18 bots'

-- ============================================================
-- 3. trading.performance
-- ============================================================
-- الغرض: تسجيل نتائج الصفقات الفعلية أو المحاكاة لقياس الأداء.
--
-- الأعمدة التي يستعلم عنها الداشبورد (صفحة الأداء):
--   timestamp, strategy_name, symbol, pnl, pnl_pct
--
-- pnl     = Profit & Loss بالريال السعودي
-- pnl_pct = نسبة الربح/الخسارة مئوياً
-- ============================================================
CREATE SCHEMA IF NOT EXISTS trading;

CREATE TABLE IF NOT EXISTS trading.performance (
    id              BIGSERIAL       PRIMARY KEY,

    -- توقيت إغلاق الصفقة
    timestamp       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    -- الاستراتيجية التي فتحت الصفقة
    strategy_name   VARCHAR(100)    NOT NULL,

    -- رمز السهم
    symbol          VARCHAR(20)     NOT NULL,

    -- سعر الدخول والخروج
    entry_price     NUMERIC(12, 4),
    exit_price      NUMERIC(12, 4),

    -- الكمية (عدد الأسهم)
    quantity        INTEGER         DEFAULT 1,

    -- الربح/الخسارة الصافي بالريال
    pnl             NUMERIC(14, 4),

    -- الربح/الخسارة كنسبة مئوية من رأس المال
    pnl_pct         NUMERIC(8, 4),

    -- نوع الصفقة
    trade_type      VARCHAR(10)     CHECK (trade_type IN ('BUY', 'SELL', 'LONG', 'SHORT')),

    -- مصدر الصفقة (freqtrade, manual, backtest, paper)
    source          VARCHAR(50)     DEFAULT 'manual',

    -- الإطار الزمني المستخدم
    timeframe       VARCHAR(10),

    -- بيانات إضافية (stop_loss, take_profit, fees, إلخ)
    metadata        JSONB,

    -- ملاحظات
    notes           TEXT,

    -- وقت إدراج السجل
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- Index رئيسي: ترتيب بالتاريخ (أكثر استعلامات الداشبورد)
CREATE INDEX IF NOT EXISTS idx_performance_timestamp
    ON trading.performance (timestamp DESC);

-- Index للفلترة بالاستراتيجية
CREATE INDEX IF NOT EXISTS idx_performance_strategy_ts
    ON trading.performance (strategy_name, timestamp DESC);

-- Index للفلترة بالسهم
CREATE INDEX IF NOT EXISTS idx_performance_symbol_ts
    ON trading.performance (symbol, timestamp DESC);

-- Index للفلترة بالربح/الخسارة (لحساب إحصائيات الفوز/الخسارة)
CREATE INDEX IF NOT EXISTS idx_performance_pnl
    ON trading.performance (pnl);

\echo '✅ trading.performance created'

-- ============================================================
-- 4. Grant Permissions
--    (يضمن أن alpha_user يملك صلاحيات كاملة على الجداول الجديدة)
-- ============================================================
GRANT USAGE  ON SCHEMA strategies TO alpha_user;
GRANT USAGE  ON SCHEMA bots       TO alpha_user;
GRANT USAGE  ON SCHEMA trading    TO alpha_user;

GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA strategies TO alpha_user;
GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA bots       TO alpha_user;
GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA trading    TO alpha_user;

GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA strategies TO alpha_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA bots       TO alpha_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA trading    TO alpha_user;

-- صلاحيات للجداول المستقبلية
ALTER DEFAULT PRIVILEGES IN SCHEMA strategies
    GRANT ALL ON TABLES    TO alpha_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA bots
    GRANT ALL ON TABLES    TO alpha_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA trading
    GRANT ALL ON TABLES    TO alpha_user;

\echo '✅ Permissions granted'

-- ============================================================
-- 5. Verification — عرض الجداول المُنشأة للتأكيد
-- ============================================================
\echo ''
\echo '══════════════════════════════════════════════════════'
\echo '📋 Tables in target schemas:'
\echo '══════════════════════════════════════════════════════'

SELECT
    schemaname   AS "Schema",
    tablename    AS "Table",
    tableowner   AS "Owner"
FROM pg_tables
WHERE schemaname IN ('strategies', 'bots', 'trading')
ORDER BY schemaname, tablename;

\echo ''
\echo '══════════════════════════════════════════════════════'
\echo '🤖 Bots in bots.status:'
\echo '══════════════════════════════════════════════════════'

SELECT
    bot_name    AS "Bot Name",
    status      AS "Status",
    last_run    AS "Last Run"
FROM bots.status
ORDER BY bot_name;

\echo ''
\echo '══════════════════════════════════════════════════════'
\echo '✅ Migration completed successfully!'
\echo '══════════════════════════════════════════════════════'
