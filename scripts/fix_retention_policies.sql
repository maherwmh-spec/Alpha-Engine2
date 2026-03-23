-- ============================================================
-- Alpha-Engine2: Fix Retention Policies
-- يطبق سياسات الاحتفاظ على الجداول الفعلية الموجودة في قاعدة البيانات
-- الجداول الفعلية: market_data.ohlcv (hypertable أصلي)
-- Continuous Aggregates: ohlcv_5m, ohlcv_15m, ohlcv_30m, ohlcv_1h, ohlcv_1d
-- ============================================================

\echo '============================================================'
\echo 'Alpha-Engine2: Applying Retention Policies on Actual Tables'
\echo '============================================================'

-- ============================================================
-- Step 1: إزالة سياسات الاحتفاظ القديمة إن وجدت
-- ============================================================
\echo 'Step 1: Removing any existing retention policies...'

DO $$
DECLARE
    v_job_id INTEGER;
BEGIN
    -- إزالة retention policy من ohlcv (الجدول الأصلي)
    SELECT job_id INTO v_job_id
    FROM timescaledb_information.jobs
    WHERE hypertable_name = 'ohlcv'
      AND proc_name = 'execute_retention_policy'
    LIMIT 1;

    IF v_job_id IS NOT NULL THEN
        PERFORM remove_retention_policy('market_data.ohlcv');
        RAISE NOTICE '✅ Removed old retention policy from ohlcv';
    ELSE
        RAISE NOTICE 'ℹ️ No existing retention policy for ohlcv';
    END IF;
END $$;

DO $$
DECLARE
    v_job_id INTEGER;
    v_agg TEXT;
    v_aggs TEXT[] := ARRAY['ohlcv_5m', 'ohlcv_15m', 'ohlcv_30m', 'ohlcv_1h', 'ohlcv_1d'];
BEGIN
    FOREACH v_agg IN ARRAY v_aggs LOOP
        SELECT job_id INTO v_job_id
        FROM timescaledb_information.jobs
        WHERE hypertable_name = v_agg
          AND proc_name = 'execute_retention_policy'
        LIMIT 1;

        IF v_job_id IS NOT NULL THEN
            EXECUTE format('SELECT remove_retention_policy(''market_data.%I'')', v_agg);
            RAISE NOTICE '✅ Removed old retention policy from %', v_agg;
        ELSE
            RAISE NOTICE 'ℹ️ No existing retention policy for %', v_agg;
        END IF;
    END LOOP;
END $$;

-- ============================================================
-- Step 2: تطبيق retention policy على الجدول الأصلي ohlcv (1m raw data)
-- الاحتفاظ: 365 يوم (سنة كاملة)
-- ============================================================
\echo 'Step 2: Applying retention policy on ohlcv (1m raw data) → 365 days...'

SELECT add_retention_policy(
    'market_data.ohlcv',
    INTERVAL '365 days',
    if_not_exists => TRUE
);
\echo '  ✅ ohlcv (raw 1m) → 365 days (1 year)'

-- ============================================================
-- Step 3: تطبيق retention policies على Continuous Aggregates
-- ============================================================
\echo 'Step 3: Applying retention policies on Continuous Aggregates...'

-- ohlcv_5m → 730 يوم (سنتان)
SELECT add_retention_policy(
    'market_data.ohlcv_5m',
    INTERVAL '730 days',
    if_not_exists => TRUE
);
\echo '  ✅ ohlcv_5m → 730 days (2 years)'

-- ohlcv_15m → 730 يوم (سنتان)
SELECT add_retention_policy(
    'market_data.ohlcv_15m',
    INTERVAL '730 days',
    if_not_exists => TRUE
);
\echo '  ✅ ohlcv_15m → 730 days (2 years)'

-- ohlcv_30m → 1095 يوم (3 سنوات)
SELECT add_retention_policy(
    'market_data.ohlcv_30m',
    INTERVAL '1095 days',
    if_not_exists => TRUE
);
\echo '  ✅ ohlcv_30m → 1095 days (3 years)'

-- ohlcv_1h → 1825 يوم (5 سنوات)
SELECT add_retention_policy(
    'market_data.ohlcv_1h',
    INTERVAL '1825 days',
    if_not_exists => TRUE
);
\echo '  ✅ ohlcv_1h → 1825 days (5 years)'

-- ohlcv_1d → بدون retention (للأبد)
-- لا نضيف retention policy لـ ohlcv_1d
\echo '  ✅ ohlcv_1d → UNLIMITED (no retention policy)'

-- ============================================================
-- Step 4: التحقق من تطبيق السياسات
-- ============================================================
\echo 'Step 4: Verifying retention policies...'

SELECT
    h.hypertable_name AS "Table",
    j.config->>'drop_after' AS "Retention",
    j.schedule_interval AS "Check Every",
    j.job_id AS "Job ID"
FROM timescaledb_information.hypertables h
JOIN timescaledb_information.jobs j
    ON j.hypertable_name = h.hypertable_name
   AND j.proc_name = 'execute_retention_policy'
ORDER BY h.hypertable_name;

\echo ''
\echo 'Tables WITHOUT retention policy (kept forever):'
SELECT
    h.hypertable_name AS "Table",
    'UNLIMITED' AS "Retention",
    'No retention policy applied' AS "Reason"
FROM timescaledb_information.hypertables h
WHERE h.hypertable_name NOT IN (
    SELECT hypertable_name
    FROM timescaledb_information.jobs
    WHERE proc_name = 'execute_retention_policy'
)
ORDER BY h.hypertable_name;

-- ============================================================
-- Step 5: التحقق من Continuous Aggregates
-- ============================================================
\echo 'Step 5: Verifying Continuous Aggregates...'

SELECT
    view_name AS "Aggregate View",
    view_schema AS "Schema",
    materialization_hypertable_name AS "Backing Table",
    compression_enabled AS "Compressed"
FROM timescaledb_information.continuous_aggregates
ORDER BY view_name;

-- ============================================================
-- Step 6: تطبيق ضغط البيانات القديمة (Compression) على ohlcv
-- ============================================================
\echo 'Step 6: Enabling compression on ohlcv (compress data older than 7 days)...'

DO $$
BEGIN
    -- تفعيل compression على الجدول الأصلي
    ALTER TABLE market_data.ohlcv SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'symbol,timeframe',
        timescaledb.compress_orderby = 'time DESC'
    );
    RAISE NOTICE '✅ Compression enabled on ohlcv';
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'ℹ️ Compression already configured or not applicable: %', SQLERRM;
END $$;

SELECT add_compression_policy(
    'market_data.ohlcv',
    INTERVAL '7 days',
    if_not_exists => TRUE
);
\echo '  ✅ ohlcv: compress data older than 7 days'

-- ============================================================
-- الملخص النهائي
-- ============================================================
DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '============================================================';
    RAISE NOTICE '✅ Retention policies applied successfully!';
    RAISE NOTICE '';
    RAISE NOTICE 'market_data.ohlcv (raw 1m)  → 365 days (1 year)';
    RAISE NOTICE 'market_data.ohlcv_5m        → 730 days (2 years)';
    RAISE NOTICE 'market_data.ohlcv_15m       → 730 days (2 years)';
    RAISE NOTICE 'market_data.ohlcv_30m       → 1095 days (3 years)';
    RAISE NOTICE 'market_data.ohlcv_1h        → 1825 days (5 years)';
    RAISE NOTICE 'market_data.ohlcv_1d        → UNLIMITED (no retention)';
    RAISE NOTICE '';
    RAISE NOTICE 'Continuous Aggregates: still active ✅';
    RAISE NOTICE 'Compression on ohlcv: after 7 days ✅';
    RAISE NOTICE '============================================================';
END $$;
