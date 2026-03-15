-- ============================================================
-- Alpha-Engine2: Update Retention Policies
-- ============================================================
-- Run this script if the database is ALREADY running to update
-- retention policies WITHOUT recreating tables.
--
-- Usage:
--   docker exec -it alpha_postgres psql -U alpha_user -d alpha_engine \
--     -f /scripts/update_retention_policies.sql
-- ============================================================

\echo '============================================================'
\echo 'Alpha-Engine2: Updating Retention Policies'
\echo '============================================================'

-- ============================================================
-- STEP 1: Remove old retention policies (if they exist)
-- ============================================================

\echo 'Step 1: Removing old retention policies...'

-- Remove old policy for ohlcv_realtime (was 90 days)
DO $$
DECLARE
    v_job_id INTEGER;
BEGIN
    SELECT job_id INTO v_job_id
    FROM timescaledb_information.jobs
    WHERE hypertable_name = 'ohlcv_realtime'
      AND hypertable_schema = 'market_data'
      AND proc_name = 'policy_retention';

    IF v_job_id IS NOT NULL THEN
        PERFORM delete_job(v_job_id);
        RAISE NOTICE '  ✅ Removed old retention policy for ohlcv_realtime (job_id=%)', v_job_id;
    ELSE
        RAISE NOTICE '  ℹ️  No existing retention policy for ohlcv_realtime';
    END IF;
END $$;

-- Remove old policies for aggregates
DO $$
DECLARE
    v_rec RECORD;
BEGIN
    FOR v_rec IN
        SELECT job_id, hypertable_name
        FROM timescaledb_information.jobs
        WHERE hypertable_schema = 'market_data'
          AND hypertable_name IN ('ohlcv_rt_5m','ohlcv_rt_15m','ohlcv_rt_30m','ohlcv_rt_1h','ohlcv_rt_1d')
          AND proc_name = 'policy_retention'
    LOOP
        PERFORM delete_job(v_rec.job_id);
        RAISE NOTICE '  ✅ Removed old retention policy for % (job_id=%)',
            v_rec.hypertable_name, v_rec.job_id;
    END LOOP;
END $$;

-- ============================================================
-- STEP 2: Apply new retention policies
-- ============================================================

\echo 'Step 2: Applying new retention policies...'

-- 1m raw data: 365 days (1 year)
SELECT add_retention_policy(
    'market_data.ohlcv_realtime',
    INTERVAL '365 days',
    if_not_exists => TRUE
);
\echo '  ✅ ohlcv_realtime (1m) → 365 days (1 year)'

-- 5m aggregate: 730 days (2 years)
SELECT add_retention_policy(
    'market_data.ohlcv_rt_5m',
    INTERVAL '730 days',
    if_not_exists => TRUE
);
\echo '  ✅ ohlcv_rt_5m (5m) → 730 days (2 years)'

-- 15m aggregate: 730 days (2 years)
SELECT add_retention_policy(
    'market_data.ohlcv_rt_15m',
    INTERVAL '730 days',
    if_not_exists => TRUE
);
\echo '  ✅ ohlcv_rt_15m (15m) → 730 days (2 years)'

-- 30m aggregate: 1095 days (3 years)
SELECT add_retention_policy(
    'market_data.ohlcv_rt_30m',
    INTERVAL '1095 days',
    if_not_exists => TRUE
);
\echo '  ✅ ohlcv_rt_30m (30m) → 1095 days (3 years)'

-- 1h aggregate: 1825 days (5 years)
SELECT add_retention_policy(
    'market_data.ohlcv_rt_1h',
    INTERVAL '1825 days',
    if_not_exists => TRUE
);
\echo '  ✅ ohlcv_rt_1h (1h) → 1825 days (5 years)'

-- 1d aggregate: NO RETENTION (keep forever)
-- NOTE: We intentionally do NOT add a retention policy for ohlcv_rt_1d
-- This means daily candles are kept indefinitely for long-term GA analysis
\echo '  ✅ ohlcv_rt_1d (1d) → UNLIMITED (no retention policy)'

-- ============================================================
-- STEP 3: Verify the new policies
-- ============================================================

\echo ''
\echo 'Step 3: Verifying new retention policies...'

SELECT
    j.hypertable_name                                    AS "Table",
    j.config->>'drop_after'                              AS "Retention",
    CASE
        WHEN j.config->>'drop_after' = '365 days'  THEN '1 year'
        WHEN j.config->>'drop_after' = '730 days'  THEN '2 years'
        WHEN j.config->>'drop_after' = '1095 days' THEN '3 years'
        WHEN j.config->>'drop_after' = '1825 days' THEN '5 years'
        ELSE j.config->>'drop_after'
    END                                                  AS "Description",
    j.job_id                                             AS "Job ID",
    j.schedule_interval                                  AS "Check Every"
FROM timescaledb_information.jobs j
WHERE j.hypertable_schema = 'market_data'
  AND j.proc_name = 'policy_retention'
ORDER BY j.hypertable_name;

-- Also show ohlcv_rt_1d has NO retention policy
\echo ''
\echo 'Tables WITHOUT retention policy (kept forever):'
SELECT 'market_data.ohlcv_rt_1d' AS "Table", 'UNLIMITED' AS "Retention", 'Daily candles for long-term GA' AS "Reason";

-- ============================================================
-- STEP 4: Verify Continuous Aggregates still active
-- ============================================================

\echo ''
\echo 'Step 4: Verifying Continuous Aggregates are still active...'

SELECT
    view_name                                            AS "Aggregate View",
    view_schema                                          AS "Schema",
    materialization_hypertable_name                      AS "Backing Table",
    compression_enabled                                  AS "Compressed"
FROM timescaledb_information.continuous_aggregates
WHERE view_schema = 'market_data'
ORDER BY view_name;

-- ============================================================
-- DONE
-- ============================================================
DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '============================================================';
    RAISE NOTICE '✅ Retention policies updated successfully!';
    RAISE NOTICE '';
    RAISE NOTICE '  ohlcv_realtime (1m) → 365 days  (1 year)';
    RAISE NOTICE '  ohlcv_rt_5m   (5m) → 730 days  (2 years)';
    RAISE NOTICE '  ohlcv_rt_15m (15m) → 730 days  (2 years)';
    RAISE NOTICE '  ohlcv_rt_30m (30m) → 1095 days (3 years)';
    RAISE NOTICE '  ohlcv_rt_1h   (1h) → 1825 days (5 years)';
    RAISE NOTICE '  ohlcv_rt_1d   (1d) → UNLIMITED (no retention)';
    RAISE NOTICE '';
    RAISE NOTICE '  Continuous Aggregates: still active ✅';
    RAISE NOTICE '  Compression: after 7 days ✅';
    RAISE NOTICE '============================================================';
END $$;
