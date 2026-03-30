-- ============================================================
-- Alpha-Engine2: Update Custom Retention Policies (v2)
-- ============================================================
-- This script creates a custom function and job to delete old data
-- from the unified `ohlcv` table based on the `timeframe` column.
-- This is necessary because standard policies apply to the whole table.
--
-- RETENTION POLICY (v2 - Long-Term Storage):
--   1m   → 365  days  (1 year)
--   5m   → 730  days  (2 years)
--   15m  → 1095 days  (3 years)
--   30m  → 1825 days  (5 years)
--   1h   → 2555 days  (7 years)
--   1d   → UNLIMITED  (never deleted)
-- ============================================================

\echo '============================================================'
\echo 'Alpha-Engine2: Updating Custom Retention Policies'
\echo '============================================================'

-- ============================================================
-- STEP 1: Drop old standard retention policies and jobs
-- ============================================================
\echo 'Step 1: Removing old standard retention policies...'

DO $$
DECLARE
    v_job_id INTEGER;
BEGIN
    -- Remove policy from the old ohlcv_realtime table
    SELECT job_id INTO v_job_id FROM timescaledb_information.jobs
    WHERE proc_name = 'policy_retention' AND hypertable_name = 'ohlcv_realtime';
    IF v_job_id IS NOT NULL THEN
        PERFORM delete_job(v_job_id);
        RAISE NOTICE '  ✅ Removed old retention policy for ohlcv_realtime';
    END IF;

    -- Remove policy from the new unified ohlcv table (if any)
    SELECT job_id INTO v_job_id FROM timescaledb_information.jobs
    WHERE proc_name = 'policy_retention' AND hypertable_name = 'ohlcv';
    IF v_job_id IS NOT NULL THEN
        PERFORM delete_job(v_job_id);
        RAISE NOTICE '  ✅ Removed old retention policy for ohlcv';
    END IF;

    -- Remove our old custom job if it exists
    SELECT job_id INTO v_job_id FROM timescaledb_information.jobs
    WHERE proc_name = 'apply_custom_retention';
    IF v_job_id IS NOT NULL THEN
        PERFORM delete_job(v_job_id);
        RAISE NOTICE '  ✅ Removed old custom retention job';
    END IF;
END $$;

-- ============================================================
-- STEP 2: Create the custom retention function
-- ============================================================
\echo 'Step 2: Creating custom retention function apply_custom_retention()...'

CREATE OR REPLACE FUNCTION apply_custom_retention(job_id INT, config JSONB) LANGUAGE PLPGSQL AS $$
DECLARE
    deleted_rows_1m BIGINT;
    deleted_rows_5m BIGINT;
    deleted_rows_15m BIGINT;
    deleted_rows_30m BIGINT;
    deleted_rows_1h BIGINT;
BEGIN
    RAISE NOTICE 'Running custom retention policy job...';

    -- 1m data: 365 days
    WITH deleted AS (
        DELETE FROM market_data.ohlcv
        WHERE timeframe = '1m' AND time < NOW() - INTERVAL '365 days'
        RETURNING *
    ) SELECT count(*) INTO deleted_rows_1m FROM deleted;
    RAISE NOTICE '  - 1m: Deleted % rows older than 365 days.', deleted_rows_1m;

    -- 5m data: 730 days
    WITH deleted AS (
        DELETE FROM market_data.ohlcv
        WHERE timeframe = '5m' AND time < NOW() - INTERVAL '730 days'
        RETURNING *
    ) SELECT count(*) INTO deleted_rows_5m FROM deleted;
    RAISE NOTICE '  - 5m: Deleted % rows older than 730 days.', deleted_rows_5m;

    -- 15m data: 1095 days
    WITH deleted AS (
        DELETE FROM market_data.ohlcv
        WHERE timeframe = '15m' AND time < NOW() - INTERVAL '1095 days'
        RETURNING *
    ) SELECT count(*) INTO deleted_rows_15m FROM deleted;
    RAISE NOTICE '  - 15m: Deleted % rows older than 1095 days.', deleted_rows_15m;

    -- 30m data: 1825 days
    WITH deleted AS (
        DELETE FROM market_data.ohlcv
        WHERE timeframe = '30m' AND time < NOW() - INTERVAL '1825 days'
        RETURNING *
    ) SELECT count(*) INTO deleted_rows_30m FROM deleted;
    RAISE NOTICE '  - 30m: Deleted % rows older than 1825 days.', deleted_rows_30m;

    -- 1h data: 2555 days
    WITH deleted AS (
        DELETE FROM market_data.ohlcv
        WHERE timeframe = '1h' AND time < NOW() - INTERVAL '2555 days'
        RETURNING *
    ) SELECT count(*) INTO deleted_rows_1h FROM deleted;
    RAISE NOTICE '  - 1h: Deleted % rows older than 2555 days.', deleted_rows_1h;

    RAISE NOTICE '  - 1d: UNLIMITED (no deletion).';
    RAISE NOTICE 'Custom retention policy job finished.';
END;
$$;

-- ============================================================
-- STEP 3: Schedule the custom function to run daily
-- ============================================================
\echo 'Step 3: Scheduling the custom retention job to run daily...'

SELECT add_job(
    'apply_custom_retention',
    '1 day',
    initial_start => NOW() + INTERVAL '5 minutes',
    if_not_exists => TRUE
);

\echo '✅ Custom retention job scheduled successfully.'

-- ============================================================
-- DONE
-- ============================================================
DO $$
BEGIN
    RAISE NOTICE '============================================================';
    RAISE NOTICE '✅ Custom retention policies scheduled successfully!';
    RAISE NOTICE '   The job will run daily to clean up old data.';
    RAISE NOTICE '============================================================';
END $$;
