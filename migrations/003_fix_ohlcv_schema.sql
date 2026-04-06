-- ============================================================
-- Migration 003: Fix market_data.ohlcv Schema
-- إضافة عمود source إذا لم يكن موجوداً
-- إصلاح NOT NULL constraints لتتوافق مع INSERT في market_reporter
-- ============================================================

-- 1. إضافة عمود source إذا لم يكن موجوداً
ALTER TABLE market_data.ohlcv
    ADD COLUMN IF NOT EXISTS source VARCHAR(100) DEFAULT 'unknown';

-- 2. ضمان أن name لها default (لتجنب NOT NULL error)
ALTER TABLE market_data.ohlcv
    ALTER COLUMN name SET DEFAULT 'Unknown';

-- 3. ضمان أن open_interest لها default
ALTER TABLE market_data.ohlcv
    ALTER COLUMN open_interest SET DEFAULT 0;

-- 4. إضافة unique index إذا لم يكن موجوداً (للـ ON CONFLICT)
CREATE UNIQUE INDEX IF NOT EXISTS idx_ohlcv_time_symbol_timeframe
    ON market_data.ohlcv (time, symbol, timeframe);

-- 5. تحقق من النتيجة
SELECT
    column_name,
    data_type,
    column_default,
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'market_data'
  AND table_name   = 'ohlcv'
ORDER BY ordinal_position;

\echo '✅ Migration 003: ohlcv schema fixed — source column added'
