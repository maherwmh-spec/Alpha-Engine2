-- =============================================================================
-- Migration 004: إضافة عمود name إلى جدول market_data.symbols
-- =============================================================================
-- FIX #4: جدول symbols لا يحتوي على عمود name مخصص للاسم المعروض.
--
-- الجدول الحالي (من migration 002) يحتوي على:
--   symbol, name_ar, name_en, sector_id, sector_name_ar,
--   market, is_active, listing_date, isin, last_synced_at,
--   created_at, updated_at
--
-- هذا الـ migration يضيف:
--   name TEXT — الاسم المعروض (عربي بالأساس، يُملأ من name_ar)
--
-- آمن للتشغيل أكثر من مرة (ADD COLUMN IF NOT EXISTS)
-- =============================================================================

BEGIN;

-- ── الخطوة 1: إضافة عمود name (آمن للتكرار) ─────────────────────────────────
ALTER TABLE market_data.symbols
    ADD COLUMN IF NOT EXISTS name TEXT;

-- ── الخطوة 2: ملء name من name_ar للصفوف الموجودة ───────────────────────────
UPDATE market_data.symbols
SET name = name_ar
WHERE name IS NULL
  AND name_ar IS NOT NULL
  AND name_ar <> '';

-- للصفوف التي ليس لها name_ar، نستخدم name_en كبديل
UPDATE market_data.symbols
SET name = name_en
WHERE name IS NULL
  AND name_en IS NOT NULL
  AND name_en <> '';

-- ── الخطوة 3: إنشاء index على عمود name ─────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_symbols_name
    ON market_data.symbols (name)
    WHERE name IS NOT NULL;

-- ── الخطوة 4: إضافة comment توثيقي ──────────────────────────────────────────
COMMENT ON COLUMN market_data.symbols.name IS
    'الاسم المعروض للشركة (عربي بالأساس). يُستخدم في الداش بورد والتقارير. يُملأ من sync_symbols.py.';

COMMIT;

-- ── التحقق من نجاح المهاجرة ──────────────────────────────────────────────────
DO $$
DECLARE
    col_exists      BOOLEAN;
    total_rows      INTEGER;
    rows_with_name  INTEGER;
BEGIN
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'market_data'
          AND table_name   = 'symbols'
          AND column_name  = 'name'
    ) INTO col_exists;

    IF NOT col_exists THEN
        RAISE EXCEPTION '❌ Migration 004 FAILED: column "name" not found in market_data.symbols';
    END IF;

    SELECT COUNT(*) INTO total_rows    FROM market_data.symbols;
    SELECT COUNT(*) INTO rows_with_name FROM market_data.symbols WHERE name IS NOT NULL;

    RAISE NOTICE '✅ Migration 004 OK: column "name" added to market_data.symbols';
    RAISE NOTICE '   Total rows: % | Rows with name: %', total_rows, rows_with_name;
END $$;
