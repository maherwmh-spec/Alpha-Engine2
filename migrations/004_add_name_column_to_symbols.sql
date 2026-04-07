-- =============================================================================
-- Migration 004: إضافة عمود name إلى جدول market_data.symbols
-- =============================================================================
-- FIX #4: جدول symbols لا يحتوي على عمود name مخصص للاسم المعروض.
--
-- الهدف:
--   إضافة عمود name كاسم مختصر/معروض للشركة (عربي بالأساس).
--   هذا العمود يُستخدم في:
--   - الداش بورد لعرض اسم الشركة بجانب الرمز
--   - WebSocket subscription labels
--   - التقارير والإشارات
--
-- ملاحظة:
--   الجدول يحتوي بالفعل على:
--     - name_ar : الاسم الكامل بالعربية
--     - name_en : الاسم الكامل بالإنجليزية
--   العمود الجديد name يكون:
--     - اسم مختصر/معروض (عربي بالأساس)
--     - يُملأ تلقائياً من name_ar إذا كان فارغاً
--     - يُستخدم في الواجهات والتقارير
-- =============================================================================

BEGIN;

-- ── الخطوة 1: إضافة عمود name ────────────────────────────────────────────────
ALTER TABLE market_data.symbols
    ADD COLUMN IF NOT EXISTS name TEXT;

COMMENT ON COLUMN market_data.symbols.name IS
    'الاسم المعروض للشركة (عربي بالأساس). يُستخدم في الداش بورد والتقارير.
     يُملأ من sync_symbols.py عبر scraping من argaam.com.
     إذا كان فارغاً، يُستخدم name_ar كبديل.';

-- ── الخطوة 2: ملء name من name_ar للصفوف الموجودة ───────────────────────────
-- للصفوف التي لها name_ar ولكن name فارغ
UPDATE market_data.symbols
SET name = name_ar
WHERE name IS NULL
  AND name_ar IS NOT NULL
  AND name_ar <> '';

-- ── الخطوة 3: إنشاء index على عمود name لتسريع البحث ────────────────────────
CREATE INDEX IF NOT EXISTS idx_symbols_name
    ON market_data.symbols (name)
    WHERE name IS NOT NULL;

-- ── الخطوة 4: تسجيل المهاجرة ─────────────────────────────────────────────────
-- (اختياري: إذا كان لديك جدول لتتبع المهاجرات)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'schema_migrations'
    ) THEN
        INSERT INTO public.schema_migrations (version, applied_at)
        VALUES ('004_add_name_column_to_symbols', NOW())
        ON CONFLICT (version) DO NOTHING;
    END IF;
END $$;

COMMIT;

-- ── التحقق من نجاح المهاجرة ──────────────────────────────────────────────────
DO $$
DECLARE
    col_exists BOOLEAN;
    rows_with_name INTEGER;
BEGIN
    -- التحقق من وجود العمود
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'market_data'
          AND table_name = 'symbols'
          AND column_name = 'name'
    ) INTO col_exists;

    IF col_exists THEN
        -- عدّ الصفوف التي لها name
        SELECT COUNT(*) INTO rows_with_name
        FROM market_data.symbols
        WHERE name IS NOT NULL;

        RAISE NOTICE '✅ Migration 004 successful: column name added to market_data.symbols';
        RAISE NOTICE '   Rows with name populated: %', rows_with_name;
    ELSE
        RAISE EXCEPTION '❌ Migration 004 failed: column name not found in market_data.symbols';
    END IF;
END $$;
