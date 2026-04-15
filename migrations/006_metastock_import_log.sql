-- ============================================================
-- Migration 006: MetaStock Import Log
-- Alpha-Engine2 — سجل عمليات استيراد ملفات MetaStock
-- ============================================================
-- التاريخ: 2026-04-15
-- الوصف: إنشاء جدول لتتبع عمليات استيراد MetaStock
-- ============================================================

-- ── 1. إنشاء جدول سجل الاستيراد ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS market_data.metastock_import_log (
    id              BIGSERIAL    PRIMARY KEY,
    imported_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    source_file     VARCHAR(500),                       -- اسم ملف ZIP أو المجلد
    status          VARCHAR(20)  NOT NULL DEFAULT 'pending',
                                                        -- pending | success | partial | error
    symbols_count   INTEGER      DEFAULT 0,             -- عدد الرموز المستوردة
    imported_rows   INTEGER      DEFAULT 0,             -- إجمالي الشموع المستوردة
    skipped_rows    INTEGER      DEFAULT 0,             -- الشموع المتخطاة
    errors          JSONB        DEFAULT '[]'::jsonb,   -- قائمة الأخطاء
    symbols_detail  JSONB        DEFAULT '[]'::jsonb,   -- تفاصيل كل رمز
    telegram_user   VARCHAR(100),                       -- معرف مستخدم تيليجرام
    duration_sec    DECIMAL(10,2),                      -- مدة الاستيراد بالثواني
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ── 2. Indexes ───────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_ms_import_log_imported_at
    ON market_data.metastock_import_log (imported_at DESC);

CREATE INDEX IF NOT EXISTS idx_ms_import_log_status
    ON market_data.metastock_import_log (status);

-- ── 3. تعليق توضيحي ─────────────────────────────────────────────────────────
COMMENT ON TABLE market_data.metastock_import_log IS
    'سجل عمليات استيراد ملفات MetaStock عبر بوت التليجرام';

-- ── 4. عمود source في ohlcv (إذا لم يكن موجوداً) ────────────────────────────
-- تم إضافته في migration 003، هذا للتأكد فقط
ALTER TABLE market_data.ohlcv
    ADD COLUMN IF NOT EXISTS source VARCHAR(100) DEFAULT 'unknown';

-- ── 5. تحقق نهائي ────────────────────────────────────────────────────────────
DO $$
BEGIN
    RAISE NOTICE 'Migration 006 complete:';
    RAISE NOTICE '  - market_data.metastock_import_log created';
    RAISE NOTICE '  - market_data.ohlcv.source column verified';
END $$;
