-- ============================================================
-- Migration: إنشاء جدول القطاعات وإدراج بيانات سوق تاسي
-- ============================================================

-- إنشاء schema إذا لم يكن موجوداً
CREATE SCHEMA IF NOT EXISTS market_data;

-- إنشاء جدول القطاعات
CREATE TABLE IF NOT EXISTS market_data.sectors (
    sector_id    INTEGER      PRIMARY KEY,
    symbol       VARCHAR(10)  NOT NULL UNIQUE,
    name_en      VARCHAR(100) NOT NULL,
    name_ar      VARCHAR(100) NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- إدراج بيانات القطاعات (21 قطاع)
INSERT INTO market_data.sectors (sector_id, symbol, name_en, name_ar) VALUES
(90001, '90001', 'TASI',                                          'المؤشر العام تاسي'),
(90010, '90010', 'Banks',                                         'البنوك'),
(90011, '90011', 'Capital Goods',                                 'السلع الرأسمالية'),
(90012, '90012', 'Commercial and Professional Svc',               'الخدمات التجارية والمهنية'),
(90013, '90013', 'Consumer Discretionary Distribution & Retail',  'توزيع السلع الاستهلاكية التقديرية والتجزئة'),
(90014, '90014', 'Consumer Durables and Apparel',                 'السلع المعمّرة والملابس'),
(90015, '90015', 'Consumer Staples Distribution & Retail',        'توزيع السلع الاستهلاكية الأساسية والتجزئة'),
(90016, '90016', 'Consumer svc',                                  'خدمات المستهلك'),
(90017, '90017', 'Energy',                                        'الطاقة'),
(90018, '90018', 'Financial Services',                            'الخدمات المالية'),
(90019, '90019', 'Food and Beverages',                            'الأغذية والمشروبات'),
(90020, '90020', 'Health Care Equipment and Svc',                 'معدات وخدمات الرعاية الصحية'),
(90021, '90021', 'Insurance',                                     'التأمين'),
(90022, '90022', 'Materials',                                     'المواد الأساسية'),
(90023, '90023', 'Media and Entertainment',                       'الإعلام والترفيه'),
(90024, '90024', 'Pharma, Biotech and Life Science',              'الأدوية والتقنية الحيوية'),
(90025, '90025', 'REITs',                                         'صناديق الاستثمار العقاري'),
(90026, '90026', 'Real Estate Mgmt and Dev',                      'إدارة وتطوير العقارات'),
(90027, '90027', 'Software and Svc',                              'البرمجيات والخدمات'),
(90028, '90028', 'Telecommunication Svc',                         'خدمات الاتصالات'),
(90029, '90029', 'Transportation',                                 'النقل'),
(90030, '90030', 'Utilities',                                     'المرافق')
ON CONFLICT (sector_id) DO UPDATE SET
    name_en    = EXCLUDED.name_en,
    name_ar    = EXCLUDED.name_ar;

-- إضافة عمود sector_id إلى جدول الأسهم إذا لم يكن موجوداً
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'market_data'
          AND table_name   = 'ohlcv'
          AND column_name  = 'sector_id'
    ) THEN
        ALTER TABLE market_data.ohlcv ADD COLUMN sector_id INTEGER REFERENCES market_data.sectors(sector_id);
    END IF;
END $$;

-- تحقق من الإدراج
SELECT sector_id, symbol, name_en, name_ar FROM market_data.sectors ORDER BY sector_id;
