-- =============================================================================
-- Migration 002: Market Data Tables
-- Creates: market_data.sector_candles, market_data.indices, market_data.symbols
-- =============================================================================

-- ── 1. جدول شمع القطاعات ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS market_data.sector_candles (
    time            TIMESTAMPTZ     NOT NULL,
    symbol          VARCHAR(10)     NOT NULL,   -- e.g. 90010, 90017, 90001
    name            VARCHAR(100),               -- اسم القطاع بالعربي
    timeframe       VARCHAR(5)      NOT NULL DEFAULT '1m',
    open            NUMERIC(18, 4)  NOT NULL,
    high            NUMERIC(18, 4)  NOT NULL,
    low             NUMERIC(18, 4)  NOT NULL,
    close           NUMERIC(18, 4)  NOT NULL,
    volume          BIGINT          NOT NULL DEFAULT 0,
    members_count   INT             DEFAULT 0,  -- عدد الأسهم المُكوِّنة للقطاع
    source          VARCHAR(50)     DEFAULT 'db_sector_calculator',
    created_at      TIMESTAMPTZ     DEFAULT NOW()
);

-- تحويل إلى hypertable (TimescaleDB)
SELECT create_hypertable(
    'market_data.sector_candles',
    'time',
    if_not_exists => TRUE
);

-- فهرس مركّب للاستعلامات الشائعة
CREATE INDEX IF NOT EXISTS idx_sector_candles_symbol_time
    ON market_data.sector_candles (symbol, time DESC);

CREATE INDEX IF NOT EXISTS idx_sector_candles_timeframe
    ON market_data.sector_candles (timeframe, time DESC);

-- UNIQUE لمنع التكرار (upsert)
CREATE UNIQUE INDEX IF NOT EXISTS idx_sector_candles_unique
    ON market_data.sector_candles (symbol, timeframe, time);

COMMENT ON TABLE market_data.sector_candles IS
    'شمع OHLCV للقطاعات والمؤشر العام (90001-90030) محسوبة من بيانات الأسهم';

-- ── 2. جدول بيانات المؤشرات ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS market_data.indices (
    time            TIMESTAMPTZ     NOT NULL,
    symbol          VARCHAR(10)     NOT NULL,   -- e.g. 90001 (TASI), TASI
    name            VARCHAR(100),               -- اسم المؤشر
    timeframe       VARCHAR(5)      NOT NULL DEFAULT '1m',
    open            NUMERIC(18, 4),
    high            NUMERIC(18, 4),
    low             NUMERIC(18, 4),
    close           NUMERIC(18, 4)  NOT NULL,
    volume          BIGINT          DEFAULT 0,
    change_pct      NUMERIC(8, 4),              -- نسبة التغيير %
    change_abs      NUMERIC(18, 4),             -- التغيير المطلق
    source          VARCHAR(50)     DEFAULT 'db_calculator',
    created_at      TIMESTAMPTZ     DEFAULT NOW()
);

-- تحويل إلى hypertable
SELECT create_hypertable(
    'market_data.indices',
    'time',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_indices_symbol_time
    ON market_data.indices (symbol, time DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_indices_unique
    ON market_data.indices (symbol, timeframe, time);

COMMENT ON TABLE market_data.indices IS
    'بيانات OHLCV للمؤشرات الرئيسية (TASI وغيره) من مصادر متعددة';

-- ── 3. جدول قائمة الأسهم (Universe) ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS market_data.symbols (
    symbol          VARCHAR(10)     PRIMARY KEY,
    name_ar         VARCHAR(200),               -- الاسم العربي
    name_en         VARCHAR(200),               -- الاسم الإنجليزي
    sector_id       VARCHAR(10),                -- رمز القطاع (90010 إلخ)
    sector_name_ar  VARCHAR(100),               -- اسم القطاع بالعربي
    market          VARCHAR(20)     NOT NULL DEFAULT 'TASI',  -- TASI / NOMU
    is_active       BOOLEAN         NOT NULL DEFAULT TRUE,
    listing_date    DATE,
    isin            VARCHAR(20),
    last_synced_at  TIMESTAMPTZ     DEFAULT NOW(),
    created_at      TIMESTAMPTZ     DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_symbols_market
    ON market_data.symbols (market, is_active);

CREATE INDEX IF NOT EXISTS idx_symbols_sector
    ON market_data.symbols (sector_id);

COMMENT ON TABLE market_data.symbols IS
    'قائمة كاملة لأسهم السوق السعودي — تُحدَّث يومياً من SAHMK API';

-- ── 4. Trigger لتحديث updated_at تلقائياً ────────────────────────────────────
CREATE OR REPLACE FUNCTION market_data.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_symbols_updated_at ON market_data.symbols;
CREATE TRIGGER trg_symbols_updated_at
    BEFORE UPDATE ON market_data.symbols
    FOR EACH ROW EXECUTE FUNCTION market_data.update_updated_at_column();

-- ── 5. إدراج بيانات أولية للقطاعات المعروفة ──────────────────────────────────
-- (تُستخدم كـ seed حتى يعمل sync_symbols لأول مرة)
INSERT INTO market_data.symbols (symbol, name_ar, name_en, sector_id, sector_name_ar, market)
VALUES
    ('1010', 'مصرف الرياض',                  'Riyad Bank',                '90010', 'البنوك',                    'TASI'),
    ('1020', 'بنك الجزيرة',                  'Bank Al-Jazira',            '90010', 'البنوك',                    'TASI'),
    ('1030', 'بنك الاستثمار السعودي',         'Saudi Investment Bank',     '90010', 'البنوك',                    'TASI'),
    ('1050', 'مصرف الإنماء',                  'Bank Albilad',              '90010', 'البنوك',                    'TASI'),
    ('1060', 'البنك الأهلي السعودي',          'Saudi National Bank',       '90010', 'البنوك',                    'TASI'),
    ('1080', 'بنك العربي الوطني',             'Arab National Bank',        '90010', 'البنوك',                    'TASI'),
    ('1100', 'مصرف الراجحي',                  'Al Rajhi Bank',             '90010', 'البنوك',                    'TASI'),
    ('1110', 'البنك السعودي الفرنسي',         'Banque Saudi Fransi',       '90010', 'البنوك',                    'TASI'),
    ('1120', 'بنك الإنماء',                   'Alinma Bank',               '90010', 'البنوك',                    'TASI'),
    ('1140', 'بنك الخليج',                    'Gulf International Bank',   '90010', 'البنوك',                    'TASI'),
    ('2222', 'أرامكو السعودية',               'Saudi Aramco',              '90017', 'الطاقة',                    'TASI'),
    ('2010', 'سابك',                          'SABIC',                     '90022', 'المواد الأساسية',           'TASI'),
    ('2350', 'سافكو',                         'SAFCO',                     '90022', 'المواد الأساسية',           'TASI'),
    ('4200', 'مجموعة الخبر الإعلامية',        'Okaz Organization',         '90013', 'السلع الاستهلاكية التقديرية','TASI'),
    ('4190', 'مجموعة جرير للتسويق',           'Jarir Marketing',           '90013', 'السلع الاستهلاكية التقديرية','TASI'),
    ('2380', 'بترو رابغ',                     'Petro Rabigh',              '90017', 'الطاقة',                    'TASI'),
    ('3020', 'الاتصالات السعودية',             'Saudi Telecom',             '90020', 'خدمات الاتصالات',          'TASI'),
    ('4030', 'بترو كيم',                      'Petro Chem',                '90017', 'الطاقة',                    'TASI'),
    ('1180', 'بنك الأوسط',                    'Saudi British Bank',        '90010', 'البنوك',                    'TASI'),
    ('4210', 'عبدالله العثيم للاستثمار',      'Abdullah Al Othaim',        '90013', 'السلع الاستهلاكية التقديرية','TASI')
ON CONFLICT (symbol) DO UPDATE SET
    name_ar        = EXCLUDED.name_ar,
    name_en        = EXCLUDED.name_en,
    sector_id      = EXCLUDED.sector_id,
    sector_name_ar = EXCLUDED.sector_name_ar,
    market         = EXCLUDED.market,
    updated_at     = NOW();

-- ── تحقق نهائي ───────────────────────────────────────────────────────────────
DO $$
BEGIN
    RAISE NOTICE 'Migration 002 complete:';
    RAISE NOTICE '  - market_data.sector_candles created';
    RAISE NOTICE '  - market_data.indices created';
    RAISE NOTICE '  - market_data.symbols created (% seed rows)',
        (SELECT COUNT(*) FROM market_data.symbols);
END $$;
