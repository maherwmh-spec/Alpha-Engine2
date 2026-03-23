#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tests/test_retention_policies.py
اختبارات شاملة للتحقق من سياسات الاحتفاظ بالبيانات في TimescaleDB
20 اختبار يغطي: الجداول، السياسات، الضغط، التجميع المستمر

تشغيل:
    python -m pytest tests/test_retention_policies.py -v
    # أو مباشرة:
    python tests/test_retention_policies.py
"""

import os
import sys
import unittest
import asyncio
from datetime import datetime, timedelta
from typing import Optional

try:
    import asyncpg
    ASYNCPG_AVAILABLE = True
except ImportError:
    ASYNCPG_AVAILABLE = False

# ─── DSN ────────────────────────────────────────────────────────────────────
DSN = os.environ.get(
    "DATABASE_URL",
    "postgresql://alpha_user:alpha_password_2024@localhost:5432/alpha_engine"
)

# ─── Expected Retention Policies ────────────────────────────────────────────
EXPECTED_POLICIES = {
    "ohlcv_realtime": 365,    # 1 year
    "ohlcv_rt_5m":    730,    # 2 years
    "ohlcv_rt_15m":   730,    # 2 years
    "ohlcv_rt_30m":   1095,   # 3 years
    "ohlcv_rt_1h":    1825,   # 5 years
    # ohlcv_rt_1d → NO retention (unlimited)
}

EXPECTED_NO_RETENTION = ["ohlcv_rt_1d"]

EXPECTED_TABLES = [
    "ohlcv_realtime",
    "ohlcv_rt_5m",
    "ohlcv_rt_15m",
    "ohlcv_rt_30m",
    "ohlcv_rt_1h",
    "ohlcv_rt_1d",
]

EXPECTED_AGGREGATES = [
    "ohlcv_rt_5m",
    "ohlcv_rt_15m",
    "ohlcv_rt_30m",
    "ohlcv_rt_1h",
    "ohlcv_rt_1d",
]


# ─── Async Helper ────────────────────────────────────────────────────────────
def run_async(coro):
    """Run async coroutine in sync context."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


async def get_connection() -> Optional[asyncpg.Connection]:
    """Get a database connection."""
    try:
        return await asyncpg.connect(DSN)
    except Exception as e:
        print(f"⚠️  Cannot connect to DB: {e}")
        return None


# ─── Test Suite ──────────────────────────────────────────────────────────────
class TestRetentionPolicies(unittest.TestCase):
    """20 اختبار شامل لسياسات الاحتفاظ بالبيانات."""

    @classmethod
    def setUpClass(cls):
        """إعداد الاتصال بقاعدة البيانات."""
        if not ASYNCPG_AVAILABLE:
            raise unittest.SkipTest("asyncpg not installed")
        cls.conn = run_async(get_connection())
        if cls.conn is None:
            raise unittest.SkipTest("Cannot connect to database")

    @classmethod
    def tearDownClass(cls):
        """إغلاق الاتصال."""
        if cls.conn:
            run_async(cls.conn.close())

    # ── TEST GROUP 1: Table Existence (5 tests) ──────────────────────────────

    def test_01_ohlcv_realtime_exists(self):
        """✅ جدول ohlcv_realtime موجود."""
        result = run_async(self.conn.fetchval(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema='market_data' AND table_name='ohlcv_realtime'"
        ))
        self.assertEqual(result, 1, "❌ جدول ohlcv_realtime غير موجود")

    def test_02_ohlcv_rt_5m_exists(self):
        """✅ جدول ohlcv_rt_5m موجود."""
        result = run_async(self.conn.fetchval(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema='market_data' AND table_name='ohlcv_rt_5m'"
        ))
        self.assertEqual(result, 1, "❌ جدول ohlcv_rt_5m غير موجود")

    def test_03_ohlcv_rt_15m_exists(self):
        """✅ جدول ohlcv_rt_15m موجود."""
        result = run_async(self.conn.fetchval(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema='market_data' AND table_name='ohlcv_rt_15m'"
        ))
        self.assertEqual(result, 1, "❌ جدول ohlcv_rt_15m غير موجود")

    def test_04_ohlcv_rt_1h_exists(self):
        """✅ جدول ohlcv_rt_1h موجود."""
        result = run_async(self.conn.fetchval(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema='market_data' AND table_name='ohlcv_rt_1h'"
        ))
        self.assertEqual(result, 1, "❌ جدول ohlcv_rt_1h غير موجود")

    def test_05_ohlcv_rt_1d_exists(self):
        """✅ جدول ohlcv_rt_1d موجود."""
        result = run_async(self.conn.fetchval(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema='market_data' AND table_name='ohlcv_rt_1d'"
        ))
        self.assertEqual(result, 1, "❌ جدول ohlcv_rt_1d غير موجود")

    # ── TEST GROUP 2: Retention Policy Values (6 tests) ─────────────────────

    def test_06_retention_1m_is_365_days(self):
        """✅ سياسة الاحتفاظ لـ 1m = 365 يوم."""
        result = run_async(self.conn.fetchval("""
            SELECT (config->>'drop_after')::interval
            FROM timescaledb_information.jobs
            WHERE hypertable_schema = 'market_data'
              AND hypertable_name = 'ohlcv_realtime'
              AND proc_name = 'policy_retention'
            LIMIT 1
        """))
        self.assertIsNotNone(result, "❌ لا توجد سياسة احتفاظ لـ ohlcv_realtime")
        self.assertEqual(result.days, 365, f"❌ المتوقع 365 يوم، الموجود {result.days}")

    def test_07_retention_5m_is_730_days(self):
        """✅ سياسة الاحتفاظ لـ 5m = 730 يوم."""
        result = run_async(self.conn.fetchval("""
            SELECT (config->>'drop_after')::interval
            FROM timescaledb_information.jobs
            WHERE hypertable_schema = 'market_data'
              AND hypertable_name = 'ohlcv_rt_5m'
              AND proc_name = 'policy_retention'
            LIMIT 1
        """))
        self.assertIsNotNone(result, "❌ لا توجد سياسة احتفاظ لـ ohlcv_rt_5m")
        self.assertEqual(result.days, 730, f"❌ المتوقع 730 يوم، الموجود {result.days}")

    def test_08_retention_15m_is_730_days(self):
        """✅ سياسة الاحتفاظ لـ 15m = 730 يوم."""
        result = run_async(self.conn.fetchval("""
            SELECT (config->>'drop_after')::interval
            FROM timescaledb_information.jobs
            WHERE hypertable_schema = 'market_data'
              AND hypertable_name = 'ohlcv_rt_15m'
              AND proc_name = 'policy_retention'
            LIMIT 1
        """))
        self.assertIsNotNone(result, "❌ لا توجد سياسة احتفاظ لـ ohlcv_rt_15m")
        self.assertEqual(result.days, 730, f"❌ المتوقع 730 يوم، الموجود {result.days}")

    def test_09_retention_30m_is_1095_days(self):
        """✅ سياسة الاحتفاظ لـ 30m = 1095 يوم."""
        result = run_async(self.conn.fetchval("""
            SELECT (config->>'drop_after')::interval
            FROM timescaledb_information.jobs
            WHERE hypertable_schema = 'market_data'
              AND hypertable_name = 'ohlcv_rt_30m'
              AND proc_name = 'policy_retention'
            LIMIT 1
        """))
        self.assertIsNotNone(result, "❌ لا توجد سياسة احتفاظ لـ ohlcv_rt_30m")
        self.assertEqual(result.days, 1095, f"❌ المتوقع 1095 يوم، الموجود {result.days}")

    def test_10_retention_1h_is_1825_days(self):
        """✅ سياسة الاحتفاظ لـ 1h = 1825 يوم."""
        result = run_async(self.conn.fetchval("""
            SELECT (config->>'drop_after')::interval
            FROM timescaledb_information.jobs
            WHERE hypertable_schema = 'market_data'
              AND hypertable_name = 'ohlcv_rt_1h'
              AND proc_name = 'policy_retention'
            LIMIT 1
        """))
        self.assertIsNotNone(result, "❌ لا توجد سياسة احتفاظ لـ ohlcv_rt_1h")
        self.assertEqual(result.days, 1825, f"❌ المتوقع 1825 يوم، الموجود {result.days}")

    def test_11_retention_1d_is_unlimited(self):
        """✅ لا توجد سياسة احتفاظ لـ 1d (unlimited)."""
        result = run_async(self.conn.fetchval("""
            SELECT COUNT(*)
            FROM timescaledb_information.jobs
            WHERE hypertable_schema = 'market_data'
              AND hypertable_name = 'ohlcv_rt_1d'
              AND proc_name = 'policy_retention'
        """))
        self.assertEqual(result, 0, "❌ يجب أن لا تكون هناك سياسة احتفاظ لـ ohlcv_rt_1d (يجب أن تُحفظ للأبد)")

    # ── TEST GROUP 3: Continuous Aggregates (4 tests) ────────────────────────

    def test_12_continuous_aggregate_5m_active(self):
        """✅ التجميع المستمر 5m نشط."""
        result = run_async(self.conn.fetchval("""
            SELECT COUNT(*)
            FROM timescaledb_information.continuous_aggregates
            WHERE view_schema = 'market_data' AND view_name = 'ohlcv_rt_5m'
        """))
        self.assertEqual(result, 1, "❌ التجميع المستمر ohlcv_rt_5m غير موجود")

    def test_13_continuous_aggregate_15m_active(self):
        """✅ التجميع المستمر 15m نشط."""
        result = run_async(self.conn.fetchval("""
            SELECT COUNT(*)
            FROM timescaledb_information.continuous_aggregates
            WHERE view_schema = 'market_data' AND view_name = 'ohlcv_rt_15m'
        """))
        self.assertEqual(result, 1, "❌ التجميع المستمر ohlcv_rt_15m غير موجود")

    def test_14_continuous_aggregate_1h_active(self):
        """✅ التجميع المستمر 1h نشط."""
        result = run_async(self.conn.fetchval("""
            SELECT COUNT(*)
            FROM timescaledb_information.continuous_aggregates
            WHERE view_schema = 'market_data' AND view_name = 'ohlcv_rt_1h'
        """))
        self.assertEqual(result, 1, "❌ التجميع المستمر ohlcv_rt_1h غير موجود")

    def test_15_continuous_aggregate_1d_active(self):
        """✅ التجميع المستمر 1d نشط."""
        result = run_async(self.conn.fetchval("""
            SELECT COUNT(*)
            FROM timescaledb_information.continuous_aggregates
            WHERE view_schema = 'market_data' AND view_name = 'ohlcv_rt_1d'
        """))
        self.assertEqual(result, 1, "❌ التجميع المستمر ohlcv_rt_1d غير موجود")

    # ── TEST GROUP 4: Compression (3 tests) ──────────────────────────────────

    def test_16_compression_enabled_on_realtime(self):
        """✅ الضغط مفعّل على ohlcv_realtime."""
        result = run_async(self.conn.fetchval("""
            SELECT compression_enabled
            FROM timescaledb_information.hypertables
            WHERE hypertable_schema = 'market_data'
              AND hypertable_name = 'ohlcv_realtime'
        """))
        self.assertTrue(result, "❌ الضغط غير مفعّل على ohlcv_realtime")

    def test_17_compression_policy_exists(self):
        """✅ سياسة الضغط التلقائي موجودة."""
        result = run_async(self.conn.fetchval("""
            SELECT COUNT(*)
            FROM timescaledb_information.jobs
            WHERE hypertable_schema = 'market_data'
              AND hypertable_name = 'ohlcv_realtime'
              AND proc_name = 'policy_compression'
        """))
        self.assertGreater(result, 0, "❌ لا توجد سياسة ضغط تلقائي على ohlcv_realtime")

    def test_18_total_retention_policies_count(self):
        """✅ عدد سياسات الاحتفاظ = 5 (بدون 1d)."""
        result = run_async(self.conn.fetchval("""
            SELECT COUNT(*)
            FROM timescaledb_information.jobs
            WHERE hypertable_schema = 'market_data'
              AND proc_name = 'policy_retention'
        """))
        self.assertEqual(result, 5, f"❌ المتوقع 5 سياسات احتفاظ، الموجود {result}")

    # ── TEST GROUP 5: Data Integrity (2 tests) ───────────────────────────────

    def test_19_ohlcv_realtime_has_required_columns(self):
        """✅ جدول ohlcv_realtime يحتوي على جميع الأعمدة المطلوبة."""
        required_cols = {'time', 'symbol', 'open', 'high', 'low', 'close', 'volume'}
        result = run_async(self.conn.fetch("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'market_data'
              AND table_name = 'ohlcv_realtime'
        """))
        existing_cols = {row['column_name'] for row in result}
        missing = required_cols - existing_cols
        self.assertEqual(len(missing), 0, f"❌ أعمدة مفقودة: {missing}")

    def test_20_market_data_schema_exists(self):
        """✅ schema market_data موجود."""
        result = run_async(self.conn.fetchval(
            "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name='market_data'"
        ))
        self.assertEqual(result, 1, "❌ schema market_data غير موجود")


# ─── Main Runner ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("🧪 اختبارات سياسات الاحتفاظ بالبيانات")
    print("=" * 60)

    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestRetentionPolicies)

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    total   = result.testsRun
    failed  = len(result.failures) + len(result.errors)
    passed  = total - failed

    print("\n" + "=" * 60)
    print(f"📊 النتائج: {total} اختبار | ✅ {passed} نجح | ❌ {failed} فشل")

    if failed == 0:
        print("🎉 تم تعديل سياسات الاحتفاظ بنجاح!")
        print("\n✅ سياسات الاحتفاظ الجديدة:")
        print(f"  {'الإطار الزمني':<10} {'الجدول':<20} {'الاحتفاظ':<15} {'الوصف'}")
        print(f"  {'1m':<10} {'ohlcv_realtime':<20} {'365 يوم':<15} سنة كاملة (كان 90 يوم)")
        print(f"  {'5m':<10} {'ohlcv_rt_5m':<20} {'730 يوم':<15} سنتان (كان 90 يوم)")
        print(f"  {'15m':<10} {'ohlcv_rt_15m':<20} {'730 يوم':<15} سنتان (كان 90 يوم)")
        print(f"  {'30m':<10} {'ohlcv_rt_30m':<20} {'1095 يوم':<15} 3 سنوات (كان 90 يوم)")
        print(f"  {'1h':<10} {'ohlcv_rt_1h':<20} {'1825 يوم':<15} 5 سنوات (كان 90 يوم)")
        print(f"  {'1d':<10} {'ohlcv_rt_1d':<20} {'غير محدود':<15} للأبد (كان 90 يوم)")
    else:
        print("⚠️  بعض الاختبارات فشلت — راجع الأخطاء أعلاه")

    print("=" * 60)
    sys.exit(0 if failed == 0 else 1)
