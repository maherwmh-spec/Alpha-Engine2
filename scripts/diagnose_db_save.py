#!/usr/bin/env python3
"""
diagnose_db_save.py
===================
سكريبت تشخيصي يكشف سبب عدم حفظ الشموع في DB.
يُشغَّل داخل container market_reporter:
  docker compose exec market_reporter python3 scripts/diagnose_db_save.py
"""

import os
import sys
import asyncio
import inspect
from datetime import datetime

# ── 1. اختبار الاتصال بـ DB مباشرة ─────────────────────────────────────────
async def test_db_connection():
    import asyncpg
    dsn = os.environ.get(
        "DATABASE_URL",
        "postgresql://alpha_user:alpha_password_2024@postgres:5432/alpha_engine"
    )
    print(f"\n[1] Testing DB connection: {dsn[:50]}...")
    try:
        conn = await asyncpg.connect(dsn=dsn)
        row = await conn.fetchrow("SELECT COUNT(*) as cnt FROM market_data.ohlcv")
        print(f"    ✅ DB connected | Total rows in ohlcv: {row['cnt']}")

        # آخر 5 صفوف
        rows = await conn.fetch(
            "SELECT symbol, timeframe, time, source FROM market_data.ohlcv "
            "ORDER BY time DESC LIMIT 5"
        )
        if rows:
            print("    📊 Latest 5 rows in ohlcv:")
            for r in rows:
                print(f"       {r['symbol']} | {r['timeframe']} | {r['time']} | {r['source']}")
        else:
            print("    ⚠️  ohlcv table is EMPTY")

        await conn.close()
        return True
    except Exception as e:
        print(f"    ❌ DB connection FAILED: {e}")
        return False


# ── 2. اختبار INSERT مباشر ──────────────────────────────────────────────────
async def test_direct_insert():
    import asyncpg
    import pytz
    dsn = os.environ.get(
        "DATABASE_URL",
        "postgresql://alpha_user:alpha_password_2024@postgres:5432/alpha_engine"
    )
    print(f"\n[2] Testing direct INSERT into ohlcv...")
    try:
        conn = await asyncpg.connect(dsn=dsn)
        ts = datetime.now(tz=pytz.timezone('Asia/Riyadh')).replace(second=0, microsecond=0)
        await conn.execute("""
            INSERT INTO market_data.ohlcv
                (time, symbol, timeframe, name, open, high, low, close, volume, open_interest, source)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (time, symbol, timeframe) DO UPDATE SET close = EXCLUDED.close
        """, ts, 'TEST_DIAG', '1m', 'Test', 100.0, 101.0, 99.0, 100.5, 1000, 0, 'diagnose_script')
        print(f"    ✅ Direct INSERT succeeded at {ts}")

        # تحقق
        row = await conn.fetchrow(
            "SELECT * FROM market_data.ohlcv WHERE symbol='TEST_DIAG' AND timeframe='1m' ORDER BY time DESC LIMIT 1"
        )
        if row:
            print(f"    ✅ Row confirmed in DB: {dict(row)}")
        else:
            print(f"    ❌ Row NOT found after INSERT!")

        # حذف سجل الاختبار
        await conn.execute("DELETE FROM market_data.ohlcv WHERE symbol='TEST_DIAG'")
        await conn.close()
        return True
    except Exception as e:
        print(f"    ❌ Direct INSERT FAILED: {e}")
        import traceback; traceback.print_exc()
        return False


# ── 3. اختبار DB_POOL في bot.py ─────────────────────────────────────────────
def test_bot_db_pool():
    print(f"\n[3] Checking bot.py DB_POOL state...")
    try:
        # استيراد الـ module مباشرة
        sys.path.insert(0, '/app')
        import importlib
        spec = importlib.util.spec_from_file_location(
            "__main__", "/app/bots/market_reporter/bot.py"
        )
        # لا نُشغّل الـ module — فقط نفحص المتغير العام
        import bots.market_reporter.bot as bot_module
        pool = getattr(bot_module, 'DB_POOL', 'NOT_FOUND')
        print(f"    DB_POOL = {pool}")
        if pool is None:
            print("    ❌ DB_POOL is None — _save_candle_to_db exits immediately!")
        elif pool == 'NOT_FOUND':
            print("    ⚠️  DB_POOL variable not found in module")
        else:
            print("    ✅ DB_POOL is initialized")
    except Exception as e:
        print(f"    ⚠️  Could not import bot module: {e}")


# ── 4. اختبار _on_candle_complete مباشرة ────────────────────────────────────
async def test_candle_complete_callback():
    print(f"\n[4] Testing _on_candle_complete → _save_candle_to_db pipeline...")
    try:
        sys.path.insert(0, '/app')
        import bots.market_reporter.bot as bot_module
        import asyncpg
        import pytz

        # تهيئة DB_POOL
        dsn = os.environ.get(
            "DATABASE_URL",
            "postgresql://alpha_user:alpha_password_2024@postgres:5432/alpha_engine"
        )
        bot_module.DB_POOL = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=3)
        print(f"    ✅ DB_POOL created for test")

        # إنشاء instance من MarketReporter
        reporter = bot_module.MarketReporter()
        reporter._main_loop = asyncio.get_running_loop()

        # شمعة اختبارية
        ts = datetime.now(tz=pytz.timezone('Asia/Riyadh')).replace(second=0, microsecond=0)
        test_candle = {
            'symbol':    'TEST_CB',
            'timestamp': ts,
            'open':      100.0,
            'high':      101.0,
            'low':       99.0,
            'close':     100.5,
            'volume':    5000,
            'tick_count': 10,
        }

        print(f"    📤 Calling _save_candle_to_db directly...")
        await reporter._save_candle_to_db(test_candle)

        # تحقق
        async with bot_module.DB_POOL.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM market_data.ohlcv WHERE symbol='TEST_CB' ORDER BY time DESC LIMIT 1"
            )
        if row:
            print(f"    ✅ _save_candle_to_db WORKS! Row: {dict(row)}")
        else:
            print(f"    ❌ _save_candle_to_db FAILED — no row in DB!")

        # تنظيف
        async with bot_module.DB_POOL.acquire() as conn:
            await conn.execute("DELETE FROM market_data.ohlcv WHERE symbol='TEST_CB'")

        await bot_module.DB_POOL.close()

    except Exception as e:
        print(f"    ❌ Callback test FAILED: {e}")
        import traceback; traceback.print_exc()


# ── 5. فحص Redis ────────────────────────────────────────────────────────────
def test_redis():
    print(f"\n[5] Checking Redis for recent candles...")
    try:
        import redis
        r = redis.Redis(host='redis', port=6379, decode_responses=True)
        keys = r.keys("ohlcv:1m:*:latest")
        print(f"    Redis keys matching 'ohlcv:1m:*:latest': {len(keys)}")
        if keys:
            print(f"    Sample keys: {keys[:5]}")
            # اقرأ أول واحد
            val = r.hgetall(keys[0]) or r.get(keys[0])
            print(f"    Sample value: {val}")
        else:
            print("    ⚠️  No candle keys in Redis — _on_candle_complete may not be called at all!")
    except Exception as e:
        print(f"    ⚠️  Redis check failed: {e}")


# ── Main ─────────────────────────────────────────────────────────────────────
async def main():
    print("=" * 60)
    print("🔍  DB Save Diagnostic Tool")
    print("=" * 60)

    db_ok = await test_db_connection()
    if db_ok:
        await test_direct_insert()
    test_bot_db_pool()
    if db_ok:
        await test_candle_complete_callback()
    test_redis()

    print("\n" + "=" * 60)
    print("Done.")
    print("=" * 60)


if __name__ == '__main__':
    asyncio.run(main())
