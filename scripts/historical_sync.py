#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
scripts/historical_sync.py
===========================
سكربت مستقل لمزامنة البيانات التاريخية لجميع أسهم تاسي.

الغرض:
  - جلب بيانات OHLCV التاريخية من Sahmk API لجميع الأطر الزمنية
  - يُشغَّل يدوياً أو عبر Celery Beat خارج أوقات التداول
  - مفصول تماماً عن market_reporter الذي يُخصَّص للبيانات اللحظية فقط

الاستخدام:
  # تشغيل كامل (جميع الأطر الزمنية):
  docker compose exec app python scripts/historical_sync.py

  # تشغيل إطار زمني محدد فقط:
  docker compose exec app python scripts/historical_sync.py --timeframe 1d
  docker compose exec app python scripts/historical_sync.py --timeframe 1h --days 30

  # تشغيل سهم واحد فقط:
  docker compose exec app python scripts/historical_sync.py --symbol 2222

الأطر الزمنية الافتراضية:
  1d  → 5 سنوات (1825 يوم)
  1h  → 6 أشهر  (180 يوم)
  30m → 3 أشهر  (90 يوم)
  15m → 2 شهر   (60 يوم)
  5m  → شهر     (30 يوم)
  1m  → أسبوع   (7 أيام)
"""

import argparse
import asyncio
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import asyncpg
import pandas as pd
from loguru import logger

# ── إضافة مسار المشروع ────────────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.sahmk_client import get_sahmk_client, is_tasi_or_sector, is_sector_symbol
from scripts.utils import get_saudi_time
from scripts.sector_calculator import SECTOR_DISPLAY_NAMES

# ── إعدادات ──────────────────────────────────────────────────────────────────
FETCH_CONCURRENCY = 15   # عدد الأسهم التي تُجلب بالتوازي
DB_POOL: Optional[asyncpg.Pool] = None

# الأطر الزمنية الافتراضية: (timeframe, days_back)
DEFAULT_TIMEFRAMES: List[tuple] = [
    ("1d",  1825),  # 5 سنوات
    ("1h",  180),   # 6 أشهر
    ("30m", 90),    # 3 أشهر
    ("15m", 60),    # شهران
    ("5m",  30),    # شهر
    ("1m",  7),     # أسبوع
]

SECTOR_NAMES: Dict[str, str] = SECTOR_DISPLAY_NAMES


# ─────────────────────────────────────────────────────────────────────────────
# Database
# ─────────────────────────────────────────────────────────────────────────────

async def init_db_pool() -> Optional[asyncpg.Pool]:
    """تهيئة connection pool لقاعدة البيانات."""
    global DB_POOL
    if DB_POOL is not None:
        return DB_POOL
    try:
        dsn = os.environ.get(
            "DATABASE_URL",
            "postgresql://alpha_user:alpha_password_2024@postgres:5432/alpha_engine"
        )
        DB_POOL = await asyncpg.create_pool(dsn=dsn, min_size=3, max_size=15)
        logger.success("✅ DB pool initialized")
        return DB_POOL
    except Exception as e:
        logger.critical(f"❌ DB pool failed: {e}")
        return None


async def save_historical_to_db(
    pool: asyncpg.Pool,
    df: pd.DataFrame,
    symbol: str,
    timeframe: str
) -> int:
    """حفظ DataFrame تاريخي في TimescaleDB. يُعيد عدد الصفوف المحفوظة."""
    if pool is None or df is None or df.empty:
        return 0

    df_copy = df.copy()

    # إعادة تسمية timestamp → time
    if 'timestamp' in df_copy.columns:
        df_copy.rename(columns={'timestamp': 'time'}, inplace=True)

    # ضمان timezone-aware timestamps
    df_copy['time'] = pd.to_datetime(df_copy['time'])
    if df_copy['time'].dt.tz is None:
        df_copy['time'] = df_copy['time'].dt.tz_localize('Asia/Riyadh')
    else:
        df_copy['time'] = df_copy['time'].dt.tz_convert('Asia/Riyadh')

    # ملء الأعمدة المطلوبة
    df_copy['symbol']    = symbol
    df_copy['timeframe'] = timeframe
    df_copy['name']      = SECTOR_NAMES.get(symbol) or df_copy.get('name', 'Unknown')
    if isinstance(df_copy['name'], pd.Series):
        df_copy['name'] = df_copy['name'].fillna('Unknown')

    df_copy['volume'] = df_copy['volume'].fillna(0).astype(int) \
        if 'volume' in df_copy.columns else 0
    df_copy['open_interest'] = df_copy['open_interest'].fillna(0).astype(int) \
        if 'open_interest' in df_copy.columns else 0
    df_copy['source'] = f'historical_sync_{timeframe}'

    cols = ['time', 'symbol', 'timeframe', 'name',
            'open', 'high', 'low', 'close',
            'volume', 'open_interest', 'source']
    df_copy = df_copy[[c for c in cols if c in df_copy.columns]]
    records = [tuple(row) for row in df_copy.itertuples(index=False, name=None)]
    if not records:
        return 0

    sql_with = """
    INSERT INTO market_data.ohlcv
        (time, symbol, timeframe, name, open, high, low, close,
         volume, open_interest, source)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
    ON CONFLICT (time, symbol, timeframe) DO UPDATE SET
        open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
        close=EXCLUDED.close, volume=EXCLUDED.volume,
        open_interest=EXCLUDED.open_interest, source=EXCLUDED.source;
    """
    sql_without = """
    INSERT INTO market_data.ohlcv
        (time, symbol, timeframe, name, open, high, low, close,
         volume, open_interest)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
    ON CONFLICT (time, symbol, timeframe) DO UPDATE SET
        open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
        close=EXCLUDED.close, volume=EXCLUDED.volume,
        open_interest=EXCLUDED.open_interest;
    """
    records_no_src = [r[:10] for r in records]

    try:
        async with pool.acquire() as conn:
            try:
                await conn.executemany(sql_with, records)
            except Exception as e:
                if 'source' in str(e).lower() or 'column' in str(e).lower():
                    await conn.executemany(sql_without, records_no_src)
                else:
                    raise
        return len(records)
    except Exception as e:
        logger.error(f"❌ DB save error [{symbol} {timeframe}]: {e}")
        return 0


# ─────────────────────────────────────────────────────────────────────────────
# Fetch Logic
# ─────────────────────────────────────────────────────────────────────────────

async def fetch_one_symbol(
    sahmk,
    pool: asyncpg.Pool,
    symbol: str,
    timeframe: str,
    days: int,
    semaphore: asyncio.Semaphore
) -> bool:
    """جلب وحفظ بيانات سهم واحد لإطار زمني واحد."""
    async with semaphore:
        try:
            end   = get_saudi_time()
            start = end - timedelta(days=days)
            df = await asyncio.to_thread(
                sahmk.get_historical_ohlcv,
                symbol=symbol, timeframe=timeframe,
                start_date=start, end_date=end
            )
            if df is not None and not df.empty:
                saved = await save_historical_to_db(pool, df, symbol, timeframe)
                logger.debug(f"  ✅ {symbol} [{timeframe}] — {saved} rows saved")
                return True
            else:
                logger.debug(f"  ⚠️ {symbol} [{timeframe}] — no data returned")
                return False
        except Exception as e:
            logger.error(f"  ❌ {symbol} [{timeframe}]: {e}")
            return False


async def sync_timeframe(
    sahmk,
    pool: asyncpg.Pool,
    symbols: List[str],
    timeframe: str,
    days: int
) -> None:
    """جلب بيانات جميع الأسهم لإطار زمني واحد بالتوازي."""
    # استبعاد القطاعات — لا توجد بيانات تاريخية لها عبر REST
    stock_symbols = [s for s in symbols if not is_sector_symbol(s)]

    logger.info(
        f"📊 [{timeframe}] Fetching {len(stock_symbols)} symbols "
        f"({days} days back) ..."
    )

    semaphore = asyncio.Semaphore(FETCH_CONCURRENCY)
    tasks = [
        fetch_one_symbol(sahmk, pool, sym, timeframe, days, semaphore)
        for sym in stock_symbols
    ]
    results = await asyncio.gather(*tasks)
    ok  = sum(1 for r in results if r)
    err = len(results) - ok
    logger.success(f"✅ [{timeframe}] Done — ✅{ok} saved  ❌{err} failed")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

async def run_sync(
    timeframes: Optional[List[tuple]] = None,
    symbol_filter: Optional[str] = None
) -> None:
    """الدالة الرئيسية للمزامنة التاريخية."""
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("🗄️  Alpha Engine2 — Historical Sync")
    logger.info(f"   Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    # ── تهيئة DB ──────────────────────────────────────────────────────────────
    pool = await init_db_pool()
    if pool is None:
        logger.critical("❌ Cannot connect to DB. Aborting.")
        return

    # ── تهيئة Sahmk Client ────────────────────────────────────────────────────
    try:
        sahmk = get_sahmk_client()
        logger.success("✅ Sahmk client ready")
    except Exception as e:
        logger.critical(f"❌ Sahmk client init failed: {e}")
        await pool.close()
        return

    # ── جلب قائمة الأسهم ──────────────────────────────────────────────────────
    if symbol_filter:
        symbols = [symbol_filter]
        logger.info(f"🎯 Single symbol mode: {symbol_filter}")
    else:
        symbols = sahmk.get_symbols_list() or []
        symbols = [s for s in symbols if is_tasi_or_sector(s)]
        logger.info(f"📋 Total symbols to sync: {len(symbols)}")

    if not symbols:
        logger.error("❌ No symbols found. Aborting.")
        await pool.close()
        return

    # ── تشغيل المزامنة لكل إطار زمني ─────────────────────────────────────────
    tfs = timeframes or DEFAULT_TIMEFRAMES
    for tf, days in tfs:
        await sync_timeframe(sahmk, pool, symbols, tf, days)

    # ── إغلاق الاتصالات ───────────────────────────────────────────────────────
    await pool.close()

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.success("=" * 60)
    logger.success(f"✅ Historical sync complete in {elapsed:.0f}s")
    logger.success("=" * 60)


def main():
    """نقطة الدخول مع دعم معاملات سطر الأوامر."""
    parser = argparse.ArgumentParser(
        description="Alpha Engine2 — Historical Data Sync",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
أمثلة:
  python scripts/historical_sync.py
  python scripts/historical_sync.py --timeframe 1d
  python scripts/historical_sync.py --timeframe 1h --days 60
  python scripts/historical_sync.py --symbol 2222
  python scripts/historical_sync.py --symbol 2222 --timeframe 1d --days 365
        """
    )
    parser.add_argument(
        "--timeframe", "-t",
        choices=["1d", "1h", "30m", "15m", "5m", "1m"],
        help="إطار زمني واحد فقط (الافتراضي: جميع الأطر)"
    )
    parser.add_argument(
        "--days", "-d",
        type=int,
        default=None,
        help="عدد الأيام للرجوع إليها (يتجاوز الافتراضي)"
    )
    parser.add_argument(
        "--symbol", "-s",
        type=str,
        default=None,
        help="رمز سهم واحد فقط (مثال: 2222)"
    )
    args = parser.parse_args()

    # بناء قائمة الأطر الزمنية بناءً على المعاملات
    if args.timeframe:
        default_days = dict(DEFAULT_TIMEFRAMES).get(args.timeframe, 30)
        days = args.days or default_days
        timeframes = [(args.timeframe, days)]
    elif args.days:
        # إذا حُدِّد days بدون timeframe، طبّقه على جميع الأطر
        timeframes = [(tf, args.days) for tf, _ in DEFAULT_TIMEFRAMES]
    else:
        timeframes = None  # استخدم الافتراضي

    try:
        asyncio.run(run_sync(
            timeframes=timeframes,
            symbol_filter=args.symbol
        ))
    except KeyboardInterrupt:
        logger.info("🛑 Historical sync stopped by user.")
    except Exception as e:
        logger.critical(f"💥 Historical sync crashed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
