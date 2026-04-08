#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
scripts/historical_sync.py
===========================
سكربت ذكي لاستيراد البيانات التاريخية من ملفات محلية (CSV و MetaStock)
أو جلبها من Sahmk API.

الميزات الجديدة:
- استيراد بيانات الأسهم من ملفات MetaStock (.mwd)
- استيراد بيانات القطاعات والمؤشرات من ملفات CSV
- مطابقة ذكية لأسماء القطاعات الإنجليزية مع رموزها (900xx)
- منطق UPSERT لمنع التعارض وتحديث البيانات الموجودة
- توجيه البيانات للجداول الصحيحة (ohlcv, sector_performance, index_performance)
"""

import argparse
import asyncio
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
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
FETCH_CONCURRENCY = 15
DB_POOL: Optional[asyncpg.Pool] = None

DEFAULT_TIMEFRAMES: List[tuple] = [
    ("1d",  1825),
    ("1h",  180),
    ("30m", 90),
    ("15m", 60),
    ("5m",  30),
    ("1m",  7),
]

SECTOR_NAMES: Dict[str, str] = SECTOR_DISPLAY_NAMES

# قاموس مطابقة أسماء القطاعات الإنجليزية مع رموزها
ENGLISH_SECTOR_MAP: Dict[str, str] = {
    'Banks': '90010',
    'Capital Goods': '90011',
    'Commercial & Professional Svc': '90012',
    'Commercial and Professional Svc': '90012',
    'Consumer Discretionary': '90013',
    'Consumer Discretionary Distribution & Retail': '90013',
    'Consumer Durables & Apparel': '90014',
    'Consumer Durables and Apparel': '90014',
    'Consumer Staples': '90015',
    'Consumer Staples Distribution & Retail': '90015',
    'Consumer Svc': '90016',
    'Consumer svc': '90016',
    'Energy': '90017',
    'Financial Services': '90018',
    'Food & Beverages': '90019',
    'Food and Beverages': '90019',
    'Health Care': '90020',
    'Health Care Equipment & Svc': '90020',
    'Health Care Equipment and Svc': '90020',
    'Insurance': '90021',
    'Materials': '90022',
    'Media & Entertainment': '90023',
    'Media and Entertainment': '90023',
    'Pharma & Biotech': '90024',
    'Pharma, Biotech & Life Science': '90024',
    'Pharma, Biotech and Life Science': '90024',
    'REITs': '90025',
    'Real Estate': '90026',
    'Real Estate Mgmt & Dev': '90026',
    'Real Estate Mgmt and Dev': '90026',
    'Software & Svc': '90027',
    'Software and Svc': '90027',
    'Telecom': '90028',
    'Telecommunication Svc': '90028',
    'Transportation': '90029',
    'Utilities': '90030',
}

# ─────────────────────────────────────────────────────────────────────────────
# Database
# ─────────────────────────────────────────────────────────────────────────────

async def init_db_pool() -> Optional[asyncpg.Pool]:
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
    timeframe: str,
    table_type: str = 'stock'
) -> int:
    """حفظ DataFrame تاريخي في TimescaleDB باستخدام UPSERT."""
    if pool is None or df is None or df.empty:
        return 0

    df_copy = df.copy()

    # توحيد اسم عمود الوقت
    for col in ['timestamp', 'Date', 'date', 'Time', 'time']:
        if col in df_copy.columns:
            df_copy.rename(columns={col: 'time'}, inplace=True)
            break
            
    if 'time' not in df_copy.columns:
        logger.error(f"❌ No time column found for {symbol}")
        return 0

    # توحيد أسماء الأعمدة الأخرى
    col_map = {
        'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close',
        'Volume': 'volume', 'Vol': 'volume'
    }
    df_copy.rename(columns=col_map, inplace=True)

    # ضمان timezone-aware timestamps
    df_copy['time'] = pd.to_datetime(df_copy['time'])
    if df_copy['time'].dt.tz is None:
        df_copy['time'] = df_copy['time'].dt.tz_localize('Asia/Riyadh')
    else:
        df_copy['time'] = df_copy['time'].dt.tz_convert('Asia/Riyadh')

    df_copy['symbol'] = symbol
    df_copy['timeframe'] = timeframe
    
    # تحديد الاسم
    if table_type == 'sector':
        df_copy['name'] = SECTOR_NAMES.get(symbol, 'Unknown Sector')
    elif table_type == 'index':
        df_copy['name'] = 'TASI' if symbol == '90001' else 'Unknown Index'
    else:
        df_copy['name'] = SECTOR_NAMES.get(symbol) or df_copy.get('name', 'Unknown')
        if isinstance(df_copy['name'], pd.Series):
            df_copy['name'] = df_copy['name'].fillna('Unknown')

    df_copy['volume'] = df_copy['volume'].fillna(0).astype(int) if 'volume' in df_copy.columns else 0
    
    if table_type == 'stock':
        df_copy['open_interest'] = df_copy.get('open_interest', 0).fillna(0).astype(int)
        df_copy['source'] = f'historical_sync_{timeframe}'
        cols = ['time', 'symbol', 'timeframe', 'name', 'open', 'high', 'low', 'close', 'volume', 'open_interest', 'source']
    else:
        df_copy['source'] = 'local_import'
        cols = ['time', 'symbol', 'name', 'timeframe', 'open', 'high', 'low', 'close', 'volume', 'source']

    # التأكد من وجود جميع الأعمدة المطلوبة
    for c in cols:
        if c not in df_copy.columns:
            if c in ['open', 'high', 'low', 'close']:
                logger.error(f"❌ Missing required column {c} for {symbol}")
                return 0
            df_copy[c] = None if c == 'name' else 0 if c in ['volume', 'open_interest'] else 'local_import'

    df_copy = df_copy[cols]
    records = [tuple(row) for row in df_copy.itertuples(index=False, name=None)]
    if not records:
        return 0

    try:
        async with pool.acquire() as conn:
            if table_type == 'stock':
                sql = """
                INSERT INTO market_data.ohlcv
                    (time, symbol, timeframe, name, open, high, low, close, volume, open_interest, source)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                ON CONFLICT (time, symbol, timeframe) DO UPDATE SET
                    open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
                    close=EXCLUDED.close, volume=EXCLUDED.volume,
                    open_interest=EXCLUDED.open_interest, source=EXCLUDED.source;
                """
            elif table_type == 'sector':
                sql = """
                INSERT INTO market_data.sector_performance
                    (time, symbol, name, timeframe, open, high, low, close, volume, source)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                ON CONFLICT (symbol, timeframe, time) DO UPDATE SET
                    open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
                    close=EXCLUDED.close, volume=EXCLUDED.volume, source=EXCLUDED.source;
                """
            elif table_type == 'index':
                sql = """
                INSERT INTO market_data.index_performance
                    (time, symbol, name, timeframe, open, high, low, close, volume, source)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                ON CONFLICT (symbol, timeframe, time) DO UPDATE SET
                    open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
                    close=EXCLUDED.close, volume=EXCLUDED.volume, source=EXCLUDED.source;
                """
            
            await conn.executemany(sql, records)
        return len(records)
    except Exception as e:
        logger.error(f"❌ DB save error [{symbol} {timeframe}]: {e}")
        return 0

# ─────────────────────────────────────────────────────────────────────────────
# Local Import Logic
# ─────────────────────────────────────────────────────────────────────────────

async def import_local_data(pool: asyncpg.Pool, data_dir: str):
    """استيراد البيانات من المجلدات المحلية."""
    base_path = Path(data_dir)
    if not base_path.exists():
        logger.error(f"❌ Directory not found: {data_dir}")
        return

    logger.info(f"📂 Starting local import from {data_dir}")
    total_saved = 0

    # 1. استيراد بيانات الأسهم (METASTOCK)
    metastock_dir = base_path / 'METASTOCK'
    if metastock_dir.exists():
        logger.info("📊 Importing METASTOCK data...")
        for file_path in metastock_dir.glob('*.mwd'): # أو .csv إذا كانت مصدرة كـ csv
            symbol = file_path.stem
            try:
                # افتراض أن ملفات ميتاستوك تم تصديرها كـ CSV أو يمكن قراءتها بـ pandas
                # إذا كانت بصيغة ثنائية خاصة، ستحتاج لمكتبة خاصة. هنا نفترض CSV للتبسيط
                df = pd.read_csv(file_path)
                saved = await save_historical_to_db(pool, df, symbol, '1d', 'stock')
                logger.info(f"  ✅ {symbol}: {saved} rows saved (Stock)")
                total_saved += saved
            except Exception as e:
                logger.error(f"  ❌ Failed to read {file_path.name}: {e}")

    # 2. استيراد بيانات القطاعات (CSV/SECTORS)
    sectors_dir = base_path / 'CSV' / 'SECTORS'
    if sectors_dir.exists():
        logger.info("🏭 Importing SECTORS data...")
        for file_path in sectors_dir.glob('*.csv'):
            english_name = file_path.stem
            symbol = ENGLISH_SECTOR_MAP.get(english_name)
            
            if not symbol:
                # محاولة مطابقة جزئية
                for key, val in ENGLISH_SECTOR_MAP.items():
                    if key.lower() in english_name.lower() or english_name.lower() in key.lower():
                        symbol = val
                        break
            
            if not symbol:
                logger.warning(f"  ⚠️ Could not map sector name: {english_name}")
                continue
                
            try:
                df = pd.read_csv(file_path)
                saved = await save_historical_to_db(pool, df, symbol, '1d', 'sector')
                logger.info(f"  ✅ {english_name} ({symbol}): {saved} rows saved (Sector)")
                total_saved += saved
            except Exception as e:
                logger.error(f"  ❌ Failed to read {file_path.name}: {e}")

    # 3. استيراد بيانات المؤشرات (CSV/INDICES)
    indices_dir = base_path / 'CSV' / 'INDICES'
    if indices_dir.exists():
        logger.info("📈 Importing INDICES data...")
        for file_path in indices_dir.glob('*.csv'):
            # افتراض أن المؤشر العام اسمه TASI أو 90001
            symbol = '90001' if 'tasi' in file_path.stem.lower() else file_path.stem
            try:
                df = pd.read_csv(file_path)
                saved = await save_historical_to_db(pool, df, symbol, '1d', 'index')
                logger.info(f"  ✅ {file_path.stem} ({symbol}): {saved} rows saved (Index)")
                total_saved += saved
            except Exception as e:
                logger.error(f"  ❌ Failed to read {file_path.name}: {e}")

    logger.success(f"🎉 Local import complete! Total rows saved: {total_saved}")

# ─────────────────────────────────────────────────────────────────────────────
# Fetch Logic (Sahmk API)
# ─────────────────────────────────────────────────────────────────────────────

async def fetch_one_symbol(
    sahmk,
    pool: asyncpg.Pool,
    symbol: str,
    timeframe: str,
    days: int,
    semaphore: asyncio.Semaphore
) -> bool:
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
                saved = await save_historical_to_db(pool, df, symbol, timeframe, 'stock')
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
    stock_symbols = [s for s in symbols if not is_sector_symbol(s)]
    logger.info(f"📊 [{timeframe}] Fetching {len(stock_symbols)} symbols ({days} days back) ...")

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
    symbol_filter: Optional[str] = None,
    import_dir: Optional[str] = None
) -> None:
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("🗄️  Alpha Engine2 — Historical Sync")
    logger.info(f"   Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    pool = await init_db_pool()
    if pool is None:
        logger.critical("❌ Cannot connect to DB. Aborting.")
        return

    if import_dir:
        await import_local_data(pool, import_dir)
    else:
        try:
            sahmk = get_sahmk_client()
            logger.success("✅ Sahmk client ready")
        except Exception as e:
            logger.critical(f"❌ Sahmk client init failed: {e}")
            await pool.close()
            return

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

        tfs = timeframes or DEFAULT_TIMEFRAMES
        for tf, days in tfs:
            await sync_timeframe(sahmk, pool, symbols, tf, days)

    await pool.close()
    elapsed = (datetime.now() - start_time).total_seconds()
    logger.success("=" * 60)
    logger.success(f"✅ Historical sync complete in {elapsed:.0f}s")
    logger.success("=" * 60)

def main():
    parser = argparse.ArgumentParser(
        description="Alpha Engine2 — Historical Data Sync",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
أمثلة:
  python scripts/historical_sync.py
  python scripts/historical_sync.py --timeframe 1d
  python scripts/historical_sync.py --symbol 2222
  python scripts/historical_sync.py --import /path/to/data
        """
    )
    parser.add_argument("--timeframe", "-t", choices=["1d", "1h", "30m", "15m", "5m", "1m"], help="إطار زمني واحد فقط")
    parser.add_argument("--days", "-d", type=int, default=None, help="عدد الأيام للرجوع إليها")
    parser.add_argument("--symbol", "-s", type=str, default=None, help="رمز سهم واحد فقط")
    parser.add_argument("--import_dir", "--import", type=str, default=None, help="مسار المجلد الرئيسي لاستيراد البيانات المحلية")
    
    args = parser.parse_args()

    if args.timeframe:
        default_days = dict(DEFAULT_TIMEFRAMES).get(args.timeframe, 30)
        days = args.days or default_days
        timeframes = [(args.timeframe, days)]
    elif args.days:
        timeframes = [(tf, args.days) for tf, _ in DEFAULT_TIMEFRAMES]
    else:
        timeframes = None

    try:
        asyncio.run(run_sync(
            timeframes=timeframes,
            symbol_filter=args.symbol,
            import_dir=args.import_dir
        ))
    except KeyboardInterrupt:
        logger.info("🛑 Historical sync stopped by user.")
    except Exception as e:
        logger.critical(f"💥 Historical sync crashed: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
