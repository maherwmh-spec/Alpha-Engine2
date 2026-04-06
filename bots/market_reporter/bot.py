#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Bot 2: Market Reporter - مُراسل السوق (Async Refactor)
Collects real-time and historical market data using Sahmk API

Refactor Notes:
- Full async: All DB/REST operations are non-blocking.
- Parallelism: Fetches historical data and applies filters concurrently.
- Robustness: Improved error handling, retries, and state management.
- Efficiency: Uses asyncpg for direct, high-performance PostgreSQL interaction.
- FIX: redis_manager.get/set are SYNC — removed all await calls on them.
- FIX: _save_historical_to_db uses executemany (not broken unnest cast).
- FIX: All schema columns included (timeframe, name, open_interest).
- FIX: Timestamps are timezone-aware for TimescaleDB.
- FIX: volume cast to int (bigint in DB).
- FIX: Callbacks use get_event_loop() safely.
"""

import asyncio
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import aiohttp
import asyncpg
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from loguru import logger

from config.config_manager import config
from scripts.redis_manager import redis_manager
from scripts.sahmk_client import SahmkClient, get_sahmk_client, is_tasi_or_sector
from scripts.utils import get_saudi_time, is_trading_hours
from scripts.sector_calculator import (
    compute_sector_candles_from_db,
    save_sector_candles_to_db,
    save_index_to_db,
    is_sector_symbol,
    SECTOR_DISPLAY_NAMES,
)

# --- Constants ---
DB_POOL: Optional[asyncpg.Pool] = None
FETCH_CONCURRENCY = 20  # Number of symbols to fetch in parallel


# قاموس أسماء القطاعات — مصدره sector_calculator
SECTOR_NAMES: Dict[str, str] = SECTOR_DISPLAY_NAMES


class MarketReporter:
    """
    Real-time and historical market data collector (Async Version)
    """

    def __init__(self):
        self.name = "market_reporter"
        self.logger = logger.bind(bot=self.name)
        self.bot_config = config.get_bot_config(self.name)

        # --- Configurable Parameters ---
        self._load_config()

        # --- State ---
        self.sahmk: Optional[SahmkClient] = None
        self._realtime_active = False
        self._subscribed_syms: List[str] = []
        self._candles_saved = 0

        # Queue لنقل الشموع من WebSocket thread إلى async consumer
        # run_coroutine_threadsafe يُجدول لكن قد لا يُنفَّذ إذا كان الـ loop مشغولاً
        # الحل: Queue + consumer task مستقل يعمل دائماً
        self._candle_queue: asyncio.Queue = None  # يُهيَّأ في run()

        self.logger.info("✅ MarketReporter initialized (Async)")

    def _load_config(self):
        """Load all configurations from YAML."""
        lf = config.get("liquidity_filter", {})
        self.rv_threshold    = lf.get("relative_volume_threshold", 1.5)
        self.min_active_days = lf.get("min_trading_days", 20)
        self.min_avg_change  = lf.get("min_avg_daily_change", 0.008)
        self.lf_lookback_days = lf.get("lookback_days", 30)

        vf = config.get("volatility_filter", {})
        self.max_gap_pct = vf.get("max_gap_threshold", 0.075)

    # ═══════════════════════════════════════════════════════════════════════
    # Main Entry & Lifecycle
    # ═══════════════════════════════════════════════════════════════════════

    async def run(self):
        """Main async entry point - starts all data collection (LONG-RUNNING)."""
        self.logger.info("🚀 MarketReporter starting...")

        # حفظ الـ event loop الرئيسي لاستخدامه في الـ callback القادم من WebSocket thread
        self._main_loop = asyncio.get_running_loop()

        # تهيئة Queue لنقل الشموع من WebSocket thread إلى async consumer
        self._candle_queue = asyncio.Queue(maxsize=10000)

        try:
            self.sahmk = get_sahmk_client()
            self.sahmk.set_on_candle_complete(self._on_candle_complete)
            self.sahmk.set_on_tick(self._on_tick_received)
            self.logger.success("✅ Sahmk API client ready")
        except Exception as e:
            self.logger.critical(f"❌ Sahmk client init failed: {e}. Heartbeat-only mode.")
            await self._heartbeat_only_mode()
            return

        await self._init_db_pool()

        # بدء consumer task مستقل يحفظ الشموع في DB بشكل مستمر
        # يُبدأ بعد _init_db_pool لضمان أن DB_POOL جاهز
        asyncio.create_task(self._candle_db_consumer())
        self.logger.success("✅ Candle DB consumer task started")

        # --- Main Loop ---
        while True:
            try:
                self.logger.info("🔄 Starting main cycle: Fetch, Filter, Stream...")

                # Step 1: Fetch historical data for all symbols concurrently
                all_symbols = self.sahmk.get_symbols_list() or self._default_symbols()
                self.logger.info(f"\ud83d\udccb Total symbols from API: {len(all_symbols)}")

                # جلب جميع الفواصل الزمنية المطلوبة
                # --- جلب جميع الأطر الزمنية المطلوبة ---
                timeframes_config = [
                    ("1d",  365 * 5), # 5 سنوات
                    ("1h",  180),     # 6 أشهر
                    ("30m", 90),      # 3 أشهر
                    ("15m", 60),      # شهران
                    ("5m",  30),      # شهر واحد
                    ("1m",  7),       # أسبوع واحد
                ]
                for tf, days in timeframes_config:
                    self.logger.info(f"\ud83d\udcca Fetching {tf} data for {len(all_symbols)} symbols ({days} days)...")
                    await self.fetch_all_symbols_historical(
                        all_symbols, timeframe=tf, days=days
                    )

                # Step 2: Apply filters to get the active watchlist
                filtered_symbols = await self.get_filtered_symbols_for_analysis(all_symbols)

                # Step 3: Start/update the real-time stream with the filtered list
                await self.start_realtime_stream(filtered_symbols)

                self.logger.success(
                    f"✅ Cycle complete. Streaming {len(filtered_symbols)} symbols. "
                    f"Next refresh in 1 hour."
                )

            except Exception as e:
                self.logger.error(f"❌ Unhandled error in main loop: {e}", exc_info=True)

            await asyncio.sleep(3600)  # Refresh every hour

    async def _heartbeat_only_mode(self):
        """Fallback loop when essential services fail to initialize."""
        tick = 0
        while True:
            await asyncio.sleep(60)
            tick += 1
            self.logger.info(f"💓 MarketReporter heartbeat (degraded) | tick={tick}")

    # ═══════════════════════════════════════════════════════════════════════
    # Database Operations (Async via asyncpg)
    # ═══════════════════════════════════════════════════════════════════════

    async def _init_db_pool(self):
        """Initialize the asyncpg database pool."""
        global DB_POOL
        if DB_POOL is None:
            try:
                dsn = os.environ.get(
                    "DATABASE_URL",
                    "postgresql://alpha_user:alpha_password_2024@postgres:5432/alpha_engine"
                )
                DB_POOL = await asyncpg.create_pool(dsn=dsn, min_size=5, max_size=20)
                self.logger.success("✅ Database pool initialized.")
            except Exception as e:
                self.logger.critical(f"❌ DB pool creation failed: {e}")
                DB_POOL = None

    async def _save_candle_to_db(self, candle: Dict):
        """
        Save a 1m candle to TimescaleDB.
        Schema PK: (time, symbol, timeframe)  — timeframe is required.
        volume is bigint → must be int, not float.

        للقطاعات (900xx): source = 'db_sector_calculator'
        للأسهم العادية:   source = 'sahmk_websocket'

        فلتر الحفظ باستخدام is_tasi_or_sector():
        - مسموح: أسهم تاسي (4 أرقام، يبدأ بـ 1-8) + قطاعات 900xx
        - مستبعد: أي رمز يبدأ بـ 9 وليس 900 (نمو/ETFs مثل 9401, 9510)
        """
        if DB_POOL is None:
            self.logger.warning("DB pool not available, skipping candle save.")
            return

        # ── فلتر الحفظ: استبعاد رموز 9xx غير 900 (نمو/ETFs) ──
        symbol = candle.get('symbol', '')
        if not is_tasi_or_sector(symbol):
            self.logger.warning(
                f"🚫 SAVE BLOCKED: symbol '{symbol}' is Nomu/ETF "
                f"(starts with 9, not 900) — skipping DB save"
            )
            return

        # Ensure timezone-aware timestamp
        ts = pd.to_datetime(candle['timestamp'])
        if ts.tzinfo is None:
            import pytz
            ts = pytz.timezone('Asia/Riyadh').localize(ts)

        # تحديد المصدر بناءً على نوع الرمز
        symbol = candle['symbol']
        source = candle.get('source') or (
            'db_sector_calculator' if is_sector_symbol(symbol) else 'sahmk_websocket'
        )
        name = SECTOR_NAMES.get(symbol) or candle.get('name') or 'Unknown'

        # ── INSERT مع fallback: جرّب مع source أولاً، ثم بدونه ──────────────
        sql_with_source = """
        INSERT INTO market_data.ohlcv
            (time, symbol, timeframe, name, open, high, low, close,
             volume, open_interest, source)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (time, symbol, timeframe) DO UPDATE SET
            open          = EXCLUDED.open,
            high          = EXCLUDED.high,
            low           = EXCLUDED.low,
            close         = EXCLUDED.close,
            volume        = EXCLUDED.volume,
            open_interest = EXCLUDED.open_interest,
            source        = EXCLUDED.source;
        """
        sql_without_source = """
        INSERT INTO market_data.ohlcv
            (time, symbol, timeframe, name, open, high, low, close,
             volume, open_interest)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (time, symbol, timeframe) DO UPDATE SET
            open          = EXCLUDED.open,
            high          = EXCLUDED.high,
            low           = EXCLUDED.low,
            close         = EXCLUDED.close,
            volume        = EXCLUDED.volume,
            open_interest = EXCLUDED.open_interest;
        """
        args_base = (
            ts,
            symbol,
            '1m',
            name,
            float(candle['open']),
            float(candle['high']),
            float(candle['low']),
            float(candle['close']),
            int(float(candle['volume'])),
            0,
        )
        try:
            async with DB_POOL.acquire() as conn:
                try:
                    await conn.execute(sql_with_source, *args_base, source)
                    self.logger.debug(
                        f"✅ Saved 1m candle: {symbol} @ {ts} | "
                        f"close={candle['close']} vol={candle['volume']}"
                    )
                except Exception as e_src:
                    # إذا فشل بسبب عمود source غير موجود → جرّب بدونه
                    err_str = str(e_src).lower()
                    if 'source' in err_str or 'column' in err_str:
                        self.logger.warning(
                            f"⚠️ 'source' column missing in ohlcv — "
                            f"falling back to INSERT without source. "
                            f"Run: psql -f migrations/003_fix_ohlcv_schema.sql"
                        )
                        await conn.execute(sql_without_source, *args_base)
                        self.logger.debug(
                            f"✅ Saved 1m candle (no-source): {symbol} @ {ts}"
                        )
                    else:
                        raise
        except Exception as e:
            self.logger.error(f"❌ DB save error for {candle['symbol']}: {e}")

    async def _save_historical_to_db(self, df: pd.DataFrame, symbol: str, timeframe: str):
        """
        Save historical DataFrame to TimescaleDB using asyncpg executemany.
        Uses plain INSERT with positional params (NOT unnest cast which fails).
        """
        if DB_POOL is None or df.empty:
            return

        df_copy = df.copy()

        # Rename timestamp → time if needed
        if 'timestamp' in df_copy.columns:
            df_copy.rename(columns={'timestamp': 'time'}, inplace=True)

        # Ensure timezone-aware timestamps
        df_copy['time'] = pd.to_datetime(df_copy['time'])
        if df_copy['time'].dt.tz is None:
            df_copy['time'] = df_copy['time'].dt.tz_localize('Asia/Riyadh')
        else:
            df_copy['time'] = df_copy['time'].dt.tz_convert('Asia/Riyadh')

        # Fill required schema columns
        df_copy['symbol']        = symbol
        df_copy['timeframe']     = timeframe
        # تعيين الاسم الصحيح: للقطاعات من SECTOR_NAMES، للأسهم من البيانات أو 'Unknown'
        sector_name = SECTOR_NAMES.get(symbol)
        if sector_name:
            df_copy['name'] = sector_name
        elif 'name' in df_copy.columns:
            df_copy['name'] = df_copy['name'].fillna('Unknown')
        else:
            df_copy['name'] = 'Unknown'
        df_copy['open_interest'] = df_copy['open_interest'].fillna(0).astype(int) \
                                   if 'open_interest' in df_copy.columns else 0
        df_copy['source']        = f'sahmk_rest_{timeframe}'

        # volume must be bigint
        df_copy['volume'] = df_copy['volume'].fillna(0).astype(int)

        cols = ['time', 'symbol', 'timeframe', 'name',
                'open', 'high', 'low', 'close',
                'volume', 'open_interest', 'source']
        df_copy = df_copy[[c for c in cols if c in df_copy.columns]]

        records = [tuple(row) for row in df_copy.itertuples(index=False, name=None)]

        sql_upsert_with_source = """
        INSERT INTO market_data.ohlcv
            (time, symbol, timeframe, name, open, high, low, close,
             volume, open_interest, source)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (time, symbol, timeframe) DO UPDATE SET
            open          = EXCLUDED.open,
            high          = EXCLUDED.high,
            low           = EXCLUDED.low,
            close         = EXCLUDED.close,
            volume        = EXCLUDED.volume,
            open_interest = EXCLUDED.open_interest,
            source        = EXCLUDED.source;
        """
        sql_upsert_no_source = """
        INSERT INTO market_data.ohlcv
            (time, symbol, timeframe, name, open, high, low, close,
             volume, open_interest)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (time, symbol, timeframe) DO UPDATE SET
            open          = EXCLUDED.open,
            high          = EXCLUDED.high,
            low           = EXCLUDED.low,
            close         = EXCLUDED.close,
            volume        = EXCLUDED.volume,
            open_interest = EXCLUDED.open_interest;
        """
        # records_no_source: إزالة عمود source (آخر عمود)
        records_no_source = [r[:10] for r in records]
        try:
            async with DB_POOL.acquire() as conn:
                try:
                    await conn.executemany(sql_upsert_with_source, records)
                    self.logger.debug(
                        f"💾 Saved {len(records)} historical candles [{symbol} {timeframe}]"
                    )
                except Exception as e_src:
                    err_str = str(e_src).lower()
                    if 'source' in err_str or 'column' in err_str:
                        self.logger.warning(
                            f"⚠️ 'source' column missing — saving {symbol} without source. "
                            f"Run: psql -f migrations/003_fix_ohlcv_schema.sql"
                        )
                        await conn.executemany(sql_upsert_no_source, records_no_source)
                        self.logger.debug(
                            f"💾 Saved {len(records)} historical candles (no-source) [{symbol} {timeframe}]"
                        )
                    else:
                        raise
        except Exception as e:
            self.logger.error(f"❌ Historical DB save error for {symbol}: {e}")

    async def _fetch_ohlcv_for_filter(self, symbol: str) -> Optional[pd.DataFrame]:
        """Fetch recent daily OHLCV from DB for filtering (async)."""
        if DB_POOL is None:
            return None

        sql = """
        SELECT time, close, volume FROM market_data.ohlcv
        WHERE symbol = $1 AND timeframe = '1d'
          AND time >= $2
        ORDER BY time ASC;
        """
        try:
            async with DB_POOL.acquire() as conn:
                records = await conn.fetch(
                    sql,
                    symbol,
                    get_saudi_time() - timedelta(days=self.lf_lookback_days + 5)
                )
            if not records:
                return None
            return pd.DataFrame(records, columns=['time', 'close', 'volume'])
        except Exception as e:
            self.logger.error(f"❌ Filter data fetch error for {symbol}: {e}")
            return None

    # ═══════════════════════════════════════════════════════════════════════
    # Real-time Stream & Callbacks
    # ═══════════════════════════════════════════════════════════════════════

    async def start_realtime_stream(self, symbols: List[str]):
        """Start or update the Sahmk WebSocket stream."""
        if not self.sahmk:
            self.logger.error("❌ Cannot start stream: Sahmk client not available.")
            return

        # Skip restart if symbol list is unchanged
        if self._realtime_active and sorted(self._subscribed_syms) == sorted(symbols):
            self.logger.info("👍 Symbol list unchanged, no stream restart needed.")
            return

        if self._realtime_active:
            self.logger.info("🔄 Restarting stream with updated symbol list...")
            self.sahmk.stop_realtime_stream()
            await asyncio.sleep(5)  # Grace period

        self._subscribed_syms = symbols
        self._realtime_active = True

        self.logger.info(f"📡 Starting WebSocket stream for {len(symbols)} symbols.")
        self.sahmk.start_realtime_stream(symbols)
        self.logger.success("✅ Real-time stream active.")

        # --- بدء حلقة جلب بيانات القطاعات عبر REST API ---
        if not hasattr(self, '_sector_fetch_task') or self._sector_fetch_task.done():
            self.logger.info("📊 Starting sector/index REST API polling loop...")
            self._sector_fetch_task = asyncio.create_task(self._sector_polling_loop())

    def _on_tick_received(self, tick: Dict):
        """
        Callback from WebSocket thread.
        redis_manager.set is SYNC — call directly, no await.
        """
        try:
            redis_manager.set(
                f"realtime:price:{tick['symbol']}",
                {
                    'price':     tick['price'],
                    'volume':    tick.get('volume', 0),
                    'timestamp': str(tick['timestamp'])
                },
                ttl=10
            )
        except Exception as e:
            self.logger.error(f"❌ Tick callback error: {e}")

    def _on_candle_complete(self, candle: Dict):
        """
        Callback from WebSocket thread (non-async context).

        الإصلاح النهائي:
          بدلاً من run_coroutine_threadsafe (يُجدول لكن قد لا يُنفَّذ إذا كان الـ loop مشغولاً
          بعمليات طويلة كجلب البيانات التاريخية)، نضع الشمعة في Queue thread-safe.
          consumer task مستقل يقرأ من الـ Queue ويحفظ في DB بشكل مستمر.
        """
        # ── Redis (sync) ──────────────────────────────────────────────────────────
        try:
            redis_manager.set(
                f"ohlcv:1m:{candle['symbol']}:latest",
                {k: str(v) if isinstance(v, datetime) else v
                 for k, v in candle.items()},
                ttl=120
            )
            self._candles_saved += 1
            if self._candles_saved % 100 == 0:
                self.logger.info(f"📈 {self._candles_saved} candles processed (Redis+DB).")
        except Exception as e:
            self.logger.error(f"❌ Redis candle save error: {e}")

        # ── DB (async → عبر Queue thread-safe) ──────────────────────────────────
        loop = getattr(self, '_main_loop', None)
        queue = getattr(self, '_candle_queue', None)
        if loop is not None and queue is not None and loop.is_running():
            try:
                # put_nowait لا يحتاج await — آمن من threads
                loop.call_soon_threadsafe(queue.put_nowait, candle)
            except asyncio.QueueFull:
                self.logger.warning(
                    f"⚠️ Candle queue full — dropping candle for {candle['symbol']}"
                )
            except Exception as e:
                self.logger.error(f"❌ Queue put error for {candle['symbol']}: {e}")
        else:
            self.logger.warning(
                f"⚠️ Candle for {candle['symbol']} received before main loop ready — skipped DB save"
            )

    async def _candle_db_consumer(self):
        """
        Consumer task مستقل يقرأ الشموع من الـ Queue ويحفظها في DB.
        يعمل بشكل مستمر طوال عمر البوت — لا يتوقف حتى لو كان الـ loop مشغولاً
        بعمليات أخرى (جلب تاريخي، فلترة، إلخ).
        """
        self.logger.info("🗄️  Candle DB consumer started — waiting for candles...")
        batch: List[Dict] = []
        BATCH_SIZE = 50       # احفظ كل 50 شمعة دفعة واحدة
        FLUSH_INTERVAL = 5.0  # أو كل 5 ثوانٍ على الأكثر

        last_flush = asyncio.get_running_loop().time()

        while True:
            try:
                # انتظر شمعة جديدة بحد أقصى FLUSH_INTERVAL ثانية
                now = asyncio.get_running_loop().time()
                timeout = FLUSH_INTERVAL - (now - last_flush)
                timeout = max(0.1, timeout)
                try:
                    candle = await asyncio.wait_for(
                        self._candle_queue.get(), timeout=timeout
                    )
                    batch.append(candle)
                    self._candle_queue.task_done()
                except asyncio.TimeoutError:
                    pass  # flush ما تراكم

                # flush إذا امتلأت الـ batch أو انتهى الـ interval
                now = asyncio.get_running_loop().time()
                should_flush = (
                    len(batch) >= BATCH_SIZE or
                    (batch and (now - last_flush) >= FLUSH_INTERVAL)
                )

                if should_flush and batch:
                    saved = 0
                    for c in batch:
                        try:
                            await self._save_candle_to_db(c)
                            saved += 1
                        except Exception as e:
                            self.logger.error(
                                f"❌ Consumer DB save error [{c.get('symbol')}]: {e}"
                            )
                    self.logger.info(
                        f"🗄️  Flushed {saved}/{len(batch)} candles to DB "
                        f"(queue size: {self._candle_queue.qsize()})"
                    )
                    batch.clear()
                    last_flush = now

            except Exception as e:
                self.logger.error(f"❌ Candle consumer error: {e}", exc_info=True)
                await asyncio.sleep(1)

    # ═══════════════════════════════════════════════════════════════════════
    # Historical Data (Async & Parallel)
    # ═══════════════════════════════════════════════════════════════════════

    async def fetch_historical_data(
        self, symbol: str, timeframe: str, days: int
    ) -> Optional[pd.DataFrame]:
        """Fetch and save historical OHLCV for one symbol."""
        if not self.sahmk:
            return None
        try:
            end   = get_saudi_time()
            start = end - timedelta(days=days)
            df = await asyncio.to_thread(
                self.sahmk.get_historical_ohlcv,
                symbol=symbol, timeframe=timeframe,
                start_date=start, end_date=end
            )
            if df is not None and not df.empty:
                await self._save_historical_to_db(df, symbol, timeframe)
                return df
        except Exception as e:
            self.logger.error(f"❌ Historical fetch error [{symbol}]: {e}")
        return None

    async def fetch_all_symbols_historical(
        self, symbols: List[str], timeframe: str, days: int
    ):
        """
        Fetch historical data for all symbols concurrently.

        القطاعات (900xx): تُستبعد تماماً من هنا — لا توجد بيانات تاريخية لها.
        بياناتها اللحظية تُحسب من الأسهم في DB عبر _sector_polling_loop.
        """
        stock_symbols = [s for s in symbols if not is_sector_symbol(s)]

        self.logger.info(
            f"📊 Fetching historical data ({timeframe}) | {len(stock_symbols)} stocks via Sahmk"
        )

        semaphore = asyncio.Semaphore(FETCH_CONCURRENCY)
        tasks = [
            self._fetch_and_log(sym, timeframe, days, semaphore)
            for sym in stock_symbols
        ]
        results = await asyncio.gather(*tasks)
        ok = sum(1 for r in results if r is not None)
        self.logger.success(
            f"✅ Stocks historical fetch ({timeframe}) | ✅{ok} ❌{len(stock_symbols) - ok}"
        )

    async def _fetch_and_log(
        self, symbol: str, timeframe: str, days: int, semaphore: asyncio.Semaphore
    ) -> Optional[pd.DataFrame]:
        async with semaphore:
            return await self.fetch_historical_data(symbol, timeframe, days)

    # ═══════════════════════════════════════════════════════════════════════
    # Symbol Filtering (Async & Parallel)
    # ═══════════════════════════════════════════════════════════════════════

    # --- تغيير فلسفة الفلترة: جمع شامل، فلترة لاحقة ---
    # الفلاتر الصارمة (سيولة، تذبذب) تم نقلها إلى الروبوتات اللاحقة (Scientist, Monitor)
    # هنا، نطبق فلتر خفيف جداً فقط لاستبعاد الأسهم الميتة تماماً.
    async def get_filtered_symbols_for_analysis(
        self, all_symbols: List[str]
    ) -> List[str]:
        """Applies a very light filter to exclude dead stocks, then returns all symbols.

        فلتر الرموز باستخدام is_tasi_or_sector():
        - مسموح: أسهم تاسي (4 أرقام، يبدأ بـ 1-8) + قطاعات 900xx
        - مستبعد: أسهم نمو/ETFs (4 أرقام تبدأ بـ 9، ليس 900)
        """
        # redis_manager.get is SYNC — no await
        cached = redis_manager.get('filtered_symbols:ready')
        if cached:
            return cached

        # ── الفلتر الأول: استبعاد رموز 9xx غير 900 (نمو/ETFs) باستخدام is_tasi_or_sector() ──
        excluded_9xx = [
            s for s in all_symbols
            if s.startswith('9') and not s.startswith('900')
        ]
        tasi_only = [
            s for s in all_symbols
            if is_tasi_or_sector(s)
        ]

        if excluded_9xx:
            self.logger.info(
                f"🚫 Excluded {len(excluded_9xx)} symbols starting with 9 "
                f"(Nomu/ETFs) from analysis: {sorted(excluded_9xx)[:10]}"
                f"{'...' if len(excluded_9xx) > 10 else ''}"
            )
        tasi_count   = sum(1 for s in tasi_only if len(s) == 4 and s[0] in '12345678')
        sector_count = sum(1 for s in tasi_only if s.startswith('900'))
        self.logger.info(
            f"🔍 TASI symbols filtered: {tasi_count} TASI stocks + {sector_count} sectors/index "
            f"(Nomu and ETFs starting with 9 excluded)"
        )
        self.logger.info(f"🔍 Filtering {len(tasi_only)} TASI+sector symbols (after 9xx exclusion)...")

        # Fetch all data in parallel from DB
        semaphore = asyncio.Semaphore(FETCH_CONCURRENCY)
        fetch_tasks = [
            self._fetch_ohlcv_for_filter_safe(sym, semaphore)
            for sym in tasi_only if sym not in SECTOR_NAMES  # لا تحاول جلب بيانات تاريخية للقطاعات
        ]
        data_frames = await asyncio.gather(*fetch_tasks)

        # --- الفلترة الخفيفة: فقط استبعاد الأسهم ذات حجم تداول صفر لمدة 30 يوماً ---
        stock_syms = [s for s in tasi_only if s not in SECTOR_NAMES]
        filter_tasks = [
            self._apply_light_filter(sym, df)
            for sym, df in zip(stock_syms, data_frames)
            if df is not None and not df.empty
        ]
        filter_results = await asyncio.gather(*filter_tasks)

        # كل الرموز التي لم يتم استبعادها تعتبر "ناجحة"
        passing_symbols = [sym for sym, passed in filter_results if passed]

        # إضافة القطاعات والمؤشر العام دائماً إلى قائمة الرموز المفلترة
        for sector_sym in SECTOR_NAMES.keys():
            if sector_sym not in passing_symbols:
                passing_symbols.append(sector_sym)

        self.logger.success(
            f"✅ Filtering complete: {len(passing_symbols)} symbols passed "
            f"({len(excluded_9xx)} Nomu/ETF 9xx symbols excluded)."
        )
        # redis_manager.set is SYNC — no await
        redis_manager.set('filtered_symbols:ready', passing_symbols, ttl=300)
        return passing_symbols

    async def _fetch_ohlcv_for_filter_safe(
        self, symbol: str, semaphore: asyncio.Semaphore
    ) -> Optional[pd.DataFrame]:
        async with semaphore:
            return await self._fetch_ohlcv_for_filter(symbol)

    async def _apply_light_filter(
        self, symbol: str, df: pd.DataFrame
    ) -> Tuple[str, bool]:
        """Applies a very light filter: exclude if volume has been zero for 10+ days."""
        try:
            # فلتر خفيف: إذا كان حجم التداول صفراً لآخر 10 أيام، استبعد السهم
            # فلتر خفيف جداً: إذا كان حجم التداول صفراً لآخر 30 يوماً، استبعد السهم
            if len(df) >= 30 and (df["volume"].tail(30) == 0).all():
                self.logger.debug(f"Excluding {symbol}: Zero volume for last 30 days.")
                return symbol, False

            # إذا لم يتم استبعاده، فإنه ينجح في الفلتر
            return symbol, True
        except Exception as e:
            self.logger.error(f"❌ Filter error for {symbol}: {e}")
            return symbol, False

    # ═══════════════════════════════════════════════════════════════════════
    # Utilities
    # ═══════════════════════════════════════════════════════════════════════

    async def _sector_polling_loop(self):
        """
        حلقة تعمل كل دقيقة لحساب بيانات القطاعات والمؤشر العام.
        يحسب VWAP من بيانات الأسهم الموجودة في DB (آخر 10 دقائق).
        يعمل دائماً — خلال التداول وخارجه — طالما توجد بيانات في DB.
        """
        self.logger.info("📊 Sector polling loop started (DB VWAP calculator — always on)")
        while True:
            await asyncio.sleep(60)  # انتظر دقيقة

            all_candles: Dict = {}

            # ── المصدر الأساسي: حساب من DB ───────────────────────────────
            try:
                if DB_POOL is not None:
                    async with DB_POOL.acquire() as conn:
                        all_candles = await compute_sector_candles_from_db(conn)
                    if all_candles:
                        self.logger.debug(
                            f"✅ DB calculator: {len(all_candles)} sector candles computed"
                        )
            except Exception as e:
                self.logger.warning(f"⚠️ DB sector calculator error: {e}")
                all_candles = {}

            if not all_candles:
                self.logger.warning(
                    "⚠️ DB calculator returned no data — "
                    "waiting for stock candles to accumulate in DB (normal at startup)"
                )
                continue
            # ── حفظ القطاعات في sector_candles + المؤشر في indices ────────────────────────
            try:
                async with DB_POOL.acquire() as save_conn:
                    # 1. حفظ جميع الشمع (قطاعات + مؤشر) في sector_candles
                    saved_count = await save_sector_candles_to_db(save_conn, all_candles)

                    # 2. حفظ المؤشر العام (90001) بشكل منفصل في indices
                    if '90001' in all_candles:
                        await save_index_to_db(save_conn, all_candles['90001'])

                tasi_ok = '✅' if '90001' in all_candles else '❌'
                sector_count = len([s for s in all_candles if s != '90001'])
                self.logger.success(
                    f"✅ Sector data saved to sector_candles: {saved_count} rows "
                    f"(TASI={tasi_ok}, sectors={sector_count})"
                )
            except Exception as e:
                self.logger.error(f"❌ Sector save error: {e}", exc_info=True)

    def _default_symbols(self) -> List[str]:
        """Fallback TASI symbols."""
        return [
            "2222", "1120", "2010", "2350", "4200",
            "1180", "2380", "3020", "1010", "4030"
        ]


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────
async def main():
    """Async main function to run the bot."""
    reporter = MarketReporter()
    await reporter.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 MarketReporter stopped by user.")
    except Exception as e:
        logger.critical(f"💥 MarketReporter crashed: {e}", exc_info=True)
