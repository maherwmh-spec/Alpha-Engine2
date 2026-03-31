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
from scripts.sahmk_client import SahmkClient, get_sahmk_client
from scripts.utils import get_saudi_time, is_trading_hours
from scripts.saudi_exchange_scraper import (
    get_all_sector_candles,
    is_sector_symbol,
    SECTOR_DISPLAY_NAMES,
)
from scripts.sector_calculator import compute_sector_candles_from_db

# --- Constants ---
DB_POOL: Optional[asyncpg.Pool] = None
FETCH_CONCURRENCY = 20  # Number of symbols to fetch in parallel


# قاموس أسماء القطاعات — مصدره الآن saudi_exchange_scraper (مصدر واحد للحقيقة)
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
        # asyncio.get_event_loop() في Python 3.10+ يُنشئ loop جديد في threads الفرعية
        # لذا يجب حفظه هنا بينما نحن في الـ main coroutine
        self._main_loop = asyncio.get_running_loop()

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

        للقطاعات (900xx): source = 'saudi_exchange_scraper'
        للأسهم العادية:   source = 'sahmk_websocket'
        """
        if DB_POOL is None:
            self.logger.warning("DB pool not available, skipping candle save.")
            return

        # Ensure timezone-aware timestamp
        ts = pd.to_datetime(candle['timestamp'])
        if ts.tzinfo is None:
            import pytz
            ts = pytz.timezone('Asia/Riyadh').localize(ts)

        # تحديد المصدر بناءً على نوع الرمز
        symbol = candle['symbol']
        source = candle.get('source') or (
            'saudi_exchange_scraper' if is_sector_symbol(symbol) else 'sahmk_websocket'
        )
        name = SECTOR_NAMES.get(symbol) or candle.get('name') or 'Unknown'

        sql = """
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
        try:
            async with DB_POOL.acquire() as conn:
                await conn.execute(
                    sql,
                    ts,                        # $1  time (timestamptz)
                    symbol,                    # $2  symbol
                    '1m',                      # $3  timeframe
                    name,                      # $4  name
                    float(candle['open']),     # $5  open
                    float(candle['high']),     # $6  high
                    float(candle['low']),      # $7  low
                    float(candle['close']),    # $8  close
                    int(float(candle['volume'])),  # $9  volume (bigint)
                    0,                         # $10 open_interest
                    source,                    # $11 source
                )
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

        sql_upsert = """
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
        try:
            async with DB_POOL.acquire() as conn:
                await conn.executemany(sql_upsert, records)
            self.logger.debug(
                f"💾 Saved {len(records)} historical candles [{symbol} {timeframe}]"
            )
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

        المشكلة القديمة:
          asyncio.get_event_loop() في Python 3.10+ يُنشئ loop جديد في threads الفرعية
          بدلاً من إرجاع الـ loop الرئيسي، فيكون is_running() == False دائماً.

        الإصلاح:
          نحفظ الـ loop الرئيسي في self._main_loop عند بدء run() ونستخدمه هنا.
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

        # ── DB (async → عبر run_coroutine_threadsafe) ────────────────────────────
        try:
            loop = getattr(self, '_main_loop', None)
            if loop is not None and loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    self._save_candle_to_db(candle), loop
                )
            else:
                # لم يُسجّل الـ loop بعد (run() لم يُستدع بعد)
                self.logger.warning(
                    f"⚠️ Candle for {candle['symbol']} received before main loop ready — skipped DB save"
                )
        except Exception as e:
            self.logger.error(f"❌ DB candle schedule error for {candle['symbol']}: {e}")

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

        القطاعات (900xx): تُستبعد من Sahmk ويُجلب snapshot واحد من Saudi Exchange
        للإطار 1m فقط (لا توجد بيانات تاريخية للقطاعات).
        """
        stock_symbols  = [s for s in symbols if not is_sector_symbol(s)]
        sector_symbols = [s for s in symbols if is_sector_symbol(s)]

        self.logger.info(
            f"📊 Fetching historical data | "
            f"{len(stock_symbols)} stocks via Sahmk + "
            f"{len(sector_symbols)} sectors via Saudi Exchange (1m only)"
        )

        # ── الأسهم العادية: Sahmk REST API ──────────────────────────────────
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

        # ── القطاعات: snapshot لحظي من Saudi Exchange (1m فقط) ──────────────
        if sector_symbols and timeframe == '1m':
            self.logger.info(
                f"📊 Fetching sector snapshots from Saudi Exchange for {len(sector_symbols)} symbols..."
            )
            try:
                all_candles = await asyncio.to_thread(get_all_sector_candles)
                saved = 0
                for sym in sector_symbols:
                    candle = all_candles.get(sym)
                    if candle:
                        await self._save_candle_to_db(candle)
                        saved += 1
                self.logger.success(
                    f"✅ Sector snapshots saved: {saved}/{len(sector_symbols)}"
                )
            except Exception as e:
                self.logger.error(f"❌ Sector historical fetch error: {e}")
        elif sector_symbols:
            self.logger.debug(
                f"⏭️  Skipping sector fetch for timeframe={timeframe} (only 1m supported)"
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
        """Applies a very light filter to exclude dead stocks, then returns all symbols."""
        # redis_manager.get is SYNC — no await
        cached = redis_manager.get('filtered_symbols:ready')
        if cached:
            return cached

        self.logger.info(f"🔍 Filtering {len(all_symbols)} symbols...")

        # Fetch all data in parallel from DB
        semaphore = asyncio.Semaphore(FETCH_CONCURRENCY)
        fetch_tasks = [
            self._fetch_ohlcv_for_filter_safe(sym, semaphore)
            for sym in all_symbols if sym not in SECTOR_NAMES # لا تحاول جلب بيانات تاريخية للقطاعات
        ]
        data_frames = await asyncio.gather(*fetch_tasks)

        # --- الفلترة الجديدة: فقط استبعاد الأسهم ذات حجم تداول صفر لمدة 10 أيام ---
        filter_tasks = [
            self._apply_light_filter(sym, df)
            for sym, df in zip(all_symbols, data_frames)
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
            f"✅ Filtering complete: {len(passing_symbols)}/{len(all_symbols)} passed."
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

        استراتيجية المصدر (بالأولوية):
          1. أساسي: حساب VWAP من بيانات الأسهم في DB (sector_calculator)
             → يعمل دائماً بدون اتصال خارجي
          2. احتياطي: Saudi Exchange scraper (إذا كان DB فارغاً)
             → قد يفشل بسبب Cloudflare من IP الخادم

        يعمل فقط خلال ساعات التداول (09:55 - 15:05، الأحد-الخميس).
        """
        self.logger.info("📊 Sector polling loop started (DB calculator + Saudi Exchange fallback)")
        while True:
            await asyncio.sleep(60)  # انتظر دقيقة

            # ── فحص وقت التداول ──────────────────────────────────────────────
            now = get_saudi_time()
            if now.weekday() in [4, 5]:  # الجمعة = 4، السبت = 5
                self.logger.debug("⏭️  Sector polling: market closed (weekend)")
                continue
            market_start = now.replace(hour=9,  minute=55, second=0, microsecond=0)
            market_end   = now.replace(hour=15, minute=5,  second=0, microsecond=0)
            if not (market_start <= now <= market_end):
                self.logger.debug(
                    f"⏭️  Sector polling: outside trading hours "
                    f"({now.strftime('%H:%M')} not in 09:55-15:05)"
                )
                continue

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

            # ── المصدر الاحتياطي: Saudi Exchange scraper ──────────────────
            if not all_candles:
                self.logger.info(
                    "🔄 DB calculator returned no data — trying Saudi Exchange scraper..."
                )
                try:
                    all_candles = await asyncio.to_thread(get_all_sector_candles)
                except Exception as e:
                    self.logger.error(f"❌ Saudi Exchange scraper error: {e}")
                    all_candles = {}

            if not all_candles:
                self.logger.warning(
                    "⚠️ Both sources failed — no sector data this cycle. "
                    "Check DB has recent 1m stock data."
                )
                continue

            # ── حفظ كل شمعة في DB ───────────────────────────────────────────
            try:
                save_tasks = [
                    self._save_candle_to_db(candle)
                    for candle in all_candles.values()
                ]
                await asyncio.gather(*save_tasks, return_exceptions=True)

                tasi_ok = '✅' if '90001' in all_candles else '❌'
                source  = all_candles[next(iter(all_candles))].get('source', 'unknown')
                self.logger.success(
                    f"✅ Sector data saved: {len(all_candles)} candles "
                    f"(TASI={tasi_ok}, source={source})"
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
