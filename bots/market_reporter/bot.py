#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Bot 2: Market Reporter - مُراسل السوق
======================================
مسؤول حصراً عن جمع البيانات اللحظية عبر WebSocket أثناء أوقات التداول.

المنطق الجديد (بعد الإصلاح الهيكلي):
  ┌─────────────────────────────────────────────────────────────────┐
  │  عند بدء التشغيل:                                               │
  │                                                                  │
  │  إذا كان السوق مفتوحاً (10:00 - 15:00 أيام الأحد-الخميس):      │
  │    → يتخطى المزامنة التاريخية تماماً                            │
  │    → يتصل بـ WebSocket فوراً ويشترك في جميع الأسهم النشطة       │
  │                                                                  │
  │  إذا كان السوق مغلقاً:                                          │
  │    → يعمل في وضع الانتظار (heartbeat)                           │
  │    → ينتظر حتى يفتح السوق ثم يبدأ الاتصال                      │
  │                                                                  │
  │  المزامنة التاريخية: مفصولة تماماً في scripts/historical_sync.py │
  └─────────────────────────────────────────────────────────────────┘

ملاحظات تقنية:
  - redis_manager.get/set هي SYNC — لا تستخدم await معها
  - Queue + consumer task مستقل لحفظ الشموع في DB
  - بيانات القطاعات تُحسب من DB عبر _sector_polling_loop
"""

import asyncio
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import pytz
import asyncpg
import numpy as np
import pandas as pd
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

# --- Timezone ---
_RIYADH_TZ = pytz.timezone('Asia/Riyadh')

# --- Constants ---
DB_POOL: Optional[asyncpg.Pool] = None
FETCH_CONCURRENCY = 20

SECTOR_NAMES: Dict[str, str] = SECTOR_DISPLAY_NAMES


class MarketReporter:
    """
    Real-time market data collector — WebSocket only during trading hours.
    المزامنة التاريخية مفصولة في scripts/historical_sync.py
    """

    def __init__(self):
        self.name = "market_reporter"
        self.logger = logger.bind(bot=self.name)
        self.bot_config = config.get_bot_config(self.name)
        self._load_config()

        self.sahmk: Optional[SahmkClient] = None
        self._realtime_active = False
        self._subscribed_syms: List[str] = []
        self._candles_saved = 0
        self._candle_queue: asyncio.Queue = None
        self._main_loop = None

        self.logger.info("✅ MarketReporter initialized (realtime-only mode)")

    def _load_config(self):
        """Load configurations from YAML."""
        lf = config.get("liquidity_filter", {})
        self.rv_threshold     = lf.get("relative_volume_threshold", 1.5)
        self.min_active_days  = lf.get("min_trading_days", 20)
        self.min_avg_change   = lf.get("min_avg_daily_change", 0.008)
        self.lf_lookback_days = lf.get("lookback_days", 30)

        vf = config.get("volatility_filter", {})
        self.max_gap_pct = vf.get("max_gap_threshold", 0.075)

    # ═══════════════════════════════════════════════════════════════════════
    # Main Entry & Lifecycle
    # ═══════════════════════════════════════════════════════════════════════

    async def run(self):
        """
        نقطة الدخول الرئيسية.

        المنطق:
          - إذا كان السوق مفتوحاً → ابدأ WebSocket فوراً
          - إذا كان السوق مغلقاً → انتظر حتى يفتح
          - لا مزامنة تاريخية هنا (استخدم scripts/historical_sync.py)
        """
        self.logger.info("🚀 MarketReporter starting (realtime-only)...")
        self._main_loop = asyncio.get_running_loop()
        self._candle_queue = asyncio.Queue(maxsize=10000)

        # ── تهيئة Sahmk Client ────────────────────────────────────────────
        try:
            self.sahmk = get_sahmk_client()
            self.sahmk.set_on_candle_complete(self._on_candle_complete)
            self.sahmk.set_on_tick(self._on_tick_received)
            self.logger.success("✅ Sahmk API client ready")
        except Exception as e:
            self.logger.critical(f"❌ Sahmk client init failed: {e}. Heartbeat-only mode.")
            await self._heartbeat_only_mode()
            return

        # ── تهيئة DB Pool ─────────────────────────────────────────────────
        await self._init_db_pool()

        # ── بدء consumer task مستقل لحفظ الشموع في DB ────────────────────
        asyncio.create_task(self._candle_db_consumer())
        self.logger.success("✅ Candle DB consumer task started")

        # ── الحلقة الرئيسية ───────────────────────────────────────────────
        while True:
            try:
                now_saudi = get_saudi_time()
                trading_open = is_trading_hours()

                if trading_open:
                    # ── وضع التداول: WebSocket فوري بدون مزامنة تاريخية ──
                    self.logger.info(
                        f"📈 Market is OPEN ({now_saudi.strftime('%H:%M')} AST) "
                        f"— starting WebSocket stream immediately"
                    )
                    await self._run_realtime_session()
                else:
                    # ── السوق مغلق: انتظر حتى يفتح ──────────────────────
                    wait_secs = self._seconds_until_market_open()
                    self.logger.info(
                        f"💤 Market is CLOSED ({now_saudi.strftime('%H:%M')} AST) "
                        f"— waiting {wait_secs // 60:.0f} min until open. "
                        f"Run 'python scripts/historical_sync.py' for historical data."
                    )
                    # إيقاف الـ stream إذا كان جارياً
                    if self._realtime_active:
                        self.sahmk.stop_realtime_stream()
                        self._realtime_active = False
                        self.logger.info("📴 WebSocket stream stopped (market closed)")

                    # انتظر بفترات قصيرة مع إعادة الفحص كل دقيقة
                    await asyncio.sleep(min(wait_secs, 60))

            except Exception as e:
                self.logger.error(f"❌ Main loop error: {e}", exc_info=True)
                await asyncio.sleep(30)

    async def _run_realtime_session(self):
        """
        تشغيل جلسة البيانات اللحظية حتى إغلاق السوق.
        يبدأ WebSocket فوراً ويظل يعمل حتى 15:00.
        """
        # ── جلب قائمة الأسهم النشطة ──────────────────────────────────────
        all_symbols = self.sahmk.get_symbols_list() or self._default_symbols()
        self.logger.info(f"📋 Active symbols: {len(all_symbols)}")

        # ── بدء WebSocket فوراً ───────────────────────────────────────────
        await self.start_realtime_stream(all_symbols)

        # ── انتظر حتى يُغلق السوق مع فحص دوري كل دقيقة ─────────────────
        self.logger.success(
            f"✅ Streaming {len(all_symbols)} symbols. "
            f"Will stop at 15:00 AST."
        )
        while is_trading_hours():
            await asyncio.sleep(60)
            # تحديث قائمة الأسهم كل ساعة إذا تغيرت
            if not hasattr(self, '_last_symbol_refresh'):
                self._last_symbol_refresh = get_saudi_time()
            elif (get_saudi_time() - self._last_symbol_refresh).seconds >= 3600:
                new_symbols = self.sahmk.get_symbols_list() or all_symbols
                if sorted(new_symbols) != sorted(self._subscribed_syms):
                    self.logger.info("🔄 Symbol list changed — restarting stream")
                    await self.start_realtime_stream(new_symbols)
                self._last_symbol_refresh = get_saudi_time()

        self.logger.info("🔔 Market closed (15:00 AST) — stopping WebSocket stream")
        if self._realtime_active:
            self.sahmk.stop_realtime_stream()
            self._realtime_active = False

    def _seconds_until_market_open(self) -> int:
        """احسب الثواني حتى فتح السوق القادم."""
        now = get_saudi_time()
        # يوم التداول القادم: الأحد=0 ... الخميس=4
        # الجمعة=4 (weekday)، السبت=5 في Python
        # السوق السعودي: الأحد-الخميس (weekday 6,0,1,2,3 في Python)
        # Python: Monday=0, ..., Sunday=6
        # تاسي: Sunday=6, Monday=0, Tuesday=1, Wednesday=2, Thursday=3
        trading_days_python = {6, 0, 1, 2, 3}  # Sun-Thu

        # حاول اليوم نفسه أولاً (إذا كان قبل 10:00)
        today_open = now.replace(hour=10, minute=0, second=0, microsecond=0)
        if now.weekday() in trading_days_python and now < today_open:
            return max(1, int((today_open - now).total_seconds()))

        # ابحث عن اليوم التالي
        candidate = now + timedelta(days=1)
        for _ in range(7):
            candidate = candidate.replace(hour=10, minute=0, second=0, microsecond=0)
            if candidate.weekday() in trading_days_python:
                return max(1, int((candidate - now).total_seconds()))
            candidate += timedelta(days=1)

        return 3600  # fallback: ساعة واحدة

    async def _heartbeat_only_mode(self):
        """وضع الطوارئ عند فشل تهيئة الخدمات الأساسية."""
        tick = 0
        while True:
            await asyncio.sleep(60)
            tick += 1
            self.logger.info(f"💓 MarketReporter heartbeat (degraded) | tick={tick}")

    # ═══════════════════════════════════════════════════════════════════════
    # Database Operations
    # ═══════════════════════════════════════════════════════════════════════

    async def _init_db_pool(self):
        """تهيئة asyncpg connection pool."""
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
        """Save a 1m candle to TimescaleDB with Asia/Riyadh timestamps."""
        if DB_POOL is None:
            return

        symbol = candle.get('symbol', '')
        if not is_tasi_or_sector(symbol):
            return

        # --- Timezone normalisation: always store as Asia/Riyadh ---
        ts = pd.to_datetime(candle['timestamp'])
        if ts.tzinfo is None:
            # Naive timestamp: assume it is already local Riyadh time
            # (WebSocket delivers wall-clock Riyadh time without tz info)
            ts = _RIYADH_TZ.localize(ts)
        else:
            # Aware timestamp (e.g. UTC from some sources): convert to Riyadh
            ts = ts.tz_convert(_RIYADH_TZ)

        source = candle.get('source') or (
            'db_sector_calculator' if is_sector_symbol(symbol) else 'sahmk_websocket'
        )
        name = SECTOR_NAMES.get(symbol) or candle.get('name') or 'Unknown'

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
        args = (
            ts, symbol, '1m', name,
            float(candle['open']), float(candle['high']),
            float(candle['low']), float(candle['close']),
            int(candle.get('volume', 0)), 0,
        )
        try:
            async with DB_POOL.acquire() as conn:
                try:
                    await conn.execute(sql_with, *args, source)
                except Exception as e:
                    if 'source' in str(e).lower() or 'column' in str(e).lower():
                        await conn.execute(sql_without, *args)
                    else:
                        raise
        except Exception as e:
            self.logger.error(f"❌ DB save error [{symbol}]: {e}")

    async def _fetch_ohlcv_for_filter(self, symbol: str) -> Optional[pd.DataFrame]:
        """جلب بيانات يومية من DB للفلترة."""
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
                    sql, symbol,
                    get_saudi_time() - timedelta(days=self.lf_lookback_days + 5)
                )
            if not records:
                return None
            return pd.DataFrame(records, columns=['time', 'close', 'volume'])
        except Exception as e:
            self.logger.error(f"❌ Filter data fetch error [{symbol}]: {e}")
            return None

    # ═══════════════════════════════════════════════════════════════════════
    # Real-time Stream & Callbacks
    # ═══════════════════════════════════════════════════════════════════════

    async def start_realtime_stream(self, symbols: List[str]):
        """بدء أو تحديث WebSocket stream."""
        if not self.sahmk:
            self.logger.error("❌ Cannot start stream: Sahmk client not available.")
            return

        if self._realtime_active and sorted(self._subscribed_syms) == sorted(symbols):
            self.logger.info("👍 Symbol list unchanged, no stream restart needed.")
            return

        if self._realtime_active:
            self.logger.info("🔄 Restarting stream with updated symbol list...")
            self.sahmk.stop_realtime_stream()
            await asyncio.sleep(5)

        self._subscribed_syms = symbols
        self._realtime_active = True

        self.logger.info(f"📡 Starting WebSocket stream for {len(symbols)} symbols.")
        self.sahmk.start_realtime_stream(symbols)
        self.logger.success("✅ Real-time stream active.")

        # بدء حلقة بيانات القطاعات
        if not hasattr(self, '_sector_fetch_task') or self._sector_fetch_task.done():
            self._sector_fetch_task = asyncio.create_task(self._sector_polling_loop())

    def _on_tick_received(self, tick: Dict):
        """Callback من WebSocket thread — sync only."""
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
        Callback من WebSocket thread.
        يضع الشمعة في Queue thread-safe بدلاً من run_coroutine_threadsafe.
        """
        try:
            redis_manager.set(
                f"ohlcv:1m:{candle['symbol']}:latest",
                {k: str(v) if isinstance(v, datetime) else v
                 for k, v in candle.items()},
                ttl=120
            )
            self._candles_saved += 1
            if self._candles_saved % 100 == 0:
                self.logger.info(f"📈 {self._candles_saved} candles processed.")
        except Exception as e:
            self.logger.error(f"❌ Redis candle save error: {e}")

        loop  = getattr(self, '_main_loop', None)
        queue = getattr(self, '_candle_queue', None)
        if loop is not None and queue is not None and loop.is_running():
            try:
                loop.call_soon_threadsafe(queue.put_nowait, candle)
            except asyncio.QueueFull:
                self.logger.warning(f"⚠️ Queue full — dropping candle [{candle['symbol']}]")
            except Exception as e:
                self.logger.error(f"❌ Queue put error [{candle['symbol']}]: {e}")

    async def _candle_db_consumer(self):
        """Consumer مستقل يقرأ من Queue ويحفظ في DB."""
        self.logger.info("🗄️  Candle DB consumer started...")
        batch: List[Dict] = []
        BATCH_SIZE     = 50
        FLUSH_INTERVAL = 5.0
        last_flush = asyncio.get_running_loop().time()

        while True:
            try:
                now     = asyncio.get_running_loop().time()
                timeout = max(0.1, FLUSH_INTERVAL - (now - last_flush))
                try:
                    candle = await asyncio.wait_for(
                        self._candle_queue.get(), timeout=timeout
                    )
                    batch.append(candle)
                    self._candle_queue.task_done()
                except asyncio.TimeoutError:
                    pass

                now = asyncio.get_running_loop().time()
                if batch and (len(batch) >= BATCH_SIZE or (now - last_flush) >= FLUSH_INTERVAL):
                    saved = 0
                    for c in batch:
                        try:
                            await self._save_candle_to_db(c)
                            saved += 1
                        except Exception as e:
                            self.logger.error(f"❌ Consumer save error [{c.get('symbol')}]: {e}")
                    self.logger.info(
                        f"🗄️  Flushed {saved}/{len(batch)} candles "
                        f"(queue: {self._candle_queue.qsize()})"
                    )
                    batch.clear()
                    last_flush = now
            except Exception as e:
                self.logger.error(f"❌ Candle consumer error: {e}", exc_info=True)
                await asyncio.sleep(1)

    # ═══════════════════════════════════════════════════════════════════════
    # Symbol Filtering (light — from DB only)
    # ═══════════════════════════════════════════════════════════════════════

    async def get_filtered_symbols_for_analysis(
        self, all_symbols: List[str]
    ) -> List[str]:
        """فلتر خفيف: استبعاد الأسهم الميتة (حجم صفر لـ 30 يوماً)."""
        cached = redis_manager.get('filtered_symbols:ready')
        if cached:
            return cached

        excluded_9xx = [s for s in all_symbols if s.startswith('9') and not s.startswith('900')]
        tasi_only    = [s for s in all_symbols if is_tasi_or_sector(s)]

        if excluded_9xx:
            self.logger.info(f"🚫 Excluded {len(excluded_9xx)} Nomu/ETF symbols")

        semaphore    = asyncio.Semaphore(FETCH_CONCURRENCY)
        stock_syms   = [s for s in tasi_only if s not in SECTOR_NAMES]
        fetch_tasks  = [self._fetch_ohlcv_for_filter_safe(s, semaphore) for s in stock_syms]
        data_frames  = await asyncio.gather(*fetch_tasks)

        filter_tasks = [
            self._apply_light_filter(sym, df)
            for sym, df in zip(stock_syms, data_frames)
            if df is not None and not df.empty
        ]
        filter_results  = await asyncio.gather(*filter_tasks)
        passing_symbols = [sym for sym, passed in filter_results if passed]

        for sector_sym in SECTOR_NAMES.keys():
            if sector_sym not in passing_symbols:
                passing_symbols.append(sector_sym)

        self.logger.success(f"✅ Filtering complete: {len(passing_symbols)} symbols")
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
        """استبعاد الأسهم ذات حجم تداول صفر لآخر 30 يوماً."""
        try:
            if len(df) >= 30 and (df["volume"].tail(30) == 0).all():
                return symbol, False
            return symbol, True
        except Exception:
            return symbol, False

    # ═══════════════════════════════════════════════════════════════════════
    # Sector Polling
    # ═══════════════════════════════════════════════════════════════════════

    async def _sector_polling_loop(self):
        """حساب بيانات القطاعات من DB كل دقيقة."""
        self.logger.info("📊 Sector polling loop started")
        while True:
            await asyncio.sleep(60)
            try:
                if DB_POOL is None:
                    continue
                async with DB_POOL.acquire() as conn:
                    all_candles = await compute_sector_candles_from_db(conn)
                if not all_candles:
                    continue
                async with DB_POOL.acquire() as save_conn:
                    saved = await save_sector_candles_to_db(save_conn, all_candles)
                    if '90001' in all_candles:
                        await save_index_to_db(save_conn, all_candles['90001'])
                self.logger.success(
                    f"✅ Sector data: {saved} rows saved "
                    f"(TASI={'✅' if '90001' in all_candles else '❌'})"
                )
            except Exception as e:
                self.logger.error(f"❌ Sector polling error: {e}", exc_info=True)

    # ═══════════════════════════════════════════════════════════════════════
    # Utilities
    # ═══════════════════════════════════════════════════════════════════════

    def _default_symbols(self) -> List[str]:
        """قائمة احتياطية من أسهم تاسي."""
        return [
            "2222", "1120", "2010", "2350", "4200",
            "1180", "2380", "3020", "1010", "4030"
        ]


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────
async def main():
    reporter = MarketReporter()
    await reporter.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 MarketReporter stopped by user.")
    except Exception as e:
        logger.critical(f"💥 MarketReporter crashed: {e}", exc_info=True)
