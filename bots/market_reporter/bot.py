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
from scripts.utils import get_saudi_time

# --- Constants ---
DB_POOL: Optional[asyncpg.Pool] = None
FETCH_CONCURRENCY = 20  # Number of symbols to fetch in parallel


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
                await self.fetch_all_symbols_historical(
                    all_symbols, timeframe='1d', days=self.lf_lookback_days + 5
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
        """
        if DB_POOL is None:
            self.logger.warning("DB pool not available, skipping candle save.")
            return

        # Ensure timezone-aware timestamp
        ts = pd.to_datetime(candle['timestamp'])
        if ts.tzinfo is None:
            import pytz
            ts = pytz.timezone('Asia/Riyadh').localize(ts)

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
                    ts,                                  # $1  time (timestamptz)
                    candle['symbol'],                    # $2  symbol
                    '1m',                                # $3  timeframe
                    candle.get('name', 'Unknown'),       # $4  name (nullable)
                    float(candle['open']),               # $5  open
                    float(candle['high']),               # $6  high
                    float(candle['low']),                # $7  low
                    float(candle['close']),              # $8  close
                    int(float(candle['volume'])),        # $9  volume (bigint)
                    0,                                   # $10 open_interest
                    'sahmk_websocket'                    # $11 source
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
        df_copy['name']          = df_copy['name'].fillna('Unknown') \
                                   if 'name' in df_copy.columns else 'Unknown'
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
        Callback from WebSocket thread.
        DB save is async → offload to event loop.
        Redis set is SYNC → call directly.
        """
        # Save to Redis synchronously (no await needed)
        try:
            redis_manager.set(
                f"ohlcv:1m:{candle['symbol']}:latest",
                {k: str(v) if isinstance(v, datetime) else v
                 for k, v in candle.items()},
                ttl=120
            )
            self._candles_saved += 1
            if self._candles_saved % 500 == 0:
                self.logger.info(f"📈 {self._candles_saved} candles saved to DB.")
        except Exception as e:
            self.logger.error(f"❌ Redis candle save error: {e}")

        # Save to DB asynchronously via event loop
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    self._save_candle_to_db(candle), loop
                )
        except RuntimeError:
            pass  # No running event loop — skip async DB save

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
        """Fetch historical data for all symbols concurrently."""
        self.logger.info(
            f"📊 Fetching historical data for {len(symbols)} symbols..."
        )
        semaphore = asyncio.Semaphore(FETCH_CONCURRENCY)
        tasks = [
            self._fetch_and_log(sym, timeframe, days, semaphore)
            for sym in symbols
        ]
        results = await asyncio.gather(*tasks)
        ok = sum(1 for r in results if r is not None)
        self.logger.success(
            f"✅ Historical fetch complete | ✅{ok} ❌{len(symbols) - ok}"
        )

    async def _fetch_and_log(
        self, symbol: str, timeframe: str, days: int, semaphore: asyncio.Semaphore
    ) -> Optional[pd.DataFrame]:
        async with semaphore:
            return await self.fetch_historical_data(symbol, timeframe, days)

    # ═══════════════════════════════════════════════════════════════════════
    # Symbol Filtering (Async & Parallel)
    # ═══════════════════════════════════════════════════════════════════════

    async def get_filtered_symbols_for_analysis(
        self, all_symbols: List[str]
    ) -> List[str]:
        """Return symbols that pass ALL filters, run concurrently."""
        # redis_manager.get is SYNC — no await
        cached = redis_manager.get('filtered_symbols:ready')
        if cached:
            return cached

        self.logger.info(f"🔍 Filtering {len(all_symbols)} symbols...")

        # Fetch all data in parallel from DB
        semaphore = asyncio.Semaphore(FETCH_CONCURRENCY)
        fetch_tasks = [
            self._fetch_ohlcv_for_filter_safe(sym, semaphore)
            for sym in all_symbols
        ]
        data_frames = await asyncio.gather(*fetch_tasks)

        # Apply filters in parallel
        filter_tasks = [
            self._apply_filters_to_symbol(sym, df)
            for sym, df in zip(all_symbols, data_frames)
            if df is not None and not df.empty
        ]
        filter_results = await asyncio.gather(*filter_tasks)

        passing_symbols = [sym for sym, passed in filter_results if passed]

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

    async def _apply_filters_to_symbol(
        self, symbol: str, df: pd.DataFrame
    ) -> Tuple[str, bool]:
        """Applies liquidity + volatility filters to one symbol's data."""
        try:
            avg_vol     = df['volume'].mean()
            today_vol   = df['volume'].iloc[-1]
            active_days = int((df['volume'] > 0).sum())
            rel_vol     = today_vol / avg_vol if avg_vol > 0 else 0

            df = df.copy()
            df['chg'] = df['close'].pct_change().abs()
            avg_chg   = df['chg'].mean()

            # Liquidity check
            if not (
                rel_vol     >= self.rv_threshold and
                active_days >= self.min_active_days and
                avg_chg     >= self.min_avg_change
            ):
                return symbol, False

            # Volatility check
            if len(df) >= 2:
                yesterday_chg = abs(
                    (df['close'].iloc[-1] - df['close'].iloc[-2]) /
                    df['close'].iloc[-2]
                )
                if yesterday_chg > self.max_gap_pct:
                    return symbol, False

            return symbol, True
        except Exception as e:
            self.logger.error(f"❌ Filter error for {symbol}: {e}")
            return symbol, False

    # ═══════════════════════════════════════════════════════════════════════
    # Utilities
    # ═══════════════════════════════════════════════════════════════════════

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
