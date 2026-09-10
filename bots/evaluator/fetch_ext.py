"""Fetch higher timeframes from Timescale views or resample 1m OHLCV."""
from __future__ import annotations

from typing import Optional

import pandas as pd

_CA = {
    "5m": "market_data.ohlcv_5m",
    "15m": "market_data.ohlcv_15m",
    "30m": "market_data.ohlcv_30m",
    "1h": "market_data.ohlcv_1h",
    "1d": "market_data.ohlcv_1d",
}
_RULE = {"5m": "5min", "15m": "15min", "30m": "30min", "1h": "1h", "1d": "1D"}
_1M_MULT = {"5m": 8, "15m": 20, "30m": 35, "1h": 70, "1d": 400}


def _to_df(rows) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=["time", "open", "high", "low", "close", "volume"])
    if df.empty:
        return df
    df = df.sort_values("time").reset_index(drop=True)
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)
    return df


def _resample_1m(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    rule = _RULE.get(timeframe)
    if not rule or df.empty:
        return df
    x = df.set_index(pd.to_datetime(df["time"]))
    out = x.resample(rule).agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
    ).dropna(subset=["open", "close"])
    out = out.reset_index().rename(columns={"index": "time", "time": "time"})
    if "time" not in out.columns:
        out = out.rename(columns={out.columns[0]: "time"})
    return out


def patch_fetch() -> None:
    from bots.evaluator.bot import StrategyEvaluator

    if getattr(StrategyEvaluator, "_fetch_tf_patched", False):
        return
    orig = StrategyEvaluator._fetch_candles

    async def _fetch_candles(self, symbol: str, timeframe: str, limit: int):
        df = await orig(self, symbol, timeframe, limit)
        if df is not None and len(df) >= 50:
            return df
        if timeframe == "1m" or self.db_pool is None:
            return df
        try:
            async with self.db_pool.acquire() as conn:
                view = _CA.get(timeframe)
                if view:
                    rows = await conn.fetch(
                        f"""
                        SELECT time, open, high, low, close, volume
                        FROM {view}
                        WHERE symbol = $1
                        ORDER BY time DESC
                        LIMIT $2
                        """,
                        symbol,
                        int(limit),
                    )
                    cadf = _to_df(rows)
                    if len(cadf) >= 50:
                        self._last_fetch_status = "ok"
                        self._last_fetch_reason = f"continuous_aggregate:{view}"
                        self._last_fetch_candles_count = len(cadf)
                        return cadf
                need = min(int(limit) * int(_1M_MULT.get(timeframe, 10)), 20000)
                rows = await conn.fetch(
                    """
                    SELECT time, open, high, low, close, volume
                    FROM market_data.ohlcv
                    WHERE symbol = $1 AND timeframe = '1m'
                    ORDER BY time DESC
                    LIMIT $2
                    """,
                    symbol,
                    need,
                )
            src = _to_df(rows)
            if src.empty:
                return df
            out = _resample_1m(src, timeframe)
            if len(out) >= 50:
                self._last_fetch_status = "ok"
                self._last_fetch_reason = f"resampled_1m:{timeframe}"
                self._last_fetch_candles_count = len(out)
                return out.tail(int(limit)).reset_index(drop=True)
            return out if not out.empty else df
        except Exception as exc:
            self._last_fetch_reason = f"fetch_ext: {type(exc).__name__}: {exc}"
            return df

    StrategyEvaluator._fetch_candles = _fetch_candles
    StrategyEvaluator._fetch_tf_patched = True
