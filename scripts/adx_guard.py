"""Safe ADX wrapper — skip short series instead of raising IndexError."""
from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd
import ta


def min_adx_bars(period: int = 14) -> int:
    return max(30, int(period) * 2 + 2)


def safe_adx(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    period: int = 14,
    logger: Optional[object] = None,
) -> pd.Series:
    n = len(high)
    idx = getattr(high, "index", None)
    empty = pd.Series([np.nan] * n, index=idx)
    need = min_adx_bars(period)
    if n < need:
        if logger is not None:
            logger.debug(f"ADX skipped: bars={n} need>={need} period={period}")
        return empty
    try:
        out = ta.trend.ADXIndicator(high, low, close, window=int(period)).adx()
        if out is None or len(out) == 0:
            return empty
        return out
    except (IndexError, ValueError, KeyError) as exc:
        if logger is not None:
            logger.debug(f"ADX skipped ({type(exc).__name__}): {exc}")
        return empty
    except Exception as exc:
        if logger is not None:
            logger.debug(f"ADX skipped: {exc}")
        return empty


def install_adx_guard(cls) -> None:
    def calculate_adx(self, high, low, close, period: int = 14):
        return safe_adx(high, low, close, period=period, logger=getattr(self, "logger", None))

    cls.calculate_adx = calculate_adx
