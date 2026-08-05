"""Pure pandas helpers for expanded genetic indicators (no TA-Lib required)."""
from __future__ import annotations

import numpy as np
import pandas as pd


def willr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high = df["high"].rolling(period).max()
    low = df["low"].rolling(period).min()
    denom = (high - low).replace(0, np.nan)
    return (high - df["close"]) / denom * -100


def mfi(df: pd.DataFrame, period: int = 14) -> pd.Series:
    tp = (df["high"] + df["low"] + df["close"]) / 3.0
    mf = tp * df["volume"]
    delta = tp.diff()
    pos = mf.where(delta > 0, 0.0).rolling(period).sum()
    neg = mf.where(delta < 0, 0.0).rolling(period).sum().abs()
    ratio = pos / neg.replace(0, np.nan)
    return 100 - (100 / (1 + ratio))


def stoch_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0.0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    rmin = rsi.rolling(period).min()
    rmax = rsi.rolling(period).max()
    return (rsi - rmin) / (rmax - rmin).replace(0, np.nan)


def psar(df: pd.DataFrame, af_start: float = 0.02, af_max: float = 0.2) -> pd.Series:
    """Simplified parabolic SAR."""
    n = len(df)
    out = np.zeros(n)
    if n == 0:
        return pd.Series(out, index=df.index)
    bull = True
    af = af_start
    ep = float(df["high"].iloc[0])
    sar = float(df["low"].iloc[0])
    out[0] = sar
    for i in range(1, n):
        high = float(df["high"].iloc[i])
        low = float(df["low"].iloc[i])
        prev_sar = sar
        sar = prev_sar + af * (ep - prev_sar)
        if bull:
            sar = min(sar, float(df["low"].iloc[i - 1]))
            if i >= 2:
                sar = min(sar, float(df["low"].iloc[i - 2]))
            if low < sar:
                bull = False
                sar = ep
                ep = low
                af = af_start
            else:
                if high > ep:
                    ep = high
                    af = min(af_max, af + af_start)
        else:
            sar = max(sar, float(df["high"].iloc[i - 1]))
            if i >= 2:
                sar = max(sar, float(df["high"].iloc[i - 2]))
            if high > sar:
                bull = True
                sar = ep
                ep = high
                af = af_start
            else:
                if low < ep:
                    ep = low
                    af = min(af_max, af + af_start)
        out[i] = sar
    return pd.Series(out, index=df.index)


def supertrend(df: pd.DataFrame, period: int = 10, multiplier: float = 3.0) -> pd.Series:
    hl2 = (df["high"] + df["low"]) / 2.0
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - df["close"].shift(1)).abs(),
            (df["low"] - df["close"].shift(1)).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = tr.rolling(period).mean()
    upper = hl2 + multiplier * atr
    lower = hl2 - multiplier * atr
    st = pd.Series(index=df.index, dtype=float)
    direction = pd.Series(True, index=df.index)
    for i in range(len(df)):
        if i == 0 or pd.isna(atr.iloc[i]):
            st.iloc[i] = lower.iloc[i] if not pd.isna(lower.iloc[i]) else df["close"].iloc[i]
            continue
        if df["close"].iloc[i] > upper.iloc[i - 1]:
            direction.iloc[i] = True
        elif df["close"].iloc[i] < lower.iloc[i - 1]:
            direction.iloc[i] = False
        else:
            direction.iloc[i] = direction.iloc[i - 1]
            if direction.iloc[i]:
                lower.iloc[i] = max(lower.iloc[i], lower.iloc[i - 1])
            else:
                upper.iloc[i] = min(upper.iloc[i], upper.iloc[i - 1])
        st.iloc[i] = lower.iloc[i] if direction.iloc[i] else upper.iloc[i]
    return st


def keltner(df: pd.DataFrame, period: int = 20, multiplier: float = 2.0):
    mid = df["close"].ewm(span=period, adjust=False).mean()
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - df["close"].shift(1)).abs(),
            (df["low"] - df["close"].shift(1)).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = tr.rolling(period).mean()
    return mid + multiplier * atr, mid - multiplier * atr


def donchian(df: pd.DataFrame, period: int = 20):
    return df["high"].rolling(period).max(), df["low"].rolling(period).min()


def obv_slope(df: pd.DataFrame, period: int = 10) -> pd.Series:
    direction = np.sign(df["close"].diff()).fillna(0.0)
    obv = (direction * df["volume"]).cumsum()
    return obv.diff(period)


def cmf(df: pd.DataFrame, period: int = 20) -> pd.Series:
    hl = (df["high"] - df["low"]).replace(0, np.nan)
    mfm = ((df["close"] - df["low"]) - (df["high"] - df["close"])) / hl
    mfv = mfm * df["volume"]
    return mfv.rolling(period).sum() / df["volume"].rolling(period).sum().replace(0, np.nan)
