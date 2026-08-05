"""Extend StrategyEvaluator indicator computation and condition evaluation.

Imported by bots.evaluator.bot at module load to monkey-patch methods.
"""
from __future__ import annotations

from typing import Any, Dict

import pandas as pd

from bots.evaluator import indicators_ext as ix


def compute_indicators_extended(self, df: pd.DataFrame, dna: Dict[str, Any]) -> pd.DataFrame:
    all_conditions = dna.get("entry_conditions", []) + dna.get("exit_conditions", [])

    for cond in all_conditions:
        indicator = cond.get("indicator", "")

        if indicator == "RSI":
            period = int(cond.get("period", 14))
            df[f"rsi_{period}"] = self._rsi(df["close"], period)

        elif indicator == "SMA_CROSS":
            fp = int(cond.get("fast_period", 10))
            sp = int(cond.get("slow_period", 50))
            df[f"sma_fast_{fp}"] = df["close"].rolling(fp).mean()
            df[f"sma_slow_{sp}"] = df["close"].rolling(sp).mean()

        elif indicator == "EMA_CROSS":
            fp = int(cond.get("fast_period", 9))
            sp = int(cond.get("slow_period", 21))
            df[f"ema_fast_{fp}"] = df["close"].ewm(span=fp, adjust=False).mean()
            df[f"ema_slow_{sp}"] = df["close"].ewm(span=sp, adjust=False).mean()

        elif indicator == "EMA_PRICE":
            period = int(cond.get("period", 50))
            df[f"ema_{period}"] = df["close"].ewm(span=period, adjust=False).mean()

        elif indicator == "SMA_PRICE":
            period = int(cond.get("period", 50))
            df[f"sma_{period}"] = df["close"].rolling(period).mean()

        elif indicator == "MACD":
            fp = int(cond.get("fast_period", 12))
            sp = int(cond.get("slow_period", 26))
            sig = int(cond.get("signal_period", 9))
            ema_fast = df["close"].ewm(span=fp, adjust=False).mean()
            ema_slow = df["close"].ewm(span=sp, adjust=False).mean()
            df["macd_line"] = ema_fast - ema_slow
            df["macd_signal"] = df["macd_line"].ewm(span=sig, adjust=False).mean()

        elif indicator == "BOLLINGER":
            period = int(cond.get("period", 20))
            std = float(cond.get("std", 2.0))
            ma = df["close"].rolling(period).mean()
            sigma = df["close"].rolling(period).std()
            df[f"bb_upper_{period}"] = ma + std * sigma
            df[f"bb_lower_{period}"] = ma - std * sigma

        elif indicator == "ATR":
            period = int(cond.get("period", 14))
            df[f"atr_{period}"] = self._atr(df, period)

        elif indicator == "ADX":
            period = int(cond.get("period", 14))
            df[f"adx_{period}"] = self._adx(df, period)

        elif indicator == "VOLUME_SURGE":
            ma_period = int(cond.get("ma_period", 20))
            df[f"vol_ma_{ma_period}"] = df["volume"].rolling(ma_period).mean()

        elif indicator in ("STOCH", "CCI"):
            period = int(cond.get("period", cond.get("fastk_period", 14)))
            df[f"stoch_k_{period}"] = self._stoch_k(df, period)

        elif indicator == "STOCH_RSI":
            period = int(cond.get("period", 14))
            df[f"stoch_rsi_{period}"] = ix.stoch_rsi(df["close"], period)

        elif indicator == "WILLR":
            period = int(cond.get("period", 14))
            df[f"willr_{period}"] = ix.willr(df, period)

        elif indicator == "MFI":
            period = int(cond.get("period", 14))
            df[f"mfi_{period}"] = ix.mfi(df, period)

        elif indicator == "PSAR":
            af = float(cond.get("af", 0.02))
            max_af = float(cond.get("max_af", 0.2))
            df["psar"] = ix.psar(df, af_start=af, af_max=max_af)

        elif indicator == "SUPERTREND":
            period = int(cond.get("period", 10))
            mult = float(cond.get("multiplier", 3.0))
            df[f"supertrend_{period}_{mult}"] = ix.supertrend(df, period, mult)

        elif indicator == "KELTNER":
            period = int(cond.get("period", 20))
            mult = float(cond.get("multiplier", 2.0))
            upper, lower = ix.keltner(df, period, mult)
            df[f"kelt_upper_{period}"] = upper
            df[f"kelt_lower_{period}"] = lower

        elif indicator == "DONCHIAN":
            period = int(cond.get("period", 20))
            upper, lower = ix.donchian(df, period)
            df[f"don_upper_{period}"] = upper
            df[f"don_lower_{period}"] = lower

        elif indicator == "OBV_SLOPE":
            period = int(cond.get("period", 10))
            df[f"obv_slope_{period}"] = ix.obv_slope(df, period)

        elif indicator == "CMF":
            period = int(cond.get("period", 20))
            df[f"cmf_{period}"] = ix.cmf(df, period)

    return df


def evaluate_condition_extended(self, df: pd.DataFrame, cond: Dict[str, Any], side: str) -> pd.Series:
    indicator = cond.get("indicator", "")
    operator = cond.get("operator", "")
    value = cond.get("value", 0)
    n = len(df)
    false_series = pd.Series([False] * n, index=df.index)

    try:
        # Prefer original method for legacy indicators when possible
        base = getattr(self, "_evaluate_condition_legacy", None)
        if base is not None and indicator in {
            "RSI", "SMA_CROSS", "EMA_CROSS", "EMA_PRICE", "MACD",
            "BOLLINGER", "ADX", "VOLUME_SURGE", "STOCH", "CCI",
        }:
            return base(df, cond, side)

        if indicator == "SMA_PRICE":
            period = int(cond.get("period", 50))
            col = f"sma_{period}"
            if col not in df.columns:
                return false_series
            return df["close"] > df[col] if operator == "price_above" else df["close"] < df[col]

        if indicator == "STOCH_RSI":
            period = int(cond.get("period", 14))
            col = f"stoch_rsi_{period}"
            if col not in df.columns:
                return false_series
            return df[col] < value if operator in ("<", "<=") else df[col] > value

        if indicator == "WILLR":
            period = int(cond.get("period", 14))
            col = f"willr_{period}"
            if col not in df.columns:
                return false_series
            return df[col] < value if operator in ("<", "<=") else df[col] > value

        if indicator == "MFI":
            period = int(cond.get("period", 14))
            col = f"mfi_{period}"
            if col not in df.columns:
                return false_series
            return df[col] < value if operator in ("<", "<=") else df[col] > value

        if indicator == "PSAR":
            if "psar" not in df.columns:
                return false_series
            if operator == "price_above_psar":
                return df["close"] > df["psar"]
            return df["close"] < df["psar"]

        if indicator == "SUPERTREND":
            period = int(cond.get("period", 10))
            mult = float(cond.get("multiplier", 3.0))
            col = f"supertrend_{period}_{mult}"
            if col not in df.columns:
                return false_series
            if operator == "price_above_st":
                return df["close"] > df[col]
            return df["close"] < df[col]

        if indicator == "KELTNER":
            period = int(cond.get("period", 20))
            if operator == "price_below_lower":
                col = f"kelt_lower_{period}"
                return df["close"] < df[col] if col in df.columns else false_series
            col = f"kelt_upper_{period}"
            return df["close"] > df[col] if col in df.columns else false_series

        if indicator == "DONCHIAN":
            period = int(cond.get("period", 20))
            if operator == "break_upper":
                col = f"don_upper_{period}"
                return df["close"] >= df[col] if col in df.columns else false_series
            col = f"don_lower_{period}"
            return df["close"] <= df[col] if col in df.columns else false_series

        if indicator == "OBV_SLOPE":
            period = int(cond.get("period", 10))
            col = f"obv_slope_{period}"
            if col not in df.columns:
                return false_series
            return df[col] > 0 if operator == "slope_up" else df[col] < 0

        if indicator == "CMF":
            period = int(cond.get("period", 20))
            col = f"cmf_{period}"
            if col not in df.columns:
                return false_series
            return df[col] > value if operator in (">", ">=") else df[col] < value

        if base is not None:
            return base(df, cond, side)

    except Exception:
        pass
    return false_series


def apply_evaluator_patches() -> None:
    from bots.evaluator.bot import StrategyEvaluator

    if not getattr(StrategyEvaluator, "_phase1_patched", False):
        StrategyEvaluator._evaluate_condition_legacy = StrategyEvaluator._evaluate_condition
        StrategyEvaluator._compute_indicators = compute_indicators_extended
        StrategyEvaluator._evaluate_condition = evaluate_condition_extended
        StrategyEvaluator._phase1_patched = True
