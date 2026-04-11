"""
bots/evaluator/bot.py
═══════════════════════════════════════════════════════════════
Alpha-Engine2 — مقيّم الاستراتيجيات الجينية

الوظيفة:
  1. يأخذ "شيفرة جينية" JSON + سهم + هدف ربحي
  2. يُترجم الشيفرة إلى مؤشرات فنية حقيقية على بيانات السهم
  3. يُشغّل backtest بسيط على بيانات DB
  4. يحسب Fitness Score بناءً على معادلة الهدف الربحي
  5. يُعيد نتائج مفصّلة تُخزّن في genetic.performance
═══════════════════════════════════════════════════════════════
"""
from __future__ import annotations

import asyncio
import math
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from loguru import logger as _loguru_logger
def get_logger(name): return _loguru_logger.bind(bot=name)

# ─────────────────────────────────────────────────────────────
# معادلات التقييم (Fitness Formulas) — الأهداف الأربعة
# ─────────────────────────────────────────────────────────────
FITNESS_FORMULAS: Dict[str, str] = {
    "scalping":
        "Score = (win_rate × 0.50) + (avg_profit_pct × 0.30) + (profit_factor × 0.10) "
        "- (max_drawdown_pct × 0.10)",

    "short_swings":
        "Score = (total_profit_pct × 0.40) + (win_rate × 0.30) "
        "+ (sharpe_ratio × 0.20) - (max_drawdown_pct × 0.10)",

    "medium_trends":
        "Score = (total_profit_pct × 0.50) + (sharpe_ratio × 0.30) "
        "- (max_drawdown_pct × 0.20)",

    "momentum":
        "Score = (total_profit_pct × 0.60) + (profit_factor × 0.20) "
        "- (max_drawdown_pct × 0.20)",
}

# الحد الأدنى للصفقات لاعتبار النتيجة صالحة
MIN_TRADES = 5


# ═══════════════════════════════════════════════════════════════
class StrategyEvaluator:
    """
    مقيّم الاستراتيجيات الجينية.
    يعمل بالكامل على بيانات DB دون الحاجة لـ freqtrade runtime.
    """

    def __init__(self, db_pool=None):
        self.logger = get_logger("StrategyEvaluator")
        self.db_pool = db_pool   # asyncpg pool (يُمرَّر من الخارج)

    # ─────────────────────────────────────────────────────────
    # الدالة الرئيسية: تقييم شيفرة جينية كاملة
    # ─────────────────────────────────────────────────────────
    async def evaluate(
        self,
        dna: Dict[str, Any],
        candles_limit: int = 2000,
    ) -> Dict[str, Any]:
        """
        يُقيّم شيفرة جينية على بيانات السهم المحددة.

        Returns:
            {
              "strategy_hash":    str,
              "symbol":           str,
              "profit_objective": str,
              "fitness_score":    float,
              "fitness_formula":  str,
              "total_profit_pct": float,
              "win_rate":         float,
              "total_trades":     int,
              "avg_profit_pct":   float,
              "max_drawdown_pct": float,
              "sharpe_ratio":     float,
              "profit_factor":    float,
              "avg_duration_min": int,
              "candles_count":    int,
              "status":           "ok" | "insufficient_data" | "no_trades" | "error",
            }
        """
        symbol          = dna.get("symbol", "")
        profit_objective = dna.get("profit_objective", "scalping")
        strategy_hash   = dna.get("hash", "")
        timeframe       = dna.get("timeframe", "1m")

        result_base = {
            "strategy_hash":    strategy_hash,
            "symbol":           symbol,
            "profit_objective": profit_objective,
            "fitness_score":    0.0,
            "fitness_formula":  FITNESS_FORMULAS.get(profit_objective, ""),
            "total_profit_pct": 0.0,
            "win_rate":         0.0,
            "total_trades":     0,
            "avg_profit_pct":   0.0,
            "max_drawdown_pct": 0.0,
            "sharpe_ratio":     0.0,
            "profit_factor":    0.0,
            "avg_duration_min": 0,
            "candles_count":    0,
            "status":           "error",
        }

        try:
            # ── 1. جلب البيانات من DB ──
            df = await self._fetch_candles(symbol, timeframe, candles_limit)
            if df is None or len(df) < 50:
                result_base["status"] = "insufficient_data"
                self.logger.warning(
                    f"⚠️  Insufficient data for {symbol} [{timeframe}]: "
                    f"{len(df) if df is not None else 0} candles"
                )
                return result_base

            result_base["candles_count"] = len(df)

            # ── 2. حساب المؤشرات الفنية ──
            df = self._compute_indicators(df, dna)

            # ── 3. توليد إشارات الدخول والخروج ──
            df = self._generate_signals(df, dna)

            # ── 4. محاكاة الصفقات ──
            trades = self._simulate_trades(df, dna)

            if len(trades) < MIN_TRADES:
                result_base["status"] = "no_trades"
                result_base["total_trades"] = len(trades)
                self.logger.info(
                    f"📉 No sufficient trades for {symbol}: {len(trades)} < {MIN_TRADES}"
                )
                return result_base

            # ── 5. حساب مقاييس الأداء ──
            metrics = self._compute_metrics(trades)
            result_base.update(metrics)

            # ── 6. حساب Fitness Score ──
            fitness = self._compute_fitness(metrics, profit_objective)
            result_base["fitness_score"] = fitness
            result_base["status"] = "ok"

            self.logger.info(
                f"✅ Evaluated {symbol} [{profit_objective}]: "
                f"fitness={fitness:.4f}, trades={metrics['total_trades']}, "
                f"win_rate={metrics['win_rate']:.1%}, "
                f"profit={metrics['total_profit_pct']:.2f}%"
            )
            return result_base

        except Exception as e:
            self.logger.error(
                f"❌ Evaluation error for {symbol} [{profit_objective}]: {e}"
            )
            result_base["status"] = "error"
            return result_base

    # ─────────────────────────────────────────────────────────
    # جلب الشموع من DB
    # ─────────────────────────────────────────────────────────
    async def _fetch_candles(
        self, symbol: str, timeframe: str, limit: int
    ) -> Optional[pd.DataFrame]:
        """يجلب الشموع من market_data.ohlcv."""
        if self.db_pool is None:
            # وضع التطوير: بيانات اصطناعية
            return self._generate_synthetic_candles(limit)

        try:
            async with self.db_pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT time, open, high, low, close, volume
                    FROM market_data.ohlcv
                    WHERE symbol = $1 AND timeframe = $2
                    ORDER BY time DESC
                    LIMIT $3
                    """,
                    symbol, timeframe, limit,
                )
            if not rows:
                return None

            df = pd.DataFrame(rows, columns=["time", "open", "high", "low", "close", "volume"])
            df = df.sort_values("time").reset_index(drop=True)
            for col in ["open", "high", "low", "close"]:
                df[col] = df[col].astype(float)
            df["volume"] = df["volume"].astype(float)
            return df

        except Exception as e:
            self.logger.error(f"DB fetch error for {symbol}: {e}")
            return None

    def _generate_synthetic_candles(self, n: int = 500) -> pd.DataFrame:
        """يولّد بيانات اصطناعية للاختبار (Random Walk)."""
        rng = np.random.default_rng(42)
        price = 100.0
        rows = []
        for i in range(n):
            change = rng.normal(0, 0.5)
            open_  = price
            close_ = max(0.1, price + change)
            high_  = max(open_, close_) + abs(rng.normal(0, 0.2))
            low_   = min(open_, close_) - abs(rng.normal(0, 0.2))
            vol    = int(rng.uniform(100_000, 1_000_000))
            rows.append({
                "time":   pd.Timestamp("2024-01-01") + pd.Timedelta(minutes=i),
                "open":   round(open_,  4),
                "high":   round(high_,  4),
                "low":    round(low_,   4),
                "close":  round(close_, 4),
                "volume": vol,
            })
            price = close_
        return pd.DataFrame(rows)

    # ─────────────────────────────────────────────────────────
    # حساب المؤشرات الفنية
    # ─────────────────────────────────────────────────────────
    def _compute_indicators(
        self, df: pd.DataFrame, dna: Dict[str, Any]
    ) -> pd.DataFrame:
        """يحسب المؤشرات المطلوبة في الشيفرة الجينية."""
        all_conditions = (
            dna.get("entry_conditions", []) + dna.get("exit_conditions", [])
        )

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

            elif indicator == "MACD":
                fp = int(cond.get("fast_period", 12))
                sp = int(cond.get("slow_period", 26))
                sig = int(cond.get("signal_period", 9))
                ema_fast = df["close"].ewm(span=fp, adjust=False).mean()
                ema_slow = df["close"].ewm(span=sp, adjust=False).mean()
                df["macd_line"]   = ema_fast - ema_slow
                df["macd_signal"] = df["macd_line"].ewm(span=sig, adjust=False).mean()

            elif indicator == "BOLLINGER":
                period = int(cond.get("period", 20))
                std    = float(cond.get("std", 2.0))
                ma     = df["close"].rolling(period).mean()
                sigma  = df["close"].rolling(period).std()
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

        return df

    # ─────────────────────────────────────────────────────────
    # توليد إشارات الدخول والخروج
    # ─────────────────────────────────────────────────────────
    def _generate_signals(
        self, df: pd.DataFrame, dna: Dict[str, Any]
    ) -> pd.DataFrame:
        """يُضيف عمودَي enter_long و exit_long إلى DataFrame."""
        df["enter_long"] = False
        df["exit_long"]  = False

        # ── إشارات الدخول ──
        entry_mask = pd.Series([True] * len(df), index=df.index)
        for cond in dna.get("entry_conditions", []):
            mask = self._evaluate_condition(df, cond, "entry")
            entry_mask = entry_mask & mask

        # ── إشارات الخروج ──
        exit_mask = pd.Series([False] * len(df), index=df.index)
        for cond in dna.get("exit_conditions", []):
            mask = self._evaluate_condition(df, cond, "exit")
            exit_mask = exit_mask | mask

        df["enter_long"] = entry_mask
        df["exit_long"]  = exit_mask
        return df

    def _evaluate_condition(
        self, df: pd.DataFrame, cond: Dict[str, Any], side: str
    ) -> pd.Series:
        """يُقيّم شرط واحد ويُعيد Series من True/False."""
        indicator = cond.get("indicator", "")
        operator  = cond.get("operator", "")
        value     = cond.get("value", 0)
        n         = len(df)
        false_series = pd.Series([False] * n, index=df.index)

        try:
            if indicator == "RSI":
                period = int(cond.get("period", 14))
                col    = f"rsi_{period}"
                if col not in df.columns:
                    return false_series
                if operator in ("<", "<="):
                    return df[col] < value
                else:
                    return df[col] > value

            elif indicator in ("SMA_CROSS", "EMA_CROSS"):
                prefix = "sma" if indicator == "SMA_CROSS" else "ema"
                fp = int(cond.get("fast_period", 10))
                sp = int(cond.get("slow_period", 50))
                fast_col = f"{prefix}_fast_{fp}"
                slow_col = f"{prefix}_slow_{sp}"
                if fast_col not in df.columns or slow_col not in df.columns:
                    return false_series
                if operator == "crosses_above":
                    return (
                        (df[fast_col] > df[slow_col]) &
                        (df[fast_col].shift(1) <= df[slow_col].shift(1))
                    )
                else:
                    return (
                        (df[fast_col] < df[slow_col]) &
                        (df[fast_col].shift(1) >= df[slow_col].shift(1))
                    )

            elif indicator == "EMA_PRICE":
                period = int(cond.get("period", 50))
                col    = f"ema_{period}"
                if col not in df.columns:
                    return false_series
                if operator == "price_above":
                    return df["close"] > df[col]
                else:
                    return df["close"] < df[col]

            elif indicator == "MACD":
                if "macd_line" not in df.columns:
                    return false_series
                if operator == "macd_crosses_above_signal":
                    return (
                        (df["macd_line"] > df["macd_signal"]) &
                        (df["macd_line"].shift(1) <= df["macd_signal"].shift(1))
                    )
                else:
                    return (
                        (df["macd_line"] < df["macd_signal"]) &
                        (df["macd_line"].shift(1) >= df["macd_signal"].shift(1))
                    )

            elif indicator == "BOLLINGER":
                period = int(cond.get("period", 20))
                if operator == "price_below_lower":
                    col = f"bb_lower_{period}"
                    return df["close"] < df[col] if col in df.columns else false_series
                else:
                    col = f"bb_upper_{period}"
                    return df["close"] > df[col] if col in df.columns else false_series

            elif indicator == "ADX":
                period = int(cond.get("period", 14))
                col    = f"adx_{period}"
                return df[col] > value if col in df.columns else false_series

            elif indicator == "VOLUME_SURGE":
                ma_period   = int(cond.get("ma_period", 20))
                multiplier  = float(cond.get("multiplier", 1.5))
                col         = f"vol_ma_{ma_period}"
                return (
                    df["volume"] > df[col] * multiplier
                    if col in df.columns else false_series
                )

            elif indicator in ("STOCH", "CCI"):
                period = int(cond.get("period", cond.get("fastk_period", 14)))
                col    = f"stoch_k_{period}"
                if col not in df.columns:
                    return false_series
                if operator in ("<", "<="):
                    return df[col] < value
                else:
                    return df[col] > value

        except Exception:
            pass

        return false_series

    # ─────────────────────────────────────────────────────────
    # محاكاة الصفقات
    # ─────────────────────────────────────────────────────────
    def _simulate_trades(
        self, df: pd.DataFrame, dna: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        محاكاة بسيطة للصفقات:
          - دخول عند enter_long = True
          - خروج عند exit_long = True أو stoploss أو roi
        """
        stoploss = float(dna.get("stoploss", -0.02))
        roi_dict = dna.get("roi", {"0": 0.03})
        roi_target = float(list(roi_dict.values())[0])

        trades: List[Dict[str, Any]] = []
        in_trade = False
        entry_price = 0.0
        entry_idx   = 0

        for i in range(1, len(df)):
            row = df.iloc[i]

            if not in_trade:
                # فحص إشارة الدخول
                if df.iloc[i - 1].get("enter_long", False):
                    in_trade    = True
                    entry_price = float(row["open"])
                    entry_idx   = i
            else:
                current_price = float(row["close"])
                pnl_pct = (current_price - entry_price) / entry_price

                # فحص الخروج
                exit_reason = None
                if pnl_pct <= stoploss:
                    exit_reason = "stoploss"
                    exit_price  = entry_price * (1 + stoploss)
                elif pnl_pct >= roi_target:
                    exit_reason = "roi"
                    exit_price  = current_price
                elif row.get("exit_long", False):
                    exit_reason = "signal"
                    exit_price  = current_price

                if exit_reason:
                    final_pnl = (exit_price - entry_price) / entry_price
                    trades.append({
                        "entry_idx":   entry_idx,
                        "exit_idx":    i,
                        "entry_price": entry_price,
                        "exit_price":  exit_price,
                        "pnl_pct":     final_pnl,
                        "duration":    i - entry_idx,
                        "exit_reason": exit_reason,
                    })
                    in_trade = False

        return trades

    # ─────────────────────────────────────────────────────────
    # حساب مقاييس الأداء
    # ─────────────────────────────────────────────────────────
    def _compute_metrics(self, trades: List[Dict]) -> Dict[str, Any]:
        """يحسب جميع مقاييس الأداء من قائمة الصفقات."""
        if not trades:
            return {
                "total_profit_pct": 0.0, "win_rate": 0.0, "total_trades": 0,
                "avg_profit_pct": 0.0, "max_drawdown_pct": 0.0,
                "sharpe_ratio": 0.0, "profit_factor": 0.0, "avg_duration_min": 0,
            }

        pnls = [t["pnl_pct"] for t in trades]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]

        total_profit = sum(pnls) * 100
        win_rate     = len(wins) / len(pnls)
        avg_profit   = (sum(pnls) / len(pnls)) * 100

        # Max Drawdown
        cumulative = 1.0
        peak       = 1.0
        max_dd     = 0.0
        for p in pnls:
            cumulative *= (1 + p)
            if cumulative > peak:
                peak = cumulative
            dd = (peak - cumulative) / peak
            if dd > max_dd:
                max_dd = dd

        # Sharpe Ratio (annualised, assuming 1m candles → ~1440 trades/day)
        if len(pnls) > 1:
            mean_r = np.mean(pnls)
            std_r  = np.std(pnls, ddof=1)
            sharpe = (mean_r / std_r * math.sqrt(252 * 1440)) if std_r > 0 else 0.0
        else:
            sharpe = 0.0

        # Profit Factor
        gross_profit = sum(wins) if wins else 0.0
        gross_loss   = abs(sum(losses)) if losses else 0.0
        profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (
            999.0 if gross_profit > 0 else 0.0
        )

        avg_duration = int(np.mean([t["duration"] for t in trades]))

        return {
            "total_profit_pct": round(total_profit, 4),
            "win_rate":         round(win_rate, 4),
            "total_trades":     len(trades),
            "avg_profit_pct":   round(avg_profit, 4),
            "max_drawdown_pct": round(max_dd * 100, 4),
            "sharpe_ratio":     round(sharpe, 4),
            "profit_factor":    round(profit_factor, 4),
            "avg_duration_min": avg_duration,
        }

    # ─────────────────────────────────────────────────────────
    # حساب Fitness Score — معادلات الأهداف الأربعة
    # ─────────────────────────────────────────────────────────
    def _compute_fitness(
        self, metrics: Dict[str, Any], profit_objective: str
    ) -> float:
        """
        يحسب درجة التقييم النهائية بناءً على الهدف الربحي.

        scalping:       win_rate×0.50 + avg_profit×0.30 + profit_factor×0.10 - drawdown×0.10
        short_swings:   total_profit×0.40 + win_rate×0.30 + sharpe×0.20 - drawdown×0.10
        medium_trends:  total_profit×0.50 + sharpe×0.30 - drawdown×0.20
        momentum:       total_profit×0.60 + profit_factor×0.20 - drawdown×0.20
        """
        wr  = metrics["win_rate"]
        tp  = metrics["total_profit_pct"] / 100.0   # تحويل من % إلى نسبة
        ap  = metrics["avg_profit_pct"]   / 100.0
        dd  = metrics["max_drawdown_pct"] / 100.0
        sr  = metrics["sharpe_ratio"]     / 10.0    # تطبيع
        pf  = min(metrics["profit_factor"], 5.0)    # تحديد سقف
        pf_norm = pf / 5.0                          # تطبيع 0-1

        if profit_objective == "scalping":
            score = (wr * 0.50) + (ap * 0.30) + (pf_norm * 0.10) - (dd * 0.10)

        elif profit_objective == "short_swings":
            score = (tp * 0.40) + (wr * 0.30) + (sr * 0.20) - (dd * 0.10)

        elif profit_objective == "medium_trends":
            score = (tp * 0.50) + (sr * 0.30) - (dd * 0.20)

        elif profit_objective == "momentum":
            score = (tp * 0.60) + (pf_norm * 0.20) - (dd * 0.20)

        else:
            score = tp - dd

        # تأكد من أن الدرجة في نطاق معقول
        return round(max(-1.0, min(1.0, score)), 6)

    # ─────────────────────────────────────────────────────────
    # حفظ النتائج في DB
    # ─────────────────────────────────────────────────────────
    async def save_result(self, result: Dict[str, Any]) -> bool:
        """يحفظ نتيجة التقييم في genetic.performance."""
        if self.db_pool is None:
            self.logger.debug("No DB pool — skipping save")
            return False

        # التأكد من وجود strategy_hash
        strategy_hash = result.get("strategy_hash")
        if not strategy_hash:
            self.logger.error("Missing 'strategy_hash' in result. Cannot save to DB.")
            return False

        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO genetic.performance (
                        strategy_hash, symbol, profit_objective,
                        total_profit_pct, win_rate, total_trades,
                        avg_profit_pct, max_drawdown_pct, sharpe_ratio,
                        profit_factor, avg_duration_min,
                        fitness_score, fitness_formula, candles_count
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9,
                        $10, $11, $12, $13, $14
                    )
                    ON CONFLICT DO NOTHING
                    """,
                    strategy_hash,
                    result.get("symbol", "UNKNOWN"),
                    result.get("profit_objective", "UNKNOWN"),
                    result.get("total_profit_pct", 0.0),
                    result.get("win_rate", 0.0),
                    result.get("total_trades", 0),
                    result.get("avg_profit_pct", 0.0),
                    result.get("max_drawdown_pct", 0.0),
                    result.get("sharpe_ratio", 0.0),
                    result.get("profit_factor", 0.0),
                    result.get("avg_duration_min", 0),
                    result.get("fitness_score", 0.0),
                    result.get("fitness_formula", ""),
                    result.get("candles_count", 0),
                )

                # تحديث fitness_score في جدول الاستراتيجيات
                await conn.execute(
                    """
                    UPDATE genetic.strategies
                    SET fitness_score = $1,
                        status = CASE
                            WHEN $1 >= 0.3 THEN 'elite'
                            WHEN $1 >= 0.0 THEN 'evaluated'
                            ELSE 'retired'
                        END
                    WHERE strategy_hash = $2
                    """,
                    result.get("fitness_score", 0.0),
                    strategy_hash,
                )
            return True

        except Exception as e:
            self.logger.error(f"DB save error: {e}")
            return False

    # ─────────────────────────────────────────────────────────
    # حفظ الشيفرة الجينية في DB
    # ─────────────────────────────────────────────────────────
    async def save_strategy(self, dna: Dict[str, Any]) -> bool:
        """يحفظ الشيفرة الجينية في genetic.strategies."""
        if self.db_pool is None:
            return False
        try:
            import json
            from bots.generator.bot import OBJECTIVE_TO_RISK_BOX
            risk_box = OBJECTIVE_TO_RISK_BOX.get(dna.get("profit_objective", ""), "speculation")

            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO genetic.strategies (
                        strategy_hash, symbol, profit_objective, risk_box,
                        generation, dna, status,
                        parent_a_hash, parent_b_hash, mutation_count
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (strategy_hash) DO NOTHING
                    """,
                    dna.get("hash", ""),
                    dna.get("symbol", ""),
                    dna.get("profit_objective", ""),
                    risk_box,
                    dna.get("generation", 1),
                    json.dumps(dna, ensure_ascii=False),
                    "pending",
                    dna.get("parent_a_hash"),
                    dna.get("parent_b_hash"),
                    dna.get("mutation_count", 0),
                )
            return True
        except Exception as e:
            self.logger.error(f"DB strategy save error: {e}")
            return False

    # ─────────────────────────────────────────────────────────
    # مؤشرات فنية مساعدة (بدون talib)
    # ─────────────────────────────────────────────────────────
    @staticmethod
    def _rsi(series: pd.Series, period: int = 14) -> pd.Series:
        delta  = series.diff()
        gain   = delta.where(delta > 0, 0.0).rolling(period).mean()
        loss   = (-delta.where(delta < 0, 0.0)).rolling(period).mean()
        rs     = gain / loss.replace(0, np.nan)
        return 100 - (100 / (1 + rs))

    @staticmethod
    def _atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
        hl  = df["high"] - df["low"]
        hc  = (df["high"] - df["close"].shift(1)).abs()
        lc  = (df["low"]  - df["close"].shift(1)).abs()
        tr  = pd.concat([hl, hc, lc], axis=1).max(axis=1)
        return tr.rolling(period).mean()

    @staticmethod
    def _adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
        """تقريب مبسّط لـ ADX."""
        hl  = (df["high"] - df["low"]).rolling(period).mean()
        return hl / df["close"] * 100

    @staticmethod
    def _stoch_k(df: pd.DataFrame, period: int = 14) -> pd.Series:
        low_min  = df["low"].rolling(period).min()
        high_max = df["high"].rolling(period).max()
        denom    = (high_max - low_min).replace(0, np.nan)
        return (df["close"] - low_min) / denom * 100
