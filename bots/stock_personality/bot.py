"""
Bot: Stock Personality (Phase 2)
────────────────────────────────
Computes per-symbol statistical, behavioral, and phase features.
v1: compute + store only (no Scientist consumption).
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from loguru import logger
from sqlalchemy import text

from config.config_manager import config
from scripts.database import db
from scripts.redis_manager import redis_manager


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class StockPersonalityBot:
    """Nightly personality engine for TASI symbols."""

    def __init__(self):
        self.name = "stock_personality"
        self.logger = logger.bind(bot=self.name)
        self.benchmark = str(
            config.get("bots.stock_personality.benchmark_symbol", "90001")
        )
        self.timeframe = str(
            config.get("bots.stock_personality.timeframe", "1d")
        )
        self.max_lookback = int(
            config.get("bots.stock_personality.max_lookback_days", 252)
        )
        # Min bars per window
        self.min_30 = 20
        self.min_90 = 60
        self.min_all = 100

    # ── Public entry ─────────────────────────────────────────────────────────
    def run(
        self,
        symbols: Optional[List[str]] = None,
        max_symbols: Optional[int] = None,
    ) -> Dict[str, Any]:
        symbols = symbols or self._list_symbols()
        if max_symbols:
            symbols = symbols[: int(max_symbols)]

        self.logger.info(
            f"Starting StockPersonality for {len(symbols)} symbols "
            f"(benchmark={self.benchmark}, tf={self.timeframe})"
        )

        bench = self._fetch_ohlcv(self.benchmark, limit=self.max_lookback + 5)
        if bench is None or len(bench) < self.min_30:
            self.logger.error(
                f"Benchmark {self.benchmark} insufficient data — aborting run"
            )
            return {
                "ok": 0,
                "insufficient_data": 0,
                "errors": len(symbols),
                "total": len(symbols),
                "status": "benchmark_unavailable",
            }

        bench_ret = bench["close"].pct_change().dropna()

        stats = {"ok": 0, "insufficient_data": 0, "errors": 0, "total": len(symbols)}
        for symbol in symbols:
            try:
                row = self.compute_personality(symbol, bench, bench_ret)
                self._upsert(row)
                if row.get("status") == "ok":
                    stats["ok"] += 1
                elif row.get("status") == "insufficient_data":
                    stats["insufficient_data"] += 1
                else:
                    stats["errors"] += 1
            except Exception as exc:
                stats["errors"] += 1
                self.logger.error(f"{symbol}: personality failed: {exc}")
                try:
                    self._upsert(
                        {
                            "symbol": symbol,
                            "status": "error",
                            "benchmark_symbol": self.benchmark,
                            "computed_at": _utcnow(),
                            "updated_at": _utcnow(),
                        }
                    )
                except Exception:
                    pass

        self.logger.success(
            f"StockPersonality complete: {stats}"
        )
        return stats

    # ── Core compute ─────────────────────────────────────────────────────────
    def compute_personality(
        self,
        symbol: str,
        bench: pd.DataFrame,
        bench_ret: pd.Series,
    ) -> Dict[str, Any]:
        df = self._fetch_ohlcv(symbol, limit=self.max_lookback + 5)
        now = _utcnow()
        base: Dict[str, Any] = {
            "symbol": symbol,
            "benchmark_symbol": self.benchmark,
            "computed_at": now,
            "updated_at": now,
            "status": "insufficient_data",
        }
        if df is None or len(df) < self.min_30:
            base["sample_size_all"] = 0 if df is None else len(df)
            return base

        # Align on time index for beta windows
        stock = df.set_index("time")
        market = bench.set_index("time")
        aligned = pd.DataFrame(
            {
                "s_close": stock["close"],
                "s_high": stock["high"],
                "s_low": stock["low"],
                "s_volume": stock["volume"],
                "m_close": market["close"],
            }
        ).dropna(subset=["s_close"])

        if len(aligned) < self.min_30:
            base["sample_size_all"] = len(aligned)
            return base

        s_ret = aligned["s_close"].pct_change()
        m_ret = aligned["m_close"].pct_change()
        rets = pd.DataFrame({"s": s_ret, "m": m_ret}).dropna()

        n_all = len(rets)
        base["sample_size_all"] = n_all
        base["sample_size_30d"] = min(30, n_all)
        base["sample_size_90d"] = min(90, n_all)

        # Statistical multi-window
        b30, a30 = self._beta_alpha(rets, 30, self.min_30)
        b90, a90 = self._beta_alpha(rets, 90, self.min_90)
        ball, aall = self._beta_alpha(rets, None, self.min_all if n_all >= self.min_all else self.min_30)

        base["beta_30d"] = b30
        base["alpha_30d"] = a30
        base["beta_90d"] = b90
        base["alpha_90d"] = a90
        base["beta_all"] = ball
        base["alpha_all"] = aall
        base["volatility_30d"] = self._vol(rets["s"], 30)
        base["volatility_90d"] = self._vol(rets["s"], 90)

        beta_ref = b90 if b90 is not None else b30
        base["beta_class"] = self._beta_class(beta_ref)
        base["vol_class"] = self._vol_class(base["volatility_30d"])

        # Behavioral on price/volume (use last 90 bars of stock)
        win = stock.tail(90).copy()
        behavioral = self._behavioral_metrics(win)
        base.update(behavioral)

        phase_info = self._detect_phase(win, behavioral)
        base.update(phase_info)

        base["status"] = "ok"
        return base

    # ── Stats helpers ────────────────────────────────────────────────────────
    @staticmethod
    def _beta_alpha(
        rets: pd.DataFrame, window: Optional[int], min_n: int
    ) -> Tuple[Optional[float], Optional[float]]:
        if window is None:
            sub = rets
        else:
            sub = rets.tail(window)
        if len(sub) < min_n:
            return None, None
        var_m = float(sub["m"].var())
        if var_m <= 0 or np.isnan(var_m):
            return None, None
        cov = float(sub["s"].cov(sub["m"]))
        beta = cov / var_m
        alpha = float(sub["s"].mean() - beta * sub["m"].mean())
        return round(beta, 6), round(alpha, 8)

    @staticmethod
    def _vol(series: pd.Series, window: int) -> Optional[float]:
        sub = series.tail(window).dropna()
        if len(sub) < 10:
            return None
        return round(float(sub.std() * np.sqrt(252)), 6)

    @staticmethod
    def _beta_class(beta: Optional[float]) -> Optional[str]:
        if beta is None:
            return None
        if beta < 0.8:
            return "defensive"
        if beta <= 1.2:
            return "neutral"
        return "aggressive"

    @staticmethod
    def _vol_class(vol: Optional[float]) -> Optional[str]:
        if vol is None:
            return None
        if vol < 0.20:
            return "low"
        if vol < 0.40:
            return "medium"
        return "high"

    # ── Behavioral ───────────────────────────────────────────────────────────
    def _behavioral_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        if len(df) < 15:
            return {
                "accumulation_score": None,
                "range_tightness": None,
                "explosiveness_score": None,
                "distribution_score": None,
                "distribution_style": None,
                "cycle_length_est": None,
                "breathing_rhythm_days": None,
            }

        high = df["high"].astype(float)
        low = df["low"].astype(float)
        close = df["close"].astype(float)
        volume = df["volume"].astype(float).fillna(0.0)
        ret = close.pct_change()

        # ATR approx
        tr = pd.concat(
            [
                high - low,
                (high - close.shift(1)).abs(),
                (low - close.shift(1)).abs(),
            ],
            axis=1,
        ).max(axis=1)
        atr = tr.rolling(14).mean()

        rng = (high - low) / atr.replace(0, np.nan)
        range_tightness = float(rng.tail(30).median()) if rng.notna().any() else None

        vol_ma = volume.rolling(20).mean()
        # Tight range days: range in lowest 20% of recent ranges
        recent_range = (high - low).tail(60)
        q20 = recent_range.quantile(0.20) if len(recent_range) else None
        tight_mask = (high - low) <= q20 if q20 is not None else pd.Series(False, index=df.index)
        low_vol_mask = volume < vol_ma
        acc_days = (tight_mask.tail(30) & low_vol_mask.tail(30)).sum()
        accumulation_score = float(min(100.0, (acc_days / max(30, 1)) * 100.0 * 1.5))

        up_explosive = (ret > 0.02).tail(60).sum()
        explosiveness_score = float(min(100.0, (up_explosive / max(len(ret.tail(60)), 1)) * 100.0 * 3))

        down_dist = ((ret < -0.02) & (volume > vol_ma * 1.2)).tail(60).sum()
        distribution_score = float(min(100.0, (down_dist / max(len(ret.tail(60)), 1)) * 100.0 * 3))

        distribution_style = self._distribution_style(ret.tail(40), volume.tail(40), vol_ma.tail(40))

        cycle_length, rhythm = self._cycle_and_rhythm(close)

        return {
            "accumulation_score": round(accumulation_score, 2),
            "range_tightness": round(range_tightness, 4) if range_tightness is not None else None,
            "explosiveness_score": round(explosiveness_score, 2),
            "distribution_score": round(distribution_score, 2),
            "distribution_style": distribution_style,
            "cycle_length_est": cycle_length,
            "breathing_rhythm_days": rhythm,
        }

    @staticmethod
    def _distribution_style(
        ret: pd.Series, volume: pd.Series, vol_ma: pd.Series
    ) -> Optional[str]:
        if len(ret) < 10:
            return None
        down = ret < -0.015
        up = ret > 0.015
        high_vol = volume > vol_ma * 1.15
        down_hv = int((down & high_vol).sum())
        bounce = int((up & (ret.shift(1) < -0.01)).sum())
        if down_hv >= 3 and bounce >= 2:
            return "vibrant"
        if down_hv >= 4 and bounce <= 1:
            return "violent"
        if int(down.sum()) >= 3:
            return "slow"
        return "slow"

    @staticmethod
    def _cycle_and_rhythm(close: pd.Series) -> Tuple[Optional[int], Optional[float]]:
        if len(close) < 30:
            return None, None
        # Local peaks/troughs via simple rolling max/min
        w = 5
        peaks = (close == close.rolling(w, center=True).max()) & (close.shift(1) < close)
        troughs = (close == close.rolling(w, center=True).min()) & (close.shift(1) > close)
        peak_idx = list(np.where(peaks.fillna(False).to_numpy())[0])
        trough_idx = list(np.where(troughs.fillna(False).to_numpy())[0])

        cycle = None
        if len(peak_idx) >= 2:
            gaps = np.diff(peak_idx)
            cycle = int(np.median(gaps))

        rhythm = None
        events = sorted(peak_idx + trough_idx)
        if len(events) >= 3:
            rhythm = float(np.mean(np.diff(events)))

        return cycle, round(rhythm, 2) if rhythm is not None else None

    # ── Phase detection ──────────────────────────────────────────────────────
    def _detect_phase(
        self, df: pd.DataFrame, behavioral: Dict[str, Any]
    ) -> Dict[str, Any]:
        close = df["close"].astype(float)
        volume = df["volume"].astype(float).fillna(0.0)
        ma50 = close.rolling(50).mean() if len(close) >= 50 else close.rolling(min(20, len(close))).mean()
        ma20 = close.rolling(20).mean() if len(close) >= 20 else close.mean()

        last = float(close.iloc[-1])
        ma50_last = float(ma50.iloc[-1]) if not np.isnan(ma50.iloc[-1]) else last
        ma20_last = float(ma20.iloc[-1]) if not isinstance(ma20, float) and not np.isnan(ma20.iloc[-1]) else (
            float(ma20) if isinstance(ma20, float) else last
        )

        vol_ma = volume.rolling(20).mean()
        vol_rising = float(volume.tail(10).mean()) > float(vol_ma.tail(10).mean()) * 1.05 if vol_ma.notna().any() else False

        acc = float(behavioral.get("accumulation_score") or 0)
        exp = float(behavioral.get("explosiveness_score") or 0)
        dist = float(behavioral.get("distribution_score") or 0)
        tight = float(behavioral.get("range_tightness") or 99)

        # Peak / base over window
        peak = float(close.max())
        base_price = float(close.min())
        gain_from_base = (last - base_price) / base_price * 100.0 if base_price > 0 else 0.0
        drop_from_peak = (peak - last) / peak * 100.0 if peak > 0 else 0.0

        scores = {
            "accumulation": 0.0,
            "markup": 0.0,
            "distribution": 0.0,
            "markdown": 0.0,
        }

        # Accumulation
        if acc >= 40 and tight is not None and tight < 1.2 and abs(last / ma50_last - 1) < 0.05:
            scores["accumulation"] = min(100.0, acc * 0.7 + (40 if tight < 1.0 else 20))

        # Markup
        if exp >= 25 and last > ma20_last and last > ma50_last and vol_rising:
            scores["markup"] = min(100.0, exp * 0.8 + 20)

        # Distribution
        if dist >= 25 and drop_from_peak < 12 and last >= peak * 0.92:
            scores["distribution"] = min(100.0, dist * 0.8 + 15)

        # Markdown
        neg_streak = int((close.pct_change().tail(8) < 0).sum())
        if last < ma20_last and last < ma50_last and (drop_from_peak >= 8 or neg_streak >= 5):
            scores["markdown"] = min(100.0, drop_from_peak * 4 + neg_streak * 5)

        best_phase = max(scores, key=scores.get)
        best_score = scores[best_phase]
        if best_score < 25:
            phase = "transition"
            confidence = max(10.0, 40.0 - best_score)
        else:
            phase = best_phase
            confidence = min(100.0, best_score)

        # days_in_phase: approximate consecutive bars matching simple condition
        days_in_phase = self._estimate_days_in_phase(close, ma20, phase)

        return {
            "phase": phase,
            "phase_confidence": round(float(confidence), 2),
            "days_in_phase": days_in_phase,
            "gain_from_base": round(float(gain_from_base), 4),
            "drop_from_peak": round(float(drop_from_peak), 4),
        }

    @staticmethod
    def _estimate_days_in_phase(close: pd.Series, ma20: Any, phase: str) -> int:
        n = len(close)
        if n < 5:
            return n
        count = 1
        for i in range(n - 2, max(0, n - 60), -1):
            c = float(close.iloc[i])
            m = float(ma20.iloc[i]) if hasattr(ma20, "iloc") else float(close.iloc[i])
            ok = False
            if phase == "markup":
                ok = c >= m
            elif phase == "markdown":
                ok = c <= m
            elif phase == "accumulation":
                ok = abs(c / m - 1) < 0.06 if m else False
            elif phase == "distribution":
                ok = c >= float(close.iloc[max(0, i - 20) : i + 1].max()) * 0.9
            else:
                ok = True
            if ok:
                count += 1
            else:
                break
        return count

    # ── Data access ──────────────────────────────────────────────────────────
    def _list_symbols(self) -> List[str]:
        try:
            cached = redis_manager.get("sahmk:symbols_list")
            if cached and isinstance(cached, list):
                return [str(s) for s in cached if str(s)]
        except Exception:
            pass
        try:
            with db.get_session() as session:
                rows = session.execute(
                    text(
                        """
                        SELECT symbol FROM market_data.symbols
                        WHERE is_active = TRUE
                        ORDER BY symbol
                        """
                    )
                ).fetchall()
            if rows:
                return [str(r[0]) for r in rows]
        except Exception as exc:
            self.logger.warning(f"symbols list from DB failed: {exc}")
        return ["1120", "2010", "2222", "1010", "1180"]

    def _fetch_ohlcv(self, symbol: str, limit: int = 260) -> Optional[pd.DataFrame]:
        try:
            with db.get_session() as session:
                rows = session.execute(
                    text(
                        """
                        SELECT time, open, high, low, close, volume
                        FROM market_data.ohlcv
                        WHERE symbol = :symbol AND timeframe = :tf
                        ORDER BY time DESC
                        LIMIT :limit
                        """
                    ),
                    {"symbol": symbol, "tf": self.timeframe, "limit": limit},
                ).fetchall()
            if not rows:
                # try indices table for benchmark
                with db.get_session() as session:
                    rows = session.execute(
                        text(
                            """
                            SELECT time, open, high, low, close, volume
                            FROM market_data.indices
                            WHERE symbol = :symbol AND timeframe = :tf
                            ORDER BY time DESC
                            LIMIT :limit
                            """
                        ),
                        {"symbol": symbol, "tf": self.timeframe, "limit": limit},
                    ).fetchall()
            if not rows:
                return None
            df = pd.DataFrame(
                rows, columns=["time", "open", "high", "low", "close", "volume"]
            )
            df["time"] = pd.to_datetime(df["time"], utc=True)
            for c in ["open", "high", "low", "close", "volume"]:
                df[c] = pd.to_numeric(df[c], errors="coerce")
            df = df.sort_values("time").reset_index(drop=True)
            return df
        except Exception as exc:
            self.logger.error(f"fetch ohlcv {symbol}: {exc}")
            return None

    def _upsert(self, row: Dict[str, Any]) -> None:
        cols = [
            "symbol",
            "beta_30d", "beta_90d", "beta_all",
            "alpha_30d", "alpha_90d", "alpha_all",
            "volatility_30d", "volatility_90d",
            "accumulation_score", "range_tightness",
            "explosiveness_score", "distribution_score",
            "distribution_style", "cycle_length_est", "breathing_rhythm_days",
            "phase", "phase_confidence", "days_in_phase",
            "gain_from_base", "drop_from_peak",
            "beta_class", "vol_class",
            "benchmark_symbol",
            "sample_size_30d", "sample_size_90d", "sample_size_all",
            "status", "computed_at", "updated_at",
        ]
        data = {c: row.get(c) for c in cols}
        placeholders = ", ".join(f":{c}" for c in cols)
        col_list = ", ".join(cols)
        updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != "symbol")

        sql = f"""
            INSERT INTO market_data.stock_personalities ({col_list})
            VALUES ({placeholders})
            ON CONFLICT (symbol) DO UPDATE SET {updates}
        """
        with db.get_session() as session:
            session.execute(text(sql), data)
            session.commit()


if __name__ == "__main__":
    bot = StockPersonalityBot()
    print(bot.run(max_symbols=5))
