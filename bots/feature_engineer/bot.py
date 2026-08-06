"""
Bot: Feature Engineer (Phase 3)
────────────────────────────────
Nightly fixed feature set from OHLCV. No Featuretools.
v1: compute + store only.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from loguru import logger
from sqlalchemy import text

from config.config_manager import config
from scripts.database import db
from scripts.redis_manager import redis_manager


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class FeatureEngineerBot:
    """Compute stable nightly features per symbol."""

    def __init__(self):
        self.name = "feature_engineer"
        self.logger = logger.bind(bot=self.name)
        self.timeframe = str(config.get("bots.feature_engineer.timeframe", "1d"))
        self.lookback = int(config.get("bots.feature_engineer.lookback_days", 120))
        self.min_bars = int(config.get("bots.feature_engineer.min_bars", 30))

    def run(
        self,
        symbols: Optional[List[str]] = None,
        max_symbols: Optional[int] = None,
    ) -> Dict[str, Any]:
        symbols = symbols or self._list_symbols()
        if max_symbols:
            symbols = symbols[: int(max_symbols)]

        self.logger.info(
            f"FeatureEngineer start: {len(symbols)} symbols tf={self.timeframe}"
        )
        stats = {"ok": 0, "insufficient_data": 0, "errors": 0, "total": len(symbols)}

        for symbol in symbols:
            try:
                row = self.compute(symbol)
                self._upsert(row)
                st = row.get("status")
                if st == "ok":
                    stats["ok"] += 1
                elif st == "insufficient_data":
                    stats["insufficient_data"] += 1
                else:
                    stats["errors"] += 1
            except Exception as exc:
                stats["errors"] += 1
                self.logger.error(f"{symbol}: feature engineer failed: {exc}")
                try:
                    self._upsert(
                        {
                            "symbol": symbol,
                            "timeframe": self.timeframe,
                            "status": "error",
                            "computed_at": _utcnow(),
                            "updated_at": _utcnow(),
                        }
                    )
                except Exception:
                    pass

        self.logger.success(f"FeatureEngineer complete: {stats}")
        return stats

    def compute(self, symbol: str) -> Dict[str, Any]:
        now = _utcnow()
        base: Dict[str, Any] = {
            "symbol": symbol,
            "timeframe": self.timeframe,
            "status": "insufficient_data",
            "computed_at": now,
            "updated_at": now,
            "sample_size": 0,
        }
        df = self._fetch_ohlcv(symbol, limit=self.lookback + 5)
        if df is None or len(df) < self.min_bars:
            base["sample_size"] = 0 if df is None else len(df)
            return base

        close = df["close"].astype(float)
        high = df["high"].astype(float)
        low = df["low"].astype(float)
        volume = df["volume"].astype(float).fillna(0.0)
        ret = close.pct_change()

        def _safe(val):
            if val is None or (isinstance(val, float) and (np.isnan(val) or np.isinf(val))):
                return None
            return float(val)

        ret_1d = _safe(ret.iloc[-1]) if len(ret) else None
        ret_5d = _safe(close.iloc[-1] / close.iloc[-6] - 1) if len(close) >= 6 else None
        ret_20d = _safe(close.iloc[-1] / close.iloc[-21] - 1) if len(close) >= 21 else None

        vol_10d = _safe(ret.tail(10).std() * np.sqrt(252)) if len(ret.dropna()) >= 10 else None
        vol_20d = _safe(ret.tail(20).std() * np.sqrt(252)) if len(ret.dropna()) >= 20 else None

        rsi_14 = _safe(self._rsi(close, 14).iloc[-1])
        roc_10 = _safe(close.pct_change(10).iloc[-1]) if len(close) >= 11 else None

        sma20 = close.rolling(20).mean()
        sma50 = close.rolling(50).mean()
        sma20_dist = _safe((close.iloc[-1] / sma20.iloc[-1] - 1) if len(close) >= 20 and sma20.iloc[-1] else None)
        sma50_dist = _safe((close.iloc[-1] / sma50.iloc[-1] - 1) if len(close) >= 50 and sma50.iloc[-1] else None)

        vol_ma20 = volume.rolling(20).mean()
        volume_ratio_20 = _safe(
            (volume.iloc[-1] / vol_ma20.iloc[-1]) if len(volume) >= 20 and vol_ma20.iloc[-1] else None
        )

        last_range = high.iloc[-1] - low.iloc[-1]
        range_pct = _safe(last_range / close.iloc[-1] if close.iloc[-1] else None)
        close_loc = _safe(
            ((close.iloc[-1] - low.iloc[-1]) / last_range) if last_range > 0 else 0.5
        )

        features_json = {
            "ret_1d": ret_1d,
            "ret_5d": ret_5d,
            "ret_20d": ret_20d,
            "vol_10d": vol_10d,
            "vol_20d": vol_20d,
            "rsi_14": rsi_14,
            "roc_10": roc_10,
            "sma20_dist": sma20_dist,
            "sma50_dist": sma50_dist,
            "volume_ratio_20": volume_ratio_20,
            "range_pct": range_pct,
            "close_loc": close_loc,
        }

        base.update(
            {
                "ret_1d": ret_1d,
                "ret_5d": ret_5d,
                "ret_20d": ret_20d,
                "vol_10d": vol_10d,
                "vol_20d": vol_20d,
                "rsi_14": rsi_14,
                "roc_10": roc_10,
                "sma20_dist": sma20_dist,
                "sma50_dist": sma50_dist,
                "volume_ratio_20": volume_ratio_20,
                "range_pct": range_pct,
                "close_loc": close_loc,
                "features_json": json.dumps(features_json),
                "sample_size": len(df),
                "status": "ok",
            }
        )
        return base

    @staticmethod
    def _rsi(series: pd.Series, period: int = 14) -> pd.Series:
        delta = series.diff()
        gain = delta.where(delta > 0, 0.0).rolling(period).mean()
        loss = (-delta.where(delta < 0, 0.0)).rolling(period).mean()
        rs = gain / loss.replace(0, np.nan)
        return 100 - (100 / (1 + rs))

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
            self.logger.warning(f"symbols list failed: {exc}")
        return ["1120", "2010", "2222", "1010", "1180"]

    def _fetch_ohlcv(self, symbol: str, limit: int = 120) -> Optional[pd.DataFrame]:
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
                return None
            df = pd.DataFrame(
                rows, columns=["time", "open", "high", "low", "close", "volume"]
            )
            df["time"] = pd.to_datetime(df["time"], utc=True)
            for c in ["open", "high", "low", "close", "volume"]:
                df[c] = pd.to_numeric(df[c], errors="coerce")
            return df.sort_values("time").reset_index(drop=True)
        except Exception as exc:
            self.logger.error(f"fetch ohlcv {symbol}: {exc}")
            return None

    def _upsert(self, row: Dict[str, Any]) -> None:
        cols = [
            "symbol", "timeframe",
            "ret_1d", "ret_5d", "ret_20d",
            "vol_10d", "vol_20d",
            "rsi_14", "roc_10",
            "sma20_dist", "sma50_dist",
            "volume_ratio_20", "range_pct", "close_loc",
            "features_json", "sample_size", "status",
            "computed_at", "updated_at",
        ]
        data = {c: row.get(c) for c in cols}
        # features_json may already be str
        if data.get("features_json") is not None and not isinstance(data["features_json"], str):
            data["features_json"] = json.dumps(data["features_json"])

        col_list = ", ".join(cols)
        placeholders = ", ".join(f":{c}" for c in cols)
        updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != "symbol")
        sql = f"""
            INSERT INTO market_data.stock_features ({col_list})
            VALUES ({placeholders})
            ON CONFLICT (symbol) DO UPDATE SET {updates}
        """
        with db.get_session() as session:
            session.execute(text(sql), data)
            session.commit()


if __name__ == "__main__":
    print(FeatureEngineerBot().run(max_symbols=5))
