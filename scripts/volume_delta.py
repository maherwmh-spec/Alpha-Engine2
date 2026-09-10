"""Convert session-cumulative volume on live 1m candles into per-minute volume."""
from __future__ import annotations

from datetime import date, datetime, timezone, timedelta
from typing import Any, Dict, Optional, Tuple

_STATE: Dict[str, Tuple[date, int]] = {}
_SESSION_SEED_THRESHOLD = 2_000_000
_RIYADH = timezone(timedelta(hours=3))


def _as_date(ts: Any) -> date:
    if isinstance(ts, datetime):
        return ts.astimezone(_RIYADH).date() if ts.tzinfo else ts.date()
    if isinstance(ts, date):
        return ts
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00")).date()
    except Exception:
        return datetime.now(_RIYADH).date()


def to_riyadh_minute(ts: Any) -> datetime:
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(_RIYADH).replace(second=0, microsecond=0)
    return datetime.now(_RIYADH).replace(second=0, microsecond=0)


def minute_volume_from_cumulative(symbol: str, raw_volume: int, ts: Any = None) -> int:
    try:
        raw = int(float(raw_volume or 0))
    except (TypeError, ValueError):
        raw = 0
    if raw < 0:
        raw = 0
    day = _as_date(ts)
    key = str(symbol or "")
    prev = _STATE.get(key)
    if prev is None or prev[0] != day:
        _STATE[key] = (day, raw)
        if raw >= _SESSION_SEED_THRESHOLD:
            return 0
        return raw
    if raw < prev[1]:
        _STATE[key] = (day, raw)
        return raw
    delta = raw - prev[1]
    _STATE[key] = (day, raw)
    return max(int(delta), 0)


def install_aggregator_hook() -> None:
    from scripts.sahmk_client import CandleAggregator

    def add_tick(self, symbol: str, price: float, volume: float, timestamp: datetime) -> Optional[dict]:
        with self._lock:
            minute_key = to_riyadh_minute(timestamp)
            try:
                raw = float(volume or 0)
            except (TypeError, ValueError):
                raw = 0.0
            if symbol not in self.current_candles:
                self.current_candles[symbol] = {
                    "symbol": symbol,
                    "timestamp": minute_key,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "volume": raw,
                    "tick_count": 1,
                }
                return None
            current = self.current_candles[symbol]
            if minute_key > current["timestamp"]:
                completed = current.copy()
                completed["volume"] = minute_volume_from_cumulative(
                    symbol,
                    completed.get("volume", 0),
                    completed.get("timestamp"),
                )
                self.current_candles[symbol] = {
                    "symbol": symbol,
                    "timestamp": minute_key,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "volume": raw,
                    "tick_count": 1,
                }
                return completed
            current["high"] = max(current["high"], price)
            current["low"] = min(current["low"], price)
            current["close"] = price
            current["volume"] = raw
            current["tick_count"] += 1
            return None

    CandleAggregator.add_tick = add_tick
