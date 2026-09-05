"""Convert session-cumulative volume on live 1m candles into per-minute volume."""
from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, Tuple

# symbol -> (session_date, last_cumulative)
_STATE: Dict[str, Tuple[date, int]] = {}


def _as_date(ts: Any) -> date:
    if isinstance(ts, datetime):
        return ts.date()
    if isinstance(ts, date):
        return ts
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00")).date()
    except Exception:
        return datetime.utcnow().date()


def minute_volume_from_cumulative(symbol: str, raw_volume: int, ts: Any = None) -> int:
    """SahmK ticks often carry session-cumulative volume. Store per-minute delta."""
    try:
        raw = int(raw_volume or 0)
    except (TypeError, ValueError):
        raw = 0
    if raw < 0:
        raw = 0

    day = _as_date(ts)
    key = str(symbol or "")
    prev = _STATE.get(key)

    if prev is None or prev[0] != day or raw < prev[1]:
        _STATE[key] = (day, raw)
        return raw

    delta = raw - prev[1]
    _STATE[key] = (day, raw)
    return max(delta, 0)
