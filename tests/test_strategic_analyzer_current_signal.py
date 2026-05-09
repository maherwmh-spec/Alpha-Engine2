
from __future__ import annotations

import sys
import types

import pandas as pd

sys.modules.setdefault("scripts.database", types.SimpleNamespace(db=None))
sys.modules.setdefault("scripts.redis_manager", types.SimpleNamespace(redis_manager=None))

from bots.strategic_analyzer.bot import StrategicAnalyzer


def test_generate_current_signal_uses_latest_closed_candle(monkeypatch):
    analyzer = StrategicAnalyzer()
    df = pd.DataFrame([
        {
            "time": pd.Timestamp("2024-01-01 10:00") + pd.Timedelta(minutes=i),
            "open": 100 - i,
            "high": 101 - i,
            "low": 99 - i,
            "close": 100 - i,
            "volume": 1000,
        }
        for i in range(35)
    ])
    monkeypatch.setattr(analyzer, "_fetch_recent_closed_candles", lambda symbol, timeframe, limit=200: df)
    strategy = {
        "objective": "scalping",
        "fitness": 0.72,
        "dna": {"timeframe": "1m", "entry_conditions": [{"indicator": "RSI", "period": 2, "operator": "<", "value": 60}], "exit_conditions": []},
    }
    signal = analyzer._generate_current_signal_from_dna("1010", strategy)
    assert signal is not None
    assert signal["signal"] == "BUY"
    assert signal["confidence"] == 0.72
    assert signal["metadata"]["source"] == "genetic_last_closed_candle"
