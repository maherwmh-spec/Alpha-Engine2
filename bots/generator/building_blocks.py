"""Expanded BUILDING_BLOCKS for genetic strategy discovery (Phase 1 level A).

PSAR / SUPERTREND are allowed but role_hint marks them as trend_filter_or_exit_not_alone.
Chart patterns (level B) are deferred.
"""
from __future__ import annotations

from typing import Any, Dict

OBJECTIVE_TO_RISK_BOX: Dict[str, str] = {
    "scalping": "speculation",
    "short_swings": "growth",
    "medium_trends": "investment",
    "momentum": "big_strategy",
}

PROFIT_OBJECTIVES = list(OBJECTIVE_TO_RISK_BOX.keys())

BUILDING_BLOCKS: Dict[str, Dict[str, Any]] = {
    "RSI": {
        "type": "oscillator",
        "params": {"period": {"min": 7, "max": 21, "step": 1, "default": 14}},
        "entry_ops": ["<", "<="],
        "exit_ops": [">", ">="],
        "entry_values": [25, 30, 35, 40],
        "exit_values": [60, 65, 70, 75],
    },
    "STOCH": {
        "type": "oscillator",
        "params": {
            "fastk_period": {"min": 5, "max": 21, "step": 1, "default": 14},
            "slowk_period": {"min": 3, "max": 7, "step": 1, "default": 3},
            "slowd_period": {"min": 3, "max": 7, "step": 1, "default": 3},
        },
        "entry_ops": ["<"],
        "exit_ops": [">"],
        "entry_values": [20, 25, 30],
        "exit_values": [70, 75, 80],
    },
    "STOCH_RSI": {
        "type": "oscillator",
        "params": {"period": {"min": 10, "max": 21, "step": 1, "default": 14}},
        "entry_ops": ["<"],
        "exit_ops": [">"],
        "entry_values": [0.2, 0.3],
        "exit_values": [0.7, 0.8],
    },
    "CCI": {
        "type": "oscillator",
        "params": {"period": {"min": 10, "max": 30, "step": 2, "default": 20}},
        "entry_ops": ["<"],
        "exit_ops": [">"],
        "entry_values": [-100, -150, -200],
        "exit_values": [100, 150, 200],
    },
    "WILLR": {
        "type": "oscillator",
        "params": {"period": {"min": 10, "max": 21, "step": 1, "default": 14}},
        "entry_ops": ["<"],
        "exit_ops": [">"],
        "entry_values": [-80, -75, -70],
        "exit_values": [-30, -25, -20],
    },
    "MFI": {
        "type": "oscillator",
        "params": {"period": {"min": 10, "max": 21, "step": 1, "default": 14}},
        "entry_ops": ["<"],
        "exit_ops": [">"],
        "entry_values": [20, 25, 30],
        "exit_values": [70, 75, 80],
    },
    "SMA_CROSS": {
        "type": "ma_cross",
        "params": {
            "fast_period": {"min": 5, "max": 20, "step": 1, "default": 10},
            "slow_period": {"min": 20, "max": 100, "step": 5, "default": 50},
        },
        "entry_ops": ["crosses_above"],
        "exit_ops": ["crosses_below"],
    },
    "EMA_CROSS": {
        "type": "ma_cross",
        "params": {
            "fast_period": {"min": 5, "max": 20, "step": 1, "default": 9},
            "slow_period": {"min": 20, "max": 100, "step": 5, "default": 21},
        },
        "entry_ops": ["crosses_above"],
        "exit_ops": ["crosses_below"],
    },
    "EMA_PRICE": {
        "type": "ma_price",
        "params": {"period": {"min": 10, "max": 200, "step": 5, "default": 50}},
        "entry_ops": ["price_above"],
        "exit_ops": ["price_below"],
    },
    "SMA_PRICE": {
        "type": "ma_price",
        "params": {"period": {"min": 10, "max": 200, "step": 5, "default": 50}},
        "entry_ops": ["price_above"],
        "exit_ops": ["price_below"],
    },
    "MACD": {
        "type": "trend",
        "params": {
            "fast_period": {"min": 8, "max": 16, "step": 1, "default": 12},
            "slow_period": {"min": 20, "max": 30, "step": 1, "default": 26},
            "signal_period": {"min": 7, "max": 12, "step": 1, "default": 9},
        },
        "entry_ops": ["macd_crosses_above_signal"],
        "exit_ops": ["macd_crosses_below_signal"],
    },
    "ADX": {
        "type": "trend_strength",
        "role_hint": "regime_filter",
        "params": {"period": {"min": 10, "max": 20, "step": 1, "default": 14}},
        "entry_ops": [">"],
        "entry_values": [20, 25, 30],
    },
    "PSAR": {
        "type": "trend",
        "role_hint": "trend_filter_or_exit_not_alone",
        "params": {
            "af": {"min": 0.02, "max": 0.04, "step": 0.01, "default": 0.02},
            "max_af": {"min": 0.15, "max": 0.25, "step": 0.05, "default": 0.2},
        },
        "entry_ops": ["price_above_psar"],
        "exit_ops": ["price_below_psar"],
    },
    "SUPERTREND": {
        "type": "trend",
        "role_hint": "trend_filter_or_exit_not_alone",
        "params": {
            "period": {"min": 7, "max": 14, "step": 1, "default": 10},
            "multiplier": {"min": 2.0, "max": 4.0, "step": 0.5, "default": 3.0},
        },
        "entry_ops": ["price_above_st"],
        "exit_ops": ["price_below_st"],
    },
    "BOLLINGER": {
        "type": "volatility",
        "params": {
            "period": {"min": 15, "max": 25, "step": 1, "default": 20},
            "std": {"min": 1.5, "max": 3.0, "step": 0.5, "default": 2.0},
        },
        "entry_ops": ["price_below_lower"],
        "exit_ops": ["price_above_upper"],
    },
    "ATR": {
        "type": "volatility",
        "params": {
            "period": {"min": 10, "max": 20, "step": 1, "default": 14},
            "multiplier": {"min": 1.0, "max": 3.0, "step": 0.5, "default": 1.5},
        },
        "entry_ops": ["atr_breakout"],
    },
    "KELTNER": {
        "type": "volatility",
        "params": {
            "period": {"min": 15, "max": 25, "step": 1, "default": 20},
            "multiplier": {"min": 1.5, "max": 2.5, "step": 0.5, "default": 2.0},
        },
        "entry_ops": ["price_below_lower"],
        "exit_ops": ["price_above_upper"],
    },
    "DONCHIAN": {
        "type": "volatility",
        "params": {"period": {"min": 15, "max": 55, "step": 5, "default": 20}},
        "entry_ops": ["break_upper"],
        "exit_ops": ["break_lower"],
    },
    "VOLUME_SURGE": {
        "type": "volume",
        "params": {
            "ma_period": {"min": 10, "max": 30, "step": 5, "default": 20},
            "multiplier": {"min": 1.2, "max": 3.0, "step": 0.2, "default": 1.5},
        },
        "entry_ops": ["volume_above_ma"],
    },
    "OBV_SLOPE": {
        "type": "volume",
        "params": {"period": {"min": 5, "max": 20, "step": 1, "default": 10}},
        "entry_ops": ["slope_up"],
        "exit_ops": ["slope_down"],
    },
    "CMF": {
        "type": "volume",
        "params": {"period": {"min": 10, "max": 30, "step": 2, "default": 20}},
        "entry_ops": [">"],
        "exit_ops": ["<"],
        "entry_values": [0.05, 0.1],
        "exit_values": [-0.05, -0.1],
    },
}

OBJECTIVE_PARAMS: Dict[str, Dict[str, Any]] = {
    "scalping": {
        "stoploss_range": (-0.005, -0.015),
        "roi_range": (0.005, 0.02),
        "trailing_stop": True,
        "timeframe": "1m",
    },
    "short_swings": {
        "stoploss_range": (-0.015, -0.03),
        "roi_range": (0.03, 0.08),
        "trailing_stop": True,
        "timeframe": "5m",
    },
    "medium_trends": {
        "stoploss_range": (-0.03, -0.06),
        "roi_range": (0.08, 0.20),
        "trailing_stop": False,
        "timeframe": "15m",
    },
    "momentum": {
        "stoploss_range": (-0.02, -0.04),
        "roi_range": (0.05, 0.15),
        "trailing_stop": True,
        "timeframe": "5m",
    },
}
