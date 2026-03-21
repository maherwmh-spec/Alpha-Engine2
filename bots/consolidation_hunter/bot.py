"""Bot 9: Consolidation Hunter - Detects consolidation before breakouts"""
import pandas as pd
import numpy as np
from typing import Dict, List
from loguru import logger
from config.config_manager import config
from scripts.redis_manager import redis_manager
try:
    import talib as ta
    TALIB_AVAILABLE = True
except ImportError:
    TALIB_AVAILABLE = False
    ta = None

class ConsolidationHunter:
    def __init__(self):
        self.name = "consolidation_hunter"
        self.logger = logger.bind(bot=self.name)
        self.config = config.get_bot_config(self.name)
        self.compression_threshold = 0.5  # BB width < 50% of average
    
    def _bbands_numpy(self, close: pd.Series, period: int = 20, num_std: float = 2.0):
        """Pure-numpy Bollinger Bands fallback (no TA-Lib required)."""
        sma = close.rolling(period).mean()
        std = close.rolling(period).std()
        upper = sma + num_std * std
        lower = sma - num_std * std
        return upper, sma, lower

    def detect_bb_squeeze(self, df: pd.DataFrame) -> bool:
        try:
            # Use TA-Lib if available, else pure-numpy fallback
            if TALIB_AVAILABLE and ta is not None:
                bb_upper, bb_mid, bb_lower = ta.BBANDS(df['close'], timeperiod=20)
            else:
                bb_upper, bb_mid, bb_lower = self._bbands_numpy(df['close'])

            # Calculate BB width
            bb_width = (bb_upper - bb_lower) / bb_mid
            bb_width_ma = bb_width.rolling(20).mean()

            # Check if current width is compressed
            current_width = bb_width.iloc[-1]
            avg_width = bb_width_ma.iloc[-1]

            is_squeezed = current_width < (avg_width * self.compression_threshold)
            return is_squeezed
        except:
            return False
    
    def detect_triangle_pattern(self, df: pd.DataFrame) -> Dict:
        try:
            # Simple triangle detection: converging highs and lows
            recent_highs = df['high'].tail(20)
            recent_lows = df['low'].tail(20)
            
            # Check if range is narrowing
            early_range = recent_highs.head(10).max() - recent_lows.head(10).min()
            late_range = recent_highs.tail(10).max() - recent_lows.tail(10).min()
            
            is_triangle = late_range < (early_range * 0.7)
            
            return {
                'detected': is_triangle,
                'early_range': early_range,
                'late_range': late_range,
                'compression_ratio': late_range / early_range if early_range > 0 else 0
            }
        except:
            return {'detected': False}
    
    def analyze_consolidation(self, symbol: str, df: pd.DataFrame) -> Dict:
        try:
            self.logger.info(f"Analyzing consolidation for {symbol}")
            
            # Check BB squeeze
            bb_squeeze = self.detect_bb_squeeze(df)
            
            # Check triangle pattern
            triangle = self.detect_triangle_pattern(df)
            
            # Calculate volume trend (decreasing volume = consolidation)
            volume_trend = df['volume'].tail(20).mean() / df['volume'].tail(50).mean()
            low_volume = volume_trend < 0.8
            
            # Overall consolidation score
            consolidation_score = sum([bb_squeeze, triangle['detected'], low_volume]) / 3
            
            is_consolidating = consolidation_score >= 0.67  # 2 out of 3 signals
            
            result = {
                'symbol': symbol,
                'is_consolidating': is_consolidating,
                'consolidation_score': round(consolidation_score, 2),
                'bb_squeeze': bb_squeeze,
                'triangle_pattern': triangle['detected'],
                'low_volume': low_volume,
                'breakout_probability': 'HIGH' if is_consolidating else 'LOW'
            }
            
            if is_consolidating:
                self.logger.success(f"{symbol} is CONSOLIDATING - Breakout potential!")
            
            redis_manager.set(f"consolidation:{symbol}", result, ttl=3600)
            return result
        except Exception as e:
            self.logger.error(f"Error analyzing {symbol}: {e}")
            return {}
    
    def run(self, symbols: List[str] = None):
        self.logger.info("Starting Consolidation Hunter")
        # Would analyze all symbols
        return {'status': 'success'}
