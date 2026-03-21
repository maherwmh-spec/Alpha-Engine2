"""
Strategy 4: Price Explosions (الانفجارات السعرية) - COMPLETE
Timeframe: 1m, Target: 15%+, Rare events
"""

from freqtrade.strategy import IStrategy
from pandas import DataFrame
try:
    import talib.abstract as ta
except ImportError:
    ta = None
try:
    import freqtrade.vendor.qtpylib.indicators as qtpylib
except ImportError:
    qtpylib = None
import numpy as np


class PriceExplosions(IStrategy):
    INTERFACE_VERSION = 3
    
    minimal_roi = {"0": 0.20, "30": 0.15}
    stoploss = -0.04
    trailing_stop = True
    trailing_stop_positive = 0.05
    trailing_stop_positive_offset = 0.08
    timeframe = '1m'
    
    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        # Volume explosion detection
        dataframe['volume_ma_10'] = dataframe['volume'].rolling(10).mean()
        dataframe['volume_ma_50'] = dataframe['volume'].rolling(50).mean()
        dataframe['volume_ratio'] = dataframe['volume'] / dataframe['volume_ma_50']
        
        # Price momentum
        dataframe['roc_5'] = ta.ROC(dataframe, timeperiod=5)
        dataframe['roc_10'] = ta.ROC(dataframe, timeperiod=10)
        
        # Volatility
        dataframe['atr'] = ta.ATR(dataframe, timeperiod=14)
        dataframe['atr_ma'] = dataframe['atr'].rolling(20).mean()
        
        # RSI
        dataframe['rsi'] = ta.RSI(dataframe, timeperiod=14)
        
        # MACD
        macd = ta.MACD(dataframe)
        dataframe['macd'] = macd['macd']
        dataframe['macdhist'] = macd['macdhist']
        
        # Bollinger Bands (wider)
        bollinger = qtpylib.bollinger_bands(dataframe['close'], window=20, stds=3)
        dataframe['bb_lower'] = bollinger['lower']
        dataframe['bb_upper'] = bollinger['upper']
        
        # Price acceleration
        dataframe['price_change'] = dataframe['close'].pct_change()
        dataframe['price_accel'] = dataframe['price_change'].diff()
        
        # Detect compression (low volatility before explosion)
        dataframe['bb_width'] = (bollinger['upper'] - bollinger['lower']) / bollinger['mid']
        dataframe['bb_width_ma'] = dataframe['bb_width'].rolling(20).mean()
        dataframe['compression'] = dataframe['bb_width'] < dataframe['bb_width_ma'] * 0.5
        
        return dataframe
    
    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        dataframe.loc[
            (
                # Volume explosion (3x+ normal)
                (dataframe['volume_ratio'] > 3.0) &
                
                # Strong momentum
                (dataframe['roc_5'] > 2.0) &
                
                # Volatility spike
                (dataframe['atr'] > dataframe['atr_ma'] * 1.5) &
                
                # RSI shows strength but not overbought yet
                (dataframe['rsi'] > 50) &
                (dataframe['rsi'] < 80) &
                
                # MACD bullish
                (dataframe['macdhist'] > 0) &
                
                # Price acceleration
                (dataframe['price_accel'] > 0) &
                
                # Breakout from compression (optional but preferred)
                (
                    (dataframe['compression'].shift(5)) |  # Was compressed 5 candles ago
                    (dataframe['volume_ratio'] > 5.0)  # OR extreme volume
                )
            ),
            'enter_long'] = 1
        return dataframe
    
    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        dataframe.loc[
            (
                # Volume dries up
                (dataframe['volume_ratio'] < 1.0) |
                
                # Momentum reversal
                (dataframe['roc_5'] < 0) |
                
                # RSI extreme overbought
                (dataframe['rsi'] > 85) |
                
                # MACD reversal
                (dataframe['macdhist'] < 0) |
                
                # Price deceleration
                (dataframe['price_accel'] < -0.001)
            ),
            'exit_long'] = 1
        return dataframe
