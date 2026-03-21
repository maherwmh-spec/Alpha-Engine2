"""
Strategy 2: Short Waves (الموجات القصيرة) - COMPLETE
Timeframe: 5m, Target: 3-8%, Duration: 1-7 days
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
from functools import reduce


class ShortWaves(IStrategy):
    INTERFACE_VERSION = 3
    
    minimal_roi = {"0": 0.08, "60": 0.05, "120": 0.03}
    stoploss = -0.02
    trailing_stop = True
    trailing_stop_positive = 0.02
    trailing_stop_positive_offset = 0.03
    timeframe = '5m'
    
    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        dataframe['rsi'] = ta.RSI(dataframe, timeperiod=14)
        macd = ta.MACD(dataframe)
        dataframe['macd'] = macd['macd']
        dataframe['macdsignal'] = macd['macdsignal']
        dataframe['ema_12'] = ta.EMA(dataframe, timeperiod=12)
        dataframe['ema_26'] = ta.EMA(dataframe, timeperiod=26)
        bollinger = qtpylib.bollinger_bands(dataframe['close'], window=20, stds=2)
        dataframe['bb_lower'] = bollinger['lower']
        dataframe['bb_upper'] = bollinger['upper']
        dataframe['volume_ma'] = dataframe['volume'].rolling(20).mean()
        dataframe['adx'] = ta.ADX(dataframe)
        return dataframe
    
    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        dataframe.loc[
            (
                (dataframe['rsi'] < 35) &
                (qtpylib.crossed_above(dataframe['ema_12'], dataframe['ema_26'])) &
                (dataframe['macd'] > dataframe['macdsignal']) &
                (dataframe['close'] < dataframe['bb_lower'] * 1.02) &
                (dataframe['volume'] > dataframe['volume_ma'] * 1.2) &
                (dataframe['adx'] > 20)
            ),
            'enter_long'] = 1
        return dataframe
    
    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        dataframe.loc[
            (
                (dataframe['rsi'] > 70) |
                (qtpylib.crossed_below(dataframe['ema_12'], dataframe['ema_26'])) |
                (dataframe['close'] > dataframe['bb_upper'] * 0.98)
            ),
            'exit_long'] = 1
        return dataframe
