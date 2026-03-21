"""
Strategy 3: Medium Waves (الموجات المتوسطة) - COMPLETE
Timeframe: 15m, Target: 8-15%, Duration: 1-2 weeks
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


class MediumWaves(IStrategy):
    INTERFACE_VERSION = 3
    
    minimal_roi = {"0": 0.15, "240": 0.10, "480": 0.08}
    stoploss = -0.03
    trailing_stop = True
    trailing_stop_positive = 0.03
    trailing_stop_positive_offset = 0.05
    timeframe = '15m'
    
    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        dataframe['rsi'] = ta.RSI(dataframe, timeperiod=14)
        macd = ta.MACD(dataframe)
        dataframe['macd'] = macd['macd']
        dataframe['macdsignal'] = macd['macdsignal']
        dataframe['ema_20'] = ta.EMA(dataframe, timeperiod=20)
        dataframe['ema_50'] = ta.EMA(dataframe, timeperiod=50)
        dataframe['sma_200'] = ta.SMA(dataframe, timeperiod=200)
        bollinger = qtpylib.bollinger_bands(dataframe['close'], window=20, stds=2.5)
        dataframe['bb_lower'] = bollinger['lower']
        dataframe['bb_upper'] = bollinger['upper']
        dataframe['bb_mid'] = bollinger['mid']
        dataframe['atr'] = ta.ATR(dataframe, timeperiod=14)
        dataframe['adx'] = ta.ADX(dataframe)
        stoch = ta.STOCH(dataframe)
        dataframe['slowk'] = stoch['slowk']
        dataframe['volume_ma'] = dataframe['volume'].rolling(30).mean()
        return dataframe
    
    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        dataframe.loc[
            (
                (dataframe['rsi'] < 40) &
                (dataframe['close'] > dataframe['sma_200']) &
                (qtpylib.crossed_above(dataframe['ema_20'], dataframe['ema_50'])) &
                (dataframe['macd'] > dataframe['macdsignal']) &
                (dataframe['close'] < dataframe['bb_lower'] * 1.03) &
                (dataframe['volume'] > dataframe['volume_ma'] * 1.5) &
                (dataframe['adx'] > 25) &
                (dataframe['slowk'] < 30)
            ),
            'enter_long'] = 1
        return dataframe
    
    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        dataframe.loc[
            (
                (dataframe['rsi'] > 75) |
                (qtpylib.crossed_below(dataframe['ema_20'], dataframe['ema_50'])) |
                (dataframe['close'] > dataframe['bb_upper'] * 0.97) |
                (dataframe['slowk'] > 80)
            ),
            'exit_long'] = 1
        return dataframe
