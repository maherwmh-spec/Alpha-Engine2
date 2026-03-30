'''
# --- Price Explosions Strategy ---
# يركز على اكتشاف الانفجارات السعرية المفاجئة المصحوبة بحجم تداول عالٍ

from freqtrade.strategy import IStrategy
from pandas import DataFrame
import talib.abstract as ta

class PriceExplosionsStrategy(IStrategy):
    timeframe = '5m'
    stoploss = -0.25
    minimal_roi = {
        "0": 0.15,
        "30": 0.10,
        "60": 0.05
    }

    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        # Average True Range (ATR) - مقياس للتقلب
        dataframe['atr'] = ta.ATR(dataframe, timeperiod=14)
        # On-Balance Volume (OBV) - يربط السعر بحجم التداول
        dataframe['obv'] = ta.OBV(dataframe)
        return dataframe

    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        dataframe.loc[
            (
                (dataframe['close'] > dataframe['close'].shift(1) + (dataframe['atr'] * 2)) & # حركة سعرية قوية
                (dataframe['volume'] > dataframe['volume'].shift(1) * 3) # زيادة كبيرة في حجم التداول
            ),
            'enter_long'] = 1
        return dataframe

    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        dataframe.loc[
            (
                (dataframe['close'] < dataframe['close'].shift(1) - dataframe['atr'])
            ),
            'exit_long'] = 1
        return dataframe
'''
