'''
# --- Medium Waves Strategy ---
# يركز على التقاط الموجات السعرية المتوسطة الأجل

from freqtrade.strategy import IStrategy
from pandas import DataFrame
import talib.abstract as ta

class MediumWavesStrategy(IStrategy):
    timeframe = '15m'
    stoploss = -0.20
    minimal_roi = {
        "0": 0.10,
        "60": 0.07,
        "120": 0.03
    }

    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        # MACD
        macd = ta.MACD(dataframe)
        dataframe['macd'] = macd['macd']
        dataframe['macdsignal'] = macd['macdsignal']
        dataframe['macdhist'] = macd['macdhist']
        return dataframe

    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        dataframe.loc[
            (
                (dataframe['macd'] > dataframe['macdsignal']) &
                (dataframe['macdhist'] > 0)
            ),
            'enter_long'] = 1
        return dataframe

    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        dataframe.loc[
            (
                (dataframe['macd'] < dataframe['macdsignal'])
            ),
            'exit_long'] = 1
        return dataframe
'''
