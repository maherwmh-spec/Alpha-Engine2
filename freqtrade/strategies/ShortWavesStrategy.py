'''
# --- Short Waves Strategy ---
# يركز على التقاط الموجات السعرية القصيرة

from freqtrade.strategy import IStrategy
from pandas import DataFrame
import talib.abstract as ta
import freqtrade.vendor.qtpylib.indicators as qtpylib

class ShortWavesStrategy(IStrategy):
    timeframe = '5m'
    stoploss = -0.15
    minimal_roi = {
        "0": 0.05,
        "30": 0.03,
        "60": 0.01
    }

    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        # Bollinger Bands
        bollinger = qtpylib.bollinger_bands(qtpylib.typical_price(dataframe), window=20, stds=2)
        dataframe['bb_lowerband'] = bollinger['lower']
        dataframe['bb_middleband'] = bollinger['mid']
        dataframe['bb_upperband'] = bollinger['upper']
        return dataframe

    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        dataframe.loc[
            (
                (dataframe['close'] < dataframe['bb_lowerband'])  # السعر تحت البولينجر السفلي
            ),
            'enter_long'] = 1
        return dataframe

    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        dataframe.loc[
            (
                (dataframe['close'] > dataframe['bb_upperband']) # السعر فوق البولينجر العلوي
            ),
            'exit_long'] = 1
        return dataframe
'''
