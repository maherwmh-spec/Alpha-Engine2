'''
# --- Aggressive Daily Strategy ---
# يركز على التداولات اليومية السريعة بهدف ربح صغير (1-3%)

from freqtrade.strategy import IStrategy
from pandas import DataFrame
import talib.abstract as ta
import freqtrade.vendor.qtpylib.indicators as qtpylib

class AggressiveDailyStrategy(IStrategy):
    # --- معلمات الاستراتيجية ---
    timeframe = '1m'
    stoploss = -0.10
    minimal_roi = {
        "0": 0.03,  # 3% ربح
        "10": 0.02,
        "30": 0.01,
        "60": 0
    }

    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        # --- مؤشرات فنية ---
        dataframe['rsi'] = ta.RSI(dataframe)
        dataframe['ema20'] = ta.EMA(dataframe, timeperiod=20)
        dataframe['ema50'] = ta.EMA(dataframe, timeperiod=50)
        return dataframe

    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        # --- شروط الدخول ---
        dataframe.loc[
            (
                (dataframe['rsi'] < 30) &  # RSI منخفض
                (qtpylib.crossed_above(dataframe['ema20'], dataframe['ema50'])) # تقاطع إيجابي
            ),
            'enter_long'] = 1
        return dataframe

    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        # --- شروط الخروج ---
        dataframe.loc[
            (
                (dataframe['rsi'] > 70) # RSI مرتفع
            ),
            'exit_long'] = 1
        return dataframe
'''
