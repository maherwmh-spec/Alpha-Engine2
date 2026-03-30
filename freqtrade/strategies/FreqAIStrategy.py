
"""
# --- FreqAI Master Strategy ---
# هذه هي الاستراتيجية الرئيسية التي يستخدمها FreqAI لاتخاذ القرارات.
# هي لا تحتوي على منطق شراء/بيع تقليدي، بل تعتمد على توقعات النموذج.
"""
from freqtrade.strategy import IStrategy
from freqtrade.freqai.prediction_models.LightGBMRegressor import LightGBMRegressor
from freqtrade.freqai.strategies.IFreqaiStrategy import IFreqaiStrategy
from pandas import DataFrame

class FreqAIStrategy(IFreqaiStrategy):
    # --- إعدادات FreqAI ---
    # اسم النموذج الذي سيتم استخدامه
    freqai_info = {
        "model_name": "LightGBMRegressor",
        "model_path": "freqtrade/freqai/",
    }

    # الإطار الزمني الرئيسي للاستراتيجية
    timeframe = '5m'

    # --- تعريف مساحة الميزات (Features) للنموذج ---
    def define_features(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        # هنا يتم تعريف جميع المؤشرات والميزات التي سيتعلم منها النموذج
        # يمكن إضافة أي مؤشرات من مكتبة TA-Lib أو غيرها
        dataframe['rsi'] = ta.RSI(dataframe)
        dataframe['ema20'] = ta.EMA(dataframe, timeperiod=20)
        dataframe['ema50'] = ta.EMA(dataframe, timeperiod=50)
        dataframe['macd'], dataframe['macdsignal'], dataframe['macdhist'] = ta.MACD(dataframe)
        bollinger = qtpylib.bollinger_bands(qtpylib.typical_price(dataframe), window=20, stds=2)
        dataframe['bb_lowerband'] = bollinger['lower']
        dataframe['bb_middleband'] = bollinger['mid']
        dataframe['bb_upperband'] = bollinger['upper']
        dataframe['atr'] = ta.ATR(dataframe, timeperiod=14)
        dataframe['obv'] = ta.OBV(dataframe)

        return dataframe
