# -*- coding: utf-8 -*-
"""
# --- FreqAI Master Strategy with Hugging Face Integration ---
# هذه هي الاستراتيجية الرئيسية التي يستخدمها FreqAI لاتخاذ القرارات.
# تدمج "البصمة الجينية" للأسهم ومشاعر الأخبار كميزات إضافية للنموذج.
"""

from freqtrade.freqai.strategies.IFreqaiStrategy import IFreqaiStrategy
from pandas import DataFrame
import talib.abstract as ta
import freqtrade.vendor.qtpylib.indicators as qtpylib
from loguru import logger

# --- استيراد محركات Hugging Face ---
# نفترض أن هذه الملفات موجودة في المسار الصحيح
from user_data.huggingface.genetic_engine import GeneticEmbeddingEngine
from user_data.huggingface.sentiment_engine import SentimentEngine


# --- بيانات وهمية (يجب استبدالها بمصدر بيانات حقيقي) ---
# TODO: يجب ربط هذا بقاعدة بيانات أو API لجلب وصف الشركات الفعلي
COMPANY_PROFILES = {
    '1010': 'Saudi National Bank (SNB), a leading financial institution in Saudi Arabia, offering a wide range of banking services including corporate, retail, and investment banking. Known for its large asset base and significant role in the national economy.',
    '2222': 'Saudi Aramco, one of the world\'s largest integrated energy and chemicals companies. It operates in exploration, production, refining, and marketing of crude oil and related products.',
    '7010': 'Saudi Telecom Company (STC), the leading provider of telecommunications services in Saudi Arabia, offering mobile, internet, and landline services. Expanding into digital payments and cloud services.'
}

# TODO: يجب ربط هذا بمصدر أخبار حقيقي لجلب الأخبار المتعلقة بالسهم
NEWS_HEADLINES = {
    '1010': [
        "SNB posts record profits for the fiscal year, exceeding analyst expectations.",
        "Fitch affirms SNB's 'A-' rating with a stable outlook.",
        "Central bank regulations might slightly impact lending margins next quarter."
    ],
    '2222': [
        "Aramco announces major expansion in downstream projects.",
        "Oil prices drop amid global demand concerns.",
        "Aramco signs new partnership for green hydrogen development."
    ]
}

class FreqAIStrategy(IFreqaiStrategy):
    
    timeframe = '5m'

    def __init__(self, config: dict):
        super().__init__(config)
        # --- تهيئة محركات Hugging Face مرة واحدة ---
        # يتم استدعاء هذا مرة واحدة عند بدء تشغيل الاستراتيجية
        huggingface_config = config.get('huggingface', {})
        self.genetic_engine = GeneticEmbeddingEngine(model_name=huggingface_config.get('genetic_model', 'all-MiniLM-L6-v2'))
        self.sentiment_engine = SentimentEngine(model_name=huggingface_config.get('sentiment_model', 'ProsusAI/finbert'))
        logger.info("Hugging Face engines initialized for FreqAIStrategy.")

    def define_features(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        """
        تعريف الميزات: مؤشرات فنية + بصمة جينية + مشاعر الأخبار
        """
        # -- 1. المؤشرات الفنية (Dynamic Features) --
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

        # -- 2. البصمة الجينية (Static Genetic Features) --
        symbol = metadata['pair'].split('/')[0]
        company_description = COMPANY_PROFILES.get(symbol, "")
        genetic_embedding = self.genetic_engine.get_genetic_embedding(company_description)
        
        # إضافة متجه البصمة الجينية كميزات منفصلة
        for i, feature_val in enumerate(genetic_embedding):
            dataframe[f'genetic_{i}'] = feature_val

        # -- 3. مشاعر الأخبار (Static Sentiment Features) --
        headlines = NEWS_HEADLINES.get(symbol, [])
        sentiment_scores = self.sentiment_engine.analyze_sentiment(headlines)

        dataframe['sentiment_pos'] = sentiment_scores.get('positive', 0.0)
        dataframe['sentiment_neg'] = sentiment_scores.get('negative', 0.0)
        dataframe['sentiment_neu'] = sentiment_scores.get('neutral', 0.0)

        logger.debug(f"Added {len(genetic_embedding)} genetic and 3 sentiment features for {symbol}")

        return dataframe
