# -*- coding: utf-8 -*-
"""
FreqAI Master Strategy with Hugging Face Integration
الاستراتيجية الرئيسية التي تدمج البصمة الجينية للأسهم ومشاعر الأخبار كميزات للنموذج.
"""

import sys
import os
import logging
from pandas import DataFrame
from functools import reduce

# --- الاستيراد الصحيح لـ Freqtrade 2026.x ---
from freqtrade.strategy.interface import IStrategy
from freqtrade.strategy import DecimalParameter, IntParameter

logger = logging.getLogger(__name__)

# --- إضافة مسار huggingface إلى sys.path ---
_strategy_dir = os.path.dirname(os.path.abspath(__file__))
_user_data_dir = os.path.dirname(_strategy_dir)
if _user_data_dir not in sys.path:
    sys.path.insert(0, _user_data_dir)

# --- استيراد محركات Hugging Face (مع معالجة حالة عدم توفر المكتبات) ---
try:
    from huggingface.genetic_engine import GeneticEmbeddingEngine
    from huggingface.sentiment_engine import SentimentEngine
    HF_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Hugging Face engines not available: {e}. Running without HF features.")
    HF_AVAILABLE = False

# --- بيانات وصف الشركات (يجب ربطها بقاعدة بيانات لاحقاً) ---
COMPANY_PROFILES = {
    '1010': 'Saudi National Bank (SNB), a leading financial institution in Saudi Arabia, '
            'offering corporate, retail, and investment banking services.',
    '2222': 'Saudi Aramco, one of the world largest integrated energy and chemicals companies. '
            'Operates in exploration, production, refining, and marketing of crude oil.',
    '7010': 'Saudi Telecom Company (STC), the leading provider of telecommunications services '
            'in Saudi Arabia, offering mobile, internet, and digital services.',
    'BTC':  'Bitcoin, the first and largest cryptocurrency by market capitalization, '
            'used as a decentralized digital currency and store of value.',
    'ETH':  'Ethereum, a decentralized blockchain platform featuring smart contract functionality, '
            'the second largest cryptocurrency by market capitalization.'
}

NEWS_HEADLINES = {
    '1010': [
        "SNB posts record profits for the fiscal year, exceeding analyst expectations.",
        "Fitch affirms SNB's 'A-' rating with a stable outlook."
    ],
    '2222': [
        "Aramco announces major expansion in downstream projects.",
        "Aramco signs new partnership for green hydrogen development."
    ],
    'BTC': [
        "Bitcoin reaches new all-time high amid institutional adoption.",
        "Regulatory clarity boosts crypto market sentiment."
    ],
    'ETH': [
        "Ethereum upgrade improves network scalability and reduces gas fees.",
        "DeFi activity surges on Ethereum network."
    ]
}


class FreqAIStrategy(IStrategy):
    """
    استراتيجية FreqAI الرئيسية مع دمج Hugging Face.
    تستخدم مؤشرات فنية + بصمة جينية + مشاعر الأخبار كميزات للنموذج.
    """

    # --- إعدادات الاستراتيجية ---
    INTERFACE_VERSION = 3
    timeframe = '5m'
    can_short = False
    minimal_roi = {"60": 0.01, "30": 0.02, "0": 0.04}
    stoploss = -0.10
    trailing_stop = False
    process_only_new_candles = True
    use_exit_signal = True
    exit_profit_only = False
    ignore_roi_if_entry_signal = False
    startup_candle_count = 50

    # --- معاملات قابلة للتحسين ---
    buy_rsi = IntParameter(20, 40, default=30, space="buy")
    sell_rsi = IntParameter(60, 80, default=70, space="sell")

    def __init__(self, config: dict):
        super().__init__(config)
        self.genetic_engine = None
        self.sentiment_engine = None

        if HF_AVAILABLE:
            try:
                hf_config = config.get('huggingface', {})
                genetic_model = hf_config.get('genetic_model', 'all-MiniLM-L6-v2')
                sentiment_model = hf_config.get('sentiment_model', 'ProsusAI/finbert')
                self.genetic_engine = GeneticEmbeddingEngine(model_name=genetic_model)
                self.sentiment_engine = SentimentEngine(model_name=sentiment_model)
                logger.info(f"✅ GeneticEmbeddingEngine initialized: {genetic_model}")
                logger.info(f"✅ SentimentEngine initialized: {sentiment_model}")
            except Exception as e:
                logger.warning(f"Could not initialize HF engines: {e}")

    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        """
        حساب جميع المؤشرات الفنية والميزات الجينية والمشاعرية.
        """
        # -- 1. المؤشرات الفنية --
        dataframe['rsi'] = self._rsi(dataframe)
        dataframe['ema20'] = dataframe['close'].ewm(span=20).mean()
        dataframe['ema50'] = dataframe['close'].ewm(span=50).mean()
        dataframe['volume_ma'] = dataframe['volume'].rolling(20).mean()

        # MACD
        ema12 = dataframe['close'].ewm(span=12).mean()
        ema26 = dataframe['close'].ewm(span=26).mean()
        dataframe['macd'] = ema12 - ema26
        dataframe['macd_signal'] = dataframe['macd'].ewm(span=9).mean()
        dataframe['macd_hist'] = dataframe['macd'] - dataframe['macd_signal']

        # Bollinger Bands
        bb_mid = dataframe['close'].rolling(20).mean()
        bb_std = dataframe['close'].rolling(20).std()
        dataframe['bb_upper'] = bb_mid + (bb_std * 2)
        dataframe['bb_lower'] = bb_mid - (bb_std * 2)
        dataframe['bb_mid'] = bb_mid

        # ATR
        high_low = dataframe['high'] - dataframe['low']
        high_close = (dataframe['high'] - dataframe['close'].shift()).abs()
        low_close = (dataframe['low'] - dataframe['close'].shift()).abs()
        tr = high_low.combine(high_close, max).combine(low_close, max)
        dataframe['atr'] = tr.rolling(14).mean()

        # -- 2. البصمة الجينية (Genetic Embedding) --
        symbol = metadata['pair'].split('/')[0]
        if self.genetic_engine is not None:
            company_desc = COMPANY_PROFILES.get(symbol, f"Stock symbol {symbol}")
            try:
                embedding = self.genetic_engine.get_genetic_embedding(company_desc)
                for i, val in enumerate(embedding):
                    dataframe[f'genetic_{i}'] = float(val)
                logger.debug(f"Added {len(embedding)} genetic features for {symbol}")
            except Exception as e:
                logger.warning(f"Genetic embedding failed for {symbol}: {e}")
        else:
            # ميزات جينية افتراضية عند عدم توفر المحرك
            dataframe['genetic_0'] = 0.0
            dataframe['genetic_1'] = 0.0

        # -- 3. مشاعر الأخبار (Sentiment) --
        if self.sentiment_engine is not None:
            headlines = NEWS_HEADLINES.get(symbol, [])
            if headlines:
                try:
                    scores = self.sentiment_engine.analyze_sentiment(headlines)
                    dataframe['sentiment_pos'] = scores.get('positive', 0.0)
                    dataframe['sentiment_neg'] = scores.get('negative', 0.0)
                    dataframe['sentiment_neu'] = scores.get('neutral', 1.0)
                except Exception as e:
                    logger.warning(f"Sentiment analysis failed for {symbol}: {e}")
                    dataframe['sentiment_pos'] = 0.0
                    dataframe['sentiment_neg'] = 0.0
                    dataframe['sentiment_neu'] = 1.0
            else:
                dataframe['sentiment_pos'] = 0.0
                dataframe['sentiment_neg'] = 0.0
                dataframe['sentiment_neu'] = 1.0
        else:
            dataframe['sentiment_pos'] = 0.0
            dataframe['sentiment_neg'] = 0.0
            dataframe['sentiment_neu'] = 1.0

        return dataframe

    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        """شروط الدخول: RSI منخفض + MACD إيجابي + مشاعر إيجابية"""
        conditions = [
            dataframe['rsi'] < self.buy_rsi.value,
            dataframe['macd'] > dataframe['macd_signal'],
            dataframe['close'] > dataframe['ema20'],
            dataframe['volume'] > dataframe['volume_ma'] * 0.8,
            dataframe['sentiment_neg'] < 0.5,
        ]
        dataframe.loc[reduce(lambda x, y: x & y, conditions), 'enter_long'] = 1
        return dataframe

    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        """شروط الخروج: RSI مرتفع + MACD سلبي"""
        conditions = [
            dataframe['rsi'] > self.sell_rsi.value,
            dataframe['macd'] < dataframe['macd_signal'],
        ]
        dataframe.loc[reduce(lambda x, y: x & y, conditions), 'exit_long'] = 1
        return dataframe

    @staticmethod
    def _rsi(dataframe: DataFrame, period: int = 14) -> 'pd.Series':
        """حساب مؤشر RSI يدوياً بدون talib"""
        import pandas as pd
        delta = dataframe['close'].diff()
        gain = delta.where(delta > 0, 0.0).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0.0)).rolling(window=period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))
