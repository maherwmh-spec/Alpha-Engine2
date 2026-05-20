# -*- coding: utf-8 -*-
"""
FreqAI Master Strategy with Hugging Face Integration.
الاستراتيجية الرئيسية التي تدمج البصمة الجينية للأسهم ومشاعر الأخبار كميزات للنموذج.
"""
from __future__ import annotations

import logging
import os
import sys
from functools import reduce

from pandas import DataFrame

from freqtrade.strategy import IntParameter
from freqtrade.strategy.interface import IStrategy

logger = logging.getLogger(__name__)

# --- إضافة مسار huggingface إلى sys.path ---
_strategy_dir = os.path.dirname(os.path.abspath(__file__))
_user_data_dir = os.path.dirname(_strategy_dir)
if _user_data_dir not in sys.path:
    sys.path.insert(0, _user_data_dir)

try:
    from huggingface.data_provider import HFDataProvider
    from huggingface.genetic_engine import GeneticEmbeddingEngine
    from huggingface.sentiment_engine import SentimentEngine

    HF_AVAILABLE = True
except ImportError as exc:
    logger.warning("Hugging Face engines not available: %s. Running without HF features.", exc)
    HF_AVAILABLE = False


class FreqAIStrategy(IStrategy):
    """استراتيجية FreqAI الرئيسية مع دمج Hugging Face ببيانات ديناميكية وفولباك آمن."""

    INTERFACE_VERSION = 3
    timeframe = "5m"
    can_short = False
    minimal_roi = {"60": 0.01, "30": 0.02, "0": 0.04}
    stoploss = -0.10
    trailing_stop = False
    process_only_new_candles = True
    use_exit_signal = True
    exit_profit_only = False
    ignore_roi_if_entry_signal = False
    startup_candle_count = 50

    buy_rsi = IntParameter(20, 40, default=30, space="buy")
    sell_rsi = IntParameter(60, 80, default=70, space="sell")

    def __init__(self, config: dict):
        super().__init__(config)
        self.genetic_engine = None
        self.sentiment_engine = None
        self.hf_data_provider = None

        if HF_AVAILABLE:
            try:
                hf_config = config.get("huggingface", {})
                genetic_model = hf_config.get("genetic_model", os.getenv("HF_GENETIC_MODEL", "all-MiniLM-L6-v2"))
                sentiment_model = hf_config.get("sentiment_model", os.getenv("HF_SENTIMENT_MODEL", "ProsusAI/finbert"))
                cache_dir = hf_config.get("cache_dir", os.getenv("HF_CACHE_DIR", "/tmp/alpha_hf_cache"))
                self.hf_data_provider = HFDataProvider(cache_dir=cache_dir)
                self.genetic_engine = GeneticEmbeddingEngine(model_name=genetic_model, cache_dir=cache_dir)
                self.sentiment_engine = SentimentEngine(model_name=sentiment_model)
                logger.info("HF integration initialized genetic_model=%s sentiment_model=%s cache_dir=%s", genetic_model, sentiment_model, cache_dir)
            except Exception as exc:
                logger.warning("Could not initialize HF integration: %s", exc)

    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        """حساب جميع المؤشرات الفنية والميزات الجينية والمشاعرية."""
        dataframe["rsi"] = self._rsi(dataframe)
        dataframe["ema20"] = dataframe["close"].ewm(span=20).mean()
        dataframe["ema50"] = dataframe["close"].ewm(span=50).mean()
        dataframe["volume_ma"] = dataframe["volume"].rolling(20).mean()

        ema12 = dataframe["close"].ewm(span=12).mean()
        ema26 = dataframe["close"].ewm(span=26).mean()
        dataframe["macd"] = ema12 - ema26
        dataframe["macd_signal"] = dataframe["macd"].ewm(span=9).mean()
        dataframe["macd_hist"] = dataframe["macd"] - dataframe["macd_signal"]

        bb_mid = dataframe["close"].rolling(20).mean()
        bb_std = dataframe["close"].rolling(20).std()
        dataframe["bb_upper"] = bb_mid + (bb_std * 2)
        dataframe["bb_lower"] = bb_mid - (bb_std * 2)
        dataframe["bb_mid"] = bb_mid

        high_low = dataframe["high"] - dataframe["low"]
        high_close = (dataframe["high"] - dataframe["close"].shift()).abs()
        low_close = (dataframe["low"] - dataframe["close"].shift()).abs()
        tr = high_low.combine(high_close, max).combine(low_close, max)
        dataframe["atr"] = tr.rolling(14).mean()

        pair = metadata.get("pair", "UNKNOWN/UNKNOWN")
        symbol = pair.split("/")[0].upper()
        company_desc = None
        if self.hf_data_provider is not None:
            company_desc = self.hf_data_provider.get_company_profile(symbol, pair=pair)

        if self.genetic_engine is not None:
            try:
                embedding = self.genetic_engine.get_genetic_embedding(company_desc or f"Stock symbol {symbol}", symbol=symbol)
                for i, val in enumerate(embedding):
                    dataframe[f"genetic_{i}"] = float(val)
                logger.debug("Added %s genetic features for %s", len(embedding), symbol)
            except Exception as exc:
                logger.warning("Genetic embedding failed for %s: %s", symbol, exc)
                dataframe["genetic_0"] = 0.0
                dataframe["genetic_1"] = 0.0
        else:
            dataframe["genetic_0"] = 0.0
            dataframe["genetic_1"] = 0.0

        if self.sentiment_engine is not None:
            try:
                headlines = []
                if self.hf_data_provider is not None:
                    headlines = self.hf_data_provider.fetch_news_headlines(symbol, company_profile=company_desc)
                scores = self.sentiment_engine.analyze_sentiment(headlines)
                dataframe["sentiment_pos"] = scores.get("positive", 0.0)
                dataframe["sentiment_neg"] = scores.get("negative", 0.0)
                dataframe["sentiment_neu"] = scores.get("neutral", 1.0)
                dataframe["hf_news_count"] = len(headlines)
            except Exception as exc:
                logger.warning("Sentiment analysis failed for %s: %s", symbol, exc)
                dataframe["sentiment_pos"] = 0.0
                dataframe["sentiment_neg"] = 0.0
                dataframe["sentiment_neu"] = 1.0
                dataframe["hf_news_count"] = 0
        else:
            dataframe["sentiment_pos"] = 0.0
            dataframe["sentiment_neg"] = 0.0
            dataframe["sentiment_neu"] = 1.0
            dataframe["hf_news_count"] = 0

        return dataframe

    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        """شروط الدخول: RSI منخفض + MACD إيجابي + مشاعر غير سلبية."""
        conditions = [
            dataframe["rsi"] < self.buy_rsi.value,
            dataframe["macd"] > dataframe["macd_signal"],
            dataframe["close"] > dataframe["ema20"],
            dataframe["volume"] > dataframe["volume_ma"] * 0.8,
            dataframe["sentiment_neg"] < 0.5,
        ]
        dataframe.loc[reduce(lambda x, y: x & y, conditions), "enter_long"] = 1
        return dataframe

    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        """شروط الخروج: RSI مرتفع + MACD سلبي."""
        conditions = [
            dataframe["rsi"] > self.sell_rsi.value,
            dataframe["macd"] < dataframe["macd_signal"],
        ]
        dataframe.loc[reduce(lambda x, y: x & y, conditions), "exit_long"] = 1
        return dataframe

    @staticmethod
    def _rsi(dataframe: DataFrame, period: int = 14):
        """حساب مؤشر RSI يدوياً بدون talib."""
        delta = dataframe["close"].diff()
        gain = delta.where(delta > 0, 0.0).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0.0)).rolling(window=period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))
