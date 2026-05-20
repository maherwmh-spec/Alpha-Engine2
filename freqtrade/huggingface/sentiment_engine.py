# -*- coding: utf-8 -*-
"""
SentimentEngine: محرك تحليل مشاعر الأخبار المالية.

Uses FinBERT when the runtime has transformers/torch and falls back to a small,
conservative lexical scorer when the model is unavailable. The fallback prevents
FreqAI feature generation from failing or becoming dependent on static data.
"""
from __future__ import annotations

from typing import Dict, List

from loguru import logger

try:
    import torch
    from transformers import AutoModelForSequenceClassification, AutoTokenizer

    TRANSFORMERS_AVAILABLE = True
except Exception:
    torch = None
    AutoTokenizer = None
    AutoModelForSequenceClassification = None
    TRANSFORMERS_AVAILABLE = False


POSITIVE_TERMS = {
    "profit", "growth", "record", "upgrade", "expansion", "partnership", "beats", "surge",
    "positive", "strong", "stable", "improves", "adoption", "award", "contract",
}
NEGATIVE_TERMS = {
    "loss", "drop", "decline", "downgrade", "concern", "weak", "negative", "lawsuit", "risk",
    "falls", "debt", "delay", "cuts", "regulatory", "warning", "misses",
}


class SentimentEngine:
    def __init__(self, model_name: str = "ProsusAI/finbert"):
        self.tokenizer = None
        self.model = None
        self.model_name = model_name
        if TRANSFORMERS_AVAILABLE:
            try:
                self.tokenizer = AutoTokenizer.from_pretrained(model_name)
                self.model = AutoModelForSequenceClassification.from_pretrained(model_name)
                logger.success("sentiment_engine_initialized model={}", model_name)
            except Exception as exc:
                logger.warning("sentiment_engine_model_load_failed model={} error={}; using lexical fallback", model_name, exc)
        else:
            logger.warning("sentiment_engine_transformers_unavailable; using lexical fallback")

    def _fallback_sentiment(self, headlines: List[str]) -> Dict[str, float]:
        if not headlines:
            return {"positive": 0.0, "negative": 0.0, "neutral": 1.0}
        positive = 0
        negative = 0
        for headline in headlines:
            words = {word.strip(".,:;!?()[]{}'\"").lower() for word in headline.split()}
            positive += len(words & POSITIVE_TERMS)
            negative += len(words & NEGATIVE_TERMS)
        total = positive + negative
        if total == 0:
            return {"positive": 0.0, "negative": 0.0, "neutral": 1.0}
        pos_score = positive / (total + len(headlines))
        neg_score = negative / (total + len(headlines))
        neutral = max(0.0, 1.0 - pos_score - neg_score)
        return {"positive": float(pos_score), "negative": float(neg_score), "neutral": float(neutral)}

    def analyze_sentiment(self, headlines: List[str]) -> Dict[str, float]:
        """يحلل قائمة من عناوين الأخبار ويرجع متوسط درجات المشاعر."""
        clean_headlines = [str(item).strip() for item in headlines if str(item).strip()]
        if not clean_headlines:
            return {"positive": 0.0, "negative": 0.0, "neutral": 1.0}

        if not self.model or not self.tokenizer or torch is None:
            return self._fallback_sentiment(clean_headlines)

        try:
            inputs = self.tokenizer(clean_headlines, padding=True, truncation=True, return_tensors="pt", max_length=512)
            with torch.no_grad():
                outputs = self.model(**inputs)
                predictions = torch.nn.functional.softmax(outputs.logits, dim=-1)
            mean_scores = predictions.mean(dim=0)
            return {
                "positive": float(mean_scores[0].item()),
                "negative": float(mean_scores[1].item()),
                "neutral": float(mean_scores[2].item()),
            }
        except Exception as exc:
            logger.warning("sentiment_analysis_failed error={}; using lexical fallback", exc)
            return self._fallback_sentiment(clean_headlines)
