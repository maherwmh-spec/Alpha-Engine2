"""
Bot: Sentiment Analyzer (Phase 3)
────────────────────────────────
Unified nightly sentiment per symbol.
Uses FinBERT when available; no_news / model_unavailable statuses otherwise.
v1: compute + store only.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from loguru import logger
from sqlalchemy import text

from config.config_manager import config
from scripts.database import db
from scripts.redis_manager import redis_manager


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class SentimentAnalyzerBot:
    """Nightly sentiment aggregation per symbol."""

    def __init__(self):
        self.name = "sentiment_analyzer"
        self.logger = logger.bind(bot=self.name)
        self.model_name = str(
            config.get("bots.sentiment_analyzer.model_name", "ProsusAI/finbert")
        )
        self.max_articles = int(
            config.get("bots.sentiment_analyzer.max_articles", 8)
        )
        self._model = None
        self._tokenizer = None
        self._model_status = "unloaded"  # unloaded | ready | unavailable

    def run(
        self,
        symbols: Optional[List[str]] = None,
        max_symbols: Optional[int] = None,
    ) -> Dict[str, Any]:
        symbols = symbols or self._list_symbols()
        if max_symbols:
            symbols = symbols[: int(max_symbols)]

        self._ensure_model()
        self.logger.info(
            f"SentimentAnalyzer start: {len(symbols)} symbols model={self.model_name} "
            f"status={self._model_status}"
        )

        stats = {
            "ok": 0,
            "no_news": 0,
            "model_unavailable": 0,
            "errors": 0,
            "total": len(symbols),
        }

        for symbol in symbols:
            try:
                row = self.compute(symbol)
                self._upsert(row)
                st = row.get("status")
                if st == "ok":
                    stats["ok"] += 1
                elif st == "no_news":
                    stats["no_news"] += 1
                elif st == "model_unavailable":
                    stats["model_unavailable"] += 1
                else:
                    stats["errors"] += 1
            except Exception as exc:
                stats["errors"] += 1
                self.logger.error(f"{symbol}: sentiment failed: {exc}")
                try:
                    self._upsert(
                        {
                            "symbol": symbol,
                            "status": "error",
                            "model_name": self.model_name,
                            "articles_count": 0,
                            "computed_at": _utcnow(),
                            "updated_at": _utcnow(),
                        }
                    )
                except Exception:
                    pass

        self.logger.success(f"SentimentAnalyzer complete: {stats}")
        return stats

    def compute(self, symbol: str) -> Dict[str, Any]:
        now = _utcnow()
        articles = self._fetch_news(symbol, limit=self.max_articles)

        if not articles:
            return {
                "symbol": symbol,
                "sentiment_label": "neutral",
                "sentiment_score": 0.0,
                "score_positive": 0.0,
                "score_neutral": 1.0,
                "score_negative": 0.0,
                "articles_count": 0,
                "model_name": self.model_name,
                "status": "no_news",
                "computed_at": now,
                "updated_at": now,
            }

        if self._model_status != "ready":
            # Store neutral placeholder so pipeline does not break
            return {
                "symbol": symbol,
                "sentiment_label": "neutral",
                "sentiment_score": 0.0,
                "score_positive": 0.0,
                "score_neutral": 1.0,
                "score_negative": 0.0,
                "articles_count": len(articles),
                "model_name": self.model_name,
                "status": "model_unavailable",
                "computed_at": now,
                "updated_at": now,
            }

        scores = []
        for text_body in articles:
            s = self._score_text(text_body)
            if s is not None:
                scores.append(s)

        if not scores:
            return {
                "symbol": symbol,
                "sentiment_label": "neutral",
                "sentiment_score": 0.0,
                "score_positive": 0.0,
                "score_neutral": 1.0,
                "score_negative": 0.0,
                "articles_count": len(articles),
                "model_name": self.model_name,
                "status": "error",
                "computed_at": now,
                "updated_at": now,
            }

        pos = float(np.mean([s["positive"] for s in scores]))
        neu = float(np.mean([s["neutral"] for s in scores]))
        neg = float(np.mean([s["negative"] for s in scores]))
        label_map = {"positive": pos, "neutral": neu, "negative": neg}
        label = max(label_map, key=label_map.get)
        conf = label_map[label]

        return {
            "symbol": symbol,
            "sentiment_label": label,
            "sentiment_score": round(conf, 6),
            "score_positive": round(pos, 6),
            "score_neutral": round(neu, 6),
            "score_negative": round(neg, 6),
            "articles_count": len(articles),
            "model_name": self.model_name,
            "status": "ok",
            "computed_at": now,
            "updated_at": now,
        }

    def _ensure_model(self) -> None:
        if self._model_status in ("ready", "unavailable"):
            return
        try:
            import torch
            from transformers import AutoModelForSequenceClassification, AutoTokenizer

            self.logger.info(f"Loading sentiment model: {self.model_name}")
            self._tokenizer = AutoTokenizer.from_pretrained(self.model_name)
            self._model = AutoModelForSequenceClassification.from_pretrained(self.model_name)
            self._model.eval()
            if torch.cuda.is_available():
                self._model = self._model.cuda()
                self.logger.info("Sentiment model on GPU")
            else:
                self.logger.info("Sentiment model on CPU")
            self._model_status = "ready"
        except Exception as exc:
            self.logger.warning(f"Sentiment model unavailable: {exc}")
            self._model = None
            self._tokenizer = None
            self._model_status = "unavailable"

    def _score_text(self, text_body: str) -> Optional[Dict[str, float]]:
        if not text_body or not self._model or not self._tokenizer:
            return None
        try:
            import torch

            inputs = self._tokenizer(
                text_body[:512],
                return_tensors="pt",
                truncation=True,
                max_length=512,
            )
            if torch.cuda.is_available():
                inputs = {k: v.cuda() for k, v in inputs.items()}
            with torch.no_grad():
                outputs = self._model(**inputs)
            probs = torch.nn.functional.softmax(outputs.logits, dim=-1)[0].cpu().numpy()
            # FinBERT: [negative, neutral, positive]
            return {
                "negative": float(probs[0]),
                "neutral": float(probs[1]),
                "positive": float(probs[2]),
            }
        except Exception as exc:
            self.logger.debug(f"score_text failed: {exc}")
            return None

    def _fetch_news(self, symbol: str, limit: int = 8) -> List[str]:
        texts: List[str] = []
        try:
            with db.get_session() as session:
                rows = session.execute(
                    text(
                        """
                        SELECT title, content
                        FROM market_data.news
                        WHERE symbol = :symbol
                        ORDER BY published_at DESC NULLS LAST, created_at DESC
                        LIMIT :limit
                        """
                    ),
                    {"symbol": symbol, "limit": limit},
                ).fetchall()
            for title, content in rows or []:
                chunk = f"{(title or '').strip()} {(content or '')[:300]}".strip()
                if chunk:
                    texts.append(chunk)
        except Exception as exc:
            self.logger.debug(f"news fetch {symbol}: {exc}")
        return texts

    def _list_symbols(self) -> List[str]:
        try:
            cached = redis_manager.get("sahmk:symbols_list")
            if cached and isinstance(cached, list):
                return [str(s) for s in cached if str(s)]
        except Exception:
            pass
        try:
            with db.get_session() as session:
                rows = session.execute(
                    text(
                        """
                        SELECT symbol FROM market_data.symbols
                        WHERE is_active = TRUE
                        ORDER BY symbol
                        """
                    )
                ).fetchall()
            if rows:
                return [str(r[0]) for r in rows]
        except Exception as exc:
            self.logger.warning(f"symbols list failed: {exc}")
        return ["1120", "2010", "2222", "1010", "1180"]

    def _upsert(self, row: Dict[str, Any]) -> None:
        cols = [
            "symbol",
            "sentiment_label",
            "sentiment_score",
            "score_positive",
            "score_neutral",
            "score_negative",
            "articles_count",
            "model_name",
            "status",
            "computed_at",
            "updated_at",
        ]
        data = {c: row.get(c) for c in cols}
        col_list = ", ".join(cols)
        placeholders = ", ".join(f":{c}" for c in cols)
        updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != "symbol")
        sql = f"""
            INSERT INTO market_data.news_sentiments ({col_list})
            VALUES ({placeholders})
            ON CONFLICT (symbol) DO UPDATE SET {updates}
        """
        with db.get_session() as session:
            session.execute(text(sql), data)
            session.commit()


if __name__ == "__main__":
    print(SentimentAnalyzerBot().run(max_symbols=5))
