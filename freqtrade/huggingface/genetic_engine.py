# -*- coding: utf-8 -*-
"""
GeneticEmbeddingEngine: محرك توليد "البصمة الجينية" للأسهم.

Production behavior:
- Uses SentenceTransformer when available.
- Stores/reuses embeddings in a local JSON cache to reduce repeated model calls.
- Falls back to zero vectors safely when the model or source text is unavailable.
"""
from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)

try:
    from sentence_transformers import SentenceTransformer
    ST_AVAILABLE = True
except ImportError:
    ST_AVAILABLE = False
    logger.warning("sentence_transformers not installed. GeneticEmbeddingEngine will return zero vectors.")


class GeneticEmbeddingEngine:
    """محرك البصمة الجينية: يحوّل وصف الشركة النصي إلى متجه رقمي."""

    def __init__(self, model_name: str = "all-MiniLM-L6-v2", cache_dir: Optional[str] = None):
        self.model = None
        self.model_name = model_name
        self.embedding_size = 384
        self.cache_dir = Path(cache_dir or os.getenv("HF_EMBEDDING_CACHE_DIR", os.getenv("HF_CACHE_DIR", "/tmp/alpha_hf_cache")))
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.cache_ttl_seconds = int(os.getenv("HF_EMBEDDING_CACHE_TTL_SECONDS", "604800"))

        if ST_AVAILABLE:
            try:
                self.model = SentenceTransformer(model_name)
                dimension = getattr(self.model, "get_sentence_embedding_dimension", lambda: self.embedding_size)()
                if dimension:
                    self.embedding_size = int(dimension)
                logger.info("GeneticEmbeddingEngine initialized with model: %s", model_name)
            except Exception as exc:
                logger.error("Failed to load SentenceTransformer model '%s': %s", model_name, exc)
        else:
            logger.warning("GeneticEmbeddingEngine: sentence_transformers unavailable, using zero vectors.")

    def _cache_path(self, symbol: str) -> Path:
        safe_symbol = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in (symbol or "unknown"))
        safe_model = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in self.model_name)
        return self.cache_dir / f"embedding_{safe_symbol}_{safe_model}.json"

    def _read_cached_embedding(self, symbol: str, description: str) -> Optional[List[float]]:
        path = self._cache_path(symbol)
        try:
            if not path.exists():
                return None
            with path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
            if payload.get("description_hash") != hash(description):
                return None
            if time.time() - float(payload.get("created_at", 0)) > self.cache_ttl_seconds:
                return None
            embedding = payload.get("embedding")
            if isinstance(embedding, list) and embedding:
                return [float(item) for item in embedding]
        except Exception as exc:
            logger.warning("Embedding cache read failed for %s: %s", symbol, exc)
        return None

    def _write_cached_embedding(self, symbol: str, description: str, embedding: List[float]) -> None:
        try:
            with self._cache_path(symbol).open("w", encoding="utf-8") as handle:
                json.dump({
                    "created_at": time.time(),
                    "model_name": self.model_name,
                    "description_hash": hash(description),
                    "embedding": embedding,
                }, handle, ensure_ascii=False)
        except Exception as exc:
            logger.warning("Embedding cache write failed for %s: %s", symbol, exc)

    def get_genetic_embedding(self, description: str, symbol: Optional[str] = None) -> List[float]:
        """يولد متجه البصمة الجينية من الوصف النصي للشركة."""
        if not description:
            return [0.0] * self.embedding_size

        cache_symbol = symbol or "generic"
        cached = self._read_cached_embedding(cache_symbol, description)
        if cached is not None:
            return cached

        if not self.model:
            return [0.0] * self.embedding_size

        try:
            embedding = self.model.encode(description, convert_to_tensor=False)
            values = embedding.tolist() if hasattr(embedding, "tolist") else list(embedding)
            values = [float(item) for item in values]
            self._write_cached_embedding(cache_symbol, description, values)
            return values
        except Exception as exc:
            logger.error("Error generating embedding: %s", exc)
            return [0.0] * self.embedding_size

    def compare_companies(self, desc_a: str, desc_b: str) -> float:
        """يحسب درجة التشابه الجيني بين شركتين."""
        if not self.model:
            return 0.0
        try:
            embeddings = self.model.encode([desc_a, desc_b], convert_to_tensor=False)
            import numpy as np

            a, b = embeddings[0], embeddings[1]
            denominator = np.linalg.norm(a) * np.linalg.norm(b)
            if denominator == 0:
                return 0.0
            similarity = float(np.dot(a, b) / denominator)
            return max(0.0, min(1.0, similarity))
        except Exception as exc:
            logger.error("Error comparing companies: %s", exc)
            return 0.0
