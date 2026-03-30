# -*- coding: utf-8 -*-
"""
GeneticEmbeddingEngine: محرك توليد "البصمة الجينية" للأسهم

يستخدم هذا المحرك نماذج Hugging Face لتحويل الوصف النصي لشركة ما
إلى متجه رقمي (embedding) يمثل خصائصها الأساسية أو "جيناتها".
"""

import logging
from typing import List

logger = logging.getLogger(__name__)

try:
    from sentence_transformers import SentenceTransformer
    ST_AVAILABLE = True
except ImportError:
    ST_AVAILABLE = False
    logger.warning("sentence_transformers not installed. GeneticEmbeddingEngine will return zero vectors.")


class GeneticEmbeddingEngine:
    """
    محرك البصمة الجينية: يحوّل وصف الشركة النصي إلى متجه رقمي (embedding).
    """

    def __init__(self, model_name: str = 'all-MiniLM-L6-v2'):
        """
        :param model_name: اسم النموذج من Hugging Face Hub.
        """
        self.model = None
        self.model_name = model_name
        self.embedding_size = 384  # حجم all-MiniLM-L6-v2

        if ST_AVAILABLE:
            try:
                self.model = SentenceTransformer(model_name)
                logger.info(f"GeneticEmbeddingEngine initialized with model: {model_name}")
            except Exception as e:
                logger.error(f"Failed to load SentenceTransformer model '{model_name}': {e}")
        else:
            logger.warning("GeneticEmbeddingEngine: sentence_transformers unavailable, using zero vectors.")

    def get_genetic_embedding(self, description: str) -> List[float]:
        """
        يولد متجه البصمة الجينية من الوصف النصي للشركة.

        :param description: وصف نصي للشركة.
        :return: قائمة أرقام تمثل البصمة الجينية.
        """
        if not self.model or not description:
            return [0.0] * self.embedding_size

        try:
            embedding = self.model.encode(description, convert_to_tensor=False)
            return embedding.tolist()
        except Exception as e:
            logger.error(f"Error generating embedding: {e}")
            return [0.0] * self.embedding_size

    def compare_companies(self, desc_a: str, desc_b: str) -> float:
        """
        يحسب درجة التشابه الجيني بين شركتين (0.0 = مختلفتان تماماً، 1.0 = متطابقتان).

        :param desc_a: وصف الشركة الأولى.
        :param desc_b: وصف الشركة الثانية.
        :return: درجة التشابه بين 0 و 1.
        """
        if not self.model:
            return 0.0

        try:
            embeddings = self.model.encode([desc_a, desc_b], convert_to_tensor=False)
            # حساب cosine similarity
            import numpy as np
            a, b = embeddings[0], embeddings[1]
            similarity = float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))
            return max(0.0, min(1.0, similarity))
        except Exception as e:
            logger.error(f"Error comparing companies: {e}")
            return 0.0


# --- مثال للاستخدام ---
if __name__ == '__main__':
    COMPANY_PROFILES = {
        '1010': ('Saudi National Bank (SNB), a leading financial institution in Saudi Arabia, '
                 'offering corporate, retail, and investment banking services.'),
        '2222': ('Saudi Aramco, one of the worlds largest integrated energy and chemicals companies. '
                 'Operates in exploration, production, refining, and marketing of crude oil.'),
        '7010': ('Saudi Telecom Company (STC), the leading provider of telecommunications services '
                 'in Saudi Arabia, offering mobile, internet, and digital services.')
    }

    engine = GeneticEmbeddingEngine()

    if engine.model:
        for symbol, profile in COMPANY_PROFILES.items():
            genetic_vector = engine.get_genetic_embedding(profile)
            print(f"Genetic Vector for {symbol} (first 5): {genetic_vector[:5]}")
            print(f"Vector size: {len(genetic_vector)}")
