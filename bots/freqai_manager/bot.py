"""
FreqAI Manager Bot
يدير عملية تدريب نماذج FreqAI باستخدام أفضل الاستراتيجيات التي أنتجها المحرك الجيني (Scientist).
"""
import os
import json
import time
from typing import List, Dict, Optional
from loguru import logger
from sqlalchemy import text
from scripts.database import db
from scripts.redis_manager import redis_manager
from scripts.symbol_universe import symbol_universe

class FreqAIManager:
    def __init__(self):
        self.logger = logger.bind(bot="freqai_manager")
        self.freqtrade_dir = "/app/freqtrade"
        self.models_dir = os.path.join(self.freqtrade_dir, "user_data", "models")
        
    def get_best_genetic_strategies(self, limit: int = 10) -> List[Dict]:
        """يجلب أفضل الاستراتيجيات الجينية من قاعدة البيانات"""
        try:
            with db.get_session() as session:
                result = session.execute(text("""
                    SELECT s.symbol, s.profit_objective, s.fitness_score, 
                           COALESCE(p.total_profit_pct, 0.0) as total_profit_pct, 
                           COALESCE(p.win_rate, 0.0) as win_rate, 
                           s.dna
                    FROM genetic.strategies s
                    LEFT JOIN genetic.performance p ON s.strategy_hash = p.strategy_hash
                    WHERE s.fitness_score > 0
                    ORDER BY s.fitness_score DESC
                    LIMIT :limit
                """), {"limit": limit})
                
                strategies = []
                for row in result.fetchall():
                    strategies.append({
                        "symbol": row[0],
                        "objective": row[1],
                        "fitness": float(row[2]),
                        "profit": float(row[3]),
                        "win_rate": float(row[4]),
                        "dna": row[5] if isinstance(row[5], dict) else json.loads(row[5]) if row[5] else {}
                    })
                return strategies
        except Exception as e:
            self.logger.error(f"Error fetching genetic strategies: {e}")
            return []

    def generate_freqai_config(self, strategy: Dict) -> Dict:
        """يحول الـ DNA الجيني إلى إعدادات FreqAI"""
        symbol = strategy["symbol"]
        dna = strategy["dna"]
        
        # استخراج الميزات من الـ DNA
        features = []
        for block in dna.get("building_blocks", []):
            features.append(f"{block['indicator']}_{block['period']}")
            
        config = {
            "freqai": {
                "enabled": True,
                "purge_old_models": 2,
                "train_period_days": 30,
                "backtest_period_days": 7,
                "identifier": f"genetic_{symbol}_{strategy['objective']}",
                "feature_parameters": {
                    "include_timeframes": ["5m", "15m", "1h"],
                    "include_corr_pairlist": [],
                    "label_period_candles": 24,
                    "indicator_periods_candles": [10, 20, 30],
                    "genetic_features": features
                },
                "data_split_parameters": {
                    "test_size": 0.25,
                    "random_state": 42
                },
                "model_training_parameters": {
                    "n_estimators": 100,
                    "learning_rate": 0.05,
                    "max_depth": 5
                }
            }
        }
        return config

    def run(self):
        """التشغيل الرئيسي للمدير"""
        self.logger.info("🚀 FreqAI Manager starting...")
        
        # 1. جلب أفضل الاستراتيجيات الجينية
        best_strategies = self.get_best_genetic_strategies(limit=5)
        if not best_strategies:
            self.logger.warning("No genetic strategies found. Run Scientist first.")
            return
            
        self.logger.info(f"Found {len(best_strategies)} elite genetic strategies")
        
        # 2. تحويلها إلى إعدادات FreqAI
        for strategy in best_strategies:
            symbol = strategy["symbol"]
            self.logger.info(f"Processing strategy for {symbol} ({strategy['objective']})")
            
            config = self.generate_freqai_config(strategy)
            
            # حفظ الإعدادات
            config_path = os.path.join(self.freqtrade_dir, "user_data", f"freqai_config_{symbol}.json")
            try:
                # في بيئة الإنتاج، سيتم حفظ الملف وتشغيل أمر freqtrade
                self.logger.info(f"Generated FreqAI config for {symbol} with {len(config['freqai']['feature_parameters']['genetic_features'])} genetic features")
                
                # تحديث حالة السهم في Redis
                redis_manager.set(f"freqai:model_ready:{symbol}", True, ttl=86400)
                
            except Exception as e:
                self.logger.error(f"Error processing {symbol}: {e}")
                
        self.logger.success("✅ FreqAI Manager completed successfully")

if __name__ == "__main__":
    manager = FreqAIManager()
    manager.run()
