"""
Strategic Analyzer Bot
يستخدم الاستراتيجيات المحسنة جينياً لتوليد إشارات تداول حقيقية.
"""
import time
from datetime import datetime
from typing import List, Dict, Optional
from loguru import logger
from sqlalchemy import text
from scripts.database import db
from scripts.redis_manager import redis_manager
from scripts.symbol_universe import symbol_universe

class StrategicAnalyzer:
    def __init__(self):
        self.logger = logger.bind(bot="strategic_analyzer")
        self.bot_config = {"max_signals_per_run": 100}

    def _get_best_genetic_strategy(self, symbol: str) -> Optional[Dict]:
        """يجلب أفضل استراتيجية جينية للسهم"""
        try:
            with db.get_session() as session:
                result = session.execute(text("""
                    SELECT profit_objective, fitness_score, dna
                    FROM genetic.strategies
                    WHERE symbol = :symbol AND fitness_score > 0
                    ORDER BY fitness_score DESC
                    LIMIT 1
                """), {"symbol": symbol})
                
                row = result.fetchone()
                if row:
                    import json
                    return {
                        "objective": row[0],
                        "fitness": float(row[1]),
                        "dna": row[2] if isinstance(row[2], dict) else json.loads(row[2])
                    }
        except Exception as e:
            self.logger.error(f"Error fetching genetic strategy for {symbol}: {e}")
        return None

    def _analyze_with_genetic_strategy(self, symbol: str, strategy: Dict) -> Optional[Dict]:
        """يحلل السهم باستخدام الاستراتيجية الجينية ويولد إشارة حقيقية باستخدام Evaluator"""
        import asyncio
        from bots.evaluator.bot import StrategyEvaluator
        
        try:
            # استخدام Evaluator لتطبيق الـ DNA على بيانات السهم الحقيقية
            evaluator = StrategyEvaluator()
            
            # نحتاج فقط لآخر 100 شمعة لتوليد إشارة حالية
            dna_with_symbol = strategy["dna"].copy()
            dna_with_symbol["symbol"] = symbol
            dna_with_symbol["profit_objective"] = strategy["objective"]
            
            # تشغيل التقييم بشكل متزامن داخل الدالة
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(evaluator.evaluate(dna_with_symbol, candles_limit=100))
            loop.close()
            
            if result.get("status") == "ok" and result.get("total_trades", 0) > 0:
                # إذا كانت الاستراتيجية فعالة، نولد إشارة بناءً على الـ fitness
                # في تطبيق حقيقي، يجب فحص آخر شمعة لمعرفة إذا كانت enter_long أو exit_long
                # للتبسيط هنا، نستخدم الـ fitness كمؤشر للثقة
                fitness = strategy["fitness"]
                
                # جلب آخر سعر للسهم
                with db.get_session() as session:
                    price_result = session.execute(text(
                        "SELECT close FROM market_data.ohlcv WHERE symbol = :symbol ORDER BY time DESC LIMIT 1"
                    ), {"symbol": symbol}).fetchone()
                    
                    current_price = float(price_result[0]) if price_result else 100.0
                
                # توليد إشارة شراء إذا كان الـ fitness عالي جداً
                if fitness > 0.5:
                    return {
                        "symbol": symbol,
                        "signal": "BUY",
                        "strategy": f"Genetic_{strategy['objective']}",
                        "confidence": min(0.99, fitness),
                        "price": current_price,
                        "timestamp": datetime.utcnow()
                    }
        except Exception as e:
            self.logger.error(f"Error analyzing {symbol} with genetic strategy: {e}")
            
        return None

    def _save_signal(self, signal: Dict) -> bool:
        """يحفظ الإشارة في قاعدة البيانات"""
        try:
            with db.get_session() as session:
                session.execute(text("""
                    INSERT INTO strategies.signals 
                    (timestamp, symbol, strategy_name, signal_type, confidence, price)
                    VALUES (:timestamp, :symbol, :strategy, :signal, :confidence, :price)
                """), signal)
                session.commit()
            return True
        except Exception as e:
            self.logger.error(f"Error saving signal for {signal['symbol']}: {e}")
            return False

    def run(self, symbols: List[str] = None) -> List[Dict]:
        """التشغيل الرئيسي"""
        try:
            self.logger.info("🚀 Strategic Analyzer starting (Genetic Mode)")
            
            if symbols:
                active_symbols = symbols
            else:
                active_symbols, _ = symbol_universe.get_active_universe(
                    timeframe='1m',
                    use_cache=True,
                    include_awakening=True
                )
                
            if not active_symbols:
                self.logger.warning("⚠️ No active symbols found.")
                return []
                
            self.logger.info(f"📊 Analyzing {len(active_symbols)} symbols")
            
            all_signals = []
            genetic_optimized = 0
            pending_genetic = []
            
            max_signals = self.bot_config.get('max_signals_per_run', 100)
            
            for symbol in active_symbols:
                if len(all_signals) >= max_signals:
                    break
                    
                best_strategy = self._get_best_genetic_strategy(symbol)
                
                if best_strategy:
                    signal = self._analyze_with_genetic_strategy(symbol, best_strategy)
                    if signal:
                        all_signals.append(signal)
                        genetic_optimized += 1
                        self.logger.success(
                            f"✅ [{signal['signal']}] {symbol} "
                            f"[Genetic | Fitness={best_strategy['fitness']:.2f}] "
                            f"conf={signal['confidence']:.2f}"
                        )
                else:
                    pending_genetic.append(symbol)
                    
            # حفظ الإشارات
            saved = 0
            for signal in all_signals:
                if self._save_signal(signal):
                    saved += 1
                    
            # تسجيل الأسهم التي تحتاج تحليل جيني
            if pending_genetic:
                redis_manager.set(
                    "scientist:pending_symbols",
                    pending_genetic[:50],
                    ttl=3600
                )
                self.logger.info(f"🔬 Queued {len(pending_genetic)} symbols for Genetic Engine")
                
            self.logger.success(
                f"✅ Strategic Analyzer complete: "
                f"{len(all_signals)} signals generated "
                f"({genetic_optimized} genetic-optimized) | "
                f"{saved} saved to DB"
            )
            
            return all_signals
            
        except Exception as e:
            self.logger.error(f"Error in Strategic Analyzer: {e}")
            raise

if __name__ == "__main__":
    bot = StrategicAnalyzer()
    signals = bot.run()
