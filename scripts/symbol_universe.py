"""
Symbol Universe — اكتشاف أسهم السوق الكامل وتصنيفها
المبدأ الأساسي:
  لا توجد قائمة ثابتة. كل سهم يُقيَّم بناءً على سيولته التاريخية الخاصة به.
  السيولة المقارِنة ليست "هل هو سائل مقارنةً بالسوق؟"
  بل "هل هو سائل مقارنةً بنفسه في الفترات الماضية؟"
الإصلاح الجذري (v3):
  - استبدال حساب النافذة الزمنية اليدوي بـ SQL مباشر
  - SQL يجد آخر يوم فيه بيانات فعلية لكل سهم
  - لا اعتماد على timezone calculations المعقدة
"""
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from loguru import logger
from sqlalchemy import text
from scripts.database import db
from scripts.redis_manager import redis_manager

# ── ثوابت التصنيف ──────────────────────────────────────────────────────────────
ACTIVITY_THRESHOLDS = {
    'ACTIVE':    1.0,   # الحجم الحالي >= 100% من متوسطه التاريخي
    'AWAKENING': 0.5,   # الحجم الحالي >= 50% من متوسطه التاريخي
}
MIN_HISTORICAL_CANDLES = 30
HISTORICAL_WINDOW_DAYS = 30
UNIVERSE_CACHE_KEY = "symbol_universe:active"
UNIVERSE_CACHE_TTL = 300  # 5 دقائق

class SymbolUniverse:
    """
    يكتشف كل أسهم السوق من قاعدة البيانات ويصنّفها
    بناءً على سيولتها التاريخية الخاصة بها.
    الإصلاح الجذري: كل الحسابات تتم في SQL مباشرة على DB
    بدلاً من حساب النوافذ الزمنية في Python (كان مصدر الخطأ).
    """
    def __init__(self):
        self.logger = logger.bind(module="symbol_universe")

    # ── الاكتشاف الكامل ────────────────────────────────────────────────────────
    def discover_all_symbols(self) -> List[str]:
        """
        يجلب قائمة أسهم تاسي الرئيسي.
        المصدر الأولوي: market_data.symbols (القائمة المتزامنة يومياً من SAHMK)
        الفالباك: market_data.ohlcv (إذا كان جدول symbols فارغاً أو غير متاح)
        فلتر مضمون: فقط أسهم تاسي الرئيسية (رموز مكونة من 4 أرقام وتبدأ من 1 إلى 8)
        المؤشر: فقط 90001
        القطاعات: فقط 90010 إلى 90030
        استبعاد كامل: كل الرموز التي تبدأ بـ 9 ولا تتبع القاعدة أعلاه (نمو + ETFs)
        """
        # ── المصدر الأولوي: market_data.symbols ───────────────────────────────────
        try:
            with db.get_session() as session:
                result = session.execute(text("""
                    SELECT symbol
                    FROM market_data.symbols
                    WHERE is_active = TRUE
                      AND (
                          symbol ~ '^[1-8][0-9]{3}$' -- أسهم تاسي الرئيسية
                          OR symbol = '90001' -- المؤشر العام
                          OR (symbol ~ '^900[1-3][0-9]$' AND symbol::int BETWEEN 90010 AND 90030) -- القطاعات
                      )
                    ORDER BY symbol
                """))
                symbols = [row[0] for row in result.fetchall()]
            
            if symbols:
                self.logger.info(
                    f"📊 discover_all_symbols: {len(symbols)} valid symbols "
                    f"from market_data.symbols (primary source)"
                )
                return symbols
                
            # جدول symbols فارغ — استخدم fallback
            self.logger.warning(
                "⚠️ market_data.symbols is empty — "
                "falling back to ohlcv. Run: python3 scripts/sync_symbols.py"
            )
        except Exception as e:
            self.logger.warning(
                f"⚠️ market_data.symbols unavailable ({e}) — "
                "falling back to ohlcv discovery"
            )

        # ── Fallback: market_data.ohlcv ───────────────────────────────────────────────
        try:
            with db.get_session() as session:
                result = session.execute(text("""
                    SELECT DISTINCT symbol
                    FROM market_data.ohlcv
                    WHERE (
                        symbol ~ '^[1-8][0-9]{3}$' -- أسهم تاسي الرئيسية
                        OR symbol = '90001' -- المؤشر العام
                        OR (symbol ~ '^900[1-3][0-9]$' AND symbol::int BETWEEN 90010 AND 90030) -- القطاعات
                    )
                    ORDER BY symbol
                """))
                symbols = [row[0] for row in result.fetchall()]
                
            self.logger.info(
                f"📊 discover_all_symbols: {len(symbols)} valid symbols "
                f"from ohlcv (fallback — run sync_symbols.py to populate symbols table)"
            )
            return symbols
        except Exception as e:
            self.logger.error(f"❌ discover_all_symbols error: {e}")
            return []

    # ── التصنيف الرئيسي (SQL-based) ───────────────────────────────────────────
    def classify_symbol(self, symbol: str, timeframe: str = '1m') -> Dict:
        """
        يصنف السهم بناءً على سيولته التاريخية باستخدام SQL.
        """
        try:
            with db.get_session() as session:
                # 1. إيجاد آخر يوم فيه بيانات للسهم
                result = session.execute(text("""
                    SELECT MAX(time::date) 
                    FROM market_data.ohlcv 
                    WHERE symbol = :symbol AND timeframe = :tf
                """), {'symbol': symbol, 'tf': timeframe})
                last_date = result.scalar()
                
                if not last_date:
                    return {
                        'symbol': symbol, 'status': 'DORMANT', 
                        'volume_ratio': 0.0, 'candle_count': 0, 'eligible': False
                    }
                    
                # 2. حساب الحجم في آخر يوم
                result = session.execute(text("""
                    SELECT SUM(volume), COUNT(*) 
                    FROM market_data.ohlcv 
                    WHERE symbol = :symbol AND timeframe = :tf AND time::date = :last_date
                """), {'symbol': symbol, 'tf': timeframe, 'last_date': last_date})
                row = result.fetchone()
                current_volume = float(row[0] or 0)
                current_candles = int(row[1] or 0)
                
                # 3. حساب المتوسط التاريخي (30 يوم قبل آخر يوم)
                start_date = last_date - timedelta(days=HISTORICAL_WINDOW_DAYS)
                result = session.execute(text("""
                    SELECT SUM(volume) / COUNT(DISTINCT time::date), COUNT(*) 
                    FROM market_data.ohlcv 
                    WHERE symbol = :symbol AND timeframe = :tf 
                      AND time::date >= :start_date AND time::date < :last_date
                """), {'symbol': symbol, 'tf': timeframe, 'start_date': start_date, 'last_date': last_date})
                row = result.fetchone()
                avg_historical_volume = float(row[0] or 0)
                historical_candles = int(row[1] or 0)
                
                # 4. التصنيف
                if historical_candles < MIN_HISTORICAL_CANDLES or avg_historical_volume == 0:
                    return {
                        'symbol': symbol, 'status': 'DORMANT', 
                        'volume_ratio': 0.0, 'candle_count': current_candles + historical_candles, 
                        'eligible': False
                    }
                    
                volume_ratio = current_volume / avg_historical_volume
                
                if volume_ratio >= ACTIVITY_THRESHOLDS['ACTIVE']:
                    status = 'ACTIVE'
                elif volume_ratio >= ACTIVITY_THRESHOLDS['AWAKENING']:
                    status = 'AWAKENING'
                else:
                    status = 'DORMANT'
                    
                return {
                    'symbol': symbol, 
                    'status': status, 
                    'volume_ratio': volume_ratio, 
                    'candle_count': current_candles + historical_candles, 
                    'eligible': status in ['ACTIVE', 'AWAKENING']
                }
                
        except Exception as e:
            self.logger.error(f"Error classifying {symbol}: {e}")
            return {
                'symbol': symbol, 'status': 'DORMANT', 
                'volume_ratio': 0.0, 'candle_count': 0, 'eligible': False
            }

    def get_active_universe(self, timeframe: str = '1m', use_cache: bool = True, include_awakening: bool = True) -> Tuple[List[str], List[Dict]]:
        """
        يجلب قائمة الأسهم النشطة.
        """
        if use_cache:
            cached = redis_manager.get(UNIVERSE_CACHE_KEY)
            if cached:
                active_symbols = [r['symbol'] for r in cached if r['eligible']]
                if not include_awakening:
                    active_symbols = [r['symbol'] for r in cached if r['status'] == 'ACTIVE']
                return active_symbols, cached
                
        all_symbols = self.discover_all_symbols()
        classifications = []
        
        for symbol in all_symbols:
            classification = self.classify_symbol(symbol, timeframe)
            classifications.append(classification)
            
        active_symbols = [r['symbol'] for r in classifications if r['eligible']]
        if not include_awakening:
            active_symbols = [r['symbol'] for r in classifications if r['status'] == 'ACTIVE']
            
        active_count = sum(1 for r in classifications if r['status'] == 'ACTIVE')
        awakening_count = sum(1 for r in classifications if r['status'] == 'AWAKENING')
        dormant_count  = sum(1 for r in classifications if r['status'] == 'DORMANT')
        
        self.logger.info(
            f"🌍 Universe scan complete: {len(all_symbols)} total | "
            f"ACTIVE={active_count} | AWAKENING={awakening_count} | DORMANT={dormant_count}"
        )
        
        if use_cache:
            redis_manager.set(UNIVERSE_CACHE_KEY, classifications, ttl=UNIVERSE_CACHE_TTL)
            
        return active_symbols, classifications

    def get_best_strategy_for_symbol(self, symbol: str) -> Optional[Dict]:
        """
        يجلب أفضل استراتيجية مُكتشَفة لسهم معين من نتائج الـ Scientist.
        """
        try:
            with db.get_session() as session:
                result = session.execute(text("""
                    SELECT profit_objective, fitness_score, dna
                    FROM genetic.strategies
                    WHERE symbol = :symbol AND fitness_score > 0
                    ORDER BY fitness_score DESC
                    LIMIT 1
                """), {'symbol': symbol})
                row = result.fetchone()
                
            if row:
                import json
                return {
                    'objective': row[0],
                    'fitness': float(row[1]),
                    'dna': row[2] if isinstance(row[2], dict) else json.loads(row[2])
                }
        except Exception as e:
            self.logger.debug(f"No scientist result for {symbol}: {e}")
        return None

    def get_symbols_needing_scientist(self, limit: int = 10) -> List[str]:
        """
        يُعيد قائمة الأسهم النشطة التي لم يُجرِ عليها الـ Scientist بعد.
        """
        try:
            active_symbols, _ = self.get_active_universe(use_cache=True)
            if not active_symbols:
                return []
                
            with db.get_session() as session:
                placeholders = ','.join([f':s{i}' for i in range(len(active_symbols))])
                params = {f's{i}': sym for i, sym in enumerate(active_symbols)}
                
                result = session.execute(text(f"""
                    SELECT symbol, MAX(created_at) as last_run
                    FROM genetic.strategies
                    WHERE symbol IN ({placeholders})
                    GROUP BY symbol
                """), params)
                
                analyzed = {row[0]: row[1] for row in result.fetchall()}
                
            stale_cutoff = datetime.utcnow() - timedelta(days=7)
            needs_scientist = []
            
            for symbol in active_symbols:
                last_run = analyzed.get(symbol)
                if last_run is None or last_run < stale_cutoff:
                    needs_scientist.append(symbol)
                    
            import random
            random.shuffle(needs_scientist)
            
            self.logger.info(
                f"🔬 Symbols needing Scientist: {len(needs_scientist)} "
                f"(showing top {min(limit, len(needs_scientist))})"
            )
            
            return needs_scientist[:limit]
            
        except Exception as e:
            self.logger.error(f"Error getting symbols needing scientist: {e}")
            return []

# ── Singleton ──────────────────────────────────────────────────────────────────
symbol_universe = SymbolUniverse()

# ── اختبار مباشر ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    from loguru import logger as log
    log.info("=== Symbol Universe Test (v3 — SQL-based) ===")
    universe = SymbolUniverse()
    
    active, classifications = universe.get_active_universe(use_cache=False)
    
    log.info(f"\n{'Symbol':<10} {'Status':<12} {'Ratio':<8} {'Candles':<10} {'Eligible'}")
    log.info("-" * 55)
    for r in classifications[:20]:
        log.info(
            f"{r['symbol']:<10} {r['status']:<12} "
            f"{r['volume_ratio']:<8.2f} {r['candle_count']:<10} {r['eligible']}"
        )
        
    log.info(f"\n✅ Active universe: {len(active)} symbols")
    log.info(f"Active symbols (top 10): {active[:10]}")
