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
        """يجلب كل الرموز الموجودة في market_data.ohlcv (باستثناء القطاعات 9xxxx)."""
        try:
            with db.get_session() as session:
                result = session.execute(text("""
                    SELECT DISTINCT symbol
                    FROM market_data.ohlcv
                    WHERE symbol NOT LIKE '9%'
                    ORDER BY symbol
                """))
                symbols = [row[0] for row in result.fetchall()]
            self.logger.info(f"📊 Discovered {len(symbols)} symbols in DB")
            return symbols
        except Exception as e:
            self.logger.error(f"Error discovering symbols: {e}")
            return []

    # ── التصنيف الرئيسي (SQL-based) ───────────────────────────────────────────

    def classify_symbol(self, symbol: str, timeframe: str = '1m') -> Dict:
        """
        يُصنّف سهماً واحداً بناءً على سيولته التاريخية الخاصة.

        الإصلاح: كل الحسابات في SQL — لا Python timezone magic.

        المنطق:
          1. احسب متوسط الحجم اليومي لآخر 30 يوم (باستثناء آخر يومين)
          2. احسب حجم آخر يوم تداول فيه بيانات فعلية
          3. volume_ratio = آخر يوم / المتوسط التاريخي
        """
        try:
            with db.get_session() as session:
                # ── الاستعلام الموحّد: كل الحسابات في SQL واحد ──
                result = session.execute(text("""
                    WITH daily_volumes AS (
                        SELECT
                            DATE_TRUNC('day', time) AS trading_day,
                            SUM(volume)             AS daily_vol,
                            COUNT(*)                AS candle_count
                        FROM market_data.ohlcv
                        WHERE symbol    = :symbol
                          AND timeframe = :timeframe
                          AND volume    > 0
                        GROUP BY DATE_TRUNC('day', time)
                        ORDER BY trading_day DESC
                    ),
                    ranked AS (
                        SELECT
                            trading_day,
                            daily_vol,
                            candle_count,
                            ROW_NUMBER() OVER (ORDER BY trading_day DESC) AS rn
                        FROM daily_volumes
                    ),
                    -- آخر يوم فيه بيانات (rn=1)
                    last_day AS (
                        SELECT daily_vol AS last_vol, candle_count AS last_candles
                        FROM ranked WHERE rn = 1
                    ),
                    -- متوسط الأيام من rn=2 إلى rn=31 (30 يوم تاريخي)
                    history AS (
                        SELECT
                            AVG(daily_vol)  AS hist_avg,
                            COUNT(*)        AS hist_days,
                            SUM(candle_count) AS total_candles
                        FROM ranked
                        WHERE rn BETWEEN 2 AND 31
                    )
                    SELECT
                        COALESCE(last_day.last_vol, 0)      AS last_vol,
                        COALESCE(last_day.last_candles, 0)  AS last_candles,
                        COALESCE(history.hist_avg, 0)       AS hist_avg,
                        COALESCE(history.hist_days, 0)      AS hist_days,
                        COALESCE(history.total_candles, 0)  AS total_candles
                    FROM history
                    FULL OUTER JOIN last_day ON TRUE
                """), {'symbol': symbol, 'timeframe': timeframe})
                row = result.fetchone()

            if not row:
                return self._dormant(symbol, 0, 'no_data')

            last_vol      = float(row[0] or 0)
            last_candles  = int(row[1] or 0)
            hist_avg      = float(row[2] or 0)
            hist_days     = int(row[3] or 0)
            total_candles = int(row[4] or 0)

            # أيضاً احسب إجمالي الشموع لهذا السهم (لفحص MIN_HISTORICAL_CANDLES)
            with db.get_session() as session:
                cnt_result = session.execute(text("""
                    SELECT COUNT(*) FROM market_data.ohlcv
                    WHERE symbol = :symbol AND timeframe = :timeframe
                """), {'symbol': symbol, 'timeframe': timeframe})
                total_row = cnt_result.fetchone()
            all_candles = int(total_row[0]) if total_row else 0

            if all_candles < MIN_HISTORICAL_CANDLES:
                return self._dormant(symbol, all_candles, f'insufficient_data ({all_candles} candles)')

            if hist_avg <= 0:
                # لا يوجد تاريخ كافٍ — لكن إذا كان آخر يوم فيه بيانات، صنّفه AWAKENING
                if last_vol > 0:
                    return {
                        'symbol': symbol,
                        'status': 'AWAKENING',
                        'volume_ratio': 0.5,  # نسبة افتراضية
                        'current_volume': last_vol,
                        'historical_avg_volume': 0.0,
                        'candle_count': all_candles,
                        'eligible': True,
                        'reason': 'new_symbol_with_data'
                    }
                return self._dormant(symbol, all_candles, 'no_historical_volume')

            volume_ratio = last_vol / hist_avg if hist_avg > 0 else 0.0

            if volume_ratio >= ACTIVITY_THRESHOLDS['ACTIVE']:
                status, eligible = 'ACTIVE', True
            elif volume_ratio >= ACTIVITY_THRESHOLDS['AWAKENING']:
                status, eligible = 'AWAKENING', True
            else:
                status, eligible = 'DORMANT', False

            return {
                'symbol': symbol,
                'status': status,
                'volume_ratio': round(volume_ratio, 3),
                'current_volume': last_vol,
                'historical_avg_volume': hist_avg,
                'candle_count': all_candles,
                'eligible': eligible,
                'reason': f'volume_ratio={volume_ratio:.2f} (last={last_vol:.0f} / hist_avg={hist_avg:.0f})'
            }

        except Exception as e:
            self.logger.error(f"Error classifying {symbol}: {e}")
            return self._dormant(symbol, 0, f'error: {e}')

    def _dormant(self, symbol: str, candles: int, reason: str) -> Dict:
        return {
            'symbol': symbol,
            'status': 'DORMANT',
            'volume_ratio': 0.0,
            'current_volume': 0.0,
            'historical_avg_volume': 0.0,
            'candle_count': candles,
            'eligible': False,
            'reason': reason
        }

    # ── الدالة الرئيسية: جلب الكون الكامل ─────────────────────────────────────

    def get_active_universe(
        self,
        timeframe: str = '1m',
        use_cache: bool = True,
        include_awakening: bool = True
    ) -> Tuple[List[str], List[Dict]]:
        """
        يُعيد قائمة الأسهم النشطة حالياً بناءً على سيولتها الذاتية.
        """
        if use_cache:
            cached = redis_manager.get(UNIVERSE_CACHE_KEY)
            if cached:
                symbols = [r['symbol'] for r in cached if r.get('eligible')]
                self.logger.debug(f"📦 Universe from cache: {len(symbols)} active symbols")
                return symbols, cached

        all_symbols = self.discover_all_symbols()
        if not all_symbols:
            self.logger.warning("⚠️ No symbols found in DB — market_data.ohlcv is empty")
            return [], []

        classifications = []
        for symbol in all_symbols:
            classification = self.classify_symbol(symbol, timeframe)
            classifications.append(classification)

        classifications.sort(key=lambda x: x['volume_ratio'], reverse=True)

        active_symbols = [
            r['symbol'] for r in classifications
            if r['eligible'] and (
                r['status'] == 'ACTIVE' or
                (include_awakening and r['status'] == 'AWAKENING')
            )
        ]

        active_count   = sum(1 for r in classifications if r['status'] == 'ACTIVE')
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
                    SELECT strategy_name, parameters, sharpe_ratio,
                           total_return, win_rate, created_at
                    FROM strategies.backtest_results
                    WHERE symbol = :symbol
                      AND sharpe_ratio IS NOT NULL
                      AND sharpe_ratio > 0
                    ORDER BY sharpe_ratio DESC
                    LIMIT 1
                """), {'symbol': symbol})
                row = result.fetchone()

            if row:
                import json
                params = row[1] if isinstance(row[1], dict) else json.loads(row[1] or '{}')
                return {
                    'strategy_name': row[0],
                    'parameters': params,
                    'sharpe_ratio': float(row[2] or 0),
                    'total_return': float(row[3] or 0),
                    'win_rate': float(row[4] or 0),
                    'source': 'scientist',
                    'created_at': str(row[5])
                }
        except Exception as e:
            self.logger.debug(f"No scientist result for {symbol}: {e}")

        return None

    def get_symbols_needing_scientist(self, limit: int = 10) -> List[str]:
        """
        يُعيد قائمة الأسهم النشطة التي لم يُجرِ عليها الـ Scientist بعد
        أو التي مضى على آخر تحليل أكثر من 7 أيام.
        """
        try:
            active_symbols, _ = self.get_active_universe(use_cache=True)
            if not active_symbols:
                return []

            with db.get_session() as session:
                # استخدام IN بدلاً من ANY لتجنب مشاكل SQLAlchemy مع PostgreSQL arrays
                placeholders = ','.join([f':s{i}' for i in range(len(active_symbols))])
                params = {f's{i}': sym for i, sym in enumerate(active_symbols)}
                result = session.execute(text(f"""
                    SELECT symbol, MAX(created_at) as last_run
                    FROM strategies.backtest_results
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

    if active:
        best = universe.get_best_strategy_for_symbol(active[0])
        if best:
            log.info(f"\n🏆 Best strategy for {active[0]}: {best['strategy_name']} "
                     f"(Sharpe={best['sharpe_ratio']:.2f})")
        else:
            log.info(f"\n🔬 {active[0]} needs Scientist analysis")

    needs = universe.get_symbols_needing_scientist(limit=5)
    log.info(f"\n🔬 Needs Scientist: {needs}")
