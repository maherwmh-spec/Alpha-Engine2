"""
Symbol Universe — اكتشاف أسهم السوق الكامل وتصنيفها

المبدأ الأساسي:
  لا توجد قائمة ثابتة. كل سهم يُقيَّم بناءً على سيولته التاريخية الخاصة به.
  السيولة المقارِنة ليست "هل هو سائل مقارنةً بالسوق؟"
  بل "هل هو سائل مقارنةً بنفسه في الفترات الماضية؟"

المخرجات:
  - قائمة ديناميكية بكل الأسهم النشطة في DB مع درجة نشاطها الحالي
  - تصنيف كل سهم: ACTIVE / AWAKENING / DORMANT
  - يُحدَّث كل دورة تشغيل (لا يُخزَّن كقائمة ثابتة)
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

# نسبة الحجم الحالي إلى المتوسط التاريخي الخاص بالسهم
ACTIVITY_THRESHOLDS = {
    'ACTIVE':    1.0,   # الحجم الحالي >= 100% من متوسطه التاريخي
    'AWAKENING': 0.5,   # الحجم الحالي >= 50% من متوسطه التاريخي
    # أقل من 50% → DORMANT (خامل)
}

# الحد الأدنى لعدد الشموع التاريخية المطلوبة لتقييم السهم
MIN_HISTORICAL_CANDLES = 30

# نافذة حساب المتوسط التاريخي (عدد الأيام)
HISTORICAL_WINDOW_DAYS = 30

# نافذة الحجم الحالي (عدد الدقائق الأخيرة) — تُستخدم فقط أثناء التداول
CURRENT_WINDOW_MINUTES = 60

# ساعات التداول (KSA = UTC+3)
MARKET_OPEN_HOUR_KSA  = 10   # 10:00 KSA
MARKET_CLOSE_HOUR_KSA = 15   # 15:00 KSA

# مفتاح Redis للكاش
UNIVERSE_CACHE_KEY = "symbol_universe:active"
UNIVERSE_CACHE_TTL = 300  # 5 دقائق


class SymbolUniverse:
    """
    يكتشف كل أسهم السوق من قاعدة البيانات ويصنّفها
    بناءً على سيولتها التاريخية الخاصة بها.

    لا توجد قائمة ثابتة — كل شيء ديناميكي.
    """

    def __init__(self):
        self.logger = logger.bind(module="symbol_universe")

    # ── الاكتشاف الكامل ────────────────────────────────────────────────────────

    def discover_all_symbols(self) -> List[str]:
        """
        يجلب كل الرموز الموجودة في market_data.ohlcv
        (باستثناء رموز القطاعات 9xxxx).
        """
        try:
            with db.get_session() as session:
                query = text("""
                    SELECT DISTINCT symbol
                    FROM market_data.ohlcv
                    WHERE symbol NOT LIKE '9%'
                    ORDER BY symbol
                """)
                result = session.execute(query)
                symbols = [row[0] for row in result.fetchall()]

            self.logger.info(f"📊 Discovered {len(symbols)} symbols in DB")
            return symbols

        except Exception as e:
            self.logger.error(f"Error discovering symbols: {e}")
            return []

    # ── حساب السيولة التاريخية الخاصة بكل سهم ─────────────────────────────────

    def _is_market_open(self) -> bool:
        """هل السوق مفتوحة الآن؟"""
        from datetime import timezone
        now_ksa = datetime.now(timezone.utc) + timedelta(hours=3)
        # الأحد=6, الاثنين=0, ... الخميس=4, الجمعة=5, السبت=6
        weekday = now_ksa.weekday()  # Monday=0 ... Sunday=6
        # تداول: الأحد(6) - الخميس(3)
        is_trading_day = weekday in (6, 0, 1, 2, 3)
        is_trading_hour = MARKET_OPEN_HOUR_KSA <= now_ksa.hour < MARKET_CLOSE_HOUR_KSA
        return is_trading_day and is_trading_hour

    def _get_last_session_window(self) -> Tuple[datetime, datetime]:
        """
        يُعيد نافذة زمنية تمثّل آخر جلسة تداول مكتملة أو الجلسة الحالية.

        أثناء التداول: آخر 60 دقيقة من الآن.
        خارج التداول: آخر يوم تداول كامل (من 10:00 إلى 15:00 KSA).
        """
        from datetime import timezone
        now_ksa = datetime.now(timezone.utc) + timedelta(hours=3)
        now_utc = datetime.now(timezone.utc)

        if self._is_market_open():
            # أثناء التداول: آخر 60 دقيقة
            return now_utc - timedelta(minutes=60), now_utc

        # خارج التداول: نبحث عن آخر يوم تداول
        # نرجع للخلف حتى نجد يوم تداول
        candidate = now_ksa.date()
        for _ in range(7):
            wd = candidate.weekday()
            if wd in (6, 0, 1, 2, 3):  # أحد-خميس
                break
            candidate -= timedelta(days=1)

        # نافذة آخر جلسة: من 10:00 إلى 15:00 KSA بتوقيت UTC
        from datetime import timezone as tz
        session_start_ksa = datetime(
            candidate.year, candidate.month, candidate.day,
            MARKET_OPEN_HOUR_KSA, 0, 0, tzinfo=tz.utc
        ) - timedelta(hours=3)  # KSA → UTC
        session_end_ksa = datetime(
            candidate.year, candidate.month, candidate.day,
            MARKET_CLOSE_HOUR_KSA, 0, 0, tzinfo=tz.utc
        ) - timedelta(hours=3)

        return session_start_ksa, session_end_ksa

    def _get_historical_avg_volume(
        self, symbol: str, timeframe: str = '1m', days: int = HISTORICAL_WINDOW_DAYS
    ) -> Optional[float]:
        """
        يحسب متوسط الحجم اليومي التاريخي الخاص بالسهم
        خلال الـ N يوماً الماضية.

        هذا هو المرجع الذاتي — لا مقارنة بالسوق.
        """
        try:
            with db.get_session() as session:
                query = text("""
                    SELECT
                        DATE_TRUNC('day', time) AS day,
                        SUM(volume) AS daily_volume
                    FROM market_data.ohlcv
                    WHERE symbol = :symbol
                      AND timeframe = :timeframe
                      AND time >= NOW() - (:days * INTERVAL '1 day')
                    GROUP BY DATE_TRUNC('day', time)
                    HAVING SUM(volume) > 0
                    ORDER BY day DESC
                """)
                result = session.execute(query, {
                    'symbol': symbol,
                    'timeframe': timeframe,
                    'days': days
                })
                rows = result.fetchall()

            if not rows or len(rows) < 2:
                return None

            daily_volumes = [float(row[1]) for row in rows]
            avg = float(np.mean(daily_volumes))
            return avg if avg > 0 else None

        except Exception as e:
            self.logger.debug(f"Error getting historical volume for {symbol}: {e}")
            return None

    def _get_current_volume(
        self, symbol: str, timeframe: str = '1m'
    ) -> Optional[float]:
        """
        يحسب إجمالي الحجم في نافذة آخر جلسة تداول.

        أثناء التداول: آخر 60 دقيقة.
        خارج التداول: آخر جلسة تداول مكتملة (10:00-15:00 KSA).
        """
        try:
            window_start, window_end = self._get_last_session_window()

            with db.get_session() as session:
                query = text("""
                    SELECT COALESCE(SUM(volume), 0)
                    FROM market_data.ohlcv
                    WHERE symbol = :symbol
                      AND timeframe = :timeframe
                      AND time >= :window_start
                      AND time <= :window_end
                """)
                result = session.execute(query, {
                    'symbol': symbol,
                    'timeframe': timeframe,
                    'window_start': window_start,
                    'window_end': window_end
                })
                row = result.fetchone()

            return float(row[0]) if row and row[0] else 0.0

        except Exception as e:
            self.logger.debug(f"Error getting current volume for {symbol}: {e}")
            return 0.0

    def _get_candle_count(self, symbol: str, timeframe: str = '1m') -> int:
        """يحسب عدد الشموع المتاحة للسهم."""
        try:
            with db.get_session() as session:
                query = text("""
                    SELECT COUNT(*)
                    FROM market_data.ohlcv
                    WHERE symbol = :symbol AND timeframe = :timeframe
                """)
                result = session.execute(query, {'symbol': symbol, 'timeframe': timeframe})
                row = result.fetchone()
            return int(row[0]) if row else 0
        except Exception:
            return 0

    # ── التصنيف الرئيسي ────────────────────────────────────────────────────────

    def classify_symbol(self, symbol: str, timeframe: str = '1m') -> Dict:
        """
        يُصنّف سهماً واحداً بناءً على سيولته التاريخية الخاصة.

        المخرجات:
            {
                'symbol': '2222',
                'status': 'ACTIVE',           # ACTIVE / AWAKENING / DORMANT
                'volume_ratio': 1.35,         # الحجم الحالي / المتوسط التاريخي
                'current_volume': 1500000,
                'historical_avg_volume': 1111111,
                'candle_count': 18798,
                'eligible': True              # هل يُحلَّل هذا السهم؟
            }
        """
        candle_count = self._get_candle_count(symbol, timeframe)

        if candle_count < MIN_HISTORICAL_CANDLES:
            return {
                'symbol': symbol,
                'status': 'DORMANT',
                'volume_ratio': 0.0,
                'current_volume': 0.0,
                'historical_avg_volume': 0.0,
                'candle_count': candle_count,
                'eligible': False,
                'reason': f'insufficient_data ({candle_count} candles)'
            }

        hist_avg = self._get_historical_avg_volume(symbol, timeframe)
        current_vol = self._get_current_volume(symbol, timeframe)

        if not hist_avg or hist_avg == 0:
            return {
                'symbol': symbol,
                'status': 'DORMANT',
                'volume_ratio': 0.0,
                'current_volume': current_vol,
                'historical_avg_volume': 0.0,
                'candle_count': candle_count,
                'eligible': False,
                'reason': 'no_historical_volume'
            }

        # المقارنة الذاتية:
        # - أثناء التداول: حجم آخر 60 دقيقة vs متوسط ساعة تداول تاريخي (يومي ÷ 5)
        # - خارج التداول: حجم آخر جلسة كاملة vs متوسط يوم تداول تاريخي
        if self._is_market_open():
            # مقارنة ساعية: المتوسط اليومي ÷ 5 ساعات تداول
            reference_avg = hist_avg / 5.0
        else:
            # مقارنة جلسة كاملة: المتوسط اليومي مباشرة
            reference_avg = hist_avg

        volume_ratio = current_vol / reference_avg if reference_avg > 0 else 0.0

        # التصنيف
        if volume_ratio >= ACTIVITY_THRESHOLDS['ACTIVE']:
            status = 'ACTIVE'
            eligible = True
        elif volume_ratio >= ACTIVITY_THRESHOLDS['AWAKENING']:
            status = 'AWAKENING'
            eligible = True
        else:
            status = 'DORMANT'
            eligible = False

        return {
            'symbol': symbol,
            'status': status,
            'volume_ratio': round(volume_ratio, 3),
            'current_volume': current_vol,
            'historical_avg_volume': hist_avg,
            'candle_count': candle_count,
            'eligible': eligible,
            'reason': f'volume_ratio={volume_ratio:.2f}'
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

        Args:
            timeframe: الإطار الزمني للتحليل
            use_cache: استخدام Redis cache (5 دقائق)
            include_awakening: تضمين الأسهم في مرحلة الصحوة

        Returns:
            (active_symbols, full_classification)
        """
        # محاولة الكاش أولاً
        if use_cache:
            cached = redis_manager.get(UNIVERSE_CACHE_KEY)
            if cached:
                symbols = [r['symbol'] for r in cached if r.get('eligible')]
                self.logger.debug(f"📦 Universe from cache: {len(symbols)} active symbols")
                return symbols, cached

        # اكتشاف كل الرموز
        all_symbols = self.discover_all_symbols()

        if not all_symbols:
            self.logger.warning("⚠️ No symbols found in DB — market_data.ohlcv is empty")
            return [], []

        # تصنيف كل سهم
        classifications = []
        for symbol in all_symbols:
            classification = self.classify_symbol(symbol, timeframe)
            classifications.append(classification)

        # ترتيب حسب volume_ratio تنازلياً
        classifications.sort(key=lambda x: x['volume_ratio'], reverse=True)

        # الأسهم المؤهلة
        active_symbols = [
            r['symbol'] for r in classifications
            if r['eligible'] and (
                r['status'] == 'ACTIVE' or
                (include_awakening and r['status'] == 'AWAKENING')
            )
        ]

        # إحصائيات
        active_count = sum(1 for r in classifications if r['status'] == 'ACTIVE')
        awakening_count = sum(1 for r in classifications if r['status'] == 'AWAKENING')
        dormant_count = sum(1 for r in classifications if r['status'] == 'DORMANT')

        self.logger.info(
            f"🌍 Universe scan complete: {len(all_symbols)} total | "
            f"ACTIVE={active_count} | AWAKENING={awakening_count} | DORMANT={dormant_count}"
        )

        # حفظ في Redis
        if use_cache:
            redis_manager.set(UNIVERSE_CACHE_KEY, classifications, ttl=UNIVERSE_CACHE_TTL)

        return active_symbols, classifications

    def get_best_strategy_for_symbol(self, symbol: str) -> Optional[Dict]:
        """
        يجلب أفضل استراتيجية مُكتشَفة لسهم معين من نتائج الـ Scientist.

        يبحث في strategies.backtest_results عن أعلى sharpe_ratio
        لهذا السهم تحديداً.

        Returns:
            {
                'strategy_name': 'genetic_2222_v3',
                'parameters': {...},
                'sharpe_ratio': 1.85,
                'total_return': 0.23,
                'win_rate': 0.62,
                'source': 'scientist'  # أو 'default'
            }
        """
        try:
            # 1. البحث في نتائج الـ Scientist
            with db.get_session() as session:
                query = text("""
                    SELECT strategy_name, parameters, sharpe_ratio,
                           total_return, win_rate, created_at
                    FROM strategies.backtest_results
                    WHERE symbol = :symbol
                      AND sharpe_ratio IS NOT NULL
                      AND sharpe_ratio > 0
                    ORDER BY sharpe_ratio DESC
                    LIMIT 1
                """)
                result = session.execute(query, {'symbol': symbol})
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

        # 2. لا توجد نتائج Scientist → يُعيد None (لا معاملات ثابتة)
        return None

    def get_symbols_needing_scientist(self, limit: int = 10) -> List[str]:
        """
        يُعيد قائمة الأسهم النشطة التي لم يُجرِ عليها الـ Scientist بعد
        أو التي مضى على آخر تحليل أكثر من 7 أيام.

        هذه الأسهم تُعطى أولوية لتشغيل الـ Scientist عليها.
        """
        try:
            active_symbols, _ = self.get_active_universe(use_cache=True)

            if not active_symbols:
                return []

            with db.get_session() as session:
                query = text("""
                    SELECT symbol, MAX(created_at) as last_run
                    FROM strategies.backtest_results
                    WHERE symbol = ANY(:symbols)
                    GROUP BY symbol
                """)
                result = session.execute(query, {'symbols': active_symbols})
                analyzed = {row[0]: row[1] for row in result.fetchall()}

            # الأسهم التي لم تُحلَّل أو تجاوزت 7 أيام
            stale_cutoff = datetime.utcnow() - timedelta(days=7)
            needs_scientist = []

            for symbol in active_symbols:
                last_run = analyzed.get(symbol)
                if last_run is None or last_run < stale_cutoff:
                    needs_scientist.append(symbol)

            # ترتيب عشوائي لتوزيع العمل
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

    log.info("=== Symbol Universe Test ===")

    universe = SymbolUniverse()

    # اكتشاف الكون الكامل
    active, classifications = universe.get_active_universe(use_cache=False)

    log.info(f"\n{'Symbol':<10} {'Status':<12} {'Ratio':<8} {'Candles':<10} {'Eligible'}")
    log.info("-" * 55)
    for r in classifications[:20]:
        log.info(
            f"{r['symbol']:<10} {r['status']:<12} "
            f"{r['volume_ratio']:<8.2f} {r['candle_count']:<10} {r['eligible']}"
        )

    log.info(f"\n✅ Active universe: {len(active)} symbols")
    log.info(f"Active symbols: {active[:10]}...")

    # اختبار أفضل استراتيجية
    if active:
        best = universe.get_best_strategy_for_symbol(active[0])
        if best:
            log.info(f"\n🏆 Best strategy for {active[0]}: {best['strategy_name']} "
                     f"(Sharpe={best['sharpe_ratio']:.2f})")
        else:
            log.info(f"\n🔬 {active[0]} needs Scientist analysis")

    # الأسهم التي تحتاج Scientist
    needs = universe.get_symbols_needing_scientist(limit=5)
    log.info(f"\n🔬 Needs Scientist: {needs}")
