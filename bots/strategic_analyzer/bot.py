"""
Bot 4: Strategic Analyzer (المُحلِّل الاستراتيجي)

المبدأ الأساسي:
  - لا توجد استراتيجية ثابتة لأي سهم
  - لا توجد قائمة ثابتة من الأسهم
  - كل سهم يُحلَّل بالمعاملات التي اكتشفها الـ Scientist له تحديداً
  - إذا لم يُحلَّل السهم بعد → يُضاف لقائمة انتظار الـ Scientist
  - الأسهم تُكتشف ديناميكياً من DB بناءً على سيولتها التاريخية الخاصة
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from loguru import logger
import json

from sqlalchemy import text
from config.config_manager import config
from scripts.database import db, insert_signal
from scripts.redis_manager import redis_manager
from scripts.utils import get_saudi_time
from scripts.symbol_universe import symbol_universe


class StrategicAnalyzer:
    """
    المُحلِّل الاستراتيجي — يعمل على كون الأسهم الكامل
    بمعاملات مخصصة لكل سهم من نتائج الـ Scientist.
    """

    def __init__(self):
        self.name = "strategic_analyzer"
        self.logger = logger.bind(bot=self.name)
        self.bot_config = config.get_bot_config(self.name)

    # ── جلب المؤشرات التقنية ───────────────────────────────────────────────────

    def _get_indicators(self, symbol: str, timeframe: str) -> Optional[Dict]:
        """يجلب آخر مؤشرات تقنية للسهم من DB أو Redis cache."""
        try:
            cached = redis_manager.get_cached_indicators(symbol, timeframe)
            if cached:
                return cached

            with db.get_session() as session:
                query = text("""
                    SELECT rsi, macd, macd_signal, macd_hist,
                           bb_upper, bb_middle, bb_lower,
                           ema_9, ema_21, sma_50, sma_200,
                           atr, stoch_k, stoch_d, adx, obv
                    FROM market_data.technical_indicators
                    WHERE symbol = :symbol AND timeframe = :timeframe
                    ORDER BY time DESC
                    LIMIT 1
                """)
                result = session.execute(query, {'symbol': symbol, 'timeframe': timeframe})
                row = result.fetchone()

            if not row:
                return None

            return {
                'rsi': row[0], 'macd': row[1], 'macd_signal': row[2],
                'macd_hist': row[3], 'bb_upper': row[4], 'bb_middle': row[5],
                'bb_lower': row[6], 'ema_9': row[7], 'ema_21': row[8],
                'sma_50': row[9], 'sma_200': row[10], 'atr': row[11],
                'stoch_k': row[12], 'stoch_d': row[13], 'adx': row[14],
                'obv': row[15]
            }

        except Exception as e:
            self.logger.debug(f"No indicators for {symbol}/{timeframe}: {e}")
            return None

    def _get_ohlcv(self, symbol: str, timeframe: str, limit: int = 50) -> Optional[pd.DataFrame]:
        """يجلب بيانات OHLCV الأخيرة للسهم."""
        try:
            with db.get_session() as session:
                query = text("""
                    SELECT time, open, high, low, close, volume
                    FROM market_data.ohlcv
                    WHERE symbol = :symbol AND timeframe = :timeframe
                    ORDER BY time DESC
                    LIMIT :limit
                """)
                result = session.execute(query, {
                    'symbol': symbol, 'timeframe': timeframe, 'limit': limit
                })
                rows = result.fetchall()

            if not rows or len(rows) < 10:
                return None

            df = pd.DataFrame(rows, columns=['time', 'open', 'high', 'low', 'close', 'volume'])
            df['time'] = pd.to_datetime(df['time'])
            df = df.sort_values('time').reset_index(drop=True)
            df[['open', 'high', 'low', 'close']] = df[['open', 'high', 'low', 'close']].astype(float)
            df['volume'] = df['volume'].fillna(0).astype(float)
            return df

        except Exception as e:
            self.logger.debug(f"No OHLCV for {symbol}/{timeframe}: {e}")
            return None

    # ── المحرك الاستكشافي: تحليل بمعاملات الـ Scientist ──────────────────────

    def _analyze_with_scientist_params(
        self, symbol: str, scientist_result: Dict
    ) -> Optional[Dict]:
        """
        يُحلِّل السهم باستخدام المعاملات المُكتشَفة من الـ Scientist.
        هذه هي الاستراتيجية "الأفضل" لهذا السهم تحديداً.

        المعاملات المُستخدَمة من نتائج الـ Scientist:
          - rsi_period, rsi_buy, rsi_sell
          - bb_period, bb_std
          - macd_fast, macd_slow, macd_signal
          - stop_loss, take_profit
        """
        try:
            params = scientist_result.get('parameters', {})
            if not params:
                return None

            # استخراج المعاملات المُكتشَفة (مع قيم افتراضية آمنة)
            rsi_buy = float(params.get('rsi_buy', 30))
            rsi_sell = float(params.get('rsi_sell', 70))
            bb_std = float(params.get('bb_std', 2.0))
            stop_loss = float(params.get('stop_loss', 0.03))
            take_profit = float(params.get('take_profit', 0.05))
            sharpe = scientist_result.get('sharpe_ratio', 0)

            # جلب المؤشرات (نجرب أكثر من إطار زمني)
            indicators = (
                self._get_indicators(symbol, '1m') or
                self._get_indicators(symbol, '5m') or
                self._get_indicators(symbol, '1h')
            )

            if not indicators:
                return None

            # السعر الحالي
            current_price_data = redis_manager.get_cached_price(symbol)
            if not current_price_data:
                return None
            price = float(current_price_data.get('close', 0))
            if price <= 0:
                return None

            signal_type = 'HOLD'
            confidence = 0.0
            reasons = []

            # RSI بمعاملات الـ Scientist
            rsi = indicators.get('rsi')
            if rsi is not None:
                if rsi < rsi_buy:
                    signal_type = 'BUY'
                    confidence += 0.35
                    reasons.append(f'RSI={rsi:.1f} < Scientist threshold {rsi_buy}')
                elif rsi > rsi_sell:
                    signal_type = 'SELL'
                    confidence += 0.35
                    reasons.append(f'RSI={rsi:.1f} > Scientist threshold {rsi_sell}')

            # Bollinger Bands
            bb_lower = indicators.get('bb_lower')
            bb_upper = indicators.get('bb_upper')
            if bb_lower and bb_upper:
                band_width = (bb_upper - bb_lower) / bb_lower if bb_lower > 0 else 0
                # تعديل عتبة BB بناءً على bb_std من الـ Scientist
                adjusted_lower = bb_lower * (1 - (bb_std - 2.0) * 0.01)
                adjusted_upper = bb_upper * (1 + (bb_std - 2.0) * 0.01)

                if price <= adjusted_lower:
                    if signal_type == 'BUY':
                        confidence += 0.25
                    else:
                        signal_type = 'BUY'
                        confidence += 0.20
                    reasons.append(f'Price at/below Scientist-adjusted BB lower')
                elif price >= adjusted_upper:
                    if signal_type == 'SELL':
                        confidence += 0.25
                    else:
                        signal_type = 'SELL'
                        confidence += 0.20
                    reasons.append(f'Price at/above Scientist-adjusted BB upper')

            # MACD
            macd = indicators.get('macd')
            macd_sig = indicators.get('macd_signal')
            macd_hist = indicators.get('macd_hist')
            if macd is not None and macd_sig is not None:
                if macd > macd_sig and (macd_hist or 0) > 0:
                    if signal_type == 'BUY':
                        confidence += 0.20
                    reasons.append('MACD bullish')
                elif macd < macd_sig and (macd_hist or 0) < 0:
                    if signal_type == 'SELL':
                        confidence += 0.20
                    reasons.append('MACD bearish')

            # ADX (قوة الاتجاه)
            adx = indicators.get('adx')
            if adx and adx > 25:
                confidence += 0.10
                reasons.append(f'Strong trend ADX={adx:.1f}')

            # مكافأة جودة الـ Scientist (Sharpe ratio)
            if sharpe > 1.5:
                confidence += 0.10
                reasons.append(f'High-quality Scientist model (Sharpe={sharpe:.2f})')
            elif sharpe > 1.0:
                confidence += 0.05

            confidence = min(confidence, 1.0)

            # الحد الأدنى للثقة: 50%
            if signal_type != 'HOLD' and confidence >= 0.50:
                return {
                    'strategy': f"scientist_{symbol}",
                    'strategy_type': 'scientist_optimized',
                    'symbol': symbol,
                    'signal': signal_type,
                    'confidence': round(confidence, 4),
                    'price': price,
                    'timeframe': '1m',
                    'stop_loss_pct': stop_loss,
                    'take_profit_pct': take_profit,
                    'scientist_sharpe': sharpe,
                    'reasons': reasons,
                    'timestamp': get_saudi_time()
                }

            return None

        except Exception as e:
            self.logger.error(f"Error in scientist analysis for {symbol}: {e}")
            return None

    # ── التحليل العام: للأسهم التي لم يُحلّلها الـ Scientist بعد ──────────────

    def _analyze_generic(self, symbol: str) -> Optional[Dict]:
        """
        تحليل عام للأسهم التي لم يُجرِ عليها الـ Scientist بعد.
        يستخدم منطقاً بسيطاً لاكتشاف الفرص الواضحة فقط (ثقة عالية).
        الهدف: توليد إشارات أولية ريثما يُكمل الـ Scientist تحليله.
        """
        try:
            # نجرب أكثر من إطار زمني
            indicators = (
                self._get_indicators(symbol, '1m') or
                self._get_indicators(symbol, '5m')
            )
            if not indicators:
                return None

            current_price_data = redis_manager.get_cached_price(symbol)
            if not current_price_data:
                return None
            price = float(current_price_data.get('close', 0))
            if price <= 0:
                return None

            signal_type = 'HOLD'
            confidence = 0.0
            reasons = []

            rsi = indicators.get('rsi')
            macd = indicators.get('macd')
            macd_sig = indicators.get('macd_signal')
            macd_hist = indicators.get('macd_hist')
            bb_lower = indicators.get('bb_lower')
            bb_upper = indicators.get('bb_upper')
            adx = indicators.get('adx')

            # RSI متطرف جداً (عتبات صارمة لأن لا Scientist)
            if rsi is not None:
                if rsi < 25:
                    signal_type = 'BUY'
                    confidence += 0.30
                    reasons.append(f'RSI extremely oversold ({rsi:.1f})')
                elif rsi > 75:
                    signal_type = 'SELL'
                    confidence += 0.30
                    reasons.append(f'RSI extremely overbought ({rsi:.1f})')

            # MACD + BB تأكيد مزدوج
            if macd is not None and macd_sig is not None:
                if macd > macd_sig and (macd_hist or 0) > 0:
                    if signal_type == 'BUY':
                        confidence += 0.25
                        reasons.append('MACD confirms BUY')
                elif macd < macd_sig and (macd_hist or 0) < 0:
                    if signal_type == 'SELL':
                        confidence += 0.25
                        reasons.append('MACD confirms SELL')

            if bb_lower and bb_upper:
                if price <= bb_lower and signal_type == 'BUY':
                    confidence += 0.20
                    reasons.append('Price at BB lower')
                elif price >= bb_upper and signal_type == 'SELL':
                    confidence += 0.20
                    reasons.append('Price at BB upper')

            if adx and adx > 30:
                confidence += 0.10
                reasons.append(f'Strong trend ADX={adx:.1f}')

            confidence = min(confidence, 1.0)

            # عتبة أعلى للتحليل العام (65%) لأن لا Scientist
            if signal_type != 'HOLD' and confidence >= 0.65:
                return {
                    'strategy': 'generic_discovery',
                    'strategy_type': 'pending_scientist',
                    'symbol': symbol,
                    'signal': signal_type,
                    'confidence': round(confidence, 4),
                    'price': price,
                    'timeframe': '1m',
                    'stop_loss_pct': 0.03,
                    'take_profit_pct': 0.05,
                    'scientist_sharpe': None,
                    'reasons': reasons + ['[Pending Scientist optimization]'],
                    'timestamp': get_saudi_time()
                }

            return None

        except Exception as e:
            self.logger.debug(f"Generic analysis failed for {symbol}: {e}")
            return None

    # ── حفظ الإشارة ───────────────────────────────────────────────────────────

    def _save_signal(self, signal: Dict) -> bool:
        """يحفظ الإشارة في DB."""
        try:
            with db.get_session() as session:
                insert_signal(
                    session,
                    strategy_name=signal['strategy'],
                    symbol=signal['symbol'],
                    signal_type=signal['signal'],
                    price=signal['price'],
                    confidence=signal['confidence'],
                    timeframe=signal['timeframe'],
                    metadata={
                        'reasons': signal.get('reasons', []),
                        'strategy_type': signal.get('strategy_type'),
                        'scientist_sharpe': signal.get('scientist_sharpe'),
                        'stop_loss_pct': signal.get('stop_loss_pct'),
                        'take_profit_pct': signal.get('take_profit_pct'),
                    }
                )
            redis_manager.cache_signal(signal['strategy'], signal['symbol'], signal)
            return True
        except Exception as e:
            self.logger.error(f"Failed to save signal for {signal['symbol']}: {e}")
            return False

    # ── الدالة الرئيسية ────────────────────────────────────────────────────────

    def run(self, symbols: List[str] = None) -> List[Dict]:
        """
        يُشغّل التحليل الاستكشافي على كون الأسهم الكامل.

        الخطوات:
          1. اكتشاف الأسهم النشطة من DB (بناءً على سيولتها الذاتية)
          2. لكل سهم: جلب أفضل استراتيجية من الـ Scientist
          3. إذا وُجدت → تحليل بمعاملاتها المخصصة
          4. إذا لم تُوجد → تحليل عام + إضافة لقائمة انتظار الـ Scientist
          5. حفظ الإشارات الناجحة في DB
        """
        try:
            self.logger.info("🚀 Strategic Analyzer starting (exploratory mode)")

            # ── 1. اكتشاف الأسهم النشطة ──────────────────────────────────────
            if symbols:
                active_symbols = symbols
                self.logger.info(f"Using provided symbols: {len(active_symbols)}")
            else:
                active_symbols, classifications = symbol_universe.get_active_universe(
                    timeframe='1m',
                    use_cache=True,
                    include_awakening=True
                )

            if not active_symbols:
                self.logger.warning(
                    "⚠️ No active symbols found. "
                    "Either DB is empty or all symbols are DORMANT. "
                    "Waiting for market data..."
                )
                return []

            self.logger.info(
                f"📊 Active universe: {len(active_symbols)} symbols to analyze"
            )

            # ── 2. تحليل كل سهم ──────────────────────────────────────────────
            all_signals = []
            scientist_optimized = 0
            pending_scientist = 0
            needs_scientist_queue = []

            max_signals = self.bot_config.get('max_signals_per_run', 100)

            for symbol in active_symbols:
                if len(all_signals) >= max_signals:
                    break

                # جلب أفضل استراتيجية من الـ Scientist
                best_strategy = symbol_universe.get_best_strategy_for_symbol(symbol)

                if best_strategy:
                    # ── تحليل بمعاملات الـ Scientist ──────────────────────
                    signal = self._analyze_with_scientist_params(symbol, best_strategy)
                    if signal:
                        all_signals.append(signal)
                        scientist_optimized += 1
                        self.logger.success(
                            f"✅ [{signal['signal']}] {symbol} "
                            f"[scientist_optimized | Sharpe={best_strategy['sharpe_ratio']:.2f}] "
                            f"conf={signal['confidence']:.2f}"
                        )
                else:
                    # ── لا Scientist بعد → تحليل عام + أضف لقائمة الانتظار ──
                    needs_scientist_queue.append(symbol)
                    signal = self._analyze_generic(symbol)
                    if signal:
                        all_signals.append(signal)
                        pending_scientist += 1
                        self.logger.info(
                            f"📌 [{signal['signal']}] {symbol} "
                            f"[pending_scientist] conf={signal['confidence']:.2f}"
                        )

            # ── 3. حفظ الإشارات ──────────────────────────────────────────────
            saved = 0
            for signal in all_signals:
                if self._save_signal(signal):
                    saved += 1

            # ── 4. تسجيل الأسهم التي تحتاج Scientist في Redis ───────────────
            if needs_scientist_queue:
                redis_manager.set(
                    "scientist:pending_symbols",
                    needs_scientist_queue[:50],  # أولوية أول 50
                    ttl=3600
                )
                self.logger.info(
                    f"🔬 Queued {len(needs_scientist_queue)} symbols for Scientist analysis"
                )

            # ── 5. ملخص ──────────────────────────────────────────────────────
            self.logger.success(
                f"✅ Strategic Analyzer complete: "
                f"{len(all_signals)} signals generated "
                f"({scientist_optimized} scientist-optimized, {pending_scientist} pending) | "
                f"{saved} saved to DB"
            )

            return all_signals

        except Exception as e:
            self.logger.error(f"Error in Strategic Analyzer: {e}")
            raise


if __name__ == "__main__":
    bot = StrategicAnalyzer()
    signals = bot.run()
    print(f"\nGenerated {len(signals)} signals")
    for s in signals[:5]:
        print(f"  {s['symbol']}: {s['signal']} [{s['strategy']}] conf={s['confidence']:.2f}")
