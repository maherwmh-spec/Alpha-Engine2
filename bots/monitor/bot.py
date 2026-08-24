"""
Bot 5: Monitor (المُراقِب)
Monitors signals and sends final alerts
Phase 5: also checks active_monitoring + paper trades via AnalyzeService

Price moves read from market_data.ohlcv (1m preferred) — stock_prices table removed/legacy.
"""

from typing import List, Dict, Tuple
from loguru import logger
from datetime import datetime, timedelta

from sqlalchemy import text
from config.config_manager import config
from scripts.database import db, insert_alert
from scripts.redis_manager import redis_manager
from scripts.utils import get_saudi_time, calculate_percentage_change


class Monitor:
    """Monitoring and alert generation bot"""
    
    def __init__(self):
        self.name = "monitor"
        self.logger = logger.bind(bot=self.name)
        self.config = config.get_bot_config(self.name)
        self.alert_threshold = self.config.get('alert_threshold', 0.01)  # 1%
    
    def check_price_movements(self) -> List[Dict]:
        """Check for significant price movements using market_data.ohlcv."""
        try:
            alerts = []
            
            with db.get_session() as session:
                # Prefer 1m bars in the last hour; fall back to any timeframe if needed
                query = text("""
                SELECT DISTINCT symbol FROM market_data.ohlcv
                WHERE time > NOW() - INTERVAL '1 hour'
                  AND timeframe = '1m'
                """)
                result = session.execute(query)
                symbols = [row[0] for row in result.fetchall()]
                if not symbols:
                    query = text("""
                    SELECT DISTINCT symbol FROM market_data.ohlcv
                    WHERE time > NOW() - INTERVAL '1 day'
                      AND timeframe IN ('15m', '30m', '1d')
                    """)
                    result = session.execute(query)
                    symbols = [row[0] for row in result.fetchall()]
            
            for symbol in symbols:
                with db.get_session() as session:
                    query = text("""
                    SELECT close FROM market_data.ohlcv
                    WHERE symbol = :symbol
                      AND timeframe = '1m'
                    ORDER BY time DESC
                    LIMIT 2
                    """)
                    result = session.execute(query, {'symbol': symbol})
                    prices = [row[0] for row in result.fetchall()]
                    if len(prices) < 2:
                        query = text("""
                        SELECT close FROM market_data.ohlcv
                        WHERE symbol = :symbol
                        ORDER BY time DESC
                        LIMIT 2
                        """)
                        result = session.execute(query, {'symbol': symbol})
                        prices = [row[0] for row in result.fetchall()]
                
                if len(prices) >= 2:
                    change_pct = calculate_percentage_change(prices[1], prices[0])
                    
                    if abs(change_pct) >= self.alert_threshold * 100:
                        alert = {
                            'symbol': symbol,
                            'type': 'PRICE_MOVEMENT',
                            'change': change_pct,
                            'current_price': prices[0],
                            'previous_price': prices[1]
                        }
                        alerts.append(alert)
                        
                        with db.get_session() as session:
                            insert_alert(
                                session,
                                alert_type='PRICE_MOVEMENT',
                                priority=1 if abs(change_pct) > 3 else 2,
                                title=f'{symbol}: {change_pct:+.2f}% movement',
                                message=f'Price moved from {prices[1]:.2f} to {prices[0]:.2f}',
                                symbol=symbol
                            )
            
            return alerts
            
        except Exception as e:
            self.logger.error(f"Error checking price movements: {e}")
            return []
    
    def check_signals(self) -> List[Dict]:
        """Check for new trading signals"""
        try:
            alerts = []
            
            with db.get_session() as session:
                query = text("""
                SELECT strategy_name, symbol, signal_type, confidence, price
                FROM strategies.signals
                WHERE timestamp > NOW() - INTERVAL '10 minutes'
                AND confidence >= 0.7
                ORDER BY confidence DESC
                """)
                result = session.execute(query)
                signals = result.fetchall()
            
            for strategy, symbol, signal_type, confidence, price in signals:
                alert = {
                    'symbol': symbol,
                    'type': 'SIGNAL',
                    'strategy': strategy,
                    'signal': signal_type,
                    'confidence': confidence,
                    'price': price
                }
                alerts.append(alert)
                
                with db.get_session() as session:
                    insert_alert(
                        session,
                        alert_type='SIGNAL',
                        priority=1,
                        title=f'🎯 {strategy}: {signal_type} {symbol}',
                        message=f'Confidence: {confidence:.0%} | Price: {price:.2f}',
                        symbol=symbol,
                        strategy_name=strategy
                    )
            
            return alerts
            
        except Exception as e:
            self.logger.error(f"Error checking signals: {e}")
            return []

    def check_phase5_active(self) -> Dict:
        """Phase 5: active monitoring + paper SL/TP."""
        try:
            from scripts.analyze_service import AnalyzeService
            result = AnalyzeService().check_active_and_papers()
            n = len(result.get("alerts") or [])
            if n:
                self.logger.info(f"Phase5 active/paper alerts: {n}")
            return result
        except Exception as e:
            self.logger.error(f"Phase5 active check failed: {e}")
            return {"alerts": [], "error": str(e)}
    
    def run(self):
        """Run monitor bot"""
        try:
            self.logger.info("Starting Monitor")
            
            price_alerts = self.check_price_movements()
            signal_alerts = self.check_signals()
            phase5 = self.check_phase5_active()
            
            total_alerts = len(price_alerts) + len(signal_alerts) + len(phase5.get("alerts") or [])
            
            self.logger.success(f"Monitor completed: {total_alerts} alerts generated")
            return {
                'price_alerts': price_alerts,
                'signal_alerts': signal_alerts,
                'phase5': phase5,
            }
            
        except Exception as e:
            self.logger.error(f"Error in Monitor: {e}")
            raise

    def _get_relative_volume(self, symbol: str) -> float:
        try:
            cached = redis_manager.get(f"relative_volume:{symbol}")
            if cached is not None:
                return float(cached)

            with db.get_session() as session:
                result = session.execute(
                    text("""
                    SELECT volume FROM market_data.ohlcv
                    WHERE symbol = :symbol AND timeframe = '1d'
                    ORDER BY time DESC LIMIT 21
                    """),
                    {'symbol': symbol}
                )
                rows = result.fetchall()

            if not rows or len(rows) < 2:
                return None

            import numpy as np
            volumes = [float(r[0]) for r in rows]
            current_vol = volumes[0]
            avg_vol_20d = float(np.mean(volumes[1:21])) if len(volumes) > 1 else current_vol

            if avg_vol_20d <= 0:
                return None

            rel_vol = current_vol / avg_vol_20d
            redis_manager.set(f"relative_volume:{symbol}", rel_vol, ttl=1800)
            return rel_vol

        except Exception as e:
            self.logger.error(f"[RelVol] Error for {symbol}: {e}")
            return None

    def apply_contextual_filters(self, signal: Dict) -> Tuple[bool, str]:
        try:
            symbol = signal.get('symbol')
            signal_type = signal.get('type')
            strategy = signal.get('strategy')
            
            if signal_type == 'BUY':
                tasi_change = redis_manager.get('tasi_daily_change', 0.0)
                if tasi_change <= -0.02:
                    return False, f"TASI down {tasi_change:.2%} - no BUY signals"
            
            sector = redis_manager.get(f"symbol_sector:{symbol}", "Unknown")
            sector_trend = redis_manager.get(f"sector_trend:{sector}", "neutral")
            
            if signal_type == 'BUY' and sector_trend == 'downtrend':
                return False, f"Sector {sector} in downtrend - no BUY"
            elif signal_type == 'SELL' and sector_trend == 'uptrend':
                return False, f"Sector {sector} in uptrend - no SELL"
            
            if strategy == 'aggressive_daily':
                current_atr = redis_manager.get(f"atr:{symbol}", 0.0)
                avg_atr_20d = redis_manager.get(f"atr_20d_avg:{symbol}", 0.0)
                if avg_atr_20d > 0:
                    atr_ratio = current_atr / avg_atr_20d
                    if atr_ratio < 0.8:
                        return False, f"Low ATR ({atr_ratio:.2f}x) - no aggressive signal"
            
            return True, "Passed all contextual filters"
            
        except Exception as e:
            self.logger.error(f"Error applying contextual filters: {e}")
            return True, "Error in filters - allowing signal"
    
    def apply_signal_decay(self, signal: Dict) -> Tuple[bool, float]:
        try:
            strategy = signal.get('strategy')
            signal_time = signal.get('timestamp')
            
            if not signal_time:
                return True, 1.0
            
            lifetimes = {
                'aggressive_daily': 30,
                'short_waves': 240,
                'medium_waves': 480,
                'price_explosions': 1440
            }
            
            lifetime = lifetimes.get(strategy, 60)
            now = get_saudi_time()
            age_minutes = (now - signal_time).total_seconds() / 60
            
            if age_minutes > lifetime:
                return False, 0.0
            
            confidence_multiplier = 1.0 - (0.5 * age_minutes / lifetime)
            confidence_multiplier = max(0.5, min(1.0, confidence_multiplier))
            return True, confidence_multiplier
            
        except Exception as e:
            self.logger.error(f"Error applying signal decay: {e}")
            return True, 1.0
    
    def process_signal_with_filters(self, signal: Dict) -> Dict:
        try:
            should_send, filter_reason = self.apply_contextual_filters(signal)
            if not should_send:
                signal['filtered'] = True
                signal['filter_reason'] = filter_reason
                return signal
            
            is_valid, confidence = self.apply_signal_decay(signal)
            if not is_valid:
                signal['expired'] = True
                signal['filter_reason'] = "Signal expired"
                return signal
            
            original_confidence = signal.get('confidence', 1.0)
            signal['confidence'] = original_confidence * confidence
            signal['decay_applied'] = True
            signal['filtered'] = False
            return signal
            
        except Exception as e:
            self.logger.error(f"Error processing signal: {e}")
            return signal

    def apply_strict_edge_filter(self, signal: Dict) -> Tuple[bool, str]:
        try:
            symbol = signal.get('symbol', 'Unknown')
            fitness_score = signal.get('fitness_score', 0.0)
            if fitness_score < 0.92:
                return False, f"Fitness too low: {fitness_score:.4f} < 0.92"
            
            confidence = signal.get('confidence', 0.0)
            if confidence < 0.85:
                return False, f"Confidence too low: {confidence:.2%} < 85%"
            
            timeframe_confirmations = signal.get('timeframe_confirmations', [])
            if len(timeframe_confirmations) < 3:
                return False, f"Insufficient timeframe confirmations: {len(timeframe_confirmations)} < 3"
            
            market_structure = signal.get('market_structure', {})
            if not market_structure or not market_structure.get('confirmed', False):
                return False, "Market Structure not confirmed"
            
            volume_profile = signal.get('volume_profile', {})
            if not volume_profile or not volume_profile.get('confirmed', False):
                return False, "Volume Profile not confirmed"

            rel_vol = self._get_relative_volume(symbol)
            rel_vol_threshold = config.get('liquidity_filter.relative_volume_threshold', 1.5)
            if rel_vol is not None and rel_vol < rel_vol_threshold:
                return False, f"Relative volume too low: {rel_vol:.2f}x < {rel_vol_threshold}x"

            return True, "All edge criteria met"
            
        except Exception as e:
            self.logger.error(f"Error applying edge filter: {e}")
            return False, f"Error: {str(e)}"
    
    def enrich_signal_with_confirmations(self, signal: Dict) -> Dict:
        try:
            symbol = signal.get('symbol')
            timeframe_confirmations = []
            for tf in ['5m', '15m', '1h']:
                cached = redis_manager.get(f"signal_confirmation:{symbol}:{tf}")
                if cached and cached.get('confirmed'):
                    timeframe_confirmations.append(tf)
            signal['timeframe_confirmations'] = timeframe_confirmations
            market_structure = redis_manager.get(f"market_structure:{symbol}")
            signal['market_structure'] = market_structure or {'confirmed': False}
            volume_profile = redis_manager.get(f"volume_profile:{symbol}")
            signal['volume_profile'] = volume_profile or {'confirmed': False}
            return signal
        except Exception as e:
            self.logger.error(f"Error enriching signal: {e}")
            return signal
    
    def process_signal_with_edge_filter(self, signal: Dict) -> Dict:
        try:
            signal = self.enrich_signal_with_confirmations(signal)
            passes, reason = self.apply_strict_edge_filter(signal)
            signal['edge_filter_passed'] = passes
            signal['edge_filter_reason'] = reason
            return signal
        except Exception as e:
            self.logger.error(f"Error processing signal with edge filter: {e}")
            signal['edge_filter_passed'] = False
            signal['edge_filter_reason'] = f"Error: {str(e)}"
            return signal
