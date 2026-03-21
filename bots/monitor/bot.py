"""
Bot 5: Monitor (المُراقِب)
Monitors signals and sends final alerts
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
        """Check for significant price movements"""
        try:
            alerts = []
            
            # Get all symbols with recent price data
            with db.get_session() as session:
                query = text("""
                SELECT DISTINCT symbol FROM market_data.stock_prices
                WHERE time > NOW() - INTERVAL '1 hour'
                """)
                result = session.execute(query)
                symbols = [row[0] for row in result.fetchall()]
            
            for symbol in symbols:
                # Get current and previous price
                with db.get_session() as session:
                    query = text("""
                    SELECT close FROM market_data.stock_prices
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
                        
                        # Save alert to database
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
            
            # Get recent signals
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
                
                # Save alert
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
    
    def run(self):
        """Run monitor bot"""
        try:
            self.logger.info("Starting Monitor")
            
            price_alerts = self.check_price_movements()
            signal_alerts = self.check_signals()
            
            total_alerts = len(price_alerts) + len(signal_alerts)
            
            self.logger.success(f"Monitor completed: {total_alerts} alerts generated")
            return {'price_alerts': price_alerts, 'signal_alerts': signal_alerts}
            
        except Exception as e:
            self.logger.error(f"Error in Monitor: {e}")
            raise

    def _get_relative_volume(self, symbol: str) -> float:
        """
        Calculate relative volume for a symbol: current volume / 20-day average volume.
        Fetches from TimescaleDB (market_data.ohlcv, timeframe='1d').
        Returns float or None if data unavailable.
        """
        try:
            # Check Redis cache first
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
            # Cache for 30 minutes
            redis_manager.set(f"relative_volume:{symbol}", rel_vol, ttl=1800)
            return rel_vol

        except Exception as e:
            self.logger.error(f"[RelVol] Error for {symbol}: {e}")
            return None

    def apply_contextual_filters(self, signal: Dict) -> Tuple[bool, str]:
        """
        Apply contextual filters before sending signal
        
        Filters:
        1. No BUY signal if TASI is down -2% or more
        2. No signal against sector daily trend
        3. No aggressive daily signal if ATR < 0.8x average ATR 20-day
        
        Returns:
            Tuple of (should_send, rejection_reason)
        """
        try:
            symbol = signal.get('symbol')
            signal_type = signal.get('type')  # BUY or SELL
            strategy = signal.get('strategy')
            
            # Filter 1: TASI check for BUY signals
            if signal_type == 'BUY':
                tasi_change = redis_manager.get('tasi_daily_change', 0.0)
                
                if tasi_change <= -0.02:  # -2% or worse
                    return False, f"TASI down {tasi_change:.2%} - no BUY signals"
            
            # Filter 2: Sector trend check
            sector = redis_manager.get(f"symbol_sector:{symbol}", "Unknown")
            sector_trend = redis_manager.get(f"sector_trend:{sector}", "neutral")
            
            if signal_type == 'BUY' and sector_trend == 'downtrend':
                return False, f"Sector {sector} in downtrend - no BUY"
            elif signal_type == 'SELL' and sector_trend == 'uptrend':
                return False, f"Sector {sector} in uptrend - no SELL"
            
            # Filter 3: ATR check for aggressive daily strategy
            if strategy == 'aggressive_daily':
                current_atr = redis_manager.get(f"atr:{symbol}", 0.0)
                avg_atr_20d = redis_manager.get(f"atr_20d_avg:{symbol}", 0.0)
                
                if avg_atr_20d > 0:
                    atr_ratio = current_atr / avg_atr_20d
                    
                    if atr_ratio < 0.8:
                        return False, f"Low ATR ({atr_ratio:.2f}x) - no aggressive signal"
            
            # All filters passed
            return True, "Passed all contextual filters"
            
        except Exception as e:
            self.logger.error(f"Error applying contextual filters: {e}")
            return True, "Error in filters - allowing signal"
    
    def apply_signal_decay(self, signal: Dict) -> Tuple[bool, float]:
        """
        Apply signal decay based on age
        
        Signal lifetimes:
        - Aggressive daily: 30 minutes
        - Short waves: 4 hours
        - Medium waves: 8 hours
        - Price explosions: 24 hours
        
        Returns:
            Tuple of (is_valid, confidence_multiplier)
        """
        try:
            strategy = signal.get('strategy')
            signal_time = signal.get('timestamp')
            
            if not signal_time:
                return True, 1.0
            
            # Define lifetimes (in minutes)
            lifetimes = {
                'aggressive_daily': 30,
                'short_waves': 240,  # 4 hours
                'medium_waves': 480,  # 8 hours
                'price_explosions': 1440  # 24 hours
            }
            
            lifetime = lifetimes.get(strategy, 60)  # Default 1 hour
            
            # Calculate age
            now = get_saudi_time()
            age_minutes = (now - signal_time).total_seconds() / 60
            
            # Check if expired
            if age_minutes > lifetime:
                self.logger.info(f"Signal expired: {strategy} signal aged {age_minutes:.0f}min > {lifetime}min")
                return False, 0.0
            
            # Calculate confidence decay (linear)
            # Confidence = 1.0 at age 0, decays to 0.5 at lifetime
            confidence_multiplier = 1.0 - (0.5 * age_minutes / lifetime)
            confidence_multiplier = max(0.5, min(1.0, confidence_multiplier))
            
            if confidence_multiplier < 0.8:
                self.logger.info(f"Signal decaying: {strategy} confidence={confidence_multiplier:.2f}")
            
            return True, confidence_multiplier
            
        except Exception as e:
            self.logger.error(f"Error applying signal decay: {e}")
            return True, 1.0
    
    def process_signal_with_filters(self, signal: Dict) -> Dict:
        """
        Process signal through all filters
        
        Returns:
            Processed signal with filter results
        """
        try:
            # Apply contextual filters
            should_send, filter_reason = self.apply_contextual_filters(signal)
            
            if not should_send:
                signal['filtered'] = True
                signal['filter_reason'] = filter_reason
                self.logger.info(f"Signal FILTERED: {signal['symbol']} - {filter_reason}")
                return signal
            
            # Apply signal decay
            is_valid, confidence = self.apply_signal_decay(signal)
            
            if not is_valid:
                signal['expired'] = True
                signal['filter_reason'] = "Signal expired"
                self.logger.info(f"Signal EXPIRED: {signal['symbol']}")
                return signal
            
            # Adjust confidence
            original_confidence = signal.get('confidence', 1.0)
            signal['confidence'] = original_confidence * confidence
            signal['decay_applied'] = True
            signal['filtered'] = False
            
            self.logger.success(f"Signal PASSED: {signal['symbol']} (confidence={signal['confidence']:.2f})")
            return signal
            
        except Exception as e:
            self.logger.error(f"Error processing signal: {e}")
            return signal

    def apply_strict_edge_filter(self, signal: Dict) -> Tuple[bool, str]:
        """
        Apply strict edge filter to signal
        
        Only send signal if ALL conditions are met:
        1. Fitness score ≥ 0.92
        2. Confidence (after Sentiment + Contextual Filters) ≥ 85%
        3. Confirmation from 3 timeframes + Market Structure + Volume Profile
        
        Args:
            signal: Trading signal to filter
            
        Returns:
            Tuple of (passes_filter: bool, reason: str)
        """
        try:
            symbol = signal.get('symbol', 'Unknown')
            
            # 1. Check Fitness Score
            fitness_score = signal.get('fitness_score', 0.0)
            if fitness_score < 0.92:
                reason = f"Fitness too low: {fitness_score:.4f} < 0.92"
                self.logger.info(f"{symbol}: REJECTED by Edge Filter - {reason}")
                return False, reason
            
            # 2. Check Confidence (after adjustments)
            confidence = signal.get('confidence', 0.0)
            if confidence < 0.85:
                reason = f"Confidence too low: {confidence:.2%} < 85%"
                self.logger.info(f"{symbol}: REJECTED by Edge Filter - {reason}")
                return False, reason
            
            # 3. Check Multi-Timeframe Confirmation
            timeframe_confirmations = signal.get('timeframe_confirmations', [])
            if len(timeframe_confirmations) < 3:
                reason = f"Insufficient timeframe confirmations: {len(timeframe_confirmations)} < 3"
                self.logger.info(f"{symbol}: REJECTED by Edge Filter - {reason}")
                return False, reason
            
            # 4. Check Market Structure Confirmation
            market_structure = signal.get('market_structure', {})
            if not market_structure or not market_structure.get('confirmed', False):
                reason = "Market Structure not confirmed"
                self.logger.info(f"{symbol}: REJECTED by Edge Filter - {reason}")
                return False, reason
            
            # 5. Check Volume Profile Confirmation
            volume_profile = signal.get('volume_profile', {})
            if not volume_profile or not volume_profile.get('confirmed', False):
                reason = "Volume Profile not confirmed"
                self.logger.info(f"{symbol}: REJECTED by Edge Filter - {reason}")
                return False, reason

            # 6. Relative Volume Filter (per-symbol, no price floor)
            rel_vol = self._get_relative_volume(symbol)
            rel_vol_threshold = config.get('liquidity_filter.relative_volume_threshold', 1.5)
            if rel_vol is not None and rel_vol < rel_vol_threshold:
                reason = f"Relative volume too low: {rel_vol:.2f}x < {rel_vol_threshold}x"
                self.logger.info(f"{symbol}: REJECTED by Edge Filter - {reason}")
                return False, reason

            # All checks passed
            rel_vol_str = f", RelVol={rel_vol:.2f}x" if rel_vol is not None else ""
            self.logger.success(
                f"{symbol}: PASSED Edge Filter - "
                f"Fitness={fitness_score:.4f}, Confidence={confidence:.2%}, "
                f"Timeframes={len(timeframe_confirmations)}, MS=✓, VP=✓{rel_vol_str}"
            )

            return True, "All edge criteria met"
            
        except Exception as e:
            self.logger.error(f"Error applying edge filter: {e}")
            return False, f"Error: {str(e)}"
    
    def enrich_signal_with_confirmations(self, signal: Dict) -> Dict:
        """
        Enrich signal with multi-timeframe, market structure, and volume profile confirmations
        
        This function fetches additional data to support the edge filter
        
        Args:
            signal: Base signal
            
        Returns:
            Enriched signal with confirmation data
        """
        try:
            symbol = signal.get('symbol')
            
            # Get multi-timeframe confirmations from cache
            timeframe_confirmations = []
            for tf in ['5m', '15m', '1h']:
                cached = redis_manager.get(f"signal_confirmation:{symbol}:{tf}")
                if cached and cached.get('confirmed'):
                    timeframe_confirmations.append(tf)
            
            signal['timeframe_confirmations'] = timeframe_confirmations
            
            # Get market structure confirmation
            market_structure = redis_manager.get(f"market_structure:{symbol}")
            if market_structure:
                signal['market_structure'] = market_structure
            else:
                signal['market_structure'] = {'confirmed': False}
            
            # Get volume profile confirmation
            volume_profile = redis_manager.get(f"volume_profile:{symbol}")
            if volume_profile:
                signal['volume_profile'] = volume_profile
            else:
                signal['volume_profile'] = {'confirmed': False}
            
            return signal
            
        except Exception as e:
            self.logger.error(f"Error enriching signal: {e}")
            return signal
    
    def process_signal_with_edge_filter(self, signal: Dict) -> Dict:
        """
        Process signal through strict edge filter
        
        Args:
            signal: Trading signal
            
        Returns:
            Processed signal with filter result
        """
        try:
            # Enrich signal with confirmations
            signal = self.enrich_signal_with_confirmations(signal)
            
            # Apply edge filter
            passes, reason = self.apply_strict_edge_filter(signal)
            
            signal['edge_filter_passed'] = passes
            signal['edge_filter_reason'] = reason
            
            if not passes:
                self.logger.warning(
                    f"{signal['symbol']}: Signal REJECTED by Edge Filter - {reason}"
                )
            
            return signal
            
        except Exception as e:
            self.logger.error(f"Error processing signal with edge filter: {e}")
            signal['edge_filter_passed'] = False
            signal['edge_filter_reason'] = f"Error: {str(e)}"
            return signal
