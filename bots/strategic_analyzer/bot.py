"""
Bot 4: Strategic Analyzer (المُحلِّل الاستراتيجي)
Analyzes strategies and generates trading signals
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from loguru import logger

from sqlalchemy import text
from config.config_manager import config
from scripts.database import db, insert_signal
from scripts.redis_manager import redis_manager
from scripts.utils import (
    get_saudi_time, calculate_percentage_change,
    is_support_level, is_resistance_level
)
import json


class StrategicAnalyzer:
    """Strategy analysis and signal generation bot"""
    
    def __init__(self):
        self.name = "strategic_analyzer"
        self.logger = logger.bind(bot=self.name)
        self.config = config.get_bot_config(self.name)
        
        # Load strategy configurations
        self.strategies = {
            'aggressive_daily': config.get_strategy_config('aggressive_daily'),
            'short_waves': config.get_strategy_config('short_waves'),
            'medium_waves': config.get_strategy_config('medium_waves'),
            'price_explosions': config.get_strategy_config('price_explosions')
        }
    
    def get_latest_indicators(self, symbol: str, timeframe: str) -> Optional[Dict]:
        """Get latest technical indicators for a symbol"""
        try:
            # Try cache first
            cached = redis_manager.get_cached_indicators(symbol, timeframe)
            if cached:
                return cached
            
            # Get from database
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
            
            indicators = {
                'rsi': row[0],
                'macd': row[1],
                'macd_signal': row[2],
                'macd_hist': row[3],
                'bb_upper': row[4],
                'bb_middle': row[5],
                'bb_lower': row[6],
                'ema_9': row[7],
                'ema_21': row[8],
                'sma_50': row[9],
                'sma_200': row[10],
                'atr': row[11],
                'stoch_k': row[12],
                'stoch_d': row[13],
                'adx': row[14],
                'obv': row[15]
            }
            
            return indicators
            
        except Exception as e:
            self.logger.error(f"Error getting indicators for {symbol}: {e}")
            return None
    
    def analyze_aggressive_daily(self, symbol: str) -> Optional[Dict]:
        """
        Analyze for aggressive daily strategy (1-3% quick trades)
        Timeframe: 1m
        """
        try:
            strategy = self.strategies['aggressive_daily']
            if not strategy.get('enabled', False):
                return None
            
            timeframe = strategy.get('timeframe', '1m')
            indicators = self.get_latest_indicators(symbol, timeframe)
            
            if not indicators:
                return None
            
            # Get current price
            current_price = redis_manager.get_cached_price(symbol)
            if not current_price:
                return None
            
            price = current_price.get('close', 0)
            if price == 0:
                return None
            
            # Signal generation logic
            signal_type = 'HOLD'
            confidence = 0.0
            reasons = []
            
            # RSI oversold/overbought
            if indicators['rsi'] and indicators['rsi'] < 30:
                signal_type = 'BUY'
                confidence += 0.3
                reasons.append('RSI oversold')
            elif indicators['rsi'] and indicators['rsi'] > 70:
                signal_type = 'SELL'
                confidence += 0.3
                reasons.append('RSI overbought')
            
            # MACD crossover
            if indicators['macd'] and indicators['macd_signal']:
                if indicators['macd'] > indicators['macd_signal'] and indicators['macd_hist'] > 0:
                    if signal_type == 'BUY':
                        confidence += 0.2
                    else:
                        signal_type = 'BUY'
                        confidence += 0.15
                    reasons.append('MACD bullish crossover')
                elif indicators['macd'] < indicators['macd_signal'] and indicators['macd_hist'] < 0:
                    if signal_type == 'SELL':
                        confidence += 0.2
                    else:
                        signal_type = 'SELL'
                        confidence += 0.15
                    reasons.append('MACD bearish crossover')
            
            # Bollinger Bands
            if indicators['bb_lower'] and indicators['bb_upper']:
                if price <= indicators['bb_lower']:
                    if signal_type == 'BUY':
                        confidence += 0.25
                    else:
                        signal_type = 'BUY'
                        confidence += 0.15
                    reasons.append('Price at lower Bollinger Band')
                elif price >= indicators['bb_upper']:
                    if signal_type == 'SELL':
                        confidence += 0.25
                    else:
                        signal_type = 'SELL'
                        confidence += 0.15
                    reasons.append('Price at upper Bollinger Band')
            
            # Stochastic
            if indicators['stoch_k'] and indicators['stoch_d']:
                if indicators['stoch_k'] < 20 and indicators['stoch_d'] < 20:
                    if signal_type == 'BUY':
                        confidence += 0.15
                    reasons.append('Stochastic oversold')
                elif indicators['stoch_k'] > 80 and indicators['stoch_d'] > 80:
                    if signal_type == 'SELL':
                        confidence += 0.15
                    reasons.append('Stochastic overbought')
            
            # Normalize confidence
            confidence = min(confidence, 1.0)
            
            if signal_type != 'HOLD' and confidence >= 0.5:
                result = {
                    'strategy': 'aggressive_daily',
                    'symbol': symbol,
                    'signal': signal_type,
                    'confidence': confidence,
                    'price': price,
                    'timeframe': timeframe,
                    'reasons': reasons,
                    'timestamp': get_saudi_time()
                }
                
                self.logger.info(f"Signal: {symbol} - {signal_type} (confidence: {confidence:.2f})")
                return result
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error analyzing aggressive_daily for {symbol}: {e}")
            return None
    
    def analyze_short_waves(self, symbol: str) -> Optional[Dict]:
        """
        Analyze for short waves strategy (1-7 days)
        Timeframe: 5m
        """
        try:
            strategy = self.strategies['short_waves']
            if not strategy.get('enabled', False):
                return None
            
            timeframe = strategy.get('timeframe', '5m')
            indicators = self.get_latest_indicators(symbol, timeframe)
            
            if not indicators:
                return None
            
            current_price = redis_manager.get_cached_price(symbol)
            if not current_price:
                return None
            
            price = current_price.get('close', 0)
            
            signal_type = 'HOLD'
            confidence = 0.0
            reasons = []
            
            # EMA crossover (9 and 21)
            if indicators['ema_9'] and indicators['ema_21']:
                if indicators['ema_9'] > indicators['ema_21']:
                    signal_type = 'BUY'
                    confidence += 0.35
                    reasons.append('EMA 9 > EMA 21')
                elif indicators['ema_9'] < indicators['ema_21']:
                    signal_type = 'SELL'
                    confidence += 0.35
                    reasons.append('EMA 9 < EMA 21')
            
            # ADX for trend strength
            if indicators['adx'] and indicators['adx'] > 25:
                confidence += 0.2
                reasons.append(f'Strong trend (ADX: {indicators["adx"]:.1f})')
            
            # RSI confirmation
            if indicators['rsi']:
                if signal_type == 'BUY' and 40 < indicators['rsi'] < 60:
                    confidence += 0.15
                    reasons.append('RSI in neutral zone')
                elif signal_type == 'SELL' and 40 < indicators['rsi'] < 60:
                    confidence += 0.15
                    reasons.append('RSI in neutral zone')
            
            # Volume confirmation (OBV)
            if indicators['obv']:
                # Get previous OBV
                with db.get_session() as session:
                    query = text("""
                    SELECT obv FROM market_data.technical_indicators
                    WHERE symbol = :symbol AND timeframe = :timeframe
                    ORDER BY time DESC
                    LIMIT 2
                    """)
                    result = session.execute(query, {'symbol': symbol, 'timeframe': timeframe})
                    obv_values = [row[0] for row in result.fetchall()]
                
                if len(obv_values) >= 2:
                    if signal_type == 'BUY' and obv_values[0] > obv_values[1]:
                        confidence += 0.15
                        reasons.append('Volume increasing')
                    elif signal_type == 'SELL' and obv_values[0] < obv_values[1]:
                        confidence += 0.15
                        reasons.append('Volume decreasing')
            
            confidence = min(confidence, 1.0)
            
            if signal_type != 'HOLD' and confidence >= 0.6:
                result = {
                    'strategy': 'short_waves',
                    'symbol': symbol,
                    'signal': signal_type,
                    'confidence': confidence,
                    'price': price,
                    'timeframe': timeframe,
                    'reasons': reasons,
                    'timestamp': get_saudi_time()
                }
                
                self.logger.info(f"Signal: {symbol} - {signal_type} (confidence: {confidence:.2f})")
                return result
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error analyzing short_waves for {symbol}: {e}")
            return None
    
    def analyze_medium_waves(self, symbol: str) -> Optional[Dict]:
        """
        Analyze for medium waves strategy (1-2 weeks)
        Timeframe: 1h
        """
        try:
            strategy = self.strategies['medium_waves']
            if not strategy.get('enabled', False):
                return None
            
            timeframe = strategy.get('timeframe', '1h')
            indicators = self.get_latest_indicators(symbol, timeframe)
            
            if not indicators:
                return None
            
            current_price = redis_manager.get_cached_price(symbol)
            if not current_price:
                return None
            
            price = current_price.get('close', 0)
            
            signal_type = 'HOLD'
            confidence = 0.0
            reasons = []
            
            # SMA crossover (50 and 200) - Golden/Death Cross
            if indicators['sma_50'] and indicators['sma_200']:
                if indicators['sma_50'] > indicators['sma_200']:
                    signal_type = 'BUY'
                    confidence += 0.4
                    reasons.append('Golden Cross (SMA 50 > SMA 200)')
                elif indicators['sma_50'] < indicators['sma_200']:
                    signal_type = 'SELL'
                    confidence += 0.4
                    reasons.append('Death Cross (SMA 50 < SMA 200)')
            
            # Price position relative to SMAs
            if indicators['sma_50']:
                if price > indicators['sma_50']:
                    if signal_type == 'BUY':
                        confidence += 0.2
                        reasons.append('Price above SMA 50')
                else:
                    if signal_type == 'SELL':
                        confidence += 0.2
                        reasons.append('Price below SMA 50')
            
            # MACD confirmation
            if indicators['macd'] and indicators['macd_signal']:
                if signal_type == 'BUY' and indicators['macd'] > indicators['macd_signal']:
                    confidence += 0.2
                    reasons.append('MACD confirms bullish')
                elif signal_type == 'SELL' and indicators['macd'] < indicators['macd_signal']:
                    confidence += 0.2
                    reasons.append('MACD confirms bearish')
            
            # ADX for trend strength
            if indicators['adx'] and indicators['adx'] > 30:
                confidence += 0.15
                reasons.append(f'Very strong trend (ADX: {indicators["adx"]:.1f})')
            
            confidence = min(confidence, 1.0)
            
            if signal_type != 'HOLD' and confidence >= 0.7:
                result = {
                    'strategy': 'medium_waves',
                    'symbol': symbol,
                    'signal': signal_type,
                    'confidence': confidence,
                    'price': price,
                    'timeframe': timeframe,
                    'reasons': reasons,
                    'timestamp': get_saudi_time()
                }
                
                self.logger.info(f"Signal: {symbol} - {signal_type} (confidence: {confidence:.2f})")
                return result
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error analyzing medium_waves for {symbol}: {e}")
            return None
    
    def analyze_price_explosions(self, symbol: str) -> Optional[Dict]:
        """
        Analyze for price explosions strategy (rare high-profit opportunities)
        Timeframe: 1d
        """
        try:
            strategy = self.strategies['price_explosions']
            if not strategy.get('enabled', False):
                return None
            
            timeframe = strategy.get('timeframe', '1d')
            
            # Get recent price data
            with db.get_session() as session:
                query = text("""
                SELECT close, volume FROM market_data.stock_prices
                WHERE symbol = :symbol AND timeframe = :timeframe
                ORDER BY time DESC
                LIMIT 30
                """)
                result = session.execute(query, {'symbol': symbol, 'timeframe': timeframe})
                data = result.fetchall()
            
            if len(data) < 10:
                return None
            
            prices = [row[0] for row in data]
            volumes = [row[1] for row in data]
            
            current_price = prices[0]
            avg_price_20 = np.mean(prices[:20])
            avg_volume_20 = np.mean(volumes[:20])
            
            signal_type = 'HOLD'
            confidence = 0.0
            reasons = []
            
            # Breakout detection
            if current_price > avg_price_20 * 1.05:  # 5% above average
                signal_type = 'BUY'
                confidence += 0.3
                reasons.append(f'Price breakout: {((current_price/avg_price_20 - 1) * 100):.1f}% above average')
            
            # Volume spike
            if volumes[0] > avg_volume_20 * 2:  # 2x average volume
                if signal_type == 'BUY':
                    confidence += 0.3
                else:
                    signal_type = 'BUY'
                    confidence += 0.2
                reasons.append(f'Volume spike: {(volumes[0]/avg_volume_20):.1f}x average')
            
            # Consecutive green candles
            green_candles = 0
            for i in range(min(5, len(prices)-1)):
                if prices[i] > prices[i+1]:
                    green_candles += 1
                else:
                    break
            
            if green_candles >= 3:
                confidence += 0.2
                reasons.append(f'{green_candles} consecutive up days')
            
            # Get indicators for confirmation
            indicators = self.get_latest_indicators(symbol, timeframe)
            if indicators:
                if indicators['rsi'] and indicators['rsi'] > 60:
                    confidence += 0.15
                    reasons.append('Strong momentum (RSI > 60)')
                
                if indicators['adx'] and indicators['adx'] > 35:
                    confidence += 0.15
                    reasons.append(f'Very strong trend (ADX: {indicators["adx"]:.1f})')
            
            confidence = min(confidence, 1.0)
            
            if signal_type == 'BUY' and confidence >= 0.75:
                result = {
                    'strategy': 'price_explosions',
                    'symbol': symbol,
                    'signal': signal_type,
                    'confidence': confidence,
                    'price': current_price,
                    'timeframe': timeframe,
                    'reasons': reasons,
                    'timestamp': get_saudi_time()
                }
                
                self.logger.success(f"🚀 EXPLOSION DETECTED: {symbol} - {signal_type} (confidence: {confidence:.2f})")
                return result
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error analyzing price_explosions for {symbol}: {e}")
            return None
    
    def run(self, symbols: List[str] = None):
        """Run strategic analysis on symbols"""
        try:
            self.logger.info("Starting Strategic Analyzer")
            
            # Get symbols: config watchlist → DB ohlcv → fallback
            if symbols is None:
                # 1. Try watchlist from config
                watchlist = config.get('watchlist', {})
                symbols = watchlist.get('symbols', [])
                
                # 2. Fallback: query from market_data.ohlcv
                if not symbols:
                    try:
                        with db.get_session() as session:
                            query = text("""
                                SELECT DISTINCT symbol
                                FROM market_data.ohlcv
                                WHERE timeframe = '1m'
                                  AND time >= NOW() - INTERVAL '1 day'
                                  AND symbol NOT LIKE '9%'
                                LIMIT 100
                            """)
                            result = session.execute(query)
                            symbols = [row[0] for row in result.fetchall()]
                    except Exception as db_err:
                        self.logger.warning(f"Could not fetch symbols from DB: {db_err}")
            
            if not symbols:
                self.logger.warning("No symbols to analyze — check watchlist in config.yaml")
                return
            
            self.logger.info(f"Analyzing {len(symbols)} symbols: {symbols[:5]}...")
            
            all_signals = []
            
            for symbol in symbols:
                # Analyze with all strategies
                signals = [
                    self.analyze_aggressive_daily(symbol),
                    self.analyze_short_waves(symbol),
                    self.analyze_medium_waves(symbol),
                    self.analyze_price_explosions(symbol)
                ]
                
                # Save valid signals
                for signal in signals:
                    if signal:
                        all_signals.append(signal)
                        
                        # Save to database
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
                                    metadata={'reasons': signal['reasons']}
                                )
                            self.logger.success(
                                f"✅ Signal saved: {signal['symbol']} "
                                f"{signal['signal']} [{signal['strategy']}] "
                                f"conf={signal['confidence']:.2f}"
                            )
                        except Exception as save_err:
                            self.logger.error(f"Failed to save signal: {save_err}")
                        
                        # Cache signal
                        redis_manager.cache_signal(signal['strategy'], symbol, signal)
            
            self.logger.success(f"Strategic Analyzer completed: {len(all_signals)} signals generated")
            return all_signals
            
        except Exception as e:
            self.logger.error(f"Error in Strategic Analyzer: {e}")
            raise


if __name__ == "__main__":
    # Test the bot
    bot = StrategicAnalyzer()
    bot.run(symbols=['2222'])

    def get_news_sentiment(self, symbol: str) -> float:
        """
        Get news sentiment score for a symbol from FinBERT analysis
        
        Returns:
            float: Sentiment score from -1.0 (very negative) to +1.0 (very positive)
        """
        try:
            # Check cache first
            cached_sentiment = redis_manager.get(f"sentiment:{symbol}")
            
            if cached_sentiment is not None:
                return cached_sentiment
            
            # Get from database (latest sentiment from Scientist bot)
            with db.get_session() as session:
                query = text("""
                SELECT sentiment_score
                FROM market_data.news_sentiment
                WHERE symbol = :symbol
                ORDER BY timestamp DESC
                LIMIT 1
                """)
                result = session.execute(query, {'symbol': symbol})
                row = result.fetchone()
            
            if row:
                sentiment_score = row[0]
                # Cache for 1 hour
                redis_manager.set(f"sentiment:{symbol}", sentiment_score, ttl=3600)
                return sentiment_score
            
            # No sentiment data available - return neutral
            return 0.0
            
        except Exception as e:
            self.logger.error(f"Error getting news sentiment for {symbol}: {e}")
            return 0.0
    
    def apply_sentiment_weight(self, signal: Dict, sentiment_score: float) -> Dict:
        """
        Apply news sentiment weight to signal confidence
        
        If sentiment score is negative (-0.6 or lower), reduce confidence by 40%
        
        Args:
            signal: Original signal dict
            sentiment_score: Sentiment score from FinBERT (-1.0 to +1.0)
            
        Returns:
            Modified signal with adjusted confidence
        """
        try:
            original_confidence = signal.get('confidence', 1.0)
            
            # Apply penalty for negative sentiment
            if sentiment_score <= -0.6:
                # Reduce confidence by 40%
                confidence_multiplier = 0.6
                adjusted_confidence = original_confidence * confidence_multiplier
                
                signal['confidence'] = adjusted_confidence
                signal['sentiment_adjusted'] = True
                signal['sentiment_score'] = sentiment_score
                signal['sentiment_penalty'] = 0.4
                
                self.logger.info(
                    f"{signal['symbol']}: Negative sentiment detected ({sentiment_score:.2f}) - "
                    f"Confidence reduced from {original_confidence:.2f} to {adjusted_confidence:.2f}"
                )
            else:
                # No adjustment needed
                signal['sentiment_adjusted'] = False
                signal['sentiment_score'] = sentiment_score
                signal['sentiment_penalty'] = 0.0
            
            return signal
            
        except Exception as e:
            self.logger.error(f"Error applying sentiment weight: {e}")
            return signal
    
    def generate_signal_with_sentiment(self, symbol: str, strategy: str, 
                                      signal_type: str, confidence: float, 
                                      indicators: Dict) -> Dict:
        """
        Generate signal with sentiment adjustment
        
        Args:
            symbol: Stock symbol
            strategy: Strategy name
            signal_type: BUY or SELL
            confidence: Base confidence (0.0 to 1.0)
            indicators: Technical indicators dict
            
        Returns:
            Signal dict with sentiment adjustment applied
        """
        try:
            # Create base signal
            signal = {
                'symbol': symbol,
                'strategy': strategy,
                'type': signal_type,
                'confidence': confidence,
                'indicators': indicators,
                'timestamp': get_saudi_time()
            }
            
            # Get news sentiment
            sentiment_score = self.get_news_sentiment(symbol)
            
            # Apply sentiment weight
            signal = self.apply_sentiment_weight(signal, sentiment_score)
            
            return signal
            
        except Exception as e:
            self.logger.error(f"Error generating signal with sentiment: {e}")
            return signal
