"""
Bot 1: Technical Miner (المُنَقِّب الفني) - ENHANCED
Performs comprehensive advanced technical analysis on stock data
Includes: Market Structure, Volume Profile, Fibonacci, Chart Patterns, and more
"""

import pandas as pd
import numpy as np
import ta
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from loguru import logger

from sqlalchemy import text
from config.config_manager import config
from scripts.database import db, insert_technical_indicators
from scripts.redis_manager import redis_manager
from scripts.utils import (
    get_saudi_time, is_trading_hours, sanitize_dataframe,
    detect_trend, calculate_volatility
)
from scripts.advanced_technical_analysis import (
    AdvancedTechnicalAnalysis, analyze_stock_advanced
)


class TechnicalMiner:
    """Enhanced Technical analysis bot with advanced features"""
    
    def __init__(self):
        self.name = "technical_miner"
        self.logger = logger.bind(bot=self.name)
        self.config = config.get_bot_config(self.name)
        self.enabled_indicators = self.config.get('indicators', [])
        self.advanced_ta = AdvancedTechnicalAnalysis()
        
    # ==================== BASIC INDICATORS ====================
    
    def calculate_rsi(self, prices: pd.Series, period: int = 14) -> pd.Series:
        """Calculate RSI (Relative Strength Index)"""
        try:
            return ta.momentum.RSIIndicator(prices, window=period).rsi()
        except Exception as e:
            self.logger.error(f"Error calculating RSI: {e}")
            return pd.Series([np.nan] * len(prices))
    
    def calculate_macd(self, prices: pd.Series) -> Dict[str, pd.Series]:
        """Calculate MACD (Moving Average Convergence Divergence)"""
        try:
            macd = ta.trend.MACD(prices)
            return {
                'macd': macd.macd(),
                'macd_signal': macd.macd_signal(),
                'macd_hist': macd.macd_diff()
            }
        except Exception as e:
            self.logger.error(f"Error calculating MACD: {e}")
            return {
                'macd': pd.Series([np.nan] * len(prices)),
                'macd_signal': pd.Series([np.nan] * len(prices)),
                'macd_hist': pd.Series([np.nan] * len(prices))
            }
    
    def calculate_bollinger_bands(self, prices: pd.Series, period: int = 20) -> Dict[str, pd.Series]:
        """Calculate Bollinger Bands"""
        try:
            bb = ta.volatility.BollingerBands(prices, window=period)
            return {
                'bb_upper': bb.bollinger_hband(),
                'bb_middle': bb.bollinger_mavg(),
                'bb_lower': bb.bollinger_lband(),
                'bb_width': bb.bollinger_wband()
            }
        except Exception as e:
            self.logger.error(f"Error calculating Bollinger Bands: {e}")
            return {
                'bb_upper': pd.Series([np.nan] * len(prices)),
                'bb_middle': pd.Series([np.nan] * len(prices)),
                'bb_lower': pd.Series([np.nan] * len(prices)),
                'bb_width': pd.Series([np.nan] * len(prices))
            }
    
    def calculate_ema(self, prices: pd.Series, period: int = 20) -> pd.Series:
        """Calculate EMA (Exponential Moving Average)"""
        try:
            return ta.trend.EMAIndicator(prices, window=period).ema_indicator()
        except Exception as e:
            self.logger.error(f"Error calculating EMA: {e}")
            return pd.Series([np.nan] * len(prices))
    
    def calculate_sma(self, prices: pd.Series, period: int = 20) -> pd.Series:
        """Calculate SMA (Simple Moving Average)"""
        try:
            return ta.trend.SMAIndicator(prices, window=period).sma_indicator()
        except Exception as e:
            self.logger.error(f"Error calculating SMA: {e}")
            return pd.Series([np.nan] * len(prices))
    
    def calculate_atr(self, high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
        """Calculate ATR (Average True Range)"""
        try:
            return ta.volatility.AverageTrueRange(high, low, close, window=period).average_true_range()
        except Exception as e:
            self.logger.error(f"Error calculating ATR: {e}")
            return pd.Series([np.nan] * len(high))
    
    def calculate_stochastic(self, high: pd.Series, low: pd.Series, close: pd.Series) -> Dict[str, pd.Series]:
        """Calculate Stochastic Oscillator"""
        try:
            stoch = ta.momentum.StochasticOscillator(high, low, close)
            return {
                'stoch_k': stoch.stoch(),
                'stoch_d': stoch.stoch_signal()
            }
        except Exception as e:
            self.logger.error(f"Error calculating Stochastic: {e}")
            return {
                'stoch_k': pd.Series([np.nan] * len(high)),
                'stoch_d': pd.Series([np.nan] * len(high))
            }
    
    def calculate_adx(self, high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
        """Calculate ADX (Average Directional Index)"""
        try:
            return ta.trend.ADXIndicator(high, low, close, window=period).adx()
        except Exception as e:
            self.logger.error(f"Error calculating ADX: {e}")
            return pd.Series([np.nan] * len(high))
    
    def calculate_obv(self, close: pd.Series, volume: pd.Series) -> pd.Series:
        """Calculate OBV (On-Balance Volume)"""
        try:
            return ta.volume.OnBalanceVolumeIndicator(close, volume).on_balance_volume()
        except Exception as e:
            self.logger.error(f"Error calculating OBV: {e}")
            return pd.Series([np.nan] * len(close))
    
    # ==================== ADVANCED ANALYSIS ====================
    
    def analyze_stock(self, symbol: str, df: pd.DataFrame, timeframe: str = '1d') -> Dict:
        """
        Comprehensive technical analysis for a stock
        Includes both basic and advanced indicators
        """
        try:
            self.logger.info(f"Analyzing {symbol} on {timeframe} timeframe")
            
            # Ensure dataframe is clean
            df = sanitize_dataframe(df)
            
            if len(df) < 50:
                self.logger.warning(f"Insufficient data for {symbol}: {len(df)} candles")
                return {}
            
            # ===== BASIC INDICATORS =====
            basic_indicators = {
                'rsi': self.calculate_rsi(df['close']),
                **self.calculate_macd(df['close']),
                **self.calculate_bollinger_bands(df['close']),
                'ema_20': self.calculate_ema(df['close'], 20),
                'ema_50': self.calculate_ema(df['close'], 50),
                'sma_200': self.calculate_sma(df['close'], 200),
                'atr': self.calculate_atr(df['high'], df['low'], df['close']),
                **self.calculate_stochastic(df['high'], df['low'], df['close']),
                'adx': self.calculate_adx(df['high'], df['low'], df['close']),
                'obv': self.calculate_obv(df['close'], df['volume'])
            }
            
            # ===== ADVANCED ANALYSIS =====
            advanced_analysis = analyze_stock_advanced(df)
            
            # ===== TREND & VOLATILITY =====
            trend = detect_trend(df)
            volatility = calculate_volatility(df)
            
            # ===== COMPILE RESULTS =====
            analysis = {
                'symbol': symbol,
                'timeframe': timeframe,
                'timestamp': get_saudi_time(),
                'last_price': float(df['close'].iloc[-1]),
                'last_volume': float(df['volume'].iloc[-1]),
                
                # Basic Indicators (latest values)
                'rsi': float(basic_indicators['rsi'].iloc[-1]) if not pd.isna(basic_indicators['rsi'].iloc[-1]) else None,
                'macd': float(basic_indicators['macd'].iloc[-1]) if not pd.isna(basic_indicators['macd'].iloc[-1]) else None,
                'macd_signal': float(basic_indicators['macd_signal'].iloc[-1]) if not pd.isna(basic_indicators['macd_signal'].iloc[-1]) else None,
                'macd_hist': float(basic_indicators['macd_hist'].iloc[-1]) if not pd.isna(basic_indicators['macd_hist'].iloc[-1]) else None,
                'bb_upper': float(basic_indicators['bb_upper'].iloc[-1]) if not pd.isna(basic_indicators['bb_upper'].iloc[-1]) else None,
                'bb_middle': float(basic_indicators['bb_middle'].iloc[-1]) if not pd.isna(basic_indicators['bb_middle'].iloc[-1]) else None,
                'bb_lower': float(basic_indicators['bb_lower'].iloc[-1]) if not pd.isna(basic_indicators['bb_lower'].iloc[-1]) else None,
                'bb_width': float(basic_indicators['bb_width'].iloc[-1]) if not pd.isna(basic_indicators['bb_width'].iloc[-1]) else None,
                'ema_20': float(basic_indicators['ema_20'].iloc[-1]) if not pd.isna(basic_indicators['ema_20'].iloc[-1]) else None,
                'ema_50': float(basic_indicators['ema_50'].iloc[-1]) if not pd.isna(basic_indicators['ema_50'].iloc[-1]) else None,
                'sma_200': float(basic_indicators['sma_200'].iloc[-1]) if not pd.isna(basic_indicators['sma_200'].iloc[-1]) else None,
                'atr': float(basic_indicators['atr'].iloc[-1]) if not pd.isna(basic_indicators['atr'].iloc[-1]) else None,
                'stoch_k': float(basic_indicators['stoch_k'].iloc[-1]) if not pd.isna(basic_indicators['stoch_k'].iloc[-1]) else None,
                'stoch_d': float(basic_indicators['stoch_d'].iloc[-1]) if not pd.isna(basic_indicators['stoch_d'].iloc[-1]) else None,
                'adx': float(basic_indicators['adx'].iloc[-1]) if not pd.isna(basic_indicators['adx'].iloc[-1]) else None,
                'obv': float(basic_indicators['obv'].iloc[-1]) if not pd.isna(basic_indicators['obv'].iloc[-1]) else None,
                
                # Trend & Volatility
                'trend': trend,
                'volatility': volatility,
                
                # Advanced Analysis
                'market_structure': {
                    'bos_count': len(advanced_analysis['bos']),
                    'choch_count': len(advanced_analysis['choch']),
                    'order_blocks_count': len(advanced_analysis['order_blocks']),
                    'fvg_count': len(advanced_analysis['fvg']),
                    'liquidity_pools_count': len(advanced_analysis['liquidity_pools']),
                    'latest_bos': advanced_analysis['bos'][-1] if advanced_analysis['bos'] else None,
                    'latest_choch': advanced_analysis['choch'][-1] if advanced_analysis['choch'] else None,
                    'active_order_blocks': [ob.__dict__ for ob in advanced_analysis['order_blocks'][-3:]],  # Last 3
                    'active_fvgs': [fvg.__dict__ for fvg in advanced_analysis['fvg'][-3:]],
                },
                
                'volume_analysis': {
                    'poc': advanced_analysis['volume_profile']['poc'],
                    'vah': advanced_analysis['volume_profile']['vah'],
                    'val': advanced_analysis['volume_profile']['val'],
                    'high_volume_nodes_count': len(advanced_analysis['volume_profile']['high_volume_nodes']),
                    'low_volume_nodes_count': len(advanced_analysis['volume_profile']['low_volume_nodes']),
                    'volume_delta': float(advanced_analysis['volume_delta'].iloc[-1]) if len(advanced_analysis['volume_delta']) > 0 else 0,
                    'volume_delta_ma_5': float(advanced_analysis['volume_delta'].rolling(5).mean().iloc[-1]) if len(advanced_analysis['volume_delta']) > 5 else 0,
                },
                
                'market_regime': advanced_analysis['market_regime'].iloc[-1] if len(advanced_analysis['market_regime']) > 0 and advanced_analysis['market_regime'].iloc[-1] is not None else 'unknown',
                
                'candlestick_patterns': {
                    'engulfing_count': len(advanced_analysis['engulfing']),
                    'pin_bar_count': len(advanced_analysis['pin_bars']),
                    'latest_engulfing': advanced_analysis['engulfing'][-1] if advanced_analysis['engulfing'] else None,
                    'latest_pin_bar': advanced_analysis['pin_bars'][-1] if advanced_analysis['pin_bars'] else None,
                },
                
                'chart_patterns': {
                    'head_shoulders_count': len(advanced_analysis['head_shoulders']),
                    'double_patterns_count': len(advanced_analysis['double_patterns']),
                    'active_head_shoulders': advanced_analysis['head_shoulders'][-1] if advanced_analysis['head_shoulders'] else None,
                    'active_double_pattern': advanced_analysis['double_patterns'][-1] if advanced_analysis['double_patterns'] else None,
                },
                
                'fibonacci': advanced_analysis['fibonacci'],
            }
            
            # Save to database
            self._save_to_database(analysis)
            
            # Cache in Redis (30 minutes TTL)
            cache_key = f"technical_analysis:{symbol}:{timeframe}"
            redis_manager.set(cache_key, analysis, ttl=1800)
            
            self.logger.success(f"Analysis complete for {symbol} on {timeframe}")
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Error analyzing {symbol}: {e}")
            return {}
    
    def analyze_multiple_timeframes(self, symbol: str, 
                                   timeframes: List[str] = ['1m', '5m', '15m', '1h', '4h', '1d']) -> Dict:
        """
        Analyze stock across multiple timeframes
        """
        results = {}
        
        for tf in timeframes:
            try:
                # Fetch data for this timeframe (placeholder - implement actual data fetching)
                df = self._fetch_data(symbol, tf)
                
                if df is not None and len(df) > 0:
                    results[tf] = self.analyze_stock(symbol, df, tf)
                else:
                    self.logger.warning(f"No data for {symbol} on {tf}")
                    
            except Exception as e:
                self.logger.error(f"Error analyzing {symbol} on {tf}: {e}")
        
        return results
    
    def get_trading_signals(self, analysis: Dict) -> List[Dict]:
        """
        Generate trading signals based on analysis
        """
        signals = []
        
        try:
            # RSI Signals
            if analysis.get('rsi'):
                if analysis['rsi'] < 30:
                    signals.append({
                        'type': 'buy',
                        'indicator': 'rsi',
                        'reason': f"RSI oversold at {analysis['rsi']:.2f}",
                        'strength': (30 - analysis['rsi']) / 30
                    })
                elif analysis['rsi'] > 70:
                    signals.append({
                        'type': 'sell',
                        'indicator': 'rsi',
                        'reason': f"RSI overbought at {analysis['rsi']:.2f}",
                        'strength': (analysis['rsi'] - 70) / 30
                    })
            
            # MACD Signals
            if analysis.get('macd_hist'):
                if analysis['macd_hist'] > 0 and analysis.get('macd', 0) > analysis.get('macd_signal', 0):
                    signals.append({
                        'type': 'buy',
                        'indicator': 'macd',
                        'reason': 'MACD bullish crossover',
                        'strength': min(abs(analysis['macd_hist']) / analysis['last_price'], 1.0)
                    })
                elif analysis['macd_hist'] < 0 and analysis.get('macd', 0) < analysis.get('macd_signal', 0):
                    signals.append({
                        'type': 'sell',
                        'indicator': 'macd',
                        'reason': 'MACD bearish crossover',
                        'strength': min(abs(analysis['macd_hist']) / analysis['last_price'], 1.0)
                    })
            
            # Bollinger Bands Signals
            if analysis.get('bb_lower') and analysis.get('bb_upper'):
                if analysis['last_price'] <= analysis['bb_lower']:
                    signals.append({
                        'type': 'buy',
                        'indicator': 'bollinger_bands',
                        'reason': 'Price at lower Bollinger Band',
                        'strength': 0.7
                    })
                elif analysis['last_price'] >= analysis['bb_upper']:
                    signals.append({
                        'type': 'sell',
                        'indicator': 'bollinger_bands',
                        'reason': 'Price at upper Bollinger Band',
                        'strength': 0.7
                    })
            
            # Market Structure Signals
            ms = analysis.get('market_structure', {})
            if ms.get('latest_bos'):
                bos = ms['latest_bos']
                if bos['type'] == 'bullish_bos':
                    signals.append({
                        'type': 'buy',
                        'indicator': 'market_structure',
                        'reason': 'Bullish Break of Structure detected',
                        'strength': 0.8
                    })
                elif bos['type'] == 'bearish_bos':
                    signals.append({
                        'type': 'sell',
                        'indicator': 'market_structure',
                        'reason': 'Bearish Break of Structure detected',
                        'strength': 0.8
                    })
            
            # Volume Profile Signals
            va = analysis.get('volume_analysis', {})
            if va.get('poc') and va.get('vah') and va.get('val'):
                if analysis['last_price'] < va['val']:
                    signals.append({
                        'type': 'buy',
                        'indicator': 'volume_profile',
                        'reason': 'Price below Value Area Low',
                        'strength': 0.6
                    })
                elif analysis['last_price'] > va['vah']:
                    signals.append({
                        'type': 'sell',
                        'indicator': 'volume_profile',
                        'reason': 'Price above Value Area High',
                        'strength': 0.6
                    })
            
            # Candlestick Pattern Signals
            cp = analysis.get('candlestick_patterns', {})
            if cp.get('latest_engulfing'):
                eng = cp['latest_engulfing']
                if eng['type'] == 'bullish_engulfing':
                    signals.append({
                        'type': 'buy',
                        'indicator': 'candlestick_pattern',
                        'reason': 'Bullish Engulfing with volume confirmation',
                        'strength': min(eng['strength'], 1.0)
                    })
                elif eng['type'] == 'bearish_engulfing':
                    signals.append({
                        'type': 'sell',
                        'indicator': 'candlestick_pattern',
                        'reason': 'Bearish Engulfing with volume confirmation',
                        'strength': min(eng['strength'], 1.0)
                    })
            
        except Exception as e:
            self.logger.error(f"Error generating signals: {e}")
        
        return signals
    
    def _save_to_database(self, analysis: Dict):
        """Save analysis to database"""
        try:
            insert_technical_indicators(
                symbol=analysis['symbol'],
                timeframe=analysis['timeframe'],
                data=analysis
            )
        except Exception as e:
            self.logger.error(f"Error saving to database: {e}")
    
    def _fetch_data(self, symbol: str, timeframe: str, limit: int = 300) -> Optional[pd.DataFrame]:
        """
        Fetch OHLCV data from TimescaleDB for the given symbol and timeframe.
        Supports automatic resampling from 1m data if target timeframe is not stored.
        """
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

            if rows and len(rows) >= 20:
                df = pd.DataFrame(rows, columns=['time', 'open', 'high', 'low', 'close', 'volume'])
                df['time'] = pd.to_datetime(df['time'])
                df = df.sort_values('time').reset_index(drop=True)
                df[['open', 'high', 'low', 'close', 'volume']] = \
                    df[['open', 'high', 'low', 'close', 'volume']].astype(float)
                self.logger.info(f"[TechnicalMiner] Fetched {len(df)} {timeframe} bars for {symbol}")
                return df

            # Fallback: resample from 1m data
            resample_map = {'5m': '5min', '15m': '15min', '1h': '1H', '4h': '4H', '1d': '1D'}
            if timeframe in resample_map:
                with db.get_session() as session:
                    result = session.execute(
                        text("""
                        SELECT time, open, high, low, close, volume
                        FROM market_data.ohlcv
                        WHERE symbol = :symbol AND timeframe = '1m'
                        ORDER BY time DESC LIMIT 2000
                        """),
                        {'symbol': symbol}
                    )
                    rows_1m = result.fetchall()

                if rows_1m and len(rows_1m) >= 20:
                    df1m = pd.DataFrame(rows_1m, columns=['time', 'open', 'high', 'low', 'close', 'volume'])
                    df1m['time'] = pd.to_datetime(df1m['time'])
                    df1m = df1m.sort_values('time').set_index('time').astype(float)
                    rule = resample_map[timeframe]
                    df_res = df1m.resample(rule).agg({
                        'open': 'first', 'high': 'max',
                        'low': 'min', 'close': 'last', 'volume': 'sum'
                    }).dropna().reset_index()
                    self.logger.info(
                        f"[TechnicalMiner] Resampled {len(df_res)} {timeframe} bars for {symbol} from 1m"
                    )
                    return df_res

            self.logger.warning(f"[TechnicalMiner] No data available for {symbol} @ {timeframe}")
            return None

        except Exception as e:
            self.logger.error(f"[TechnicalMiner] Error fetching data for {symbol} @ {timeframe}: {e}")
            return None
    
    def _get_symbols_from_db(self) -> List[str]:
        """
        جلب كل الأسهم التي لها بيانات في DB (آخر 7 أيام).
        هذا يضمن التحليل الشامل لكل سهم جمع بياناته بدون قائمة ثابتة.
        """
        try:
            with db.get_session() as session:
                result = session.execute(text("""
                    SELECT DISTINCT symbol
                    FROM market_data.ohlcv
                    WHERE time >= NOW() - INTERVAL '7 days'
                      AND symbol NOT LIKE '900%'
                    ORDER BY symbol
                """))
                symbols = [row[0] for row in result.fetchall()]
            self.logger.info(f"[TechnicalMiner] Found {len(symbols)} symbols in DB")
            return symbols
        except Exception as e:
            self.logger.error(f"[TechnicalMiner] Error fetching symbols from DB: {e}")
            return []

    def run(self, symbols: List[str] = None):
        """
        Main execution method
        """
        try:
            if symbols is None:
                symbols = self._get_symbols_from_db()

            if not symbols:
                self.logger.warning(
                    "[TechnicalMiner] No symbols found in DB. "
                    "Waiting for market_reporter to collect data first."
                )
                return

            self.logger.info(f"Starting Technical Miner for {len(symbols)} symbols")

            for symbol in symbols:
                try:
                    # Analyze across multiple timeframes
                    results = self.analyze_multiple_timeframes(symbol)
                    
                    # Generate signals
                    for tf, analysis in results.items():
                        if analysis:
                            signals = self.get_trading_signals(analysis)
                            if signals:
                                self.logger.info(f"{symbol} [{tf}]: {len(signals)} signals generated")
                                
                except Exception as e:
                    self.logger.error(f"Error processing {symbol}: {e}")
                    continue
            
            self.logger.success("Technical Miner execution complete")
            
        except Exception as e:
            self.logger.error(f"Error in Technical Miner run: {e}")


if __name__ == "__main__":
    bot = TechnicalMiner()
    bot.run()
