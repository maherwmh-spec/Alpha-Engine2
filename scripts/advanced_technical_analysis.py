"""
Advanced Technical Analysis Library for Alpha-Engine2
Includes: Market Structure, Volume Profile, Fibonacci, Chart Patterns, etc.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from enum import Enum


class MarketRegime(Enum):
    """Market regime types"""
    TRENDING_UP = "trending_up"
    TRENDING_DOWN = "trending_down"
    RANGING = "ranging"
    HIGH_VOLATILITY = "high_volatility"
    LOW_VOLATILITY = "low_volatility"
    COMPRESSION = "compression"


@dataclass
class OrderBlock:
    """Order Block structure"""
    start_idx: int
    end_idx: int
    high: float
    low: float
    type: str  # 'bullish' or 'bearish'
    strength: float


@dataclass
class FairValueGap:
    """Fair Value Gap (FVG) structure"""
    idx: int
    gap_high: float
    gap_low: float
    type: str  # 'bullish' or 'bearish'
    filled: bool = False


@dataclass
class LiquidityPool:
    """Liquidity Pool structure"""
    idx: int
    price: float
    type: str  # 'equal_highs', 'equal_lows', 'sweep'
    strength: float


class AdvancedTechnicalAnalysis:
    """Advanced Technical Analysis Tools"""
    
    def __init__(self):
        self.swing_lookback = 5
        self.volume_profile_bins = 50
        
    # ==================== MARKET STRUCTURE ====================
    
    def detect_bos(self, df: pd.DataFrame) -> List[Dict]:
        """
        Detect Break of Structure (BOS)
        BOS occurs when price breaks the last swing high/low
        """
        bos_signals = []
        
        # Find swing highs and lows
        swing_highs = self._find_swing_highs(df)
        swing_lows = self._find_swing_lows(df)
        
        last_swing_high = None
        last_swing_low = None
        
        for i in range(len(df)):
            # Update last swing high/low
            if i in swing_highs:
                last_swing_high = df['high'].iloc[i]
            if i in swing_lows:
                last_swing_low = df['low'].iloc[i]
            
            # Check for BOS
            if last_swing_high and df['close'].iloc[i] > last_swing_high:
                bos_signals.append({
                    'idx': i,
                    'type': 'bullish_bos',
                    'price': df['close'].iloc[i],
                    'broken_level': last_swing_high
                })
            
            if last_swing_low and df['close'].iloc[i] < last_swing_low:
                bos_signals.append({
                    'idx': i,
                    'type': 'bearish_bos',
                    'price': df['close'].iloc[i],
                    'broken_level': last_swing_low
                })
        
        return bos_signals
    
    def detect_choch(self, df: pd.DataFrame) -> List[Dict]:
        """
        Detect Change of Character (CHOCH)
        CHOCH is a minor structure break that hints at trend reversal
        """
        choch_signals = []
        
        # Detect trend
        trend = self._detect_trend(df)
        
        for i in range(self.swing_lookback, len(df)):
            window = df.iloc[i-self.swing_lookback:i+1]
            
            # Bullish CHOCH: in downtrend, price breaks recent high
            if trend[i] == -1:  # downtrend
                recent_high = window['high'].max()
                if df['close'].iloc[i] > recent_high:
                    choch_signals.append({
                        'idx': i,
                        'type': 'bullish_choch',
                        'price': df['close'].iloc[i]
                    })
            
            # Bearish CHOCH: in uptrend, price breaks recent low
            elif trend[i] == 1:  # uptrend
                recent_low = window['low'].min()
                if df['close'].iloc[i] < recent_low:
                    choch_signals.append({
                        'idx': i,
                        'type': 'bearish_choch',
                        'price': df['close'].iloc[i]
                    })
        
        return choch_signals
    
    def detect_order_blocks(self, df: pd.DataFrame) -> List[OrderBlock]:
        """
        Detect Order Blocks (OB)
        Order blocks are the last opposite-colored candle before a strong move
        """
        order_blocks = []
        
        for i in range(2, len(df) - 1):
            # Bullish OB: down candle before strong up move
            if (df['close'].iloc[i] < df['open'].iloc[i] and  # down candle
                df['close'].iloc[i+1] > df['open'].iloc[i+1] and  # next is up
                df['close'].iloc[i+1] > df['high'].iloc[i]):  # strong move
                
                strength = (df['close'].iloc[i+1] - df['open'].iloc[i+1]) / df['open'].iloc[i+1]
                
                order_blocks.append(OrderBlock(
                    start_idx=i,
                    end_idx=i,
                    high=df['high'].iloc[i],
                    low=df['low'].iloc[i],
                    type='bullish',
                    strength=strength
                ))
            
            # Bearish OB: up candle before strong down move
            elif (df['close'].iloc[i] > df['open'].iloc[i] and  # up candle
                  df['close'].iloc[i+1] < df['open'].iloc[i+1] and  # next is down
                  df['close'].iloc[i+1] < df['low'].iloc[i]):  # strong move
                
                strength = (df['open'].iloc[i+1] - df['close'].iloc[i+1]) / df['open'].iloc[i+1]
                
                order_blocks.append(OrderBlock(
                    start_idx=i,
                    end_idx=i,
                    high=df['high'].iloc[i],
                    low=df['low'].iloc[i],
                    type='bearish',
                    strength=strength
                ))
        
        return order_blocks
    
    def detect_fvg(self, df: pd.DataFrame) -> List[FairValueGap]:
        """
        Detect Fair Value Gaps (FVG)
        FVG is a 3-candle pattern where there's a gap between candle 1 and 3
        """
        fvgs = []
        
        for i in range(2, len(df)):
            # Bullish FVG: gap between candle[i-2].high and candle[i].low
            if df['low'].iloc[i] > df['high'].iloc[i-2]:
                fvgs.append(FairValueGap(
                    idx=i-1,
                    gap_high=df['low'].iloc[i],
                    gap_low=df['high'].iloc[i-2],
                    type='bullish'
                ))
            
            # Bearish FVG: gap between candle[i-2].low and candle[i].high
            elif df['high'].iloc[i] < df['low'].iloc[i-2]:
                fvgs.append(FairValueGap(
                    idx=i-1,
                    gap_high=df['low'].iloc[i-2],
                    gap_low=df['high'].iloc[i],
                    type='bearish'
                ))
        
        return fvgs
    
    def detect_liquidity_pools(self, df: pd.DataFrame) -> List[LiquidityPool]:
        """
        Detect Liquidity Pools (equal highs/lows)
        """
        pools = []
        tolerance = 0.002  # 0.2% tolerance
        
        # Find equal highs
        for i in range(self.swing_lookback, len(df)):
            window_highs = df['high'].iloc[i-self.swing_lookback:i+1]
            max_high = window_highs.max()
            
            # Count how many times this high appears
            equal_count = sum(abs(h - max_high) / max_high < tolerance for h in window_highs)
            
            if equal_count >= 2:
                pools.append(LiquidityPool(
                    idx=i,
                    price=max_high,
                    type='equal_highs',
                    strength=equal_count
                ))
        
        # Find equal lows
        for i in range(self.swing_lookback, len(df)):
            window_lows = df['low'].iloc[i-self.swing_lookback:i+1]
            min_low = window_lows.min()
            
            equal_count = sum(abs(l - min_low) / min_low < tolerance for l in window_lows)
            
            if equal_count >= 2:
                pools.append(LiquidityPool(
                    idx=i,
                    price=min_low,
                    type='equal_lows',
                    strength=equal_count
                ))
        
        return pools
    
    # ==================== VOLUME PROFILE ====================
    
    def calculate_volume_profile(self, df: pd.DataFrame, 
                                 mode: str = 'visible') -> Dict:
        """
        Calculate Volume Profile
        Returns: POC, VAH, VAL, volume nodes
        """
        if mode == 'visible':
            data = df.copy()
        else:  # fixed range
            data = df.tail(100).copy()
        
        # Create price bins
        price_min = data['low'].min()
        price_max = data['high'].max()
        bins = np.linspace(price_min, price_max, self.volume_profile_bins)
        
        # Calculate volume at each price level
        volume_at_price = np.zeros(len(bins) - 1)
        
        for i in range(len(data)):
            candle_low = data['low'].iloc[i]
            candle_high = data['high'].iloc[i]
            candle_volume = data['volume'].iloc[i]
            
            # Distribute volume across price levels
            for j in range(len(bins) - 1):
                if bins[j] <= candle_high and bins[j+1] >= candle_low:
                    overlap = min(bins[j+1], candle_high) - max(bins[j], candle_low)
                    candle_range = candle_high - candle_low
                    if candle_range > 0:
                        volume_at_price[j] += candle_volume * (overlap / candle_range)
        
        # Find POC (Point of Control) - price level with highest volume
        poc_idx = np.argmax(volume_at_price)
        poc_price = (bins[poc_idx] + bins[poc_idx + 1]) / 2
        
        # Calculate Value Area (70% of volume)
        total_volume = volume_at_price.sum()
        value_area_volume = total_volume * 0.70
        
        # Expand from POC until we reach 70% of volume
        va_volume = volume_at_price[poc_idx]
        va_low_idx = poc_idx
        va_high_idx = poc_idx
        
        while va_volume < value_area_volume:
            # Check which direction has more volume
            low_volume = volume_at_price[va_low_idx - 1] if va_low_idx > 0 else 0
            high_volume = volume_at_price[va_high_idx + 1] if va_high_idx < len(volume_at_price) - 1 else 0
            
            if low_volume > high_volume and va_low_idx > 0:
                va_low_idx -= 1
                va_volume += low_volume
            elif va_high_idx < len(volume_at_price) - 1:
                va_high_idx += 1
                va_volume += high_volume
            else:
                break
        
        vah_price = (bins[va_high_idx] + bins[va_high_idx + 1]) / 2
        val_price = (bins[va_low_idx] + bins[va_low_idx + 1]) / 2
        
        # Identify high/low volume nodes
        volume_threshold_high = np.percentile(volume_at_price, 80)
        volume_threshold_low = np.percentile(volume_at_price, 20)
        
        high_volume_nodes = []
        low_volume_nodes = []
        
        for i, vol in enumerate(volume_at_price):
            price = (bins[i] + bins[i + 1]) / 2
            if vol >= volume_threshold_high:
                high_volume_nodes.append({'price': price, 'volume': vol})
            elif vol <= volume_threshold_low:
                low_volume_nodes.append({'price': price, 'volume': vol})
        
        return {
            'poc': poc_price,
            'vah': vah_price,
            'val': val_price,
            'high_volume_nodes': high_volume_nodes,
            'low_volume_nodes': low_volume_nodes,
            'volume_distribution': volume_at_price,
            'price_bins': bins
        }
    
    def calculate_volume_delta(self, df: pd.DataFrame) -> pd.Series:
        """
        Calculate Volume Delta (Buy vs Sell pressure)
        Approximation: up candles = buy, down candles = sell
        """
        delta = []
        
        for i in range(len(df)):
            if df['close'].iloc[i] > df['open'].iloc[i]:
                # Bullish candle - assume buy volume
                delta.append(df['volume'].iloc[i])
            elif df['close'].iloc[i] < df['open'].iloc[i]:
                # Bearish candle - assume sell volume
                delta.append(-df['volume'].iloc[i])
            else:
                # Doji - neutral
                delta.append(0)
        
        return pd.Series(delta, index=df.index)
    
    # ==================== REGIME DETECTION ====================
    
    def detect_market_regime(self, df: pd.DataFrame) -> pd.Series:
        """
        Detect market regime for each candle
        """
        regimes = []
        
        # Calculate ATR for volatility
        atr = self._calculate_atr(df, period=14)
        atr_ma = atr.rolling(window=50).mean()
        
        # Calculate ADX for trend strength
        adx = self._calculate_adx(df, period=14)
        
        for i in range(50, len(df)):
            # Determine volatility
            if atr.iloc[i] > atr_ma.iloc[i] * 1.5:
                vol_regime = "high_vol"
            elif atr.iloc[i] < atr_ma.iloc[i] * 0.5:
                vol_regime = "low_vol"
            else:
                vol_regime = "normal_vol"
            
            # Determine trend
            if adx.iloc[i] > 25:
                # Strong trend
                if df['close'].iloc[i] > df['close'].iloc[i-20]:
                    trend_regime = MarketRegime.TRENDING_UP
                else:
                    trend_regime = MarketRegime.TRENDING_DOWN
            else:
                # Weak trend = ranging
                trend_regime = MarketRegime.RANGING
            
            # Check for compression (Bollinger Bands squeeze)
            bb_width = self._calculate_bb_width(df.iloc[i-20:i+1])
            if bb_width < 0.02:  # Very narrow bands
                regime = MarketRegime.COMPRESSION
            elif vol_regime == "high_vol":
                regime = MarketRegime.HIGH_VOLATILITY
            elif vol_regime == "low_vol":
                regime = MarketRegime.LOW_VOLATILITY
            else:
                regime = trend_regime
            
            regimes.append(regime.value)
        
        # Fill first 50 with None
        regimes = [None] * 50 + regimes
        
        return pd.Series(regimes, index=df.index)
    
    # ==================== INTERMARKET ANALYSIS ====================
    
    def calculate_correlation(self, stock_df: pd.DataFrame, 
                             reference_df: pd.DataFrame,
                             period: int = 20) -> pd.Series:
        """
        Calculate rolling correlation between stock and reference (oil, TASI, DXY, sector)
        """
        stock_returns = stock_df['close'].pct_change()
        ref_returns = reference_df['close'].pct_change()
        
        correlation = stock_returns.rolling(window=period).corr(ref_returns)
        
        return correlation
    
    # ==================== CANDLESTICK PATTERNS ====================
    
    def detect_engulfing_with_volume(self, df: pd.DataFrame) -> List[Dict]:
        """
        Detect Engulfing patterns with volume confirmation
        """
        patterns = []
        
        for i in range(1, len(df)):
            prev_body = abs(df['close'].iloc[i-1] - df['open'].iloc[i-1])
            curr_body = abs(df['close'].iloc[i] - df['open'].iloc[i])
            
            # Bullish Engulfing
            if (df['close'].iloc[i-1] < df['open'].iloc[i-1] and  # prev bearish
                df['close'].iloc[i] > df['open'].iloc[i] and  # curr bullish
                df['open'].iloc[i] < df['close'].iloc[i-1] and  # engulfs
                df['close'].iloc[i] > df['open'].iloc[i-1] and
                df['volume'].iloc[i] > df['volume'].iloc[i-1] * 1.5):  # volume confirmation
                
                patterns.append({
                    'idx': i,
                    'type': 'bullish_engulfing',
                    'strength': curr_body / prev_body
                })
            
            # Bearish Engulfing
            elif (df['close'].iloc[i-1] > df['open'].iloc[i-1] and  # prev bullish
                  df['close'].iloc[i] < df['open'].iloc[i] and  # curr bearish
                  df['open'].iloc[i] > df['close'].iloc[i-1] and  # engulfs
                  df['close'].iloc[i] < df['open'].iloc[i-1] and
                  df['volume'].iloc[i] > df['volume'].iloc[i-1] * 1.5):
                
                patterns.append({
                    'idx': i,
                    'type': 'bearish_engulfing',
                    'strength': curr_body / prev_body
                })
        
        return patterns
    
    def detect_pin_bar_with_ob(self, df: pd.DataFrame, 
                               order_blocks: List[OrderBlock]) -> List[Dict]:
        """
        Detect Pin Bar at Order Block levels
        """
        patterns = []
        
        for i in range(len(df)):
            body = abs(df['close'].iloc[i] - df['open'].iloc[i])
            total_range = df['high'].iloc[i] - df['low'].iloc[i]
            
            if total_range == 0:
                continue
            
            upper_wick = df['high'].iloc[i] - max(df['close'].iloc[i], df['open'].iloc[i])
            lower_wick = min(df['close'].iloc[i], df['open'].iloc[i]) - df['low'].iloc[i]
            
            # Bullish Pin Bar (long lower wick)
            if (lower_wick > body * 2 and 
                lower_wick > total_range * 0.6):
                
                # Check if at Order Block
                for ob in order_blocks:
                    if (ob.type == 'bullish' and 
                        df['low'].iloc[i] <= ob.high and 
                        df['low'].iloc[i] >= ob.low):
                        
                        patterns.append({
                            'idx': i,
                            'type': 'bullish_pin_bar_at_ob',
                            'wick_ratio': lower_wick / total_range
                        })
                        break
            
            # Bearish Pin Bar (long upper wick)
            elif (upper_wick > body * 2 and 
                  upper_wick > total_range * 0.6):
                
                for ob in order_blocks:
                    if (ob.type == 'bearish' and 
                        df['high'].iloc[i] >= ob.low and 
                        df['high'].iloc[i] <= ob.high):
                        
                        patterns.append({
                            'idx': i,
                            'type': 'bearish_pin_bar_at_ob',
                            'wick_ratio': upper_wick / total_range
                        })
                        break
        
        return patterns
    
    # ==================== CHART PATTERNS ====================
    
    def detect_head_and_shoulders(self, df: pd.DataFrame) -> List[Dict]:
        """
        Detect Head and Shoulders pattern
        """
        patterns = []
        swing_highs = self._find_swing_highs(df)
        
        for i in range(len(swing_highs) - 2):
            left_shoulder_idx = swing_highs[i]
            head_idx = swing_highs[i + 1]
            right_shoulder_idx = swing_highs[i + 2]
            
            ls_price = df['high'].iloc[left_shoulder_idx]
            head_price = df['high'].iloc[head_idx]
            rs_price = df['high'].iloc[right_shoulder_idx]
            
            # Check pattern validity
            if (head_price > ls_price * 1.02 and 
                head_price > rs_price * 1.02 and
                abs(ls_price - rs_price) / ls_price < 0.03):  # shoulders roughly equal
                
                patterns.append({
                    'type': 'head_and_shoulders',
                    'left_shoulder_idx': left_shoulder_idx,
                    'head_idx': head_idx,
                    'right_shoulder_idx': right_shoulder_idx,
                    'neckline': min(df['low'].iloc[left_shoulder_idx:right_shoulder_idx+1])
                })
        
        return patterns
    
    def detect_double_top_bottom(self, df: pd.DataFrame) -> List[Dict]:
        """
        Detect Double Top/Bottom patterns
        """
        patterns = []
        swing_highs = self._find_swing_highs(df)
        swing_lows = self._find_swing_lows(df)
        
        # Double Top
        for i in range(len(swing_highs) - 1):
            idx1 = swing_highs[i]
            idx2 = swing_highs[i + 1]
            
            price1 = df['high'].iloc[idx1]
            price2 = df['high'].iloc[idx2]
            
            if abs(price1 - price2) / price1 < 0.02:  # Within 2%
                patterns.append({
                    'type': 'double_top',
                    'idx1': idx1,
                    'idx2': idx2,
                    'resistance': (price1 + price2) / 2
                })
        
        # Double Bottom
        for i in range(len(swing_lows) - 1):
            idx1 = swing_lows[i]
            idx2 = swing_lows[i + 1]
            
            price1 = df['low'].iloc[idx1]
            price2 = df['low'].iloc[idx2]
            
            if abs(price1 - price2) / price1 < 0.02:
                patterns.append({
                    'type': 'double_bottom',
                    'idx1': idx1,
                    'idx2': idx2,
                    'support': (price1 + price2) / 2
                })
        
        return patterns
    
    # ==================== FIBONACCI ====================
    
    def calculate_fibonacci_levels(self, df: pd.DataFrame, 
                                   lookback: int = 50) -> Dict:
        """
        Calculate Fibonacci Retracement and Extension levels
        """
        recent_data = df.tail(lookback)
        
        swing_high = recent_data['high'].max()
        swing_low = recent_data['low'].min()
        diff = swing_high - swing_low
        
        # Retracement levels
        retracement = {
            '0.0': swing_high,
            '23.6': swing_high - (diff * 0.236),
            '38.2': swing_high - (diff * 0.382),
            '50.0': swing_high - (diff * 0.500),
            '61.8': swing_high - (diff * 0.618),
            '78.6': swing_high - (diff * 0.786),
            '100.0': swing_low
        }
        
        # Extension levels
        extension = {
            '161.8': swing_high + (diff * 0.618),
            '261.8': swing_high + (diff * 1.618)
        }
        
        return {
            'retracement': retracement,
            'extension': extension,
            'swing_high': swing_high,
            'swing_low': swing_low
        }
    
    # ==================== HELPER FUNCTIONS ====================
    
    def _find_swing_highs(self, df: pd.DataFrame) -> List[int]:
        """Find swing high indices"""
        swing_highs = []
        for i in range(self.swing_lookback, len(df) - self.swing_lookback):
            if df['high'].iloc[i] == df['high'].iloc[i-self.swing_lookback:i+self.swing_lookback+1].max():
                swing_highs.append(i)
        return swing_highs
    
    def _find_swing_lows(self, df: pd.DataFrame) -> List[int]:
        """Find swing low indices"""
        swing_lows = []
        for i in range(self.swing_lookback, len(df) - self.swing_lookback):
            if df['low'].iloc[i] == df['low'].iloc[i-self.swing_lookback:i+self.swing_lookback+1].min():
                swing_lows.append(i)
        return swing_lows
    
    def _detect_trend(self, df: pd.DataFrame, period: int = 20) -> np.ndarray:
        """Detect trend direction: 1=up, -1=down, 0=sideways"""
        sma = df['close'].rolling(window=period).mean()
        trend = np.zeros(len(df))
        
        for i in range(period, len(df)):
            if df['close'].iloc[i] > sma.iloc[i] * 1.01:
                trend[i] = 1
            elif df['close'].iloc[i] < sma.iloc[i] * 0.99:
                trend[i] = -1
        
        return trend
    
    def _calculate_atr(self, df: pd.DataFrame, period: int = 14) -> pd.Series:
        """Calculate Average True Range"""
        high_low = df['high'] - df['low']
        high_close = abs(df['high'] - df['close'].shift())
        low_close = abs(df['low'] - df['close'].shift())
        
        true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        atr = true_range.rolling(window=period).mean()
        
        return atr
    
    def _calculate_adx(self, df: pd.DataFrame, period: int = 14) -> pd.Series:
        """Calculate Average Directional Index"""
        if len(df) <= period:
            return pd.Series([np.nan] * len(df), index=df.index)
            
        plus_dm = df['high'].diff()
        minus_dm = -df['low'].diff()
        
        plus_dm[plus_dm < 0] = 0
        minus_dm[minus_dm < 0] = 0
        
        tr = self._calculate_atr(df, period=1)
        
        plus_di = 100 * (plus_dm.rolling(window=period).mean() / tr)
        minus_di = 100 * (minus_dm.rolling(window=period).mean() / tr)
        
        dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
        adx = dx.rolling(window=period).mean()
        
        return adx
    
    def _calculate_bb_width(self, df: pd.DataFrame, period: int = 20) -> float:
        """Calculate Bollinger Bands width"""
        sma = df['close'].mean()
        std = df['close'].std()
        
        upper_band = sma + (2 * std)
        lower_band = sma - (2 * std)
        
        width = (upper_band - lower_band) / sma
        
        return width


# ==================== CONVENIENCE FUNCTIONS ====================

def analyze_stock_advanced(df: pd.DataFrame) -> Dict:
    """
    Run all advanced technical analysis on a stock
    Returns comprehensive analysis dictionary
    """
    ata = AdvancedTechnicalAnalysis()
    
    analysis = {
        # Market Structure
        'bos': ata.detect_bos(df),
        'choch': ata.detect_choch(df),
        'order_blocks': ata.detect_order_blocks(df),
        'fvg': ata.detect_fvg(df),
        'liquidity_pools': ata.detect_liquidity_pools(df),
        
        # Volume Analysis
        'volume_profile': ata.calculate_volume_profile(df),
        'volume_delta': ata.calculate_volume_delta(df),
        
        # Regime
        'market_regime': ata.detect_market_regime(df),
        
        # Candlestick Patterns
        'engulfing': ata.detect_engulfing_with_volume(df),
        'pin_bars': ata.detect_pin_bar_with_ob(df, ata.detect_order_blocks(df)),
        
        # Chart Patterns
        'head_shoulders': ata.detect_head_and_shoulders(df),
        'double_patterns': ata.detect_double_top_bottom(df),
        
        # Fibonacci
        'fibonacci': ata.calculate_fibonacci_levels(df)
    }
    
    return analysis
