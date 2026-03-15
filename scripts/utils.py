"""
Alpha-Engine2 Utility Functions
Common helper functions used across the system
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
import pytz
from loguru import logger


# ========================================
# Date/Time Utilities
# ========================================

def get_saudi_time() -> datetime:
    """Get current time in Saudi Arabia timezone"""
    saudi_tz = pytz.timezone('Asia/Riyadh')
    return datetime.now(saudi_tz)


def is_trading_hours() -> bool:
    """Check if current time is within trading hours"""
    saudi_time = get_saudi_time()
    
    # Trading days: Sunday to Thursday
    if saudi_time.weekday() in [5, 6]:  # Friday, Saturday
        return False
    
    # Trading hours: 10:00 - 15:00
    trading_start = saudi_time.replace(hour=10, minute=0, second=0, microsecond=0)
    trading_end = saudi_time.replace(hour=15, minute=0, second=0, microsecond=0)
    
    return trading_start <= saudi_time <= trading_end


def get_next_trading_day() -> datetime:
    """Get the next trading day"""
    saudi_time = get_saudi_time()
    next_day = saudi_time + timedelta(days=1)
    
    # Skip Friday and Saturday
    while next_day.weekday() in [4, 5]:  # Friday, Saturday
        next_day += timedelta(days=1)
    
    return next_day.replace(hour=10, minute=0, second=0, microsecond=0)


def parse_timeframe(timeframe: str) -> timedelta:
    """
    Parse timeframe string to timedelta
    Examples: '1m', '5m', '15m', '1h', '4h', '1d'
    """
    unit = timeframe[-1]
    value = int(timeframe[:-1])
    
    if unit == 'm':
        return timedelta(minutes=value)
    elif unit == 'h':
        return timedelta(hours=value)
    elif unit == 'd':
        return timedelta(days=value)
    elif unit == 'w':
        return timedelta(weeks=value)
    else:
        raise ValueError(f"Unknown timeframe unit: {unit}")


# ========================================
# Data Processing Utilities
# ========================================

def calculate_percentage_change(old_value: float, new_value: float) -> float:
    """Calculate percentage change between two values"""
    if old_value == 0:
        return 0.0
    return ((new_value - old_value) / old_value) * 100


def normalize_symbol(symbol: str) -> str:
    """Normalize stock symbol (remove spaces, convert to uppercase)"""
    return symbol.strip().upper()


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Safely divide two numbers, return default if denominator is zero"""
    if denominator == 0:
        return default
    return numerator / denominator


def round_price(price: float, decimals: int = 2) -> float:
    """Round price to specified decimal places"""
    return round(price, decimals)


# ========================================
# Technical Analysis Utilities
# ========================================

def detect_trend(prices: List[float], window: int = 20) -> str:
    """
    Detect trend direction
    Returns: 'UPTREND', 'DOWNTREND', or 'SIDEWAYS'
    """
    if len(prices) < window:
        return 'SIDEWAYS'
    
    recent_prices = prices[-window:]
    first_half = np.mean(recent_prices[:window//2])
    second_half = np.mean(recent_prices[window//2:])
    
    change = calculate_percentage_change(first_half, second_half)
    
    if change > 2:
        return 'UPTREND'
    elif change < -2:
        return 'DOWNTREND'
    else:
        return 'SIDEWAYS'


def calculate_volatility(prices: List[float], window: int = 20) -> float:
    """Calculate price volatility (standard deviation)"""
    if len(prices) < window:
        return 0.0
    
    recent_prices = prices[-window:]
    return float(np.std(recent_prices))


def is_support_level(price: float, historical_prices: List[float], threshold: float = 0.02) -> bool:
    """Check if price is near a support level"""
    if len(historical_prices) < 20:
        return False
    
    # Find local minimums
    lows = []
    for i in range(1, len(historical_prices) - 1):
        if historical_prices[i] < historical_prices[i-1] and historical_prices[i] < historical_prices[i+1]:
            lows.append(historical_prices[i])
    
    # Check if current price is near any support
    for low in lows:
        if abs(price - low) / low < threshold:
            return True
    
    return False


def is_resistance_level(price: float, historical_prices: List[float], threshold: float = 0.02) -> bool:
    """Check if price is near a resistance level"""
    if len(historical_prices) < 20:
        return False
    
    # Find local maximums
    highs = []
    for i in range(1, len(historical_prices) - 1):
        if historical_prices[i] > historical_prices[i-1] and historical_prices[i] > historical_prices[i+1]:
            highs.append(historical_prices[i])
    
    # Check if current price is near any resistance
    for high in highs:
        if abs(price - high) / high < threshold:
            return True
    
    return False


# ========================================
# Risk Management Utilities
# ========================================

def calculate_position_size(
    capital: float,
    risk_percentage: float,
    entry_price: float,
    stop_loss: float
) -> int:
    """
    Calculate position size based on risk management
    Args:
        capital: Total capital
        risk_percentage: Percentage of capital to risk (e.g., 0.02 for 2%)
        entry_price: Entry price
        stop_loss: Stop loss price
    Returns:
        Number of shares to buy
    """
    risk_amount = capital * risk_percentage
    risk_per_share = abs(entry_price - stop_loss)
    
    if risk_per_share == 0:
        return 0
    
    position_size = risk_amount / risk_per_share
    return int(position_size)


def calculate_stop_loss(entry_price: float, percentage: float, position_type: str = 'LONG') -> float:
    """
    Calculate stop loss price
    Args:
        entry_price: Entry price
        percentage: Stop loss percentage (e.g., 0.02 for 2%)
        position_type: 'LONG' or 'SHORT'
    """
    if position_type == 'LONG':
        return entry_price * (1 - percentage)
    else:  # SHORT
        return entry_price * (1 + percentage)


def calculate_take_profit(entry_price: float, percentage: float, position_type: str = 'LONG') -> float:
    """
    Calculate take profit price
    Args:
        entry_price: Entry price
        percentage: Take profit percentage (e.g., 0.05 for 5%)
        position_type: 'LONG' or 'SHORT'
    """
    if position_type == 'LONG':
        return entry_price * (1 + percentage)
    else:  # SHORT
        return entry_price * (1 - percentage)


def calculate_sharpe_ratio(returns: List[float], risk_free_rate: float = 0.0) -> float:
    """
    Calculate Sharpe ratio
    Args:
        returns: List of returns
        risk_free_rate: Risk-free rate (default 0)
    """
    if len(returns) < 2:
        return 0.0
    
    returns_array = np.array(returns)
    excess_returns = returns_array - risk_free_rate
    
    if np.std(excess_returns) == 0:
        return 0.0
    
    return float(np.mean(excess_returns) / np.std(excess_returns))


def calculate_max_drawdown(equity_curve: List[float]) -> float:
    """
    Calculate maximum drawdown
    Args:
        equity_curve: List of equity values over time
    Returns:
        Maximum drawdown as percentage
    """
    if len(equity_curve) < 2:
        return 0.0
    
    equity_array = np.array(equity_curve)
    running_max = np.maximum.accumulate(equity_array)
    drawdown = (equity_array - running_max) / running_max
    
    return float(abs(np.min(drawdown)) * 100)


# ========================================
# Data Validation Utilities
# ========================================

def validate_price_data(data: Dict[str, Any]) -> bool:
    """Validate price data structure"""
    required_fields = ['open', 'high', 'low', 'close', 'volume']
    
    for field in required_fields:
        if field not in data:
            logger.error(f"Missing required field: {field}")
            return False
        
        if data[field] is None:
            logger.error(f"Field {field} is None")
            return False
        
        if field != 'volume' and data[field] <= 0:
            logger.error(f"Field {field} has invalid value: {data[field]}")
            return False
    
    # Validate OHLC relationship
    if not (data['low'] <= data['open'] <= data['high'] and 
            data['low'] <= data['close'] <= data['high']):
        logger.error("Invalid OHLC relationship")
        return False
    
    return True


def sanitize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Sanitize dataframe (remove NaN, inf, etc.)"""
    # Replace inf with NaN
    df = df.replace([np.inf, -np.inf], np.nan)
    
    # Forward fill NaN values
    df = df.fillna(method='ffill')
    
    # Backward fill remaining NaN
    df = df.fillna(method='bfill')
    
    # Drop any remaining NaN
    df = df.dropna()
    
    return df


# ========================================
# String Formatting Utilities
# ========================================

def format_number(number: float, decimals: int = 2) -> str:
    """Format number with thousands separator"""
    return f"{number:,.{decimals}f}"


def format_percentage(value: float, decimals: int = 2) -> str:
    """Format percentage"""
    return f"{value:.{decimals}f}%"


def format_currency(amount: float, currency: str = 'SAR') -> str:
    """Format currency"""
    return f"{format_number(amount)} {currency}"


def truncate_text(text: str, max_length: int = 100) -> str:
    """Truncate text to maximum length"""
    if len(text) <= max_length:
        return text
    return text[:max_length-3] + '...'


# ========================================
# Performance Metrics
# ========================================

def calculate_win_rate(wins: int, losses: int) -> float:
    """Calculate win rate"""
    total = wins + losses
    if total == 0:
        return 0.0
    return (wins / total) * 100


def calculate_profit_factor(gross_profit: float, gross_loss: float) -> float:
    """Calculate profit factor"""
    if gross_loss == 0:
        return 0.0 if gross_profit == 0 else float('inf')
    return gross_profit / abs(gross_loss)


def calculate_expectancy(win_rate: float, avg_win: float, avg_loss: float) -> float:
    """Calculate expectancy"""
    loss_rate = 1 - (win_rate / 100)
    return (win_rate / 100 * avg_win) - (loss_rate * abs(avg_loss))


# ========================================
# Error Handling Utilities
# ========================================

def safe_execute(func, *args, default=None, **kwargs):
    """Safely execute a function and return default on error"""
    try:
        return func(*args, **kwargs)
    except Exception as e:
        logger.error(f"Error executing {func.__name__}: {e}")
        return default


def retry_on_failure(func, max_retries: int = 3, delay: float = 1.0):
    """Retry function on failure"""
    import time
    
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying...")
                time.sleep(delay)
            else:
                logger.error(f"All {max_retries} attempts failed")
                raise


if __name__ == "__main__":
    # Test utilities
    print(f"Saudi Time: {get_saudi_time()}")
    print(f"Is Trading Hours: {is_trading_hours()}")
    print(f"Next Trading Day: {get_next_trading_day()}")
    print(f"Percentage Change: {calculate_percentage_change(100, 105)}%")
    print(f"Position Size: {calculate_position_size(100000, 0.02, 50, 48)}")
    print(f"Sharpe Ratio: {calculate_sharpe_ratio([0.01, 0.02, -0.01, 0.03, 0.01])}")
