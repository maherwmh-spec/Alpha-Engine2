"""
Sahmk API Client - عميل سهمك API
Handles both REST (historical data) and WebSocket (real-time data)
with full error handling, auto-retry, and logging.

Features:
- REST API: Fetch OHLCV historical data
- WebSocket: Real-time tick data (every second)
- Auto-retry after 30 seconds on connection failure
- Rate limiting (60 requests/minute)
- Comprehensive logging
- Saves real-time data as 1m candles to TimescaleDB
"""

import os
import asyncio
import json
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable, Any
from collections import defaultdict, deque

import requests
import websocket
import pandas as pd
import numpy as np
from loguru import logger

from config.config_manager import config
from scripts.redis_manager import redis_manager


class SahmkAPIError(Exception):
    """Custom exception for Sahmk API errors"""
    pass


class SahmkRateLimiter:
    """Token bucket rate limiter for API calls"""
    
    def __init__(self, calls_per_minute: int = 60):
        self.calls_per_minute = calls_per_minute
        self.calls = deque()
        self._lock = threading.Lock()
    
    def wait_if_needed(self):
        """Wait if rate limit would be exceeded"""
        with self._lock:
            now = time.time()
            # Remove calls older than 1 minute
            while self.calls and self.calls[0] < now - 60:
                self.calls.popleft()
            
            # If at limit, wait
            if len(self.calls) >= self.calls_per_minute:
                sleep_time = 60 - (now - self.calls[0]) + 0.1
                if sleep_time > 0:
                    logger.debug(f"Rate limit reached, waiting {sleep_time:.2f}s")
                    time.sleep(sleep_time)
            
            self.calls.append(time.time())


class CandleAggregator:
    """
    Aggregates tick data into 1-minute candles (OHLCV)
    Stores completed candles to TimescaleDB
    """
    
    def __init__(self):
        # Buffer: symbol -> list of ticks in current minute
        self.tick_buffer: Dict[str, List[Dict]] = defaultdict(list)
        # Current candle per symbol
        self.current_candles: Dict[str, Dict] = {}
        self._lock = threading.Lock()
        self.logger = logger.bind(component="CandleAggregator")
    
    def add_tick(self, symbol: str, price: float, volume: float, timestamp: datetime) -> Optional[Dict]:
        """
        Add a tick and return completed candle if minute boundary crossed
        
        Args:
            symbol: Stock symbol (e.g., "2222")
            price: Current price
            volume: Volume for this tick
            timestamp: Tick timestamp
            
        Returns:
            Completed 1m candle dict if minute boundary crossed, else None
        """
        with self._lock:
            minute_key = timestamp.replace(second=0, microsecond=0)
            
            if symbol not in self.current_candles:
                # Start new candle
                self.current_candles[symbol] = {
                    'symbol': symbol,
                    'timestamp': minute_key,
                    'open': price,
                    'high': price,
                    'low': price,
                    'close': price,
                    'volume': volume,
                    'tick_count': 1
                }
                return None
            
            current = self.current_candles[symbol]
            
            # Check if we've crossed a minute boundary
            if minute_key > current['timestamp']:
                # Complete the old candle
                completed_candle = current.copy()
                
                # Start new candle
                self.current_candles[symbol] = {
                    'symbol': symbol,
                    'timestamp': minute_key,
                    'open': price,
                    'high': price,
                    'low': price,
                    'close': price,
                    'volume': volume,
                    'tick_count': 1
                }
                
                self.logger.debug(
                    f"Completed 1m candle for {symbol}: "
                    f"O={completed_candle['open']:.2f} H={completed_candle['high']:.2f} "
                    f"L={completed_candle['low']:.2f} C={completed_candle['close']:.2f} "
                    f"V={completed_candle['volume']:.0f}"
                )
                
                return completed_candle
            else:
                # Update current candle
                current['high'] = max(current['high'], price)
                current['low'] = min(current['low'], price)
                current['close'] = price
                current['volume'] += volume
                current['tick_count'] += 1
                return None


class SahmkClient:
    """
    Full Sahmk API Client with REST + WebSocket support
    
    REST: Fetch historical OHLCV data
    WebSocket: Real-time streaming data (every second)
    """
    
    def __init__(self):
        self.logger = logger.bind(component="SahmkClient")
        
        # Load configuration
        sahmk_config = config.get('sahmk', {})
        
        # API Key - read from config.yaml or environment variable
        self.api_key = (
            os.getenv('SAHMK_API_KEY') or 
            sahmk_config.get('api_key', '')
        )
        
        if not self.api_key or self.api_key == 'YOUR_SAHMK_API_KEY_HERE':
            self.logger.error("❌ SAHMK_API_KEY not configured!")
            raise ValueError("SAHMK_API_KEY is not set in config.yaml or environment variables")
        
        self.logger.success(
            f"✅ SAHMK API Key loaded successfully: "
            f"{self.api_key[:12]}...{self.api_key[-4:]}"
        )
        
        # URLs
        self.base_url = sahmk_config.get('base_url', 'https://api.sahmk.com')
        self.websocket_url = sahmk_config.get('websocket_url', 'wss://stream.sahmk.com')
        
        # Settings
        self.rest_timeout = sahmk_config.get('rest_timeout', 30)
        self.reconnect_delay = sahmk_config.get('websocket_reconnect_delay', 30)
        self.max_retries = sahmk_config.get('max_retries', 5)
        self.rate_limit = sahmk_config.get('rate_limit_per_minute', 60)
        
        # Components
        self.rate_limiter = SahmkRateLimiter(self.rate_limit)
        self.candle_aggregator = CandleAggregator()
        
        # WebSocket state
        self._ws: Optional[websocket.WebSocketApp] = None
        self._ws_thread: Optional[threading.Thread] = None
        self._ws_running = False
        self._subscribed_symbols: List[str] = []
        self._ws_retry_count = 0
        
        # Callbacks
        self._on_candle_complete: Optional[Callable] = None
        self._on_tick: Optional[Callable] = None
        
        # REST session with retry
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {self.api_key}',
            'X-API-Key': self.api_key,
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Agent': 'AlphaEngine2/1.0'
        })
        
        self.logger.info(
            f"SahmkClient initialized | "
            f"Base URL: {self.base_url} | "
            f"WS URL: {self.websocket_url}"
        )
    
    # =============================================
    # REST API Methods
    # =============================================
    
    def _make_request(self, method: str, endpoint: str, params: Dict = None, 
                      data: Dict = None, retry: int = 0) -> Dict:
        """
        Make REST API request with retry logic
        
        Args:
            method: HTTP method (GET, POST)
            endpoint: API endpoint
            params: Query parameters
            data: Request body
            retry: Current retry count
            
        Returns:
            API response as dict
        """
        self.rate_limiter.wait_if_needed()
        
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        try:
            self.logger.debug(f"REST {method} {url} | params={params}")
            
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                json=data,
                timeout=self.rest_timeout
            )
            
            # Log response
            self.logger.debug(
                f"REST Response: {response.status_code} | "
                f"Size: {len(response.content)} bytes"
            )
            
            # Handle HTTP errors
            if response.status_code == 401:
                self.logger.error("❌ Sahmk API: Unauthorized - Check API key")
                raise SahmkAPIError("Unauthorized: Invalid API key")
            
            elif response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                self.logger.warning(f"⚠️ Rate limited, waiting {retry_after}s")
                time.sleep(retry_after)
                return self._make_request(method, endpoint, params, data, retry)
            
            elif response.status_code == 404:
                self.logger.warning(f"⚠️ Endpoint not found: {url}")
                raise SahmkAPIError(f"Endpoint not found: {endpoint}")
            
            elif response.status_code >= 500:
                raise SahmkAPIError(f"Server error: {response.status_code}")
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.ConnectionError as e:
            self.logger.error(f"❌ Connection error: {e}")
            if retry < self.max_retries:
                wait_time = self.reconnect_delay * (retry + 1)
                self.logger.info(f"🔄 Retrying in {wait_time}s (attempt {retry+1}/{self.max_retries})")
                time.sleep(wait_time)
                return self._make_request(method, endpoint, params, data, retry + 1)
            raise
            
        except requests.exceptions.Timeout as e:
            self.logger.error(f"❌ Request timeout: {e}")
            if retry < self.max_retries:
                self.logger.info(f"🔄 Retrying in {self.reconnect_delay}s (attempt {retry+1}/{self.max_retries})")
                time.sleep(self.reconnect_delay)
                return self._make_request(method, endpoint, params, data, retry + 1)
            raise
            
        except Exception as e:
            self.logger.error(f"❌ Unexpected error in REST request: {e}")
            raise
    
    def get_historical_ohlcv(self, symbol: str, timeframe: str = '1m',
                              start_date: Optional[datetime] = None,
                              end_date: Optional[datetime] = None,
                              limit: int = 1000) -> pd.DataFrame:
        """
        Fetch historical OHLCV data via REST API
        
        Args:
            symbol: Stock symbol (e.g., "2222" for Aramco)
            timeframe: Candle timeframe (1m, 5m, 15m, 30m, 1h, 1d)
            start_date: Start date for historical data
            end_date: End date (defaults to now)
            limit: Max number of candles
            
        Returns:
            DataFrame with OHLCV data
        """
        try:
            self.logger.info(f"📊 Fetching historical OHLCV: {symbol} | {timeframe} | limit={limit}")
            
            if end_date is None:
                end_date = datetime.now()
            if start_date is None:
                start_date = end_date - timedelta(days=30)
            
            params = {
                'symbol': symbol,
                'interval': timeframe,
                'from': int(start_date.timestamp()),
                'to': int(end_date.timestamp()),
                'limit': limit
            }
            
            # Try multiple possible endpoint formats
            endpoints_to_try = [
                f'v1/ohlcv/{symbol}',
                f'v1/candles/{symbol}',
                f'v1/history/{symbol}',
                f'ohlcv',
                f'candles'
            ]
            
            data = None
            for endpoint in endpoints_to_try:
                try:
                    data = self._make_request('GET', endpoint, params=params)
                    break
                except SahmkAPIError as e:
                    if 'not found' in str(e).lower():
                        continue
                    raise
            
            if data is None:
                self.logger.warning(f"⚠️ Could not fetch data for {symbol}, returning empty DataFrame")
                return pd.DataFrame()
            
            # Parse response - handle different response formats
            candles = []
            
            if isinstance(data, list):
                candles = data
            elif isinstance(data, dict):
                candles = data.get('data', data.get('candles', data.get('ohlcv', [])))
            
            if not candles:
                self.logger.warning(f"⚠️ No candles returned for {symbol}")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(candles)
            
            # Normalize column names
            column_mapping = {
                't': 'timestamp', 'time': 'timestamp', 'date': 'timestamp',
                'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume',
                'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'
            }
            df.rename(columns=column_mapping, inplace=True)
            
            # Convert timestamp
            if 'timestamp' in df.columns:
                if df['timestamp'].dtype in ['int64', 'float64']:
                    # Unix timestamp
                    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
                else:
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Ensure numeric columns
            for col in ['open', 'high', 'low', 'close', 'volume']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Add symbol column
            df['symbol'] = symbol
            
            # Sort by timestamp
            if 'timestamp' in df.columns:
                df.sort_values('timestamp', inplace=True)
                df.reset_index(drop=True, inplace=True)
            
            self.logger.success(
                f"✅ Fetched {len(df)} candles for {symbol} ({timeframe}) | "
                f"From: {df['timestamp'].min()} To: {df['timestamp'].max()}"
            )
            
            # Cache in Redis
            cache_key = f"ohlcv:{symbol}:{timeframe}"
            redis_manager.set(cache_key, df.to_dict('records'), ttl=300)
            
            return df
            
        except Exception as e:
            self.logger.error(f"❌ Error fetching historical OHLCV for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_symbols_list(self) -> List[str]:
        """
        Fetch list of all TASI symbols
        
        Returns:
            List of symbol codes
        """
        try:
            self.logger.info("📋 Fetching TASI symbols list")
            
            # Try cached version first
            cached = redis_manager.get('sahmk:symbols_list')
            if cached:
                return cached
            
            data = self._make_request('GET', 'v1/symbols', params={'market': 'TASI'})
            
            symbols = []
            if isinstance(data, list):
                symbols = [str(s.get('symbol', s)) for s in data if s]
            elif isinstance(data, dict):
                raw = data.get('data', data.get('symbols', []))
                symbols = [str(s.get('symbol', s)) for s in raw if s]
            
            if symbols:
                # Cache for 1 hour
                redis_manager.set('sahmk:symbols_list', symbols, ttl=3600)
                self.logger.success(f"✅ Fetched {len(symbols)} TASI symbols")
            
            return symbols
            
        except Exception as e:
            self.logger.error(f"❌ Error fetching symbols list: {e}")
            # Return default TASI symbols if API fails
            return [
                "2222", "1120", "2010", "2350", "4200",
                "1180", "2380", "3020", "1010", "4030"
            ]
    
    def get_quote(self, symbol: str) -> Dict:
        """
        Fetch current quote for a symbol
        
        Args:
            symbol: Stock symbol
            
        Returns:
            Dict with current price, change, volume, etc.
        """
        try:
            data = self._make_request('GET', f'v1/quote/{symbol}')
            
            quote = {
                'symbol': symbol,
                'price': float(data.get('price', data.get('last', 0))),
                'change': float(data.get('change', 0)),
                'change_pct': float(data.get('change_pct', data.get('changePercent', 0))),
                'volume': float(data.get('volume', 0)),
                'timestamp': datetime.now()
            }
            
            # Cache for 5 seconds
            redis_manager.set(f"quote:{symbol}", quote, ttl=5)
            
            return quote
            
        except Exception as e:
            self.logger.error(f"❌ Error fetching quote for {symbol}: {e}")
            return {}
    
    # =============================================
    # WebSocket Methods
    # =============================================
    
    def set_on_candle_complete(self, callback: Callable):
        """Set callback for when a 1m candle is completed"""
        self._on_candle_complete = callback
    
    def set_on_tick(self, callback: Callable):
        """Set callback for each tick received"""
        self._on_tick = callback
    
    def _on_ws_message(self, ws, message: str):
        """Handle incoming WebSocket message"""
        try:
            data = json.loads(message)
            
            # Handle different message formats
            msg_type = data.get('type', data.get('event', ''))
            
            if msg_type in ('trade', 'tick', 'price', 'quote'):
                symbol = str(data.get('symbol', data.get('s', '')))
                price = float(data.get('price', data.get('p', data.get('last', 0))))
                volume = float(data.get('volume', data.get('v', data.get('size', 0))))
                
                # Parse timestamp
                ts_raw = data.get('timestamp', data.get('t', data.get('time')))
                if ts_raw:
                    if isinstance(ts_raw, (int, float)):
                        timestamp = datetime.fromtimestamp(ts_raw / 1000 if ts_raw > 1e10 else ts_raw)
                    else:
                        timestamp = datetime.fromisoformat(str(ts_raw))
                else:
                    timestamp = datetime.now()
                
                if symbol and price > 0:
                    # Call tick callback
                    if self._on_tick:
                        self._on_tick({
                            'symbol': symbol,
                            'price': price,
                            'volume': volume,
                            'timestamp': timestamp
                        })
                    
                    # Aggregate into candle
                    completed_candle = self.candle_aggregator.add_tick(
                        symbol, price, volume, timestamp
                    )
                    
                    if completed_candle and self._on_candle_complete:
                        self._on_candle_complete(completed_candle)
                    
                    # Cache latest price
                    redis_manager.set(f"realtime:{symbol}", {
                        'price': price,
                        'volume': volume,
                        'timestamp': timestamp
                    }, ttl=10)
            
            elif msg_type == 'heartbeat':
                self.logger.debug("💓 WebSocket heartbeat received")
            
            elif msg_type == 'subscribed':
                symbols = data.get('symbols', [])
                self.logger.success(f"✅ WebSocket subscribed to: {symbols}")
            
            elif msg_type == 'error':
                error_msg = data.get('message', 'Unknown error')
                self.logger.error(f"❌ WebSocket server error: {error_msg}")
                
        except json.JSONDecodeError as e:
            self.logger.warning(f"⚠️ Invalid JSON from WebSocket: {e}")
        except Exception as e:
            self.logger.error(f"❌ Error processing WebSocket message: {e}")
    
    def _on_ws_error(self, ws, error):
        """Handle WebSocket error"""
        self.logger.error(f"❌ WebSocket error: {error}")
    
    def _on_ws_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket close - auto reconnect"""
        self.logger.warning(
            f"⚠️ WebSocket closed: code={close_status_code} msg={close_msg}"
        )
        
        if self._ws_running:
            self._ws_retry_count += 1
            wait_time = min(self.reconnect_delay * self._ws_retry_count, 300)
            self.logger.info(
                f"🔄 WebSocket reconnecting in {wait_time}s "
                f"(attempt {self._ws_retry_count})"
            )
            time.sleep(wait_time)
            
            if self._ws_running and self._subscribed_symbols:
                self._connect_websocket(self._subscribed_symbols)
    
    def _on_ws_open(self, ws):
        """Handle WebSocket connection open"""
        self.logger.success("✅ WebSocket connected to Sahmk API")
        self._ws_retry_count = 0  # Reset retry count on success
        
        # Subscribe to symbols
        if self._subscribed_symbols:
            subscribe_msg = {
                'action': 'subscribe',
                'type': 'trade',
                'symbols': self._subscribed_symbols,
                'api_key': self.api_key
            }
            ws.send(json.dumps(subscribe_msg))
            self.logger.info(f"📡 Subscribing to {len(self._subscribed_symbols)} symbols")
    
    def _connect_websocket(self, symbols: List[str]):
        """Internal method to create WebSocket connection"""
        try:
            ws_url = f"{self.websocket_url}?token={self.api_key}"
            
            self._ws = websocket.WebSocketApp(
                ws_url,
                on_open=self._on_ws_open,
                on_message=self._on_ws_message,
                on_error=self._on_ws_error,
                on_close=self._on_ws_close,
                header={
                    'Authorization': f'Bearer {self.api_key}',
                    'X-API-Key': self.api_key
                }
            )
            
            self._ws.run_forever(
                ping_interval=30,
                ping_timeout=10,
                reconnect=5
            )
            
        except Exception as e:
            self.logger.error(f"❌ WebSocket connection error: {e}")
            if self._ws_running:
                self.logger.info(f"🔄 Retrying WebSocket in {self.reconnect_delay}s")
                time.sleep(self.reconnect_delay)
                self._connect_websocket(symbols)
    
    def start_realtime_stream(self, symbols: List[str]):
        """
        Start real-time WebSocket stream for given symbols
        
        Args:
            symbols: List of stock symbols to subscribe to
        """
        if self._ws_running:
            self.logger.warning("⚠️ WebSocket already running")
            return
        
        self._subscribed_symbols = symbols
        self._ws_running = True
        self._ws_retry_count = 0
        
        self.logger.info(
            f"🚀 Starting real-time stream for {len(symbols)} symbols: "
            f"{symbols[:5]}{'...' if len(symbols) > 5 else ''}"
        )
        
        # Run WebSocket in background thread
        self._ws_thread = threading.Thread(
            target=self._connect_websocket,
            args=(symbols,),
            daemon=True,
            name="SahmkWebSocket"
        )
        self._ws_thread.start()
        
        self.logger.success("✅ Real-time stream started in background thread")
    
    def stop_realtime_stream(self):
        """Stop the WebSocket stream"""
        self._ws_running = False
        
        if self._ws:
            self._ws.close()
            self._ws = None
        
        if self._ws_thread and self._ws_thread.is_alive():
            self._ws_thread.join(timeout=5)
        
        self.logger.info("🛑 Real-time stream stopped")
    
    def subscribe_symbols(self, symbols: List[str]):
        """Add more symbols to the live subscription"""
        new_symbols = [s for s in symbols if s not in self._subscribed_symbols]
        if not new_symbols:
            return
        
        self._subscribed_symbols.extend(new_symbols)
        
        if self._ws and self._ws_running:
            subscribe_msg = {
                'action': 'subscribe',
                'type': 'trade',
                'symbols': new_symbols,
                'api_key': self.api_key
            }
            self._ws.send(json.dumps(subscribe_msg))
            self.logger.info(f"📡 Added {len(new_symbols)} new symbols to stream")
    
    def is_connected(self) -> bool:
        """Check if WebSocket is connected"""
        return (
            self._ws_running and 
            self._ws_thread is not None and 
            self._ws_thread.is_alive()
        )
    
    def get_connection_status(self) -> Dict:
        """Get detailed connection status"""
        return {
            'rest_api': 'configured',
            'api_key_loaded': bool(self.api_key),
            'websocket_running': self._ws_running,
            'websocket_connected': self.is_connected(),
            'subscribed_symbols': len(self._subscribed_symbols),
            'retry_count': self._ws_retry_count
        }


# Singleton instance
_sahmk_client: Optional[SahmkClient] = None


def get_sahmk_client() -> SahmkClient:
    """Get or create singleton SahmkClient instance"""
    global _sahmk_client
    if _sahmk_client is None:
        _sahmk_client = SahmkClient()
    return _sahmk_client
