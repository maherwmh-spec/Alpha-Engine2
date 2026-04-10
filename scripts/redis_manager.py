"""
Alpha-Engine2 Redis Manager
Handles Redis connections and caching operations

FIX: Redis password is now read EXCLUSIVELY from the REDIS_PASSWORD
     environment variable. If not set, a ValueError is raised immediately
     to prevent silent NOAUTH authentication failures.
"""

import os
import json
import redis
from typing import Any, Optional, List
from datetime import timedelta
from loguru import logger


def _get_redis_password() -> str:
    """
    Read Redis password from environment variables only.
    Priority:
      1. REDIS_PASSWORD env var (set in docker-compose)
      2. REDIS_URL env var (parsed)
    Raises ValueError if no password found.
    """
    # ── 1. REDIS_PASSWORD env var (primary source) ────────────────────────────
    password = os.getenv('REDIS_PASSWORD', '').strip()
    if password:
        return password

    # ── 2. Parse REDIS_URL env var ────────────────────────────────────────────
    redis_url = os.getenv('REDIS_URL', '').strip()
    if redis_url and '@' in redis_url:
        try:
            auth_part = redis_url.split('@')[0]   # redis://:PASSWORD
            password = auth_part.split(':')[-1]   # PASSWORD
            if password:
                return password
        except Exception:
            pass

    raise ValueError(
        "[FATAL] REDIS_PASSWORD environment variable is not set! "
        "Redis requires authentication (NOAUTH error will occur). "
        "Set REDIS_PASSWORD=alpha_redis_password_2024 in your environment or docker-compose."
    )


class RedisManager:
    """Redis connection and operations manager"""

    def __init__(self):
        self.client: Optional[redis.Redis] = None
        self._initialize()

    def _initialize(self):
        """Initialize Redis connection using REDIS_PASSWORD from environment"""
        try:
            # ── Read connection params from environment ────────────────────────
            host     = os.getenv('REDIS_HOST', 'redis')
            port     = int(os.getenv('REDIS_PORT', '6379'))
            db       = int(os.getenv('REDIS_DB', '0'))
            password = _get_redis_password()

            logger.info(
                f"[RedisManager] Connecting to redis://:{password[:4]}***@{host}:{port}/{db}"
            )

            self.client = redis.Redis(
                host=host,
                port=port,
                db=db,
                password=password,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
            )

            # Test connection — will raise if NOAUTH or connection refused
            self.client.ping()
            logger.success(
                f"[RedisManager] Redis connection initialized successfully "
                f"(host={host}, port={port}, db={db})"
            )

        except ValueError as e:
            # Missing password — re-raise immediately
            logger.critical(str(e))
            raise
        except redis.exceptions.AuthenticationError as e:
            logger.critical(
                f"[RedisManager] Redis NOAUTH error — password is wrong or missing: {e}"
            )
            raise
        except Exception as e:
            logger.error(f"[RedisManager] Failed to initialize Redis: {e}")
            raise

    def test_connection(self) -> bool:
        """Test Redis connection"""
        try:
            self.client.ping()
            logger.success("[RedisManager] Redis connection test successful")
            return True
        except Exception as e:
            logger.error(f"[RedisManager] Redis connection test failed: {e}")
            return False

    # ========================================
    # Basic Operations
    # ========================================

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        Set a key-value pair
        Args:
            key: Redis key
            value: Value to store (will be JSON serialized if not string)
            ttl: Time to live in seconds
        """
        try:
            if not isinstance(value, str):
                value = json.dumps(value)

            if ttl:
                self.client.setex(key, ttl, value)
            else:
                self.client.set(key, value)

            return True
        except Exception as e:
            logger.error(f"Failed to set key {key}: {e}")
            return False

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get value by key
        Args:
            key: Redis key
            default: Default value if key doesn't exist
        """
        try:
            value = self.client.get(key)
            if value is None:
                return default

            # Try to parse as JSON
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                return value
        except Exception as e:
            logger.error(f"Failed to get key {key}: {e}")
            return default

    def delete(self, key: str) -> bool:
        """Delete a key"""
        try:
            self.client.delete(key)
            return True
        except Exception as e:
            logger.error(f"Failed to delete key {key}: {e}")
            return False

    def exists(self, key: str) -> bool:
        """Check if key exists"""
        try:
            return bool(self.client.exists(key))
        except Exception as e:
            logger.error(f"Failed to check key {key}: {e}")
            return False

    def expire(self, key: str, ttl: int) -> bool:
        """Set expiration time for a key"""
        try:
            self.client.expire(key, ttl)
            return True
        except Exception as e:
            logger.error(f"Failed to set expiration for key {key}: {e}")
            return False

    # ========================================
    # Hash Operations
    # ========================================

    def hset(self, name: str, key: str, value: Any) -> bool:
        """Set hash field"""
        try:
            if not isinstance(value, str):
                value = json.dumps(value)
            self.client.hset(name, key, value)
            return True
        except Exception as e:
            logger.error(f"Failed to set hash {name}:{key}: {e}")
            return False

    def hget(self, name: str, key: str, default: Any = None) -> Any:
        """Get hash field"""
        try:
            value = self.client.hget(name, key)
            if value is None:
                return default

            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                return value
        except Exception as e:
            logger.error(f"Failed to get hash {name}:{key}: {e}")
            return default

    def hgetall(self, name: str) -> dict:
        """Get all hash fields"""
        try:
            data = self.client.hgetall(name)
            # Try to parse JSON values
            result = {}
            for k, v in data.items():
                try:
                    result[k] = json.loads(v)
                except (json.JSONDecodeError, TypeError):
                    result[k] = v
            return result
        except Exception as e:
            logger.error(f"Failed to get all hash {name}: {e}")
            return {}

    def hdel(self, name: str, key: str) -> bool:
        """Delete hash field"""
        try:
            self.client.hdel(name, key)
            return True
        except Exception as e:
            logger.error(f"Failed to delete hash {name}:{key}: {e}")
            return False

    # ========================================
    # List Operations
    # ========================================

    def lpush(self, key: str, *values: Any) -> bool:
        """Push values to the left of list"""
        try:
            serialized = [json.dumps(v) if not isinstance(v, str) else v for v in values]
            self.client.lpush(key, *serialized)
            return True
        except Exception as e:
            logger.error(f"Failed to lpush to {key}: {e}")
            return False

    def rpush(self, key: str, *values: Any) -> bool:
        """Push values to the right of list"""
        try:
            serialized = [json.dumps(v) if not isinstance(v, str) else v for v in values]
            self.client.rpush(key, *serialized)
            return True
        except Exception as e:
            logger.error(f"Failed to rpush to {key}: {e}")
            return False

    def lpop(self, key: str) -> Any:
        """Pop value from the left of list"""
        try:
            value = self.client.lpop(key)
            if value is None:
                return None
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                return value
        except Exception as e:
            logger.error(f"Failed to lpop from {key}: {e}")
            return None

    def rpop(self, key: str) -> Any:
        """Pop value from the right of list"""
        try:
            value = self.client.rpop(key)
            if value is None:
                return None
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                return value
        except Exception as e:
            logger.error(f"Failed to rpop from {key}: {e}")
            return None

    def lrange(self, key: str, start: int = 0, end: int = -1) -> List[Any]:
        """Get range of values from list"""
        try:
            values = self.client.lrange(key, start, end)
            result = []
            for v in values:
                try:
                    result.append(json.loads(v))
                except (json.JSONDecodeError, TypeError):
                    result.append(v)
            return result
        except Exception as e:
            logger.error(f"Failed to lrange from {key}: {e}")
            return []

    def llen(self, key: str) -> int:
        """Get length of list"""
        try:
            return self.client.llen(key)
        except Exception as e:
            logger.error(f"Failed to get length of {key}: {e}")
            return 0

    # ========================================
    # Set Operations
    # ========================================

    def sadd(self, key: str, *values: Any) -> bool:
        """Add values to set"""
        try:
            serialized = [json.dumps(v) if not isinstance(v, str) else v for v in values]
            self.client.sadd(key, *serialized)
            return True
        except Exception as e:
            logger.error(f"Failed to sadd to {key}: {e}")
            return False

    def smembers(self, key: str) -> set:
        """Get all members of set"""
        try:
            values = self.client.smembers(key)
            result = set()
            for v in values:
                try:
                    result.add(json.loads(v))
                except (json.JSONDecodeError, TypeError):
                    result.add(v)
            return result
        except Exception as e:
            logger.error(f"Failed to get members of {key}: {e}")
            return set()

    def sismember(self, key: str, value: Any) -> bool:
        """Check if value is in set"""
        try:
            if not isinstance(value, str):
                value = json.dumps(value)
            return bool(self.client.sismember(key, value))
        except Exception as e:
            logger.error(f"Failed to check member in {key}: {e}")
            return False

    def srem(self, key: str, *values: Any) -> bool:
        """Remove values from set"""
        try:
            serialized = [json.dumps(v) if not isinstance(v, str) else v for v in values]
            self.client.srem(key, *serialized)
            return True
        except Exception as e:
            logger.error(f"Failed to remove from {key}: {e}")
            return False

    # ========================================
    # Cache Operations
    # ========================================

    def cache_stock_price(self, symbol: str, price_data: dict, ttl: int = 60):
        """Cache stock price data"""
        key = f"price:{symbol}"
        return self.set(key, price_data, ttl)

    def get_cached_price(self, symbol: str) -> Optional[dict]:
        """Get cached stock price"""
        key = f"price:{symbol}"
        return self.get(key)

    def cache_indicators(self, symbol: str, timeframe: str, indicators: dict, ttl: int = 300):
        """Cache technical indicators"""
        key = f"indicators:{symbol}:{timeframe}"
        return self.set(key, indicators, ttl)

    def get_cached_indicators(self, symbol: str, timeframe: str) -> Optional[dict]:
        """Get cached technical indicators"""
        key = f"indicators:{symbol}:{timeframe}"
        return self.get(key)

    def cache_signal(self, strategy: str, symbol: str, signal: dict, ttl: int = 300):
        """Cache trading signal"""
        key = f"signal:{strategy}:{symbol}"
        return self.set(key, signal, ttl)

    def get_cached_signal(self, strategy: str, symbol: str) -> Optional[dict]:
        """Get cached trading signal"""
        key = f"signal:{strategy}:{symbol}"
        return self.get(key)

    # ========================================
    # Bot State Management
    # ========================================

    def set_bot_state(self, bot_name: str, state: dict):
        """Set bot state"""
        key = f"bot:state:{bot_name}"
        return self.set(key, state)

    def get_bot_state(self, bot_name: str) -> Optional[dict]:
        """Get bot state"""
        key = f"bot:state:{bot_name}"
        return self.get(key)

    def set_bot_running(self, bot_name: str):
        """Mark bot as running"""
        key = f"bot:running:{bot_name}"
        return self.set(key, "1", ttl=3600)  # 1 hour TTL

    def is_bot_running(self, bot_name: str) -> bool:
        """Check if bot is running"""
        key = f"bot:running:{bot_name}"
        return self.exists(key)

    def clear_bot_running(self, bot_name: str):
        """Clear bot running flag"""
        key = f"bot:running:{bot_name}"
        return self.delete(key)

    # ========================================
    # Utility Operations
    # ========================================

    def flush_all(self):
        """Flush all data (use with caution!)"""
        try:
            self.client.flushall()
            logger.warning("Redis flushed all data")
            return True
        except Exception as e:
            logger.error(f"Failed to flush Redis: {e}")
            return False

    def get_info(self) -> dict:
        """Get Redis server info"""
        try:
            return self.client.info()
        except Exception as e:
            logger.error(f"Failed to get Redis info: {e}")
            return {}

    def get_memory_usage(self) -> dict:
        """Get Redis memory usage"""
        try:
            info = self.client.info('memory')
            return {
                'used_memory': info.get('used_memory', 0),
                'used_memory_human': info.get('used_memory_human', '0B'),
                'used_memory_peak': info.get('used_memory_peak', 0),
                'used_memory_peak_human': info.get('used_memory_peak_human', '0B'),
            }
        except Exception as e:
            logger.error(f"Failed to get memory usage: {e}")
            return {}


# Global Redis instance — initialized with REDIS_PASSWORD from environment
redis_manager = RedisManager()


# Convenience functions
def cache_set(key: str, value: Any, ttl: Optional[int] = None) -> bool:
    """Set cache value"""
    return redis_manager.set(key, value, ttl)


def cache_get(key: str, default: Any = None) -> Any:
    """Get cache value"""
    return redis_manager.get(key, default)


def cache_delete(key: str) -> bool:
    """Delete cache key"""
    return redis_manager.delete(key)


if __name__ == "__main__":
    # Test Redis connection
    print("Testing Redis connection...")
    if redis_manager.test_connection():
        print("✅ Redis connection successful!")
        print(f"Memory usage: {redis_manager.get_memory_usage()}")
    else:
        print("❌ Redis connection failed!")
