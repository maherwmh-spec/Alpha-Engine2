"""
Alpha-Engine2 Redis Manager
Handles Redis connections and caching operations

Redis password is read via config_manager (environment / .env first).
"""

import json
import redis
from typing import Any, Optional, List
from loguru import logger
from config.config_manager import config


class RedisManager:
    """Redis connection and operations manager"""

    def __init__(self):
        self.client: Optional[redis.Redis] = None
        self._initialize()

    def _initialize(self):
        """Initialize Redis connection using environment-backed config"""
        try:
            params = config.get_redis_connection_params(db_index=0)
            host = params["host"]
            port = int(params["port"])
            db = int(params["db"])
            password = params["password"]

            logger.info(
                f"[RedisManager] Connecting to redis://:***@{host}:{port}/{db}"
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

            self.client.ping()
            logger.success(
                f"[RedisManager] Redis connection initialized successfully "
                f"(host={host}, port={port}, db={db})"
            )

        except ValueError as e:
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
        try:
            self.client.ping()
            logger.success("[RedisManager] Redis connection test successful")
            return True
        except Exception as e:
            logger.error(f"[RedisManager] Redis connection test failed: {e}")
            return False

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        try:
            if not isinstance(value, str):
                value = json.dumps(value, default=str)
            if ttl:
                self.client.setex(key, ttl, value)
            else:
                self.client.set(key, value)
            return True
        except Exception as e:
            logger.error(f"Failed to set key {key}: {e}")
            return False

    def get(self, key: str, default: Any = None) -> Any:
        try:
            value = self.client.get(key)
            if value is None:
                return default
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                return value
        except Exception as e:
            logger.error(f"Failed to get key {key}: {e}")
            return default

    def delete(self, key: str) -> bool:
        try:
            self.client.delete(key)
            return True
        except Exception as e:
            logger.error(f"Failed to delete key {key}: {e}")
            return False

    def exists(self, key: str) -> bool:
        try:
            return bool(self.client.exists(key))
        except Exception as e:
            logger.error(f"Failed to check key {key}: {e}")
            return False

    def expire(self, key: str, ttl: int) -> bool:
        try:
            self.client.expire(key, ttl)
            return True
        except Exception as e:
            logger.error(f"Failed to set expiration for key {key}: {e}")
            return False

    def hset(self, name: str, key: str, value: Any) -> bool:
        try:
            if not isinstance(value, str):
                value = json.dumps(value, default=str)
            self.client.hset(name, key, value)
            return True
        except Exception as e:
            logger.error(f"Failed to set hash {name}:{key}: {e}")
            return False

    def hget(self, name: str, key: str, default: Any = None) -> Any:
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
        try:
            data = self.client.hgetall(name)
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
        try:
            self.client.hdel(name, key)
            return True
        except Exception as e:
            logger.error(f"Failed to delete hash {name}:{key}: {e}")
            return False

    def lpush(self, key: str, *values: Any) -> bool:
        try:
            serialized = [json.dumps(v, default=str) if not isinstance(v, str) else v for v in values]
            self.client.lpush(key, *serialized)
            return True
        except Exception as e:
            logger.error(f"Failed to lpush to {key}: {e}")
            return False

    def rpush(self, key: str, *values: Any) -> bool:
        try:
            serialized = [json.dumps(v, default=str) if not isinstance(v, str) else v for v in values]
            self.client.rpush(key, *serialized)
            return True
        except Exception as e:
            logger.error(f"Failed to rpush to {key}: {e}")
            return False

    def lpop(self, key: str) -> Any:
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
        try:
            return self.client.llen(key)
        except Exception as e:
            logger.error(f"Failed to get length of {key}: {e}")
            return 0

    def sadd(self, key: str, *values: Any) -> bool:
        try:
            serialized = [json.dumps(v, default=str) if not isinstance(v, str) else v for v in values]
            self.client.sadd(key, *serialized)
            return True
        except Exception as e:
            logger.error(f"Failed to sadd to {key}: {e}")
            return False

    def smembers(self, key: str) -> set:
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
        try:
            if not isinstance(value, str):
                value = json.dumps(value, default=str)
            return bool(self.client.sismember(key, value))
        except Exception as e:
            logger.error(f"Failed to check member in {key}: {e}")
            return False

    def srem(self, key: str, *values: Any) -> bool:
        try:
            serialized = [json.dumps(v, default=str) if not isinstance(v, str) else v for v in values]
            self.client.srem(key, *serialized)
            return True
        except Exception as e:
            logger.error(f"Failed to remove from {key}: {e}")
            return False

    def cache_stock_price(self, symbol: str, price_data: dict, ttl: int = 60):
        return self.set(f"price:{symbol}", price_data, ttl)

    def get_cached_price(self, symbol: str) -> Optional[dict]:
        return self.get(f"price:{symbol}")

    def cache_indicators(self, symbol: str, timeframe: str, indicators: dict, ttl: int = 300):
        return self.set(f"indicators:{symbol}:{timeframe}", indicators, ttl)

    def get_cached_indicators(self, symbol: str, timeframe: str) -> Optional[dict]:
        return self.get(f"indicators:{symbol}:{timeframe}")

    def cache_signal(self, strategy: str, symbol: str, signal: dict, ttl: int = 300):
        return self.set(f"signal:{strategy}:{symbol}", signal, ttl)

    def get_cached_signal(self, strategy: str, symbol: str) -> Optional[dict]:
        return self.get(f"signal:{strategy}:{symbol}")

    def set_bot_state(self, bot_name: str, state: dict):
        return self.set(f"bot:state:{bot_name}", state)

    def get_bot_state(self, bot_name: str) -> Optional[dict]:
        return self.get(f"bot:state:{bot_name}")

    def set_bot_running(self, bot_name: str):
        return self.set(f"bot:running:{bot_name}", "1", ttl=3600)

    def is_bot_running(self, bot_name: str) -> bool:
        return self.exists(f"bot:running:{bot_name}")

    def clear_bot_running(self, bot_name: str):
        return self.delete(f"bot:running:{bot_name}")

    def flush_all(self):
        try:
            self.client.flushall()
            logger.warning("Redis flushed all data")
            return True
        except Exception as e:
            logger.error(f"Failed to flush Redis: {e}")
            return False

    def get_info(self) -> dict:
        try:
            return self.client.info()
        except Exception as e:
            logger.error(f"Failed to get Redis info: {e}")
            return {}

    def get_memory_usage(self) -> dict:
        try:
            info = self.client.info("memory")
            return {
                "used_memory": info.get("used_memory", 0),
                "used_memory_human": info.get("used_memory_human", "0B"),
                "used_memory_peak": info.get("used_memory_peak", 0),
                "used_memory_peak_human": info.get("used_memory_peak_human", "0B"),
            }
        except Exception as e:
            logger.error(f"Failed to get memory usage: {e}")
            return {}


redis_manager = RedisManager()


def cache_set(key: str, value: Any, ttl: Optional[int] = None) -> bool:
    return redis_manager.set(key, value, ttl)


def cache_get(key: str, default: Any = None) -> Any:
    return redis_manager.get(key, default)


def cache_delete(key: str) -> bool:
    return redis_manager.delete(key)


if __name__ == "__main__":
    print("Testing Redis connection...")
    if redis_manager.test_connection():
        print("✅ Redis connection successful!")
        print(f"Memory usage: {redis_manager.get_memory_usage()}")
    else:
        print("❌ Redis connection failed!")
