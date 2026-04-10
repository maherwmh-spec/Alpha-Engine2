"""
Alpha-Engine2 Configuration Manager
Loads config.yaml and provides typed access to all settings.

FIX: get_redis_url() now enforces REDIS_PASSWORD from environment variable.
     If REDIS_PASSWORD is not set, a ValueError is raised immediately.
     This prevents silent NOAUTH failures.
"""
import os
from pathlib import Path
from typing import Any, Optional
import yaml
from loguru import logger


class ConfigManager:
    """Singleton configuration manager"""

    _instance = None
    _config: dict = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load()
        return cls._instance

    def _load(self):
        """Load configuration from config.yaml"""
        config_path = Path(__file__).parent / "config.yaml"
        logger.info(f"Loading configuration from: {config_path}")
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                self._config = yaml.safe_load(f) or {}
            logger.success("Configuration loaded successfully")
        except FileNotFoundError:
            logger.error(f"config.yaml not found at {config_path}")
            self._config = {}
        except yaml.YAMLError as e:
            logger.error(f"Error parsing config.yaml: {e}")
            self._config = {}

    def get(self, key: str, default: Any = None) -> Any:
        """Get a top-level config value"""
        return self._config.get(key, default)

    def get_nested(self, *keys, default: Any = None) -> Any:
        """Get a nested config value by dot-path keys"""
        val = self._config
        for k in keys:
            if not isinstance(val, dict):
                return default
            val = val.get(k, default)
        return val

    # ── Convenience helpers ──────────────────────────────────

    def get_database_url(self) -> str:
        """Return SQLAlchemy-compatible PostgreSQL URL"""
        db = self._config.get("database", {})
        host     = os.getenv("DB_HOST",     db.get("host",     "postgres"))
        port     = os.getenv("DB_PORT",     str(db.get("port", 5432)))
        name     = os.getenv("DB_NAME",     db.get("name",     "alpha_engine"))
        user     = os.getenv("DB_USER",     db.get("user",     "alpha_user"))
        password = os.getenv("DB_PASSWORD", db.get("password", "alpha_password_2024"))
        return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}"

    def get_asyncpg_dsn(self) -> str:
        """Return asyncpg-compatible DSN"""
        db = self._config.get("database", {})
        host     = os.getenv("DB_HOST",     db.get("host",     "postgres"))
        port     = os.getenv("DB_PORT",     str(db.get("port", 5432)))
        name     = os.getenv("DB_NAME",     db.get("name",     "alpha_engine"))
        user     = os.getenv("DB_USER",     db.get("user",     "alpha_user"))
        password = os.getenv("DB_PASSWORD", db.get("password", "alpha_password_2024"))
        return f"postgresql://{user}:{password}@{host}:{port}/{name}"

    def get_redis_url(self, db_index: int = 0) -> str:
        """
        Return authenticated Redis URL.

        Priority order for password:
          1. REDIS_PASSWORD environment variable  ← MANDATORY in Docker
          2. REDIS_URL environment variable (parsed)
          3. config.yaml redis.password fallback

        Raises ValueError if no password is found, preventing silent NOAUTH.
        """
        import os

        host = os.getenv("REDIS_HOST", self._config.get("redis", {}).get("host", "redis"))
        port = os.getenv("REDIS_PORT", str(self._config.get("redis", {}).get("port", 6379)))
        db   = os.getenv("REDIS_DB",   str(self._config.get("redis", {}).get("db", db_index)))

        # ── 1. Try REDIS_PASSWORD env var first (set in docker-compose) ──────
        password = os.getenv("REDIS_PASSWORD", "").strip()

        # ── 2. Try parsing REDIS_URL env var ─────────────────────────────────
        if not password:
            redis_url_env = os.getenv("REDIS_URL", "").strip()
            if redis_url_env and "@" in redis_url_env:
                # Format: redis://:PASSWORD@host:port/db
                try:
                    auth_part = redis_url_env.split("@")[0]  # redis://:PASSWORD
                    password = auth_part.split(":")[-1]       # PASSWORD
                except Exception:
                    password = ""

        # ── 3. Fallback to config.yaml (last resort, not recommended) ────────
        if not password:
            password = self._config.get("redis", {}).get("password", "").strip()

        # ── Validation: refuse to connect without password ────────────────────
        if not password:
            raise ValueError(
                "[FATAL] REDIS_PASSWORD environment variable is not set! "
                "Redis requires authentication. "
                "Set REDIS_PASSWORD=alpha_redis_password_2024 in your environment."
            )

        logger.debug(f"[Redis] Connecting to redis://:{password[:4]}***@{host}:{port}/{db}")
        return f"redis://:{password}@{host}:{port}/{db}"

    def get_redis_url_for_backend(self) -> str:
        """Return Redis URL for Celery result backend (db=1)"""
        url = self.get_redis_url(db_index=1)
        # Replace /0 with /1 at the end if needed
        if url.endswith("/0"):
            url = url[:-2] + "/1"
        return url

    def get_telegram_token(self) -> str:
        return os.getenv(
            "TELEGRAM_BOT_TOKEN",
            self._config.get("telegram", {}).get("bot_token", "")
        )

    def get_telegram_chat_id(self) -> str:
        return os.getenv(
            "TELEGRAM_CHAT_ID",
            str(self._config.get("telegram", {}).get("chat_id", ""))
        )

    def get_sahmk_api_key(self) -> str:
        return os.getenv(
            "SAHMK_API_KEY",
            self._config.get("sahmk", {}).get("api_key", "")
        )

    def get_sahmk_ws_url(self) -> str:
        sahmk = self._config.get("sahmk", {})
        base  = sahmk.get("websocket_url", "wss://app.sahmk.sa/ws/v1/stocks/")
        key   = self.get_sahmk_api_key()
        sep   = "&" if "?" in base else "?"
        if "api_key=" not in base and key:
            return f"{base}{sep}api_key={key}"
        return base

    def is_silent_mode(self) -> bool:
        """Return True if silent/quiet mode is enabled (suppresses non-critical notifications)"""
        return bool(self._config.get("silent_mode", False))

    def enable_silent_mode(self):
        """Enable silent mode at runtime"""
        self._config["silent_mode"] = True

    def disable_silent_mode(self):
        """Disable silent mode at runtime"""
        self._config["silent_mode"] = False

    def is_bot_enabled(self, bot_name: str) -> bool:
        """Return True if a specific bot is enabled in config (defaults to True)"""
        return bool(self.get_bot_config(bot_name).get("enabled", True))

    def get_bot_config(self, bot_name: str) -> dict:
        """Return configuration dict for a specific bot by name"""
        bots_cfg = self._config.get("bots", {})
        return bots_cfg.get(bot_name, {})

    def get_market_config(self) -> dict:
        """Return market settings dict"""
        return self._config.get("market", {})

    def get_filters_config(self) -> dict:
        """Return filters settings dict"""
        return self._config.get("filters", {})

    def is_telegram_enabled(self) -> bool:
        """Return True if Telegram notifications are enabled"""
        return bool(self._config.get("telegram", {}).get("enabled", True))

    def is_strategy_enabled(self, strategy_name: str) -> bool:
        """Return True if a specific strategy is enabled in config (defaults to True)"""
        return bool(self.get_strategy_config(strategy_name).get("enabled", True))

    def get_strategy_config(self, strategy_name: str) -> dict:
        """Return configuration dict for a specific strategy by name"""
        strategies_cfg = self._config.get("strategies", {})
        return strategies_cfg.get(strategy_name, {})

    def set(self, key: str, value: Any) -> None:
        """Set a top-level config value at runtime"""
        self._config[key] = value

    def set_nested(self, value: Any, *keys) -> None:
        """Set a nested config value by dot-path keys"""
        d = self._config
        for k in keys[:-1]:
            d = d.setdefault(k, {})
        d[keys[-1]] = value


# Singleton instance
config = ConfigManager()
