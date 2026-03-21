"""
Alpha-Engine2 Configuration Manager
Loads config.yaml and provides typed access to all settings.
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

    def get_redis_url(self) -> str:
        """Return Redis URL"""
        r = self._config.get("redis", {})
        host     = os.getenv("REDIS_HOST",     r.get("host",     "redis"))
        port     = os.getenv("REDIS_PORT",     str(r.get("port", 6379)))
        db       = os.getenv("REDIS_DB",       str(r.get("db",   0)))
        password = os.getenv("REDIS_PASSWORD", r.get("password", ""))
        if password:
            return f"redis://:{password}@{host}:{port}/{db}"
        return f"redis://{host}:{port}/{db}"

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


# Singleton instance
config = ConfigManager()
