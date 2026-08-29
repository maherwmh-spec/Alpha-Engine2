"""
Alpha-Engine2 Configuration Manager.

Secrets policy (2026-08):
  * Sensitive values are read from environment variables first (.env / container env).
  * config/config.yaml holds non-sensitive settings and placeholders only.
  * Supports both DB_* and POSTGRES_* names for compatibility.
  * Passwords with special characters are URL-encoded when building DSNs.
  * Logs never print secret values.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote_plus

import yaml
from loguru import logger

try:
    from dotenv import load_dotenv

    _env_path = Path(__file__).resolve().parent.parent / ".env"
    load_dotenv(_env_path, override=False)
except ImportError:
    pass

_SECRET_ENV_ALIASES = {
    "DB_PASSWORD": ("DB_PASSWORD", "POSTGRES_PASSWORD"),
    "DB_USER": ("DB_USER", "POSTGRES_USER"),
    "DB_NAME": ("DB_NAME", "POSTGRES_DB"),
    "REDIS_PASSWORD": ("REDIS_PASSWORD",),
    "TELEGRAM_BOT_TOKEN": ("TELEGRAM_BOT_TOKEN",),
    "TELEGRAM_CHAT_ID": ("TELEGRAM_CHAT_ID",),
    "SAHMK_API_KEY": ("SAHMK_API_KEY",),
    "ADMIN_API_KEY": ("ADMIN_API_KEY",),
}

_SECRET_CONFIG_PATHS = {
    "DB_PASSWORD": ("database", "password"),
    "REDIS_PASSWORD": ("redis", "password"),
    "TELEGRAM_BOT_TOKEN": ("telegram", "bot_token"),
    "TELEGRAM_CHAT_ID": ("telegram", "chat_id"),
    "SAHMK_API_KEY": ("sahmk", "api_key"),
    "ADMIN_API_KEY": ("admin", "api_key"),
}

_PLACEHOLDER_VALUES = {
    "",
    "changeme",
    "change_me",
    "placeholder",
    "replace_me",
    "example",
    "your_token_here",
    "your_key_here",
    "your_value_here",
    "your_sahmk_api_key_here",
    "your_actual_api_key_here",
    "change_me_strong_db_password",
    "change_me_strong_redis_password",
    "change_me_telegram_bot_token",
    "change_me_telegram_chat_id",
    "change_me_admin_api_key",
    "change_me_sahmk_api_key",
    "alpha_password_2024",
    "alpha_redis_password_2024",
}

DEFAULT_SAHMK_BASE_URL = "https://api.sahmk.sa/api/v1"
DEFAULT_SAHMK_WS_URL = "wss://api.sahmk.sa/ws/v1/stocks/"


def _is_placeholder(value: Optional[str]) -> bool:
    if value is None:
        return True
    return str(value).strip().lower() in _PLACEHOLDER_VALUES


class ConfigManager:
    """Singleton configuration manager."""

    _instance = None
    _config: dict = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load()
        return cls._instance

    def _load(self):
        config_path = Path(__file__).parent / "config.yaml"
        logger.info("Loading configuration from: {}", config_path)
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                self._config = yaml.safe_load(f) or {}
            logger.success("Configuration loaded successfully")
        except FileNotFoundError:
            logger.error("config.yaml not found at {}", config_path)
            self._config = {}
        except yaml.YAMLError as e:
            logger.error("Error parsing config.yaml: {}", e)
            self._config = {}

    def get(self, key: str, default: Any = None) -> Any:
        if "." not in key:
            return self._config.get(key, default)
        return self.get_nested(*key.split("."), default=default)

    def get_nested(self, *keys, default: Any = None) -> Any:
        val: Any = self._config
        for k in keys:
            if not isinstance(val, dict):
                return default
            val = val.get(k, default)
        return val

    def _env(self, name: str, default: Optional[str] = None) -> str:
        value = os.getenv(name)
        if value is not None and str(value).strip() != "":
            return str(value).strip()
        return "" if default is None else str(default)

    def _env_first(self, *names: str, default: Optional[str] = None) -> str:
        for name in names:
            value = os.getenv(name)
            if value is not None and str(value).strip() != "" and not _is_placeholder(value):
                return str(value).strip()
        return "" if default is None else str(default)

    def _secret(self, name: str, default: Optional[str] = None) -> str:
        aliases = _SECRET_ENV_ALIASES.get(name, (name,))
        env_value = self._env_first(*aliases)
        if env_value and not _is_placeholder(env_value):
            return env_value

        path = _SECRET_CONFIG_PATHS.get(name)
        if path:
            yaml_value = self.get_nested(*path, default=None)
            if yaml_value is not None and not _is_placeholder(str(yaml_value)):
                return str(yaml_value).strip()

        return "" if default is None else str(default)

    def _required_secret(self, name: str) -> str:
        value = self._secret(name)
        if _is_placeholder(value):
            aliases = " / ".join(_SECRET_ENV_ALIASES.get(name, (name,)))
            raise ValueError(
                f"Required secret {name} is missing. Set it in .env ({aliases}). "
                f"Do not store real secrets in config.yaml."
            )
        return value

    def is_production(self) -> bool:
        return os.getenv("APP_ENV", os.getenv("ENVIRONMENT", "development")).strip().lower() in {
            "prod",
            "production",
        }

    def validate_startup_secrets(self) -> None:
        required = ["DB_PASSWORD", "REDIS_PASSWORD"]
        if self.is_telegram_enabled():
            required.extend(["TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"])
        if self.get("sahmk", {}).get("enabled", True):
            required.append("SAHMK_API_KEY")
        missing = [name for name in required if _is_placeholder(self._secret(name))]
        if missing:
            raise ValueError(
                "Startup blocked; missing secrets in environment (.env): " + ", ".join(sorted(set(missing)))
            )

    def _db_components(self) -> tuple:
        db = self._config.get("database", {})
        host = self._env_first("DB_HOST", default=str(db.get("host", "postgres")))
        port = self._env_first("DB_PORT", default=str(db.get("port", 5432)))
        name = self._env_first("DB_NAME", "POSTGRES_DB", default=str(db.get("name", "alpha_engine")))
        user = self._env_first("DB_USER", "POSTGRES_USER", default=str(db.get("user", "alpha_user")))
        password = self._required_secret("DB_PASSWORD")
        return host, port, name, user, password

    def get_database_url(self) -> str:
        """Build SQLAlchemy URL from components with URL-encoded password."""
        host, port, name, user, password = self._db_components()
        user_q = quote_plus(user)
        pass_q = quote_plus(password)
        return f"postgresql+psycopg2://{user_q}:{pass_q}@{host}:{port}/{name}"

    def get_asyncpg_dsn(self) -> str:
        host, port, name, user, password = self._db_components()
        user_q = quote_plus(user)
        pass_q = quote_plus(password)
        return f"postgresql://{user_q}:{pass_q}@{host}:{port}/{name}"

    def get_redis_url(self, db_index: int = 0) -> str:
        host = self._env_first("REDIS_HOST", default=str(self.get_nested("redis", "host", default="redis")))
        port = self._env_first("REDIS_PORT", default=str(self.get_nested("redis", "port", default=6379)))
        password = self._required_secret("REDIS_PASSWORD")
        db = self._env_first("REDIS_DB", default=str(db_index))
        try:
            db_int = int(db)
        except ValueError:
            db_int = db_index
        pass_q = quote_plus(password)
        logger.debug("[Redis] Connecting to redis://:***@{}:{}/{}", host, port, db_int)
        return f"redis://:{pass_q}@{host}:{port}/{db_int}"

    def get_redis_url_for_backend(self) -> str:
        return self.get_redis_url(db_index=1)

    def get_redis_connection_params(self, db_index: int = 0) -> dict:
        host = self._env_first("REDIS_HOST", default=str(self.get_nested("redis", "host", default="redis")))
        port = int(self._env_first("REDIS_PORT", default=str(self.get_nested("redis", "port", default=6379))))
        configured_db = self._env_first("REDIS_DB", default=str(db_index))
        try:
            db = int(configured_db)
        except ValueError:
            db = db_index
        return {
            "host": host,
            "port": port,
            "db": db,
            "password": self._required_secret("REDIS_PASSWORD"),
        }

    def get_telegram_token(self) -> str:
        return self._secret("TELEGRAM_BOT_TOKEN")

    def get_telegram_chat_id(self) -> str:
        return self._secret("TELEGRAM_CHAT_ID")

    def get_sahmk_api_key(self) -> str:
        return self._secret("SAHMK_API_KEY")

    def get_admin_api_key(self) -> str:
        return self._secret("ADMIN_API_KEY")

    def get_sahmk_ws_url(self) -> str:
        sahmk = self._config.get("sahmk", {})
        base = self._env_first(
            "SAHMK_WEBSOCKET_URL",
            default=str(sahmk.get("websocket_url", DEFAULT_SAHMK_WS_URL)),
        )
        key = self.get_sahmk_api_key()
        sep = "&" if "?" in base else "?"
        if "api_key=" not in base and key:
            return f"{base}{sep}api_key={key}"
        return base

    def get_sahmk_base_url(self) -> str:
        sahmk = self._config.get("sahmk", {})
        return self._env_first(
            "SAHMK_BASE_URL",
            default=str(sahmk.get("base_url", DEFAULT_SAHMK_BASE_URL)),
        )

    def is_silent_mode(self) -> bool:
        return bool(self._config.get("silent_mode", False))

    def enable_silent_mode(self):
        self._config["silent_mode"] = True

    def disable_silent_mode(self):
        self._config["silent_mode"] = False

    def is_bot_enabled(self, bot_name: str) -> bool:
        return bool(self.get_bot_config(bot_name).get("enabled", True))

    def get_bot_config(self, bot_name: str) -> dict:
        return self._config.get("bots", {}).get(bot_name, {})

    def get_market_config(self) -> dict:
        return self._config.get("market", {})

    def get_filters_config(self) -> dict:
        return self._config.get("filters", {})

    def is_telegram_enabled(self) -> bool:
        enabled = bool(self._config.get("telegram", {}).get("enabled", True))
        override = os.getenv("TELEGRAM_ENABLED", "").strip().lower()
        if override in {"0", "false", "no", "off"}:
            return False
        if override in {"1", "true", "yes", "on"}:
            return True
        return enabled

    def is_strategy_enabled(self, strategy_name: str) -> bool:
        return bool(self.get_strategy_config(strategy_name).get("enabled", True))

    def get_strategy_config(self, strategy_name: str) -> dict:
        return self._config.get("strategies", {}).get(strategy_name, {})

    def set(self, key: str, value: Any) -> None:
        self._config[key] = value

    def set_nested(self, value: Any, *keys) -> None:
        d = self._config
        for k in keys[:-1]:
            d = d.setdefault(k, {})
        d[keys[-1]] = value


def rebase_redis_db(url: str, db_index: int) -> str:
    base = url.rsplit("/", 1)[0]
    return f"{base}/{db_index}"


config = ConfigManager()
