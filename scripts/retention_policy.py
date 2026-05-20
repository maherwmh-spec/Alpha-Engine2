"""Simple production retention policy tasks.

This module intentionally keeps the policy conservative and configurable. It can
run as a Celery task or as a standalone script. TimescaleDB compression is best
-effort only and never blocks normal cleanup.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict

from loguru import logger
from sqlalchemy import text

from config.config_manager import config
from scripts.celery_app import app
from scripts.database import db
from scripts.redis_manager import redis_manager


DEFAULT_RETENTION_DAYS = {
    "market_data.ohlcv": 730,
    "market_data.technical_indicators": 730,
    "strategies.signals": 180,
    "alerts.notifications": 90,
    "genetic.backtests": 365,
}

TIME_COLUMN = {
    "market_data.ohlcv": "time",
    "market_data.technical_indicators": "time",
    "strategies.signals": "timestamp",
    "alerts.notifications": "timestamp",
    "genetic.backtests": "created_at",
}


def _retention_days(table_name: str) -> int:
    configured = config.get_nested("production", "retention_days", table_name.replace(".", "_"), default=None)
    return int(configured or DEFAULT_RETENTION_DAYS[table_name])


def _delete_old_rows(session, table_name: str, days: int) -> int:
    column = TIME_COLUMN[table_name]
    result = session.execute(text(f"""
        DELETE FROM {table_name}
        WHERE {column} < NOW() - (:days * INTERVAL '1 day')
    """), {"days": days})
    return int(result.rowcount or 0)


def _try_enable_timescale_compression(table_name: str) -> None:
    """Enable Timescale compression where possible; failures are isolated."""
    if not bool(config.get_nested("production", "enable_timescale_compression", default=False)):
        return
    try:
        with db.engine.connect() as conn:
            conn.execute(text("SELECT add_compression_policy(:table_name, INTERVAL '30 days', if_not_exists => TRUE)"), {"table_name": table_name})
            conn.commit()
        logger.info("compression_policy_checked table={}", table_name)
    except Exception as exc:
        logger.debug("compression_policy_skipped table={} reason={}", table_name, exc)


@app.task(name="scripts.retention_policy.run_retention_policy", bind=True, max_retries=2)
def run_retention_policy(self) -> Dict:
    """Apply conservative retention cleanup and record execution summary."""
    summary = {"timestamp": datetime.now(timezone.utc).isoformat(), "tables": {}, "status": "success"}
    try:
        with db.get_session() as session:
            for table_name in DEFAULT_RETENTION_DAYS:
                days = _retention_days(table_name)
                deleted = _delete_old_rows(session, table_name, days)
                summary["tables"][table_name] = {"retention_days": days, "deleted_rows": deleted}
                _try_enable_timescale_compression(table_name)
        redis_manager.set("retention_policy:last_summary", summary, ttl=86400)
        logger.info("retention_policy_complete {}", summary)
        return summary
    except Exception as exc:
        summary["status"] = "failed"
        summary["error"] = str(exc)
        redis_manager.set("retention_policy:last_summary", summary, ttl=86400)
        logger.error("retention_policy_failed error={}", exc)
        raise self.retry(exc=exc, countdown=300)


if __name__ == "__main__":
    print(run_retention_policy())
