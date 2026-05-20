"""Tasks for health_monitor bot."""
from __future__ import annotations

from datetime import datetime, timezone

from loguru import logger
from sqlalchemy import text

from scripts.celery_app import app
from scripts.database import db, update_bot_status
from scripts.redis_manager import redis_manager


CRITICAL_BOTS = [
    "market_reporter",
    "strategic_analyzer",
    "monitor",
    "technical_miner",
    "risk_guardian",
    "freqai_manager",
]


@app.task(name="bots.health_monitor.tasks.run_health_monitor", bind=True, max_retries=2)
def run_health_monitor(self):
    """Run advanced health checks for critical production components."""
    checks = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "postgres": False,
        "redis": False,
        "signals_recent_24h": 0,
        "critical_bots": {},
        "status": "unknown",
    }
    try:
        with db.get_session() as session:
            session.execute(text("SELECT 1"))
            checks["postgres"] = True
            recent_signals = session.execute(text("""
                SELECT COUNT(*)
                FROM strategies.signals
                WHERE timestamp >= NOW() - INTERVAL '24 hours'
            """)).scalar()
            checks["signals_recent_24h"] = int(recent_signals or 0)
            rows = session.execute(text("""
                SELECT bot_name, status, last_run, error_message
                FROM bots.status
                WHERE bot_name = ANY(:bot_names)
            """), {"bot_names": CRITICAL_BOTS}).fetchall()
            for row in rows:
                checks["critical_bots"][row[0]] = {
                    "status": row[1],
                    "last_run": row[2].isoformat() if row[2] else None,
                    "error_message": row[3],
                }

        redis_manager.set("health_monitor:ping", "ok", ttl=120)
        checks["redis"] = redis_manager.exists("health_monitor:ping")

        missing = [bot for bot in CRITICAL_BOTS if bot not in checks["critical_bots"]]
        failed = [bot for bot, info in checks["critical_bots"].items() if info.get("status") == "failed"]
        checks["missing_bot_status"] = missing
        checks["failed_bots"] = failed
        checks["status"] = "healthy" if checks["postgres"] and checks["redis"] and not failed else "degraded"
        redis_manager.set("health_monitor:last_report", checks, ttl=7200)

        with db.get_session() as session:
            update_bot_status(session, "health_monitor", checks["status"], metadata=checks)
        logger.info("health_monitor_complete status={} recent_signals={} failed_bots={} missing_status={}", checks["status"], checks["signals_recent_24h"], failed, missing)
        return checks
    except Exception as exc:
        logger.error("health_monitor_failed error={}", exc)
        with db.get_session() as session:
            update_bot_status(session, "health_monitor", "failed", error_message=str(exc), metadata=checks)
        raise self.retry(exc=exc, countdown=120)


@app.task(name="bots.health_monitor.tasks.send_daily_report", bind=True)
def send_daily_report(self):
    """Persist/return the latest health report for external alerting integrations."""
    report = redis_manager.get("health_monitor:last_report") or {"status": "unknown", "timestamp": datetime.now(timezone.utc).isoformat()}
    logger.info("health_monitor_daily_report {}", report)
    return {"status": "success", "report": report}
