"""Celery tasks for strategic_analyzer bot."""
from __future__ import annotations

from loguru import logger

from scripts.celery_app import app
from scripts.database import db, update_bot_status
from scripts.redis_manager import redis_manager


LOCK_KEY = "task_lock:strategic_analyzer"
LOCK_TTL_SECONDS = 900


@app.task(name="bots.strategic_analyzer.tasks.run_strategic_analyzer", bind=True, max_retries=3)
def run_strategic_analyzer(self):
    """Run strategic analyzer bot task with retry, lock, and status telemetry."""
    if redis_manager.exists(LOCK_KEY):
        logger.warning("strategic_analyzer_task_skipped reason=already_running")
        return {"status": "skipped", "reason": "already_running"}

    redis_manager.set(LOCK_KEY, True, ttl=LOCK_TTL_SECONDS)
    try:
        with db.get_session() as session:
            update_bot_status(session, "strategic_analyzer", "running", metadata={"task_id": self.request.id})

        from bots.strategic_analyzer.bot import StrategicAnalyzer

        analyzer = StrategicAnalyzer()
        signals = analyzer.run()
        with db.get_session() as session:
            update_bot_status(session, "strategic_analyzer", "success", metadata={"task_id": self.request.id, "signals": len(signals or [])})
        return {"status": "success", "signals": len(signals or [])}
    except Exception as exc:
        logger.error("strategic_analyzer_task_failed error={}", exc)
        with db.get_session() as session:
            update_bot_status(session, "strategic_analyzer", "failed", error_message=str(exc), metadata={"task_id": self.request.id})
        raise self.retry(exc=exc, countdown=min(60 * (self.request.retries + 1), 300))
    finally:
        redis_manager.delete(LOCK_KEY)
