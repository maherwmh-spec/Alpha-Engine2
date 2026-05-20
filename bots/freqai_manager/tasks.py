"""Celery tasks for freqai_manager bot."""
from __future__ import annotations

from loguru import logger

from scripts.celery_app import app
from scripts.database import db, update_bot_status
from scripts.redis_manager import redis_manager


LOCK_KEY = "task_lock:freqai_manager"
LOCK_TTL_SECONDS = 3600


@app.task(name="bots.freqai_manager.tasks.run_freqai_manager", bind=True, max_retries=3)
def run_freqai_manager(self):
    """Run freqai_manager bot task with overlap protection and status telemetry."""
    if redis_manager.exists(LOCK_KEY):
        logger.warning("freqai_manager_task_skipped reason=already_running")
        return {"status": "skipped", "reason": "already_running"}

    redis_manager.set(LOCK_KEY, True, ttl=LOCK_TTL_SECONDS)
    try:
        with db.get_session() as session:
            update_bot_status(session, "freqai_manager", "running", metadata={"task_id": self.request.id})

        from bots.freqai_manager.bot import FreqAIManager

        manager = FreqAIManager()
        result = manager.run()
        with db.get_session() as session:
            update_bot_status(session, "freqai_manager", "success", metadata={"task_id": self.request.id, "result_count": len(result or [])})
        return {"status": "success", "result_count": len(result or [])}
    except Exception as exc:
        logger.error("freqai_manager_task_failed error={}", exc)
        with db.get_session() as session:
            update_bot_status(session, "freqai_manager", "failed", error_message=str(exc), metadata={"task_id": self.request.id})
        raise self.retry(exc=exc, countdown=min(60 * (self.request.retries + 1), 300))
    finally:
        redis_manager.delete(LOCK_KEY)
