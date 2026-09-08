"""Celery tasks for weekly_reviewer (Phase 7)."""
from scripts.celery_app import app
from loguru import logger


@app.task(name="bots.weekly_reviewer.tasks.run_weekly_reviewer", bind=True, max_retries=2)
def run_weekly_reviewer(self, send_telegram: bool = True, force: bool = True):
    """Friday schedule always notifies; skip-flag no longer blocks the report."""
    try:
        from bots.weekly_reviewer.bot import WeeklyReviewerBot

        result = WeeklyReviewerBot().run(send_telegram=send_telegram, force=force)
        logger.success(
            f"weekly_reviewer done: week={result.get('week_id')} "
            f"status={result.get('status')} notified={result.get('notified')}"
        )
        return result
    except Exception as exc:
        logger.error(f"run_weekly_reviewer failed: {exc}")
        raise self.retry(exc=exc, countdown=300)
