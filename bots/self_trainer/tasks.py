"""Celery tasks for self_trainer (Phase 7 v1)."""
from scripts.celery_app import app
from loguru import logger


@app.task(name="bots.self_trainer.tasks.run_self_trainer", bind=True, max_retries=1)
def run_self_trainer(self, week_id: str = "", recommendations: list = None):
    """
    Standalone self_trainer task.
    Prefer running via weekly_reviewer which passes recommendations.
    """
    try:
        from bots.self_trainer.bot import SelfTrainerBot

        result = SelfTrainerBot().run(
            week_id=week_id or "manual",
            recommendations=recommendations or [],
            has_data=True,
        )
        logger.success(f"self_trainer done: {result.get('status')}")
        return result
    except Exception as exc:
        logger.error(f"run_self_trainer failed: {exc}")
        raise self.retry(exc=exc, countdown=120)
