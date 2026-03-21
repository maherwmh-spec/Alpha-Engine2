"""Celery tasks for scientist bot"""
from scripts.celery_app import app
from loguru import logger


@app.task(name='bots.scientist.tasks.run_scientist', bind=True, max_retries=3)
def run_scientist(self):
    """Run scientist bot task"""
    try:
        from bots.scientist.bot import Scientist
        scientist = Scientist()
        scientist.run()
    except Exception as exc:
        logger.error(f"scientist task failed: {exc}")
        raise self.retry(exc=exc, countdown=60)
