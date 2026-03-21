"""Stub tasks for weekly_reviewer - not yet implemented"""
from scripts.celery_app import app
from loguru import logger


@app.task(name='bots.weekly_reviewer.tasks.run_weekly_reviewer', bind=True)
def run_weekly_reviewer(self):
    """Stub task for weekly_reviewer"""
    logger.info("weekly_reviewer task stub - not yet implemented")
    return {"status": "stub", "bot": "weekly_reviewer"}
