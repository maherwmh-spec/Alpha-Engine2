"""Stub tasks for silent_mode_manager - not yet implemented"""
from scripts.celery_app import app
from loguru import logger


@app.task(name='bots.silent_mode_manager.tasks.run_silent_mode_manager', bind=True)
def run_silent_mode_manager(self):
    """Stub task for silent_mode_manager"""
    logger.info("silent_mode_manager task stub - not yet implemented")
    return {"status": "stub", "bot": "silent_mode_manager"}
