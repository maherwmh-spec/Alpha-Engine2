"""Stub tasks for freqai_manager - not yet implemented"""
from scripts.celery_app import app
from loguru import logger


@app.task(name='bots.freqai_manager.tasks.run_freqai_manager', bind=True)
def run_freqai_manager(self):
    """Stub task for freqai_manager"""
    logger.info("freqai_manager task stub - not yet implemented")
    return {"status": "stub", "bot": "freqai_manager"}
