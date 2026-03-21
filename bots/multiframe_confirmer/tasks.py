"""Stub tasks for multiframe_confirmer - not yet implemented"""
from scripts.celery_app import app
from loguru import logger


@app.task(name='bots.multiframe_confirmer.tasks.run_multiframe_confirmer', bind=True)
def run_multiframe_confirmer(self):
    """Stub task for multiframe_confirmer"""
    logger.info("multiframe_confirmer task stub - not yet implemented")
    return {"status": "stub", "bot": "multiframe_confirmer"}
