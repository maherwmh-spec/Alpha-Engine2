"""Stub tasks for behavioral_analyzer - not yet implemented"""
from scripts.celery_app import app
from loguru import logger


@app.task(name='bots.behavioral_analyzer.tasks.run_behavioral_analyzer', bind=True)
def run_behavioral_analyzer(self):
    """Stub task for behavioral_analyzer"""
    logger.info("behavioral_analyzer task stub - not yet implemented")
    return {"status": "stub", "bot": "behavioral_analyzer"}
