"""Stub tasks for self_trainer - not yet implemented"""
from scripts.celery_app import app
from loguru import logger


@app.task(name='bots.self_trainer.tasks.run_self_trainer', bind=True)
def run_self_trainer(self):
    """Stub task for self_trainer"""
    logger.info("self_trainer task stub - not yet implemented")
    return {"status": "stub", "bot": "self_trainer"}
