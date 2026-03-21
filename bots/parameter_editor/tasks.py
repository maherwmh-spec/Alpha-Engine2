"""Stub tasks for parameter_editor - not yet implemented"""
from scripts.celery_app import app
from loguru import logger


@app.task(name='bots.parameter_editor.tasks.run_parameter_editor', bind=True)
def run_parameter_editor(self):
    """Stub task for parameter_editor"""
    logger.info("parameter_editor task stub - not yet implemented")
    return {"status": "stub", "bot": "parameter_editor"}
