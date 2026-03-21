"""Stub tasks for data_importer - not yet implemented"""
from scripts.celery_app import app
from loguru import logger


@app.task(name='bots.data_importer.tasks.run_data_importer', bind=True)
def run_data_importer(self):
    """Stub task for data_importer"""
    logger.info("data_importer task stub - not yet implemented")
    return {"status": "stub", "bot": "data_importer"}
