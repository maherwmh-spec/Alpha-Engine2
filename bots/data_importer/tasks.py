"""Celery task wrapper for the DataImporter bot."""
import asyncio

from loguru import logger

from scripts.celery_app import app
from .bot import DataImporter


@app.task(name='bots.data_importer.tasks.run_data_importer', bind=True)
def run_data_importer(self):
    """Run the real DataImporter implementation from Celery."""
    logger.info("Starting data_importer task")
    result = asyncio.run(DataImporter().run())
    logger.info("data_importer task completed with status=%s", result.get("status"))
    return result
