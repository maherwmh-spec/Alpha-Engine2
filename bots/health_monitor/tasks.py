"""Stub tasks for health_monitor - not yet implemented"""
from scripts.celery_app import app
from loguru import logger


@app.task(name='bots.health_monitor.tasks.run_health_monitor', bind=True)
def run_health_monitor(self):
    """Stub task for health_monitor"""
    logger.info("health_monitor task stub - not yet implemented")
    return {"status": "stub", "bot": "health_monitor"}
