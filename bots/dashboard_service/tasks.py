"""Stub tasks for dashboard_service - not yet implemented"""
from scripts.celery_app import app
from loguru import logger


@app.task(name='bots.dashboard_service.tasks.run_dashboard_service', bind=True)
def run_dashboard_service(self):
    """Stub task for dashboard_service"""
    logger.info("dashboard_service task stub - not yet implemented")
    return {"status": "stub", "bot": "dashboard_service"}
