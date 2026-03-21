"""Tasks for health_monitor bot"""
from scripts.celery_app import app
from loguru import logger


@app.task(name='bots.health_monitor.tasks.run_health_monitor', bind=True)
def run_health_monitor(self):
    """Health monitor task stub"""
    logger.info("health_monitor task stub - not yet implemented")
    return {"status": "stub", "bot": "health_monitor"}


@app.task(name='bots.health_monitor.tasks.send_daily_report', bind=True)
def send_daily_report(self):
    """Send daily report task stub"""
    logger.info("send_daily_report task stub - not yet implemented")
    return {"status": "stub", "task": "send_daily_report"}
