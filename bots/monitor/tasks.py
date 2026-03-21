"""Celery tasks for monitor bot"""
from scripts.celery_app import app
from loguru import logger


@app.task(name='bots.monitor.tasks.run_monitor', bind=True, max_retries=3)
def run_monitor(self):
    """Run monitor bot task"""
    try:
        from bots.monitor.bot import Monitor
        monitor = Monitor()
        monitor.run()
    except Exception as exc:
        logger.error(f"monitor task failed: {exc}")
        raise self.retry(exc=exc, countdown=60)
