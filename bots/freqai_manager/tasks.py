"""Celery tasks for freqai_manager bot"""
from scripts.celery_app import app
from loguru import logger

@app.task(name='bots.freqai_manager.tasks.run_freqai_manager', bind=True, max_retries=3)
def run_freqai_manager(self):
    """Run freqai_manager bot task"""
    try:
        from bots.freqai_manager.bot import FreqAIManager
        manager = FreqAIManager()
        manager.run()
    except Exception as exc:
        logger.error(f"freqai_manager task failed: {exc}")
        raise self.retry(exc=exc, countdown=60)
