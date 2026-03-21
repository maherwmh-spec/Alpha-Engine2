"""Celery tasks for consolidation_hunter bot"""
from scripts.celery_app import app
from loguru import logger


@app.task(name='bots.consolidation_hunter.tasks.run_consolidation_hunter', bind=True, max_retries=3)
def run_consolidation_hunter(self):
    """Run consolidation hunter bot task"""
    try:
        from bots.consolidation_hunter.bot import ConsolidationHunter
        hunter = ConsolidationHunter()
        hunter.run()
    except Exception as exc:
        logger.error(f"consolidation_hunter task failed: {exc}")
        raise self.retry(exc=exc, countdown=60)
