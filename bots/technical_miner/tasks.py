"""Celery tasks for technical_miner bot"""
from scripts.celery_app import app
from loguru import logger


@app.task(name='bots.technical_miner.tasks.run_technical_miner', bind=True, max_retries=3)
def run_technical_miner(self):
    """Run technical miner bot task"""
    try:
        from bots.technical_miner.bot import TechnicalMiner
        miner = TechnicalMiner()
        miner.run()
    except Exception as exc:
        logger.error(f"technical_miner task failed: {exc}")
        raise self.retry(exc=exc, countdown=60)
