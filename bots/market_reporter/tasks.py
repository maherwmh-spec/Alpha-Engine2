"""Celery tasks for market_reporter bot"""
from scripts.celery_app import app
from loguru import logger


@app.task(name='bots.market_reporter.tasks.run_market_reporter', bind=True, max_retries=3)
def run_market_reporter(self):
    """Run market reporter bot task"""
    try:
        from bots.market_reporter.bot import MarketReporter
        reporter = MarketReporter()
        reporter.run()
    except Exception as exc:
        logger.error(f"market_reporter task failed: {exc}")
        raise self.retry(exc=exc, countdown=60)
