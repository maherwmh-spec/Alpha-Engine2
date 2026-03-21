"""Celery tasks for strategic_analyzer bot"""
from scripts.celery_app import app
from loguru import logger


@app.task(name='bots.strategic_analyzer.tasks.run_strategic_analyzer', bind=True, max_retries=3)
def run_strategic_analyzer(self):
    """Run strategic analyzer bot task"""
    try:
        from bots.strategic_analyzer.bot import StrategicAnalyzer
        analyzer = StrategicAnalyzer()
        analyzer.run()
    except Exception as exc:
        logger.error(f"strategic_analyzer task failed: {exc}")
        raise self.retry(exc=exc, countdown=60)
