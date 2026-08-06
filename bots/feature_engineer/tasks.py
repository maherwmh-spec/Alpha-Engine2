"""Celery tasks for feature_engineer bot (Phase 3)."""
from scripts.celery_app import app
from loguru import logger


@app.task(name="bots.feature_engineer.tasks.run_feature_engineer", bind=True, max_retries=2)
def run_feature_engineer(self, symbols=None, max_symbols: int = 0):
    """Nightly fixed feature engineering from OHLCV."""
    try:
        from bots.feature_engineer.bot import FeatureEngineerBot

        bot = FeatureEngineerBot()
        result = bot.run(symbols=symbols, max_symbols=max_symbols or None)
        logger.success(
            f"feature_engineer done: ok={result.get('ok')} "
            f"insufficient={result.get('insufficient_data')} "
            f"errors={result.get('errors')} total={result.get('total')}"
        )
        return result
    except Exception as exc:
        logger.error(f"run_feature_engineer failed: {exc}")
        raise self.retry(exc=exc, countdown=120)
