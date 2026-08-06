"""Celery tasks for sentiment_analyzer bot (Phase 3)."""
from scripts.celery_app import app
from loguru import logger


@app.task(name="bots.sentiment_analyzer.tasks.run_sentiment_analyzer", bind=True, max_retries=2)
def run_sentiment_analyzer(self, symbols=None, max_symbols: int = 0):
    """Nightly unified sentiment path (FinBERT when available)."""
    try:
        from bots.sentiment_analyzer.bot import SentimentAnalyzerBot

        bot = SentimentAnalyzerBot()
        result = bot.run(symbols=symbols, max_symbols=max_symbols or None)
        logger.success(
            f"sentiment_analyzer done: ok={result.get('ok')} "
            f"no_news={result.get('no_news')} "
            f"errors={result.get('errors')} total={result.get('total')}"
        )
        return result
    except Exception as exc:
        logger.error(f"run_sentiment_analyzer failed: {exc}")
        raise self.retry(exc=exc, countdown=120)
