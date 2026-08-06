"""Celery tasks for stock_personality bot (Phase 2)."""
from scripts.celery_app import app
from loguru import logger


@app.task(name="bots.stock_personality.tasks.run_stock_personality", bind=True, max_retries=2)
def run_stock_personality(self, symbols=None, max_symbols: int = 0):
    """Nightly stock personality computation (stats + behavioral + phase)."""
    try:
        from bots.stock_personality.bot import StockPersonalityBot

        bot = StockPersonalityBot()
        result = bot.run(symbols=symbols, max_symbols=max_symbols or None)
        logger.success(
            f"stock_personality done: ok={result.get('ok')} "
            f"insufficient={result.get('insufficient_data')} "
            f"errors={result.get('errors')} total={result.get('total')}"
        )
        return result
    except Exception as exc:
        logger.error(f"run_stock_personality failed: {exc}")
        raise self.retry(exc=exc, countdown=120)
