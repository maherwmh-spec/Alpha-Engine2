"""Celery tasks for watchlist_generator bot (Phase 4)."""
from scripts.celery_app import app
from loguru import logger


@app.task(name="bots.watchlist_generator.tasks.run_watchlist_generator", bind=True, max_retries=2)
def run_watchlist_generator(self, send_telegram: bool = True, trade_date: str = None):
    """Build daily watchlist and optionally send morning Telegram message."""
    try:
        from bots.watchlist_generator.bot import WatchlistGeneratorBot

        bot = WatchlistGeneratorBot()
        result = bot.run(send_telegram=send_telegram, trade_date=trade_date)
        logger.success(
            f"watchlist_generator done: count={result.get('count')} "
            f"notified={result.get('notified')} trade_date={result.get('trade_date')}"
        )
        return result
    except Exception as exc:
        logger.error(f"run_watchlist_generator failed: {exc}")
        raise self.retry(exc=exc, countdown=120)
