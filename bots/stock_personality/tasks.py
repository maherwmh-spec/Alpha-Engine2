"""Celery tasks for stock_personality bot (Phase 2)."""
from scripts.celery_app import app
from loguru import logger


@app.task(name="bots.stock_personality.tasks.run_stock_personality", bind=True, max_retries=2)
def run_stock_personality(self, symbols=None, max_symbols: int = 0):
    """Nightly stock personality computation (stats + behavioral + phase)."""
    try:
        from bots.stock_personality.bot import StockPersonalityBot
        from scripts.sahmk_official_market import sync_official_market

        try:
            market = sync_official_market()
            logger.info(f"official market snapshot: {market}")
        except Exception as exc:
            logger.warning(f"official market snapshot skipped: {exc}")

        bot = StockPersonalityBot()
        bot.benchmark = "TASI"
        original_fetch = bot._fetch_ohlcv

        def fetch_with_official_tasi(symbol, limit=260):
            df = original_fetch(symbol, limit)
            aliases = {"TASI", "90001", str(bot.benchmark)}
            if df is not None and len(df) >= bot.min_30:
                return df
            if str(symbol) not in aliases:
                return df
            from scripts.tasi_benchmark import fetch_tasi_daily_from_sahmk, persist_tasi_daily

            tasi = fetch_tasi_daily_from_sahmk(lookback_days=bot.max_lookback + 120)
            if tasi is None or len(tasi) < bot.min_30:
                logger.error("Official TASI benchmark unavailable from SahmK")
                return df
            persist_tasi_daily(tasi)
            return tasi.tail(limit).reset_index(drop=True)

        bot._fetch_ohlcv = fetch_with_official_tasi
        result = bot.run(symbols=symbols, max_symbols=max_symbols or None)
        logger.success(
            f"stock_personality done: ok={result.get('ok')} "
            f"insufficient={result.get('insufficient_data')} "
            f"errors={result.get('errors')} total={result.get('total')} "
            f"status={result.get('status')} benchmark={bot.benchmark}"
        )
        return result
    except Exception as exc:
        logger.error(f"run_stock_personality failed: {exc}")
        raise self.retry(exc=exc, countdown=120)
