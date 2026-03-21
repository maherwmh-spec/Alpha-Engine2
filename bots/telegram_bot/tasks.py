"""Stub tasks for telegram_bot - not yet implemented"""
from scripts.celery_app import app
from loguru import logger


@app.task(name='bots.telegram_bot.tasks.run_telegram_bot', bind=True)
def run_telegram_bot(self):
    """Stub task for telegram_bot"""
    logger.info("telegram_bot task stub - not yet implemented")
    return {"status": "stub", "bot": "telegram_bot"}
