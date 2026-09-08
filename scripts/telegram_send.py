"""Synchronous Telegram send. Safe inside Celery workers."""
from __future__ import annotations

import requests
from loguru import logger

from config.config_manager import config


def send_html(text: str) -> bool:
    token = (config.get_telegram_token() or "").strip()
    chat = str(config.get_telegram_chat_id() or "").strip()
    if not token or not chat:
        logger.error("telegram_send: token or chat_id missing")
        return False
    if config.is_silent_mode():
        logger.info("telegram_send: silent mode, skipped")
        return False
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={
                "chat_id": chat,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=20,
        )
        if resp.status_code != 200:
            logger.error(f"telegram_send HTTP {resp.status_code}: {resp.text[:240]}")
            return False
        logger.success("telegram_send: delivered")
        return True
    except Exception as exc:
        logger.error(f"telegram_send failed: {exc}")
        return False
