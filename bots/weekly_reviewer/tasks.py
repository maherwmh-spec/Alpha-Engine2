"""Celery tasks for weekly_reviewer (Phase 7)."""
from scripts.celery_app import app
from loguru import logger
from sqlalchemy import text

from scripts.database import db
from scripts.telegram_send import send_html


@app.task(name="bots.weekly_reviewer.tasks.run_weekly_reviewer", bind=True, max_retries=2)
def run_weekly_reviewer(self, send_telegram: bool = True, force: bool = True):
    """Build weekly report and send via HTTP (no asyncio in the worker)."""
    try:
        from bots.weekly_reviewer.bot import WeeklyReviewerBot

        result = WeeklyReviewerBot().run(send_telegram=False, force=force)
        notified = False
        if send_telegram:
            week_id = result.get("week_id")
            report = None
            try:
                with db.get_session() as session:
                    row = session.execute(
                        text(
                            "SELECT report_text FROM analytics.weekly_reviews WHERE week_id = :w"
                        ),
                        {"w": week_id},
                    ).fetchone()
                report = row[0] if row else None
            except Exception as exc:
                logger.error(f"load weekly report_text: {exc}")
            if report:
                notified = send_html(report)
                if notified:
                    try:
                        with db.get_session() as session:
                            session.execute(
                                text(
                                    "UPDATE analytics.weekly_reviews SET notified = TRUE WHERE week_id = :w"
                                ),
                                {"w": week_id},
                            )
                            session.commit()
                    except Exception:
                        pass
        result["notified"] = notified
        logger.success(
            f"weekly_reviewer done: week={result.get('week_id')} "
            f"status={result.get('status')} notified={notified}"
        )
        return result
    except Exception as exc:
        logger.error(f"run_weekly_reviewer failed: {exc}")
        raise self.retry(exc=exc, countdown=300)
