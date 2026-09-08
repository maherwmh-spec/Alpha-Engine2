"""Celery tasks for watchlist_generator bot (Phase 4)."""
from datetime import date

from scripts.celery_app import app
from loguru import logger
from sqlalchemy import text

from scripts.database import db
from scripts.telegram_send import send_html


def _format_watchlist_message(trade_date, rows) -> str:
    if not rows:
        return (
            f"📋 <b>قائمة المرشحين — {trade_date}</b>\n\n"
            "لا مرشّحين اليوم."
        )
    lines = [f"📋 <b>قائمة الأسهم المرشحة — {trade_date}</b>\n"]
    for r in rows:
        phase = r.get("phase") or "—"
        conf = r.get("phase_confidence")
        conf_s = f" · ثقة {float(conf):.0f}%" if conf is not None else ""
        score = r.get("score")
        try:
            score_s = f"{float(score):.2f}"
        except (TypeError, ValueError):
            score_s = str(score or "—")
        lines.append(
            f"{r['rank']}. <code>{r['symbol']}</code> — {phase}{conf_s} · درجة {score_s}"
        )
    lines.append("\nللتحليل: <code>/analyze SYMBOL</code>")
    return "\n".join(lines)


@app.task(name="bots.watchlist_generator.tasks.run_watchlist_generator", bind=True, max_retries=2)
def run_watchlist_generator(self, send_telegram: bool = True, trade_date: str = None):
    """Build daily watchlist. Telegram via HTTP. Never Fri/Sat."""
    try:
        if date.today().weekday() in (4, 5):
            send_telegram = False
        from bots.watchlist_generator.bot import WatchlistGeneratorBot

        bot = WatchlistGeneratorBot()
        result = bot.run(send_telegram=False, trade_date=trade_date)
        notified = False
        if send_telegram:
            td = result.get("trade_date") or str(date.today())
            rows = []
            try:
                with db.get_session() as session:
                    found = session.execute(
                        text(
                            """
                            SELECT rank, symbol, score, phase, phase_confidence
                            FROM market_data.daily_watchlist
                            WHERE trade_date = :d
                            ORDER BY rank
                            """
                        ),
                        {"d": td},
                    ).fetchall()
                rows = [
                    {
                        "rank": r[0],
                        "symbol": r[1],
                        "score": r[2],
                        "phase": r[3],
                        "phase_confidence": r[4],
                    }
                    for r in found
                ]
            except Exception as exc:
                logger.error(f"load watchlist for notify: {exc}")
            notified = send_html(_format_watchlist_message(td, rows))
            if notified:
                try:
                    with db.get_session() as session:
                        session.execute(
                            text(
                                "UPDATE market_data.daily_watchlist SET notified_at = NOW() WHERE trade_date = :d"
                            ),
                            {"d": td},
                        )
                        session.commit()
                except Exception:
                    pass
        result["notified"] = notified
        logger.success(
            f"watchlist_generator done: count={result.get('count')} "
            f"notified={notified} trade_date={result.get('trade_date')}"
        )
        return result
    except Exception as exc:
        logger.error(f"run_watchlist_generator failed: {exc}")
        raise self.retry(exc=exc, countdown=120)
