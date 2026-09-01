"""
Phase 5 Telegram command handlers.
Registered from AlphaTelegramBot.setup_handlers via register_phase5_handlers().
"""
from __future__ import annotations

from telegram import Update
from telegram.ext import ContextTypes, CommandHandler
from loguru import logger
from sqlalchemy import text

from scripts.analyze_service import AnalyzeService
from scripts.database import db


def _svc() -> AnalyzeService:
    return AnalyzeService()


def _conf_s(row: dict) -> str:
    conf = row.get("phase_confidence")
    if conf is None:
        return ""
    try:
        return f" · ثقة {float(conf):.0f}%"
    except (TypeError, ValueError):
        return ""


def _attach_phase_confidence(rows: list) -> list:
    if not rows:
        return rows
    symbols = [str(r.get("symbol") or "") for r in rows if r.get("symbol")]
    if not symbols:
        return rows
    try:
        with db.get_session() as session:
            found = session.execute(
                text(
                    """
                    SELECT symbol, phase_confidence
                    FROM market_data.stock_personalities
                    WHERE symbol IN :syms
                    """
                ).bindparams(syms=tuple(symbols)),
            ).fetchall()
        conf_map = {r[0]: r[1] for r in found}
        for row in rows:
            if row.get("phase_confidence") is None:
                row["phase_confidence"] = conf_map.get(row.get("symbol"))
    except Exception as exc:
        logger.debug(f"watchlist confidence attach failed: {exc}")
    return rows


async def cmd_analyze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(
            "الاستخدام: <code>/analyze 1120</code>",
            parse_mode="HTML",
        )
        return
    symbol = context.args[0].strip().upper()
    try:
        data = _svc().analyze(symbol, add_watch=True)
        await update.message.reply_text(data["html"], parse_mode="HTML")
    except Exception as exc:
        logger.error(f"/analyze failed: {exc}")
        await update.message.reply_text(f"❌ فشل التحليل: <code>{exc}</code>", parse_mode="HTML")


async def cmd_watchlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        rows = _attach_phase_confidence(_svc().list_watchlist_today())
        if not rows:
            await update.message.reply_text("لا توجد قائمة مرشّحين لليوم.")
            return
        lines = ["📋 <b>قائمة اليوم</b>\n"]
        for r in rows:
            score = r.get("score")
            try:
                score_s = f"{float(score):.2f}" if score is not None else "—"
            except (TypeError, ValueError):
                score_s = str(score)
            lines.append(
                f"{r['rank']}. <code>{r['symbol']}</code> — {r.get('phase') or '—'}"
                f"{_conf_s(r)} · درجة {score_s}"
            )
        lines.append("\n<code>/analyze SYMBOL</code>")
        await update.message.reply_text("\n".join(lines), parse_mode="HTML")
    except Exception as exc:
        await update.message.reply_text(f"❌ {exc}")


async def cmd_watching(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        rows = _svc().list_watching()
        if not rows:
            await update.message.reply_text("لا أسهم تحت المراقبة النشطة.")
            return
        lines = ["👁 <b>المراقبة النشطة</b>\n"]
        for r in rows:
            lines.append(
                f"• <code>{r['symbol']}</code> — {r.get('phase') or '—'} · {r.get('source')}"
            )
        await update.message.reply_text("\n".join(lines), parse_mode="HTML")
    except Exception as exc:
        await update.message.reply_text(f"❌ {exc}")


async def cmd_unwatch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("الاستخدام: <code>/unwatch 1120</code>", parse_mode="HTML")
        return
    symbol = context.args[0].strip().upper()
    ok = _svc().unwatch(symbol)
    if ok:
        await update.message.reply_text(f"تم إيقاف مراقبة <code>{symbol}</code>", parse_mode="HTML")
    else:
        await update.message.reply_text(f"{symbol} ليس تحت مراقبة نشطة.")


async def cmd_paper(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("الاستخدام: <code>/paper 1120</code>", parse_mode="HTML")
        return
    symbol = context.args[0].strip().upper()
    result = _svc().open_paper(symbol, source="telegram")
    if not result.get("ok"):
        await update.message.reply_text(f"❌ {result.get('error')}")
        return
    await update.message.reply_text(
        f"📄 <b>صفقة ورقية مفتوحة</b>\n"
        f"الرمز: <code>{result['symbol']}</code>\n"
        f"دخول: {result['entry']:.4f}\n"
        f"وقف: {result['stop_loss']:.4f}\n"
        f"هدف: {result['take_profit']:.4f}\n"
        f"id={result.get('id')}",
        parse_mode="HTML",
    )


async def cmd_paper_close(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("الاستخدام: <code>/paper_close 1120</code>", parse_mode="HTML")
        return
    symbol = context.args[0].strip().upper()
    result = _svc().close_paper(symbol)
    if not result.get("ok"):
        await update.message.reply_text(f"❌ {result.get('error')}")
        return
    await update.message.reply_text(
        f"✅ أُغلقت ورقياً {symbol}\n"
        f"خروج: {result.get('exit')} · PnL: {result.get('pnl_pct')}%",
        parse_mode="HTML",
    )


async def cmd_papers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = _svc().list_open_papers()
    if not rows:
        await update.message.reply_text("لا صفقات ورقية مفتوحة.")
        return
    lines = ["📄 <b>الصفقات الورقية المفتوحة</b>\n"]
    for r in rows:
        lines.append(
            f"• <code>{r['symbol']}</code> دخول {r['entry']} | SL {r['sl']} | TP {r['tp']}"
        )
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


def register_phase5_handlers(application) -> None:
    application.add_handler(CommandHandler("analyze", cmd_analyze))
    application.add_handler(CommandHandler("watchlist", cmd_watchlist))
    application.add_handler(CommandHandler("watching", cmd_watching))
    application.add_handler(CommandHandler("unwatch", cmd_unwatch))
    application.add_handler(CommandHandler("paper", cmd_paper))
    application.add_handler(CommandHandler("paper_close", cmd_paper_close))
    application.add_handler(CommandHandler("papers", cmd_papers))
    logger.info("Phase 5 Telegram handlers registered")
