"""
Phase 5 Telegram command handlers.
Registered from AlphaTelegramBot.setup_handlers via register_phase5_handlers().
"""
from __future__ import annotations

from telegram import Update
from telegram.ext import ContextTypes, CommandHandler
from loguru import logger

from scripts.analyze_service import AnalyzeService


def _svc() -> AnalyzeService:
    return AnalyzeService()


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
        rows = _svc().list_watchlist_today()
        if not rows:
            await update.message.reply_text("لا توجد قائمة مرشّحين لليوم.")
            return
        lines = ["📋 <b>قائمة اليوم</b>\n"]
        for r in rows:
            lines.append(
                f"{r['rank']}. <code>{r['symbol']}</code> — {r.get('phase') or '—'} · {r.get('score')}"
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
