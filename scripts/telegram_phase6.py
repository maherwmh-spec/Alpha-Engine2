"""
Phase 6 Telegram handlers: /strategy /update_strategy /strategies /params
"""
from __future__ import annotations

from telegram import Update
from telegram.ext import ContextTypes, CommandHandler
from loguru import logger

from scripts.parameter_editor import ParameterEditor, PARAM_SPEC


def _ed() -> ParameterEditor:
    return ParameterEditor()


def _fmt_params(p: dict) -> str:
    lines = []
    for k in sorted(p.keys()):
        v = p[k]
        if k.endswith("_pct") and isinstance(v, (int, float)):
            lines.append(f"• <code>{k}</code> = {float(v)*100:.2f}%")
        else:
            lines.append(f"• <code>{k}</code> = {v}")
    return "\n".join(lines)


async def cmd_strategy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("الاستخدام: <code>/strategy 1120</code>", parse_mode="HTML")
        return
    symbol = context.args[0].strip().upper()
    try:
        ed = _ed()
        eff = ed.effective_params(symbol)
        ov = ed.list_symbol_overrides(symbol)
        msg = (
            f"⚙️ <b>معاملات {symbol}</b>\n\n"
            f"<b>الفعّالة:</b>\n{_fmt_params(eff)}\n\n"
            f"<b>Overrides:</b> {', '.join(ov.keys()) if ov else 'لا يوجد'}\n\n"
            f"تعديل: <code>/update_strategy {symbol} stop_loss_pct=0.02</code>"
        )
        await update.message.reply_text(msg, parse_mode="HTML")
    except Exception as exc:
        logger.error(f"/strategy failed: {exc}")
        await update.message.reply_text(f"❌ {exc}")


async def cmd_update_strategy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(
            "الاستخدام:\n"
            "<code>/update_strategy 1120 stop_loss_pct=0.02 take_profit_pct=0.05</code>\n"
            "<code>/update_strategy 1120 reset</code>",
            parse_mode="HTML",
        )
        return
    ed = _ed()
    symbol, updates, special = ed.parse_kv_args(list(context.args))
    if not symbol:
        await update.message.reply_text("الرمز مطلوب.")
        return
    try:
        if special == "reset":
            result = ed.reset(symbol, source="telegram")
            await update.message.reply_text(
                f"✅ أُعيدت معاملات <code>{symbol}</code> للافتراضي\n\n"
                f"{_fmt_params(result['effective'])}",
                parse_mode="HTML",
            )
            return
        if not updates:
            await update.message.reply_text(
                "لم يُعثر على أزواج key=value.\nمثال: <code>/update_strategy 1120 stop_loss_pct=0.02</code>",
                parse_mode="HTML",
            )
            return
        result = ed.apply(symbol, updates, source="telegram")
        lines = [f"⚙️ <b>تحديث {symbol}</b>"]
        if result.get("applied"):
            lines.append("<b>طُبّق:</b>")
            for k, v in result["applied"].items():
                lines.append(f"• {k} = {v}")
        if result.get("errors"):
            lines.append("<b>أخطاء:</b>")
            for e in result["errors"]:
                lines.append(f"• {e}")
        lines.append("\n<b>الفعّالة الآن:</b>")
        lines.append(_fmt_params(result.get("effective") or {}))
        await update.message.reply_text("\n".join(lines), parse_mode="HTML")
    except Exception as exc:
        logger.error(f"/update_strategy failed: {exc}")
        await update.message.reply_text(f"❌ {exc}")


async def cmd_strategies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        symbols = _ed().list_overridden_symbols()
        if not symbols:
            await update.message.reply_text("لا رموز لديها overrides حالياً.")
            return
        lines = ["📋 <b>رموز بـ overrides</b>\n"]
        for s in symbols:
            lines.append(f"• <code>{s}</code>")
        lines.append("\n<code>/strategy SYMBOL</code>")
        await update.message.reply_text("\n".join(lines), parse_mode="HTML")
    except Exception as exc:
        await update.message.reply_text(f"❌ {exc}")


async def cmd_params(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lines = ["🔐 <b>المفاتيح المسموحة</b>\n"]
    for k, spec in PARAM_SPEC.items():
        t = spec.get("type")
        if t in ("float", "int"):
            lines.append(f"• <code>{k}</code> ({t}) [{spec.get('min')} … {spec.get('max')}]")
        elif t == "str":
            lines.append(f"• <code>{k}</code> ({', '.join(spec.get('choices') or [])})")
        else:
            lines.append(f"• <code>{k}</code> ({t})")
    lines.append("\nمثال:")
    lines.append("<code>/update_strategy 1120 stop_loss_pct=0.02 take_profit_pct=0.05</code>")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


def register_phase6_handlers(application) -> None:
    application.add_handler(CommandHandler("strategy", cmd_strategy))
    application.add_handler(CommandHandler("update_strategy", cmd_update_strategy))
    application.add_handler(CommandHandler("strategies", cmd_strategies))
    application.add_handler(CommandHandler("params", cmd_params))
    logger.info("Phase 6 Telegram handlers registered")
