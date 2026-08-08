"""
Alpha-Engine2 Telegram Bot
Sends alerts and handles commands.

Phase 5: /analyze /watchlist /watching /unwatch /paper /paper_close /papers
"""

import asyncio
import os
import tempfile
import zipfile
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import httpx

from telegram import Update, Bot, Document
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from loguru import logger
from datetime import datetime

from sqlalchemy import text
from config.config_manager import config
from scripts.database import db, get_pending_alerts, mark_alert_sent
from bots.data_importer.bot import DataImporter
from bots.metastock_importer.bot import MetaStockImporter
from scripts.metastock_parser import MetaStockParser, extract_metastock_zip


class AlphaTelegramBot:
    """Telegram bot for alerts and commands"""

    MAX_FILE_SIZE_MB = 200

    def __init__(self):
        self.logger = logger.bind(bot="telegram")
        self.token = config.get_telegram_token()
        self.chat_id = config.get_telegram_chat_id()
        self.enabled = config.is_telegram_enabled()

        if not self.token or not self.chat_id:
            self.logger.warning("Telegram not configured")
            self.enabled = False

        self.bot = Bot(token=self.token) if self.enabled else None
        self.application = None

    async def send_message(self, text: str, parse_mode: str = 'HTML'):
        try:
            if not self.enabled or config.is_silent_mode():
                self.logger.debug(f"Message not sent (silent mode or disabled): {text[:50]}...")
                return

            await self.bot.send_message(
                chat_id=self.chat_id,
                text=text,
                parse_mode=parse_mode
            )
            self.logger.info("Message sent to Telegram")

        except Exception as e:
            self.logger.error(f"Error sending message: {e}")

    async def send_pending_alerts(self):
        try:
            if not self.enabled or config.is_silent_mode():
                return

            with db.get_session() as session:
                alerts = get_pending_alerts(session)

            if not alerts:
                return

            self.logger.info(f"Sending {len(alerts)} pending alerts")

            for alert in alerts:
                alert_id, timestamp, alert_type, priority, title, message, symbol, strategy_name = alert[:8]

                emoji = "🔔" if priority == 1 else "📢" if priority == 2 else "ℹ️"
                text = f"{emoji} <b>{title}</b>\n\n{message}\n\n"
                text += f"<i>{timestamp.strftime('%Y-%m-%d %H:%M:%S')}</i>"

                await self.send_message(text)

                with db.get_session() as session:
                    mark_alert_sent(session, alert_id)

                await asyncio.sleep(0.5)

            self.logger.success(f"Sent {len(alerts)} alerts")

        except Exception as e:
            self.logger.error(f"Error sending pending alerts: {e}")

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "🚀 <b>Alpha-Engine2 Bot</b>\n\n"
            "مرحباً! أنا مساعدك الذكي لتحليل السوق السعودي.\n\n"
            "<b>الأوامر:</b>\n"
            "/analyze SYMBOL - تحليل فوري + مراقبة\n"
            "/watchlist - مرشّحو اليوم\n"
            "/watching - المراقبة النشطة\n"
            "/unwatch SYMBOL - إيقاف مراقبة\n"
            "/paper SYMBOL - صفقة ورقية\n"
            "/paper_close SYMBOL - إغلاق ورقي\n"
            "/papers - الصفقات الورقية\n"
            "/status - حالة النظام\n"
            "/signals - آخر الإشارات\n"
            "/help - المساعدة",
            parse_mode='HTML'
        )

    async def cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            with db.get_session() as session:
                result = session.execute(
                    text("SELECT bot_name, status, last_run FROM bots.status ORDER BY bot_name")
                )
                bots = result.fetchall()

            msg = "📊 <b>حالة النظام</b>\n\n"
            running = sum(1 for b in bots if b[1] == 'RUNNING')
            stopped = sum(1 for b in bots if b[1] == 'STOPPED')
            error   = sum(1 for b in bots if b[1] == 'ERROR')
            msg += f"✅ قيد التشغيل: {running}\n"
            msg += f"⏸ متوقف: {stopped}\n"
            msg += f"❌ خطأ: {error}\n\n"
            msg += f"🔇 الوضع الصامت: {'مفعّل' if config.is_silent_mode() else 'معطّل'}"
            await update.message.reply_text(msg, parse_mode='HTML')
        except Exception as e:
            await update.message.reply_text(f"❌ خطأ: {e}")

    async def cmd_import_tasi_data(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "⏳ <b>جارٍ استيراد البيانات...</b>",
            parse_mode='HTML'
        )
        try:
            importer = DataImporter()
            result = await importer.run()
            imported_rows = result.get('imported_rows', 0)
            file_count    = result.get('file_count', 0)
            errors        = result.get('errors', [])
            status        = result.get('status', 'unknown')
            if status == 'success' and not errors:
                reply = (
                    f"✅ <b>تم الاستيراد بنجاح</b>\n\n"
                    f"📁 الملفات: <b>{file_count}</b>\n"
                    f"📊 الصفوف: <b>{imported_rows:,}</b>"
                )
            elif status == 'partial':
                error_summary = "\n".join(f"  • {e}" for e in errors[:5])
                reply = f"⚠️ اكتمل مع أخطاء\n{error_summary}"
            else:
                reply = result.get('message', 'لا توجد ملفات.')
        except Exception as exc:
            reply = f"❌ <code>{exc}</code>"
        await update.message.reply_text(reply, parse_mode='HTML')

    async def cmd_import_metastock(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        symbols_filter = [s.upper() for s in context.args] if context.args else None
        context.user_data['ms_symbols_filter'] = symbols_filter
        filter_text = (
            f"\n📌 الرموز: <code>{', '.join(symbols_filter)}</code>"
            if symbols_filter else "\n📌 جميع الرموز"
        )
        await update.message.reply_text(
            "📂 <b>استيراد MetaStock</b>\nأرسل ZIP/DAT/MST أو URL" + filter_text,
            parse_mode='HTML'
        )

    async def cmd_ms_symbols(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        context.user_data['ms_list_only'] = True
        await update.message.reply_text("🔍 أرسل ملف MetaStock لعرض الرموز.", parse_mode='HTML')

    async def cmd_silent_on(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        config.enable_silent_mode()
        await update.message.reply_text("🔇 تم تفعيل الوضع الصامت", parse_mode='HTML')

    async def cmd_silent_off(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        config.disable_silent_mode()
        await update.message.reply_text("🔔 تم إيقاف الوضع الصامت", parse_mode='HTML')

    async def cmd_signals(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            with db.get_session() as session:
                result = session.execute(text("""
                    SELECT strategy_name, symbol, signal_type, confidence, price, timestamp
                    FROM strategies.signals
                    ORDER BY timestamp DESC LIMIT 10
                """))
                signals = result.fetchall()
            if not signals:
                await update.message.reply_text("لا توجد إشارات حديثة")
                return
            text = "🎯 <b>آخر الإشارات</b>\n\n"
            for strategy, symbol, signal_type, confidence, price, timestamp in signals:
                emoji = "🟢" if signal_type == 'BUY' else "🔴" if signal_type == 'SELL' else "⚪"
                text += f"{emoji} <b>{symbol}</b> - {signal_type}\n"
                text += f"   {strategy} | {confidence:.0%} | {price:.2f}\n\n"
            await update.message.reply_text(text, parse_mode='HTML')
        except Exception as e:
            await update.message.reply_text(f"❌ خطأ: {e}")

    async def cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        text = (
            "📚 <b>المساعدة</b>\n\n"
            "/analyze SYMBOL — تحليل + مراقبة نشطة\n"
            "/watchlist — مرشّحو اليوم\n"
            "/watching — قائمة المراقبة\n"
            "/unwatch SYMBOL — إيقاف مراقبة\n"
            "/paper SYMBOL — فتح صفقة ورقية\n"
            "/paper_close SYMBOL — إغلاق ورقي\n"
            "/papers — الصفقات المفتوحة\n"
            "/status /signals /silent_on /silent_off\n"
            "/import_tasi_data /import_metastock /ms_symbols"
        )
        await update.message.reply_text(text, parse_mode='HTML')

    async def handle_document(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        doc: Document = update.message.document
        if doc is None:
            return
        filename = doc.file_name or 'unknown'
        file_size_mb = (doc.file_size or 0) / (1024 * 1024)
        ext = Path(filename).suffix.lower()
        if ext not in ('.zip', '.dat', '.mst', '.mwd'):
            await update.message.reply_text("⚠️ نوع الملف غير مدعوم", parse_mode='HTML')
            return
        if file_size_mb > self.MAX_FILE_SIZE_MB:
            await update.message.reply_text(f"⚠️ الملف كبير جداً ({file_size_mb:.1f} MB)", parse_mode='HTML')
            return
        list_only: bool = context.user_data.pop('ms_list_only', False)
        symbols_filter = context.user_data.pop('ms_symbols_filter', None)
        progress_msg = await update.message.reply_text(f"⏬ تنزيل {filename}...", parse_mode='HTML')
        with tempfile.TemporaryDirectory(prefix='tg_ms_') as tmp_dir:
            zip_path = Path(tmp_dir) / filename
            try:
                tg_file = await doc.get_file()
                await tg_file.download_to_drive(str(zip_path))
            except Exception as e:
                await progress_msg.edit_text(f"❌ فشل التنزيل: {e}", parse_mode='HTML')
                return
            if list_only:
                await self._handle_ms_list(update, progress_msg, zip_path, tmp_dir)
                return
            await self._handle_ms_import(update, progress_msg, zip_path, tmp_dir, symbols_filter)

    async def _handle_ms_list(self, update, progress_msg, zip_path, tmp_dir):
        try:
            data_dir, _ = self._prepare_metastock_path(zip_path, tmp_dir)
            parser = MetaStockParser(data_dir)
            symbols = parser.list_symbols()
            if not symbols:
                await progress_msg.edit_text("⚠️ لم يُعثر على رموز", parse_mode='HTML')
                return
            lines = [f"📋 رموز MetaStock ({len(symbols)})\n"]
            for s in symbols[:50]:
                lines.append(f"• <code>{s['symbol']}</code>")
            await progress_msg.edit_text('\n'.join(lines), parse_mode='HTML')
        except Exception as e:
            await progress_msg.edit_text(f"❌ {e}", parse_mode='HTML')

    async def _handle_ms_import(self, update, progress_msg, zip_path, tmp_dir, symbols_filter):
        try:
            await progress_msg.edit_text("⚙️ جارٍ الاستيراد...", parse_mode='HTML')
            importer = MetaStockImporter()
            if zip_path.suffix.lower() == '.zip':
                result = await importer.import_from_zip(zip_path, symbols_filter)
            else:
                data_dir, _ = self._prepare_metastock_path(zip_path, tmp_dir)
                result = await importer.import_from_dir(data_dir, symbols_filter)
            await self._send_import_result(progress_msg, result)
        except Exception as e:
            await progress_msg.edit_text(f"❌ {e}", parse_mode='HTML')

    async def _send_import_result(self, progress_msg, result: dict):
        status = result.get('status', 'unknown')
        lines = [
            f"{'✅' if status=='success' else '⚠️'} <b>{status}</b>",
            f"رموز: {result.get('symbols_count', 0)} · صفوف: {result.get('imported_rows', 0):,}",
        ]
        await progress_msg.edit_text('\n'.join(lines), parse_mode='HTML')

    async def handle_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        text_value = (update.message.text or "").strip()
        parsed = urlparse(text_value)
        if parsed.scheme not in {"http", "https"}:
            return
        if not (context.user_data.get('ms_symbols_filter') is not None or context.user_data.get('ms_list_only')):
            return
        list_only: bool = context.user_data.pop('ms_list_only', False)
        symbols_filter = context.user_data.pop('ms_symbols_filter', None)
        progress_msg = await update.message.reply_text("⏬ تنزيل الرابط...", parse_mode='HTML')
        with tempfile.TemporaryDirectory(prefix='tg_ms_url_') as tmp_dir:
            filename = Path(parsed.path).name or 'metastock.zip'
            target = Path(tmp_dir) / filename
            try:
                async with httpx.AsyncClient(follow_redirects=True, timeout=120) as client:
                    async with client.stream('GET', text_value) as response:
                        response.raise_for_status()
                        with open(target, 'wb') as fh:
                            async for chunk in response.aiter_bytes():
                                fh.write(chunk)
            except Exception as exc:
                await progress_msg.edit_text(f"❌ {exc}", parse_mode='HTML')
                return
            if list_only:
                await self._handle_ms_list(update, progress_msg, target, tmp_dir)
            else:
                await self._handle_ms_import(update, progress_msg, target, tmp_dir, symbols_filter)

    def _prepare_metastock_path(self, source_path: Path, tmp_dir: str):
        ext = source_path.suffix.lower()
        if ext == '.zip':
            return extract_metastock_zip(source_path, Path(tmp_dir) / 'extracted'), True
        if ext in {'.dat', '.mst', '.mwd'}:
            single_dir = Path(tmp_dir) / 'single_file_metastock'
            single_dir.mkdir(parents=True, exist_ok=True)
            target = single_dir / source_path.name
            if source_path.resolve() != target.resolve():
                target.write_bytes(source_path.read_bytes())
            return single_dir, True
        raise ValueError(f"صيغة غير مدعومة: {ext}")

    def setup_handlers(self):
        self.application.add_handler(CommandHandler("start", self.cmd_start))
        self.application.add_handler(CommandHandler("status", self.cmd_status))
        self.application.add_handler(CommandHandler("import_tasi_data", self.cmd_import_tasi_data))
        self.application.add_handler(CommandHandler("import_metastock", self.cmd_import_metastock))
        self.application.add_handler(CommandHandler("upload_ms", self.cmd_import_metastock))
        self.application.add_handler(CommandHandler("ms_symbols", self.cmd_ms_symbols))
        self.application.add_handler(CommandHandler("silent_on", self.cmd_silent_on))
        self.application.add_handler(CommandHandler("silent_off", self.cmd_silent_off))
        self.application.add_handler(CommandHandler("signals", self.cmd_signals))
        self.application.add_handler(CommandHandler("help", self.cmd_help))

        # Phase 5 commands
        from scripts.telegram_phase5 import register_phase5_handlers
        register_phase5_handlers(self.application)

        self.application.add_handler(MessageHandler(filters.Document.ALL, self.handle_document))
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_text))

    async def run(self):
        if not self.enabled:
            self.logger.warning("Telegram bot is disabled")
            return
        try:
            self.logger.info("Starting Telegram bot")
            self.application = Application.builder().token(self.token).build()
            self.setup_handlers()
            await self.application.initialize()
            await self.application.start()
            await self.application.updater.start_polling()
            self.logger.success("Telegram bot is running")
            while True:
                await asyncio.sleep(1)
        except Exception as e:
            self.logger.error(f"Error running Telegram bot: {e}")


from celery import shared_task


@shared_task(name='scripts.telegram_bot.send_pending_alerts')
def send_pending_alerts():
    try:
        bot = AlphaTelegramBot()
        asyncio.run(bot.send_pending_alerts())
        return {'status': 'success'}
    except Exception as e:
        logger.error(f"Error in send_pending_alerts task: {e}")
        return {'status': 'error', 'message': str(e)}


if __name__ == "__main__":
    bot = AlphaTelegramBot()
    asyncio.run(bot.run())
