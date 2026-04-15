"""
Alpha-Engine2 Telegram Bot
Sends alerts and handles commands.

التحديثات:
  - إضافة /import_metastock : استيراد بيانات MetaStock عبر رفع ملف ZIP
  - إضافة /ms_symbols       : عرض قائمة الرموز في ملف MetaStock
  - معالج المستندات         : يقبل ملفات ZIP/DAT ترسل مباشرة للبوت
"""

import asyncio
import os
import tempfile
from pathlib import Path
from typing import Optional

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

    # الحد الأقصى لحجم الملف المقبول (200 MB)
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
        """Send message to Telegram"""
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
        """Send all pending alerts from database"""
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

    # ------------------------------------------------------------------
    # Command handlers
    # ------------------------------------------------------------------

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command"""
        await update.message.reply_text(
            "🚀 <b>Alpha-Engine2 Bot</b>\n\n"
            "مرحباً! أنا مساعدك الذكي لتحليل السوق السعودي.\n\n"
            "<b>الأوامر المتاحة:</b>\n"
            "/status - حالة النظام\n"
            "/import_tasi_data - استيراد بيانات تاسي من CSV\n"
            "/import_metastock - استيراد بيانات MetaStock (ZIP)\n"
            "/ms_symbols - عرض رموز ملف MetaStock\n"
            "/silent_on - تفعيل الوضع الصامت\n"
            "/silent_off - إيقاف الوضع الصامت\n"
            "/signals - آخر الإشارات\n"
            "/help - المساعدة",
            parse_mode='HTML'
        )

    async def cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /status command"""
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
        """
        Handle /import_tasi_data command.

        Triggers the DataImporter bot to read all CSV files from
        data/historical/ and upsert them into market_data.ohlcv.
        """
        await update.message.reply_text(
            "⏳ <b>جارٍ استيراد البيانات...</b>\n\n"
            "يتم الآن قراءة ملفات CSV من مجلد <code>data/historical/</code> "
            "وإدخالها في قاعدة البيانات. قد يستغرق ذلك بعض الوقت.",
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
                    f"📁 الملفات المعالجة: <b>{file_count}</b>\n"
                    f"📊 الصفوف المُدخلة: <b>{imported_rows:,}</b>\n\n"
                    f"البيانات متوفرة الآن في جدول <code>market_data.ohlcv</code>"
                )
            elif status == 'partial':
                error_summary = "\n".join(f"  • {e}" for e in errors[:5])
                reply = (
                    f"⚠️ <b>اكتمل الاستيراد مع أخطاء</b>\n\n"
                    f"📁 الملفات المعالجة: <b>{file_count}</b>\n"
                    f"📊 الصفوف المُدخلة: <b>{imported_rows:,}</b>\n"
                    f"❌ أخطاء ({len(errors)}):\n{error_summary}"
                )
            else:
                reply = (
                    f"ℹ️ <b>نتيجة الاستيراد</b>\n\n"
                    f"{result.get('message', 'لا توجد ملفات للاستيراد.')}"
                )

            self.logger.info(
                f"[/import_tasi_data] {imported_rows:,} rows from {file_count} files"
            )

        except Exception as exc:
            self.logger.error(f"[/import_tasi_data] Unexpected error: {exc}")
            reply = (
                f"❌ <b>فشل الاستيراد</b>\n\n"
                f"<code>{exc}</code>"
            )

        await update.message.reply_text(reply, parse_mode='HTML')

    # ── MetaStock Commands ─────────────────────────────────────────────────

    async def cmd_import_metastock(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Handle /import_metastock command.

        يُطلب من المستخدم إرسال ملف ZIP يحتوي على بيانات MetaStock.
        يمكن تمرير رموز محددة كمعاملات: /import_metastock 1010 1020 2222
        """
        # استخراج فلتر الرموز من المعاملات إن وُجدت
        symbols_filter = None
        if context.args:
            symbols_filter = [s.upper() for s in context.args]
            filter_text = f"\n📌 الرموز المحددة: <code>{', '.join(symbols_filter)}</code>"
        else:
            filter_text = "\n📌 سيتم استيراد <b>جميع الرموز</b>"

        # حفظ الفلتر في بيانات المستخدم للاستخدام عند استقبال الملف
        context.user_data['ms_symbols_filter'] = symbols_filter

        await update.message.reply_text(
            "📂 <b>استيراد بيانات MetaStock</b>\n\n"
            "أرسل ملف <b>ZIP</b> يحتوي على بيانات MetaStock "
            "(EMASTER/XMASTER + ملفات F*.DAT).\n"
            f"{filter_text}\n\n"
            "💡 <i>يمكنك تحديد رموز معينة بكتابتها بعد الأمر:\n"
            "<code>/import_metastock 1010 1020 2222</code></i>",
            parse_mode='HTML'
        )

    async def cmd_ms_symbols(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Handle /ms_symbols command.

        يُطلب من المستخدم إرسال ملف ZIP لعرض قائمة الرموز فيه دون استيراد.
        """
        context.user_data['ms_list_only'] = True
        await update.message.reply_text(
            "🔍 <b>عرض رموز MetaStock</b>\n\n"
            "أرسل ملف <b>ZIP</b> يحتوي على بيانات MetaStock "
            "لعرض قائمة الرموز المتاحة.",
            parse_mode='HTML'
        )

    async def cmd_silent_on(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Enable silent mode"""
        config.enable_silent_mode()
        await update.message.reply_text(
            "🔇 <b>تم تفعيل الوضع الصامت</b>\n\n"
            "لن يتم إرسال التنبيهات، لكن جمع البيانات مستمر.",
            parse_mode='HTML'
        )

    async def cmd_silent_off(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Disable silent mode"""
        config.disable_silent_mode()
        await update.message.reply_text(
            "🔔 <b>تم إيقاف الوضع الصامت</b>\n\n"
            "سيتم إرسال التنبيهات بشكل طبيعي.",
            parse_mode='HTML'
        )

    async def cmd_signals(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show recent signals"""
        try:
            with db.get_session() as session:
                result = session.execute(text("""
                    SELECT strategy_name, symbol, signal_type, confidence, price, timestamp
                    FROM strategies.signals
                    ORDER BY timestamp DESC
                    LIMIT 10
                """))
                signals = result.fetchall()

            if not signals:
                await update.message.reply_text("لا توجد إشارات حديثة")
                return

            text = "🎯 <b>آخر الإشارات</b>\n\n"

            for strategy, symbol, signal_type, confidence, price, timestamp in signals:
                emoji = "🟢" if signal_type == 'BUY' else "🔴" if signal_type == 'SELL' else "⚪"
                text += f"{emoji} <b>{symbol}</b> - {signal_type}\n"
                text += f"   الاستراتيجية: {strategy}\n"
                text += f"   الثقة: {confidence:.0%} | السعر: {price:.2f}\n"
                text += f"   {timestamp.strftime('%H:%M:%S')}\n\n"

            await update.message.reply_text(text, parse_mode='HTML')

        except Exception as e:
            await update.message.reply_text(f"❌ خطأ: {e}")

    async def cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show help"""
        text = (
            "📚 <b>المساعدة - Alpha-Engine2</b>\n\n"
            "<b>الأوامر المتاحة:</b>\n\n"
            "/start - بدء البوت\n"
            "/status - عرض حالة النظام والروبوتات\n"
            "/import_tasi_data - استيراد بيانات تاسي من ملفات CSV\n"
            "/import_metastock - استيراد بيانات MetaStock من ملف ZIP\n"
            "/ms_symbols - عرض قائمة الرموز في ملف MetaStock\n"
            "/silent_on - تفعيل الوضع الصامت (إيقاف التنبيهات)\n"
            "/silent_off - إيقاف الوضع الصامت (تفعيل التنبيهات)\n"
            "/signals - عرض آخر 10 إشارات\n"
            "/help - عرض هذه المساعدة\n\n"
            "<b>استيراد MetaStock:</b>\n"
            "1. اكتب <code>/import_metastock</code>\n"
            "2. أرسل ملف ZIP يحتوي على بيانات MetaStock\n"
            "3. انتظر رسالة التأكيد\n\n"
            "يمكن تحديد رموز معينة:\n"
            "<code>/import_metastock 1010 1020 2222</code>\n\n"
            "<b>استيراد CSV:</b> ضع ملفات CSV في مجلد "
            "<code>data/historical/</code> باسم "
            "<code>SYMBOL_TIMEFRAME.csv</code>."
        )
        await update.message.reply_text(text, parse_mode='HTML')

    # ── معالج المستندات (ملفات ZIP) ────────────────────────────────────────

    async def handle_document(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        معالج الملفات المرسلة عبر التليجرام.

        يقبل:
          - ملفات ZIP تحتوي على بيانات MetaStock
          - يتحقق من الحجم والامتداد قبل التنزيل
        """
        doc: Document = update.message.document
        if doc is None:
            return

        filename = doc.file_name or 'unknown'
        file_size_mb = (doc.file_size or 0) / (1024 * 1024)

        self.logger.info(
            f"[document] استقبال ملف: {filename} "
            f"({file_size_mb:.1f} MB) من المستخدم {update.effective_user.id}"
        )

        # ── التحقق من الامتداد ──────────────────────────────────────────
        ext = Path(filename).suffix.lower()
        if ext not in ('.zip',):
            await update.message.reply_text(
                "⚠️ <b>نوع الملف غير مدعوم</b>\n\n"
                "يُقبل فقط ملفات <b>ZIP</b> تحتوي على بيانات MetaStock.\n"
                "استخدم الأمر <code>/import_metastock</code> للمزيد من التفاصيل.",
                parse_mode='HTML'
            )
            return

        # ── التحقق من الحجم ─────────────────────────────────────────────
        if file_size_mb > self.MAX_FILE_SIZE_MB:
            await update.message.reply_text(
                f"⚠️ <b>الملف كبير جداً</b>\n\n"
                f"الحد الأقصى المسموح: <b>{self.MAX_FILE_SIZE_MB} MB</b>\n"
                f"حجم ملفك: <b>{file_size_mb:.1f} MB</b>",
                parse_mode='HTML'
            )
            return

        # ── استرداد إعدادات الجلسة ──────────────────────────────────────
        list_only: bool = context.user_data.pop('ms_list_only', False)
        symbols_filter = context.user_data.pop('ms_symbols_filter', None)

        # ── إشعار البدء ─────────────────────────────────────────────────
        progress_msg = await update.message.reply_text(
            f"⏬ <b>جارٍ تنزيل الملف...</b>\n"
            f"📄 {filename} ({file_size_mb:.1f} MB)",
            parse_mode='HTML'
        )

        # ── تنزيل الملف ─────────────────────────────────────────────────
        with tempfile.TemporaryDirectory(prefix='tg_ms_') as tmp_dir:
            zip_path = Path(tmp_dir) / filename
            try:
                tg_file = await doc.get_file()
                await tg_file.download_to_drive(str(zip_path))
                self.logger.info(f"[document] تم تنزيل: {zip_path}")
            except Exception as e:
                self.logger.error(f"[document] فشل التنزيل: {e}")
                await progress_msg.edit_text(
                    f"❌ <b>فشل تنزيل الملف</b>\n\n<code>{e}</code>",
                    parse_mode='HTML'
                )
                return

            # ── وضع العرض فقط (ms_symbols) ──────────────────────────────
            if list_only:
                await self._handle_ms_list(update, progress_msg, zip_path, tmp_dir)
                return

            # ── وضع الاستيراد الكامل ─────────────────────────────────────
            await self._handle_ms_import(
                update, progress_msg, zip_path, tmp_dir, symbols_filter
            )

    async def _handle_ms_list(
        self,
        update: Update,
        progress_msg,
        zip_path: Path,
        tmp_dir: str,
    ):
        """عرض قائمة الرموز في ملف MetaStock دون استيراد."""
        try:
            await progress_msg.edit_text(
                "🔍 <b>جارٍ قراءة فهرس MetaStock...</b>",
                parse_mode='HTML'
            )
            data_dir = extract_metastock_zip(zip_path, Path(tmp_dir) / 'extracted')
            parser = MetaStockParser(data_dir)
            symbols = parser.list_symbols()

            if not symbols:
                await progress_msg.edit_text(
                    "⚠️ <b>لم يُعثر على رموز في الملف</b>\n\n"
                    "تأكد أن الملف يحتوي على EMASTER أو XMASTER.",
                    parse_mode='HTML'
                )
                return

            # بناء الرسالة (حد 4096 حرف في تيليجرام)
            lines = [f"📋 <b>رموز MetaStock ({len(symbols)} رمز)</b>\n"]
            for s in symbols[:50]:  # عرض أول 50 رمز
                tf = s.get('timeframe', '?')
                name = s.get('name', '')[:20]
                fd = s.get('first_date', '?')
                ld = s.get('last_date', '?')
                lines.append(
                    f"• <code>{s['symbol']:<10}</code> {name:<20} "
                    f"[{tf}] {fd} → {ld}"
                )

            if len(symbols) > 50:
                lines.append(f"\n<i>... و {len(symbols) - 50} رمز آخر</i>")

            lines.append(
                f"\n💡 لاستيراد جميع الرموز: <code>/import_metastock</code>\n"
                f"لاستيراد رموز محددة: <code>/import_metastock 1010 2222</code>"
            )

            await progress_msg.edit_text(
                '\n'.join(lines),
                parse_mode='HTML'
            )

        except Exception as e:
            self.logger.error(f"[ms_list] خطأ: {e}")
            await progress_msg.edit_text(
                f"❌ <b>خطأ في قراءة الملف</b>\n\n<code>{e}</code>",
                parse_mode='HTML'
            )

    async def _handle_ms_import(
        self,
        update: Update,
        progress_msg,
        zip_path: Path,
        tmp_dir: str,
        symbols_filter,
    ):
        """استيراد بيانات MetaStock من ملف ZIP إلى قاعدة البيانات."""
        try:
            filter_text = (
                f"الرموز: {', '.join(symbols_filter)}"
                if symbols_filter
                else "جميع الرموز"
            )
            await progress_msg.edit_text(
                f"⚙️ <b>جارٍ الاستيراد...</b>\n\n"
                f"📌 {filter_text}\n"
                f"⏳ قد يستغرق هذا بعض الوقت...",
                parse_mode='HTML'
            )

            importer = MetaStockImporter()
            result = await importer.import_from_zip(zip_path, symbols_filter)

            await self._send_import_result(progress_msg, result)
            self.logger.success(
                f"[ms_import] اكتمل: "
                f"{result.get('imported_rows', 0):,} صف، "
                f"{result.get('symbols_count', 0)} رمز"
            )

        except Exception as e:
            self.logger.error(f"[ms_import] خطأ: {e}")
            await progress_msg.edit_text(
                f"❌ <b>فشل الاستيراد</b>\n\n<code>{e}</code>",
                parse_mode='HTML'
            )

    async def _send_import_result(self, progress_msg, result: dict):
        """تنسيق وإرسال نتيجة الاستيراد."""
        status        = result.get('status', 'unknown')
        symbols_count = result.get('symbols_count', 0)
        imported_rows = result.get('imported_rows', 0)
        errors        = result.get('errors', [])
        symbols       = result.get('symbols', [])

        if status == 'success':
            icon = "✅"
            title = "تم الاستيراد بنجاح"
        elif status == 'partial':
            icon = "⚠️"
            title = "اكتمل الاستيراد مع بعض الأخطاء"
        elif status == 'empty':
            icon = "ℹ️"
            title = "لم يُعثر على بيانات"
        else:
            icon = "❌"
            title = "فشل الاستيراد"

        lines = [
            f"{icon} <b>{title}</b>\n",
            f"📊 الرموز المستوردة: <b>{symbols_count}</b>",
            f"📈 إجمالي الشموع: <b>{imported_rows:,}</b>",
        ]

        # تفاصيل الرموز (أول 10)
        if symbols:
            lines.append("\n<b>تفاصيل الرموز:</b>")
            for s in symbols[:10]:
                lines.append(
                    f"  • <code>{s['symbol']}</code> — "
                    f"{s['rows']:,} شمعة [{s['timeframe']}]"
                )
            if len(symbols) > 10:
                lines.append(f"  <i>... و {len(symbols) - 10} رمز آخر</i>")

        # الأخطاء
        if errors:
            lines.append(f"\n❌ <b>أخطاء ({len(errors)}):</b>")
            for err in errors[:5]:
                lines.append(f"  • {err}")
            if len(errors) > 5:
                lines.append(f"  <i>... و {len(errors) - 5} خطأ آخر</i>")

        lines.append(
            f"\n<i>البيانات متوفرة في جدول "
            f"<code>market_data.ohlcv</code></i>"
        )

        await progress_msg.edit_text(
            '\n'.join(lines),
            parse_mode='HTML'
        )

    # ------------------------------------------------------------------
    # Setup & run
    # ------------------------------------------------------------------

    def setup_handlers(self):
        """Setup command handlers"""
        self.application.add_handler(CommandHandler("start",             self.cmd_start))
        self.application.add_handler(CommandHandler("status",            self.cmd_status))
        self.application.add_handler(CommandHandler("import_tasi_data",  self.cmd_import_tasi_data))
        self.application.add_handler(CommandHandler("import_metastock",  self.cmd_import_metastock))
        self.application.add_handler(CommandHandler("ms_symbols",        self.cmd_ms_symbols))
        self.application.add_handler(CommandHandler("silent_on",         self.cmd_silent_on))
        self.application.add_handler(CommandHandler("silent_off",        self.cmd_silent_off))
        self.application.add_handler(CommandHandler("signals",           self.cmd_signals))
        self.application.add_handler(CommandHandler("help",              self.cmd_help))

        # معالج الملفات المرسلة (ZIP)
        self.application.add_handler(
            MessageHandler(filters.Document.ALL, self.handle_document)
        )

    async def run(self):
        """Run the bot"""
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


# ---------------------------------------------------------------------------
# Celery task for sending pending alerts
# ---------------------------------------------------------------------------
from celery import shared_task


@shared_task(name='scripts.telegram_bot.send_pending_alerts')
def send_pending_alerts():
    """Celery task to send pending alerts"""
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
