"""
Alpha-Engine2 Telegram Bot
Sends alerts and handles commands
"""

import asyncio
from telegram import Update, Bot
from telegram.ext import Application, CommandHandler, ContextTypes
from loguru import logger
from datetime import datetime

from sqlalchemy import text
from config.config_manager import config
from scripts.database import db, get_pending_alerts, mark_alert_sent
from bots.data_importer.bot import DataImporter


class AlphaTelegramBot:
    """Telegram bot for alerts and commands"""

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
            "/silent_on - تفعيل الوضع الصامت (إيقاف التنبيهات)\n"
            "/silent_off - إيقاف الوضع الصامت (تفعيل التنبيهات)\n"
            "/signals - عرض آخر 10 إشارات\n"
            "/help - عرض هذه المساعدة\n\n"
            "<b>ملاحظة:</b> الوضع الصامت يوقف التنبيهات فقط، "
            "بينما يستمر النظام في جمع البيانات والتحليل.\n\n"
            "<b>استيراد البيانات:</b> ضع ملفات CSV في مجلد "
            "<code>data/historical/</code> باسم "
            "<code>SYMBOL_TIMEFRAME.csv</code> مثل "
            "<code>tasi_1m.csv</code> أو <code>3050_60m.csv</code>."
        )
        await update.message.reply_text(text, parse_mode='HTML')

    # ------------------------------------------------------------------
    # Setup & run
    # ------------------------------------------------------------------

    def setup_handlers(self):
        """Setup command handlers"""
        self.application.add_handler(CommandHandler("start",             self.cmd_start))
        self.application.add_handler(CommandHandler("status",            self.cmd_status))
        self.application.add_handler(CommandHandler("import_tasi_data",  self.cmd_import_tasi_data))
        self.application.add_handler(CommandHandler("silent_on",         self.cmd_silent_on))
        self.application.add_handler(CommandHandler("silent_off",        self.cmd_silent_off))
        self.application.add_handler(CommandHandler("signals",           self.cmd_signals))
        self.application.add_handler(CommandHandler("help",              self.cmd_help))

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
