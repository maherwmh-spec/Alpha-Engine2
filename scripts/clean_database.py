"""
سكربت لتنظيف قاعدة البيانات من الأسهم غير المرغوبة
"""
from loguru import logger
from sqlalchemy import text
from scripts.database import db

def clean_database():
    logger.info("🧹 Starting database cleanup...")
    
    try:
        with db.get_session() as session:
            # 1. تنظيف جدول market_data.symbols
            result = session.execute(text("""
                DELETE FROM market_data.symbols
                WHERE NOT (
                    symbol ~ '^[1-8][0-9]{3}$' -- أسهم تاسي الرئيسية
                    OR symbol = '90001' -- المؤشر العام
                    OR (symbol ~ '^900[1-3][0-9]$' AND symbol::int BETWEEN 90010 AND 90030) -- القطاعات
                )
            """))
            deleted_symbols = result.rowcount
            logger.info(f"🗑️ Deleted {deleted_symbols} invalid symbols from market_data.symbols")
            
            # 2. تنظيف جدول market_data.ohlcv
            result = session.execute(text("""
                DELETE FROM market_data.ohlcv
                WHERE NOT (
                    symbol ~ '^[1-8][0-9]{3}$'
                    OR symbol = '90001'
                    OR (symbol ~ '^900[1-3][0-9]$' AND symbol::int BETWEEN 90010 AND 90030)
                )
            """))
            deleted_ohlcv = result.rowcount
            logger.info(f"🗑️ Deleted {deleted_ohlcv} invalid rows from market_data.ohlcv")
            
            # 3. تنظيف جدول strategies.signals
            result = session.execute(text("""
                DELETE FROM strategies.signals
                WHERE NOT (
                    symbol ~ '^[1-8][0-9]{3}$'
                    OR symbol = '90001'
                    OR (symbol ~ '^900[1-3][0-9]$' AND symbol::int BETWEEN 90010 AND 90030)
                )
            """))
            deleted_signals = result.rowcount
            logger.info(f"🗑️ Deleted {deleted_signals} invalid rows from strategies.signals")
            
            # 4. تنظيف جدول genetic.strategies
            result = session.execute(text("""
                DELETE FROM genetic.strategies
                WHERE NOT (
                    symbol ~ '^[1-8][0-9]{3}$'
                    OR symbol = '90001'
                    OR (symbol ~ '^900[1-3][0-9]$' AND symbol::int BETWEEN 90010 AND 90030)
                )
            """))
            deleted_genetic = result.rowcount
            logger.info(f"🗑️ Deleted {deleted_genetic} invalid rows from genetic.strategies")
            
            session.commit()
            logger.success("✅ Database cleanup completed successfully")
            
    except Exception as e:
        logger.error(f"❌ Error during database cleanup: {e}")

if __name__ == "__main__":
    clean_database()
