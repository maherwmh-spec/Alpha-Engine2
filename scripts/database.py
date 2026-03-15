"""
Alpha-Engine2 Database Manager
Handles database connections and operations using SQLAlchemy 2.x
"""

from sqlalchemy import create_engine, MetaData, text
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from sqlalchemy.pool import QueuePool
from contextlib import contextmanager
from typing import Generator, Optional
from datetime import datetime
from loguru import logger

from config.config_manager import config


# Create base class for declarative models
Base = declarative_base()


class DatabaseManager:
    """Database connection and session manager"""

    def __init__(self):
        self.engine = None
        self.SessionLocal = None
        self._initialize()

    def _initialize(self):
        """Initialize database connection"""
        try:
            database_url = config.get_database_url()

            self.engine = create_engine(
                database_url,
                poolclass=QueuePool,
                pool_size=config.get('database.pool_size', 10),
                max_overflow=config.get('database.max_overflow', 20),
                pool_pre_ping=True,
                echo=False,
            )

            self.SessionLocal = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self.engine
            )

            logger.success("Database connection initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """Context manager for database sessions"""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database session error: {e}")
            raise
        finally:
            session.close()

    def execute_raw(self, query: str, params: Optional[dict] = None):
        """Execute raw SQL query — always wraps string in text()"""
        with self.engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            conn.commit()
            return result

    def test_connection(self) -> bool:
        """Test database connection (SQLAlchemy 2.x compatible)"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))   # ← FIX: text() required in SQLAlchemy 2.x
            logger.success("Database connection test successful")
            return True
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False

    def create_all_tables(self):
        """Create all tables defined in models"""
        try:
            Base.metadata.create_all(bind=self.engine)
            logger.success("All tables created successfully")
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            raise

    def drop_all_tables(self):
        """Drop all tables (use with caution!)"""
        try:
            Base.metadata.drop_all(bind=self.engine)
            logger.warning("All tables dropped")
        except Exception as e:
            logger.error(f"Failed to drop tables: {e}")
            raise


# Global database instance
db = DatabaseManager()


def get_db_session() -> Generator[Session, None, None]:
    """Get database session"""
    return db.get_session()


# ========================================
# Helper Functions — all raw SQL uses text()
# ========================================

def insert_stock_price(session: Session, symbol: str, timeframe: str,
                       open_price: float, high: float, low: float,
                       close: float, volume: int, timestamp: datetime):
    """Insert stock price data"""
    session.execute(text("""
        INSERT INTO market_data.ohlcv
        (time, symbol, open, high, low, close, volume, timeframe)
        VALUES (:time, :symbol, :open, :high, :low, :close, :volume, :timeframe)
        ON CONFLICT (time, symbol, timeframe) DO UPDATE
        SET open = EXCLUDED.open,
            high = EXCLUDED.high,
            low  = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume
    """), {
        'time': timestamp, 'symbol': symbol,
        'open': open_price, 'high': high, 'low': low,
        'close': close, 'volume': volume, 'timeframe': timeframe
    })


def insert_technical_indicators(session: Session, symbol: str, timeframe: str,
                                timestamp: datetime, indicators: dict):
    """Insert technical indicators"""
    session.execute(text("""
        INSERT INTO market_data.technical_indicators
        (time, symbol, timeframe, rsi, macd, macd_signal, macd_hist,
         bb_upper, bb_middle, bb_lower, ema_9, ema_21, sma_50, sma_200,
         atr, stoch_k, stoch_d, adx, obv)
        VALUES (:time, :symbol, :timeframe, :rsi, :macd, :macd_signal, :macd_hist,
                :bb_upper, :bb_middle, :bb_lower, :ema_9, :ema_21, :sma_50, :sma_200,
                :atr, :stoch_k, :stoch_d, :adx, :obv)
        ON CONFLICT (time, symbol, timeframe) DO UPDATE
        SET rsi = EXCLUDED.rsi, macd = EXCLUDED.macd,
            macd_signal = EXCLUDED.macd_signal, macd_hist = EXCLUDED.macd_hist,
            bb_upper = EXCLUDED.bb_upper, bb_middle = EXCLUDED.bb_middle,
            bb_lower = EXCLUDED.bb_lower, ema_9 = EXCLUDED.ema_9,
            ema_21 = EXCLUDED.ema_21, sma_50 = EXCLUDED.sma_50,
            sma_200 = EXCLUDED.sma_200, atr = EXCLUDED.atr,
            stoch_k = EXCLUDED.stoch_k, stoch_d = EXCLUDED.stoch_d,
            adx = EXCLUDED.adx, obv = EXCLUDED.obv
    """), {'time': timestamp, 'symbol': symbol, 'timeframe': timeframe, **indicators})


def insert_signal(session: Session, strategy_name: str, symbol: str,
                  signal_type: str, price: float, confidence: float,
                  timeframe: str, metadata: dict = None):
    """Insert trading signal"""
    session.execute(text("""
        INSERT INTO strategies.signals
        (timestamp, strategy_name, symbol, signal_type, price, confidence, timeframe, metadata)
        VALUES (:timestamp, :strategy_name, :symbol, :signal_type, :price, :confidence, :timeframe, :metadata)
    """), {
        'timestamp': datetime.now(), 'strategy_name': strategy_name,
        'symbol': symbol, 'signal_type': signal_type, 'price': price,
        'confidence': confidence, 'timeframe': timeframe, 'metadata': metadata
    })


def insert_alert(session: Session, alert_type: str, priority: int,
                 title: str, message: str, symbol: str = None,
                 strategy_name: str = None, metadata: dict = None):
    """Insert alert notification"""
    session.execute(text("""
        INSERT INTO alerts.notifications
        (timestamp, alert_type, priority, title, message, symbol, strategy_name, metadata)
        VALUES (:timestamp, :alert_type, :priority, :title, :message, :symbol, :strategy_name, :metadata)
    """), {
        'timestamp': datetime.now(), 'alert_type': alert_type, 'priority': priority,
        'title': title, 'message': message, 'symbol': symbol,
        'strategy_name': strategy_name, 'metadata': metadata
    })


def update_bot_status(session: Session, bot_name: str, status: str,
                      error_message: str = None, metadata: dict = None):
    """Update bot status"""
    session.execute(text("""
        INSERT INTO bots.status (bot_name, status, last_run, error_message, metadata)
        VALUES (:bot_name, :status, :last_run, :error_message, :metadata)
        ON CONFLICT (bot_name) DO UPDATE
        SET status = EXCLUDED.status,
            last_run = EXCLUDED.last_run,
            error_message = EXCLUDED.error_message,
            metadata = EXCLUDED.metadata,
            updated_at = NOW()
    """), {
        'bot_name': bot_name, 'status': status,
        'last_run': datetime.now(), 'error_message': error_message, 'metadata': metadata
    })


def get_latest_price(session: Session, symbol: str) -> Optional[float]:
    """Get latest price for a symbol"""
    result = session.execute(text("""
        SELECT close FROM market_data.ohlcv
        WHERE symbol = :symbol ORDER BY time DESC LIMIT 1
    """), {'symbol': symbol}).fetchone()
    return result[0] if result else None


def get_active_positions(session: Session, strategy_name: str = None):
    """Get active positions"""
    if strategy_name:
        return session.execute(text("""
            SELECT * FROM strategies.positions
            WHERE status = 'OPEN' AND strategy_name = :strategy_name
            ORDER BY entry_time DESC
        """), {'strategy_name': strategy_name}).fetchall()
    return session.execute(text("""
        SELECT * FROM strategies.positions
        WHERE status = 'OPEN' ORDER BY entry_time DESC
    """)).fetchall()


def get_pending_alerts(session: Session):
    """Get pending alerts that haven't been sent"""
    return session.execute(text("""
        SELECT * FROM alerts.notifications
        WHERE sent = FALSE ORDER BY priority ASC, timestamp DESC
    """)).fetchall()


def mark_alert_sent(session: Session, alert_id: int):
    """Mark alert as sent"""
    session.execute(text("""
        UPDATE alerts.notifications SET sent = TRUE, sent_at = NOW()
        WHERE id = :alert_id
    """), {'alert_id': alert_id})


def get_parameter(session: Session, key: str) -> Optional[str]:
    """Get parameter value"""
    result = session.execute(text("""
        SELECT value FROM bots.parameters WHERE key = :key
    """), {'key': key}).fetchone()
    return result[0] if result else None


def set_parameter(session: Session, key: str, value: str, updated_by: str = 'system'):
    """Set parameter value"""
    session.execute(text("""
        UPDATE bots.parameters
        SET value = :value, updated_at = NOW(), updated_by = :updated_by
        WHERE key = :key
    """), {'key': key, 'value': value, 'updated_by': updated_by})


if __name__ == "__main__":
    print("Testing database connection...")
    if db.test_connection():
        print("✅ Database connection successful!")
    else:
        print("❌ Database connection failed!")
