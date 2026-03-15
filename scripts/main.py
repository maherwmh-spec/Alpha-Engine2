"""
Alpha-Engine2 Main Application
Entry point for the Alpha-Engine2 system
"""

import sys
import signal
import time
from pathlib import Path
from loguru import logger

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.config_manager import config
from scripts.database import db
from scripts.redis_manager import redis_manager
from scripts.logger import setup_logger


class AlphaEngine:
    """Main application class"""
    
    def __init__(self):
        self.running = False
        self.setup_signal_handlers()
    
    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
    
    def shutdown(self, signum, frame):
        """Graceful shutdown"""
        logger.warning(f"Received signal {signum}, shutting down...")
        self.running = False
    
    def check_dependencies(self) -> bool:
        """Check if all dependencies are available"""
        logger.info("Checking system dependencies...")
        
        # Check database connection
        if not db.test_connection():
            logger.error("Database connection failed")
            return False
        
        # Check Redis connection
        if not redis_manager.test_connection():
            logger.error("Redis connection failed")
            return False
        
        logger.success("All dependencies are available")
        return True
    
    def initialize_system(self):
        """Initialize the system"""
        logger.info("Initializing Alpha-Engine2...")
        
        # Display configuration
        logger.info(f"Silent Mode: {config.is_silent_mode()}")
        logger.info(f"Trading Market: {config.get('market.name', 'TASI')}")
        logger.info(f"Timezone: {config.get('market.timezone', 'Asia/Riyadh')}")
        
        # Count enabled bots
        enabled_bots = []
        for bot_name in [
            'data_importer', 'technical_miner', 'market_reporter', 'scientist',
            'strategic_analyzer', 'monitor', 'behavioral_analyzer',
            'multiframe_confirmer', 'risk_guardian', 'consolidation_hunter',
            'self_trainer', 'weekly_reviewer', 'health_monitor',
            'backup_manager', 'parameter_editor', 'dashboard_service',
            'freqai_manager', 'silent_mode_manager'
        ]:
            if config.is_bot_enabled(bot_name):
                enabled_bots.append(bot_name)
        
        logger.info(f"Enabled bots: {len(enabled_bots)}/17")
        
        # Count enabled strategies
        enabled_strategies = []
        for strategy_name in ['aggressive_daily', 'short_waves', 'medium_waves', 'price_explosions']:
            if config.is_strategy_enabled(strategy_name):
                enabled_strategies.append(strategy_name)
        
        logger.info(f"Enabled strategies: {len(enabled_strategies)}/4")

        # Merge watchlist symbols with config symbols
        base_symbols = config.get('symbols', [])
        watchlist_symbols = redis_manager.get('user_watchlist') or []
        if isinstance(watchlist_symbols, list) and watchlist_symbols:
            merged = list(dict.fromkeys(base_symbols + watchlist_symbols))
            config.set('symbols', merged)
            logger.info(
                f"[Watchlist] Merged {len(watchlist_symbols)} watchlist symbols "
                f"(total active: {len(merged)})"
            )
        else:
            logger.info(f"[Watchlist] No extra symbols. Using {len(base_symbols)} base symbols.")

        logger.success("System initialized successfully")
    
    def run(self):
        """Main application loop"""
        logger.info("Starting Alpha-Engine2...")
        
        # Check dependencies
        if not self.check_dependencies():
            logger.error("Dependency check failed. Exiting...")
            sys.exit(1)
        
        # Initialize system
        self.initialize_system()
        
        # Display banner
        self.display_banner()
        
        # Set running flag
        self.running = True
        
        logger.success("Alpha-Engine2 is running!")
        logger.info("Press Ctrl+C to stop")
        
        # Main loop - keep application running
        # Actual work is done by Celery workers
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.warning("Interrupted by user")
        
        logger.info("Alpha-Engine2 stopped")
    
    def display_banner(self):
        """Display startup banner"""
        banner = """
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║                    Alpha-Engine2 🚀                           ║
║                                                               ║
║     نظام تحليل كمي ذكي لسوق الأسهم السعودي (TASI)            ║
║                                                               ║
║     17 روبوت متخصص | 4 استراتيجيات | تعلم آلي متقدم          ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
"""
        print(banner)
        logger.info("System Status: OPERATIONAL")


def main():
    """Main entry point"""
    try:
        # Setup logger
        setup_logger()
        
        # Create and run application
        app = AlphaEngine()
        app.run()
        
    except Exception as e:
        logger.critical(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
