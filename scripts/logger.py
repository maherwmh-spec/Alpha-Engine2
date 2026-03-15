"""
Alpha-Engine2 Logger Configuration
Centralized logging setup using Loguru
"""

import sys
from pathlib import Path
from loguru import logger
from config.config_manager import config


def setup_logger():
    """Setup and configure logger"""
    
    # Remove default logger
    logger.remove()
    
    # Get logging configuration
    log_config = config.get('logging', {})
    log_level = log_config.get('level', 'INFO')
    log_file = log_config.get('file', 'logs/alpha_engine.log')
    rotation = log_config.get('rotation', '1 day')
    retention = log_config.get('retention', '30 days')
    log_format = log_config.get(
        'format',
        '{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message}'
    )
    
    # Ensure logs directory exists
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Add console handler (stdout)
    logger.add(
        sys.stdout,
        format=log_format,
        level=log_level,
        colorize=True,
        backtrace=True,
        diagnose=True,
    )
    
    # Add file handler with rotation
    logger.add(
        log_file,
        format=log_format,
        level=log_level,
        rotation=rotation,
        retention=retention,
        compression='zip',
        backtrace=True,
        diagnose=True,
        enqueue=True,  # Async logging
    )
    
    # Add error-specific log file
    error_log_file = str(log_path.parent / 'errors.log')
    logger.add(
        error_log_file,
        format=log_format,
        level='ERROR',
        rotation=rotation,
        retention=retention,
        compression='zip',
        backtrace=True,
        diagnose=True,
        enqueue=True,
    )
    
    logger.success("Logger configured successfully")
    logger.info(f"Log level: {log_level}")
    logger.info(f"Log file: {log_file}")
    
    return logger


# Initialize logger on import
setup_logger()


# Convenience function for bot logging
def get_bot_logger(bot_name: str):
    """Get logger with bot name context"""
    return logger.bind(bot=bot_name)


if __name__ == "__main__":
    # Test logger
    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.success("This is a success message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    logger.critical("This is a critical message")
    
    # Test bot logger
    bot_logger = get_bot_logger("test_bot")
    bot_logger.info("This is a bot-specific log message")
