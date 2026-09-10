"""Launch MarketReporter with per-minute volume conversion enabled."""
import asyncio
from loguru import logger

from bots.market_reporter.bot import MarketReporter
from scripts.volume_delta import install_aggregator_hook


async def main():
    install_aggregator_hook()
    reporter = MarketReporter()
    await reporter.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("MarketReporter stopped by user.")
