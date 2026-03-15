#!/bin/bash
# Entrypoint for market_reporter service
# Installs Playwright at runtime (not build time) to avoid X server issues

set -e

echo "[entrypoint] Starting market_reporter setup..."

# Install Playwright system dependencies at runtime
echo "[entrypoint] Installing Playwright system dependencies..."
apt-get update -qq && apt-get install -y -qq \
    libnss3 \
    libatk-bridge2.0-0 \
    libdrm2 \
    libxkbcommon0 \
    libgbm1 \
    libasound2 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libpango-1.0-0 \
    libcairo2 \
    libatspi2.0-0 \
    2>/dev/null || true

# Install Playwright browser
echo "[entrypoint] Installing Playwright Chromium browser..."
playwright install chromium 2>/dev/null || echo "[entrypoint] WARNING: playwright install failed, scraping may not work"

echo "[entrypoint] Starting market_reporter bot..."
exec python -m bots.market_reporter.bot "$@"
