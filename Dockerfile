FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    wget \
    libpq-dev \
    # libta-lib0 removed (not available in debian bookworm)
    # libta-lib0-dev removed (not available in debian bookworm)
    && rm -rf /var/lib/apt/lists/*

# Install TA-Lib from source (replaces the unavailable apt packages)
RUN wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz && \
    tar -xzf ta-lib-0.4.0-src.tar.gz && \
    cd ta-lib/ && \
    ./configure --prefix=/usr && \
    make && \
    make install && \
    cd .. && \
    rm -rf ta-lib ta-lib-0.4.0-src.tar.gz

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# NOTE: playwright install chromium moved to runtime (entrypoint scripts)
# Reason: playwright install-deps requires X server deps not available at build time

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p data/raw data/processed data/models data/backups logs

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Default command
CMD ["python", "scripts/main.py"]
