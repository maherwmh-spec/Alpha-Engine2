# Freqtrade Integration for Alpha-Engine2

This directory integrates Freqtrade into the Alpha-Engine2 project for advanced backtesting, paper trading, and AI-driven strategy optimization using FreqAI.

## Core Concepts

- **No Live Trading**: This setup is strictly for analysis and paper trading. It does not execute real orders.
- **Data Source**: It is configured to use the same TimescaleDB database as the rest of Alpha-Engine2, which is populated by the `market_reporter` bot using Sahmk.sa data.
- **AI-Powered**: The primary strategy is `FreqAIStrategy`, which leverages a LightGBM machine learning model to make trading decisions. The model is continually trained on new data.
- **Genetic Feature Selection**: FreqAI is configured to use genetic algorithms to automatically select the most predictive features for the model.

## How to Run

### 1. Start the Freqtrade Service

The service is defined in the main `docker-compose.yml` file. You can start it along with all other services:

```bash
docker compose up -d freqtrade
```

Or, to run it in the foreground and watch the logs:

```bash
docker compose up freqtrade
```

### 2. Access the Freqtrade UI

Once the container is running, you can access the web interface at:

- **URL**: `http://<your_server_ip>:8080`
- **Username**: `admin`
- **Password**: `alpha_password_2024` (as defined in `config.json`)

### 3. Backtesting

To backtest a strategy, use the `freqtrade backtesting` command. You need to `exec` into the container:

```bash
docker compose exec alpha_freqtrade freqtrade backtesting --strategy AggressiveDailyStrategy --timerange 20260101-
```

### 4. Hyperparameter Optimization

To optimize strategy parameters, use the `hyperopt` command:

```bash
docker compose exec alpha_freqtrade freqtrade hyperopt --strategy ShortWavesStrategy --epochs 50 --spaces all
```

### 5. FreqAI Training

The `FreqAIStrategy` will automatically train itself based on the configuration in `config.json`. You can monitor the training process in the logs.

To manually trigger a training session:

```bash
docker compose exec alpha_freqtrade freqtrade trade --strategy FreqAIStrategy --train
```

## Directory Structure

- `config.json`: Main configuration for Freqtrade, including database, Telegram, and FreqAI settings.
- `strategies/`: Contains all trading strategies.
  - `AggressiveDailyStrategy.py`: Focuses on high-frequency, small-profit trades.
  - `ShortWavesStrategy.py`: Aims to catch short-term price waves.
  - `MediumWavesStrategy.py`: Aims to catch medium-term price waves.
  - `PriceExplosionsStrategy.py`: Looks for sudden price movements with high volume.
  - `FreqAIStrategy.py`: The master AI strategy that uses the ML model.
- `freqai/`: Contains FreqAI model configurations and saved models.
  - `LightGBM_genetic.json`: Defines the parameters for the LightGBM model and genetic feature selection.
- `user_data/`:
  - `data/`: Stores market data downloaded by Freqtrade (if not using the shared DB).
  - `logs/`: Freqtrade's log files.
