"""
DataImporter Bot
Reads CSV files from data/historical/ and upserts them into market_data.ohlcv.
"""
import os
import asyncio
from pathlib import Path
from typing import Dict, Any, List

import pandas as pd
import asyncpg
from loguru import logger

from config.config_manager import config


class DataImporter:
    """Imports historical OHLCV data from CSV files into TimescaleDB."""

    def __init__(self):
        self.dsn = config.get_asyncpg_dsn()
        self.data_dir = Path("/app/data/historical")
        self.pool = None

    async def _init_pool(self):
        if self.pool is None:
            self.pool = await asyncpg.create_pool(self.dsn, min_size=1, max_size=5)

    async def _close_pool(self):
        if self.pool:
            await self.pool.close()
            self.pool = None

    async def run(self) -> Dict[str, Any]:
        """
        Main entry point: scan data/historical/ for CSV files,
        parse them and upsert into market_data.ohlcv.
        Returns a dict with: status, file_count, imported_rows, errors.
        """
        result = {
            "status": "success",
            "file_count": 0,
            "imported_rows": 0,
            "errors": []
        }

        if not self.data_dir.exists():
            logger.warning(f"Data directory not found: {self.data_dir}")
            result["status"] = "error"
            result["errors"].append(f"Directory not found: {self.data_dir}")
            return result

        csv_files = list(self.data_dir.glob("*.csv"))
        if not csv_files:
            logger.info("No CSV files found in data/historical/")
            result["status"] = "success"
            return result

        await self._init_pool()

        for csv_path in csv_files:
            try:
                rows = await self._import_file(csv_path)
                result["file_count"] += 1
                result["imported_rows"] += rows
                logger.success(f"Imported {rows} rows from {csv_path.name}")
            except Exception as e:
                logger.error(f"Error importing {csv_path.name}: {e}")
                result["errors"].append(f"{csv_path.name}: {str(e)}")

        await self._close_pool()

        if result["errors"] and result["file_count"] > 0:
            result["status"] = "partial"
        elif result["errors"]:
            result["status"] = "error"

        return result

    async def _import_file(self, csv_path: Path) -> int:
        """Parse a single CSV file and upsert rows into market_data.ohlcv."""
        df = pd.read_csv(csv_path)

        # Normalize column names (case-insensitive)
        df.columns = [c.lower().strip() for c in df.columns]

        # Detect symbol from filename if not in columns
        symbol = csv_path.stem.upper()
        if "symbol" not in df.columns:
            df["symbol"] = symbol
        else:
            df["symbol"] = df["symbol"].astype(str).str.upper()

        # Normalize timestamp column
        time_col = next((c for c in df.columns if c in ("time", "date", "datetime", "timestamp")), None)
        if time_col is None:
            raise ValueError(f"No time column found in {csv_path.name}")
        df["time"] = pd.to_datetime(df[time_col], utc=True)

        # Ensure required columns exist
        for col in ("open", "high", "low", "close", "volume"):
            if col not in df.columns:
                raise ValueError(f"Missing column '{col}' in {csv_path.name}")

        df["volume"] = df["volume"].fillna(0).astype(int)
        df["open"]   = df["open"].astype(float)
        df["high"]   = df["high"].astype(float)
        df["low"]    = df["low"].astype(float)
        df["close"]  = df["close"].astype(float)

        timeframe = "1d"
        records = [
            (
                row["time"].to_pydatetime(),
                row["symbol"],
                timeframe,
                row["open"],
                row["high"],
                row["low"],
                row["close"],
                int(row["volume"]),
                "csv_import",
            )
            for _, row in df.iterrows()
        ]

        upsert_sql = """
            INSERT INTO market_data.ohlcv
                (time, symbol, timeframe, open, high, low, close, volume, source)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (time, symbol, timeframe)
            DO UPDATE SET
                open   = EXCLUDED.open,
                high   = EXCLUDED.high,
                low    = EXCLUDED.low,
                close  = EXCLUDED.close,
                volume = EXCLUDED.volume,
                source = EXCLUDED.source
        """

        async with self.pool.acquire() as conn:
            await conn.executemany(upsert_sql, records)

        return len(records)
