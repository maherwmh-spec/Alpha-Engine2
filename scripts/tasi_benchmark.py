"""Fetch official TASI daily OHLCV from SahmK (no local 90001 calculation)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
import requests
from loguru import logger
from sqlalchemy import text

from config.config_manager import config
from scripts.database import db

TASI_SYMBOL = "TASI"
LEGACY_INDEX_SYMBOL = "90001"


def fetch_tasi_daily_from_sahmk(lookback_days: int = 400) -> Optional[pd.DataFrame]:
    key = (config.get_sahmk_api_key() or "").strip()
    if not key:
        logger.error("TASI fetch aborted: SAHMK_API_KEY missing")
        return None

    base = config.get_sahmk_base_url().rstrip("/")
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=max(lookback_days, 120))
    url = f"{base}/historical/TASI/"
    params = {
        "interval": "1d",
        "from": start.isoformat(),
        "to": end.isoformat(),
        "limit": 2000,
    }
    try:
        resp = requests.get(
            url,
            headers={"X-API-Key": key, "Accept": "application/json"},
            params=params,
            timeout=45,
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:
        logger.error(f"TASI SahmK request failed: {exc}")
        return None

    rows = []
    if isinstance(payload, dict):
        rows = payload.get("data") or payload.get("results") or payload.get("candles") or []
    elif isinstance(payload, list):
        rows = payload
    if not rows:
        logger.warning(f"TASI historical empty: keys={list(payload)[:12] if isinstance(payload, dict) else type(payload)}")
        return None

    df = pd.DataFrame(rows)
    rename = {
        "date": "time",
        "timestamp": "time",
        "t": "time",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume",
        "adjusted_close": "close",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
    if "time" not in df.columns or "close" not in df.columns:
        logger.error(f"TASI unexpected columns: {list(df.columns)}")
        return None
    df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
    for col in ("open", "high", "low", "close", "volume"):
        if col not in df.columns:
            df[col] = df["close"] if col != "volume" else 0
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["time", "close"]).sort_values("time").reset_index(drop=True)
    if df["open"].isna().all():
        df["open"] = df["close"]
    if df["high"].isna().all():
        df["high"] = df["close"]
    if df["low"].isna().all():
        df["low"] = df["close"]
    df["volume"] = df["volume"].fillna(0)
    logger.success(
        f"Fetched official TASI daily bars from SahmK: {len(df)} "
        f"({df['time'].min()} → {df['time'].max()})"
    )
    return df[["time", "open", "high", "low", "close", "volume"]]


def persist_tasi_daily(df: pd.DataFrame) -> int:
    """Store official TASI bars in market_data.indices (TASI + 90001 alias). Not a local index calc."""
    if df is None or df.empty:
        return 0
    inserted = 0
    sql = text(
        """
        INSERT INTO market_data.indices
            (time, symbol, name, timeframe, open, high, low, close, volume, source)
        VALUES
            (:time, :symbol, :name, '1d', :open, :high, :low, :close, :volume, 'sahmk_tasi')
        ON CONFLICT (symbol, timeframe, time) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            source = EXCLUDED.source
        """
    )
    names = {TASI_SYMBOL: "TASI", LEGACY_INDEX_SYMBOL: "TASI - المؤشر العام"}
    try:
        with db.get_session() as session:
            for symbol, name in names.items():
                for rec in df.itertuples(index=False):
                    session.execute(
                        sql,
                        {
                            "time": rec.time.to_pydatetime() if hasattr(rec.time, "to_pydatetime") else rec.time,
                            "symbol": symbol,
                            "name": name,
                            "open": float(rec.open),
                            "high": float(rec.high),
                            "low": float(rec.low),
                            "close": float(rec.close),
                            "volume": float(rec.volume or 0),
                        },
                    )
                    inserted += 1
            session.commit()
        logger.info(f"Persisted official TASI daily bars into indices ({inserted} upserts)")
        return inserted
    except Exception as exc:
        logger.warning(f"Could not persist TASI into indices (in-memory still usable): {exc}")
        return 0
