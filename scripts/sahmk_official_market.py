"""Official SahmK market snapshot: TASI summary + sector performance.

Does not compute local 90001/sector VWAP. Maps names to legacy 900xx codes when possible.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from loguru import logger
from sqlalchemy import text

from config.config_manager import config
from scripts.database import db

try:
    from scripts.sector_calculator import SECTOR_NAMES
except Exception:
    SECTOR_NAMES = {}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _client() -> Tuple[str, Dict[str, str]]:
    key = (config.get_sahmk_api_key() or "").strip()
    if not key:
        raise ValueError("SAHMK_API_KEY missing")
    base = config.get_sahmk_base_url().rstrip("/")
    return base, {"X-API-Key": key, "Accept": "application/json"}


def _get(path: str) -> Any:
    base, headers = _client()
    url = base + path
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _map_sector_code(name_en: str, name_ar: str) -> Optional[str]:
    en = (name_en or "").strip().lower()
    ar = (name_ar or "").strip()
    for code, label in (SECTOR_NAMES or {}).items():
        lab = str(label)
        en_part = lab.split(" - ")[0].strip().lower() if " - " in lab else lab.lower()
        ar_part = lab.split(" - ")[-1].strip() if " - " in lab else ""
        if en and en_part and (en == en_part or en in lab.lower() or en_part in en):
            return str(code)
        if ar and ar_part and (ar == ar_part or ar in lab):
            return str(code)
    return None


def fetch_tasi_summary() -> Dict[str, Any]:
    data = _get("/market/summary/?index=TASI")
    return data if isinstance(data, dict) else {}


def fetch_sectors(index: str = "TASI") -> List[Dict[str, Any]]:
    data = _get(f"/market/sectors/?index={index}")
    if isinstance(data, dict):
        rows = data.get("sectors") or data.get("data") or data.get("results") or []
        return rows if isinstance(rows, list) else []
    if isinstance(data, list):
        return data
    return []


def persist_index_snapshot(summary: Dict[str, Any]) -> bool:
    sql = text(
        """
        INSERT INTO market_data.sahmk_index_snapshot
            (as_of, market_index, index_value, index_change, index_change_percent,
             is_delayed, total_volume, advancing, declining, unchanged, market_mood, source, raw)
        VALUES
            (:as_of, :market_index, :index_value, :index_change, :index_change_percent,
             :is_delayed, :total_volume, :advancing, :declining, :unchanged, :market_mood, 'sahmk_official', CAST(:raw AS JSONB))
        """
    )
    try:
        with db.get_session() as session:
            session.execute(
                sql,
                {
                    "as_of": _now(),
                    "market_index": summary.get("index") or "TASI",
                    "index_value": summary.get("index_value"),
                    "index_change": summary.get("index_change"),
                    "index_change_percent": summary.get("index_change_percent"),
                    "is_delayed": summary.get("is_delayed"),
                    "total_volume": summary.get("total_volume"),
                    "advancing": summary.get("advancing"),
                    "declining": summary.get("declining"),
                    "unchanged": summary.get("unchanged"),
                    "market_mood": summary.get("market_mood"),
                    "raw": json.dumps(summary, ensure_ascii=False, default=str),
                },
            )
            session.commit()
        return True
    except Exception as exc:
        logger.warning(f"persist TASI snapshot failed (run migration first?): {exc}")
        return False


def persist_sectors(rows: List[Dict[str, Any]], index: str = "TASI") -> int:
    if not rows:
        return 0
    sql = text(
        """
        INSERT INTO market_data.sahmk_sector_performance
            (as_of, market_index, sector_code, sector_name_en, sector_name_ar,
             change_percent, avg_change_percent, volume, num_stocks, source, raw)
        VALUES
            (:as_of, :market_index, :sector_code, :sector_name_en, :sector_name_ar,
             :change_percent, :avg_change_percent, :volume, :num_stocks, 'sahmk_official', CAST(:raw AS JSONB))
        """
    )
    as_of = _now()
    saved = 0
    try:
        with db.get_session() as session:
            for row in rows:
                name_en = str(row.get("sector_name") or row.get("name_en") or row.get("name") or "")
                name_ar = str(row.get("sector_name_ar") or row.get("name_ar") or "")
                session.execute(
                    sql,
                    {
                        "as_of": as_of,
                        "market_index": index,
                        "sector_code": _map_sector_code(name_en, name_ar),
                        "sector_name_en": name_en or "unknown",
                        "sector_name_ar": name_ar or None,
                        "change_percent": row.get("change_percent"),
                        "avg_change_percent": row.get("avg_change_percent"),
                        "volume": row.get("volume"),
                        "num_stocks": row.get("num_stocks"),
                        "raw": json.dumps(row, ensure_ascii=False, default=str),
                    },
                )
                saved += 1
            session.commit()
        logger.success(f"Saved {saved} official SahmK sector rows")
        return saved
    except Exception as exc:
        logger.warning(f"persist sectors failed (run migration first?): {exc}")
        return 0


def sync_official_market() -> Dict[str, Any]:
    """Fetch TASI summary + sectors from SahmK and persist snapshots."""
    out: Dict[str, Any] = {"tasi": None, "sectors": 0, "ok": False}
    try:
        summary = fetch_tasi_summary()
        out["tasi"] = {
            "index": summary.get("index"),
            "value": summary.get("index_value"),
            "delayed": summary.get("is_delayed"),
        }
        persist_index_snapshot(summary)
        sectors = fetch_sectors("TASI")
        out["sectors"] = persist_sectors(sectors, "TASI")
        out["ok"] = True
        logger.success(
            f"Official market sync: TASI={out['tasi']} sectors_saved={out['sectors']}"
        )
        return out
    except Exception as exc:
        logger.error(f"Official market sync failed: {exc}")
        out["error"] = str(exc)
        return out
