
from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any, Dict, List

from fastapi import FastAPI, Header, HTTPException
from loguru import logger
from sqlalchemy import text

from config.config_manager import config
from scripts.database import db
from scripts.redis_manager import redis_manager

app = FastAPI(title="Alpha-Engine2 API", version="0.2.0")


def _db_scalar(sql: str, params: Dict[str, Any] | None = None) -> Any:
    with db.get_session() as session:
        return session.execute(text(sql), params or {}).scalar()


def _require_admin(api_key: str | None) -> None:
    expected = config.get_admin_api_key()
    if not expected or api_key != expected:
        raise HTTPException(status_code=401, detail="invalid_admin_api_key")


@app.get("/health")
def health() -> Dict[str, Any]:
    db_ok = db.test_connection()
    redis_ok = redis_manager.test_connection()
    return {"status": "ok" if db_ok and redis_ok else "degraded", "database": db_ok, "redis": redis_ok}


@app.get("/bots/status")
def bots_status() -> Dict[str, Any]:
    try:
        with db.get_session() as session:
            rows = session.execute(text("SELECT bot_name, status, last_run FROM bots.status ORDER BY bot_name")).fetchall()
        return {"status": "ok", "bots": [{"bot_name": r[0], "status": r[1], "last_run": str(r[2]) if r[2] else None} for r in rows]}
    except Exception as exc:
        logger.error("bots_status_failed error={}", exc)
        return {"status": "error", "message": str(exc)}


@app.get("/market/websocket/status")
def websocket_status() -> Dict[str, Any]:
    status = redis_manager.get("market_reporter:websocket_status") or redis_manager.get("sahmk:websocket_status") or {}
    metrics = redis_manager.get("market_reporter:metrics") or redis_manager.get("sahmk:metrics") or {}
    return {"status": "ok", "websocket": status, "metrics": metrics}


@app.get("/genetic/status")
def genetic_status() -> Dict[str, Any]:
    try:
        total = _db_scalar("SELECT COUNT(*) FROM genetic.strategies")
        latest = _db_scalar("SELECT MAX(created_at) FROM genetic.strategies")
        return {"status": "ok", "strategies": total, "latest_strategy_at": str(latest) if latest else None}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@app.get("/risk/boxes")
def risk_boxes() -> Dict[str, Any]:
    return {"status": "ok", "risk_boxes": config.get("risk_boxes", {})}


@app.get("/market/aggregates/health")
def aggregates_health() -> Dict[str, Any]:
    views = ["ohlcv_5m", "ohlcv_15m", "ohlcv_30m", "ohlcv_1h", "ohlcv_1d"]
    result: List[Dict[str, Any]] = []
    with db.get_session() as session:
        for view in views:
            exists = session.execute(text("SELECT to_regclass(:name)"), {"name": f"market_data.{view}"}).scalar()
            rows = None
            if exists:
                rows = session.execute(text(f"SELECT COUNT(*) FROM market_data.{view}")).scalar()
            result.append({"name": view, "exists": bool(exists), "rows": rows})
    return {"status": "ok", "aggregates": result}


@app.post("/admin/refresh-continuous-aggregates")
def refresh_continuous_aggregates(x_api_key: str | None = Header(default=None)) -> Dict[str, Any]:
    _require_admin(x_api_key)
    sql_path = Path(__file__).resolve().parents[1] / "scripts" / "refresh_continuous_aggregates.sql"
    database_url = config.get_asyncpg_dsn()
    try:
        completed = subprocess.run(["psql", database_url, "-v", "ON_ERROR_STOP=1", "-f", str(sql_path)], capture_output=True, text=True, timeout=600)
        if completed.returncode != 0:
            raise HTTPException(status_code=500, detail=completed.stderr[-2000:])
        return {"status": "success", "stdout_tail": completed.stdout[-2000:]}
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail="psql_not_available")
