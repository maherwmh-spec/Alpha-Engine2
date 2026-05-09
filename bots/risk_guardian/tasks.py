
"""Risk Guardian MVP: validates recent BUY signals against configured risk boxes."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Dict, Tuple

from loguru import logger
from sqlalchemy import text

from config.config_manager import config
from scripts.celery_app import app
from scripts.database import db


def _risk_box_for_strategy(strategy_name: str) -> Tuple[str | None, Dict]:
    objective = strategy_name.replace("Genetic_", "") if strategy_name else ""
    for name, box in (config.get("risk_boxes", {}) or {}).items():
        if box.get("linked_objective") == objective:
            return name, box
    return None, {}


@app.task(name="bots.risk_guardian.tasks.run_risk_guardian")
def run_risk_guardian(*args, **kwargs):
    """Approve/reject recent signals in metadata without changing legacy schema."""
    if not config.is_bot_enabled("risk_guardian"):
        return {"status": "disabled", "bot": "risk_guardian"}
    reviewed = approved = rejected = 0
    try:
        with db.get_session() as session:
            rows = session.execute(text("""
                SELECT id, strategy_name, symbol, signal_type, confidence, metadata
                FROM strategies.signals
                WHERE timestamp >= :since
                  AND signal_type = 'BUY'
                  AND COALESCE(metadata->>'risk_status', '') = ''
                ORDER BY timestamp DESC
                LIMIT 200
            """), {"since": datetime.now(timezone.utc) - timedelta(days=2)}).fetchall()
            for row in rows:
                reviewed += 1
                signal_id, strategy_name, symbol, signal_type, confidence, metadata = row
                box_name, box = _risk_box_for_strategy(strategy_name)
                reason = "approved"
                risk_status = "approved"
                if not box_name:
                    risk_status, reason = "rejected", "no_matching_risk_box"
                elif float(confidence or 0) <= 0:
                    risk_status, reason = "rejected", "invalid_confidence"
                else:
                    open_trades = session.execute(text("""
                        SELECT COUNT(*) FROM strategies.signals
                        WHERE strategy_name = :strategy_name AND signal_type = 'BUY'
                          AND timestamp >= NOW() - INTERVAL '7 days'
                          AND COALESCE(metadata->>'risk_status', 'approved') = 'approved'
                    """), {"strategy_name": strategy_name}).scalar() or 0
                    if open_trades >= int(box.get("max_open_trades", 999999)):
                        risk_status, reason = "rejected", "max_open_trades_exceeded"
                meta = metadata if isinstance(metadata, dict) else (json.loads(metadata) if metadata else {})
                meta.update({
                    "risk_status": risk_status,
                    "risk_reason": reason,
                    "risk_box": box_name,
                    "risk_reviewed_at": datetime.now(timezone.utc).isoformat(),
                    "max_allocation_pct": box.get("max_allocation_pct"),
                })
                session.execute(text("UPDATE strategies.signals SET metadata = CAST(:metadata AS jsonb) WHERE id = :id"), {"id": signal_id, "metadata": json.dumps(meta, ensure_ascii=False)})
                approved += int(risk_status == "approved")
                rejected += int(risk_status == "rejected")
            session.commit()
        logger.info("risk_guardian_complete reviewed={} approved={} rejected={}", reviewed, approved, rejected)
        return {"status": "success", "reviewed": reviewed, "approved": approved, "rejected": rejected}
    except Exception as exc:
        logger.error("risk_guardian_failed error={}", exc)
        raise
