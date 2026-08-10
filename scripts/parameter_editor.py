"""
Phase 6 — ParameterEditor
Validate, apply, and resolve effective strategy parameters without code changes.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger
from sqlalchemy import text

from config.config_manager import config
from scripts.database import db


ALLOWED_OBJECTIVES = {
    "scalping",
    "short_swings",
    "medium_trends",
    "momentum",
}

# key -> (type, min, max)  type: float | bool | str | int
PARAM_SPEC: Dict[str, Dict[str, Any]] = {
    "stop_loss_pct": {"type": "float", "min": 0.005, "max": 0.15},
    "take_profit_pct": {"type": "float", "min": 0.005, "max": 0.30},
    "min_confidence": {"type": "float", "min": 0.3, "max": 0.99},
    "alert_cooldown_minutes": {"type": "int", "min": 5, "max": 240},
    "enabled": {"type": "bool"},
    "objective": {"type": "str", "choices": sorted(ALLOWED_OBJECTIVES)},
    "trailing": {"type": "bool"},
}


class ParameterEditor:
    """Manage parameter overrides with safety bounds and audit trail."""

    def __init__(self):
        self.logger = logger.bind(service="parameter_editor")

    def allowed_params(self) -> Dict[str, Any]:
        return {k: {kk: vv for kk, vv in v.items() if kk != "choices" or True} for k, v in PARAM_SPEC.items()}

    def defaults(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        sl = config.get("profit_objectives.short_swings.stoploss_range", [-0.015, -0.03])
        tp = config.get("profit_objectives.short_swings.roi_range", [0.03, 0.08])
        return {
            "stop_loss_pct": abs(float(sl[0])) if isinstance(sl, list) and sl else 0.02,
            "take_profit_pct": float(tp[0]) if isinstance(tp, list) and tp else 0.04,
            "min_confidence": float(config.get("strategic_analyzer.min_confidence_generic", 0.65)),
            "alert_cooldown_minutes": int(config.get("bots.analyze.alert_cooldown_minutes", 30)),
            "enabled": True,
            "objective": "short_swings",
            "trailing": bool(
                config.get("profit_objectives.short_swings.trailing_stop", True)
            ),
        }

    def effective_params(self, symbol: str) -> Dict[str, Any]:
        symbol = str(symbol).strip().upper()
        out = self.defaults(symbol)
        # global overrides
        out.update(self._load_overrides(scope="global", symbol=None))
        # symbol overrides
        out.update(self._load_overrides(scope="symbol", symbol=symbol))
        return out

    def list_symbol_overrides(self, symbol: str) -> Dict[str, Any]:
        symbol = str(symbol).strip().upper()
        return self._load_overrides(scope="symbol", symbol=symbol)

    def list_overridden_symbols(self, limit: int = 30) -> List[str]:
        with db.get_session() as session:
            rows = session.execute(
                text(
                    """
                    SELECT DISTINCT symbol
                    FROM strategies.parameter_overrides
                    WHERE scope = 'symbol' AND enabled = TRUE AND symbol IS NOT NULL
                    ORDER BY symbol
                    LIMIT :lim
                    """
                ),
                {"lim": limit},
            ).fetchall()
        return [r[0] for r in rows]

    def apply(
        self,
        symbol: str,
        updates: Dict[str, Any],
        source: str = "telegram",
        updated_by: str = "telegram",
    ) -> Dict[str, Any]:
        """Apply key=value updates. Returns {ok, applied, errors}."""
        symbol = str(symbol).strip().upper()
        applied: Dict[str, Any] = {}
        errors: List[str] = []

        if not updates:
            return {"ok": False, "applied": {}, "errors": ["لا توجد معاملات للتعديل"]}

        for key, raw in updates.items():
            key = str(key).strip()
            ok, val, err = self._validate(key, raw)
            if not ok:
                errors.append(err or f"رفض {key}")
                continue
            old = self._get_one(symbol, key)
            self._upsert(symbol, key, val, updated_by=updated_by)
            self._audit(symbol, key, old, val, action="set", source=source)
            applied[key] = val

        return {
            "ok": len(applied) > 0 and len(errors) == 0,
            "partial": len(applied) > 0 and len(errors) > 0,
            "applied": applied,
            "errors": errors,
            "effective": self.effective_params(symbol),
        }

    def reset(self, symbol: str, source: str = "telegram") -> Dict[str, Any]:
        symbol = str(symbol).strip().upper()
        existing = self.list_symbol_overrides(symbol)
        with db.get_session() as session:
            session.execute(
                text(
                    """
                    DELETE FROM strategies.parameter_overrides
                    WHERE scope = 'symbol' AND symbol = :s
                    """
                ),
                {"s": symbol},
            )
            session.commit()
        for k, v in existing.items():
            self._audit(symbol, k, v, None, action="reset", source=source)
        return {"ok": True, "cleared": list(existing.keys()), "effective": self.effective_params(symbol)}

    def parse_kv_args(self, args: List[str]) -> Tuple[Optional[str], Dict[str, str], Optional[str]]:
        """
        Parse: SYMBOL k=v k=v ...  or SYMBOL reset
        Returns (symbol, updates, special) where special is 'reset' or None.
        """
        if not args:
            return None, {}, None
        symbol = args[0].strip().upper()
        rest = args[1:]
        if rest and rest[0].lower() == "reset":
            return symbol, {}, "reset"
        updates: Dict[str, str] = {}
        for token in rest:
            if "=" not in token:
                continue
            k, v = token.split("=", 1)
            updates[k.strip()] = v.strip()
        return symbol, updates, None

    # ── internals ────────────────────────────────────────────────────────────
    def _validate(self, key: str, raw: Any) -> Tuple[bool, Any, Optional[str]]:
        if key not in PARAM_SPEC:
            return False, None, f"مفتاح غير مسموح: {key}"
        spec = PARAM_SPEC[key]
        t = spec["type"]
        try:
            if t == "float":
                val = float(raw)
                if val < spec["min"] or val > spec["max"]:
                    return False, None, f"{key} خارج الحد [{spec['min']}, {spec['max']}]"
                return True, val, None
            if t == "int":
                val = int(float(raw))
                if val < spec["min"] or val > spec["max"]:
                    return False, None, f"{key} خارج الحد [{spec['min']}, {spec['max']}]"
                return True, val, None
            if t == "bool":
                if isinstance(raw, bool):
                    return True, raw, None
                s = str(raw).strip().lower()
                if s in ("1", "true", "yes", "on", "نعم"):
                    return True, True, None
                if s in ("0", "false", "no", "off", "لا"):
                    return True, False, None
                return False, None, f"{key} يجب أن يكون true/false"
            if t == "str":
                val = str(raw).strip().lower()
                choices = spec.get("choices") or []
                if choices and val not in choices:
                    return False, None, f"{key} يجب أن يكون أحد: {', '.join(choices)}"
                return True, val, None
        except Exception as exc:
            return False, None, f"قيمة غير صالحة لـ {key}: {exc}"
        return False, None, f"نوع غير مدعوم لـ {key}"

    def _load_overrides(self, scope: str, symbol: Optional[str]) -> Dict[str, Any]:
        try:
            with db.get_session() as session:
                if scope == "global":
                    rows = session.execute(
                        text(
                            """
                            SELECT param_key, param_value
                            FROM strategies.parameter_overrides
                            WHERE scope = 'global' AND enabled = TRUE
                            """
                        )
                    ).fetchall()
                else:
                    rows = session.execute(
                        text(
                            """
                            SELECT param_key, param_value
                            FROM strategies.parameter_overrides
                            WHERE scope = 'symbol' AND enabled = TRUE AND symbol = :s
                            """
                        ),
                        {"s": symbol},
                    ).fetchall()
            out: Dict[str, Any] = {}
            for k, v in rows:
                if isinstance(v, (dict, list)):
                    # JSONB sometimes returns already-decoded; unwrap scalar
                    if isinstance(v, dict) and "value" in v and len(v) == 1:
                        out[k] = v["value"]
                    else:
                        out[k] = v
                else:
                    out[k] = v
            return out
        except Exception as exc:
            self.logger.debug(f"load overrides failed: {exc}")
            return {}

    def _get_one(self, symbol: str, key: str) -> Any:
        try:
            with db.get_session() as session:
                row = session.execute(
                    text(
                        """
                        SELECT param_value FROM strategies.parameter_overrides
                        WHERE scope = 'symbol' AND symbol = :s AND param_key = :k
                        LIMIT 1
                        """
                    ),
                    {"s": symbol, "k": key},
                ).fetchone()
            if not row:
                return None
            v = row[0]
            if isinstance(v, dict) and "value" in v and len(v) == 1:
                return v["value"]
            return v
        except Exception:
            return None

    def _upsert(self, symbol: str, key: str, value: Any, updated_by: str = "telegram") -> None:
        payload = json.dumps(value)
        with db.get_session() as session:
            session.execute(
                text(
                    """
                    INSERT INTO strategies.parameter_overrides
                        (scope, symbol, strategy_ref, param_key, param_value, enabled, updated_by, updated_at)
                    VALUES
                        ('symbol', :s, NULL, :k, CAST(:v AS JSONB), TRUE, :by, NOW())
                    ON CONFLICT (scope, COALESCE(symbol, ''), COALESCE(strategy_ref, ''), param_key)
                    DO UPDATE SET
                        param_value = EXCLUDED.param_value,
                        enabled = TRUE,
                        updated_by = EXCLUDED.updated_by,
                        updated_at = NOW()
                    """
                ),
                {"s": symbol, "k": key, "v": payload, "by": updated_by},
            )
            session.commit()

    def _audit(
        self,
        symbol: Optional[str],
        key: str,
        old: Any,
        new: Any,
        action: str,
        source: str,
    ) -> None:
        try:
            with db.get_session() as session:
                session.execute(
                    text(
                        """
                        INSERT INTO strategies.parameter_audit
                            (symbol, param_key, old_value, new_value, action, source, created_at)
                        VALUES
                            (:s, :k, CAST(:old AS JSONB), CAST(:new AS JSONB), :a, :src, NOW())
                        """
                    ),
                    {
                        "s": symbol,
                        "k": key,
                        "old": json.dumps(old) if old is not None else None,
                        "new": json.dumps(new) if new is not None else None,
                        "a": action,
                        "src": source,
                    },
                )
                session.commit()
        except Exception as exc:
            self.logger.warning(f"audit failed: {exc}")
