"""
Phase 7 — SelfTrainer (v1)
Reads weekly recommendations; suggests or optionally applies safe parameter bumps.
No FreqAI/GPU mandatory path in v1.
"""
from __future__ import annotations

from typing import Any, Dict, List

from loguru import logger

from config.config_manager import config


class SelfTrainerBot:
    def __init__(self):
        self.name = "self_trainer"
        self.logger = logger.bind(bot=self.name)
        self.auto_apply = bool(config.get("bots.self_trainer.auto_apply", False))
        self.bump = float(config.get("bots.self_trainer.bump_min_confidence", 0.05))
        self.max_symbols = int(config.get("bots.self_trainer.max_symbols_adjust", 5))

    def run(
        self,
        week_id: str = "",
        recommendations: List[Dict[str, Any]] | None = None,
        has_data: bool = True,
    ) -> Dict[str, Any]:
        recommendations = recommendations or []
        notes: List[str] = []

        if not has_data:
            return {
                "status": "skipped",
                "week_id": week_id,
                "notes": ["الحالة التدريبية: skipped (لا بيانات)"],
            }

        if not recommendations:
            return {
                "status": "suggested",
                "week_id": week_id,
                "notes": ["الحالة التدريبية: suggested (لا توصيات)"],
            }

        if not self.auto_apply:
            notes.append("الحالة التدريبية: suggested (auto_apply=false)")
            for r in recommendations[: self.max_symbols]:
                notes.append(
                    f"اقتراح: {r.get('symbol')} → {r.get('action')} ({r.get('reason')})"
                )
            return {"status": "suggested", "week_id": week_id, "notes": notes}

        # auto_apply path — only min_confidence bumps via ParameterEditor bounds
        applied = []
        try:
            from scripts.parameter_editor import ParameterEditor

            ed = ParameterEditor()
            for r in recommendations[: self.max_symbols]:
                if r.get("action") != "raise_min_confidence":
                    continue
                symbol = str(r["symbol"]).upper()
                eff = ed.effective_params(symbol)
                current = float(eff.get("min_confidence", 0.65))
                new_val = min(0.99, current + float(r.get("bump", self.bump)))
                result = ed.apply(
                    symbol,
                    {"min_confidence": new_val},
                    source="self_trainer",
                    updated_by="self_trainer",
                )
                if result.get("applied"):
                    applied.append(f"{symbol}: min_confidence {current:.2f}→{new_val:.2f}")
        except Exception as exc:
            self.logger.error(f"auto_apply failed: {exc}")
            return {
                "status": "error",
                "week_id": week_id,
                "notes": [f"الحالة التدريبية: error — {exc}"],
            }

        notes.append("الحالة التدريبية: applied")
        notes.extend(applied or ["لم يُطبَّق أي تعديل"])
        return {"status": "applied", "week_id": week_id, "applied": applied, "notes": notes}
