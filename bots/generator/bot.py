"""
bots/generator/bot.py
═══════════════════════════════════════════════════════════════
Alpha-Engine2 — المحرك الجيني لتوليد استراتيجيات التداول

الفلسفة: "المُكتشف" وليس "المُنتقي"
Phase 1: BUILDING_BLOCKS expanded via building_blocks.py; elite seeding supported.
═══════════════════════════════════════════════════════════════
"""
from __future__ import annotations

import hashlib
import json
import random
import copy
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger as _loguru_logger

def get_logger(name):
    return _loguru_logger.bind(bot=name)

from bots.generator.building_blocks import (
    BUILDING_BLOCKS,
    OBJECTIVE_PARAMS,
    OBJECTIVE_TO_RISK_BOX,
    PROFIT_OBJECTIVES,
)

# Re-export for callers that imported from this module
__all__ = [
    "GeneticGenerator",
    "BUILDING_BLOCKS",
    "OBJECTIVE_PARAMS",
    "OBJECTIVE_TO_RISK_BOX",
    "PROFIT_OBJECTIVES",
]


class GeneticGenerator:
    """المحرك الجيني لتوليد وتطوير استراتيجيات التداول."""

    def __init__(self):
        self.logger = get_logger("GeneticGenerator")
        self._rng = random.Random()

    def generate_population(
        self,
        symbol: str,
        profit_objective: str,
        size: int = 50,
        seed: Optional[int] = None,
        elite_seeds: Optional[List[Dict[str, Any]]] = None,
    ) -> List[Dict[str, Any]]:
        if seed is not None:
            self._rng.seed(seed)

        if profit_objective not in PROFIT_OBJECTIVES:
            raise ValueError(
                f"profit_objective must be one of {PROFIT_OBJECTIVES}, got '{profit_objective}'"
            )

        population: List[Dict[str, Any]] = []

        # Seed from prior elite/active DNA (cross-day memory)
        if elite_seeds:
            for raw in elite_seeds:
                if not isinstance(raw, dict):
                    continue
                dna = copy.deepcopy(raw)
                dna["symbol"] = symbol
                dna["profit_objective"] = profit_objective
                dna["risk_box"] = OBJECTIVE_TO_RISK_BOX[profit_objective]
                dna["name"] = dna.get("name") or f"Seeded_{self._short_id()}"
                # Drop runtime metrics so hash reflects strategy structure
                for k in list(dna.keys()):
                    if k in {
                        "fitness_score",
                        "total_profit_pct",
                        "win_rate",
                        "total_trades",
                        "status",
                    }:
                        dna.pop(k, None)
                dna["hash"] = self.compute_hash(dna)
                population.append(dna)
                if len(population) >= max(1, size // 3):
                    break

        while len(population) < size:
            population.append(self._generate_random_dna(symbol, profit_objective))

        self.logger.info(
            f"Generated population: {len(population)} strategies for {symbol} [{profit_objective}] "
            f"(seeded={min(len(elite_seeds or []), max(1, size // 3))})"
        )
        return population[:size]

    def _generate_random_dna(self, symbol: str, profit_objective: str) -> Dict[str, Any]:
        obj_params = OBJECTIVE_PARAMS[profit_objective]
        risk_box = OBJECTIVE_TO_RISK_BOX[profit_objective]

        stoploss = round(self._rng.uniform(*obj_params["stoploss_range"]), 4)
        roi_min, roi_max = obj_params["roi_range"]
        roi_target = round(self._rng.uniform(roi_min, roi_max), 4)
        roi = {
            "0": roi_target,
            "60": round(roi_target * 0.6, 4),
            "120": round(roi_target * 0.3, 4),
        }

        n_entry = self._rng.randint(1, 3)
        entry_conditions = self._sample_conditions("entry", n_entry)
        n_exit = self._rng.randint(1, 2)
        exit_conditions = self._sample_conditions("exit", n_exit)

        # Discourage PSAR/SUPERTREND as the only entry condition
        if len(entry_conditions) == 1 and entry_conditions[0].get("indicator") in {"PSAR", "SUPERTREND"}:
            extra = self._sample_conditions("entry", 1)
            if extra and extra[0].get("indicator") not in {"PSAR", "SUPERTREND"}:
                entry_conditions.append(extra[0])

        dna: Dict[str, Any] = {
            "name": f"GeneticStrategy_{self._short_id()}",
            "symbol": symbol,
            "profit_objective": profit_objective,
            "risk_box": risk_box,
            "timeframe": obj_params["timeframe"],
            "stoploss": stoploss,
            "roi": roi,
            "trailing_stop": obj_params["trailing_stop"],
            "entry_conditions": entry_conditions,
            "exit_conditions": exit_conditions,
        }
        dna["hash"] = self.compute_hash(dna)
        return dna

    def _sample_conditions(self, condition_type: str, count: int) -> List[Dict[str, Any]]:
        ops_key = f"{condition_type}_ops"
        values_key = f"{condition_type}_values"

        eligible = [name for name, block in BUILDING_BLOCKS.items() if ops_key in block]
        chosen_indicators = self._rng.sample(eligible, min(count, len(eligible)))

        conditions = []
        for indicator_name in chosen_indicators:
            block = BUILDING_BLOCKS[indicator_name]
            cond: Dict[str, Any] = {"indicator": indicator_name}

            for param_name, param_spec in block["params"].items():
                if isinstance(param_spec["min"], float):
                    # discrete-ish float via uniform
                    val = round(self._rng.uniform(param_spec["min"], param_spec["max"]), 2)
                else:
                    val = self._rng.randrange(
                        param_spec["min"],
                        param_spec["max"] + 1,
                        param_spec.get("step", 1),
                    )
                cond[param_name] = val

            cond["operator"] = self._rng.choice(block[ops_key])
            if values_key in block:
                cond["value"] = self._rng.choice(block[values_key])
            conditions.append(cond)
        return conditions

    def select_elite(
        self,
        population: List[Dict[str, Any]],
        elite_ratio: float = 0.2,
    ) -> List[Dict[str, Any]]:
        sorted_pop = sorted(
            population,
            key=lambda x: x.get("fitness_score", 0.0),
            reverse=True,
        )
        elite_count = max(2, int(len(sorted_pop) * elite_ratio))
        elite = sorted_pop[:elite_count]
        self.logger.info(
            f"Elite selected: {len(elite)}/{len(population)} "
            f"(best fitness={elite[0].get('fitness_score', 0):.4f})"
        )
        return elite

    def crossover(
        self,
        parent_a: Dict[str, Any],
        parent_b: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        child_a = copy.deepcopy(parent_a)
        child_b = copy.deepcopy(parent_b)

        if self._rng.random() > 0.5:
            child_a["entry_conditions"] = copy.deepcopy(parent_b["entry_conditions"])
            child_b["entry_conditions"] = copy.deepcopy(parent_a["entry_conditions"])
        if self._rng.random() > 0.5:
            child_a["exit_conditions"] = copy.deepcopy(parent_b["exit_conditions"])
            child_b["exit_conditions"] = copy.deepcopy(parent_a["exit_conditions"])
        if self._rng.random() > 0.7:
            child_a["stoploss"], child_b["stoploss"] = (
                child_b["stoploss"],
                child_a["stoploss"],
            )

        for child, pa, pb in [
            (child_a, parent_a, parent_b),
            (child_b, parent_b, parent_a),
        ]:
            child["parent_a_hash"] = pa.get("hash", "")
            child["parent_b_hash"] = pb.get("hash", "")
            child["mutation_count"] = 0
            child["name"] = f"GeneticStrategy_{self._short_id()}"
            child["hash"] = self.compute_hash(child)
        return child_a, child_b

    def mutate(self, individual: Dict[str, Any], mutation_rate: float = 0.15) -> Dict[str, Any]:
        mutant = copy.deepcopy(individual)
        mutations_applied = 0

        for cond in mutant.get("entry_conditions", []):
            if self._rng.random() < mutation_rate:
                self._mutate_condition(cond)
                mutations_applied += 1
        for cond in mutant.get("exit_conditions", []):
            if self._rng.random() < mutation_rate:
                self._mutate_condition(cond)
                mutations_applied += 1
        if self._rng.random() < mutation_rate:
            obj_params = OBJECTIVE_PARAMS[mutant["profit_objective"]]
            mutant["stoploss"] = round(self._rng.uniform(*obj_params["stoploss_range"]), 4)
            mutations_applied += 1
        if self._rng.random() < mutation_rate * 0.5:
            new_cond = self._sample_conditions("entry", 1)
            if new_cond and mutant.get("entry_conditions"):
                idx = self._rng.randrange(len(mutant["entry_conditions"]))
                mutant["entry_conditions"][idx] = new_cond[0]
                mutations_applied += 1

        mutant["mutation_count"] = individual.get("mutation_count", 0) + mutations_applied
        mutant["hash"] = self.compute_hash(mutant)
        return mutant

    def _mutate_condition(self, cond: Dict[str, Any]) -> None:
        indicator_name = cond.get("indicator")
        if indicator_name not in BUILDING_BLOCKS:
            return
        block = BUILDING_BLOCKS[indicator_name]
        param_names = list(block["params"].keys())
        if not param_names:
            return
        param_name = self._rng.choice(param_names)
        spec = block["params"][param_name]
        if isinstance(spec["min"], float):
            cond[param_name] = round(self._rng.uniform(spec["min"], spec["max"]), 2)
        else:
            cond[param_name] = self._rng.randrange(
                spec["min"], spec["max"] + 1, spec.get("step", 1)
            )

    def breed_next_generation(
        self,
        elite: List[Dict[str, Any]],
        target_size: int = 50,
        mutation_rate: float = 0.15,
    ) -> List[Dict[str, Any]]:
        next_gen: List[Dict[str, Any]] = list(elite)
        while len(next_gen) < target_size:
            if len(elite) < 2:
                break
            pa, pb = self._rng.sample(elite, 2)
            child_a, child_b = self.crossover(pa, pb)
            child_a = self.mutate(child_a, mutation_rate)
            child_b = self.mutate(child_b, mutation_rate)
            next_gen.append(child_a)
            if len(next_gen) < target_size:
                next_gen.append(child_b)
        return next_gen[:target_size]

    @staticmethod
    def compute_hash(dna: Dict[str, Any]) -> str:
        dna_copy = {k: v for k, v in dna.items() if k != "hash"}
        canonical = json.dumps(dna_copy, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(canonical.encode()).hexdigest()

    def _short_id(self) -> str:
        return "%06x" % self._rng.randint(0, 0xFFFFFF)

    def dna_to_json(self, dna: Dict[str, Any]) -> str:
        return json.dumps(dna, indent=2, ensure_ascii=False)

    def validate_dna(self, dna: Dict[str, Any]) -> Tuple[bool, str]:
        required_keys = [
            "name",
            "symbol",
            "profit_objective",
            "risk_box",
            "timeframe",
            "stoploss",
            "roi",
            "entry_conditions",
            "exit_conditions",
        ]
        for key in required_keys:
            if key not in dna:
                return False, f"Missing required key: '{key}'"
        if dna["profit_objective"] not in PROFIT_OBJECTIVES:
            return False, f"Invalid profit_objective: '{dna['profit_objective']}'"
        if not dna["entry_conditions"]:
            return False, "entry_conditions cannot be empty"
        if dna["stoploss"] >= 0:
            return False, f"stoploss must be negative, got {dna['stoploss']}"
        return True, ""
