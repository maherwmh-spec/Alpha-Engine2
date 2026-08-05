"""Unified genetic evolution loop (Phase 1 wire-up).

Called from Scientist._async_evolution_loop to avoid bloating the legacy DEAP file.
Adds:
- seed from elite/active DNA across days
- overtrade fitness penalty
- promote single active strategy per (symbol, objective)
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import asyncio
from loguru import logger

from bots.scientist.quality_gates import (
    apply_overtrade_penalty,
    load_elite_seeds,
    promote_active_strategy,
)


async def run_evolution_loop(
    *,
    generator,
    evaluator,
    symbol: str,
    objective: str,
    generations: int,
    population_size: int,
    elite_ratio: float,
    mutation_rate: float,
    min_fitness_to_save: float,
    return_summary: bool = False,
    db_pool=None,
) -> Any:
    log = logger.bind(bot="scientist.unified_loop")
    log.info(
        f"Evolving {symbol} [{objective}] — {generations} gen × {population_size} pop (seeded)"
    )

    pool = db_pool if db_pool is not None else getattr(evaluator, "db_pool", None)

    # ── Cross-day elite memory ──
    elite_seeds: List[Dict[str, Any]] = []
    try:
        elite_seeds = await load_elite_seeds(pool, symbol, objective, limit=max(2, population_size // 3))
        if elite_seeds:
            log.info(f"Loaded {len(elite_seeds)} elite seeds for {symbol} [{objective}]")
    except Exception as exc:
        log.warning(f"elite seed load failed: {exc}")

    try:
        population = generator.generate_population(
            symbol,
            objective,
            size=population_size,
            elite_seeds=elite_seeds or None,
        )
    except TypeError:
        # Backward compatibility if generator lacks elite_seeds kwarg
        population = generator.generate_population(symbol, objective, size=population_size)

    evaluated_pop: List[Dict[str, Any]] = []
    evaluated_count = 0
    failure_reasons: Dict[str, Any] = {}

    def _record_failure(result):
        status = result.get("status", "error") if isinstance(result, dict) else "error"
        if status == "ok":
            return
        key = "no_trades_count" if status == "no_trades" else status
        failure_reasons[key] = failure_reasons.get(key, 0) + 1
        detail = result.get("failure_reason", status) if isinstance(result, dict) else status
        if detail:
            details = failure_reasons.setdefault("details", [])
            if len(details) < 10 and detail not in details:
                details.append(detail)

    for gen in range(1, generations + 1):
        for ind in population:
            ind["generation"] = gen

        tasks = [evaluator.evaluate(ind) for ind in population]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for ind, result in zip(population, results):
            if isinstance(result, Exception):
                ind["fitness_score"] = 0.0
                failure_reasons["error"] = failure_reasons.get("error", 0) + 1
                continue

            evaluated_count += 1
            _record_failure(result)
            raw_fitness = float(result.get("fitness_score", 0.0) or 0.0)
            trades_n = int(result.get("total_trades", 0) or 0)
            ind["fitness_score"] = apply_overtrade_penalty(raw_fitness, trades_n)
            for key in [
                "total_profit_pct",
                "win_rate",
                "total_trades",
                "avg_profit_pct",
                "max_drawdown_pct",
                "sharpe_ratio",
                "profit_factor",
                "avg_duration_min",
            ]:
                ind[key] = result.get(key, 0)

        population.sort(key=lambda x: x.get("fitness_score", 0.0), reverse=True)
        best = population[0].get("fitness_score", 0.0)
        avg = sum(x.get("fitness_score", 0.0) for x in population) / max(len(population), 1)
        log.info(f"Gen {gen:2d}/{generations} | {symbol} [{objective}] | best={best:.4f} avg={avg:.4f}")

        if gen == generations:
            evaluated_pop = population
            break

        elite = generator.select_elite(population, elite_ratio)
        population = generator.breed_next_generation(
            elite, target_size=population_size, mutation_rate=mutation_rate
        )

    # ── Persist elite + promote single active ──
    elite_saved = 0
    best_hash: Optional[str] = None
    best_fit = -1e9

    for ind in evaluated_pop:
        fit = float(ind.get("fitness_score", 0.0) or 0.0)
        if fit < min_fitness_to_save:
            continue
        saved = await evaluator.save_strategy(ind)
        if saved:
            # ensure strategy_hash for performance row
            if "strategy_hash" not in ind and ind.get("hash"):
                ind["strategy_hash"] = ind["hash"]
            await evaluator.save_result(ind)
            elite_saved += 1
            if fit > best_fit and ind.get("hash"):
                best_fit = fit
                best_hash = ind.get("hash")

    if best_hash:
        try:
            await promote_active_strategy(pool, symbol, objective, best_hash)
        except Exception as exc:
            log.warning(f"promote_active failed: {exc}")

    log.info(f"Saved {elite_saved} elite strategies for {symbol} [{objective}]")

    if return_summary:
        return {
            "symbol": symbol,
            "objective": objective,
            "generations_run": generations,
            "population_size": population_size,
            "evaluated_count": evaluated_count,
            "elite_saved": elite_saved,
            "active_hash": best_hash,
            "failure_reasons": failure_reasons,
            "status": "ok" if elite_saved > 0 else "completed_no_elite",
        }
    return elite_saved
