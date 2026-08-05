"""
Quality gates for the unified genetic cycle (Phase 1).

Transferred behaviors from legacy DEAP Scientist path:
- light walk-forward style holdout check
- light Monte Carlo robustness check
- Saudi-specific edge score (optional weight/filter)
- promote single active strategy per (symbol, profit_objective)
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import numpy as np
from loguru import logger


def light_wfo(
    fitness_full: float,
    fitness_holdout: Optional[float] = None,
    min_ratio: float = 0.70,
) -> Dict[str, Any]:
    """Accept if holdout fitness is at least min_ratio of full-sample fitness.

    When holdout is not provided, gate passes (caller may skip expensive re-eval).
    """
    if fitness_holdout is None:
        return {"passed": True, "reason": "wfo_skipped_no_holdout", "ratio": 1.0}
    if fitness_full <= 0:
        return {"passed": fitness_holdout >= 0, "reason": "non_positive_full", "ratio": 0.0}
    ratio = float(fitness_holdout) / float(fitness_full)
    passed = ratio >= min_ratio
    return {
        "passed": passed,
        "reason": "ok" if passed else f"wfo_degradation ratio={ratio:.3f} < {min_ratio}",
        "ratio": round(ratio, 4),
    }


def light_monte_carlo(
    trade_pnls: List[float],
    n_simulations: int = 80,
    pass_rate_threshold: float = 0.55,
    seed: int = 42,
) -> Dict[str, Any]:
    """Bootstrap trade PnLs; require a minimum fraction of positive mean resamples."""
    if not trade_pnls or len(trade_pnls) < 5:
        return {
            "passed": False,
            "reason": "insufficient_trades_for_mc",
            "pass_rate": 0.0,
            "robust": False,
        }
    rng = np.random.default_rng(seed)
    arr = np.asarray(trade_pnls, dtype=float)
    means = []
    for _ in range(n_simulations):
        sample = rng.choice(arr, size=len(arr), replace=True)
        means.append(float(np.mean(sample)))
    means_arr = np.asarray(means)
    pass_rate = float(np.mean(means_arr > 0))
    robust = pass_rate >= pass_rate_threshold
    return {
        "passed": robust,
        "reason": "ok" if robust else f"mc_pass_rate={pass_rate:.2f} < {pass_rate_threshold}",
        "pass_rate": round(pass_rate, 4),
        "robust": robust,
        "mean_of_means": round(float(np.mean(means_arr)), 6),
    }


def saudi_edge_score(
    opening_strength: float = 0.5,
    closing_score: float = 0.5,
    oil_correlation: float = 1.0,
) -> Dict[str, Any]:
    """Lightweight Saudi market edge score in [0, 1]."""
    score = (
        float(opening_strength) * 0.3
        + float(closing_score) * 0.3
        + float(oil_correlation) * 0.4
    )
    score = max(0.0, min(1.0, score))
    return {"saudi_edge_score": round(score, 4), "passed": score >= 0.35}


def apply_overtrade_penalty(fitness: float, total_trades: int, soft_cap: int = 80) -> float:
    """Penalize strategies that churn excessively (helps PSAR/SuperTrend noise)."""
    if total_trades <= soft_cap:
        return float(fitness)
    excess = total_trades - soft_cap
    penalty = min(0.35, excess * 0.002)
    return float(fitness) - penalty


def trend_indicator_safe_roles() -> Dict[str, str]:
    """Hints for Generator/Evaluator documentation."""
    return {
        "PSAR": "trend_filter_or_exit_not_alone",
        "SUPERTREND": "trend_filter_or_exit_not_alone",
        "ADX": "regime_filter",
    }


async def promote_active_strategy(
    db_pool,
    symbol: str,
    profit_objective: str,
    strategy_hash: str,
) -> bool:
    """Ensure only one active strategy per (symbol, profit_objective)."""
    if db_pool is None or not strategy_hash:
        return False
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE genetic.strategies
                SET status = 'elite'
                WHERE symbol = $1
                  AND profit_objective = $2
                  AND status = 'active'
                  AND strategy_hash <> $3
                """,
                symbol,
                profit_objective,
                strategy_hash,
            )
            await conn.execute(
                """
                UPDATE genetic.strategies
                SET status = 'active'
                WHERE strategy_hash = $1
                """,
                strategy_hash,
            )
        logger.info(
            "promoted_active symbol={} objective={} hash={}",
            symbol,
            profit_objective,
            strategy_hash[:12],
        )
        return True
    except Exception as exc:
        logger.error("promote_active_failed error={}", exc)
        return False


async def load_elite_seeds(
    db_pool,
    symbol: str,
    profit_objective: str,
    limit: int = 8,
) -> List[Dict[str, Any]]:
    """Load prior elite/active DNA rows to seed the next population."""
    if db_pool is None:
        return []
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT dna, fitness_score, strategy_hash, status
                FROM genetic.strategies
                WHERE symbol = $1
                  AND profit_objective = $2
                  AND status IN ('elite', 'active', 'evaluated')
                  AND fitness_score > 0
                ORDER BY
                  CASE status WHEN 'active' THEN 0 WHEN 'elite' THEN 1 ELSE 2 END,
                  fitness_score DESC
                LIMIT $3
                """,
                symbol,
                profit_objective,
                limit,
            )
        import json

        seeds: List[Dict[str, Any]] = []
        for row in rows:
            dna = row["dna"]
            if isinstance(dna, str):
                dna = json.loads(dna)
            if isinstance(dna, dict):
                dna = dict(dna)
                dna["fitness_score"] = float(row["fitness_score"] or 0.0)
                dna["hash"] = row["strategy_hash"] or dna.get("hash")
                seeds.append(dna)
        return seeds
    except Exception as exc:
        logger.warning("load_elite_seeds_failed symbol={} objective={} error={}", symbol, profit_objective, exc)
        return []
