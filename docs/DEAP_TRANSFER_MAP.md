# DEAP Transfer Map — Phase 1 Genetic Engine Unification

**Status:** Binding design document before isolating the legacy DEAP path.
**Date:** 2026-08-05

## Policy

- Single scheduled path: `bots.scientist.tasks.run_genetic_cycle`
- Legacy DEAP (`Scientist.run` / `evolve_strategy`) is **isolated from beat schedule**
- Useful behaviors are re-implemented on Generator + Evaluator path
- Physical deletion of DEAP code is deferred until the unified path is stable

## Active strategy policy

For each `(symbol, profit_objective)`:

| Status | Meaning | Count |
|--------|---------|-------|
| `active` | Used by Strategic Analyzer | **1** (best) |
| `elite` | Seed pool for next evolution | up to 5 |
| `evaluated` | Passed min fitness | retained with cleanup |
| `retired` | Weak / superseded | not used live |

## Function / behavior map

| DEAP / Scientist legacy | Destination on unified path | Notes |
|-------------------------|-----------------------------|-------|
| `_backtest_strategy` | `StrategyEvaluator._simulate_trades` | Prefer Evaluator DNA backtest |
| `_calculate_sharpe` / `_calculate_max_drawdown` | `StrategyEvaluator._compute_metrics` | Already present |
| `_evaluate_individual` fitness mix | `StrategyEvaluator._compute_fitness` | Objective-specific formulas |
| `walk_forward_optimization` | `bots/scientist/quality_gates.py` → `light_wfo` | Simplified 80/20 gate |
| `monte_carlo_simulation` | `quality_gates.py` → `light_monte_carlo` | Reduced simulations |
| `apply_saudi_specific_edges` | `quality_gates.py` → `saudi_edge_score` | Filter/weight only |
| `evolve_strategy` DEAP loop | **Not transferred** — replaced by `run_genetic_cycle` | |
| Fixed 10-gene ranges (RSI/BB/MACD only) | **Not transferred** — `BUILDING_BLOCKS` | Wider search space |
| Hall of Fame | `genetic.strategies` elite + seed-from-elite | Persistent across days |
| Save to `strategies.backtest_results` | Prefer `genetic.strategies` + `genetic.performance` | Legacy table optional |
| `engineer_features` / FinBERT embeddings | Deferred to later phase | Not required for phase 1 |

## Isolation checklist

1. [x] Document this map
2. [x] Remove / stop scheduling `run_scientist` on Celery beat
3. [x] Keep `run_scientist` task as explicit deprecation wrapper (no DEAP schedule)
4. [ ] Runtime seed-from-elite + promote single `active` strategy
5. [ ] Optional move of DEAP-only helpers under `bots/scientist/legacy/` after stable runs

## Cost controls (phase 1)

- Moderate `population_size` / `generations`
- Indicator cache per evaluation window where practical
- Chart patterns (level B) deferred or limited
- Light WFO + light Monte Carlo before promote-to-elite/active
