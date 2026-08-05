# Phase 1 Implementation Status (2026-08-05)

## Completed in repository

1. **`docs/DEAP_TRANSFER_MAP.md`** — mandatory transfer map before DEAP isolation.
2. **`bots/scientist/quality_gates.py`** — light WFO, light Monte Carlo, Saudi edge score, overtrade penalty, `load_elite_seeds`, `promote_active_strategy`.
3. **`bots/scientist/tasks.py`** — `run_scientist` deprecated and redirects to unified `run_genetic_cycle`.
4. **`scripts/celery_app.py`** — removed beat entry `scientist-run` (legacy DEAP hourly). Only `genetic-engine-run` at 18:00.
5. **`bots/generator/building_blocks.py`** — expanded level-A blocks (21 indicators) including PSAR/SUPERTREND with role hints.
6. **`bots/generator/bot.py`** — imports expanded blocks; `generate_population(..., elite_seeds=)`; discourages PSAR/SUPERTREND as sole entry.
7. **`bots/evaluator/indicators_ext.py`** — pure pandas helpers for new indicators.

## Active strategy policy (enforced via quality_gates helpers)

- One `active` per `(symbol, profit_objective)` via `promote_active_strategy`.
- Elite retained for cross-day seeding via `load_elite_seeds`.

## Remaining wire-up (next micro-commits)

1. Call `load_elite_seeds` inside `Scientist._async_evolution_loop` before `generate_population`.
2. After save of best DNA, call `promote_active_strategy`.
3. Extend `StrategyEvaluator._compute_indicators` / `_evaluate_condition` to use `indicators_ext` for WILLR, MFI, STOCH_RSI, PSAR, SUPERTREND, KELTNER, DONCHIAN, OBV_SLOPE, CMF.
4. Apply `apply_overtrade_penalty` on fitness when total_trades is high.

## Cost controls already reflected

- Moderate default generations/population in task.
- Patterns level-B not added yet.
- Light gates (not full 500 MC / multi-window WFO).
