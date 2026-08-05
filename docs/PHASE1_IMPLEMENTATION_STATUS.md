# Phase 1 Implementation Status (2026-08-05)

## Completed

1. `docs/DEAP_TRANSFER_MAP.md` — transfer map + active policy
2. `bots/scientist/quality_gates.py` — WFO/MC helpers, overtrade penalty, load_elite_seeds, promote_active_strategy
3. `bots/scientist/unified_loop.py` — seeded evolution loop + promote active
4. `bots/scientist/tasks.py` — patches Scientist._async_evolution_loop → unified_loop; DEAP run_scientist deprecated
5. `scripts/celery_app.py` — only genetic-engine-run (18:00); DEAP hourly schedule removed
6. `bots/generator/building_blocks.py` + `bot.py` — 21 level-A blocks + elite_seeds
7. `bots/evaluator/indicators_ext.py` + `condition_ext.py` + `tasks.py` — extended indicators/conditions patched on load

## Wire-up checklist

- [x] Call load_elite_seeds before generate_population
- [x] promote_active_strategy after best save
- [x] Evaluator computes/evaluates expanded indicators
- [x] apply_overtrade_penalty on fitness

## Deploy note

```bash
git pull origin master
# restart celery worker + beat so patches and schedule load
```
