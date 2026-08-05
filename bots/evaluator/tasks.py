"""Celery tasks for strategy evaluator bot"""
from scripts.celery_app import app
from loguru import logger


def _apply_evaluator_patches() -> None:
    try:
        from bots.evaluator.condition_ext import apply_evaluator_patches

        apply_evaluator_patches()
    except Exception as exc:
        logger.warning(f"evaluator phase1 patches skipped: {exc}")


_apply_evaluator_patches()


@app.task(name='bots.evaluator.tasks.evaluate_strategy', bind=True, max_retries=3)
def evaluate_strategy(self, dna: dict, candles_limit: int = 2000):
    """Evaluate a single genetic strategy DNA."""
    try:
        import asyncio
        import asyncpg
        from bots.evaluator.bot import StrategyEvaluator
        from config.config_manager import config as _config

        _apply_evaluator_patches()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            async def _run_eval_and_save():
                db_pool = None
                try:
                    db_pool = await asyncpg.create_pool(_config.get_asyncpg_dsn())
                    evaluator = StrategyEvaluator(db_pool=db_pool)
                    res = await evaluator.evaluate(dna, candles_limit=candles_limit)
                    dna["fitness_score"] = res.get("fitness_score", 0.0)
                    for key in [
                        "total_profit_pct", "win_rate", "total_trades",
                        "avg_profit_pct", "max_drawdown_pct", "sharpe_ratio",
                        "profit_factor", "avg_duration_min",
                    ]:
                        dna[key] = res.get(key, 0)
                    if dna["fitness_score"] >= 0.0:
                        saved = await evaluator.save_strategy(dna)
                        if saved:
                            if "strategy_hash" not in dna and dna.get("hash"):
                                dna["strategy_hash"] = dna["hash"]
                            await evaluator.save_result(dna)
                            logger.info(f"Saved strategy and performance for {dna.get('symbol')}")
                    return res
                finally:
                    if db_pool:
                        await db_pool.close()

            result = loop.run_until_complete(_run_eval_and_save())
        finally:
            loop.close()

        logger.info(
            f"Evaluated {dna.get('symbol')} [{dna.get('profit_objective')}]: "
            f"fitness={result.get('fitness_score', 0):.4f}"
        )
        return result

    except Exception as exc:
        logger.error(f"evaluate_strategy task failed: {exc}")
        raise self.retry(exc=exc, countdown=30)


@app.task(name='bots.evaluator.tasks.evaluate_population', bind=True, max_retries=2)
def evaluate_population(self, population: list, candles_limit: int = 2000):
    """Evaluate a full population of strategies."""
    try:
        import asyncio
        import asyncpg
        from bots.evaluator.bot import StrategyEvaluator
        from config.config_manager import config as _config

        _apply_evaluator_patches()

        results = []
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            async def _run_pop_eval_and_save():
                db_pool = None
                try:
                    db_pool = await asyncpg.create_pool(_config.get_asyncpg_dsn())
                    evaluator = StrategyEvaluator(db_pool=db_pool)
                    for dna in population:
                        res = await evaluator.evaluate(dna, candles_limit=candles_limit)
                        dna["fitness_score"] = res.get("fitness_score", 0.0)
                        for key in [
                            "total_profit_pct", "win_rate", "total_trades",
                            "avg_profit_pct", "max_drawdown_pct", "sharpe_ratio",
                            "profit_factor", "avg_duration_min",
                        ]:
                            dna[key] = res.get(key, 0)
                        if dna["fitness_score"] >= 0.0:
                            saved = await evaluator.save_strategy(dna)
                            if saved:
                                if "strategy_hash" not in dna and dna.get("hash"):
                                    dna["strategy_hash"] = dna["hash"]
                                await evaluator.save_result(dna)
                        results.append(res)
                finally:
                    if db_pool:
                        await db_pool.close()

            loop.run_until_complete(_run_pop_eval_and_save())
        finally:
            loop.close()

        evaluated = [r for r in results if r["status"] == "ok"]
        logger.info(f"Population evaluated: {len(evaluated)}/{len(population)} ok")
        return {"status": "ok", "results": results, "evaluated_count": len(evaluated)}

    except Exception as exc:
        logger.error(f"evaluate_population task failed: {exc}")
        raise self.retry(exc=exc, countdown=60)
