"""Celery tasks for strategy evaluator bot"""
from scripts.celery_app import app
from loguru import logger


@app.task(name='bots.evaluator.tasks.evaluate_strategy', bind=True, max_retries=3)
def evaluate_strategy(self, dna: dict, candles_limit: int = 2000):
    """
    Evaluate a single genetic strategy DNA.
    Returns fitness metrics dict.
    """
    try:
        import asyncio
        from bots.evaluator.bot import StrategyEvaluator

        evaluator = StrategyEvaluator(db_pool=None)  # db_pool injected at runtime

        loop = asyncio.new_event_loop()
        try:
            result = loop.run_until_complete(
                evaluator.evaluate(dna, candles_limit=candles_limit)
            )
        finally:
            loop.close()

        logger.info(
            f"✅ Evaluated {dna.get('symbol')} [{dna.get('profit_objective')}]: "
            f"fitness={result.get('fitness_score', 0):.4f}"
        )
        return result

    except Exception as exc:
        logger.error(f"evaluate_strategy task failed: {exc}")
        raise self.retry(exc=exc, countdown=30)


@app.task(name='bots.evaluator.tasks.evaluate_population', bind=True, max_retries=2)
def evaluate_population(self, population: list, candles_limit: int = 2000):
    """
    Evaluate a full population of strategies.
    Returns list of results with fitness scores.
    """
    try:
        import asyncio
        from bots.evaluator.bot import StrategyEvaluator

        evaluator = StrategyEvaluator(db_pool=None)
        results = []

        loop = asyncio.new_event_loop()
        try:
            for dna in population:
                result = loop.run_until_complete(
                    evaluator.evaluate(dna, candles_limit=candles_limit)
                )
                results.append(result)
        finally:
            loop.close()

        evaluated = [r for r in results if r["status"] == "ok"]
        logger.info(
            f"✅ Population evaluated: {len(evaluated)}/{len(population)} ok"
        )
        return {"status": "ok", "results": results, "evaluated_count": len(evaluated)}

    except Exception as exc:
        logger.error(f"evaluate_population task failed: {exc}")
        raise self.retry(exc=exc, countdown=60)
