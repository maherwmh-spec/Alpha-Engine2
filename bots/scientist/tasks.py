"""Celery tasks for scientist bot"""
from scripts.celery_app import app
from loguru import logger


@app.task(name='bots.scientist.tasks.run_scientist', bind=True, max_retries=3)
def run_scientist(self):
    """Run scientist bot task (DEAP-based legacy evolution)"""
    try:
        from bots.scientist.bot import Scientist
        scientist = Scientist()
        scientist.run()
    except Exception as exc:
        logger.error(f"scientist task failed: {exc}")
        raise self.retry(exc=exc, countdown=60)


@app.task(name='bots.scientist.tasks.run_genetic_cycle', bind=True, max_retries=3)
def run_genetic_cycle(
    self,
    symbols=None,
    generations: int = 10,
    population_size: int = 30,
):
    """
    Run the new Genetic Engine cycle (Generator + Evaluator).
    Discovers and saves elite strategies for TASI symbols.
    """
    try:
        from bots.scientist.bot import Scientist
        scientist = Scientist()
        result = scientist.run_genetic_cycle(
            symbols=symbols,
            generations=generations,
            population_size=population_size,
        )
        logger.info(
            f"✅ run_genetic_cycle complete: "
            f"{result.get('total_elite')} elite strategies, "
            f"{result.get('elapsed_sec')}s"
        )
        return result
    except Exception as exc:
        logger.error(f"run_genetic_cycle task failed: {exc}")
        raise self.retry(exc=exc, countdown=120)
