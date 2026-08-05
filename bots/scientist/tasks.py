"""Celery tasks for scientist bot — unified genetic engine (Phase 1)."""
from scripts.celery_app import app
from loguru import logger


@app.task(name='bots.scientist.tasks.run_scientist', bind=True, max_retries=1)
def run_scientist(self):
    """DEPRECATED: legacy DEAP path is isolated.

    Kept registered so old beat entries / manual calls do not crash.
    Redirects to the unified genetic cycle with conservative settings.
    """
    logger.warning(
        "run_scientist is DEPRECATED (DEAP path isolated). "
        "Redirecting to run_genetic_cycle. See docs/DEAP_TRANSFER_MAP.md"
    )
    return run_genetic_cycle(
        symbols=None,
        generations=10,
        population_size=30,
    )


@app.task(name='bots.scientist.tasks.run_genetic_cycle', bind=True, max_retries=3)
def run_genetic_cycle(
    self,
    symbols=None,
    generations: int = 15,
    population_size: int = 30,
):
    """Unified Genetic Engine cycle (Generator + Evaluator).

    Discovers and saves elite/active strategies for TASI symbols.
    This is the only scheduled scientist path after Phase 1 unification.
    """
    try:
        logger.info(
            f"Starting UNIFIED run_genetic_cycle symbols={symbols}, "
            f"generations={generations}, pop_size={population_size}"
        )
        from bots.scientist.bot import Scientist
        scientist = Scientist()

        result = scientist.run_genetic_cycle(
            symbols=symbols,
            generations=generations,
            population_size=population_size,
            elite_ratio=0.20,
            mutation_rate=0.15,
            min_fitness_to_save=0.05,
        )

        logger.success(
            f"run_genetic_cycle completed: "
            f"{result.get('total_elite')} elite across "
            f"{result.get('symbols_processed')} symbols in "
            f"{result.get('elapsed_sec')}s"
        )
        return result
    except Exception as exc:
        logger.error(f"run_genetic_cycle failed: {exc}")
        logger.exception(exc)
        raise self.retry(exc=exc, countdown=120)
