"""Celery tasks for scientist bot — unified genetic engine (Phase 1)."""
from scripts.celery_app import app
from loguru import logger


def _apply_unified_loop_patch() -> None:
    """Replace Scientist._async_evolution_loop with seeded+promote unified path."""
    try:
        from bots.scientist.bot import Scientist
        from bots.scientist.unified_loop import run_evolution_loop

        async def _async_evolution_loop(
            self,
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
        ):
            return await run_evolution_loop(
                generator=generator,
                evaluator=evaluator,
                symbol=symbol,
                objective=objective,
                generations=generations,
                population_size=population_size,
                elite_ratio=elite_ratio,
                mutation_rate=mutation_rate,
                min_fitness_to_save=min_fitness_to_save,
                return_summary=return_summary,
                db_pool=getattr(evaluator, "db_pool", None),
            )

        Scientist._async_evolution_loop = _async_evolution_loop  # type: ignore[method-assign]
        logger.info("Scientist._async_evolution_loop patched → unified_loop")
    except Exception as exc:
        logger.warning(f"unified_loop patch skipped: {exc}")


def _apply_evaluator_patches() -> None:
    try:
        from bots.evaluator.condition_ext import apply_evaluator_patches

        apply_evaluator_patches()
        logger.info("Evaluator indicator/condition patches applied")
    except Exception as exc:
        logger.warning(f"evaluator patches skipped: {exc}")


_apply_unified_loop_patch()
_apply_evaluator_patches()


@app.task(name='bots.scientist.tasks.run_scientist', bind=True, max_retries=1)
def run_scientist(self):
    """DEPRECATED: legacy DEAP path is isolated. Redirects to run_genetic_cycle."""
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
    """Unified Genetic Engine cycle (Generator + Evaluator)."""
    try:
        _apply_unified_loop_patch()
        _apply_evaluator_patches()
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
