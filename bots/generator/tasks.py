"""Celery tasks for genetic generator bot"""
from scripts.celery_app import app
from loguru import logger


@app.task(name='bots.generator.tasks.generate_population', bind=True, max_retries=3)
def generate_population(self, symbol: str, profit_objective: str, size: int = 50):
    """Generate a population of genetic strategies for a symbol and objective."""
    try:
        from bots.generator.bot import GeneticGenerator
        gen = GeneticGenerator()
        population = gen.generate_population(symbol, profit_objective, size)
        logger.info(
            f"✅ Generated {len(population)} strategies for {symbol} [{profit_objective}]"
        )
        return {"status": "ok", "count": len(population), "population": population}
    except Exception as exc:
        logger.error(f"generate_population task failed: {exc}")
        raise self.retry(exc=exc, countdown=30)
