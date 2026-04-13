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

        # إنشاء DB pool لحفظ النتائج
        import asyncpg
        from config.config_manager import config as _config
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            # تشغيل التقييم والحفظ داخل الـ loop
            async def _run_eval_and_save():
                db_pool = None
                try:
                    db_pool = await asyncpg.create_pool(_config.get_asyncpg_dsn())
                    evaluator = StrategyEvaluator(db_pool=db_pool)
                    
                    # 1. تقييم الاستراتيجية
                    res = await evaluator.evaluate(dna, candles_limit=candles_limit)
                    
                    # 2. دمج النتائج في الـ DNA
                    dna["fitness_score"] = res.get("fitness_score", 0.0)
                    for key in [
                        "total_profit_pct", "win_rate", "total_trades",
                        "avg_profit_pct", "max_drawdown_pct", "sharpe_ratio",
                        "profit_factor", "avg_duration_min",
                    ]:
                        dna[key] = res.get(key, 0)
                        
                    # 3. حفظ الاستراتيجية والنتيجة إذا كانت جيدة
                    if dna["fitness_score"] >= 0.0:  # حفظ أي استراتيجية تم تقييمها بنجاح
                        saved = await evaluator.save_strategy(dna)
                        if saved:
                            await evaluator.save_result(dna)
                            logger.info(f"💾 Saved strategy and performance for {dna.get('symbol')}")
                            
                    return res
                finally:
                    if db_pool:
                        await db_pool.close()
            
            result = loop.run_until_complete(_run_eval_and_save())
            
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

        import asyncpg
        from config.config_manager import config as _config
        
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
                        # 1. تقييم الاستراتيجية
                        res = await evaluator.evaluate(dna, candles_limit=candles_limit)
                        
                        # 2. دمج النتائج في الـ DNA
                        dna["fitness_score"] = res.get("fitness_score", 0.0)
                        for key in [
                            "total_profit_pct", "win_rate", "total_trades",
                            "avg_profit_pct", "max_drawdown_pct", "sharpe_ratio",
                            "profit_factor", "avg_duration_min",
                        ]:
                            dna[key] = res.get(key, 0)
                            
                        # 3. حفظ الاستراتيجية والنتيجة
                        if dna["fitness_score"] >= 0.0:
                            saved = await evaluator.save_strategy(dna)
                            if saved:
                                await evaluator.save_result(dna)
                                
                        results.append(res)
                finally:
                    if db_pool:
                        await db_pool.close()
                        
            loop.run_until_complete(_run_pop_eval_and_save())
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
