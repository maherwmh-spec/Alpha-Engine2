"""Final production-readiness checks for the Genetic Engine.

These tests deliberately avoid network downloads and external services.  They
validate the production guard that blocks synthetic candles and run a small
smoke cycle in development mode to ensure the genetic-cycle summary contract is
stable.
"""
from __future__ import annotations

import asyncio
import importlib
import sys
import types


def _install_optional_dependency_stubs() -> None:
    """Install tiny import stubs for optional heavyweight ML/GA packages."""
    if "deap" not in sys.modules:
        deap = types.ModuleType("deap")
        deap.base = types.SimpleNamespace(Fitness=object, Toolbox=lambda: types.SimpleNamespace(register=lambda *a, **k: None))
        deap.creator = types.SimpleNamespace(create=lambda name, base, **kwargs: setattr(deap.creator, name, type(name, (base,), {})))
        deap.tools = types.SimpleNamespace(initRepeat=None, cxTwoPoint=None, selTournament=None)
        deap.algorithms = types.SimpleNamespace()
        sys.modules.update({
            "deap": deap,
            "deap.base": deap.base,
            "deap.creator": deap.creator,
            "deap.tools": deap.tools,
            "deap.algorithms": deap.algorithms,
        })

    if "torch" not in sys.modules:
        torch = types.ModuleType("torch")
        torch.cuda = types.SimpleNamespace(is_available=lambda: False)
        torch.no_grad = lambda: types.SimpleNamespace(__enter__=lambda self: None, __exit__=lambda self, *exc: False)
        torch.nn = types.SimpleNamespace(functional=types.SimpleNamespace(softmax=lambda logits, dim=-1: logits))
        sys.modules["torch"] = torch

    if "transformers" not in sys.modules:
        transformers = types.ModuleType("transformers")
        dummy_loader = types.SimpleNamespace(from_pretrained=lambda *args, **kwargs: None)
        transformers.AutoTokenizer = dummy_loader
        transformers.AutoModelForSequenceClassification = dummy_loader
        transformers.AutoModel = dummy_loader
        sys.modules["transformers"] = transformers

    if "scipy" not in sys.modules:
        scipy = types.ModuleType("scipy")
        stats = types.ModuleType("scipy.stats")
        scipy.stats = stats
        sys.modules.update({"scipy": scipy, "scipy.stats": stats})

    if "pytz" not in sys.modules:
        pytz = types.ModuleType("pytz")
        pytz.timezone = lambda *_args, **_kwargs: None
        sys.modules["pytz"] = pytz

    if "asyncpg" not in sys.modules:
        asyncpg = types.ModuleType("asyncpg")

        async def _create_pool(*_args, **_kwargs):
            raise ConnectionError("smoke test intentionally runs without DB")

        asyncpg.create_pool = _create_pool
        sys.modules["asyncpg"] = asyncpg

    redis_stub = types.ModuleType("scripts.redis_manager")
    redis_stub.redis_manager = types.SimpleNamespace(
        get=lambda *_args, **_kwargs: None,
        set=lambda *_args, **_kwargs: True,
        publish=lambda *_args, **_kwargs: True,
    )
    sys.modules["scripts.redis_manager"] = redis_stub


def test_production_blocks_synthetic_candles_without_db(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")

    from bots.evaluator.bot import StrategyEvaluator

    evaluator = StrategyEvaluator(db_pool=None)
    result = asyncio.run(evaluator.evaluate({
        "hash": "prod-no-db",
        "symbol": "1010",
        "profit_objective": "scalping",
        "timeframe": "1m",
        "entry_rules": [],
        "exit_rules": [],
    }))

    assert result["status"] == "db_unavailable"
    assert result["candles_count"] == 0
    assert "synthetic" in result["failure_reason"]
    assert "symbol=1010" not in result["failure_reason"] or "db_unavailable" in result["failure_reason"]


def test_run_genetic_cycle_development_smoke(monkeypatch):
    monkeypatch.setenv("APP_ENV", "development")
    _install_optional_dependency_stubs()

    scientist_module = importlib.import_module("bots.scientist.bot")
    monkeypatch.setattr(scientist_module.Scientist, "_load_sentiment_model", lambda self: None)
    monkeypatch.setattr(scientist_module.Scientist, "_load_embedding_model", lambda self: None)
    monkeypatch.setattr(scientist_module.Scientist, "_setup_deap", lambda self: None)

    scientist = scientist_module.Scientist()
    summary = scientist.run_genetic_cycle(
        symbols=["1010"],
        generations=1,
        population_size=2,
        elite_ratio=0.5,
        mutation_rate=0.01,
        min_fitness_to_save=-1.0,
    )

    assert summary["symbols_processed"] == 1
    assert summary["objectives_run"] >= 1
    assert summary["generations_run"] == 1
    assert summary["population_size"] == 2
    assert summary["evaluated_count"] >= 2
    assert isinstance(summary["failure_reasons"], dict)
    assert summary["runs"][0]["symbol"] == "1010"
    assert summary["runs"][0]["objective"]
    assert summary["runs"][0]["generations_run"] == 1
