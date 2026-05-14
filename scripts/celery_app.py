"""
Alpha-Engine2 Celery Application
Handles distributed task processing and scheduling

Dynamic autodiscovery: automatically finds all bots/*/tasks.py
so that new bots are picked up without touching this file.

FIX: Redis authentication enforced via REDIS_PASSWORD environment variable.
     broker uses db/0, result_backend uses db/1 for separation.
"""

import os
from celery import Celery
from celery.schedules import crontab
from loguru import logger
import sys
from pathlib import Path

# ── Project root on sys.path ─────────────────────────────────────────────────
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.config_manager import config

# ── Build Redis URLs from config.yaml (no .env required) ───────────────────
REDIS_PASSWORD = config.get_redis_connection_params(db_index=0)["password"]
_redis_host = config.get_redis_connection_params(db_index=0)["host"]
_redis_port = str(config.get_redis_connection_params(db_index=0)["port"])

broker_url      = config.get_redis_url(db_index=0)
result_backend  = config.get_redis_url(db_index=1)

logger.info(
    f"[Celery] Redis broker  → redis://:{REDIS_PASSWORD[:4]}***@{_redis_host}:{_redis_port}/0"
)
logger.info(
    f"[Celery] Redis backend → redis://:{REDIS_PASSWORD[:4]}***@{_redis_host}:{_redis_port}/1"
)

# ── Dynamic bot discovery ─────────────────────────────────────────────────────
# Scans bots/<name>/tasks.py at import time — works both locally and in Docker
# because the working directory is always the project root (/app).
_bots_dir = project_root / 'bots'
_discovered_packages = sorted([
    f'bots.{d.name}.tasks'
    for d in _bots_dir.iterdir()
    if d.is_dir() and (d / 'tasks.py').exists()
])

logger.info(
    f"[Celery] Dynamic autodiscovery found {len(_discovered_packages)} bot packages: "
    + ", ".join(_discovered_packages)
)

# ── Celery app ────────────────────────────────────────────────────────────────
app = Celery(
    'alpha_engine',
    broker=broker_url,
    backend=result_backend,
    include=_discovered_packages + ['scripts.sync_symbols'],  # bots + scripts tasks
)

# ── Celery configuration ──────────────────────────────────────────────────────
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Asia/Riyadh',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=30 * 60,          # 30 minutes hard limit
    task_soft_time_limit=25 * 60,     # 25 minutes soft limit
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=1000,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    broker_connection_retry_on_startup=True,
    # Explicit broker/backend URLs (redundant but ensures override)
    broker_url=broker_url,
    result_backend=result_backend,
)

# ── Task routes ───────────────────────────────────────────────────────────────
# All scientist/generator/evaluator tasks go to 'default' queue so the worker
# picks them up without needing a dedicated low_priority consumer.
app.conf.task_routes = {
    'bots.technical_miner.*': {'queue': 'high_priority'},
    'bots.monitor.*':         {'queue': 'high_priority'},
    'bots.risk_guardian.*':   {'queue': 'high_priority'},
    'bots.market_reporter.*': {'queue': 'normal'},
    'bots.strategic_analyzer.*':    {'queue': 'normal'},
    'bots.behavioral_analyzer.*':   {'queue': 'normal'},
    'bots.multiframe_confirmer.*':  {'queue': 'normal'},
    'bots.consolidation_hunter.*':  {'queue': 'normal'},
    'bots.health_monitor.*':        {'queue': 'normal'},
    'bots.weekly_reviewer.*':       {'queue': 'normal'},
    # Genetic Engine — default queue (was low_priority, caused TimeoutError)
    'bots.scientist.*':  {'queue': 'default'},
    'bots.generator.*':  {'queue': 'default'},
    'bots.evaluator.*':  {'queue': 'default'},
    # Maintenance
    'bots.self_trainer.*':    {'queue': 'low_priority'},
    'bots.backup_manager.*':  {'queue': 'maintenance'},
}

# ── Beat schedule (periodic tasks) ───────────────────────────────────────────
app.conf.beat_schedule = {

    # ── High priority ────────────────────────────────────────────────────────
    'technical-miner-run': {
        'task': 'bots.technical_miner.tasks.run_technical_miner',
        'schedule': 60.0,
        'options': {'queue': 'high_priority'},
    },
    'monitor-run': {
        'task': 'bots.monitor.tasks.run_monitor',
        'schedule': 30.0,
        'options': {'queue': 'high_priority'},
    },
    'risk-guardian-run': {
        'task': 'bots.risk_guardian.tasks.run_risk_guardian',
        'schedule': 60.0,
        'options': {'queue': 'high_priority'},
    },

    # ── Normal ───────────────────────────────────────────────────────────────
    'market-reporter-run': {
        'task': 'bots.market_reporter.tasks.run_market_reporter',
        'schedule': 300.0,
        'options': {'queue': 'normal'},
    },
    'strategic-analyzer-run': {
        'task': 'bots.strategic_analyzer.tasks.run_strategic_analyzer',
        'schedule': 600.0,
        'options': {'queue': 'normal'},
    },
    'behavioral-analyzer-run': {
        'task': 'bots.behavioral_analyzer.tasks.run_behavioral_analyzer',
        'schedule': 300.0,
        'options': {'queue': 'normal'},
    },
    'multiframe-confirmer-run': {
        'task': 'bots.multiframe_confirmer.tasks.run_multiframe_confirmer',
        'schedule': 300.0,
        'options': {'queue': 'normal'},
    },
    'consolidation-hunter-run': {
        'task': 'bots.consolidation_hunter.tasks.run_consolidation_hunter',
        'schedule': 300.0,
        'options': {'queue': 'normal'},
    },
    'health-monitor-run': {
        'task': 'bots.health_monitor.tasks.run_health_monitor',
        'schedule': 3600.0,
        'options': {'queue': 'normal'},
    },
    'health-monitor-daily-report': {
        'task': 'bots.health_monitor.tasks.send_daily_report',
        'schedule': crontab(hour=8, minute=0),
        'options': {'queue': 'normal'},
    },
    'weekly-reviewer-run': {
        'task': 'bots.weekly_reviewer.tasks.run_weekly_reviewer',
        'schedule': crontab(day_of_week=0, hour=9, minute=0),
        'options': {'queue': 'normal'},
    },

    # ── Genetic Engine (default queue) ───────────────────────────────────────────────
    'scientist-run': {
        'task': 'bots.scientist.tasks.run_scientist',
        'schedule': 3600.0,
        'options': {'queue': 'default'},
    },
    # المحرك الجيني: مرة واحدة يومياً الساعة 18:00 بتوقيت Asia/Riyadh
    'genetic-engine-run': {
        'task': 'bots.scientist.tasks.run_genetic_cycle',
        'schedule': crontab(hour=18, minute=0),
        'options': {'queue': 'default'},
    },

    # ── Low priority ─────────────────────────────────────────────────────
    # المدرب الذاتي: مرة واحدة يومياً الساعة 02:00 بتوقيت Asia/Riyadh
    'self-trainer-run': {
        'task': 'bots.self_trainer.tasks.run_self_trainer',
        'schedule': crontab(hour=2, minute=0),
        'options': {'queue': 'low_priority'},
    },
    # مدير FreqAI: مرة واحدة يومياً الساعة 03:00 بتوقيت Asia/Riyadh — طابور 'default'
    'freqai-manager-run': {
        'task': 'bots.freqai_manager.tasks.run_freqai_manager',
        'schedule': crontab(hour=3, minute=0),
        'options': {'queue': 'default'},
    },

    # ── Symbol Sync (daily) ────────────────────────────────────────────────────────────────────────────────────────────
    # يعمل يومياً الساعة 07:00 KSA (قبل فتح السوق بساعة)
    'sync-tasi-symbols': {
        'task': 'scripts.sync_symbols.sync_symbols_task',
        'schedule': crontab(hour=7, minute=0),
        'options': {'queue': 'default'},
    },

    # ── Maintenance ─────────────────────────────────────────────────────────────────────────────────
    'backup-manager-run': {
        'task': 'bots.backup_manager.tasks.run_backup',
        'schedule': crontab(hour=2, minute=0),
        'options': {'queue': 'maintenance'},
    },

    # ── Alerts ───────────────────────────────────────────────────────────────
    'send-pending-alerts': {
        'task': 'scripts.telegram_bot.send_pending_alerts',
        'schedule': 60.0,
        'options': {'queue': 'high_priority'},
    },
}

# ── Explicit autodiscover (belt-and-suspenders alongside include=) ────────────


def _extract_scheduled_bot(task_name: str):
    """Return bot name from a Celery task path like bots.<bot>.tasks.<task>."""
    parts = task_name.split('.') if task_name else []
    if len(parts) >= 4 and parts[0] == 'bots' and parts[2] == 'tasks':
        return parts[1]
    return None


def _filter_disabled_bot_schedules(schedule: dict) -> dict:
    """Remove periodic entries for bots explicitly disabled in config.yaml.

    Backward-compatible behavior: bots missing from config are treated as enabled
    so existing custom deployments are not silently disabled.
    """
    filtered = {}
    for schedule_name, schedule_def in schedule.items():
        task_name = schedule_def.get('task', '') if isinstance(schedule_def, dict) else ''
        bot_name = _extract_scheduled_bot(task_name)
        if bot_name is None:
            filtered[schedule_name] = schedule_def
            continue
        if bool(config.get(f'bots.{bot_name}.enabled', True)):
            filtered[schedule_name] = schedule_def
        else:
            logger.info(
                f"[Celery] Skipping disabled bot schedule '{schedule_name}' "
                f"({task_name}) because bots.{bot_name}.enabled=false"
            )
    return filtered


app.conf.beat_schedule = _filter_disabled_bot_schedules(app.conf.beat_schedule)

app.autodiscover_tasks(_discovered_packages)

logger.info("Celery application initialized with authenticated Redis connection")


@app.task(bind=True)
def debug_task(self, *args, **kwargs):
    """
    Debug task to test Celery connectivity.
    Usage:
        debug_task.apply_async()
        debug_task.apply_async(args=['Hello World'])
        debug_task.apply_async(kwargs={'message': 'test'})
    """
    msg = args[0] if args else kwargs.get('message', 'no message')
    logger.info(f'Request: {self.request!r} | message={msg}')
    return f'Debug task completed: {msg}'


if __name__ == '__main__':
    app.start()
