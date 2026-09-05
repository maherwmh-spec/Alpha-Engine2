"""
Alpha-Engine2 Celery Application
Handles distributed task processing and scheduling

Dynamic autodiscovery: automatically finds all bots/*/tasks.py
so that new bots are picked up without touching this file.

Trading week = Sunday–Thursday (Celery day_of_week 0-4, Asia/Riyadh).
Session (09:30–15:30): monitor / miner / hunter only.
Nightly after close: personality 16:30, features 16:45, sentiment 17:00.
Morning watchlist: 08:00 Sun–Thu.
Weekly reviewer + self_trainer: Friday 18:00.
Genetic: 21:00 Sun–Thu (after close, not during the session).
"""

import os
from celery import Celery
from celery.schedules import crontab
from loguru import logger
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.config_manager import config

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

_extra_task_modules = [
    'scripts.sync_symbols',
    'scripts.retention_policy',
    'scripts.telegram_bot',
]

app = Celery(
    'alpha_engine',
    broker=broker_url,
    backend=result_backend,
    include=_discovered_packages + _extra_task_modules,
)

app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Asia/Riyadh',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=30 * 60,
    task_soft_time_limit=25 * 60,
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=1000,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    task_default_retry_delay=60,
    task_annotations={'*': {'rate_limit': '120/m'}},
    broker_connection_retry_on_startup=True,
    broker_connection_max_retries=None,
    broker_heartbeat=30,
    beat_max_loop_interval=60,
    broker_url=broker_url,
    result_backend=result_backend,
)

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
    'bots.stock_personality.*':     {'queue': 'normal'},
    'bots.feature_engineer.*':      {'queue': 'normal'},
    'bots.sentiment_analyzer.*':    {'queue': 'normal'},
    'bots.watchlist_generator.*':   {'queue': 'normal'},
    'bots.freqai_manager.*':        {'queue': 'default'},
    'bots.scientist.*':  {'queue': 'default'},
    'bots.generator.*':  {'queue': 'default'},
    'bots.evaluator.*':  {'queue': 'default'},
    'bots.self_trainer.*':    {'queue': 'low_priority'},
    'bots.backup_manager.*':  {'queue': 'maintenance'},
    'scripts.telegram_bot.*': {'queue': 'high_priority'},
}

_WEEKDAYS = '0-4'  # Sunday–Thursday

app.conf.beat_schedule = {

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
        'schedule': crontab(hour=8, minute=0, day_of_week=_WEEKDAYS),
        'options': {'queue': 'normal'},
    },

    'weekly-reviewer-run': {
        'task': 'bots.weekly_reviewer.tasks.run_weekly_reviewer',
        'schedule': crontab(day_of_week=5, hour=18, minute=0),
        'options': {'queue': 'normal'},
    },

    'feature-engineer-run': {
        'task': 'bots.feature_engineer.tasks.run_feature_engineer',
        'schedule': crontab(hour=16, minute=45, day_of_week=_WEEKDAYS),
        'options': {'queue': 'normal'},
    },
    'sentiment-analyzer-run': {
        'task': 'bots.sentiment_analyzer.tasks.run_sentiment_analyzer',
        'schedule': crontab(hour=17, minute=0, day_of_week=_WEEKDAYS),
        'options': {'queue': 'normal'},
    },

    'stock-personality-run': {
        'task': 'bots.stock_personality.tasks.run_stock_personality',
        'schedule': crontab(hour=16, minute=30, day_of_week=_WEEKDAYS),
        'options': {'queue': 'normal'},
    },

    'genetic-engine-run': {
        'task': 'bots.scientist.tasks.run_genetic_cycle',
        'schedule': crontab(hour=21, minute=0, day_of_week=_WEEKDAYS),
        'options': {'queue': 'default'},
    },

    'watchlist-generator-run': {
        'task': 'bots.watchlist_generator.tasks.run_watchlist_generator',
        'schedule': crontab(hour=8, minute=0, day_of_week=_WEEKDAYS),
        'options': {'queue': 'normal'},
    },

    'self-trainer-run': {
        'task': 'bots.self_trainer.tasks.run_self_trainer',
        'schedule': crontab(day_of_week=5, hour=18, minute=15),
        'options': {'queue': 'low_priority'},
    },
    'freqai-manager-run': {
        'task': 'bots.freqai_manager.tasks.run_freqai_manager',
        'schedule': crontab(hour=3, minute=0),
        'options': {'queue': 'default'},
    },

    'sync-tasi-symbols': {
        'task': 'scripts.sync_symbols.sync_symbols_task',
        'schedule': crontab(hour=7, minute=0, day_of_week=_WEEKDAYS),
        'options': {'queue': 'default'},
    },

    'backup-manager-run': {
        'task': 'bots.backup_manager.tasks.run_backup',
        'schedule': crontab(hour=2, minute=0),
        'options': {'queue': 'maintenance'},
    },
    'retention-policy-run': {
        'task': 'scripts.retention_policy.run_retention_policy',
        'schedule': crontab(hour=4, minute=30),
        'options': {'queue': 'maintenance'},
    },

    'send-pending-alerts': {
        'task': 'scripts.telegram_bot.send_pending_alerts',
        'schedule': 60.0,
        'options': {'queue': 'high_priority'},
    },
}


def _extract_scheduled_bot(task_name: str):
    parts = task_name.split('.') if task_name else []
    if len(parts) >= 4 and parts[0] == 'bots' and parts[2] == 'tasks':
        return parts[1]
    return None


def _filter_disabled_bot_schedules(schedule: dict) -> dict:
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
    msg = args[0] if args else kwargs.get('message', 'no message')
    logger.info(f'Request: {self.request!r} | message={msg}')
    return f'Debug task completed: {msg}'


if __name__ == '__main__':
    app.start()
