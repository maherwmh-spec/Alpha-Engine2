"""
Alpha-Engine2 Celery Application
Handles distributed task processing and scheduling
"""

from celery import Celery
from celery.schedules import crontab
from loguru import logger
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.config_manager import config

# Initialize Celery app
app = Celery('alpha_engine')

# Configure Celery
app.conf.update(
    broker_url=config.get_redis_url(),
    result_backend=config.get_redis_url(),
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Asia/Riyadh',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=30 * 60,  # 30 minutes
    task_soft_time_limit=25 * 60,  # 25 minutes
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=1000,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    broker_connection_retry_on_startup=True,
)

# Task routes - distribute tasks to different queues
app.conf.task_routes = {
    'bots.technical_miner.*': {'queue': 'high_priority'},
    'bots.monitor.*': {'queue': 'high_priority'},
    'bots.market_reporter.*': {'queue': 'normal'},
    'bots.scientist.*': {'queue': 'low_priority'},
    'bots.self_trainer.*': {'queue': 'low_priority'},
    'bots.backup_manager.*': {'queue': 'maintenance'},
}

# Scheduled tasks (Celery Beat)
app.conf.beat_schedule = {
    # Technical Miner - every minute
    'technical-miner-run': {
        'task': 'bots.technical_miner.tasks.run_technical_miner',
        'schedule': 60.0,  # Every 60 seconds
        'options': {'queue': 'high_priority'}
    },
    
    # Monitor - every 30 seconds
    'monitor-run': {
        'task': 'bots.monitor.tasks.run_monitor',
        'schedule': 30.0,
        'options': {'queue': 'high_priority'}
    },
    
    # Market Reporter - every 5 minutes
    'market-reporter-run': {
        'task': 'bots.market_reporter.tasks.run_market_reporter',
        'schedule': 300.0,
        'options': {'queue': 'normal'}
    },
    
    # Strategic Analyzer - every 10 minutes
    'strategic-analyzer-run': {
        'task': 'bots.strategic_analyzer.tasks.run_strategic_analyzer',
        'schedule': 600.0,
        'options': {'queue': 'normal'}
    },
    
    # Behavioral Analyzer - every 5 minutes
    'behavioral-analyzer-run': {
        'task': 'bots.behavioral_analyzer.tasks.run_behavioral_analyzer',
        'schedule': 300.0,
        'options': {'queue': 'normal'}
    },
    
    # Multiframe Confirmer - every 5 minutes
    'multiframe-confirmer-run': {
        'task': 'bots.multiframe_confirmer.tasks.run_multiframe_confirmer',
        'schedule': 300.0,
        'options': {'queue': 'normal'}
    },
    
    # Risk Guardian - every minute
    'risk-guardian-run': {
        'task': 'bots.risk_guardian.tasks.run_risk_guardian',
        'schedule': 60.0,
        'options': {'queue': 'high_priority'}
    },
    
    # Consolidation Hunter - every 5 minutes
    'consolidation-hunter-run': {
        'task': 'bots.consolidation_hunter.tasks.run_consolidation_hunter',
        'schedule': 300.0,
        'options': {'queue': 'normal'}
    },
    
    # Scientist (DEAP legacy) - every hour
    'scientist-run': {
        'task': 'bots.scientist.tasks.run_scientist',
        'schedule': 3600.0,
        'options': {'queue': 'low_priority'}
    },

    # Genetic Engine v2 - every 4 hours (Generator + Evaluator cycle)
    'genetic-engine-run': {
        'task': 'bots.scientist.tasks.run_genetic_cycle',
        'schedule': 14400.0,   # 4 hours
        'options': {'queue': 'low_priority'}
    },
    
    # Self Trainer - daily at 2 AM
    'self-trainer-run': {
        'task': 'bots.self_trainer.tasks.run_self_trainer',
        'schedule': crontab(hour=2, minute=0),
        'options': {'queue': 'low_priority'}
    },
    
    # Weekly Reviewer - Sunday at 9 AM
    'weekly-reviewer-run': {
        'task': 'bots.weekly_reviewer.tasks.run_weekly_reviewer',
        'schedule': crontab(day_of_week=0, hour=9, minute=0),
        'options': {'queue': 'normal'}
    },
    
    # Health Monitor - every hour
    'health-monitor-run': {
        'task': 'bots.health_monitor.tasks.run_health_monitor',
        'schedule': 3600.0,
        'options': {'queue': 'normal'}
    },
    
    # Health Monitor Daily Report - daily at 8 AM
    'health-monitor-daily-report': {
        'task': 'bots.health_monitor.tasks.send_daily_report',
        'schedule': crontab(hour=8, minute=0),
        'options': {'queue': 'normal'}
    },
    
    # Backup Manager - daily at 2 AM
    'backup-manager-run': {
        'task': 'bots.backup_manager.tasks.run_backup',
        'schedule': crontab(hour=2, minute=0),
        'options': {'queue': 'maintenance'}
    },
    
    # FreqAI Manager - every hour
    'freqai-manager-run': {
        'task': 'bots.freqai_manager.tasks.run_freqai_manager',
        'schedule': 3600.0,
        'options': {'queue': 'low_priority'}
    },
    
    # Send pending alerts - every minute
    'send-pending-alerts': {
        'task': 'scripts.telegram_bot.send_pending_alerts',
        'schedule': 60.0,
        'options': {'queue': 'high_priority'}
    },
}

# Auto-discover tasks from all bot modules
app.autodiscover_tasks([
    'bots.technical_miner',
    'bots.market_reporter',
    'bots.scientist',
    'bots.generator',
    'bots.evaluator',
    'bots.strategic_analyzer',
    'bots.monitor',
    'bots.behavioral_analyzer',
    'bots.multiframe_confirmer',
    'bots.risk_guardian',
    'bots.consolidation_hunter',
    'bots.self_trainer',
    'bots.weekly_reviewer',
    'bots.health_monitor',
    'bots.backup_manager',
    'bots.parameter_editor',
    'bots.dashboard_service',
    'bots.freqai_manager',
    'bots.silent_mode_manager',
])

logger.info("Celery application initialized")


@app.task(bind=True)
def debug_task(self, *args, **kwargs):
    """
    Debug task to test Celery.
    Accepts optional positional and keyword arguments for flexible testing.
    Usage:
        debug_task.apply_async()                        # no args
        debug_task.apply_async(args=['Hello World'])    # with message
        debug_task.apply_async(kwargs={'key': 'val'})   # with kwargs
    """
    msg = args[0] if args else kwargs.get('message', 'no message')
    logger.info(f'Request: {self.request!r} | message={msg}')
    return f'Debug task completed: {msg}'


if __name__ == '__main__':
    app.start()
