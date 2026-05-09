"""Celery task compatibility wrappers for backup_manager.

The backup manager is disabled by default in config.yaml until a production
implementation is provided. These task names are kept for backward
compatibility so legacy Celery beat entries or manual invocations do not fail
with an unregistered task error.
"""
from scripts.celery_app import app
from loguru import logger


def _backup_manager_not_implemented():
    """Return an explicit disabled/stub payload without performing side effects."""
    logger.info("backup_manager is disabled by design - MVP not implemented yet")
    return {
        "status": "disabled",
        "bot": "backup_manager",
        "reason": "disabled_by_config_or_not_implemented",
    }


@app.task(name='bots.backup_manager.tasks.run_backup_manager', bind=True)
def run_backup_manager(self):
    """Backward-compatible backup_manager task name."""
    return _backup_manager_not_implemented()


@app.task(name='bots.backup_manager.tasks.run_backup', bind=True)
def run_backup(self):
    """Legacy scheduled task name retained for backward compatibility."""
    return _backup_manager_not_implemented()
