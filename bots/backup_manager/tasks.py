"""Stub tasks for backup_manager - not yet implemented"""
from scripts.celery_app import app
from loguru import logger


@app.task(name='bots.backup_manager.tasks.run_backup_manager', bind=True)
def run_backup_manager(self):
    """Stub task for backup_manager"""
    logger.info("backup_manager task stub - not yet implemented")
    return {"status": "stub", "bot": "backup_manager"}
