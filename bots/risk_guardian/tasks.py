"""Stub tasks for risk_guardian - not yet implemented"""
from scripts.celery_app import app
from loguru import logger


@app.task(name='bots.risk_guardian.tasks.run_risk_guardian', bind=True)
def run_risk_guardian(self):
    """Stub task for risk_guardian"""
    logger.info("risk_guardian task stub - not yet implemented")
    return {"status": "stub", "bot": "risk_guardian"}
