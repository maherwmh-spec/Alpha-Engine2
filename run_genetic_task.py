"""
Unified entry point to trigger the genetic cycle task via Celery.
This ensures the Celery app is fully initialized and the task is registered
before calling apply_async.
"""

import sys
import os
from pathlib import Path

# Add project root to sys.path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from scripts.celery_app import app
from bots.scientist.tasks import run_genetic_cycle

def main():
    print(">>> Sending genetic cycle task via unified entry point...")
    
    # Send the task to Celery
    result = run_genetic_cycle.apply_async(kwargs={
        'symbols': ['1120', '2222'],
        'generations': 5,
        'population_size': 15
    })
    
    print(f">>> Task sent successfully with ID: {result.id}")
    print(">>> Watch celery_worker logs now.")
    print(f">>> To check status: docker compose logs -f celery_worker")

if __name__ == "__main__":
    main()
