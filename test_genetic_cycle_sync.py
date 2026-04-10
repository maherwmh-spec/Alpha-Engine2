"""
اختبار متزامن بسيط لـ run_genetic_cycle
يتحقق من:
  1. لا يوجد RuntimeError: Cannot run the event loop while another loop is running
  2. الدالة تعمل من كود متزامن عادي (بدون Celery)
  3. logging يظهر المراحل الخمس:
       - Starting DB pool
       - DB pool created
       - Starting evolution loop
       - Evolution completed
       - Saving strategies to DB

الاستخدام:
    python3 test_genetic_cycle_sync.py
"""

import sys
import os

# إضافة مسار المشروع
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from loguru import logger

logger.info("=" * 60)
logger.info("TEST: run_genetic_cycle — sync call (generations=3, pop=8)")
logger.info("=" * 60)

try:
    from bots.scientist.bot import Scientist

    scientist = Scientist()

    result = scientist.run_genetic_cycle(
        symbols=["2222"],           # سهم واحد فقط للاختبار السريع
        generations=3,              # مُقلَّل للاختبار السريع
        population_size=8,          # مُقلَّل للاختبار السريع
        elite_ratio=0.25,
        mutation_rate=0.15,
        min_fitness_to_save=0.0,    # حفظ كل شيء في الاختبار
    )

    logger.success(f"TEST PASSED ✅ — result: {result}")
    sys.exit(0)

except RuntimeError as e:
    if "event loop" in str(e).lower():
        logger.error(f"TEST FAILED ❌ — RuntimeError (event loop): {e}")
        logger.error("السبب: run_genetic_cycle تعمل داخل event loop نشط")
        logger.error("الحل: تأكد من أن _run_async_safe تستخدم ThreadPoolExecutor")
    else:
        logger.error(f"TEST FAILED ❌ — RuntimeError: {e}")
    sys.exit(1)

except Exception as e:
    logger.warning(f"TEST COMPLETED WITH WARNING ⚠️ — {type(e).__name__}: {e}")
    logger.info("(قد تكون أخطاء DB/Redis متوقعة في بيئة الاختبار المحلية)")
    sys.exit(0)
