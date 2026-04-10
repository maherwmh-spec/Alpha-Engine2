# تقرير إصلاح Alpha-Engine2 — المحرك الجيني

**التاريخ:** 2026-04-10  
**الـ Commit:** `f8327e4`  
**الفرع:** `master`

---

## ملخص التشخيص

قبل البدء بالإصلاح، جرى تحليل شامل لثلاثة ملفات محورية وعدة ملفات مساندة. أظهر التحليل أن الخطأ الأصلي `AttributeError: 'Scientist' object has no attribute 'run_genetic_cycle'` كان موجوداً في نسخة قديمة من الكود، وأن الدالة أُضيفت لاحقاً في السطر 1359 من `bot.py`. غير أن ثلاث مشكلات جوهرية ظلّت قائمة:

| # | المشكلة | الملف | الأثر |
|---|---------|-------|-------|
| 1 | `StrategyEvaluator(db_pool=None)` يُفعّل وضع البيانات الاصطناعية | `bot.py` سطر 1398 | لا تُحفظ أي استراتيجيات في DB |
| 2 | `logging` ضعيف في مهمة Celery | `tasks.py` | صعوبة تتبع الأخطاء والتقدم |
| 3 | `COALESCE(name_ar, symbol)` يستهدف عموداً غير موجود | `dashboard/app.py` سطر 627 | خطأ SQL في صفحة القطاعات |

---

## الإصلاحات المنفّذة

### 1. `bots/scientist/bot.py` — الإصلاح الجوهري

**المشكلة:** كانت `run_genetic_cycle` تُمرّر `db_pool=None` إلى `StrategyEvaluator`، مما يُفعّل وضع التطوير الذي يستخدم بيانات اصطناعية (Random Walk) ويتجاهل الحفظ في قاعدة البيانات. كلتا الدالتين `save_strategy()` و `save_result()` تتحققان من `if self.db_pool is None: return False` في أول سطر.

**الإصلاح:** إنشاء `asyncpg.Pool` حقيقي باستخدام `config.get_asyncpg_dsn()` قبل تمريره للـ evaluator، مع إغلاق صريح للـ pool عند انتهاء الدورة.

```python
# الكود الجديد في run_genetic_cycle
import asyncpg
from config.config_manager import config

async def create_pool():
    return await asyncpg.create_pool(config.get_asyncpg_dsn())

loop = asyncio.new_event_loop()
db_pool = loop.run_until_complete(create_pool())
loop.close()

evaluator = StrategyEvaluator(db_pool=db_pool)

# ... [منطق الدورة] ...

# إغلاق الـ pool بعد الانتهاء
loop = asyncio.new_event_loop()
loop.run_until_complete(db_pool.close())
loop.close()
```

**النتيجة:** الدورة الجينية الآن تُقيّم الاستراتيجيات على بيانات حقيقية من `market_data.ohlcv` وتحفظ النتائج في `genetic.strategies` و `genetic.performance`.

---

### 2. `bots/scientist/tasks.py` — تعزيز الـ Logging

**المشكلة:** مهمة Celery كانت تسجّل رسالة واحدة فقط عند الإتمام، دون أي مؤشرات تقدم أو تفاصيل عند الفشل.

**الإصلاح:** إضافة ثلاث مراحل logging واضحة:

```python
# مرحلة 1: بداية المهمة
logger.info(f"🚀 Starting run_genetic_cycle task with symbols={symbols}, ...")

# مرحلة 2: تهيئة المحرك
logger.info("🧬 Initializing Genetic Engine...")

# مرحلة 3: إتمام ناجح
logger.success(f"✅ run_genetic_cycle completed: {result.get('total_elite')} elite strategies ...")

# عند الخطأ: رسالة + stack trace كامل
logger.error(f"❌ run_genetic_cycle task failed: {exc}")
logger.exception(exc)
```

---

### 3. `dashboard/app.py` — إصلاح استعلامات SQL

**المشكلة أ — القطاعات:** الاستعلام كان يستخدم `COALESCE(name_ar, symbol)` لكن جدول `market_data.sector_performance` يحتوي على عمود `name` وليس `name_ar` (وفقاً لـ migration 005).

```sql
-- قبل (خطأ)
SELECT COALESCE(name_ar, symbol) AS name FROM market_data.sector_performance

-- بعد (صحيح)
SELECT COALESCE(name, symbol) AS name FROM market_data.sector_performance
```

**المشكلة ب — المؤشر العام:** استعلام `index_performance` لم يكن يُصفّي بـ `timeframe`، مما يُعيد صفوفاً مكررة لنفس الوقت.

```sql
-- قبل (ناقص)
WHERE symbol = '90001'

-- بعد (صحيح)
WHERE symbol = '90001' AND timeframe = '1d'
```

---

## قائمة الملفات المعدّلة

| الملف | السطور المتأثرة | نوع التغيير |
|-------|----------------|-------------|
| `bots/scientist/bot.py` | 1397–1412, 1436–1439 | إضافة db_pool حقيقي + إغلاق صريح |
| `bots/scientist/tasks.py` | 29–51 | تعزيز logging بثلاث مراحل |
| `dashboard/app.py` | 598–600, 626–628 | إصلاح استعلامات SQL |

---

## أوامر إعادة البناء والتشغيل

### إعادة بناء وتشغيل celery_worker

```bash
# إعادة بناء الصورة بعد التغييرات
docker compose build celery_worker

# إعادة تشغيل الـ worker
docker compose up -d celery_worker

# مراقبة الـ logs
docker compose logs -f celery_worker
```

### اختبار متزامن للتحقق من عمل run_genetic_cycle

```bash
# الاختبار المتزامن المباشر (بدون Celery)
docker compose exec celery_worker python3 -c "
import asyncio
from bots.scientist.bot import Scientist

scientist = Scientist()
result = scientist.run_genetic_cycle(
    symbols=['2222', '1120', '2010'],
    generations=5,
    population_size=20,
    min_fitness_to_save=0.01,
)
print('=== النتيجة ===')
print(f'الأسهم المعالجة: {result[\"symbols_processed\"]}')
print(f'الأهداف المُشغَّلة: {result[\"objectives_run\"]}')
print(f'الاستراتيجيات النخبة: {result[\"total_elite\"]}')
print(f'الوقت المستغرق: {result[\"elapsed_sec\"]}s')
"
```

### تشغيل عبر Celery (الطريقة الموصى بها)

```bash
docker compose exec celery_worker python3 -c "
from bots.scientist.tasks import run_genetic_cycle
result = run_genetic_cycle.apply_async(kwargs={
    'symbols': ['2222', '1120', '2010'],
    'generations': 10,
    'population_size': 30,
})
print(result.get(timeout=600))
"
```

### التحقق من الحفظ في قاعدة البيانات

```bash
docker compose exec postgres psql -U alpha_user -d alpha_engine -c "
SELECT
    COUNT(*) AS total_strategies,
    COUNT(DISTINCT symbol) AS symbols,
    ROUND(AVG(fitness_score)::numeric, 4) AS avg_fitness,
    MAX(created_at) AS last_run
FROM genetic.strategies
WHERE fitness_score > 0;
"
```

---

## تدفق العمل الكامل بعد الإصلاح

```
Celery Task: run_genetic_cycle
        │
        ▼
Scientist.__init__()
  └─ _setup_deap()
  └─ _load_sentiment_model()
        │
        ▼
run_genetic_cycle()
  ├─ _pick_symbols_for_genetic_cycle()  ← Redis أو قائمة افتراضية
  ├─ GeneticGenerator()                  ← توليد الشيفرات الجينية
  ├─ asyncpg.create_pool(DSN)           ← ✅ اتصال حقيقي بـ DB
  └─ StrategyEvaluator(db_pool=pool)    ← ✅ تقييم + حفظ حقيقي
        │
        ▼
_run_evolution_loop() [لكل سهم × هدف]
  ├─ generate_population()
  ├─ evaluate() → _fetch_candles() من market_data.ohlcv
  ├─ _compute_fitness()
  └─ save_strategy() → genetic.strategies ✅
     save_result()   → genetic.performance ✅
        │
        ▼
إغلاق db_pool + إرجاع ملخص الدورة
```

---

## ملاحظات إضافية

**بخصوص `evaluator/tasks.py`:** لاحظنا أن هذا الملف أيضاً يستخدم `StrategyEvaluator(db_pool=None)`. إذا أردت تطبيق نفس الإصلاح عليه، يمكن إضافة نفس منطق إنشاء الـ pool.

**بخصوص `migration_sectors.sql`:** الجدول `sector_performance` يحتوي على `name VARCHAR` وليس `name_ar`. إذا كانت البيانات المُدرجة تستخدم `name_ar` كعمود مختلف، فيجب مراجعة `historical_sync.py` الذي يُدرج البيانات بعمود `name_ar` بينما يتوقع الداشبورد `name`.
