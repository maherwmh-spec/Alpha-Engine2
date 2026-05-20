# تقرير رفع جاهزية Alpha-Engine2 للتشغيل الإنتاجي المستقر

**المؤلف:** Manus AI
**التاريخ:** 20 مايو 2026
**نطاق التنفيذ:** الاستقرار، الموثوقية، تكامل المكونات، جدولة المهام، مسار الإشارات، FreqAI، Hugging Face، المراقبة وسياسة الاحتفاظ.
**قيود التعديل الملتزم بها:** لم يتم تعديل الأسرار في `config/config.yaml`، ولم يتم تعديل المحرك الجيني أو ملفات Scientist / Genetic Engine داخل `bots/scientist`.

## 1. الخلاصة التنفيذية

تم تنفيذ مجموعة تعديلات إنتاجية مركزة على مشروع **Alpha-Engine2** بهدف نقله من حالة تجريبية متقدمة إلى حالة أكثر استقرارًا وقابلية للتشغيل اليومي. ركّزت التعديلات على جعل مسارات التشغيل أقل اعتمادًا على التدخل اليدوي، مع تحسين وضوح الحالة التشغيلية للمهام، إضافة منع التشغيل المتداخل، تحسين الفولباك الآمن في Hugging Face، حفظ مخرجات FreqAI فعليًا، وتحسين مراقبة الصحة وسياسة الاحتفاظ بالبيانات.

> النتيجة العملية: أصبح النظام يملك مسارات أوضح لتدريب FreqAI، حفظ إعدادات التدريب وحالة النماذج، توليد بيانات Hugging Face ديناميكية مع كاش وفولباك، منع تكرار الإشارات ضمن نافذة زمنية، وتسجيل أسباب عدم ظهور الإشارة، إضافة إلى مراقبة صحة أكثر فاعلية وجدولة احتفاظ يومية.

لم يتم دفع التغييرات إلى GitHub تلقائيًا لأن الطلب لم ينص صراحة على إنشاء commit أو push. جميع التعديلات موجودة محليًا في مسار المشروع `/home/ubuntu/Alpha-Engine2` وجاهزة للمراجعة أو الالتزام بالمستودع عند رغبتك.

## 2. الملفات المعدلة أو المضافة

| الحالة | الملف | الغرض من التعديل |
|---|---|---|
| معدل | `bots/freqai_manager/bot.py` | إكمال مدير FreqAI لحفظ `config.json` فعليًا، تشغيل أمر التدريب، وتسجيل حالة النموذج مع آخر تدريب ومسار الملف. |
| معدل | `bots/freqai_manager/tasks.py` | إضافة منع التشغيل المتداخل لمهمة FreqAI، وتسجيل حالة المهمة مع retry آمن. |
| مضاف | `freqtrade/huggingface/data_provider.py` | إضافة مزود بيانات ديناميكي لـ Hugging Face يجلب أوصاف الشركات والأخبار من مصادر قابلة للتهيئة مع كاش وفولباك آمن. |
| معدل | `freqtrade/huggingface/genetic_engine.py` | إضافة كاش للـ embeddings وفولباك صفري آمن عند تعطل النموذج أو الاعتمادات. |
| معدل | `freqtrade/huggingface/sentiment_engine.py` | تفعيل محرك المشاعر بفولباك لغوي بسيط عند غياب Transformers أو النموذج. |
| معدل | `freqtrade/strategies/FreqAIStrategy.py` | ربط الاستراتيجية بمزود البيانات الديناميكي وحفظ embeddings واستمرار الفولباك الآمن. |
| معدل | `bots/strategic_analyzer/bot.py` | إضافة منع تكرار الإشارات، تحسين logging لأسباب عدم ظهور الإشارة، ودعم SELL/HOLD بشكل متوافق خلفيًا. |
| معدل | `bots/strategic_analyzer/tasks.py` | إضافة منع التشغيل المتداخل وتسجيل حالة مهمة المحلل الاستراتيجي. |
| معدل | `scripts/celery_app.py` | تحسين إعدادات موثوقية Celery، إضافة تضمين مهمة الاحتفاظ، وجدولتها ضمن طابور الصيانة. |
| معدل | `bots/health_monitor/tasks.py` | تحويل مراقبة الصحة من stub إلى فحص فعلي لـ PostgreSQL وRedis وجداول الإشارات وحالة المهام الحرجة. |
| مضاف | `scripts/retention_policy.py` | إضافة سياسة احتفاظ بسيطة ومجدولة للبيانات التشغيلية القديمة مع ضغط TimescaleDB اختياري وآمن. |
| معدل | `docker-compose.yml` | ضبط `APP_ENV=production` و`LOG_LEVEL=INFO` للخدمات المناسبة وإضافة health checks تطبيقية. |
| غير مرتبط بالكود | `Alpha-Engine2_Audit_Report.md` | تقرير الفحص السابق موجود كملف غير متتبع في Git، ولم يكن جزءًا من تعديلات الكود الحالية. |

## 3. ما تم إصلاحه حسب كل محور

### 3.1 مدير FreqAI

كان مدير FreqAI يعتمد على توليد إعدادات داخل الذاكرة، ما يجعل التشغيل اليومي هشًا وصعب التتبع. تم تعديله بحيث يحفظ إعدادات Freqtrade/FreqAI فعليًا إلى ملف `config.json` في المسار المحدد، ثم يشغّل أمر التدريب وفق إعداد قابل للتهيئة، ويسجل حالة النموذج في Redis وداخل جدول حالة البوتات عند توفر قاعدة البيانات.

| قبل التعديل | بعد التعديل |
|---|---|
| إعدادات FreqAI لا تُحفظ كملف إنتاجي مضمون. | يتم حفظ `config.json` فعليًا قبل التدريب. |
| لا توجد حالة واضحة لنجاح/فشل التدريب. | تُسجل حالة النموذج: نجاح/فشل، وقت آخر تدريب، ومسار المخرجات. |
| احتمالية تشغيل متداخل للمهمة. | تمت إضافة lock لمنع التداخل في مهمة Celery. |
| أخطاء التدريب قد تكون غير واضحة تشغيليًا. | تمت إضافة logging وحفظ ملخص الحالة في Redis/status. |

### 3.2 تكامل Hugging Face

تمت إضافة طبقة بيانات ديناميكية بديلة للبيانات الثابتة السابقة. المزود الجديد يدعم مصادر قابلة للتهيئة مثل ملفات JSON/RSS، ويحتفظ بالكاش لتقليل الاعتماد على المصدر الخارجي في كل تشغيل. عند فشل المصدر أو غياب مكتبات Hugging Face/Transformers، يعمل النظام بفولباك آمن بدل إسقاط الاستراتيجية أو تعطيل التشغيل.

> الفولباك الحالي محافظ ومقصود: إذا تعذّر توليد embedding حقيقي، يستخدم النظام متجهًا صفريًا أو تحليلًا لغويًا بسيطًا للمشاعر حتى لا يتعطل مسار الإشارات أو التدريب.

| المجال | ما تم تنفيذه |
|---|---|
| مصادر الأخبار/الشركات | مزود بيانات ديناميكي مع دعم كاش وقراءة من مصادر قابلة للتهيئة. |
| Embeddings | حفظ embeddings في كاش ملفات وإعادة استخدامها. |
| فشل النموذج | فولباك آمن بدل إيقاف الاستراتيجية. |
| توافق الاستراتيجية | تحديث `FreqAIStrategy` لاستهلاك المزود الجديد دون كسر السلوك القديم. |

### 3.3 مسار الإشارات والمحلل الاستراتيجي

تمت إضافة طبقة Deduplication لمنع تكرار نفس الإشارة خلال نافذة زمنية، وتحسين الرسائل التشغيلية التي توضّح لماذا لم تظهر إشارة معينة. كما تمت إضافة دعم منطقي لإشارات `SELL` و`HOLD` عندما تسمح البيانات والنتائج بذلك، مع الحفاظ على التوافق الخلفي مع إشارات `BUY` الحالية.

| التحسين | الأثر الإنتاجي |
|---|---|
| Deduplication زمني | يمنع تكرار الإشارات المزعج خلال فترة قصيرة. |
| أسباب رفض الإشارة | يسهّل تشخيص حالات عدم ظهور الإشارة في اللوحة أو قاعدة البيانات. |
| SELL/HOLD | يوسّع المسار من إشارات شراء فقط إلى قرارات أكثر اكتمالًا. |
| حماية مهمة Celery | تقلل احتمالات التداخل أو تضارب الحفظ أثناء التشغيل المجدول. |

### 3.4 الجدولة والمهام الحرجة

تمت مراجعة `Celery Beat` وتحديثه ليشمل مهمة الاحتفاظ الجديدة، مع تحسين إعدادات موثوقية Celery العامة وربط مهام FreqAI والمحلل الاستراتيجي بحماية من التداخل وإعادة المحاولة. تم الحفاظ على الجدولة الحالية للمكونات الحرجة مثل market reporter، strategic analyzer، health monitor، FreqAI manager، والتنبيهات.

| المهمة | الحالة بعد التعديل |
|---|---|
| `bots.market_reporter.tasks.run_market_reporter` | موجودة ضمن الجدولة الحالية. |
| `bots.strategic_analyzer.tasks.run_strategic_analyzer` | موجودة، مع حماية تداخل وتحسينات logging. |
| `bots.health_monitor.tasks.run_health_monitor` | موجودة، وتم تفعيل فحوصات فعلية بدل stub. |
| `bots.freqai_manager.tasks.run_freqai_manager` | موجودة، مع lock وحفظ حالة التدريب. |
| `scripts.retention_policy.run_retention_policy` | تمت إضافتها وجدولتها يوميًا ضمن طابور الصيانة. |
| `scripts.telegram_bot.send_pending_alerts` | موجودة ضمن الجدولة الحالية كل دقيقة. |

### 3.5 إعدادات الإنتاج والمراقبة والاحتفاظ

تم ضبط `APP_ENV=production` و`LOG_LEVEL=INFO` في الخدمات المناسبة داخل `docker-compose.yml`، مع إضافة health checks تطبيقية للخدمات الرئيسية عندما يكون ذلك مناسبًا. كما تمت إضافة سياسة احتفاظ للبيانات التشغيلية القديمة، مع جعل ضغط TimescaleDB اختياريًا فقط عبر إعداد مستقل حتى لا يتسبب غياب الإضافة أو الدالة في تعطيل التنظيف.

| المجال | ما تم تنفيذه |
|---|---|
| بيئة التشغيل | ضبط `APP_ENV=production` في الخدمات التطبيقية المناسبة. |
| مستوى السجلات | ضبط `LOG_LEVEL=INFO` لتقليل الضجيج في الإنتاج. |
| Health Checks | إضافة فحوصات DB/Redis/API/Celery حيث أمكن. |
| Retention Policy | مهمة يومية لتنظيف البيانات التشغيلية القديمة وفق مدد افتراضية قابلة للتوسيع. |
| Compression | متروكة اختيارية ومغلقة افتراضيًا لتجنب مخاطر TimescaleDB غير المتاحة. |

## 4. نتائج الفحوصات

تم تشغيل الفحوصات المطلوبة بعد مجموعات التعديل، ثم تشغيل فحص نهائي شامل. نتيجة الفحص النهائي كانت كما يلي:

| الفحص | النتيجة | الملاحظة |
|---|---|---|
| `python3.11 -m compileall .` | ناجح | تم تجميع ملفات Python دون أخطاء syntax. |
| `git diff --check` | ناجح | لا توجد أخطاء whitespace واضحة في الفروقات. |
| YAML parse لـ `docker-compose.yml` | ناجح | تمت قراءة الملف بنجاح عبر PyYAML. |
| `docker compose config` | لم يُنفذ في هذه البيئة | أمر `docker` غير مثبت داخل sandbox الحالي، لذلك تعذر تشغيل الفحص الرسمي رغم أن YAML نفسه صالح. |

الأمر النهائي المستخدم للتحقق كان:

```bash
cd /home/ubuntu/Alpha-Engine2
python3.11 -m compileall .
git diff --check
python3.11 - <<'PY'
import yaml
from pathlib import Path
p = Path('docker-compose.yml')
yaml.safe_load(p.read_text(encoding='utf-8'))
print('docker_compose_yaml_parse=ok')
PY
docker compose config
```

## 5. أوامر التشغيل الإنتاجي الموصى بها

بعد مراجعة التغييرات محليًا، يوصى بتشغيل الأوامر التالية على بيئة تحتوي على Docker وDocker Compose:

```bash
cd /path/to/Alpha-Engine2
python3.11 -m compileall .
docker compose config
docker compose build
docker compose up -d postgres redis
docker compose up -d app celery_worker celery_beat flower api dashboard telegram_bot freqtrade
```

بعد الإقلاع، يوصى بمراقبة السجلات والحالة خلال أول دورة تشغيل كاملة:

```bash
docker compose ps
docker compose logs -f celery_worker celery_beat api freqtrade
```

ولتشغيل فحوصات وظيفية محددة بعد الإقلاع:

```bash
curl -s http://localhost:8000/health
curl -s http://localhost:8000/bots/status
curl -s http://localhost:8000/genetic/status
```

كما يمكن إطلاق مهام محددة يدويًا للتحقق من المسارات الحرجة قبل الاعتماد على الجدولة اليومية:

```bash
docker compose exec celery_worker celery -A scripts.celery_app call bots.freqai_manager.tasks.run_freqai_manager
docker compose exec celery_worker celery -A scripts.celery_app call bots.strategic_analyzer.tasks.run_strategic_analyzer
docker compose exec celery_worker celery -A scripts.celery_app call bots.health_monitor.tasks.run_health_monitor
docker compose exec celery_worker celery -A scripts.celery_app call scripts.retention_policy.run_retention_policy
```

## 6. ملاحظات ومخاطر باقية

رغم أن التعديلات رفعت مستوى الجاهزية التشغيلية، توجد نقاط ينبغي اختبارها على بيئة إنتاجية أو staging حقيقية قبل الاعتماد اليومي الكامل. أولًا، لم أستطع تشغيل `docker compose config` داخل sandbox لأن Docker غير مثبت، لذلك يجب إعادة هذا الفحص في بيئة Docker الفعلية. ثانيًا، تشغيل FreqAI الحقيقي يعتمد على توفر أمر Freqtrade، بيانات السوق، صلاحيات الكتابة للمجلدات، واعتمادات FreqAI داخل الصورة. ثالثًا، مصادر Hugging Face الديناميكية تعمل الآن مع كاش وفولباك، لكنها ستحتاج إلى ضبط مصادر الأخبار/الشركات المناسبة حسب مزود البيانات الذي تريد الاعتماد عليه إنتاجيًا.

| الخطر الباقي | مستوى الأهمية | الإجراء الموصى به |
|---|---:|---|
| عدم تحقق `docker compose config` في sandbox | مرتفع | تشغيله في خادم Docker الفعلي قبل النشر. |
| اعتماد تدريب FreqAI على توفر بيانات وأوامر Freqtrade | مرتفع | تشغيل تدريب تجريبي كامل داخل الحاوية ومراجعة مسار النموذج الناتج. |
| مصادر الأخبار الديناميكية تحتاج ضبطًا إنتاجيًا | متوسط | تعريف مصادر موثوقة في الإعدادات أو ملفات البيانات وربطها بدورة تحديث. |
| SELL/HOLD يحتاجان تحققًا ميدانيًا من جودة الإشارات | متوسط | مقارنة الإشارات الناتجة مع بيانات تاريخية قبل الاعتماد المالي. |
| سياسة الاحتفاظ افتراضية | متوسط | مراجعة مدد الاحتفاظ حسب احتياجات لوحة التحكم والتحليلات. |
| لم يتم تعديل الأسرار عمدًا | مقبول حسب طلبك | بقيت الأسرار في `config/config.yaml` كما طلبت دون تدخل. |

## 7. حالة Git الحالية

ملخص حالة الملفات بعد التنفيذ:

```text
M  bots/freqai_manager/bot.py
M  bots/freqai_manager/tasks.py
M  bots/health_monitor/tasks.py
M  bots/strategic_analyzer/bot.py
M  bots/strategic_analyzer/tasks.py
M  docker-compose.yml
M  freqtrade/huggingface/genetic_engine.py
M  freqtrade/huggingface/sentiment_engine.py
M  freqtrade/strategies/FreqAIStrategy.py
M  scripts/celery_app.py
?? freqtrade/huggingface/data_provider.py
?? scripts/retention_policy.py
?? PRODUCTION_STABILIZATION_REPORT.md
```

يوصى بعد مراجعة التقرير وتشغيل `docker compose config` على بيئة Docker الفعلية بإنشاء commit واضح، مثل:

```bash
git add bots/freqai_manager/bot.py \
        bots/freqai_manager/tasks.py \
        bots/health_monitor/tasks.py \
        bots/strategic_analyzer/bot.py \
        bots/strategic_analyzer/tasks.py \
        docker-compose.yml \
        freqtrade/huggingface/data_provider.py \
        freqtrade/huggingface/genetic_engine.py \
        freqtrade/huggingface/sentiment_engine.py \
        freqtrade/strategies/FreqAIStrategy.py \
        scripts/celery_app.py \
        scripts/retention_policy.py \
        PRODUCTION_STABILIZATION_REPORT.md

git commit -m "Stabilize production runtime integrations"
```

## 8. الخلاصة

تم تنفيذ التعديلات المطلوبة ضمن الحدود التي حددتها: لم يتم لمس الأسرار، ولم يتم تعديل المحرك الجيني أو Scientist. أصبح المشروع أكثر جاهزية للتشغيل اليومي بفضل حفظ حالة FreqAI، تفعيل مصادر Hugging Face الديناميكية مع فولباك آمن، تحسين مسار الإشارات ومنع التكرار، تقوية Celery، إضافة مراقبة صحة فعلية، وتطبيق سياسة احتفاظ مجدولة. الخطوة التالية الحاسمة هي تشغيل `docker compose config` وبناء الحاويات وتشغيل دورة FreqAI/Strategic Analyzer كاملة على بيئة Docker فعلية للتأكد من صلاحيات الملفات وتوفر الاعتمادات وظهور الإشارات في قاعدة البيانات والواجهة.
