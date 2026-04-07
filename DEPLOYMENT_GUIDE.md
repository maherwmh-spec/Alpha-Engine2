# دليل تطبيق الإصلاحات على الخادم

## الخطوات المطلوبة منك على الخادم (بالترتيب)

---

### الخطوة 1 — سحب آخر التغييرات من GitHub

```bash
cd /path/to/Alpha-Engine2
git pull origin master
```

**التحقق:**
```bash
git log --oneline -2
# يجب أن ترى:
# 4dec20d fix: تحسين migration 004 وإضافة PGTZ وسكربت التطبيق
# cd9e826 fix: إصلاح شامل لـ 5 مشاكل جذرية في النظام
```

---

### الخطوة 2 — تشغيل migration قاعدة البيانات (FIX #4)

> **هذا هو الإصلاح الذي يحل خطأ `column "name" does not exist`**

```bash
# الطريقة الأسرع — مباشرة عبر psql في الحاوية:
docker compose exec postgres psql -U alpha_user -d alpha_engine \
    -f /dev/stdin < migrations/004_add_name_column_to_symbols.sql
```

**أو باستخدام السكربت المرفق:**
```bash
bash scripts/apply_migrations.sh
```

**التحقق من نجاح الـ migration:**
```bash
docker compose exec postgres psql -U alpha_user -d alpha_engine -c \
    "SELECT column_name FROM information_schema.columns
     WHERE table_schema='market_data' AND table_name='symbols'
     ORDER BY ordinal_position;"
```

**النتيجة المتوقعة** — يجب أن ترى عمود `name` في القائمة:
```
 column_name
--------------
 symbol
 name_ar
 name_en
 sector_id
 sector_name_ar
 market
 is_active
 listing_date
 isin
 last_synced_at
 created_at
 updated_at
 name          ← هذا العمود الجديد
```

**اختبار الاستعلام الذي كان يفشل:**
```bash
docker compose exec postgres psql -U alpha_user -d alpha_engine -c \
    "SELECT symbol, name, name_ar FROM market_data.symbols LIMIT 5;"
```

---

### الخطوة 3 — إعادة بناء وتشغيل الحاويات (FIX #0)

> **هذا هو الإصلاح الذي يحل مشكلة التوقيت UTC**

```bash
# إيقاف الحاويات الجارية
docker compose down

# إعادة البناء (لتطبيق TZ=Asia/Riyadh والمكتبات الجديدة)
docker compose build --no-cache

# تشغيل الحاويات
docker compose up -d
```

**التحقق من التوقيت:**
```bash
# التحقق من توقيت حاوية postgres
docker compose exec postgres date
# النتيجة المتوقعة: Mon Apr  7 05:XX:XX AST 2025  (توقيت +3)

# التحقق من توقيت قاعدة البيانات
docker compose exec postgres psql -U alpha_user -d alpha_engine -c \
    "SELECT NOW(), current_setting('timezone');"
# النتيجة المتوقعة: timezone = Asia/Riyadh
```

---

### الخطوة 4 — تشغيل مزامنة الأسهم (FIX #4 — الأسماء العربية)

```bash
docker compose exec app python scripts/sync_symbols.py
```

**التحقق من الأسماء:**
```bash
docker compose exec postgres psql -U alpha_user -d alpha_engine -c \
    "SELECT symbol, name, name_ar, is_active
     FROM market_data.symbols
     WHERE is_active = TRUE
     ORDER BY symbol
     LIMIT 15;"
```

---

### الخطوة 5 — التحقق الشامل النهائي

```bash
# 1. عدد الأسهم النشطة (يجب أن يكون 273+)
docker compose exec postgres psql -U alpha_user -d alpha_engine -c \
    "SELECT COUNT(*) FROM market_data.symbols WHERE is_active = TRUE;"

# 2. التوقيت (يجب Asia/Riyadh)
docker compose exec postgres date
docker compose exec app date

# 3. عمود name موجود (لا يجب أن يعطي خطأ)
docker compose exec postgres psql -U alpha_user -d alpha_engine -c \
    "SELECT symbol, name FROM market_data.symbols LIMIT 3;"

# 4. حالة الحاويات
docker compose ps
```

---

## ملخص الإصلاحات في الـ Repository

| FIX | الملف | ما تم |
|-----|-------|-------|
| #0 | `docker-compose.yml` | `TZ=Asia/Riyadh` + `PGTZ=Asia/Riyadh` لجميع الخدمات العشر |
| #1 | `scripts/sahmk_client.py` | `_get_active_symbols_from_db()` تقرأ `WHERE is_active=TRUE` |
| #2 | `dashboard/arabic_utils.py` + `requirements.txt` | `arabic-reshaper` + `python-bidi` + وحدة `fix_arabic()` |
| #3 | `dashboard/app.py` | استعلام `WHERE is_active=TRUE AND market='TASI'` بدلاً من `DISTINCT ohlcv` |
| #4 | `migrations/004_add_name_column_to_symbols.sql` + `scripts/sync_symbols.py` | `ALTER TABLE ADD COLUMN name` + جلب الأسماء العربية |

**ملاحظة مهمة:** الملفات في GitHub صحيحة 100%. ما تحتاجه هو:
1. `git pull` لسحب التغييرات
2. تشغيل migration 004 مرة واحدة على قاعدة البيانات
3. `docker compose down && docker compose build --no-cache && docker compose up -d`
