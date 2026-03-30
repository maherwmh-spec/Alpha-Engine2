#!/bin/bash
# =============================================================
# apply_sectors_migration.sh
# يُطبّق migration_sectors.sql على قاعدة بيانات Alpha-Engine2
# الاستخدام: bash scripts/apply_sectors_migration.sh
# =============================================================

set -e

MIGRATION_FILE="migration_sectors.sql"
CONTAINER="alpha_postgres"

echo "======================================================"
echo "  Alpha-Engine2 - تطبيق migration القطاعات"
echo "======================================================"

# التحقق من وجود ملف الـ migration
if [ ! -f "$MIGRATION_FILE" ]; then
    echo "❌ خطأ: الملف $MIGRATION_FILE غير موجود"
    echo "   تأكد من تشغيل الأمر من مجلد المشروع الرئيسي"
    exit 1
fi

# التحقق من أن Docker يعمل
if ! docker ps &>/dev/null; then
    echo "❌ خطأ: Docker غير متاح أو لا يعمل"
    exit 1
fi

# التحقق من أن حاوية postgres تعمل
if ! docker ps --format '{{.Names}}' | grep -q "$CONTAINER"; then
    echo "❌ خطأ: حاوية postgres ($CONTAINER) غير موجودة أو متوقفة"
    echo "   شغّل: docker compose up -d postgres"
    exit 1
fi

echo "✅ حاوية postgres تعمل"
echo "📋 تطبيق migration_sectors.sql..."

# نسخ الملف إلى الحاوية وتنفيذه
docker cp "$MIGRATION_FILE" "$CONTAINER:/tmp/migration_sectors.sql"
docker exec -i "$CONTAINER" psql \
    -U alpha_user \
    -d alpha_engine \
    -f /tmp/migration_sectors.sql

echo ""
echo "======================================================"
echo "✅ تم تطبيق migration القطاعات بنجاح!"
echo "======================================================"

# التحقق من النتيجة
echo ""
echo "📊 القطاعات المُدرجة في قاعدة البيانات:"
docker exec -i "$CONTAINER" psql \
    -U alpha_user \
    -d alpha_engine \
    -c "SELECT sector_id, symbol, name_en, name_ar FROM market_data.sectors ORDER BY sector_id;" \
    2>/dev/null || echo "⚠️  تعذّر عرض القطاعات (ربما لم يُطبَّق الـ migration بعد)"
