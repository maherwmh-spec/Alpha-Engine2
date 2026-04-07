#!/usr/bin/env bash
# =============================================================================
# scripts/apply_migrations.sh
# =============================================================================
# تشغيل ملفات migration بالترتيب على قاعدة البيانات.
#
# الاستخدام (من مجلد المشروع):
#   # داخل Docker:
#   docker compose exec postgres psql -U alpha_user -d alpha_engine \
#       -f /migrations/004_add_name_column_to_symbols.sql
#
#   # أو تشغيل هذا السكربت مباشرة (يتصل بـ postgres عبر Docker):
#   bash scripts/apply_migrations.sh
#
# المتطلبات: docker compose يعمل، postgres container جاهز
# =============================================================================

set -euo pipefail

# ── إعدادات الاتصال ───────────────────────────────────────────────────────────
CONTAINER="alpha_postgres"
DB_USER="alpha_user"
DB_NAME="alpha_engine"
MIGRATIONS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../migrations" && pwd)"

echo "============================================="
echo "  Alpha Engine2 — Database Migrations"
echo "============================================="
echo "  Container : $CONTAINER"
echo "  Database  : $DB_NAME"
echo "  User      : $DB_USER"
echo "  Dir       : $MIGRATIONS_DIR"
echo "============================================="

# ── التحقق من أن الحاوية تعمل ────────────────────────────────────────────────
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
    echo "❌ Container '$CONTAINER' is not running."
    echo "   Run: docker compose up -d postgres"
    exit 1
fi

# ── دالة تشغيل ملف SQL ───────────────────────────────────────────────────────
run_migration() {
    local file="$1"
    local filename
    filename=$(basename "$file")

    echo ""
    echo "▶ Running: $filename"

    # نسخ الملف إلى الحاوية ثم تشغيله
    docker cp "$file" "${CONTAINER}:/tmp/${filename}"
    docker exec "$CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" \
        -v ON_ERROR_STOP=1 \
        -f "/tmp/${filename}" \
        2>&1 | sed 's/^/   /'

    if [ "${PIPESTATUS[0]}" -eq 0 ]; then
        echo "   ✅ $filename — OK"
    else
        echo "   ❌ $filename — FAILED"
        exit 1
    fi
}

# ── تشغيل migration 004 فقط (الأعمدة الجديدة) ───────────────────────────────
MIGRATION_004="${MIGRATIONS_DIR}/004_add_name_column_to_symbols.sql"

if [ -f "$MIGRATION_004" ]; then
    run_migration "$MIGRATION_004"
else
    echo "❌ File not found: $MIGRATION_004"
    exit 1
fi

# ── التحقق النهائي ────────────────────────────────────────────────────────────
echo ""
echo "============================================="
echo "  Verification"
echo "============================================="

docker exec "$CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -c "
SELECT
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'market_data'
  AND table_name   = 'symbols'
ORDER BY ordinal_position;
" 2>&1

echo ""
echo "  Sample data (symbol + name):"
docker exec "$CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -c "
SELECT symbol, name, name_ar, is_active
FROM market_data.symbols
ORDER BY symbol
LIMIT 10;
" 2>&1

echo ""
echo "============================================="
echo "  ✅ Migration 004 applied successfully!"
echo "============================================="
