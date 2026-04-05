"""
Sync Symbols  —  scripts/sync_symbols.py
==========================================
يجلب القائمة الكاملة لأسهم سوق تاسي الرئيسي من SAHMK API
ويحفظها في market_data.symbols.

القواعد:
  - فقط رموز تاسي الرئيسي: طول 4 أرقام يبدأ بـ [1-8]
  - يستبعد تلقائياً: نمو (9xxx) + ETFs + القطاعات (900xx)
  - يُحدِّث الجدول بـ UPSERT (لا يحذف الأسهم القديمة بل يضع is_active=False)

الاستخدام:
  python3 scripts/sync_symbols.py                    # تشغيل مباشر
  python3 scripts/sync_symbols.py --dry-run          # معاينة بدون حفظ
  python3 scripts/sync_symbols.py --force-refresh    # تجاهل الكاش

الدوال المُصدَّرة:
  - sync_tasi_symbols()          → Dict (نتائج المزامنة)
  - get_tasi_symbols_from_db()   → List[str] (للاستخدام في symbol_universe)
"""

import re
import sys
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from loguru import logger

# ── نمط رموز تاسي الرئيسي ────────────────────────────────────────────────────
_TASI_SYMBOL_RE = re.compile(r'^[1-8][0-9]{3}$')


def is_tasi_main_market(symbol: str) -> bool:
    """True فقط لأسهم تاسي الرئيسي (4 أرقام تبدأ بـ 1-8)."""
    return bool(_TASI_SYMBOL_RE.match(str(symbol).strip()))


# ── قاموس القطاعات (للإثراء عند الحفظ) ──────────────────────────────────────
SECTOR_MAP: Dict[str, str] = {
    '90010': 'البنوك',
    '90011': 'السلع الرأسمالية',
    '90012': 'الخدمات التجارية والمهنية',
    '90013': 'السلع الاستهلاكية التقديرية',
    '90014': 'السلع المعمّرة والملابس',
    '90015': 'السلع الاستهلاكية الأساسية',
    '90016': 'خدمات المستهلك',
    '90017': 'الطاقة',
    '90018': 'الخدمات المالية',
    '90019': 'الأغذية والمشروبات',
    '90020': 'الرعاية الصحية',
    '90021': 'التأمين',
    '90022': 'المواد الأساسية',
    '90023': 'الإعلام والترفيه',
    '90024': 'الأدوية والتقنية الحيوية',
    '90025': 'صناديق الاستثمار العقاري',
    '90026': 'إدارة وتطوير العقارات',
    '90027': 'البرمجيات والخدمات',
    '90028': 'خدمات الاتصالات',
    '90029': 'النقل',
    '90030': 'المرافق',
}


# ─────────────────────────────────────────────────────────────────────────────
# الدالة الرئيسية
# ─────────────────────────────────────────────────────────────────────────────

def sync_tasi_symbols(dry_run: bool = False, force_refresh: bool = False) -> Dict:
    """
    يجلب قائمة أسهم تاسي من SAHMK API ويحفظها في market_data.symbols.

    Args:
        dry_run: إذا True — يطبع النتائج فقط دون حفظ في DB
        force_refresh: إذا True — يتجاهل الكاش ويجلب من API مباشرة

    Returns:
        {
            'total_fetched': int,
            'tasi_count': int,
            'excluded_count': int,
            'upserted': int,
            'deactivated': int,
            'symbols': List[str],
            'timestamp': str,
        }
    """
    result = {
        'total_fetched': 0,
        'tasi_count': 0,
        'excluded_count': 0,
        'upserted': 0,
        'deactivated': 0,
        'symbols': [],
        'timestamp': datetime.utcnow().isoformat(),
        'error': None,
    }

    # ── 1. جلب الرموز من SAHMK API ──────────────────────────────────────────
    try:
        from scripts.sahmk_client import get_sahmk_client
        client = get_sahmk_client()

        logger.info("🔄 sync_symbols: Fetching symbols from SAHMK API...")
        raw_symbols = client.get_symbols_list()

        if not raw_symbols:
            logger.warning("⚠️ sync_symbols: SAHMK returned empty symbols list")
            result['error'] = 'SAHMK returned empty list'
            return result

        result['total_fetched'] = len(raw_symbols)
        logger.info(f"📋 sync_symbols: Received {len(raw_symbols)} raw symbols from SAHMK")

    except Exception as e:
        logger.error(f"❌ sync_symbols: Failed to fetch from SAHMK: {e}")
        result['error'] = str(e)
        # Fallback: جلب من DB إذا فشل API
        return _sync_from_db_fallback(result, dry_run)

    # ── 2. فلترة رموز تاسي الرئيسي فقط ──────────────────────────────────────
    tasi_symbols = []
    excluded = []

    for sym in raw_symbols:
        sym_str = str(sym).strip()
        if is_tasi_main_market(sym_str):
            tasi_symbols.append(sym_str)
        else:
            excluded.append(sym_str)

    result['tasi_count'] = len(tasi_symbols)
    result['excluded_count'] = len(excluded)
    result['symbols'] = sorted(tasi_symbols)

    excluded_9 = [s for s in excluded if s.startswith('9') and len(s) == 4]
    logger.info(
        f"✅ sync_symbols: {len(tasi_symbols)} TASI symbols | "
        f"{len(excluded)} excluded "
        f"({len(excluded_9)} Nomu/ETFs starting with 9)"
    )

    if dry_run:
        logger.info(f"🔍 DRY RUN — would upsert {len(tasi_symbols)} symbols")
        logger.info(f"   Sample: {sorted(tasi_symbols)[:10]}")
        return result

    # ── 3. جلب تفاصيل الأسهم (الاسم، القطاع) من API ─────────────────────────
    symbols_data = _enrich_symbols(client, tasi_symbols)

    # ── 4. حفظ في DB ─────────────────────────────────────────────────────────
    upserted, deactivated = _upsert_symbols_to_db(symbols_data, tasi_symbols)
    result['upserted'] = upserted
    result['deactivated'] = deactivated

    logger.info(
        f"✅ sync_symbols complete: {upserted} upserted, {deactivated} deactivated "
        f"({len(tasi_symbols)} active TASI symbols)"
    )
    return result


def _enrich_symbols(client, symbols: List[str]) -> List[Dict]:
    """
    يجلب تفاصيل كل سهم (الاسم، القطاع) من SAHMK API.
    يعمل بشكل batch لتقليل عدد الطلبات.
    """
    from scripts.sector_calculator import STOCK_TO_SECTOR

    enriched = []
    for sym in symbols:
        try:
            quote = client.get_quote(sym)
            sector_id = STOCK_TO_SECTOR.get(sym, '')
            enriched.append({
                'symbol':        sym,
                'name_ar':       quote.get('name_ar') or quote.get('name') or sym,
                'name_en':       quote.get('name_en') or quote.get('name_en') or sym,
                'sector_id':     sector_id,
                'sector_name_ar': SECTOR_MAP.get(sector_id, ''),
                'market':        'TASI',
                'is_active':     True,
            })
        except Exception:
            # إذا فشل جلب التفاصيل — أضف بمعلومات أساسية
            from scripts.sector_calculator import STOCK_TO_SECTOR
            sector_id = STOCK_TO_SECTOR.get(sym, '')
            enriched.append({
                'symbol':        sym,
                'name_ar':       sym,
                'name_en':       sym,
                'sector_id':     sector_id,
                'sector_name_ar': SECTOR_MAP.get(sector_id, ''),
                'market':        'TASI',
                'is_active':     True,
            })

    logger.info(f"📊 _enrich_symbols: enriched {len(enriched)}/{len(symbols)} symbols")
    return enriched


def _upsert_symbols_to_db(symbols_data: List[Dict], active_symbols: List[str]) -> Tuple[int, int]:
    """
    يُدرج أو يُحدِّث الأسهم في market_data.symbols.
    يضع is_active=False للأسهم التي لم تعد في القائمة.
    """
    try:
        from scripts.db_manager import db
        from sqlalchemy import text

        upsert_sql = text("""
        INSERT INTO market_data.symbols
            (symbol, name_ar, name_en, sector_id, sector_name_ar, market, is_active, last_synced_at)
        VALUES
            (:symbol, :name_ar, :name_en, :sector_id, :sector_name_ar, :market, :is_active, NOW())
        ON CONFLICT (symbol) DO UPDATE SET
            name_ar        = EXCLUDED.name_ar,
            name_en        = EXCLUDED.name_en,
            sector_id      = EXCLUDED.sector_id,
            sector_name_ar = EXCLUDED.sector_name_ar,
            market         = EXCLUDED.market,
            is_active      = EXCLUDED.is_active,
            last_synced_at = NOW(),
            updated_at     = NOW();
        """)

        with db.get_session() as session:
            # Upsert الأسهم النشطة
            for row in symbols_data:
                session.execute(upsert_sql, row)

            # وضع is_active=False للأسهم التي لم تعد في القائمة
            if active_symbols:
                placeholders = ','.join([f':s{i}' for i in range(len(active_symbols))])
                params = {f's{i}': sym for i, sym in enumerate(active_symbols)}
                deactivate_result = session.execute(text(f"""
                    UPDATE market_data.symbols
                    SET is_active = FALSE, updated_at = NOW()
                    WHERE market = 'TASI'
                      AND symbol NOT IN ({placeholders})
                      AND is_active = TRUE
                """), params)
                deactivated = deactivate_result.rowcount
            else:
                deactivated = 0

            session.commit()

        return len(symbols_data), deactivated

    except Exception as e:
        logger.error(f"❌ _upsert_symbols_to_db error: {e}")
        return 0, 0


def _sync_from_db_fallback(result: Dict, dry_run: bool) -> Dict:
    """
    Fallback: إذا فشل SAHMK API، يجلب الرموز من market_data.ohlcv
    ويحفظها في market_data.symbols.
    """
    logger.warning("⚠️ sync_symbols: Using DB fallback (SAHMK API unavailable)")
    try:
        from scripts.db_manager import db
        from sqlalchemy import text
        from scripts.sahmk_client import is_tasi_or_sector

        with db.get_session() as session:
            rows = session.execute(text("""
                SELECT DISTINCT symbol
                FROM market_data.ohlcv
                WHERE symbol ~ '^[1-8][0-9]{3}$'
                ORDER BY symbol
            """)).fetchall()

        tasi_symbols = [r[0] for r in rows]
        result['tasi_count'] = len(tasi_symbols)
        result['symbols'] = tasi_symbols
        result['error'] = 'SAHMK unavailable — used DB fallback'

        logger.info(
            f"📋 sync_symbols fallback: {len(tasi_symbols)} symbols from DB ohlcv"
        )

        if not dry_run and tasi_symbols:
            from scripts.sector_calculator import STOCK_TO_SECTOR
            symbols_data = [{
                'symbol':        sym,
                'name_ar':       sym,
                'name_en':       sym,
                'sector_id':     STOCK_TO_SECTOR.get(sym, ''),
                'sector_name_ar': SECTOR_MAP.get(STOCK_TO_SECTOR.get(sym, ''), ''),
                'market':        'TASI',
                'is_active':     True,
            } for sym in tasi_symbols]
            upserted, deactivated = _upsert_symbols_to_db(symbols_data, tasi_symbols)
            result['upserted'] = upserted
            result['deactivated'] = deactivated

    except Exception as e:
        logger.error(f"❌ sync_symbols fallback error: {e}")
        result['error'] = str(e)

    return result


# ─────────────────────────────────────────────────────────────────────────────
# دالة القراءة من DB (للاستخدام في symbol_universe)
# ─────────────────────────────────────────────────────────────────────────────

def get_tasi_symbols_from_db(include_inactive: bool = False) -> List[str]:
    """
    يُعيد قائمة أسهم تاسي من market_data.symbols.
    يُستخدم بدلاً من SELECT DISTINCT FROM ohlcv.

    Args:
        include_inactive: إذا True — يشمل الأسهم غير النشطة

    Returns:
        قائمة مرتبة برموز تاسي
    """
    try:
        from scripts.db_manager import db
        from sqlalchemy import text

        where_clause = "WHERE market = 'TASI'" + (
            "" if include_inactive else " AND is_active = TRUE"
        )

        with db.get_session() as session:
            rows = session.execute(text(f"""
                SELECT symbol FROM market_data.symbols
                {where_clause}
                ORDER BY symbol
            """)).fetchall()

        symbols = [r[0] for r in rows]

        if symbols:
            logger.info(
                f"📋 get_tasi_symbols_from_db: {len(symbols)} TASI symbols "
                f"({'active only' if not include_inactive else 'all'})"
            )
            return symbols

        # Fallback إذا كان الجدول فارغاً
        logger.warning(
            "⚠️ market_data.symbols is empty — falling back to ohlcv discovery. "
            "Run sync_symbols to populate the table."
        )
        return _fallback_from_ohlcv()

    except Exception as e:
        logger.error(f"❌ get_tasi_symbols_from_db error: {e}")
        return _fallback_from_ohlcv()


def _fallback_from_ohlcv() -> List[str]:
    """Fallback: يجلب الرموز من ohlcv إذا كان جدول symbols فارغاً."""
    try:
        from scripts.db_manager import db
        from sqlalchemy import text

        with db.get_session() as session:
            rows = session.execute(text("""
                SELECT DISTINCT symbol
                FROM market_data.ohlcv
                WHERE symbol ~ '^[1-8][0-9]{3}$'
                ORDER BY symbol
            """)).fetchall()

        symbols = [r[0] for r in rows]
        logger.info(f"📋 _fallback_from_ohlcv: {len(symbols)} symbols from ohlcv")
        return symbols

    except Exception as e:
        logger.error(f"❌ _fallback_from_ohlcv error: {e}")
        return []


# ─────────────────────────────────────────────────────────────────────────────
# Celery Task
# ─────────────────────────────────────────────────────────────────────────────

def register_celery_task(app):
    """
    يُسجّل مهمة Celery لمزامنة الرموز اليومية.
    يُستدعى من scripts/celery_app.py.
    """
    @app.task(
        name='scripts.sync_symbols.sync_symbols_task',
        bind=True,
        max_retries=3,
        default_retry_delay=300,
    )
    def sync_symbols_task(self):
        """مهمة Celery: تُزامن قائمة أسهم تاسي يومياً من SAHMK API."""
        logger.info("🔄 Celery sync_symbols_task: Starting daily symbol sync...")
        try:
            result = sync_tasi_symbols()
            logger.info(
                f"✅ sync_symbols_task complete: "
                f"{result['tasi_count']} TASI symbols, "
                f"{result['upserted']} upserted, "
                f"{result['deactivated']} deactivated"
            )
            return result
        except Exception as exc:
            logger.error(f"❌ sync_symbols_task failed: {exc}")
            raise self.retry(exc=exc)

    return sync_symbols_task


# ─────────────────────────────────────────────────────────────────────────────
# Celery shared_task (يُسجَّل تلقائياً عند include=['scripts.sync_symbols'])
# استخدام shared_task يتجنّب circular import مع celery_app.py
# ─────────────────────────────────────────────────────────────────────────────

from celery import shared_task


@shared_task(
    name='scripts.sync_symbols.sync_symbols_task',
    bind=True,
    max_retries=3,
    default_retry_delay=300,
)
def sync_symbols_task(self):
    """مهمة Celery: تُزامن قائمة أسهم تاسي يومياً من SAHMK API."""
    logger.info("🔄 sync_symbols_task: Starting daily symbol sync...")
    try:
        result = sync_tasi_symbols()
        logger.info(
            f"✅ sync_symbols_task complete: "
            f"{result['tasi_count']} TASI symbols, "
            f"{result['upserted']} upserted, "
            f"{result['deactivated']} deactivated"
        )
        return result
    except Exception as exc:
        logger.error(f"❌ sync_symbols_task failed: {exc}")
        raise self.retry(exc=exc)


# ─────────────────────────────────────────────────────────────────────────────
# تشغيل مباشر
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Sync TASI symbols from SAHMK API to DB')
    parser.add_argument('--dry-run',       action='store_true', help='Preview without saving')
    parser.add_argument('--force-refresh', action='store_true', help='Ignore cache')
    parser.add_argument('--show-symbols',  action='store_true', help='Print all symbols')
    args = parser.parse_args()

    print('\n' + '=' * 65)
    print('🔄  Sync TASI Symbols — Alpha-Engine2')
    print('=' * 65 + '\n')

    result = sync_tasi_symbols(
        dry_run=args.dry_run,
        force_refresh=args.force_refresh,
    )

    print(f"\n📊 Results:")
    print(f"   Total fetched from API : {result['total_fetched']}")
    print(f"   TASI main-market       : {result['tasi_count']}")
    print(f"   Excluded (Nomu/ETFs)   : {result['excluded_count']}")
    print(f"   Upserted in DB         : {result['upserted']}")
    print(f"   Deactivated in DB      : {result['deactivated']}")

    if result.get('error'):
        print(f"\n⚠️  Warning: {result['error']}")

    if args.show_symbols and result['symbols']:
        print(f"\n📋 TASI Symbols ({len(result['symbols'])}):")
        for i, sym in enumerate(sorted(result['symbols']), 1):
            print(f"   {i:3}. {sym}")

    print(f'\n{"=" * 65}\n')
