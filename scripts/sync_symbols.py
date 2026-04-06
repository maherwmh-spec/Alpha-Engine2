"""
scripts/sync_symbols.py
=======================
مزامنة قائمة أسهم تاسي الرسمية من موقع أرقام (argaam.com).

المصدر الوحيد للحقيقة:
  https://www.argaam.com/ar/company/companies-prices?market=3
  → يعرض جداول HTML تحتوي على أسهم تاسي فقط (market_id=3)
  → لا يحتوي على أسهم نمو (market_id=14) أو ETFs

المنطق:
  1. Scraping من argaam → قائمة نظيفة ~230-280 رمز تاسي
  2. Upsert في market_data.symbols (is_active=True)
  3. تعطيل (is_active=False) أي رمز موجود في DB لم يعد في القائمة الرسمية

الاستخدام:
  python3 scripts/sync_symbols.py          # تشغيل فعلي مع حفظ في DB
  python3 scripts/sync_symbols.py --dry-run  # اختبار بدون حفظ
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
import time
from datetime import datetime, timezone
from typing import Optional

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ─── إعدادات الـ Scraping ────────────────────────────────────────────────────

_ARGAAM_TASI_URL = "https://www.argaam.com/ar/company/companies-prices?market=3"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ar,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.argaam.com/ar/",
}

_TASI_SYMBOL_RE = re.compile(r"^[1-8]\d{3}$")

# ─── دالة الفلترة ────────────────────────────────────────────────────────────

def is_tasi_main_market(symbol: str) -> bool:
    """True فقط لأسهم تاسي الرئيسي: 4 أرقام تبدأ من [1-8]."""
    return bool(_TASI_SYMBOL_RE.match(str(symbol).strip()))


# ─── Scraping من argaam ──────────────────────────────────────────────────────

def scrape_tasi_symbols_from_argaam(
    retries: int = 3,
    timeout: int = 20,
) -> list[str]:
    """
    يجلب قائمة أسهم تاسي من جداول HTML في argaam.com.

    يستخرج الرمز من العمود الأول في كل صف من جداول الأسعار.
    يُطبّق فلتر is_tasi_main_market() للتأكد من نظافة القائمة.

    Returns:
        قائمة مرتبة من رموز تاسي (مثل ['1010', '1020', ...])
    Raises:
        RuntimeError: إذا فشل الجلب بعد كل المحاولات
    """
    last_error: Optional[Exception] = None

    for attempt in range(1, retries + 1):
        try:
            logger.info(
                f"[sync_symbols] Scraping argaam.com (attempt {attempt}/{retries})..."
            )
            resp = requests.get(
                _ARGAAM_TASI_URL,
                headers=_HEADERS,
                timeout=timeout,
            )
            resp.raise_for_status()

            soup = BeautifulSoup(resp.text, "html.parser")
            symbols: set[str] = set()

            for table in soup.find_all("table"):
                for row in table.find_all("tr"):
                    cells = row.find_all("td")
                    if not cells:
                        continue
                    candidate = cells[0].get_text(strip=True)
                    if is_tasi_main_market(candidate):
                        symbols.add(candidate)

            if not symbols:
                raise ValueError(
                    "No TASI symbols found in argaam HTML — page structure may have changed"
                )

            result = sorted(symbols)
            logger.info(
                f"[sync_symbols] ✅ Scraped {len(result)} TASI symbols from argaam.com"
            )
            return result

        except Exception as exc:
            last_error = exc
            logger.warning(
                f"[sync_symbols] Attempt {attempt} failed: {exc}"
            )
            if attempt < retries:
                time.sleep(5 * attempt)

    raise RuntimeError(
        f"[sync_symbols] All {retries} scraping attempts failed. "
        f"Last error: {last_error}"
    )


# ─── حفظ في قاعدة البيانات ───────────────────────────────────────────────────

def _upsert_symbols_to_db(symbols: list[str]) -> dict:
    """
    يُنفّذ upsert للرموز في market_data.symbols:
    - يُضيف الرموز الجديدة
    - يُعيد تفعيل الرموز التي كانت معطّلة
    - يُعطّل الرموز التي لم تعد في القائمة الرسمية

    Returns:
        dict: إحصائيات العملية
    """
    from scripts.database import db

    now = datetime.now(timezone.utc)
    symbols_set = set(symbols)

    stats = {
        "inserted": 0,
        "reactivated": 0,
        "deactivated": 0,
        "unchanged": 0,
    }

    with db.get_session() as session:
        # ── 1. جلب الرموز الموجودة في DB ─────────────────────────────────
        existing_rows = session.execute(
            """
            SELECT symbol, is_active
            FROM market_data.symbols
            WHERE market = 'TASI'
            """
        ).fetchall()

        existing_map = {row[0]: row[1] for row in existing_rows}
        existing_symbols = set(existing_map.keys())

        # ── 2. Upsert الرموز الجديدة من argaam ───────────────────────────
        for sym in symbols:
            if sym not in existing_symbols:
                # رمز جديد — أضفه
                session.execute(
                    """
                    INSERT INTO market_data.symbols
                        (symbol, market, is_active, last_synced_at)
                    VALUES
                        (:symbol, 'TASI', TRUE, :now)
                    ON CONFLICT (symbol) DO UPDATE SET
                        is_active = TRUE,
                        last_synced_at = :now
                    """,
                    {"symbol": sym, "now": now},
                )
                stats["inserted"] += 1
            elif not existing_map[sym]:
                # رمز موجود لكن معطّل — أعد تفعيله
                session.execute(
                    """
                    UPDATE market_data.symbols
                    SET is_active = TRUE, last_synced_at = :now
                    WHERE symbol = :symbol
                    """,
                    {"symbol": sym, "now": now},
                )
                stats["reactivated"] += 1
            else:
                # رمز موجود ونشط — حدّث وقت المزامنة فقط
                session.execute(
                    """
                    UPDATE market_data.symbols
                    SET last_synced_at = :now
                    WHERE symbol = :symbol
                    """,
                    {"symbol": sym, "now": now},
                )
                stats["unchanged"] += 1

        # ── 3. تعطيل الرموز التي لم تعد في القائمة الرسمية ──────────────
        stale_symbols = existing_symbols - symbols_set
        if stale_symbols:
            for sym in stale_symbols:
                session.execute(
                    """
                    UPDATE market_data.symbols
                    SET is_active = FALSE, last_synced_at = :now
                    WHERE symbol = :symbol AND market = 'TASI'
                    """,
                    {"symbol": sym, "now": now},
                )
                stats["deactivated"] += 1
            logger.info(
                f"[sync_symbols] 🚫 Deactivated {len(stale_symbols)} stale symbols: "
                f"{sorted(stale_symbols)[:20]}{'...' if len(stale_symbols) > 20 else ''}"
            )

        session.commit()

    return stats


# ─── الدالة الرئيسية ─────────────────────────────────────────────────────────

def sync_tasi_symbols(dry_run: bool = False) -> dict:
    """
    تُزامن قائمة أسهم تاسي من argaam.com إلى market_data.symbols.

    Args:
        dry_run: إذا True، يجلب القائمة ويطبعها بدون حفظ في DB

    Returns:
        dict مع مفاتيح: tasi_count, symbols, db_stats, source
    """
    # ── الخطوة 1: Scraping من argaam ─────────────────────────────────────
    symbols = scrape_tasi_symbols_from_argaam()

    result = {
        "tasi_count": len(symbols),
        "symbols": symbols,
        "source": "argaam.com scraping",
        "db_stats": None,
    }

    # ── الخطوة 2: طباعة الملخص ───────────────────────────────────────────
    logger.info(
        f"\n{'='*60}\n"
        f"📊 نتائج مزامنة أسهم تاسي\n"
        f"{'='*60}\n"
        f"  المصدر              : argaam.com (market_id=3)\n"
        f"  أسهم تاسي           : {len(symbols)}\n"
        f"  أسهم نمو/ETFs       : 0 (مستبعدة تلقائياً)\n"
        f"  أول 10 رموز         : {symbols[:10]}\n"
        f"{'='*60}"
    )

    if dry_run:
        logger.info("[sync_symbols] 🔍 DRY RUN — لم يتم الحفظ في DB")
        print(f"\n✅ DRY RUN: {len(symbols)} TASI symbols from argaam.com")
        print(f"Sample: {symbols[:20]}")
        return result

    # ── الخطوة 3: حفظ في DB ──────────────────────────────────────────────
    logger.info("[sync_symbols] 💾 Saving to market_data.symbols...")
    db_stats = _upsert_symbols_to_db(symbols)
    result["db_stats"] = db_stats

    logger.info(
        f"[sync_symbols] ✅ DB sync complete:\n"
        f"  Inserted   : {db_stats['inserted']}\n"
        f"  Reactivated: {db_stats['reactivated']}\n"
        f"  Unchanged  : {db_stats['unchanged']}\n"
        f"  Deactivated: {db_stats['deactivated']} (stale symbols removed)\n"
        f"  Total active: {len(symbols)}"
    )

    return result


# ─── Celery Task ─────────────────────────────────────────────────────────────

def _get_celery_task():
    """إنشاء Celery task بشكل lazy لتجنب circular imports."""
    try:
        from celery import shared_task

        @shared_task(
            name="scripts.sync_symbols.sync_tasi_symbols_task",
            bind=True,
            max_retries=3,
        )
        def sync_tasi_symbols_task(self):
            """Celery task: مزامنة يومية لقائمة أسهم تاسي من argaam.com."""
            try:
                logger.info("[sync_symbols_task] Starting daily TASI symbols sync...")
                result = sync_tasi_symbols(dry_run=False)
                logger.info(
                    f"[sync_symbols_task] ✅ Sync complete: "
                    f"{result['tasi_count']} active TASI symbols"
                )
                return {
                    "status": "success",
                    "tasi_count": result["tasi_count"],
                    "db_stats": result["db_stats"],
                }
            except Exception as exc:
                logger.error(f"[sync_symbols_task] ❌ Sync failed: {exc}")
                raise self.retry(exc=exc, countdown=300)

        return sync_tasi_symbols_task
    except ImportError:
        return None


# تسجيل المهمة عند الاستيراد (إذا كان Celery متاحاً)
sync_tasi_symbols_task = _get_celery_task()


# ─── تشغيل مباشر ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="مزامنة قائمة أسهم تاسي من argaam.com"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="اجلب القائمة وأطبعها بدون حفظ في DB",
    )
    args = parser.parse_args()

    try:
        result = sync_tasi_symbols(dry_run=args.dry_run)
        print(f"\n✅ Done: {result['tasi_count']} TASI symbols")
        if result["db_stats"]:
            s = result["db_stats"]
            print(
                f"   DB: +{s['inserted']} new, "
                f"↑{s['reactivated']} reactivated, "
                f"✓{s['unchanged']} unchanged, "
                f"✗{s['deactivated']} deactivated"
            )
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Error: {e}", file=sys.stderr)
        sys.exit(1)
