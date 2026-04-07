"""
scripts/sync_symbols.py
=======================
Sync the official TASI stock list from argaam.com (English version).

Source:
  https://www.argaam.com/en/company/companies-prices?market=3
  -> HTML tables containing TASI stocks only (market_id=3)
  -> Does NOT include Nomu (market_id=14) or ETFs

Logic:
  1. Scrape argaam English page -> clean list of ~230-280 TASI symbols with English names
  2. Upsert into market_data.symbols (is_active=True, name=English company name)
  3. Deactivate (is_active=False) any symbol in DB no longer in the official list

Usage:
  python3 scripts/sync_symbols.py            # live run with DB save
  python3 scripts/sync_symbols.py --dry-run  # fetch and print without saving
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
from sqlalchemy import text

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Scraping settings
# ---------------------------------------------------------------------------
_ARGAAM_TASI_URL = "https://www.argaam.com/en/company/companies-prices?market=3"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.argaam.com/en/",
}
_TASI_SYMBOL_RE = re.compile(r"^[1-8]\d{3}$")
# Noise text appended to some company names on argaam
_NOISE_RE = re.compile(r"Companies with accumulated.*", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def is_tasi_main_market(symbol: str) -> bool:
    """Return True only for TASI main-market symbols: 4 digits starting [1-8]."""
    return bool(_TASI_SYMBOL_RE.match(str(symbol).strip()))


def _clean_name(raw: str) -> str:
    """Strip noise text appended to company names on the argaam page."""
    cleaned = _NOISE_RE.sub("", raw).strip()
    return cleaned or raw.strip()


# ---------------------------------------------------------------------------
# Scraping
# ---------------------------------------------------------------------------
def scrape_tasi_symbols_from_argaam(
    retries: int = 3,
    timeout: int = 20,
) -> list[dict]:
    """
    Fetch TASI symbols with English company names from argaam.com.

    Extracts:
    - symbol : first column of each row (4-digit TASI code)
    - name_en: second column (English company name)

    Returns:
        Sorted list of dicts: [{"symbol": "1010", "name_en": "RIBL"}, ...]

    Raises:
        RuntimeError: if all retry attempts fail
    """
    last_error: Optional[Exception] = None

    for attempt in range(1, retries + 1):
        try:
            logger.info(
                "[sync_symbols] Scraping argaam.com/en "
                f"(attempt {attempt}/{retries})..."
            )
            resp = requests.get(_ARGAAM_TASI_URL, headers=_HEADERS, timeout=timeout)
            resp.raise_for_status()
            resp.encoding = "utf-8"
            soup = BeautifulSoup(resp.text, "html.parser")

            symbols_data: dict[str, dict] = {}

            for table in soup.find_all("table"):
                for row in table.find_all("tr"):
                    cells = row.find_all("td")
                    if not cells:
                        continue
                    candidate = cells[0].get_text(strip=True)
                    if not is_tasi_main_market(candidate):
                        continue
                    symbol = candidate
                    name_en = ""
                    if len(cells) > 1:
                        name_en = _clean_name(cells[1].get_text(strip=True))
                    if symbol not in symbols_data:
                        symbols_data[symbol] = {"symbol": symbol, "name_en": name_en}
                    elif name_en and not symbols_data[symbol]["name_en"]:
                        symbols_data[symbol]["name_en"] = name_en

            if not symbols_data:
                raise ValueError(
                    "No TASI symbols found in argaam HTML "
                    "-- page structure may have changed"
                )

            result = sorted(symbols_data.values(), key=lambda x: x["symbol"])
            with_names = sum(1 for r in result if r["name_en"])
            logger.info(
                f"[sync_symbols] Scraped {len(result)} TASI symbols "
                f"({with_names} with English names)"
            )
            return result

        except Exception as exc:
            last_error = exc
            logger.warning(f"[sync_symbols] Attempt {attempt} failed: {exc}")
            if attempt < retries:
                time.sleep(5 * attempt)

    raise RuntimeError(
        f"[sync_symbols] All {retries} scraping attempts failed. "
        f"Last error: {last_error}"
    )


# ---------------------------------------------------------------------------
# Database upsert
# ---------------------------------------------------------------------------
def _upsert_symbols_to_db(symbols_data: list[dict]) -> dict:
    """
    Upsert symbols into market_data.symbols with English names.

    - Insert new symbols with their English names
    - Reactivate previously deactivated symbols and update names
    - Update the name column for active symbols when a better name is available
    - Deactivate symbols no longer in the official list

    Args:
        symbols_data: list of dicts [{"symbol": "1010", "name_en": "RIBL"}, ...]

    Returns:
        dict: operation statistics
    """
    from scripts.database import db

    now = datetime.now(timezone.utc)
    symbols_set = {row["symbol"] for row in symbols_data}
    stats = {
        "inserted": 0,
        "reactivated": 0,
        "deactivated": 0,
        "unchanged": 0,
        "names_updated": 0,
    }

    with db.get_session() as session:
        # 1. Fetch existing symbols from DB
        existing_rows = session.execute(
            text("""
            SELECT symbol, is_active, name
            FROM market_data.symbols
            WHERE market = 'TASI'
            """)
        ).fetchall()
        existing_map = {
            row[0]: {"is_active": row[1], "name": row[2]} for row in existing_rows
        }
        existing_symbols = set(existing_map.keys())

        # 2. Upsert symbols with English names
        for row in symbols_data:
            sym = row["symbol"]
            name_en = row.get("name_en") or ""
            display_name = name_en if name_en else None

            if sym not in existing_symbols:
                # New symbol
                session.execute(
                    text("""
                    INSERT INTO market_data.symbols
                        (symbol, market, is_active, name, last_synced_at)
                    VALUES (:symbol, 'TASI', TRUE, :name, :now)
                    ON CONFLICT (symbol) DO UPDATE SET
                        is_active = TRUE,
                        name = COALESCE(EXCLUDED.name, market_data.symbols.name),
                        last_synced_at = :now
                    """),
                    {"symbol": sym, "name": display_name, "now": now},
                )
                stats["inserted"] += 1
                if display_name:
                    stats["names_updated"] += 1

            elif not existing_map[sym]["is_active"]:
                # Reactivate
                session.execute(
                    text("""
                    UPDATE market_data.symbols
                    SET is_active = TRUE,
                        name = COALESCE(:name, name),
                        last_synced_at = :now
                    WHERE symbol = :symbol
                    """),
                    {"symbol": sym, "name": display_name, "now": now},
                )
                stats["reactivated"] += 1
                if display_name:
                    stats["names_updated"] += 1

            else:
                # Active symbol -- update name if improved
                old_name = existing_map[sym]["name"] or ""
                if display_name and display_name != old_name:
                    session.execute(
                        text("""
                        UPDATE market_data.symbols
                        SET name = :name, last_synced_at = :now
                        WHERE symbol = :symbol
                        """),
                        {"symbol": sym, "name": display_name, "now": now},
                    )
                    stats["names_updated"] += 1
                else:
                    stats["unchanged"] += 1

        # 3. Deactivate stale symbols
        stale = existing_symbols - symbols_set
        if stale:
            session.execute(
                text("""
                UPDATE market_data.symbols
                SET is_active = FALSE, last_synced_at = :now
                WHERE symbol = ANY(:stale) AND market = 'TASI'
                """),
                {"stale": list(stale), "now": now},
            )
            stats["deactivated"] = len(stale)
            logger.info(
                f"[sync_symbols] Deactivated {len(stale)} stale symbols: "
                f"{sorted(stale)[:20]}{'...' if len(stale) > 20 else ''}"
            )

        session.commit()

    return stats


# ---------------------------------------------------------------------------
# Main sync function
# ---------------------------------------------------------------------------
def sync_tasi_symbols(dry_run: bool = False) -> dict:
    """
    Sync the TASI stock list from argaam.com into market_data.symbols
    using English company names.

    Args:
        dry_run: if True, fetch and print without saving to DB

    Returns:
        dict with keys: tasi_count, symbols, db_stats, source
    """
    # Step 1: Scrape argaam (English)
    symbols_data = scrape_tasi_symbols_from_argaam()
    symbols = [row["symbol"] for row in symbols_data]
    with_names = sum(1 for row in symbols_data if row.get("name_en"))

    result = {
        "tasi_count": len(symbols),
        "symbols": symbols,
        "symbols_data": symbols_data,
        "source": "argaam.com/en (market_id=3)",
        "db_stats": None,
    }

    # Step 2: Print summary
    logger.info(
        "\n" + "=" * 60 + "\n"
        "TASI Symbols Sync Results\n"
        + "=" * 60 + "\n"
        f"  Source             : argaam.com/en (market_id=3)\n"
        f"  TASI symbols       : {len(symbols)}\n"
        f"  With English names : {with_names}\n"
        f"  Nomu/ETFs          : 0 (excluded automatically)\n"
        f"  First 10 symbols   : {symbols[:10]}\n"
        + "=" * 60
    )

    if dry_run:
        logger.info("[sync_symbols] DRY RUN -- no changes saved to DB")
        print(f"\n[DRY RUN] {len(symbols)} TASI symbols from argaam.com/en")
        print(f"  With English names: {with_names}")
        print("\nSample (symbol -> name_en):")
        for row in symbols_data[:20]:
            print(f"  {row['symbol']:6} -> {row['name_en'] or '(no name)'}")
        return result

    # Step 3: Save to DB with English names
    logger.info("[sync_symbols] Saving to market_data.symbols (English names)...")
    db_stats = _upsert_symbols_to_db(symbols_data)
    result["db_stats"] = db_stats

    logger.info(
        "[sync_symbols] DB sync complete:\n"
        f"  Inserted      : {db_stats['inserted']}\n"
        f"  Reactivated   : {db_stats['reactivated']}\n"
        f"  Unchanged     : {db_stats['unchanged']}\n"
        f"  Names updated : {db_stats['names_updated']}\n"
        f"  Deactivated   : {db_stats['deactivated']} (stale symbols removed)\n"
        f"  Total active  : {len(symbols)}"
    )
    return result


# ---------------------------------------------------------------------------
# Celery Task
# ---------------------------------------------------------------------------
def _get_celery_task():
    """Create Celery task lazily to avoid circular imports."""
    try:
        from celery import shared_task

        @shared_task(
            name="scripts.sync_symbols.sync_tasi_symbols_task",
            bind=True,
            max_retries=3,
        )
        def sync_tasi_symbols_task(self):
            """Celery task: daily TASI symbols sync from argaam.com (English names)."""
            try:
                logger.info(
                    "[sync_symbols_task] Starting daily TASI symbols sync (English names)..."
                )
                result = sync_tasi_symbols(dry_run=False)
                logger.info(
                    f"[sync_symbols_task] Sync complete: "
                    f"{result['tasi_count']} active TASI symbols"
                )
                return {
                    "status": "success",
                    "tasi_count": result["tasi_count"],
                    "db_stats": result["db_stats"],
                }
            except Exception as exc:
                logger.error(f"[sync_symbols_task] Sync failed: {exc}")
                raise self.retry(exc=exc, countdown=300)

        return sync_tasi_symbols_task
    except ImportError:
        return None


# Register task on import (if Celery is available)
sync_tasi_symbols_task = _get_celery_task()


# ---------------------------------------------------------------------------
# Direct execution
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    parser = argparse.ArgumentParser(
        description="Sync TASI stock list from argaam.com with English company names"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and print the list without saving to DB",
    )
    args = parser.parse_args()

    try:
        result = sync_tasi_symbols(dry_run=args.dry_run)
        print(f"\nDone: {result['tasi_count']} TASI symbols")
        if result["db_stats"]:
            s = result["db_stats"]
            print(
                f"   DB: +{s['inserted']} new, "
                f"^{s['reactivated']} reactivated, "
                f"={s['unchanged']} unchanged, "
                f"-{s['deactivated']} deactivated, "
                f"~{s['names_updated']} names updated"
            )
        sys.exit(0)
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)
