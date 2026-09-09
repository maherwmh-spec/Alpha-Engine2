"""Read best genetic fitness per symbol from live tables."""
from __future__ import annotations

from typing import Any, Dict, Optional

from sqlalchemy import text

from scripts.database import db


def best_fitness_map() -> Dict[str, float]:
    queries = [
        """
        SELECT symbol, MAX(fitness_score) AS fit
        FROM genetic.strategies
        WHERE fitness_score IS NOT NULL
          AND status IN ('elite', 'active', 'evaluated')
        GROUP BY symbol
        """,
        """
        SELECT symbol, MAX(fitness) AS fit
        FROM genetic.strategies
        WHERE fitness IS NOT NULL
        GROUP BY symbol
        """,
        """
        SELECT symbol, MAX(fitness) AS fit
        FROM strategies.discovered_strategies
        WHERE fitness IS NOT NULL
        GROUP BY symbol
        """,
    ]
    for q in queries:
        try:
            with db.get_session() as session:
                rows = session.execute(text(q)).fetchall()
            if rows:
                return {str(r[0]): float(r[1]) for r in rows if r[1] is not None}
        except Exception:
            continue
    return {}


def best_fitness_for(symbol: str) -> Optional[float]:
    symbol = str(symbol).strip().upper()
    try:
        with db.get_session() as session:
            row = session.execute(
                text(
                    """
                    SELECT MAX(fitness_score)
                    FROM genetic.strategies
                    WHERE symbol = :s
                      AND fitness_score IS NOT NULL
                      AND status IN ('elite', 'active', 'evaluated')
                    """
                ),
                {"s": symbol},
            ).fetchone()
        if row and row[0] is not None:
            return float(row[0])
    except Exception:
        return None
    return None
