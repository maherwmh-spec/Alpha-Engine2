"""
Bot: Watchlist Generator (Phase 4)
──────────────────────────────────
Builds daily candidate list from personality + features + sentiment + genetic.
Sends one morning Telegram message per trade_date.
v1: generate + store + notify only.
"""
from __future__ import annotations

import asyncio
import json
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

from loguru import logger
from sqlalchemy import text

from config.config_manager import config
from scripts.database import db


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class WatchlistGeneratorBot:
    """Morning watchlist from nightly pipeline outputs."""

    def __init__(self):
        self.name = "watchlist_generator"
        self.logger = logger.bind(bot=self.name)
        self.top_n = int(config.get("bots.watchlist_generator.top_n", 15))
        self.min_score = float(config.get("bots.watchlist_generator.min_score", 25.0))
        # weights: genetic, phase, features, sentiment
        self.w_genetic = float(config.get("bots.watchlist_generator.w_genetic", 0.35))
        self.w_phase = float(config.get("bots.watchlist_generator.w_phase", 0.25))
        self.w_features = float(config.get("bots.watchlist_generator.w_features", 0.25))
        self.w_sentiment = float(config.get("bots.watchlist_generator.w_sentiment", 0.15))

    def run(
        self,
        send_telegram: bool = True,
        trade_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        td = date.fromisoformat(trade_date) if trade_date else date.today()
        self.logger.info(f"WatchlistGenerator start trade_date={td} top_n={self.top_n}")

        candidates = self._build_candidates()
        ranked = [c for c in candidates if c["score"] >= self.min_score]
        ranked.sort(key=lambda x: x["score"], reverse=True)
        ranked = ranked[: self.top_n]

        for i, row in enumerate(ranked, start=1):
            row["rank"] = i
            row["trade_date"] = td

        self._replace_day(td, ranked)

        notified = False
        if send_telegram:
            notified = self._notify(td, ranked)

        result = {
            "trade_date": str(td),
            "count": len(ranked),
            "notified": notified,
            "symbols": [r["symbol"] for r in ranked],
        }
        self.logger.success(f"WatchlistGenerator complete: {result}")
        return result

    # ── Scoring ──────────────────────────────────────────────────────────────
    def _build_candidates(self) -> List[Dict[str, Any]]:
        personalities = self._load_personalities()
        features = self._load_features()
        sentiments = self._load_sentiments()
        genetic = self._load_genetic_fitness()

        symbols = set(personalities) | set(features) | set(sentiments) | set(genetic)
        out: List[Dict[str, Any]] = []

        for symbol in symbols:
            p = personalities.get(symbol) or {}
            f = features.get(symbol) or {}
            s = sentiments.get(symbol) or {}
            g = genetic.get(symbol)

            # skip hard insufficient
            if p.get("status") == "insufficient_data" and f.get("status") == "insufficient_data":
                continue
            if not p and not f and g is None:
                continue

            phase_score = self._score_phase(p)
            feat_score = self._score_features(f)
            sent_score = self._score_sentiment(s)
            gen_score = self._score_genetic(g)

            total = (
                self.w_genetic * gen_score
                + self.w_phase * phase_score
                + self.w_features * feat_score
                + self.w_sentiment * sent_score
            )
            total = round(float(total), 2)

            reasons = []
            phase = p.get("phase")
            if phase:
                reasons.append(f"مرحلة {phase}")
            if f.get("ret_5d") is not None:
                reasons.append(f"ret5d={float(f['ret_5d'])*100:.1f}%")
            if s.get("sentiment_label") and s.get("status") == "ok":
                reasons.append(f"مشاعر {s['sentiment_label']}")
            if g is not None:
                reasons.append(f"جيني={g:.3f}")

            out.append(
                {
                    "symbol": symbol,
                    "score": total,
                    "bucket": "short_swings" if gen_score >= 40 or feat_score >= 55 else "general",
                    "phase": phase,
                    "phase_confidence": p.get("phase_confidence"),
                    "sentiment_label": s.get("sentiment_label"),
                    "genetic_fitness": g,
                    "reasons_json": json.dumps(reasons, ensure_ascii=False),
                    "status": "active",
                }
            )
        return out

    @staticmethod
    def _score_phase(p: Dict[str, Any]) -> float:
        if not p or p.get("status") not in (None, "ok"):
            if p.get("status") == "insufficient_data":
                return 10.0
            return 20.0
        phase = (p.get("phase") or "transition").lower()
        conf = float(p.get("phase_confidence") or 40.0) / 100.0
        base = {
            "markup": 85.0,
            "accumulation": 75.0,
            "transition": 45.0,
            "distribution": 35.0,
            "markdown": 20.0,
        }.get(phase, 40.0)
        return max(0.0, min(100.0, base * (0.5 + 0.5 * conf)))

    @staticmethod
    def _score_features(f: Dict[str, Any]) -> float:
        if not f or f.get("status") not in (None, "ok"):
            return 15.0
        score = 40.0
        ret5 = f.get("ret_5d")
        if ret5 is not None:
            # mild positive momentum preferred
            r = float(ret5)
            if 0.01 <= r <= 0.12:
                score += 25.0
            elif 0 < r < 0.01:
                score += 10.0
            elif r > 0.12:
                score += 5.0  # extended
            elif r < -0.05:
                score -= 15.0
        rsi = f.get("rsi_14")
        if rsi is not None:
            rsi = float(rsi)
            if 40 <= rsi <= 65:
                score += 15.0
            elif 30 <= rsi < 40 or 65 < rsi <= 75:
                score += 5.0
            elif rsi > 80:
                score -= 10.0
        volr = f.get("volume_ratio_20")
        if volr is not None and float(volr) >= 1.2:
            score += 10.0
        return max(0.0, min(100.0, score))

    @staticmethod
    def _score_sentiment(s: Dict[str, Any]) -> float:
        if not s:
            return 50.0
        st = s.get("status")
        if st in ("no_news", "model_unavailable"):
            return 50.0  # neutral, do not punish
        label = (s.get("sentiment_label") or "neutral").lower()
        conf = float(s.get("sentiment_score") or 0.5)
        if label == "positive":
            return min(100.0, 60.0 + 40.0 * conf)
        if label == "negative":
            return max(0.0, 40.0 - 30.0 * conf)
        return 50.0

    @staticmethod
    def _score_genetic(fitness: Optional[float]) -> float:
        if fitness is None:
            return 0.0
        # map typical fitness ~0-1+ into 0-100
        return max(0.0, min(100.0, float(fitness) * 100.0))

    # ── Data loaders ─────────────────────────────────────────────────────────
    def _load_personalities(self) -> Dict[str, Dict[str, Any]]:
        try:
            with db.get_session() as session:
                rows = session.execute(
                    text(
                        """
                        SELECT symbol, phase, phase_confidence, status
                        FROM market_data.stock_personalities
                        """
                    )
                ).fetchall()
            return {
                str(r[0]): {
                    "phase": r[1],
                    "phase_confidence": r[2],
                    "status": r[3],
                }
                for r in rows
            }
        except Exception as exc:
            self.logger.warning(f"personalities load failed: {exc}")
            return {}

    def _load_features(self) -> Dict[str, Dict[str, Any]]:
        try:
            with db.get_session() as session:
                rows = session.execute(
                    text(
                        """
                        SELECT symbol, ret_5d, rsi_14, volume_ratio_20, status
                        FROM market_data.stock_features
                        """
                    )
                ).fetchall()
            return {
                str(r[0]): {
                    "ret_5d": r[1],
                    "rsi_14": r[2],
                    "volume_ratio_20": r[3],
                    "status": r[4],
                }
                for r in rows
            }
        except Exception as exc:
            self.logger.warning(f"features load failed: {exc}")
            return {}

    def _load_sentiments(self) -> Dict[str, Dict[str, Any]]:
        try:
            with db.get_session() as session:
                rows = session.execute(
                    text(
                        """
                        SELECT symbol, sentiment_label, sentiment_score, status
                        FROM market_data.news_sentiments
                        """
                    )
                ).fetchall()
            return {
                str(r[0]): {
                    "sentiment_label": r[1],
                    "sentiment_score": r[2],
                    "status": r[3],
                }
                for r in rows
            }
        except Exception as exc:
            self.logger.warning(f"sentiments load failed: {exc}")
            return {}

    def _load_genetic_fitness(self) -> Dict[str, float]:
        """Best effort: read best fitness per symbol from genetic tables if present."""
        queries = [
            """
            SELECT symbol, MAX(fitness) AS fit
            FROM strategies.discovered_strategies
            WHERE fitness IS NOT NULL
            GROUP BY symbol
            """,
            """
            SELECT symbol, MAX(fitness) AS fit
            FROM genetic.strategies
            WHERE fitness IS NOT NULL
            GROUP BY symbol
            """,
            """
            SELECT symbol, MAX(best_fitness) AS fit
            FROM strategies.genetic_results
            WHERE best_fitness IS NOT NULL
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

    # ── Persistence ──────────────────────────────────────────────────────────
    def _replace_day(self, trade_date: date, rows: List[Dict[str, Any]]) -> None:
        with db.get_session() as session:
            session.execute(
                text("DELETE FROM market_data.daily_watchlist WHERE trade_date = :d"),
                {"d": trade_date},
            )
            for row in rows:
                session.execute(
                    text(
                        """
                        INSERT INTO market_data.daily_watchlist
                        (trade_date, symbol, rank, score, bucket, phase, phase_confidence,
                         sentiment_label, genetic_fitness, reasons_json, status, created_at)
                        VALUES
                        (:trade_date, :symbol, :rank, :score, :bucket, :phase, :phase_confidence,
                         :sentiment_label, :genetic_fitness, CAST(:reasons_json AS JSONB), :status, NOW())
                        """
                    ),
                    {
                        "trade_date": trade_date,
                        "symbol": row["symbol"],
                        "rank": row["rank"],
                        "score": row["score"],
                        "bucket": row.get("bucket"),
                        "phase": row.get("phase"),
                        "phase_confidence": row.get("phase_confidence"),
                        "sentiment_label": row.get("sentiment_label"),
                        "genetic_fitness": row.get("genetic_fitness"),
                        "reasons_json": row.get("reasons_json") or "[]",
                        "status": row.get("status") or "active",
                    },
                )
            session.commit()

    # ── Telegram ─────────────────────────────────────────────────────────────
    def _notify(self, trade_date: date, rows: List[Dict[str, Any]]) -> bool:
        # skip if already notified today
        try:
            with db.get_session() as session:
                existing = session.execute(
                    text(
                        """
                        SELECT COUNT(*) FROM market_data.daily_watchlist
                        WHERE trade_date = :d AND notified_at IS NOT NULL
                        """
                    ),
                    {"d": trade_date},
                ).scalar()
            if existing and int(existing) > 0:
                self.logger.info(f"Already notified for {trade_date}, skip Telegram")
                return False
        except Exception:
            pass

        if not rows:
            msg = (
                f"📋 <b>قائمة المرشحين — {trade_date}</b>\n\n"
                "لا مرشّحين اليوم (بيانات غير كافية أو درجات تحت الحد)."
            )
        else:
            lines = [f"📋 <b>قائمة الأسهم المرشحة — {trade_date}</b>\n"]
            for r in rows:
                phase = r.get("phase") or "—"
                conf = r.get("phase_confidence")
                conf_s = f" · ثقة {conf:.0f}%" if conf is not None else ""
                lines.append(
                    f"{r['rank']}. <code>{r['symbol']}</code> — {phase}{conf_s} · درجة {r['score']:.0f}"
                )
            lines.append("\nللتحليل لاحقاً: <code>/analyze SYMBOL</code> (مرحلة 5)")
            msg = "\n".join(lines)

        try:
            from scripts.telegram_bot import AlphaTelegramBot

            bot = AlphaTelegramBot()
            asyncio.run(bot.send_message(msg))
            with db.get_session() as session:
                session.execute(
                    text(
                        """
                        UPDATE market_data.daily_watchlist
                        SET notified_at = NOW()
                        WHERE trade_date = :d
                        """
                    ),
                    {"d": trade_date},
                )
                session.commit()
            return True
        except Exception as exc:
            self.logger.error(f"Telegram notify failed: {exc}")
            return False


if __name__ == "__main__":
    print(WatchlistGeneratorBot().run(send_telegram=False))
