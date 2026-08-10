"""
Phase 7 — WeeklyReviewer
Aggregates last-7-days paper trades (+ optional signals), builds report, notifies once.
"""
from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger
from sqlalchemy import text

from config.config_manager import config
from scripts.database import db


try:
    from zoneinfo import ZoneInfo
    RIYADH = ZoneInfo("Asia/Riyadh")
except Exception:
    RIYADH = timezone(timedelta(hours=3))


class WeeklyReviewerBot:
    def __init__(self):
        self.name = "weekly_reviewer"
        self.logger = logger.bind(bot=self.name)
        self.min_trades = int(config.get("bots.weekly_reviewer.min_trades_for_symbol", 3))
        self.weak_wr = float(config.get("bots.weekly_reviewer.weak_win_rate", 0.40))

    def run(self, send_telegram: bool = True, force: bool = False) -> Dict[str, Any]:
        window_end = datetime.now(RIYADH)
        window_start = window_end - timedelta(days=7)
        week_id = window_end.strftime("%G-W%V")

        if not force and self._already_notified(week_id):
            self.logger.info(f"week {week_id} already notified — skip")
            return {"status": "skipped", "week_id": week_id, "reason": "already_notified"}

        papers = self._load_closed_papers(window_start, window_end)
        signals_count = self._count_signals(window_start, window_end)

        closed = len(papers)
        wins = sum(1 for p in papers if (p.get("pnl_pct") or 0) > 0)
        losses = sum(1 for p in papers if (p.get("pnl_pct") or 0) <= 0)
        win_rate = (wins / closed) if closed else None
        avg_pnl = (
            sum(float(p.get("pnl_pct") or 0) for p in papers) / closed if closed else None
        )

        by_symbol = self._symbol_stats(papers)
        best = sorted(by_symbol, key=lambda x: x["avg_pnl"], reverse=True)[:3]
        worst = sorted(by_symbol, key=lambda x: x["avg_pnl"])[:3]
        recommendations = self._recommendations(by_symbol)

        report = self._build_report(
            week_id=week_id,
            closed=closed,
            wins=wins,
            losses=losses,
            win_rate=win_rate,
            avg_pnl=avg_pnl,
            signals_count=signals_count,
            best=best,
            worst=worst,
            recommendations=recommendations,
        )

        # self_trainer v1 (suggest / optional apply)
        trainer_status = "skipped"
        try:
            from bots.self_trainer.bot import SelfTrainerBot

            tr = SelfTrainerBot().run(
                week_id=week_id,
                recommendations=recommendations,
                has_data=closed > 0,
            )
            trainer_status = tr.get("status", "skipped")
            if tr.get("notes"):
                report += "\n\n" + "\n".join(tr["notes"])
        except Exception as exc:
            self.logger.warning(f"self_trainer: {exc}")
            trainer_status = "error"

        self._save_review(
            week_id=week_id,
            window_start=window_start,
            window_end=window_end,
            signals_count=signals_count,
            closed=closed,
            wins=wins,
            losses=losses,
            win_rate=win_rate,
            avg_pnl=avg_pnl,
            best=best,
            worst=worst,
            recommendations=recommendations,
            report=report,
            trainer_status=trainer_status,
        )

        notified = False
        if send_telegram:
            notified = self._notify(week_id, report)

        return {
            "status": "ok",
            "week_id": week_id,
            "closed_papers": closed,
            "win_rate": win_rate,
            "trainer_status": trainer_status,
            "notified": notified,
        }

    def _already_notified(self, week_id: str) -> bool:
        try:
            with db.get_session() as session:
                row = session.execute(
                    text(
                        "SELECT notified FROM analytics.weekly_reviews WHERE week_id = :w"
                    ),
                    {"w": week_id},
                ).fetchone()
            return bool(row and row[0])
        except Exception:
            return False

    def _load_closed_papers(
        self, start: datetime, end: datetime
    ) -> List[Dict[str, Any]]:
        try:
            with db.get_session() as session:
                rows = session.execute(
                    text(
                        """
                        SELECT symbol, pnl_pct, entry_price, exit_price, closed_at
                        FROM market_data.paper_trades
                        WHERE status = 'closed'
                          AND closed_at >= :s AND closed_at < :e
                        """
                    ),
                    {"s": start, "e": end},
                ).fetchall()
            return [
                {
                    "symbol": r[0],
                    "pnl_pct": float(r[1]) if r[1] is not None else 0.0,
                    "entry": r[2],
                    "exit": r[3],
                    "closed_at": r[4],
                }
                for r in rows
            ]
        except Exception as exc:
            self.logger.error(f"load papers: {exc}")
            return []

    def _count_signals(self, start: datetime, end: datetime) -> int:
        try:
            with db.get_session() as session:
                row = session.execute(
                    text(
                        """
                        SELECT COUNT(*) FROM strategies.signals
                        WHERE timestamp >= :s AND timestamp < :e
                        """
                    ),
                    {"s": start, "e": end},
                ).fetchone()
            return int(row[0]) if row else 0
        except Exception:
            return 0

    def _symbol_stats(self, papers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        bucket: Dict[str, List[float]] = defaultdict(list)
        for p in papers:
            bucket[str(p["symbol"])].append(float(p.get("pnl_pct") or 0))
        out = []
        for sym, vals in bucket.items():
            n = len(vals)
            wins = sum(1 for v in vals if v > 0)
            out.append(
                {
                    "symbol": sym,
                    "trades": n,
                    "wins": wins,
                    "win_rate": wins / n if n else 0.0,
                    "avg_pnl": sum(vals) / n if n else 0.0,
                }
            )
        return out

    def _recommendations(self, stats: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        recs = []
        for s in stats:
            if s["trades"] >= self.min_trades and s["win_rate"] < self.weak_wr:
                recs.append(
                    {
                        "symbol": s["symbol"],
                        "action": "raise_min_confidence",
                        "reason": f"win_rate={s['win_rate']:.0%} on {s['trades']} trades",
                        "bump": float(config.get("bots.self_trainer.bump_min_confidence", 0.05)),
                    }
                )
        return recs

    def _build_report(
        self,
        week_id: str,
        closed: int,
        wins: int,
        losses: int,
        win_rate: Optional[float],
        avg_pnl: Optional[float],
        signals_count: int,
        best: List[Dict],
        worst: List[Dict],
        recommendations: List[Dict],
    ) -> str:
        wr = f"{win_rate*100:.0f}%" if win_rate is not None else "—"
        avg = f"{avg_pnl:.2f}%" if avg_pnl is not None else "—"
        lines = [
            f"📊 <b>تقرير الأسبوع {week_id}</b>\n",
            f"• الأوراق المغلقة: <b>{closed}</b>",
            f"• الناجحة: <b>{wins}</b> ({wr}) · الخاسرة: {losses}",
            f"• متوسط الربح: <b>{avg}</b>",
            f"• إشارات مسجّلة: {signals_count}",
        ]
        if not closed:
            lines.append("\nℹ️ لا بيانات ورقية كافية هذا الأسبوع.")
        if best:
            lines.append("\n<b>أفضل الرموز:</b>")
            for b in best:
                lines.append(
                    f"• {b['symbol']} — avg {b['avg_pnl']:.2f}% ({b['trades']} صفقات)"
                )
        if worst:
            lines.append("\n<b>الأضعف:</b>")
            for w in worst:
                lines.append(
                    f"• {w['symbol']} — avg {w['avg_pnl']:.2f}% ({w['trades']} صفقات)"
                )
        if recommendations:
            lines.append("\n<b>التوصيات:</b>")
            for r in recommendations:
                lines.append(f"• {r['symbol']}: {r['action']} — {r['reason']}")
        else:
            lines.append("\nلا توصيات ضبط تلقائي.")
        return "\n".join(lines)

    def _save_review(self, **kw) -> None:
        with db.get_session() as session:
            session.execute(
                text(
                    """
                    INSERT INTO analytics.weekly_reviews (
                        week_id, window_start, window_end,
                        signals_count, closed_papers, wins, losses,
                        win_rate, avg_pnl_pct,
                        best_symbols_json, worst_symbols_json, recommendations_json,
                        report_text, notified, trainer_status, computed_at
                    ) VALUES (
                        :week_id, :window_start, :window_end,
                        :signals_count, :closed, :wins, :losses,
                        :win_rate, :avg_pnl,
                        CAST(:best AS JSONB), CAST(:worst AS JSONB), CAST(:recs AS JSONB),
                        :report, FALSE, :trainer_status, NOW()
                    )
                    ON CONFLICT (week_id) DO UPDATE SET
                        window_start = EXCLUDED.window_start,
                        window_end = EXCLUDED.window_end,
                        signals_count = EXCLUDED.signals_count,
                        closed_papers = EXCLUDED.closed_papers,
                        wins = EXCLUDED.wins,
                        losses = EXCLUDED.losses,
                        win_rate = EXCLUDED.win_rate,
                        avg_pnl_pct = EXCLUDED.avg_pnl_pct,
                        best_symbols_json = EXCLUDED.best_symbols_json,
                        worst_symbols_json = EXCLUDED.worst_symbols_json,
                        recommendations_json = EXCLUDED.recommendations_json,
                        report_text = EXCLUDED.report_text,
                        trainer_status = EXCLUDED.trainer_status,
                        computed_at = NOW()
                    """
                ),
                {
                    "week_id": kw["week_id"],
                    "window_start": kw["window_start"],
                    "window_end": kw["window_end"],
                    "signals_count": kw["signals_count"],
                    "closed": kw["closed"],
                    "wins": kw["wins"],
                    "losses": kw["losses"],
                    "win_rate": kw["win_rate"],
                    "avg_pnl": kw["avg_pnl"],
                    "best": json.dumps(kw["best"], default=str),
                    "worst": json.dumps(kw["worst"], default=str),
                    "recs": json.dumps(kw["recommendations"], default=str),
                    "report": kw["report"],
                    "trainer_status": kw["trainer_status"],
                },
            )
            session.commit()

    def _notify(self, week_id: str, report: str) -> bool:
        try:
            from scripts.telegram_bot import AlphaTelegramBot

            bot = AlphaTelegramBot()
            asyncio.run(bot.send_message(report))
            with db.get_session() as session:
                session.execute(
                    text(
                        "UPDATE analytics.weekly_reviews SET notified = TRUE WHERE week_id = :w"
                    ),
                    {"w": week_id},
                )
                session.commit()
            return True
        except Exception as exc:
            self.logger.error(f"telegram notify failed: {exc}")
            return False
