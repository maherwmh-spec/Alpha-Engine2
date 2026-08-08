"""
Phase 5 — AnalyzeService
Unified snapshot for /analyze, watchlist display, paper trades helpers.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger
from sqlalchemy import text

from config.config_manager import config
from scripts.database import db


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class AnalyzeService:
    """Aggregate personality/features/sentiment/watchlist/price into one report."""

    def __init__(self):
        self.logger = logger.bind(service="analyze")
        self.cooldown_min = int(config.get("bots.analyze.alert_cooldown_minutes", 30))
        sl = config.get("profit_objectives.short_swings.stoploss_range", [-0.015, -0.03])
        tp = config.get("profit_objectives.short_swings.roi_range", [0.03, 0.08])
        self.default_sl_pct = abs(float(sl[0])) if isinstance(sl, list) and sl else 0.02
        self.default_tp_pct = float(tp[0]) if isinstance(tp, list) and tp else 0.04

    # ── Public API ───────────────────────────────────────────────────────────
    def analyze(self, symbol: str, add_watch: bool = True) -> Dict[str, Any]:
        symbol = str(symbol).strip().upper()
        data = self._collect(symbol)
        html = self._format_html(symbol, data)
        if add_watch:
            self.upsert_monitoring(
                symbol,
                source="analyze",
                phase=data.get("phase"),
                sentiment=data.get("sentiment_label"),
                price=data.get("last_price"),
            )
            data["watching"] = True
        data["symbol"] = symbol
        data["html"] = html
        return data

    def upsert_monitoring(
        self,
        symbol: str,
        source: str = "manual",
        phase: Optional[str] = None,
        sentiment: Optional[str] = None,
        price: Optional[float] = None,
    ) -> None:
        with db.get_session() as session:
            session.execute(
                text(
                    """
                    INSERT INTO market_data.active_monitoring
                        (symbol, source, status, last_phase, last_sentiment, last_price, added_at, updated_at)
                    VALUES
                        (:symbol, :source, 'active', :phase, :sentiment, :price, NOW(), NOW())
                    ON CONFLICT (symbol) DO UPDATE SET
                        status = 'active',
                        source = EXCLUDED.source,
                        last_phase = COALESCE(EXCLUDED.last_phase, market_data.active_monitoring.last_phase),
                        last_sentiment = COALESCE(EXCLUDED.last_sentiment, market_data.active_monitoring.last_sentiment),
                        last_price = COALESCE(EXCLUDED.last_price, market_data.active_monitoring.last_price),
                        updated_at = NOW()
                    """
                ),
                {
                    "symbol": symbol,
                    "source": source,
                    "phase": phase,
                    "sentiment": sentiment,
                    "price": price,
                },
            )
            session.commit()

    def unwatch(self, symbol: str) -> bool:
        symbol = str(symbol).strip().upper()
        with db.get_session() as session:
            res = session.execute(
                text(
                    """
                    UPDATE market_data.active_monitoring
                    SET status = 'closed', updated_at = NOW()
                    WHERE symbol = :symbol AND status = 'active'
                    """
                ),
                {"symbol": symbol},
            )
            session.commit()
            return (res.rowcount or 0) > 0

    def list_watching(self) -> List[Dict[str, Any]]:
        with db.get_session() as session:
            rows = session.execute(
                text(
                    """
                    SELECT symbol, source, last_phase, last_sentiment, last_price, added_at
                    FROM market_data.active_monitoring
                    WHERE status = 'active'
                    ORDER BY added_at DESC
                    """
                )
            ).fetchall()
        return [
            {
                "symbol": r[0],
                "source": r[1],
                "phase": r[2],
                "sentiment": r[3],
                "price": r[4],
                "added_at": r[5],
            }
            for r in rows
        ]

    def list_watchlist_today(self) -> List[Dict[str, Any]]:
        with db.get_session() as session:
            rows = session.execute(
                text(
                    """
                    SELECT rank, symbol, score, phase, sentiment_label
                    FROM market_data.daily_watchlist
                    WHERE trade_date = CURRENT_DATE
                    ORDER BY rank
                    """
                )
            ).fetchall()
        return [
            {
                "rank": r[0],
                "symbol": r[1],
                "score": r[2],
                "phase": r[3],
                "sentiment": r[4],
            }
            for r in rows
        ]

    def open_paper(self, symbol: str, source: str = "manual") -> Dict[str, Any]:
        symbol = str(symbol).strip().upper()
        price = self._last_price(symbol)
        if price is None or price <= 0:
            return {"ok": False, "error": "لا يوجد سعر متاح لفتح صفقة ورقية"}

        # close existing open paper on same symbol
        with db.get_session() as session:
            session.execute(
                text(
                    """
                    UPDATE market_data.paper_trades
                    SET status = 'cancelled', closed_at = NOW()
                    WHERE symbol = :symbol AND status = 'open'
                    """
                ),
                {"symbol": symbol},
            )
            sl = price * (1 - self.default_sl_pct)
            tp = price * (1 + self.default_tp_pct)
            row = session.execute(
                text(
                    """
                    INSERT INTO market_data.paper_trades
                        (symbol, side, status, entry_price, stop_loss, take_profit,
                         qty, opened_at, source, created_at)
                    VALUES
                        (:symbol, 'long', 'open', :entry, :sl, :tp,
                         1, NOW(), :source, NOW())
                    RETURNING id
                    """
                ),
                {
                    "symbol": symbol,
                    "entry": price,
                    "sl": sl,
                    "tp": tp,
                    "source": source,
                },
            ).fetchone()
            session.commit()
            pid = int(row[0]) if row else None

        self.upsert_monitoring(symbol, source="paper", price=price)
        return {
            "ok": True,
            "id": pid,
            "symbol": symbol,
            "entry": price,
            "stop_loss": sl,
            "take_profit": tp,
        }

    def close_paper(self, symbol: str) -> Dict[str, Any]:
        symbol = str(symbol).strip().upper()
        price = self._last_price(symbol)
        with db.get_session() as session:
            row = session.execute(
                text(
                    """
                    SELECT id, entry_price FROM market_data.paper_trades
                    WHERE symbol = :symbol AND status = 'open'
                    ORDER BY opened_at DESC LIMIT 1
                    """
                ),
                {"symbol": symbol},
            ).fetchone()
            if not row:
                return {"ok": False, "error": "لا توجد صفقة ورقية مفتوحة"}
            pid, entry = int(row[0]), float(row[1])
            exit_p = float(price) if price else entry
            pnl = (exit_p - entry) / entry * 100.0 if entry else 0.0
            session.execute(
                text(
                    """
                    UPDATE market_data.paper_trades
                    SET status = 'closed', exit_price = :exit, pnl_pct = :pnl,
                        closed_at = NOW()
                    WHERE id = :id
                    """
                ),
                {"exit": exit_p, "pnl": pnl, "id": pid},
            )
            session.commit()
        return {"ok": True, "id": pid, "exit": exit_p, "pnl_pct": round(pnl, 3)}

    def list_open_papers(self) -> List[Dict[str, Any]]:
        with db.get_session() as session:
            rows = session.execute(
                text(
                    """
                    SELECT id, symbol, entry_price, stop_loss, take_profit, opened_at
                    FROM market_data.paper_trades
                    WHERE status = 'open'
                    ORDER BY opened_at DESC
                    """
                )
            ).fetchall()
        return [
            {
                "id": r[0],
                "symbol": r[1],
                "entry": r[2],
                "sl": r[3],
                "tp": r[4],
                "opened_at": r[5],
            }
            for r in rows
        ]

    def check_active_and_papers(self) -> Dict[str, Any]:
        """Called by monitor: phase/sentiment changes + paper SL/TP."""
        alerts: List[str] = []
        from scripts.database import insert_alert

        # Active monitoring changes
        watching = self.list_watching()
        for w in watching:
            symbol = w["symbol"]
            snap = self._collect(symbol)
            phase = snap.get("phase")
            sent = snap.get("sentiment_label")
            price = snap.get("last_price")
            changed = []
            if phase and w.get("phase") and phase != w.get("phase"):
                changed.append(f"مرحلة: {w.get('phase')} → {phase}")
            if sent and w.get("sentiment") and sent != w.get("sentiment"):
                changed.append(f"مشاعر: {w.get('sentiment')} → {sent}")

            if changed and self._can_alert(symbol):
                msg = f"🔔 تحديث {symbol}\n" + "\n".join(changed)
                alerts.append(msg)
                try:
                    with db.get_session() as session:
                        insert_alert(
                            session,
                            alert_type="ACTIVE_MONITOR",
                            priority=2,
                            title=f"تحديث مراقبة {symbol}",
                            message="\n".join(changed),
                            symbol=symbol,
                        )
                        session.execute(
                            text(
                                """
                                UPDATE market_data.active_monitoring
                                SET last_phase = :phase,
                                    last_sentiment = :sent,
                                    last_price = :price,
                                    last_alert_at = NOW(),
                                    updated_at = NOW()
                                WHERE symbol = :symbol
                                """
                            ),
                            {
                                "phase": phase,
                                "sent": sent,
                                "price": price,
                                "symbol": symbol,
                            },
                        )
                        session.commit()
                except Exception as exc:
                    self.logger.error(f"alert upsert failed {symbol}: {exc}")
            else:
                # silent state refresh
                try:
                    with db.get_session() as session:
                        session.execute(
                            text(
                                """
                                UPDATE market_data.active_monitoring
                                SET last_phase = COALESCE(:phase, last_phase),
                                    last_sentiment = COALESCE(:sent, last_sentiment),
                                    last_price = COALESCE(:price, last_price),
                                    updated_at = NOW()
                                WHERE symbol = :symbol
                                """
                            ),
                            {
                                "phase": phase,
                                "sent": sent,
                                "price": price,
                                "symbol": symbol,
                            },
                        )
                        session.commit()
                except Exception:
                    pass

        # Paper SL/TP
        papers = self.list_open_papers()
        for p in papers:
            symbol = p["symbol"]
            price = self._last_price(symbol)
            if price is None:
                continue
            entry, sl, tp = p["entry"], p["sl"], p["tp"]
            hit = None
            if sl is not None and price <= float(sl):
                hit = "stop_loss"
            elif tp is not None and price >= float(tp):
                hit = "take_profit"
            if hit:
                result = self.close_paper(symbol)
                msg = (
                    f"📄 إغلاق ورقي {symbol} ({hit})\n"
                    f"خروج {result.get('exit')} · PnL {result.get('pnl_pct')}%"
                )
                alerts.append(msg)
                try:
                    with db.get_session() as session:
                        insert_alert(
                            session,
                            alert_type="PAPER_CLOSE",
                            priority=1,
                            title=f"إغلاق ورقي {symbol}",
                            message=msg,
                            symbol=symbol,
                        )
                        session.commit()
                except Exception as exc:
                    self.logger.error(f"paper alert failed: {exc}")

        return {"alerts": alerts, "watching": len(watching), "papers": len(papers)}

    # ── Internals ────────────────────────────────────────────────────────────
    def _can_alert(self, symbol: str) -> bool:
        try:
            with db.get_session() as session:
                row = session.execute(
                    text(
                        """
                        SELECT last_alert_at FROM market_data.active_monitoring
                        WHERE symbol = :symbol
                        """
                    ),
                    {"symbol": symbol},
                ).fetchone()
            if not row or not row[0]:
                return True
            last = row[0]
            if last.tzinfo is None:
                last = last.replace(tzinfo=timezone.utc)
            age = (_utcnow() - last).total_seconds() / 60.0
            return age >= self.cooldown_min
        except Exception:
            return True

    def _collect(self, symbol: str) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        out["last_price"] = self._last_price(symbol)
        out["ret_1d"] = None

        try:
            with db.get_session() as session:
                row = session.execute(
                    text(
                        """
                        SELECT phase, phase_confidence, beta_90d, volatility_30d,
                               accumulation_score, status, computed_at
                        FROM market_data.stock_personalities WHERE symbol = :s
                        """
                    ),
                    {"s": symbol},
                ).fetchone()
            if row:
                out.update(
                    {
                        "phase": row[0],
                        "phase_confidence": row[1],
                        "beta_90d": row[2],
                        "vol_30d": row[3],
                        "accumulation": row[4],
                        "personality_status": row[5],
                        "personality_at": row[6],
                    }
                )
        except Exception as exc:
            self.logger.debug(f"personality {symbol}: {exc}")

        try:
            with db.get_session() as session:
                row = session.execute(
                    text(
                        """
                        SELECT ret_1d, ret_5d, rsi_14, volume_ratio_20, status, computed_at
                        FROM market_data.stock_features WHERE symbol = :s
                        """
                    ),
                    {"s": symbol},
                ).fetchone()
            if row:
                out.update(
                    {
                        "ret_1d": row[0],
                        "ret_5d": row[1],
                        "rsi_14": row[2],
                        "volume_ratio_20": row[3],
                        "features_status": row[4],
                        "features_at": row[5],
                    }
                )
        except Exception as exc:
            self.logger.debug(f"features {symbol}: {exc}")

        try:
            with db.get_session() as session:
                row = session.execute(
                    text(
                        """
                        SELECT sentiment_label, sentiment_score, status, articles_count
                        FROM market_data.news_sentiments WHERE symbol = :s
                        """
                    ),
                    {"s": symbol},
                ).fetchone()
            if row:
                out.update(
                    {
                        "sentiment_label": row[0],
                        "sentiment_score": row[1],
                        "sentiment_status": row[2],
                        "articles_count": row[3],
                    }
                )
        except Exception as exc:
            self.logger.debug(f"sentiment {symbol}: {exc}")

        try:
            with db.get_session() as session:
                row = session.execute(
                    text(
                        """
                        SELECT rank, score, bucket
                        FROM market_data.daily_watchlist
                        WHERE trade_date = CURRENT_DATE AND symbol = :s
                        """
                    ),
                    {"s": symbol},
                ).fetchone()
            if row:
                out["wl_rank"] = row[0]
                out["wl_score"] = row[1]
                out["wl_bucket"] = row[2]
        except Exception:
            pass

        out["genetic_fitness"] = self._genetic_fitness(symbol)
        return out

    def _last_price(self, symbol: str) -> Optional[float]:
        queries = [
            (
                """
                SELECT close FROM market_data.ohlcv
                WHERE symbol = :s AND timeframe = '1d'
                ORDER BY time DESC LIMIT 1
                """
            ),
            (
                """
                SELECT close FROM market_data.ohlcv
                WHERE symbol = :s
                ORDER BY time DESC LIMIT 1
                """
            ),
        ]
        for q in queries:
            try:
                with db.get_session() as session:
                    row = session.execute(text(q), {"s": symbol}).fetchone()
                if row and row[0] is not None:
                    return float(row[0])
            except Exception:
                continue
        return None

    def _genetic_fitness(self, symbol: str) -> Optional[float]:
        queries = [
            "SELECT MAX(fitness) FROM strategies.discovered_strategies WHERE symbol = :s",
            "SELECT MAX(fitness) FROM genetic.strategies WHERE symbol = :s",
        ]
        for q in queries:
            try:
                with db.get_session() as session:
                    row = session.execute(text(q), {"s": symbol}).fetchone()
                if row and row[0] is not None:
                    return float(row[0])
            except Exception:
                continue
        return None

    def _fmt(self, v: Any, pct: bool = False, digits: int = 2) -> str:
        if v is None:
            return "—"
        try:
            f = float(v)
            if pct:
                return f"{f * 100:.{digits}f}%"
            return f"{f:.{digits}f}"
        except Exception:
            return str(v)

    def _format_html(self, symbol: str, d: Dict[str, Any]) -> str:
        conf = d.get("phase_confidence")
        conf_s = f" · ثقة {float(conf):.0f}%" if conf is not None else ""
        lines = [
            f"📊 <b>تحليل {symbol}</b>\n",
            f"السعر: <b>{self._fmt(d.get('last_price'))}</b> | ret1d: {self._fmt(d.get('ret_1d'), pct=True)}",
            f"المرحلة: <b>{d.get('phase') or '—'}</b>{conf_s}",
            f"Beta90: {self._fmt(d.get('beta_90d'), digits=3)} | Vol30: {self._fmt(d.get('vol_30d'), digits=3)}",
            f"RSI14: {self._fmt(d.get('rsi_14'))} | ret5d: {self._fmt(d.get('ret_5d'), pct=True)}",
            f"حجم نسبي: {self._fmt(d.get('volume_ratio_20'))}",
        ]
        sent = d.get("sentiment_label") or "—"
        st = d.get("sentiment_status") or ""
        lines.append(f"المشاعر: <b>{sent}</b> ({st})")
        if d.get("wl_rank") is not None:
            lines.append(
                f"Watchlist اليوم: رتبة {d['wl_rank']} · درجة {self._fmt(d.get('wl_score'))}"
            )
        else:
            lines.append("Watchlist اليوم: غير مدرج")
        gf = d.get("genetic_fitness")
        lines.append(f"جيني: fitness {self._fmt(gf, digits=3)}" if gf is not None else "جيني: —")
        lines.append("\n<i>ليس أمراً تنفيذياً — للمراقبة والورقي فقط.</i>")
        lines.append("أُضيف إلى المراقبة النشطة ✅")
        lines.append("\n<code>/paper {0}</code> · <code>/unwatch {0}</code>".format(symbol))
        return "\n".join(lines)
