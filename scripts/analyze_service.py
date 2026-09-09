"""Phase 5 AnalyzeService."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from loguru import logger
from sqlalchemy import text

from config.config_manager import config
from scripts.database import db
from scripts.genetic_fitness import best_fitness_for


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class AnalyzeService:
    def __init__(self):
        self.logger = logger.bind(service="analyze")
        self.cooldown_min = int(config.get("bots.analyze.alert_cooldown_minutes", 30))
        sl = config.get("profit_objectives.short_swings.stoploss_range", [-0.015, -0.03])
        tp = config.get("profit_objectives.short_swings.roi_range", [0.03, 0.08])
        self.default_sl_pct = abs(float(sl[0])) if isinstance(sl, list) and sl else 0.02
        self.default_tp_pct = float(tp[0]) if isinstance(tp, list) and tp else 0.04

    def _effective_risk(self, symbol: str) -> Tuple[float, float, Dict[str, Any]]:
        try:
            from scripts.parameter_editor import ParameterEditor
            eff = ParameterEditor().effective_params(symbol)
            sl = float(eff.get("stop_loss_pct", self.default_sl_pct))
            tp = float(eff.get("take_profit_pct", self.default_tp_pct))
            return sl, tp, eff
        except Exception as exc:
            self.logger.debug(f"effective params fallback: {exc}")
            return self.default_sl_pct, self.default_tp_pct, {}

    def analyze(self, symbol: str, add_watch: bool = True) -> Dict[str, Any]:
        symbol = str(symbol).strip().upper()
        data = self._collect(symbol)
        _, _, eff = self._effective_risk(symbol)
        data["effective_params"] = eff
        html = self._format_html(symbol, data)
        if add_watch:
            self.upsert_monitoring(
                symbol, source="analyze", phase=data.get("phase"),
                sentiment=data.get("sentiment_label"), price=data.get("last_price"),
            )
            data["watching"] = True
        data["symbol"] = symbol
        data["html"] = html
        return data

    def upsert_monitoring(self, symbol, source="manual", phase=None, sentiment=None, price=None):
        with db.get_session() as session:
            session.execute(text("""
                INSERT INTO market_data.active_monitoring
                    (symbol, source, status, last_phase, last_sentiment, last_price, added_at, updated_at)
                VALUES (:symbol, :source, 'active', :phase, :sentiment, :price, NOW(), NOW())
                ON CONFLICT (symbol) DO UPDATE SET
                    status='active', source=EXCLUDED.source,
                    last_phase=COALESCE(EXCLUDED.last_phase, market_data.active_monitoring.last_phase),
                    last_sentiment=COALESCE(EXCLUDED.last_sentiment, market_data.active_monitoring.last_sentiment),
                    last_price=COALESCE(EXCLUDED.last_price, market_data.active_monitoring.last_price),
                    updated_at=NOW()
            """), {"symbol": symbol, "source": source, "phase": phase, "sentiment": sentiment, "price": price})
            session.commit()

    def unwatch(self, symbol: str) -> bool:
        symbol = str(symbol).strip().upper()
        with db.get_session() as session:
            res = session.execute(text("""
                UPDATE market_data.active_monitoring SET status='closed', updated_at=NOW()
                WHERE symbol=:symbol AND status='active'
            """), {"symbol": symbol})
            session.commit()
            return (res.rowcount or 0) > 0

    def list_watching(self):
        with db.get_session() as session:
            rows = session.execute(text("""
                SELECT symbol, source, last_phase, last_sentiment, last_price, added_at
                FROM market_data.active_monitoring WHERE status='active' ORDER BY added_at DESC
            """)).fetchall()
        return [{"symbol": r[0], "source": r[1], "phase": r[2], "sentiment": r[3], "price": r[4], "added_at": r[5]} for r in rows]

    def list_watchlist_today(self):
        with db.get_session() as session:
            rows = session.execute(text("""
                SELECT rank, symbol, score, phase, sentiment_label
                FROM market_data.daily_watchlist WHERE trade_date=CURRENT_DATE ORDER BY rank
            """)).fetchall()
        return [{"rank": r[0], "symbol": r[1], "score": r[2], "phase": r[3], "sentiment": r[4]} for r in rows]

    def open_paper(self, symbol: str, source: str = "manual") -> Dict[str, Any]:
        symbol = str(symbol).strip().upper()
        price = self._last_price(symbol)
        if price is None or price <= 0:
            return {"ok": False, "error": "لا يوجد سعر متاح لفتح صفقة ورقية"}
        sl_pct, tp_pct, eff = self._effective_risk(symbol)
        with db.get_session() as session:
            session.execute(text("UPDATE market_data.paper_trades SET status='cancelled', closed_at=NOW() WHERE symbol=:symbol AND status='open'"), {"symbol": symbol})
            sl = price * (1 - sl_pct)
            tp = price * (1 + tp_pct)
            row = session.execute(text("""
                INSERT INTO market_data.paper_trades
                    (symbol, side, status, entry_price, stop_loss, take_profit, qty, opened_at, source, created_at)
                VALUES (:symbol,'long','open',:entry,:sl,:tp,1,NOW(),:source,NOW()) RETURNING id
            """), {"symbol": symbol, "entry": price, "sl": sl, "tp": tp, "source": source}).fetchone()
            session.commit()
            pid = int(row[0]) if row else None
        self.upsert_monitoring(symbol, source="paper", price=price)
        return {"ok": True, "id": pid, "symbol": symbol, "entry": price, "stop_loss": sl, "take_profit": tp, "sl_pct": sl_pct, "tp_pct": tp_pct, "objective": eff.get("objective")}

    def close_paper(self, symbol: str) -> Dict[str, Any]:
        symbol = str(symbol).strip().upper()
        price = self._last_price(symbol)
        with db.get_session() as session:
            row = session.execute(text("""
                SELECT id, entry_price FROM market_data.paper_trades
                WHERE symbol=:symbol AND status='open' ORDER BY opened_at DESC LIMIT 1
            """), {"symbol": symbol}).fetchone()
            if not row:
                return {"ok": False, "error": "لا توجد صفقة ورقية مفتوحة"}
            pid, entry = int(row[0]), float(row[1])
            exit_p = float(price) if price else entry
            pnl = (exit_p - entry) / entry * 100.0 if entry else 0.0
            session.execute(text("UPDATE market_data.paper_trades SET status='closed', exit_price=:exit, pnl_pct=:pnl, closed_at=NOW() WHERE id=:id"), {"exit": exit_p, "pnl": pnl, "id": pid})
            session.commit()
        return {"ok": True, "id": pid, "exit": exit_p, "pnl_pct": round(pnl, 3)}

    def list_open_papers(self):
        with db.get_session() as session:
            rows = session.execute(text("""
                SELECT id, symbol, entry_price, stop_loss, take_profit, opened_at
                FROM market_data.paper_trades WHERE status='open' ORDER BY opened_at DESC
            """)).fetchall()
        return [{"id": r[0], "symbol": r[1], "entry": r[2], "sl": r[3], "tp": r[4], "opened_at": r[5]} for r in rows]

    def check_active_and_papers(self) -> Dict[str, Any]:
        return {"alerts": [], "watching": len(self.list_watching()), "papers": len(self.list_open_papers())}

    def _collect(self, symbol: str) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        price, source = self._last_price_with_source(symbol)
        out["last_price"] = price
        out["price_source"] = source
        out["ret_1d"] = None
        try:
            with db.get_session() as session:
                row = session.execute(text("""
                    SELECT phase, phase_confidence, beta_90d, volatility_30d, accumulation_score, status, computed_at
                    FROM market_data.stock_personalities WHERE symbol=:s
                """), {"s": symbol}).fetchone()
            if row:
                out.update({"phase": row[0], "phase_confidence": row[1], "beta_90d": row[2], "vol_30d": row[3], "accumulation": row[4], "personality_status": row[5], "personality_at": row[6]})
        except Exception as exc:
            self.logger.debug(f"personality {symbol}: {exc}")
        try:
            with db.get_session() as session:
                row = session.execute(text("""
                    SELECT ret_1d, ret_5d, rsi_14, volume_ratio_20, status, computed_at
                    FROM market_data.stock_features WHERE symbol=:s
                """), {"s": symbol}).fetchone()
            if row:
                out.update({"ret_1d": row[0], "ret_5d": row[1], "rsi_14": row[2], "volume_ratio_20": row[3], "features_status": row[4], "features_at": row[5]})
        except Exception as exc:
            self.logger.debug(f"features {symbol}: {exc}")
        try:
            with db.get_session() as session:
                row = session.execute(text("""
                    SELECT sentiment_label, sentiment_score, status, articles_count
                    FROM market_data.news_sentiments WHERE symbol=:s
                """), {"s": symbol}).fetchone()
            if row:
                out.update({"sentiment_label": row[0], "sentiment_score": row[1], "sentiment_status": row[2], "articles_count": row[3]})
        except Exception as exc:
            self.logger.debug(f"sentiment {symbol}: {exc}")
        try:
            with db.get_session() as session:
                row = session.execute(text("""
                    SELECT rank, score, bucket FROM market_data.daily_watchlist
                    WHERE trade_date=CURRENT_DATE AND symbol=:s
                """), {"s": symbol}).fetchone()
            if row:
                out["wl_rank"], out["wl_score"], out["wl_bucket"] = row[0], row[1], row[2]
        except Exception:
            pass
        out["genetic_fitness"] = best_fitness_for(symbol)
        return out

    def _ohlcv_close(self, symbol: str, timeframe: str) -> Optional[float]:
        try:
            with db.get_session() as session:
                row = session.execute(text("SELECT close FROM market_data.ohlcv WHERE symbol=:s AND timeframe=:tf ORDER BY time DESC LIMIT 1"), {"s": symbol, "tf": timeframe}).fetchone()
            if row and row[0] is not None:
                return float(row[0])
        except Exception:
            return None
        return None

    def _quote_from_sahmk(self, symbol: str) -> Optional[float]:
        key = (config.get_sahmk_api_key() or "").strip()
        if not key:
            return None
        base = (config.get_sahmk_base_url() or "https://api.sahmk.sa/api/v1").rstrip("/")
        try:
            resp = requests.get(f"{base}/quote/{symbol}/", headers={"X-API-Key": key, "Accept": "application/json"}, timeout=15)
            if resp.status_code != 200:
                return None
            price = (resp.json() or {}).get("price")
            return float(price) if price is not None else None
        except Exception:
            return None

    def _last_price_with_source(self, symbol: str):
        px = self._ohlcv_close(symbol, "1m")
        if px and px > 0:
            return px, "1m"
        px = self._quote_from_sahmk(symbol)
        if px and px > 0:
            return px, "quote"
        px = self._ohlcv_close(symbol, "1d")
        if px and px > 0:
            return px, "1d"
        return None, "none"

    def _last_price(self, symbol: str):
        price, _ = self._last_price_with_source(symbol)
        return price

    def _fmt(self, v, pct=False, digits=2):
        if v is None:
            return "—"
        try:
            f = float(v)
            return f"{f * 100:.{digits}f}%" if pct else f"{f:.{digits}f}"
        except Exception:
            return str(v)

    def _format_html(self, symbol: str, d: Dict[str, Any]) -> str:
        conf = d.get("phase_confidence")
        conf_s = f" · ثقة {float(conf):.0f}%" if conf is not None else ""
        src = d.get("price_source") or ""
        src_s = f" ({src})" if src and src not in ("none",) else ""
        lines = [
            f"📊 <b>تحليل {symbol}</b>\n",
            f"السعر: <b>{self._fmt(d.get('last_price'))}</b>{src_s} | ret1d: {self._fmt(d.get('ret_1d'), pct=True)}",
            f"المرحلة: <b>{d.get('phase') or '—'}</b>{conf_s}",
            f"Beta90: {self._fmt(d.get('beta_90d'), digits=3)} | Vol30: {self._fmt(d.get('vol_30d'), digits=3)}",
            f"RSI14: {self._fmt(d.get('rsi_14'))} | ret5d: {self._fmt(d.get('ret_5d'), pct=True)}",
            f"حجم نسبي: {self._fmt(d.get('volume_ratio_20'))}",
        ]
        sent = d.get("sentiment_label") or "—"
        st = d.get("sentiment_status") or ""
        lines.append(f"المشاعر: <b>{sent}</b> ({st})")
        if d.get("wl_rank") is not None:
            lines.append(f"Watchlist اليوم: رتبة {d['wl_rank']} · درجة {self._fmt(d.get('wl_score'))}")
        else:
            lines.append("Watchlist اليوم: غير مدرج")
        gf = d.get("genetic_fitness")
        lines.append(f"جيني: fitness {self._fmt(gf, digits=3)}" if gf is not None else "جيني: —")
        eff = d.get("effective_params") or {}
        if eff:
            lines.append(f"إعدادات: SL {self._fmt(eff.get('stop_loss_pct'), pct=True)} · TP {self._fmt(eff.get('take_profit_pct'), pct=True)} · {eff.get('objective') or '—'}")
        lines.append("\n<i>ليس أمراً تنفيذياً — للمراقبة والورقي فقط.</i>")
        lines.append("أُضيف إلى المراقبة النشطة ✅")
        lines.append(f"\n<code>/paper {symbol}</code> · <code>/strategy {symbol}</code> · <code>/unwatch {symbol}</code>")
        return "\n".join(lines)
