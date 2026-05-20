"""
Strategic Analyzer Bot.

Generates live signals when the latest closed candle satisfies conditions
produced by the best genetic DNA. Includes production guards for deduplication,
clear rejection reasons, and BUY/SELL support with optional HOLD emission.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import pandas as pd
from loguru import logger
from sqlalchemy import text

from bots.evaluator.bot import StrategyEvaluator
from config.config_manager import config
from scripts.database import db
from scripts.redis_manager import redis_manager
from scripts.symbol_universe import symbol_universe


class StrategicAnalyzer:
    def __init__(self):
        self.logger = logger.bind(bot="strategic_analyzer")
        self.bot_config = config.get_bot_config("strategic_analyzer") or {"max_signals_per_run": 100}
        self.dedup_window_minutes = int(self.bot_config.get("dedup_window_minutes", 30))
        self.emit_hold_signals = bool(self.bot_config.get("emit_hold_signals", False))
        self.hold_confidence_floor = float(self.bot_config.get("hold_confidence_floor", 0.75))

    def _get_best_genetic_strategy(self, symbol: str) -> Optional[Dict]:
        try:
            with db.get_session() as session:
                row = session.execute(text("""
                    SELECT profit_objective, fitness_score, dna
                    FROM genetic.strategies
                    WHERE symbol = :symbol AND fitness_score > 0
                    ORDER BY fitness_score DESC
                    LIMIT 1
                """), {"symbol": symbol}).fetchone()
                if not row:
                    return None
                import json

                return {
                    "objective": row[0],
                    "fitness": float(row[1]),
                    "dna": row[2] if isinstance(row[2], dict) else json.loads(row[2]),
                }
        except Exception as exc:
            self.logger.error("strategy_fetch_failed symbol={} error={}", symbol, exc)
            return None

    def _fetch_recent_closed_candles(self, symbol: str, timeframe: str, limit: int = 200) -> pd.DataFrame:
        with db.get_session() as session:
            rows = session.execute(text("""
                SELECT time, open, high, low, close, volume
                FROM market_data.ohlcv
                WHERE symbol = :symbol
                  AND timeframe = :timeframe
                  AND time < NOW()
                ORDER BY time DESC
                LIMIT :limit
            """), {"symbol": symbol, "timeframe": timeframe, "limit": limit}).fetchall()
        if not rows:
            return pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"])
        df = pd.DataFrame(rows, columns=["time", "open", "high", "low", "close", "volume"])
        df = df.sort_values("time").reset_index(drop=True)
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype(float)
        return df

    def _is_duplicate_signal(self, signal: Dict) -> bool:
        """Return True if an equivalent signal was recently persisted."""
        try:
            with db.get_session() as session:
                row = session.execute(text("""
                    SELECT 1
                    FROM strategies.signals
                    WHERE symbol = :symbol
                      AND signal_type = :signal
                      AND strategy_name = :strategy
                      AND timeframe = :timeframe
                      AND timestamp >= NOW() - (:dedup_window_minutes * INTERVAL '1 minute')
                    LIMIT 1
                """), {
                    "symbol": signal.get("symbol"),
                    "signal": signal.get("signal"),
                    "strategy": signal.get("strategy"),
                    "timeframe": signal.get("timeframe"),
                    "dedup_window_minutes": self.dedup_window_minutes,
                }).fetchone()
            return row is not None
        except Exception as exc:
            self.logger.warning("signal_dedup_check_failed symbol={} error={}; allowing_save=true", signal.get("symbol"), exc)
            return False

    def _build_signal(self, symbol: str, signal_type: str, strategy: Dict, latest: pd.Series, timeframe: str, reason: str) -> Dict:
        fitness = float(strategy.get("fitness", 0.0))
        return {
            "symbol": symbol,
            "signal": signal_type,
            "strategy": f"Genetic_{strategy['objective']}",
            "confidence": max(0.0, min(0.99, fitness)),
            "price": float(latest["close"]),
            "timestamp": datetime.now(timezone.utc),
            "timeframe": timeframe,
            "metadata": {
                "source": "genetic_last_closed_candle",
                "fitness": fitness,
                "candle_time": str(latest["time"]),
                "reason": reason,
                "dedup_window_minutes": self.dedup_window_minutes,
            },
        }

    def _generate_current_signal_from_dna(self, symbol: str, strategy: Dict) -> Tuple[Optional[Dict], str]:
        dna = dict(strategy.get("dna") or {})
        dna["symbol"] = symbol
        dna["profit_objective"] = strategy.get("objective", dna.get("profit_objective", "scalping"))
        timeframe = dna.get("timeframe") or config.get_nested("profit_objectives", dna["profit_objective"], "timeframe", default="1m")

        min_candles = int(config.get_nested("filters", "min_candles", default=30))
        df = self._fetch_recent_closed_candles(symbol, timeframe, limit=max(200, min_candles * 4))
        if len(df) < min_candles:
            reason = f"insufficient_closed_candles timeframe={timeframe} candles={len(df)} required={min_candles}"
            self.logger.info("signal_not_generated symbol={} reason={}", symbol, reason)
            return None, reason

        evaluator = StrategyEvaluator(db_pool="provided")
        df = evaluator._compute_indicators(df, dna)
        df = evaluator._generate_signals(df, dna)
        latest = df.iloc[-1]
        enter_long = bool(latest.get("enter_long", False))
        exit_long = bool(latest.get("exit_long", False))
        fitness = float(strategy.get("fitness", 0.0))

        if enter_long and not exit_long:
            return self._build_signal(symbol, "BUY", strategy, latest, timeframe, "entry_condition_true_exit_false"), "buy_signal"
        if exit_long and not enter_long:
            return self._build_signal(symbol, "SELL", strategy, latest, timeframe, "exit_condition_true_entry_false"), "sell_signal"
        if enter_long and exit_long:
            reason = f"conflicting_conditions timeframe={timeframe} enter_long=true exit_long=true fitness={fitness:.4f}"
            self.logger.info("signal_not_generated symbol={} reason={}", symbol, reason)
            return None, reason
        if self.emit_hold_signals and fitness >= self.hold_confidence_floor:
            return self._build_signal(symbol, "HOLD", strategy, latest, timeframe, "no_entry_or_exit_high_fitness_hold"), "hold_signal"

        reason = f"conditions_false timeframe={timeframe} enter_long={enter_long} exit_long={exit_long} fitness={fitness:.4f} emit_hold={self.emit_hold_signals}"
        self.logger.info("signal_not_generated symbol={} reason={}", symbol, reason)
        return None, reason

    def _analyze_with_genetic_strategy(self, symbol: str, strategy: Dict) -> Optional[Dict]:
        try:
            signal, _reason = self._generate_current_signal_from_dna(symbol, strategy)
            return signal
        except Exception as exc:
            self.logger.error("genetic_signal_failed symbol={} error={}", symbol, exc)
            return None

    def _save_signal(self, signal: Dict) -> bool:
        if self._is_duplicate_signal(signal):
            self.logger.info(
                "signal_duplicate_skipped symbol={} signal={} strategy={} timeframe={} window_minutes={}",
                signal.get("symbol"), signal.get("signal"), signal.get("strategy"), signal.get("timeframe"), self.dedup_window_minutes,
            )
            return False
        try:
            with db.get_session() as session:
                session.execute(text("""
                    INSERT INTO strategies.signals
                    (timestamp, symbol, strategy_name, signal_type, confidence, price, timeframe, metadata)
                    VALUES (:timestamp, :symbol, :strategy, :signal, :confidence, :price, :timeframe, CAST(:metadata AS jsonb))
                """), {**signal, "metadata": __import__("json").dumps(signal.get("metadata", {}), ensure_ascii=False)})
                session.commit()
            return True
        except Exception as exc:
            self.logger.error("signal_save_failed symbol={} signal={} error={}", signal.get("symbol"), signal.get("signal"), exc)
            return False

    def run(self, symbols: List[str] = None) -> List[Dict]:
        self.logger.info(
            "strategic_analyzer_start mode=genetic_last_closed_candle dedup_window_minutes={} emit_hold_signals={}",
            self.dedup_window_minutes, self.emit_hold_signals,
        )
        if symbols:
            active_symbols = symbols
        else:
            active_symbols, _ = symbol_universe.get_active_universe(timeframe="1m", use_cache=True, include_awakening=True)
        if not active_symbols:
            self.logger.warning("no_active_symbols")
            return []

        all_signals: List[Dict] = []
        pending_genetic: List[str] = []
        max_signals = int(self.bot_config.get("max_signals_per_run", 100))
        rejection_reasons: Dict[str, int] = {}
        for symbol in active_symbols:
            if len(all_signals) >= max_signals:
                break
            best_strategy = self._get_best_genetic_strategy(symbol)
            if not best_strategy:
                pending_genetic.append(symbol)
                rejection_reasons["missing_genetic_strategy"] = rejection_reasons.get("missing_genetic_strategy", 0) + 1
                self.logger.info("signal_not_generated symbol={} reason=missing_genetic_strategy", symbol)
                continue
            try:
                signal, reason = self._generate_current_signal_from_dna(symbol, best_strategy)
            except Exception as exc:
                reason = "genetic_signal_exception"
                signal = None
                self.logger.error("genetic_signal_failed symbol={} error={}", symbol, exc)
            if signal:
                all_signals.append(signal)
                self.logger.success(
                    "signal_generated symbol={} signal={} confidence={:.2f} reason={}",
                    signal["symbol"], signal["signal"], signal["confidence"], signal.get("metadata", {}).get("reason"),
                )
            else:
                rejection_reasons[reason] = rejection_reasons.get(reason, 0) + 1

        saved = sum(1 for signal in all_signals if self._save_signal(signal))
        if pending_genetic:
            redis_manager.set("scientist:pending_symbols", pending_genetic[:50], ttl=3600)
        redis_manager.set("strategic_analyzer:last_summary", {
            "generated": len(all_signals),
            "saved": saved,
            "pending_genetic": len(pending_genetic),
            "rejection_reasons": rejection_reasons,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }, ttl=3600)
        self.logger.success(
            "strategic_analyzer_complete signals={} saved={} pending_genetic={} rejection_reasons={}",
            len(all_signals), saved, len(pending_genetic), rejection_reasons,
        )
        return all_signals


if __name__ == "__main__":
    StrategicAnalyzer().run()
