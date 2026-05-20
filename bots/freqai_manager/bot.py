"""
FreqAI Manager Bot
يدير عملية تدريب نماذج FreqAI باستخدام أفضل الاستراتيجيات التي أنتجها المحرك الجيني (Scientist).

Production hardening notes:
- Keeps backward compatibility with the existing genetic-strategy selection.
- Persists generated FreqAI configs to disk.
- Runs a configurable Freqtrade/FreqAI training command when available.
- Records model status in Redis and bots.status without requiring a new DB schema.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from loguru import logger
from sqlalchemy import text

from scripts.database import db, update_bot_status
from scripts.redis_manager import redis_manager


class FreqAIManager:
    def __init__(self):
        self.logger = logger.bind(bot="freqai_manager")
        self.project_root = Path(os.getenv("PROJECT_ROOT", "/app"))
        self.freqtrade_dir = Path(os.getenv("FREQTRADE_DIR", self._detect_freqtrade_dir()))
        self.user_data_dir = Path(os.getenv("FREQTRADE_USER_DATA_DIR", self.freqtrade_dir / "user_data"))
        self.config_output_dir = Path(os.getenv("FREQAI_CONFIG_OUTPUT_DIR", self.user_data_dir / "freqai_configs"))
        self.models_dir = Path(os.getenv("FREQAI_MODELS_DIR", self.user_data_dir / "models"))
        self.strategy_name = os.getenv("FREQAI_STRATEGY", "FreqAIStrategy")
        self.freqai_model = os.getenv("FREQAI_MODEL", "LightGBMClassifier")
        self.train_timeout_seconds = int(os.getenv("FREQAI_TRAIN_TIMEOUT_SECONDS", "1800"))
        self.enable_training = os.getenv("FREQAI_TRAIN_ENABLED", "true").lower() not in {"0", "false", "no"}
        self.base_config_path = Path(os.getenv("FREQTRADE_BASE_CONFIG", self._detect_base_config()))
        self.config_output_dir.mkdir(parents=True, exist_ok=True)
        self.models_dir.mkdir(parents=True, exist_ok=True)

    def _detect_freqtrade_dir(self) -> str:
        candidates = [Path("/freqtrade"), Path("/app/freqtrade"), Path.cwd() / "freqtrade"]
        for candidate in candidates:
            if candidate.exists():
                return str(candidate)
        return "/app/freqtrade"

    def _detect_base_config(self) -> str:
        candidates = [
            self.user_data_dir / "config.json" if hasattr(self, "user_data_dir") else Path("/freqtrade/user_data/config.json"),
            Path("/freqtrade/user_data/config.json"),
            Path("/app/freqtrade/user_data/config.json"),
            Path("/app/freqtrade/config.json"),
            Path.cwd() / "freqtrade" / "config.json",
        ]
        for candidate in candidates:
            if candidate.exists():
                return str(candidate)
        return str(Path("/app/freqtrade/config.json"))

    def get_best_genetic_strategies(self, limit: int = 10) -> List[Dict]:
        """يجلب أفضل الاستراتيجيات الجينية من قاعدة البيانات."""
        try:
            with db.get_session() as session:
                result = session.execute(text("""
                    SELECT s.symbol, s.profit_objective, s.fitness_score,
                           COALESCE(p.total_profit_pct, 0.0) as total_profit_pct,
                           COALESCE(p.win_rate, 0.0) as win_rate,
                           s.dna
                    FROM genetic.strategies s
                    LEFT JOIN genetic.performance p ON s.strategy_hash = p.strategy_hash
                    WHERE s.fitness_score > 0
                    ORDER BY s.fitness_score DESC
                    LIMIT :limit
                """), {"limit": limit})

                strategies = []
                for row in result.fetchall():
                    strategies.append({
                        "symbol": row[0],
                        "objective": row[1],
                        "fitness": float(row[2]),
                        "profit": float(row[3]),
                        "win_rate": float(row[4]),
                        "dna": row[5] if isinstance(row[5], dict) else json.loads(row[5]) if row[5] else {},
                    })
                return strategies
        except Exception as exc:
            self.logger.error("freqai_fetch_genetic_strategies_failed error={}", exc)
            return []

    def generate_freqai_config(self, strategy: Dict) -> Dict:
        """يحول الـ DNA الجيني إلى إعدادات FreqAI متوافقة مع الإعدادات الحالية."""
        symbol = strategy["symbol"]
        dna = strategy.get("dna") or {}

        features = []
        for block in dna.get("building_blocks", []):
            indicator = block.get("indicator")
            period = block.get("period")
            if indicator and period:
                features.append(f"{indicator}_{period}")

        return {
            "freqai": {
                "enabled": True,
                "purge_old_models": 2,
                "train_period_days": 30,
                "backtest_period_days": 7,
                "identifier": f"genetic_{symbol}_{strategy['objective']}",
                "feature_parameters": {
                    "include_timeframes": ["5m", "15m", "1h"],
                    "include_corr_pairlist": [],
                    "label_period_candles": 24,
                    "indicator_periods_candles": [10, 20, 30],
                    "genetic_features": features,
                },
                "data_split_parameters": {
                    "test_size": 0.25,
                    "random_state": 42,
                },
                "model_training_parameters": {
                    "n_estimators": 100,
                    "learning_rate": 0.05,
                    "max_depth": 5,
                },
            }
        }

    def _load_base_config(self) -> Dict:
        if self.base_config_path.exists():
            try:
                with self.base_config_path.open("r", encoding="utf-8") as handle:
                    return json.load(handle)
            except Exception as exc:
                self.logger.warning("freqai_base_config_load_failed path={} error={}", self.base_config_path, exc)
        return {}

    def _merge_config(self, base_config: Dict, generated_config: Dict, strategy: Dict) -> Dict:
        merged = deepcopy(base_config) if isinstance(base_config, dict) else {}
        merged.setdefault("strategy", self.strategy_name)
        merged.setdefault("freqaimodel", self.freqai_model)
        merged.setdefault("trading_mode", "spot")
        merged.setdefault("dry_run", True)
        merged.setdefault("exchange", {}).setdefault("pair_whitelist", [])

        symbol = strategy["symbol"]
        preferred_pairs = [f"{symbol}/USDT", f"{symbol}/SAR", symbol]
        pair_whitelist = merged.get("exchange", {}).get("pair_whitelist") or []
        if not pair_whitelist:
            merged["exchange"]["pair_whitelist"] = preferred_pairs[:1]

        merged["freqai"] = {**merged.get("freqai", {}), **generated_config.get("freqai", {})}
        merged.setdefault("metadata", {})
        merged["metadata"].update({
            "managed_by": "freqai_manager",
            "symbol": symbol,
            "objective": strategy.get("objective"),
            "fitness": strategy.get("fitness"),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        })
        return merged

    def save_freqai_config(self, strategy: Dict, generated_config: Dict) -> Path:
        symbol = strategy["symbol"]
        objective = str(strategy.get("objective", "default")).replace("/", "_")
        base_config = self._load_base_config()
        full_config = self._merge_config(base_config, generated_config, strategy)

        config_path = self.config_output_dir / f"freqai_config_{symbol}_{objective}.json"
        latest_path = self.config_output_dir / f"freqai_config_{symbol}.json"
        with config_path.open("w", encoding="utf-8") as handle:
            json.dump(full_config, handle, ensure_ascii=False, indent=2)
        with latest_path.open("w", encoding="utf-8") as handle:
            json.dump(full_config, handle, ensure_ascii=False, indent=2)
        self.logger.info("freqai_config_saved symbol={} path={}", symbol, config_path)
        return config_path

    def _build_training_command(self, config_path: Path) -> List[str]:
        command_template = os.getenv("FREQAI_TRAIN_COMMAND", "").strip()
        if command_template:
            return command_template.format(
                config_path=str(config_path),
                strategy=self.strategy_name,
                model=self.freqai_model,
                user_data_dir=str(self.user_data_dir),
            ).split()

        freqtrade_bin = shutil.which("freqtrade")
        if not freqtrade_bin:
            return []
        return [
            freqtrade_bin,
            "backtesting",
            "--config", str(config_path),
            "--strategy", self.strategy_name,
            "--freqaimodel", self.freqai_model,
            "--user-data-dir", str(self.user_data_dir),
        ]

    def _find_latest_model_artifact(self, identifier: str) -> Optional[str]:
        candidates = []
        if self.models_dir.exists():
            for path in self.models_dir.rglob("*"):
                if identifier in str(path) and (path.is_file() or path.is_dir()):
                    try:
                        candidates.append((path.stat().st_mtime, path))
                    except OSError:
                        continue
        if not candidates:
            return None
        return str(sorted(candidates, reverse=True)[0][1])

    def _record_model_status(self, symbol: str, status: Dict) -> None:
        status_payload = {**status, "updated_at": datetime.now(timezone.utc).isoformat()}
        redis_manager.set(f"freqai:model_status:{symbol}", status_payload, ttl=7 * 86400)
        redis_manager.set(f"freqai:model_ready:{symbol}", status_payload.get("status") == "success", ttl=86400)
        try:
            with db.get_session() as session:
                update_bot_status(session, "freqai_manager", status_payload.get("status", "unknown"), status_payload.get("error"), status_payload)
        except Exception as exc:
            self.logger.warning("freqai_status_db_update_failed symbol={} error={}", symbol, exc)

    def train_model(self, strategy: Dict, config_path: Path, generated_config: Dict) -> Dict:
        symbol = strategy["symbol"]
        identifier = generated_config["freqai"].get("identifier", symbol)
        started_at = datetime.now(timezone.utc).isoformat()
        command = self._build_training_command(config_path)

        if not self.enable_training:
            return {
                "status": "skipped",
                "symbol": symbol,
                "reason": "FREQAI_TRAIN_ENABLED=false",
                "config_path": str(config_path),
                "model_path": self._find_latest_model_artifact(identifier),
                "last_trained_at": None,
                "started_at": started_at,
            }

        if not command:
            return {
                "status": "failed",
                "symbol": symbol,
                "error": "freqtrade executable not found and FREQAI_TRAIN_COMMAND is not configured",
                "config_path": str(config_path),
                "model_path": None,
                "last_trained_at": None,
                "started_at": started_at,
            }

        self.logger.info("freqai_training_start symbol={} command={}", symbol, " ".join(command))
        started = time.time()
        try:
            completed = subprocess.run(
                command,
                cwd=str(self.freqtrade_dir) if self.freqtrade_dir.exists() else None,
                capture_output=True,
                text=True,
                timeout=self.train_timeout_seconds,
                check=False,
            )
            duration = round(time.time() - started, 2)
            model_path = self._find_latest_model_artifact(identifier)
            status = "success" if completed.returncode == 0 else "failed"
            payload = {
                "status": status,
                "symbol": symbol,
                "returncode": completed.returncode,
                "duration_seconds": duration,
                "config_path": str(config_path),
                "model_path": model_path,
                "last_trained_at": datetime.now(timezone.utc).isoformat() if status == "success" else None,
                "started_at": started_at,
                "stdout_tail": (completed.stdout or "")[-2000:],
                "stderr_tail": (completed.stderr or "")[-2000:],
            }
            if status == "failed":
                payload["error"] = f"training command returned {completed.returncode}"
            return payload
        except subprocess.TimeoutExpired as exc:
            return {
                "status": "failed",
                "symbol": symbol,
                "error": f"training timeout after {self.train_timeout_seconds}s: {exc}",
                "config_path": str(config_path),
                "model_path": self._find_latest_model_artifact(identifier),
                "last_trained_at": None,
                "started_at": started_at,
            }
        except Exception as exc:
            return {
                "status": "failed",
                "symbol": symbol,
                "error": str(exc),
                "config_path": str(config_path),
                "model_path": self._find_latest_model_artifact(identifier),
                "last_trained_at": None,
                "started_at": started_at,
            }

    def run(self) -> List[Dict]:
        """التشغيل الرئيسي للمدير."""
        self.logger.info("freqai_manager_start")
        best_strategies = self.get_best_genetic_strategies(limit=int(os.getenv("FREQAI_STRATEGY_LIMIT", "5")))
        if not best_strategies:
            status = {"status": "skipped", "reason": "no_genetic_strategies_found"}
            self._record_model_status("__global__", status)
            self.logger.warning("freqai_manager_no_genetic_strategies")
            return []

        self.logger.info("freqai_manager_strategies_found count={}", len(best_strategies))
        results: List[Dict] = []
        for strategy in best_strategies:
            symbol = strategy["symbol"]
            try:
                generated_config = self.generate_freqai_config(strategy)
                config_path = self.save_freqai_config(strategy, generated_config)
                result = self.train_model(strategy, config_path, generated_config)
                self._record_model_status(symbol, result)
                results.append(result)
                if result.get("status") == "success":
                    self.logger.success("freqai_training_success symbol={} model_path={}", symbol, result.get("model_path"))
                else:
                    self.logger.warning("freqai_training_not_success symbol={} status={} reason={}", symbol, result.get("status"), result.get("error") or result.get("reason"))
            except Exception as exc:
                failure = {
                    "status": "failed",
                    "symbol": symbol,
                    "error": str(exc),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
                self._record_model_status(symbol, failure)
                results.append(failure)
                self.logger.error("freqai_strategy_processing_failed symbol={} error={}", symbol, exc)

        summary = {
            "status": "success" if any(item.get("status") == "success" for item in results) else "degraded",
            "processed": len(results),
            "success": sum(1 for item in results if item.get("status") == "success"),
            "failed": sum(1 for item in results if item.get("status") == "failed"),
            "skipped": sum(1 for item in results if item.get("status") == "skipped"),
        }
        self._record_model_status("__summary__", summary)
        self.logger.success("freqai_manager_complete summary={}", summary)
        return results


if __name__ == "__main__":
    manager = FreqAIManager()
    manager.run()
