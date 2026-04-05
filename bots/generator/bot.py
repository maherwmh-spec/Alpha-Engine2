"""
bots/generator/bot.py
═══════════════════════════════════════════════════════════════
Alpha-Engine2 — المحرك الجيني لتوليد استراتيجيات التداول

الفلسفة: "المُكتشف" وليس "المُنتقي"
النظام يخترع استراتيجيات جديدة من مكونات خام (مكعبات البناء)
باستخدام خوارزميات جينية: توليد عشوائي → تقييم → تزاوج → طفرة → تكرار
═══════════════════════════════════════════════════════════════
"""
from __future__ import annotations

import hashlib
import json
import random
import copy
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger as _loguru_logger
def get_logger(name): return _loguru_logger.bind(bot=name)

# ─────────────────────────────────────────────────────────────
# PROFIT OBJECTIVES → RISK BOXES  (الربط المنطقي)
# ─────────────────────────────────────────────────────────────
OBJECTIVE_TO_RISK_BOX: Dict[str, str] = {
    "scalping":       "speculation",    # مضاربة خاطفة → تخصيص منخفض
    "short_swings":   "growth",         # موجات قصيرة  → تخصيص متوسط
    "medium_trends":  "investment",     # موجات متوسطة → تخصيص عالٍ
    "momentum":       "big_strategy",   # انفجارات سعرية → ATR-based
}

PROFIT_OBJECTIVES = list(OBJECTIVE_TO_RISK_BOX.keys())

# ─────────────────────────────────────────────────────────────
# BUILDING BLOCKS — مكعبات البناء الفنية
# كل مؤشر يحمل: نوعه، معاملاته مع نطاقاتها، والعمليات الممكنة
# ─────────────────────────────────────────────────────────────
BUILDING_BLOCKS: Dict[str, Dict] = {

    # ── المذبذبات (Oscillators) ──────────────────────────────
    "RSI": {
        "type": "oscillator",
        "params": {
            "period": {"min": 7,  "max": 21, "step": 1,   "default": 14},
        },
        "entry_ops":  ["<", "<="],          # شراء عند التشبع البيعي
        "exit_ops":   [">", ">="],          # بيع عند التشبع الشرائي
        "entry_values": [25, 30, 35, 40],
        "exit_values":  [60, 65, 70, 75],
    },

    "STOCH": {
        "type": "oscillator",
        "params": {
            "fastk_period": {"min": 5,  "max": 21, "step": 1, "default": 14},
            "slowk_period": {"min": 3,  "max": 7,  "step": 1, "default": 3},
            "slowd_period": {"min": 3,  "max": 7,  "step": 1, "default": 3},
        },
        "entry_ops":  ["<"],
        "exit_ops":   [">"],
        "entry_values": [20, 25, 30],
        "exit_values":  [70, 75, 80],
    },

    "CCI": {
        "type": "oscillator",
        "params": {
            "period": {"min": 10, "max": 30, "step": 2, "default": 20},
        },
        "entry_ops":  ["<"],
        "exit_ops":   [">"],
        "entry_values": [-100, -150, -200],
        "exit_values":  [100,  150,  200],
    },

    # ── المتوسطات المتحركة (Moving Averages) ─────────────────
    "SMA_CROSS": {
        "type": "ma_cross",
        "params": {
            "fast_period": {"min": 5,  "max": 20,  "step": 1, "default": 10},
            "slow_period": {"min": 20, "max": 100, "step": 5, "default": 50},
        },
        "entry_ops": ["crosses_above"],
        "exit_ops":  ["crosses_below"],
    },

    "EMA_CROSS": {
        "type": "ma_cross",
        "params": {
            "fast_period": {"min": 5,  "max": 20,  "step": 1, "default": 9},
            "slow_period": {"min": 20, "max": 100, "step": 5, "default": 21},
        },
        "entry_ops": ["crosses_above"],
        "exit_ops":  ["crosses_below"],
    },

    "EMA_PRICE": {
        "type": "ma_price",
        "params": {
            "period": {"min": 10, "max": 200, "step": 5, "default": 50},
        },
        "entry_ops": ["price_above"],   # السعر فوق المتوسط
        "exit_ops":  ["price_below"],
    },

    # ── مؤشرات الاتجاه (Trend) ───────────────────────────────
    "MACD": {
        "type": "trend",
        "params": {
            "fast_period":   {"min": 8,  "max": 16, "step": 1, "default": 12},
            "slow_period":   {"min": 20, "max": 30, "step": 1, "default": 26},
            "signal_period": {"min": 7,  "max": 12, "step": 1, "default": 9},
        },
        "entry_ops": ["macd_crosses_above_signal"],
        "exit_ops":  ["macd_crosses_below_signal"],
    },

    "ADX": {
        "type": "trend_strength",
        "params": {
            "period": {"min": 10, "max": 20, "step": 1, "default": 14},
        },
        "entry_ops": [">"],             # قوة الاتجاه
        "entry_values": [20, 25, 30],
    },

    # ── التقلب (Volatility) ───────────────────────────────────
    "BOLLINGER": {
        "type": "volatility",
        "params": {
            "period": {"min": 15, "max": 25, "step": 1, "default": 20},
            "std":    {"min": 1.5, "max": 3.0, "step": 0.5, "default": 2.0},
        },
        "entry_ops": ["price_below_lower"],   # كسر الحزام السفلي
        "exit_ops":  ["price_above_upper"],   # كسر الحزام العلوي
    },

    "ATR": {
        "type": "volatility",
        "params": {
            "period":     {"min": 10, "max": 20, "step": 1,   "default": 14},
            "multiplier": {"min": 1.0, "max": 3.0, "step": 0.5, "default": 1.5},
        },
        "entry_ops": ["atr_breakout"],
    },

    # ── الحجم (Volume) ────────────────────────────────────────
    "VOLUME_SURGE": {
        "type": "volume",
        "params": {
            "ma_period":  {"min": 10, "max": 30, "step": 5, "default": 20},
            "multiplier": {"min": 1.2, "max": 3.0, "step": 0.2, "default": 1.5},
        },
        "entry_ops": ["volume_above_ma"],
    },
}

# ─────────────────────────────────────────────────────────────
# معاملات الاستراتيجية (Stop-Loss / ROI) حسب الهدف
# ─────────────────────────────────────────────────────────────
OBJECTIVE_PARAMS: Dict[str, Dict] = {
    "scalping": {
        "stoploss_range":  (-0.005, -0.015),   # 0.5% - 1.5%
        "roi_range":       (0.005,  0.02),      # 0.5% - 2%
        "trailing_stop":   True,
        "timeframe":       "1m",
    },
    "short_swings": {
        "stoploss_range":  (-0.015, -0.03),    # 1.5% - 3%
        "roi_range":       (0.03,   0.08),      # 3% - 8%
        "trailing_stop":   True,
        "timeframe":       "5m",
    },
    "medium_trends": {
        "stoploss_range":  (-0.03,  -0.06),    # 3% - 6%
        "roi_range":       (0.08,   0.20),      # 8% - 20%
        "trailing_stop":   False,
        "timeframe":       "15m",
    },
    "momentum": {
        "stoploss_range":  (-0.02,  -0.04),    # 2% - 4% (ATR-based)
        "roi_range":       (0.05,   0.15),      # 5% - 15%
        "trailing_stop":   True,
        "timeframe":       "5m",
    },
}


# ═══════════════════════════════════════════════════════════════
class GeneticGenerator:
    """
    المحرك الجيني لتوليد وتطوير استراتيجيات التداول.

    الدورة الكاملة:
      1. generate_population()  → توليد جيل عشوائي
      2. [Evaluator يُقيّم كل استراتيجية]
      3. select_elite()         → اختيار النخبة
      4. crossover()            → التزاوج بين النخبة
      5. mutate()               → تطبيق طفرات عشوائية
      6. تكرار من الخطوة 2
    """

    def __init__(self):
        self.logger = get_logger("GeneticGenerator")
        self._rng = random.Random()   # RNG مستقل (قابل للـ seed)

    # ─────────────────────────────────────────────────────────
    # 1. توليد جيل كامل
    # ─────────────────────────────────────────────────────────
    def generate_population(
        self,
        symbol: str,
        profit_objective: str,
        size: int = 50,
        seed: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        يولّد `size` استراتيجية عشوائية لسهم وهدف ربحي محدد.
        كل استراتيجية عبارة عن "شيفرة جينية" JSON.
        """
        if seed is not None:
            self._rng.seed(seed)

        if profit_objective not in PROFIT_OBJECTIVES:
            raise ValueError(
                f"profit_objective must be one of {PROFIT_OBJECTIVES}, got '{profit_objective}'"
            )

        population = []
        for _ in range(size):
            dna = self._generate_random_dna(symbol, profit_objective)
            population.append(dna)

        self.logger.info(
            f"🧬 Generated population: {len(population)} strategies "
            f"for {symbol} [{profit_objective}]"
        )
        return population

    # ─────────────────────────────────────────────────────────
    # 2. توليد شيفرة جينية واحدة عشوائية
    # ─────────────────────────────────────────────────────────
    def _generate_random_dna(
        self, symbol: str, profit_objective: str
    ) -> Dict[str, Any]:
        """
        يولّد وصفة JSON واحدة عشوائية.
        البنية:
          {
            "name":              str,
            "symbol":            str,
            "profit_objective":  str,
            "risk_box":          str,
            "timeframe":         str,
            "stoploss":          float,
            "roi":               dict,
            "trailing_stop":     bool,
            "entry_conditions":  list[dict],
            "exit_conditions":   list[dict],
          }
        """
        obj_params = OBJECTIVE_PARAMS[profit_objective]
        risk_box   = OBJECTIVE_TO_RISK_BOX[profit_objective]

        # ── معاملات الاستراتيجية ──
        stoploss = round(
            self._rng.uniform(*obj_params["stoploss_range"]), 4
        )
        roi_min, roi_max = obj_params["roi_range"]
        roi_target = round(self._rng.uniform(roi_min, roi_max), 4)
        roi = {
            "0":   roi_target,
            "60":  round(roi_target * 0.6, 4),
            "120": round(roi_target * 0.3, 4),
        }

        # ── اختيار شروط الدخول (1-3 شروط) ──
        n_entry = self._rng.randint(1, 3)
        entry_conditions = self._sample_conditions("entry", n_entry)

        # ── اختيار شروط الخروج (1-2 شروط) ──
        n_exit = self._rng.randint(1, 2)
        exit_conditions = self._sample_conditions("exit", n_exit)

        # ── بناء الشيفرة ──
        dna: Dict[str, Any] = {
            "name":             f"GeneticStrategy_{self._short_id()}",
            "symbol":           symbol,
            "profit_objective": profit_objective,
            "risk_box":         risk_box,
            "timeframe":        obj_params["timeframe"],
            "stoploss":         stoploss,
            "roi":              roi,
            "trailing_stop":    obj_params["trailing_stop"],
            "entry_conditions": entry_conditions,
            "exit_conditions":  exit_conditions,
        }

        # ── حساب الـ hash ──
        dna["hash"] = self.compute_hash(dna)
        return dna

    # ─────────────────────────────────────────────────────────
    # 3. اختيار شروط عشوائية (entry / exit)
    # ─────────────────────────────────────────────────────────
    def _sample_conditions(
        self, condition_type: str, count: int
    ) -> List[Dict[str, Any]]:
        """
        يختار `count` شروط عشوائية من مكعبات البناء.
        condition_type: 'entry' أو 'exit'
        """
        ops_key    = f"{condition_type}_ops"
        values_key = f"{condition_type}_values"

        # المؤشرات التي تدعم هذا النوع من الشروط
        eligible = [
            name for name, block in BUILDING_BLOCKS.items()
            if ops_key in block
        ]

        chosen_indicators = self._rng.sample(
            eligible, min(count, len(eligible))
        )

        conditions = []
        for indicator_name in chosen_indicators:
            block = BUILDING_BLOCKS[indicator_name]
            cond  = {"indicator": indicator_name}

            # ── معاملات المؤشر ──
            for param_name, param_spec in block["params"].items():
                if isinstance(param_spec["min"], float):
                    val = round(
                        self._rng.uniform(param_spec["min"], param_spec["max"]),
                        2
                    )
                else:
                    val = self._rng.randrange(
                        param_spec["min"],
                        param_spec["max"] + 1,
                        param_spec.get("step", 1),
                    )
                cond[param_name] = val

            # ── العملية المنطقية ──
            cond["operator"] = self._rng.choice(block[ops_key])

            # ── القيمة المرجعية (إن وُجدت) ──
            if values_key in block:
                cond["value"] = self._rng.choice(block[values_key])

            conditions.append(cond)

        return conditions

    # ─────────────────────────────────────────────────────────
    # 4. اختيار النخبة
    # ─────────────────────────────────────────────────────────
    def select_elite(
        self,
        population: List[Dict[str, Any]],
        elite_ratio: float = 0.2,
    ) -> List[Dict[str, Any]]:
        """
        يختار أفضل `elite_ratio` من المجتمع بناءً على fitness_score.
        يتوقع أن كل فرد في المجتمع يحمل مفتاح 'fitness_score'.
        """
        sorted_pop = sorted(
            population,
            key=lambda x: x.get("fitness_score", 0.0),
            reverse=True,
        )
        elite_count = max(2, int(len(sorted_pop) * elite_ratio))
        elite = sorted_pop[:elite_count]

        self.logger.info(
            f"👑 Elite selected: {len(elite)}/{len(population)} "
            f"(best fitness={elite[0].get('fitness_score', 0):.4f})"
        )
        return elite

    # ─────────────────────────────────────────────────────────
    # 5. التزاوج (Crossover)
    # ─────────────────────────────────────────────────────────
    def crossover(
        self,
        parent_a: Dict[str, Any],
        parent_b: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        يُنتج طفلين من أبوين عبر تبادل الشروط.
        نقطة التقاطع: شروط الدخول والخروج.
        """
        child_a = copy.deepcopy(parent_a)
        child_b = copy.deepcopy(parent_b)

        # تبادل شروط الدخول بنسبة 50%
        if self._rng.random() > 0.5:
            child_a["entry_conditions"] = copy.deepcopy(parent_b["entry_conditions"])
            child_b["entry_conditions"] = copy.deepcopy(parent_a["entry_conditions"])

        # تبادل شروط الخروج بنسبة 50%
        if self._rng.random() > 0.5:
            child_a["exit_conditions"] = copy.deepcopy(parent_b["exit_conditions"])
            child_b["exit_conditions"] = copy.deepcopy(parent_a["exit_conditions"])

        # تبادل stoploss بنسبة 30%
        if self._rng.random() > 0.7:
            child_a["stoploss"], child_b["stoploss"] = (
                child_b["stoploss"], child_a["stoploss"]
            )

        # تسجيل الأبوين
        for child, pa, pb in [
            (child_a, parent_a, parent_b),
            (child_b, parent_b, parent_a),
        ]:
            child["parent_a_hash"] = pa.get("hash", "")
            child["parent_b_hash"] = pb.get("hash", "")
            child["mutation_count"] = 0
            child["name"] = f"GeneticStrategy_{self._short_id()}"
            child["hash"] = self.compute_hash(child)

        return child_a, child_b

    # ─────────────────────────────────────────────────────────
    # 6. الطفرة (Mutation)
    # ─────────────────────────────────────────────────────────
    def mutate(
        self,
        individual: Dict[str, Any],
        mutation_rate: float = 0.15,
    ) -> Dict[str, Any]:
        """
        يطبق طفرات عشوائية على فرد بنسبة `mutation_rate`.
        أنواع الطفرات:
          - تغيير معامل مؤشر موجود
          - استبدال شرط دخول/خروج بشرط جديد
          - تغيير stoploss أو roi
        """
        mutant = copy.deepcopy(individual)
        mutations_applied = 0

        # ── طفرة: تغيير معامل في شروط الدخول ──
        for cond in mutant.get("entry_conditions", []):
            if self._rng.random() < mutation_rate:
                self._mutate_condition(cond)
                mutations_applied += 1

        # ── طفرة: تغيير معامل في شروط الخروج ──
        for cond in mutant.get("exit_conditions", []):
            if self._rng.random() < mutation_rate:
                self._mutate_condition(cond)
                mutations_applied += 1

        # ── طفرة: تغيير stoploss ──
        if self._rng.random() < mutation_rate:
            obj_params = OBJECTIVE_PARAMS[mutant["profit_objective"]]
            mutant["stoploss"] = round(
                self._rng.uniform(*obj_params["stoploss_range"]), 4
            )
            mutations_applied += 1

        # ── طفرة: استبدال شرط دخول كامل ──
        if self._rng.random() < mutation_rate * 0.5:
            new_cond = self._sample_conditions("entry", 1)
            if new_cond:
                idx = self._rng.randrange(len(mutant["entry_conditions"]))
                mutant["entry_conditions"][idx] = new_cond[0]
                mutations_applied += 1

        mutant["mutation_count"] = individual.get("mutation_count", 0) + mutations_applied
        mutant["hash"] = self.compute_hash(mutant)

        if mutations_applied > 0:
            self.logger.debug(
                f"🔬 Mutated {mutations_applied} genes in {mutant['name']}"
            )
        return mutant

    def _mutate_condition(self, cond: Dict[str, Any]) -> None:
        """يغيّر معامل واحد عشوائياً في شرط موجود."""
        indicator_name = cond.get("indicator")
        if indicator_name not in BUILDING_BLOCKS:
            return
        block = BUILDING_BLOCKS[indicator_name]
        # اختر معامل عشوائي وغيّر قيمته
        param_names = list(block["params"].keys())
        if not param_names:
            return
        param_name = self._rng.choice(param_names)
        spec = block["params"][param_name]
        if isinstance(spec["min"], float):
            cond[param_name] = round(
                self._rng.uniform(spec["min"], spec["max"]), 2
            )
        else:
            cond[param_name] = self._rng.randrange(
                spec["min"], spec["max"] + 1, spec.get("step", 1)
            )

    # ─────────────────────────────────────────────────────────
    # 7. توليد جيل جديد من النخبة
    # ─────────────────────────────────────────────────────────
    def breed_next_generation(
        self,
        elite: List[Dict[str, Any]],
        target_size: int = 50,
        mutation_rate: float = 0.15,
    ) -> List[Dict[str, Any]]:
        """
        يولّد الجيل التالي من النخبة عبر:
          - الحفاظ على النخبة كما هي (elitism)
          - التزاوج العشوائي بين أزواج من النخبة
          - تطبيق طفرات على الأبناء
        """
        next_gen: List[Dict[str, Any]] = list(elite)  # الحفاظ على النخبة

        while len(next_gen) < target_size:
            # اختيار أبوين عشوائيين من النخبة
            pa, pb = self._rng.sample(elite, 2)
            child_a, child_b = self.crossover(pa, pb)

            # تطبيق طفرات
            child_a = self.mutate(child_a, mutation_rate)
            child_b = self.mutate(child_b, mutation_rate)

            next_gen.append(child_a)
            if len(next_gen) < target_size:
                next_gen.append(child_b)

        self.logger.info(
            f"🔄 Next generation ready: {len(next_gen)} individuals "
            f"({len(elite)} elite + {len(next_gen)-len(elite)} offspring)"
        )
        return next_gen[:target_size]

    # ─────────────────────────────────────────────────────────
    # 8. أدوات مساعدة
    # ─────────────────────────────────────────────────────────
    @staticmethod
    def compute_hash(dna: Dict[str, Any]) -> str:
        """
        يحسب SHA256 للشيفرة الجينية (بدون مفتاح hash نفسه).
        يُستخدم كـ unique identifier لمنع التكرار في DB.
        """
        dna_copy = {k: v for k, v in dna.items() if k != "hash"}
        canonical = json.dumps(dna_copy, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(canonical.encode()).hexdigest()

    def _short_id(self) -> str:
        """يولّد معرّف قصير عشوائي (6 أحرف hex)."""
        return "%06x" % self._rng.randint(0, 0xFFFFFF)

    def dna_to_json(self, dna: Dict[str, Any]) -> str:
        """تحويل الشيفرة إلى JSON منسّق."""
        return json.dumps(dna, indent=2, ensure_ascii=False)

    def validate_dna(self, dna: Dict[str, Any]) -> Tuple[bool, str]:
        """
        يتحقق من صحة الشيفرة الجينية.
        يُعيد (True, "") إذا كانت صحيحة، أو (False, reason) إذا كانت خاطئة.
        """
        required_keys = [
            "name", "symbol", "profit_objective", "risk_box",
            "timeframe", "stoploss", "roi",
            "entry_conditions", "exit_conditions",
        ]
        for key in required_keys:
            if key not in dna:
                return False, f"Missing required key: '{key}'"

        if dna["profit_objective"] not in PROFIT_OBJECTIVES:
            return False, f"Invalid profit_objective: '{dna['profit_objective']}'"

        if not dna["entry_conditions"]:
            return False, "entry_conditions cannot be empty"

        if dna["stoploss"] >= 0:
            return False, f"stoploss must be negative, got {dna['stoploss']}"

        return True, ""
