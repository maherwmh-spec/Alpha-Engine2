#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/explorer.py
===================
سكربت استكشافي لمعرفة أقدم تاريخ متاح لبيانات Sahmk API.

الهدف:
  - اكتشاف أقدم تاريخ يمكن جلبه من Sahmk API لكل إطار زمني.
  - معرفة كم شمعة يمكن الحصول عليها فعلياً.
  - طباعة معلومات تشخيصية مفصلة جداً بدون حفظ أي شيء في DB.

المميزات:
  - مستقل تماماً: لا يحتاج Redis أو قاعدة بيانات.
  - يقرأ API Key من: env var → config.yaml.
  - يدعم command line arguments كاملة.
  - يطبع Raw JSON Response كاملاً.
  - يدعم اختبار عدة أطر زمنية في جولة واحدة.

الاستخدام:
  python scripts/explorer.py --symbol 2222
  python scripts/explorer.py --symbol 2222 --timeframes 1m 5m 15m 1d
  python scripts/explorer.py --symbol 1120 --start-date 2018-01-01 --timeframes 1d
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from loguru import logger

# ── Bootstrap: إضافة مسار المشروع لـ sys.path ────────────────────────────────
# هذا يسمح بالاستيراد من config/ و scripts/ بغض النظر عن مكان تشغيل السكربت
_PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))


# ═══════════════════════════════════════════════════════════════════════════════
# إعداد الـ Logger
# ═══════════════════════════════════════════════════════════════════════════════

def _setup_logger() -> None:
    """إعداد loguru بشكل مستقل بدون الاعتماد على scripts.logger (يتطلب Redis)."""
    logger.remove()
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level:<8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | <level>{message}</level>",
        level="DEBUG",
        colorize=True,
        backtrace=True,
        diagnose=True,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# قراءة API Key
# ═══════════════════════════════════════════════════════════════════════════════

def _load_api_key() -> str:
    """
    قراءة Sahmk API Key بالأولوية التالية:
      1. متغير البيئة SAHMK_API_KEY
      2. config/config.yaml
    """
    # الأولوية 1: متغير البيئة
    key = os.getenv("SAHMK_API_KEY", "").strip()
    if key and key != "YOUR_SAHMK_API_KEY_HERE":
        logger.debug(f"✅ API Key loaded from environment variable: {key[:12]}...{key[-4:]}")
        return key

    # الأولوية 2: config.yaml
    config_path = _PROJECT_ROOT / "config" / "config.yaml"
    if config_path.exists():
        try:
            import yaml
            with open(config_path, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f) or {}
            key = cfg.get("sahmk", {}).get("api_key", "").strip()
            if key and key != "YOUR_SAHMK_API_KEY_HERE":
                logger.debug(f"✅ API Key loaded from config.yaml: {key[:12]}...{key[-4:]}")
                return key
        except Exception as e:
            logger.warning(f"⚠️ Could not read config.yaml: {e}")

    logger.error("❌ SAHMK_API_KEY not found! Set it as env var or in config/config.yaml")
    sys.exit(1)


# ═══════════════════════════════════════════════════════════════════════════════
# Sahmk API Client المستقل (بدون Redis أو DB)
# ═══════════════════════════════════════════════════════════════════════════════

class StandaloneExplorer:
    """
    عميل Sahmk API مستقل تماماً للاستكشاف فقط.
    لا يستخدم Redis أو قاعدة بيانات أو أي تبعية خارجية.
    """

    BASE_URL = "https://app.sahmk.sa/api/v1"
    SUPPORTED_TIMEFRAMES = ["1m", "5m", "15m", "30m", "1h", "1d"]

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            "X-API-Key": self.api_key,
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "AlphaEngine2-Explorer/1.0",
        })
        logger.success(
            f"✅ StandaloneExplorer initialized | "
            f"API Key: {self.api_key[:12]}...{self.api_key[-4:]}"
        )

    def fetch_raw(
        self,
        symbol: str,
        timeframe: str,
        start_date: datetime,
        end_date: Optional[datetime] = None,
        limit: int = 50000,
    ) -> Dict[str, Any]:
        """
        جلب البيانات التاريخية الخام من Sahmk API وإرجاعها كـ dict.

        Args:
            symbol:     رمز السهم (مثال: 2222)
            timeframe:  الإطار الزمني (1m, 5m, 15m, 30m, 1h, 1d)
            start_date: تاريخ البداية
            end_date:   تاريخ النهاية (الافتراضي: الآن)
            limit:      الحد الأقصى للشموع

        Returns:
            dict يحتوي على:
              - raw_response: الاستجابة الخام من الـ API
              - http_status:  كود HTTP
              - requested_from: التاريخ المطلوب
              - requested_to:   تاريخ النهاية المطلوب
              - elapsed_ms:     وقت الطلب بالميلي ثانية
              - error:          رسالة الخطأ إن وجدت
        """
        if end_date is None:
            end_date = datetime.now()

        params = {
            "interval": timeframe,
            "limit": limit,
            "from": start_date.strftime("%Y-%m-%d"),
            "to": end_date.strftime("%Y-%m-%d"),
        }

        url = f"{self.BASE_URL}/historical/{symbol}/"
        logger.debug(f"🌐 GET {url} | params={params}")

        result: Dict[str, Any] = {
            "raw_response": None,
            "http_status": None,
            "requested_from": start_date.strftime("%Y-%m-%d"),
            "requested_to": end_date.strftime("%Y-%m-%d"),
            "elapsed_ms": None,
            "error": None,
        }

        try:
            t0 = time.time()
            response = self.session.get(url, params=params, timeout=60)
            elapsed = (time.time() - t0) * 1000

            result["http_status"] = response.status_code
            result["elapsed_ms"] = round(elapsed, 2)

            if response.status_code == 200:
                result["raw_response"] = response.json()
            elif response.status_code == 401:
                result["error"] = "Unauthorized — تحقق من صحة API Key"
            elif response.status_code == 404:
                result["error"] = f"Endpoint not found: {url}"
            elif response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 60))
                result["error"] = f"Rate limited — انتظر {retry_after} ثانية"
            else:
                result["error"] = f"HTTP {response.status_code}: {response.text[:200]}"

        except requests.exceptions.Timeout:
            result["error"] = "Request timed out (60s)"
        except requests.exceptions.ConnectionError as e:
            result["error"] = f"Connection error: {e}"
        except Exception as e:
            result["error"] = f"Unexpected error: {e}"

        return result


# ═══════════════════════════════════════════════════════════════════════════════
# دوال التحليل والطباعة
# ═══════════════════════════════════════════════════════════════════════════════

def _parse_candles(raw_response: Any) -> List[Dict]:
    """
    استخراج قائمة الشموع من الاستجابة الخام بغض النظر عن هيكلها.
    يدعم الهياكل: list, dict['results'], dict['data'], dict['candles'], dict['ohlcv']
    """
    if raw_response is None:
        return []

    if isinstance(raw_response, list):
        return raw_response

    if isinstance(raw_response, dict):
        for key in ("results", "data", "candles", "ohlcv", "items"):
            if key in raw_response and isinstance(raw_response[key], list):
                return raw_response[key]

    return []


def _parse_timestamp(ts_value: Any) -> Optional[str]:
    """
    تحويل قيمة الـ timestamp إلى string مقروء.
    يدعم: Unix timestamp (int/float), ISO string, أي صيغة أخرى.
    """
    if ts_value is None:
        return None
    try:
        if isinstance(ts_value, (int, float)):
            return datetime.fromtimestamp(ts_value).strftime("%Y-%m-%d %H:%M:%S")
        return str(ts_value)
    except Exception:
        return str(ts_value)


def _normalize_candle(candle: Dict) -> Dict:
    """تطبيع أسماء أعمدة الشمعة إلى الصيغة الموحدة."""
    mapping = {
        "t": "timestamp", "time": "timestamp", "date": "timestamp",
        "o": "open",       "Open": "open",
        "h": "high",       "High": "high",
        "l": "low",        "Low": "low",
        "c": "close",      "Close": "close",
        "v": "volume",     "Volume": "volume",
    }
    normalized = {}
    for k, v in candle.items():
        normalized[mapping.get(k, k)] = v
    return normalized


def _print_separator(char: str = "═", width: int = 80) -> None:
    print(char * width)


def _print_timeframe_result(
    symbol: str,
    timeframe: str,
    fetch_result: Dict[str, Any],
) -> None:
    """
    طباعة نتيجة استكشاف إطار زمني واحد بشكل مفصل ومنظم.
    """
    _print_separator("─")
    print(f"  ⏱️  الإطار الزمني: {timeframe}")
    print(f"  📅  التاريخ المطلوب (from): {fetch_result['requested_from']}")
    print(f"  📅  التاريخ المطلوب (to):   {fetch_result['requested_to']}")
    print(f"  🌐  HTTP Status:             {fetch_result['http_status']}")
    print(f"  ⚡  وقت الطلب:              {fetch_result['elapsed_ms']} ms")

    # حالة الخطأ
    if fetch_result["error"]:
        print(f"  ❌  خطأ: {fetch_result['error']}")
        return

    raw = fetch_result["raw_response"]

    # طباعة هيكل الـ JSON الخام (Raw Response)
    print()
    print("  📦  هيكل الـ JSON الخام (Raw Response):")
    print("  " + "─" * 60)
    if isinstance(raw, dict):
        # طباعة المفاتيح الرئيسية فقط (بدون البيانات الضخمة)
        summary = {k: (f"[{len(v)} items]" if isinstance(v, list) else v)
                   for k, v in raw.items() if k not in ("results", "data", "candles", "ohlcv", "items")}
        print(f"  مفاتيح الاستجابة: {list(raw.keys())}")
        if summary:
            print(f"  البيانات الوصفية: {json.dumps(summary, ensure_ascii=False, indent=4)}")
    elif isinstance(raw, list):
        print(f"  الاستجابة: قائمة مباشرة بـ {len(raw)} عنصر")

    # استخراج الشموع
    candles = _parse_candles(raw)
    num_candles = len(candles)

    print()
    print(f"  📊  عدد الشموع المُرجعة: {num_candles}")

    if num_candles == 0:
        print("  ⚠️  لم يتم إرجاع أي شموع.")
        return

    # تطبيع أول وآخر شمعة
    oldest_raw = candles[0]
    newest_raw = candles[-1]
    oldest = _normalize_candle(oldest_raw)
    newest = _normalize_candle(newest_raw)

    oldest_ts = _parse_timestamp(oldest.get("timestamp"))
    newest_ts = _parse_timestamp(newest.get("timestamp"))

    print(f"  📌  التاريخ الفعلي الذي أرجعه الـ API (أقدم شمعة): {oldest_ts}")
    print(f"  📌  التاريخ الفعلي الذي أرجعه الـ API (أحدث شمعة): {newest_ts}")
    print()

    # طباعة أقدم شمعة
    print("  🕯️  أقدم شمعة (Oldest Candle):")
    print(f"       Timestamp : {oldest_ts}")
    print(f"       Open      : {oldest.get('open', 'N/A')}")
    print(f"       High      : {oldest.get('high', 'N/A')}")
    print(f"       Low       : {oldest.get('low', 'N/A')}")
    print(f"       Close     : {oldest.get('close', 'N/A')}")
    print(f"       Volume    : {oldest.get('volume', 'N/A')}")
    print(f"       Raw JSON  : {json.dumps(oldest_raw, ensure_ascii=False)}")

    print()

    # طباعة أحدث شمعة
    print("  🕯️  أحدث شمعة (Newest Candle):")
    print(f"       Timestamp : {newest_ts}")
    print(f"       Open      : {newest.get('open', 'N/A')}")
    print(f"       High      : {newest.get('high', 'N/A')}")
    print(f"       Low       : {newest.get('low', 'N/A')}")
    print(f"       Close     : {newest.get('close', 'N/A')}")
    print(f"       Volume    : {newest.get('volume', 'N/A')}")
    print(f"       Raw JSON  : {json.dumps(newest_raw, ensure_ascii=False)}")

    # طباعة عينة من أول 3 شموع خام
    print()
    print(f"  📋  عينة أول 3 شموع (Raw JSON):")
    sample = candles[:3]
    print(json.dumps(sample, ensure_ascii=False, indent=6))


# ═══════════════════════════════════════════════════════════════════════════════
# الدالة الرئيسية للاستكشاف
# ═══════════════════════════════════════════════════════════════════════════════

def explore(
    symbol: str,
    timeframes: List[str],
    start_date_str: str,
    limit: int = 50000,
) -> None:
    """
    استكشاف البيانات التاريخية لرمز معين وعدة أطر زمنية.

    Args:
        symbol:         رمز السهم
        timeframes:     قائمة الأطر الزمنية
        start_date_str: تاريخ البداية بصيغة YYYY-MM-DD
        limit:          الحد الأقصى للشموع في كل طلب
    """
    # التحقق من صحة التاريخ
    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    except ValueError:
        logger.error(f"❌ صيغة التاريخ غير صحيحة: '{start_date_str}'. استخدم YYYY-MM-DD")
        sys.exit(1)

    # تحميل API Key
    api_key = _load_api_key()

    # إنشاء المستكشف
    explorer = StandaloneExplorer(api_key=api_key)

    # طباعة رأس التقرير
    _print_separator("═")
    print(f"  🔍  Alpha-Engine2 — Sahmk API Historical Data Explorer")
    _print_separator("═")
    print(f"  📌  الرمز:              {symbol}")
    print(f"  📅  تاريخ البداية:      {start_date_str}")
    print(f"  📅  تاريخ النهاية:      {datetime.now().strftime('%Y-%m-%d')}")
    print(f"  ⏱️  الأطر الزمنية:      {', '.join(timeframes)}")
    print(f"  🔢  الحد الأقصى للشموع: {limit}")
    _print_separator("═")

    # استكشاف كل إطار زمني
    for tf in timeframes:
        print(f"\n  ┌─ جاري استكشاف الإطار الزمني: [{tf}] ─────────────────────────────────")
        logger.info(f"🔄 Fetching {symbol} | {tf} | from={start_date_str}")

        result = explorer.fetch_raw(
            symbol=symbol,
            timeframe=tf,
            start_date=start_date,
            end_date=datetime.now(),
            limit=limit,
        )

        _print_timeframe_result(symbol=symbol, timeframe=tf, fetch_result=result)
        print()

        # تأخير بسيط بين الطلبات لتجنب Rate Limiting
        if tf != timeframes[-1]:
            time.sleep(0.5)

    # طباعة ملخص نهائي
    _print_separator("═")
    print(f"  ✅  انتهى الاستكشاف للرمز: {symbol}")
    _print_separator("═")


# ═══════════════════════════════════════════════════════════════════════════════
# CLI Entry Point
# ═══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    """نقطة الدخول الرئيسية للسكربت."""
    _setup_logger()

    parser = argparse.ArgumentParser(
        prog="explorer.py",
        description="Alpha-Engine2 — Sahmk API Historical Data Explorer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
أمثلة للاستخدام:
  # استكشاف أرامكو بجميع الأطر الزمنية من 2018
  python scripts/explorer.py --symbol 2222

  # استكشاف رمز معين بأطر زمنية محددة
  python scripts/explorer.py --symbol 1120 --timeframes 1d 1h 15m

  # استكشاف من تاريخ محدد
  python scripts/explorer.py --symbol 2222 --start-date 2020-01-01 --timeframes 1m 5m 15m 1d

  # استكشاف مع حد أقصى مخصص للشموع
  python scripts/explorer.py --symbol 2222 --timeframes 1d --limit 10000

  # استخدام API Key مباشرة
  SAHMK_API_KEY=your_key python scripts/explorer.py --symbol 2222
        """,
    )

    parser.add_argument(
        "--symbol", "-s",
        type=str,
        required=True,
        metavar="SYMBOL",
        help="رمز السهم المراد استكشافه (مثال: 2222 لأرامكو)",
    )
    parser.add_argument(
        "--timeframes", "-t",
        nargs="+",
        default=["1m", "5m", "15m", "1d"],
        choices=StandaloneExplorer.SUPPORTED_TIMEFRAMES,
        metavar="TF",
        help=(
            f"الأطر الزمنية المراد فحصها. "
            f"القيم المتاحة: {', '.join(StandaloneExplorer.SUPPORTED_TIMEFRAMES)}. "
            f"الافتراضي: 1m 5m 15m 1d"
        ),
    )
    parser.add_argument(
        "--start-date", "-d",
        type=str,
        default="2018-01-01",
        metavar="YYYY-MM-DD",
        help="تاريخ البداية للبحث (الافتراضي: 2018-01-01)",
    )
    parser.add_argument(
        "--limit", "-l",
        type=int,
        default=50000,
        metavar="N",
        help="الحد الأقصى لعدد الشموع في كل طلب (الافتراضي: 50000)",
    )

    args = parser.parse_args()

    try:
        explore(
            symbol=args.symbol,
            timeframes=args.timeframes,
            start_date_str=args.start_date,
            limit=args.limit,
        )
    except KeyboardInterrupt:
        print("\n")
        logger.info("🛑 تم إيقاف الاستكشاف بواسطة المستخدم.")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"💥 خطأ غير متوقع: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
