#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/check_market_status.py
================================
أداة سريعة للتحقق من حالة سوق TASI الحالية.

الاستخدام:
    python scripts/check_market_status.py
"""

import sys
import os
from datetime import timedelta

# إضافة مسار المشروع
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.utils import get_saudi_time, is_trading_hours

# ── نسخة مستقلة من is_market_open لأغراض التحقق فقط ──────────────────────────
TRADING_DAYS_PYTHON = {6, 0, 1, 2, 3}  # Sun(6)–Thu(3)

def is_market_open():
    """تحقق من حالة سوق TASI (09:30–15:30 الأحد–الخميس)."""
    now = get_saudi_time()
    if now.weekday() not in TRADING_DAYS_PYTHON:
        return False
    market_open  = now.replace(hour=9,  minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return market_open <= now <= market_close

def seconds_until_market_open():
    """ثواني حتى فتح السوق القادم."""
    now = get_saudi_time()
    today_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    if now.weekday() in TRADING_DAYS_PYTHON and now < today_open:
        return max(1, int((today_open - now).total_seconds()))
    candidate = now + timedelta(days=1)
    for _ in range(7):
        candidate_open = candidate.replace(hour=9, minute=30, second=0, microsecond=0)
        if candidate.weekday() in TRADING_DAYS_PYTHON:
            return max(1, int((candidate_open - now).total_seconds()))
        candidate += timedelta(days=1)
    return 3600


def main():
    now = get_saudi_time()

    # ── معلومات الوقت الحالي ──────────────────────────────────────────────
    weekday_ar = {
        0: "الاثنين",
        1: "الثلاثاء",
        2: "الأربعاء",
        3: "الخميس",
        4: "الجمعة",
        5: "السبت",
        6: "الأحد",
    }
    day_name = weekday_ar.get(now.weekday(), now.strftime('%A'))

    print("=" * 55)
    print("   فحص حالة سوق TASI (السوق السعودي)")
    print("=" * 55)
    print(f"  الوقت الحالي (AST) : {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  اليوم              : {day_name} (weekday={now.weekday()})")
    print("-" * 55)

    # ── حالة السوق ───────────────────────────────────────────────────────
    open_status = is_market_open()

    if open_status:
        # تحديد المرحلة
        continuous_start = now.replace(hour=10, minute=0, second=0, microsecond=0)
        closing_auction  = now.replace(hour=15, minute=0, second=0, microsecond=0)
        market_close     = now.replace(hour=15, minute=30, second=0, microsecond=0)

        if now < continuous_start:
            phase = "المزاد الافتتاحي (09:30 – 10:00)"
        elif now < closing_auction:
            phase = "التداول المستمر  (10:00 – 15:00)"
        else:
            phase = "المزاد الختامي   (15:00 – 15:30)"

        remaining = int((market_close - now).total_seconds() // 60)
        print(f"  الحالة             : ✅ السوق مفتوح")
        print(f"  المرحلة            : {phase}")
        print(f"  يُغلق بعد          : {remaining} دقيقة")
    else:
        wait_secs = seconds_until_market_open()
        wait_hrs  = wait_secs // 3600
        wait_min  = (wait_secs % 3600) // 60
        print(f"  الحالة             : ❌ السوق مغلق")
        print(f"  يفتح بعد           : {wait_hrs} ساعة و {wait_min} دقيقة")
        print(f"  الإجراء التلقائي   : تشغيل historical_sync.py في الخلفية")

    print("-" * 55)
    print("  جدول TASI الرسمي:")
    print("    أيام التداول  : الأحد – الخميس")
    print("    مزاد افتتاحي  : 09:30")
    print("    تداول مستمر   : 10:00 – 15:00")
    print("    مزاد ختامي    : 15:00 – 15:30")
    print("=" * 55)

    # is_trading_hours من utils للمقارنة
    utils_result = is_trading_hours()
    print(f"\n  [utils.is_trading_hours()] → {utils_result}")
    print(f"  [bot.is_market_open()]     → {open_status}")

    return 0 if open_status else 1


if __name__ == "__main__":
    sys.exit(main())
