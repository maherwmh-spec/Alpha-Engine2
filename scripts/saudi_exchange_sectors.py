#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Saudi Exchange Sector Data Fetcher
يجلب بيانات القطاعات من موقع البورصة السعودية الرسمية (بدون API key)
ويحسب مؤشر كل قطاع من أسهمه المكوّنة (متوسط مرجّح بالحجم)

Endpoint: https://www.saudiexchange.sa/wps/portal/saudiexchange/ourmarkets/
          main-market-watch/!ut/p/z1/.../NJgetMainNomucMarketDetails=/
          ?sectorParameter=&tableViewParameter=1&iswatchListSelected=NO&requestLocale=en
"""

import requests
import json
from datetime import datetime
from typing import Dict, List, Optional
from loguru import logger

# ── الـ URL الثابت لبيانات السوق الكاملة ──────────────────────────────────────
SAUDI_EXCHANGE_MARKET_URL = (
    "https://www.saudiexchange.sa/wps/portal/saudiexchange/ourmarkets/"
    "main-market-watch/!ut/p/z1/"
    "jZDdCoJAEIWfpQdYZthwtUsrskwzi8j2JpYyW8pV3C2op8-6CJL-5mp-vsPhDHBIgCtxlpkwslDiWM8rztaWy5AOHYycfr-H8WDsDH2MKDIblq8AzqdWDUzDdoAz9JAB_0ePH8rFX3q_AYQew3jixhG1LcQ5vSegVdgLM-ClMHsi1a6AJBfVITVEn_K6uxChtkSbOrQ2cqObZ0h0ujFFpZ-LJfBfvg3gzWMewJfkZb5IrkG3M5Ju6wYyxQMg/"
    "p0/IZ7_IPG41I82KGASC06S67RB9A0080=CZ6_5A602H80O8DDC0QFK8HJ0O2067="
    "NJgetMainNomucMarketDetails=/"
    "?sectorParameter=&tableViewParameter=1&iswatchListSelected=NO&requestLocale=en"
)

# ── ربط أسماء القطاعات بالرموز 90xxx ──────────────────────────────────────────
# يُستخدم لتحديد رمز القطاع المقابل لكل sectorName في البيانات
SECTOR_NAME_TO_SYMBOL: Dict[str, str] = {
    'Energy':                                       '90017',
    'Materials':                                    '90022',
    'Capital Goods':                                '90011',
    'Commercial & Professional Svc':               '90012',
    'Commercial and Professional Svc':             '90012',
    'Consumer Discretionary Distribution & Retail':'90013',
    'Consumer Durables & Apparel':                 '90014',
    'Consumer Durables and Apparel':               '90014',
    'Consumer Staples Distribution & Retail':      '90015',
    'Consumer Svc':                                '90016',
    'Consumer svc':                                '90016',
    'Financial Services':                          '90018',
    'Food & Beverages':                            '90019',
    'Food and Beverages':                          '90019',
    'Health Care Equipment & Svc':                 '90020',
    'Health Care Equipment and Svc':               '90020',
    'Insurance':                                   '90021',
    'Media & Entertainment':                       '90023',
    'Media and Entertainment':                     '90023',
    'Pharma, Biotech & Life Science':              '90024',
    'Pharma, Biotech and Life Science':            '90024',
    'REITs':                                       '90025',
    'Real Estate Mgmt & Dev':                      '90026',
    'Real Estate Mgmt and Dev':                    '90026',
    'Software & Svc':                              '90027',
    'Software and Svc':                            '90027',
    'Telecommunication Svc':                       '90028',
    'Transportation':                              '90029',
    'Utilities':                                   '90030',
    'Banks':                                       '90010',
}

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/122.0.0.0 Safari/537.36'
    ),
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'ar-SA,ar;q=0.9,en-US;q=0.8,en;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br',
    'Referer': 'https://www.saudiexchange.sa/wps/portal/saudiexchange/ourmarkets/market-summary-and-statistics/market-summary/sectors-summary',
    'X-Requested-With': 'XMLHttpRequest',
    'Connection': 'keep-alive',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
}

# ── Session مشترك لإعادة استخدام cookies ──────────────────────────────────────
_SESSION: Optional[requests.Session] = None


def _get_session() -> requests.Session:
    """يُنشئ session مع cookies من زيارة الصفحة الرئيسية أولاً"""
    global _SESSION
    if _SESSION is not None:
        return _SESSION

    _SESSION = requests.Session()
    _SESSION.headers.update(HEADERS)

    # زيارة الصفحة الرئيسية أولاً للحصول على cookies
    try:
        _SESSION.get(
            'https://www.saudiexchange.sa/wps/portal/saudiexchange/ourmarkets/'
            'market-summary-and-statistics/market-summary/sectors-summary',
            timeout=15
        )
        logger.debug("✅ Saudi Exchange session initialized with cookies")
    except Exception as e:
        logger.debug(f"⚠️ Session init warning: {e}")

    return _SESSION


def fetch_all_stocks_data(timeout: int = 15) -> Optional[List[Dict]]:
    """
    يجلب بيانات جميع أسهم السوق الرئيسية من Saudi Exchange.
    يعيد قائمة من dicts، كل dict يحتوي على:
      companySymbol, sectorName, lastTradePrice, todayOpen,
      highPrice, lowPrice, volumeTraded, turnover, netChange, precentChange
    """
    try:
        session = _get_session()
        resp = session.get(SAUDI_EXCHANGE_MARKET_URL, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
        stocks = data.get('data', [])
        logger.debug(f"Saudi Exchange: fetched {len(stocks)} stock records")
        return stocks
    except Exception as e:
        logger.error(f"❌ Saudi Exchange fetch error: {e}")
        return None


def compute_sector_snapshots(stocks: List[Dict]) -> Dict[str, Dict]:
    """
    يحسب بيانات لحظية لكل قطاع من أسهمه المكوّنة.
    المنطق: متوسط مرجّح بحجم التداول (volume-weighted average price)
    يعيد dict: symbol -> snapshot_dict
    """
    from collections import defaultdict

    # تجميع الأسهم حسب القطاع
    sector_stocks: Dict[str, List[Dict]] = defaultdict(list)
    for s in stocks:
        sector_name = s.get('sectorName', '')
        if sector_name:
            sector_stocks[sector_name].append(s)

    snapshots = {}
    now = datetime.now()

    for sector_name, members in sector_stocks.items():
        symbol = SECTOR_NAME_TO_SYMBOL.get(sector_name)
        if not symbol:
            logger.debug(f"⚠️ Unknown sector name: '{sector_name}' — skipping")
            continue

        # حساب VWAP (متوسط مرجّح بالحجم)
        total_turnover = sum(float(s.get('turnover', 0) or 0) for s in members)
        total_volume   = sum(float(s.get('volumeTraded', 0) or 0) for s in members)

        if total_volume <= 0:
            # إذا لا يوجد حجم، استخدم المتوسط البسيط
            prices = [float(s.get('lastTradePrice', 0) or 0) for s in members if s.get('lastTradePrice')]
            if not prices:
                continue
            vwap = sum(prices) / len(prices)
        else:
            vwap = total_turnover / total_volume if total_volume > 0 else 0

        opens  = [float(s.get('todayOpen', 0) or 0) for s in members if s.get('todayOpen')]
        highs  = [float(s.get('highPrice', 0) or 0) for s in members if s.get('highPrice')]
        lows   = [float(s.get('lowPrice', 0) or 0) for s in members if s.get('lowPrice')]
        closes = [float(s.get('lastTradePrice', 0) or 0) for s in members if s.get('lastTradePrice')]

        if not closes:
            continue

        snapshots[symbol] = {
            'symbol':    symbol,
            'price':     round(vwap, 4) if vwap > 0 else (sum(closes) / len(closes)),
            'open':      round(sum(opens) / len(opens), 4) if opens else vwap,
            'high':      round(max(highs), 4) if highs else vwap,
            'low':       round(min(lows), 4) if lows else vwap,
            'close':     round(sum(closes) / len(closes), 4),
            'volume':    int(total_volume),
            'turnover':  round(total_turnover, 2),
            'members':   len(members),
            'timestamp': now,
            'source':    'saudi_exchange_computed',
        }

    return snapshots


def get_tasi_snapshot(timeout: int = 15) -> Optional[Dict]:
    """
    يجلب بيانات المؤشر العام (TASI) من ThemeTASIUtilityServlet
    """
    try:
        url = "https://www.saudiexchange.sa/tadawul.eportal.theme.helper/ThemeTASIUtilityServlet"
        session = _get_session()
        resp = session.get(url, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()

        tasi_bean = data.get('tasiBean', {}).get('tasiTodaysSummaryBean', {})
        now = datetime.now()

        price = float(data.get('tasiValue', '0').replace(',', '') or 0)
        if price <= 0:
            price = float(tasi_bean.get('indexPrice', 0) or 0)

        if price <= 0:
            return None

        return {
            'symbol':    '90001',
            'price':     price,
            'open':      float(tasi_bean.get('openPrice', price) or price),
            'high':      float(tasi_bean.get('highPrice', price) or price),
            'low':       float(tasi_bean.get('lowPrice', price) or price),
            'close':     price,
            'volume':    int(float(tasi_bean.get('volumeTraded', 0) or 0)),
            'turnover':  float(tasi_bean.get('turnOver', 0) or 0),
            'timestamp': now,
            'source':    'saudi_exchange_tasi',
        }
    except Exception as e:
        logger.error(f"❌ TASI snapshot error: {e}")
        return None


def get_all_sector_snapshots() -> Dict[str, Dict]:
    """
    الدالة الرئيسية: تجلب بيانات كل القطاعات + المؤشر العام
    يعيد dict: symbol -> snapshot_dict
    """
    result = {}

    # 1. المؤشر العام (TASI)
    tasi = get_tasi_snapshot()
    if tasi:
        result['90001'] = tasi
        logger.debug(f"✅ TASI: {tasi['price']}")

    # 2. القطاعات من بيانات الأسهم
    stocks = fetch_all_stocks_data()
    if stocks:
        sector_snaps = compute_sector_snapshots(stocks)
        result.update(sector_snaps)
        logger.info(f"✅ Computed {len(sector_snaps)} sector snapshots from {len(stocks)} stocks")

    return result


# ── اختبار مباشر ──────────────────────────────────────────────────────────────
if __name__ == '__main__':
    import sys
    from loguru import logger as log

    log.remove()
    log.add(sys.stdout, level="DEBUG")

    print("\n" + "="*60)
    print("🔍 Testing Saudi Exchange Sector Data Fetcher")
    print("="*60 + "\n")

    snapshots = get_all_sector_snapshots()

    if not snapshots:
        print("❌ No snapshots retrieved!")
        sys.exit(1)

    print(f"\n✅ Retrieved {len(snapshots)} sector/index snapshots:\n")
    for sym, snap in sorted(snapshots.items()):
        print(f"  {sym}: price={snap['price']:.2f}  open={snap['open']:.2f}  "
              f"high={snap['high']:.2f}  low={snap['low']:.2f}  "
              f"vol={snap['volume']:,}  members={snap.get('members', 'N/A')}")

    print(f"\n{'='*60}\n")
