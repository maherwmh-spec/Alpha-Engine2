"""
Saudi Exchange Scraper  —  scripts/saudi_exchange_scraper.py
=============================================================
مصدر بيانات بديل للقطاعات والمؤشر العام (TASI).

Sahmk API يعطي 404 لكل رموز 900xx، لذا نجلب البيانات من:
  1. موقع البورصة السعودية الرسمية (saudiexchange.sa) — بدون API key
  2. نحسب بيانات كل قطاع من أسهمه المكوّنة (VWAP)
  3. نحوّل الناتج إلى شموع OHLCV جاهزة للحفظ في TimescaleDB

الدوال المُصدَّرة:
  - get_tasi_candle()          → Dict  (شمعة TASI اللحظية)
  - get_sector_candles()       → Dict[symbol, Dict]  (شمعة لكل قطاع)
  - get_all_sector_candles()   → Dict[symbol, Dict]  (TASI + كل القطاعات)
  - is_sector_symbol(symbol)   → bool
  - SECTOR_DISPLAY_NAMES       → Dict[symbol, str]
"""

import time
import threading
from datetime import datetime
from typing import Dict, List, Optional

import requests
from loguru import logger


# ─────────────────────────────────────────────────────────────────────────────
# الثوابت
# ─────────────────────────────────────────────────────────────────────────────

# أسماء القطاعات العربية/الإنجليزية — تُستخدم كعمود name في DB
SECTOR_DISPLAY_NAMES: Dict[str, str] = {
    '90001': 'TASI - المؤشر العام',
    '90010': 'Banks - البنوك',
    '90011': 'Capital Goods - السلع الرأسمالية',
    '90012': 'Commercial & Professional Svc - الخدمات التجارية',
    '90013': 'Consumer Discretionary - السلع الاستهلاكية التقديرية',
    '90014': 'Consumer Durables & Apparel - السلع المعمّرة والملابس',
    '90015': 'Consumer Staples - السلع الاستهلاكية الأساسية',
    '90016': 'Consumer Svc - خدمات المستهلك',
    '90017': 'Energy - الطاقة',
    '90018': 'Financial Services - الخدمات المالية',
    '90019': 'Food & Beverages - الأغذية والمشروبات',
    '90020': 'Health Care - الرعاية الصحية',
    '90021': 'Insurance - التأمين',
    '90022': 'Materials - المواد الأساسية',
    '90023': 'Media & Entertainment - الإعلام والترفيه',
    '90024': 'Pharma & Biotech - الأدوية والتقنية الحيوية',
    '90025': 'REITs - صناديق الاستثمار العقاري',
    '90026': 'Real Estate - إدارة وتطوير العقارات',
    '90027': 'Software & Svc - البرمجيات والخدمات',
    '90028': 'Telecom - خدمات الاتصالات',
    '90029': 'Transportation - النقل',
    '90030': 'Utilities - المرافق',
}

ALL_SECTOR_SYMBOLS = set(SECTOR_DISPLAY_NAMES.keys())

# ربط أسماء القطاعات كما تظهر في Saudi Exchange API بالرموز 900xx
_SECTOR_NAME_MAP: Dict[str, str] = {
    'Energy':                                        '90017',
    'Materials':                                     '90022',
    'Capital Goods':                                 '90011',
    'Commercial & Professional Svc':                '90012',
    'Commercial and Professional Svc':              '90012',
    'Consumer Discretionary Distribution & Retail': '90013',
    'Consumer Durables & Apparel':                  '90014',
    'Consumer Durables and Apparel':                '90014',
    'Consumer Staples Distribution & Retail':       '90015',
    'Consumer Svc':                                 '90016',
    'Consumer svc':                                 '90016',
    'Financial Services':                           '90018',
    'Food & Beverages':                             '90019',
    'Food and Beverages':                           '90019',
    'Health Care Equipment & Svc':                  '90020',
    'Health Care Equipment and Svc':                '90020',
    'Insurance':                                    '90021',
    'Media & Entertainment':                        '90023',
    'Media and Entertainment':                      '90023',
    'Pharma, Biotech & Life Science':               '90024',
    'Pharma, Biotech and Life Science':             '90024',
    'REITs':                                        '90025',
    'Real Estate Mgmt & Dev':                       '90026',
    'Real Estate Mgmt and Dev':                     '90026',
    'Software & Svc':                               '90027',
    'Software and Svc':                             '90027',
    'Telecommunication Svc':                        '90028',
    'Transportation':                               '90029',
    'Utilities':                                    '90030',
    'Banks':                                        '90010',
}

# ─────────────────────────────────────────────────────────────────────────────
# HTTP Session مع cookie warm-up
# ─────────────────────────────────────────────────────────────────────────────

_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/122.0.0.0 Safari/537.36'
    ),
    'Accept':          'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'ar-SA,ar;q=0.9,en-US;q=0.8,en;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br',
    'X-Requested-With': 'XMLHttpRequest',
    'Connection':      'keep-alive',
    'Referer': (
        'https://www.saudiexchange.sa/wps/portal/saudiexchange/'
        'ourmarkets/market-summary-and-statistics/market-summary/sectors-summary'
    ),
}

_MARKET_DATA_URL = (
    'https://www.saudiexchange.sa/wps/portal/saudiexchange/ourmarkets/'
    'main-market-watch/!ut/p/z1/'
    'jZDdCoJAEIWfpQdYZthwtUsrskwzi8j2JpYyW8pV3C2op8-6CJL-5mp-vsPhDHBIgCtxlpkwslDiWM8rztaWy5AOHYycfr-H8WDsDH2MKDIblq8AzqdWDUzDdoAz9JAB_0ePH8rFX3q_AYQew3jixhG1LcQ5vSegVdgLM-ClMHsi1a6AJBfVITVEn_K6uxChtkSbOrQ2cqObZ0h0ujFFpZ-LJfBfvg3gzWMewJfkZb5IrkG3M5Ju6wYyxQMg/'
    'p0/IZ7_IPG41I82KGASC06S67RB9A0080=CZ6_5A602H80O8DDC0QFK8HJ0O2067='
    'NJgetMainNomucMarketDetails=/'
    '?sectorParameter=&tableViewParameter=1&iswatchListSelected=NO&requestLocale=en'
)

_TASI_URL = (
    'https://www.saudiexchange.sa/'
    'tadawul.eportal.theme.helper/ThemeTASIUtilityServlet'
)

_WARMUP_URL = (
    'https://www.saudiexchange.sa/wps/portal/saudiexchange/ourmarkets/'
    'market-summary-and-statistics/market-summary/sectors-summary'
)

_session_lock = threading.Lock()
_http_session: Optional[requests.Session] = None
_session_warmed_at: float = 0.0
_SESSION_TTL = 1800  # أعد تهيئة الـ session كل 30 دقيقة


def _get_session() -> requests.Session:
    """
    يُعيد session HTTP مُهيَّأ مع cookies من الصفحة الرئيسية.
    يُعاد تهيئته تلقائياً كل 30 دقيقة.
    """
    global _http_session, _session_warmed_at

    with _session_lock:
        now = time.time()
        if _http_session is None or (now - _session_warmed_at) > _SESSION_TTL:
            sess = requests.Session()
            sess.headers.update(_HEADERS)
            try:
                sess.get(_WARMUP_URL, timeout=10)
                logger.debug('✅ Saudi Exchange session warmed up with cookies')
            except Exception as e:
                logger.debug(f'⚠️ Session warm-up warning: {e}')
            _http_session = sess
            _session_warmed_at = now

    return _http_session


# ─────────────────────────────────────────────────────────────────────────────
# دوال الجلب الداخلية
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_all_stocks(timeout: int = 15) -> Optional[List[Dict]]:
    """
    يجلب بيانات جميع أسهم السوق الرئيسية من Saudi Exchange.
    يعيد قائمة من dicts تحتوي على:
      companySymbol, sectorName, lastTradePrice, todayOpen,
      highPrice, lowPrice, volumeTraded, turnover, previousClosePrice
    """
    try:
        resp = _get_session().get(_MARKET_DATA_URL, timeout=timeout)
        resp.raise_for_status()
        stocks = resp.json().get('data', [])
        logger.debug(f'Saudi Exchange: fetched {len(stocks)} stock records')
        return stocks
    except Exception as e:
        logger.error(f'❌ Saudi Exchange stocks fetch error: {e}')
        return None


def _fetch_tasi_raw(timeout: int = 10) -> Optional[Dict]:
    """
    يجلب بيانات TASI الخام من ThemeTASIUtilityServlet.
    يعيد dict خام أو None عند الفشل.
    """
    try:
        resp = _get_session().get(_TASI_URL, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f'❌ TASI fetch error: {e}')
        return None


# ─────────────────────────────────────────────────────────────────────────────
# بناء الشمعة
# ─────────────────────────────────────────────────────────────────────────────

def _build_candle(
    symbol: str,
    price: float,
    open_: float,
    high: float,
    low: float,
    close: float,
    volume: int,
    source: str,
    ts: Optional[datetime] = None,
) -> Dict:
    """
    يبني dict شمعة OHLCV جاهزة للحفظ في TimescaleDB.
    إذا كانت open/high/low غير متاحة، تُستخدم قيمة close كبديل تقريبي.
    """
    if ts is None:
        ts = datetime.now()
    # تقريب الثواني لأقرب دقيقة
    ts = ts.replace(second=0, microsecond=0)

    # تصحيح: تأكد من أن high >= close >= low
    actual_high  = max(high,  close, open_)
    actual_low   = min(low,   close, open_)
    actual_open  = open_  if open_  > 0 else close
    actual_close = close  if close  > 0 else price

    return {
        'symbol':    symbol,
        'name':      SECTOR_DISPLAY_NAMES.get(symbol, symbol),
        'timestamp': ts,
        'open':      round(actual_open,  4),
        'high':      round(actual_high,  4),
        'low':       round(actual_low,   4),
        'close':     round(actual_close, 4),
        'volume':    max(0, int(volume)),
        'source':    source,
    }


# ─────────────────────────────────────────────────────────────────────────────
# الدوال العامة المُصدَّرة
# ─────────────────────────────────────────────────────────────────────────────

def is_sector_symbol(symbol: str) -> bool:
    """يعيد True إذا كان الرمز قطاعاً أو مؤشراً (90001-90030)."""
    return symbol in ALL_SECTOR_SYMBOLS


def get_tasi_candle() -> Optional[Dict]:
    """
    يجلب شمعة TASI اللحظية من ThemeTASIUtilityServlet.

    Returns:
        dict شمعة OHLCV للمؤشر العام، أو None عند الفشل.
    """
    raw = _fetch_tasi_raw()
    if not raw:
        return None

    try:
        # قيمة TASI الحالية
        price_str = raw.get('tasiValue', '0')
        price = float(str(price_str).replace(',', '') or 0)

        # بيانات اليوم من tasiBean
        bean = raw.get('tasiBean', {}).get('tasiTodaysSummaryBean', {})

        def _f(key: str, default: float = price) -> float:
            val = bean.get(key)
            try:
                return float(str(val).replace(',', '')) if val else default
            except (ValueError, TypeError):
                return default

        open_  = _f('openPrice')
        high   = _f('highPrice')
        low    = _f('lowPrice')
        volume = int(_f('volumeTraded', 0))

        if price <= 0:
            logger.warning('⚠️ TASI price is 0 or missing')
            return None

        candle = _build_candle(
            symbol='90001',
            price=price,
            open_=open_,
            high=high,
            low=low,
            close=price,
            volume=volume,
            source='saudi_exchange_tasi',
        )
        logger.debug(f"TASI candle: {candle['close']:.2f} (O={candle['open']:.2f})")
        return candle

    except Exception as e:
        logger.error(f'❌ TASI candle build error: {e}')
        return None


def get_sector_candles() -> Dict[str, Dict]:
    """
    يجلب شمعة لحظية لكل قطاع من أسهمه المكوّنة عبر VWAP.

    المنطق:
      - يجلب بيانات كل أسهم السوق دفعة واحدة
      - يجمّع الأسهم حسب sectorName
      - يحسب VWAP (turnover / volume) لكل قطاع
      - يأخذ max(highPrice) و min(lowPrice) و avg(todayOpen)

    Returns:
        Dict[symbol → candle_dict] للقطاعات التي وُجدت بياناتها.
    """
    stocks = _fetch_all_stocks()
    if not stocks:
        return {}

    from collections import defaultdict
    sector_groups: Dict[str, List[Dict]] = defaultdict(list)
    for s in stocks:
        name = s.get('sectorName', '')
        if name:
            sector_groups[name].append(s)

    now = datetime.now()
    result: Dict[str, Dict] = {}

    for sector_name, members in sector_groups.items():
        symbol = _SECTOR_NAME_MAP.get(sector_name)
        if not symbol:
            logger.debug(f"⚠️ Unknown sector: '{sector_name}' — skipped")
            continue

        def _fv(s: Dict, key: str) -> float:
            try:
                return float(s.get(key) or 0)
            except (ValueError, TypeError):
                return 0.0

        total_turnover = sum(_fv(s, 'turnover')     for s in members)
        total_volume   = sum(_fv(s, 'volumeTraded') for s in members)

        closes = [_fv(s, 'lastTradePrice') for s in members if _fv(s, 'lastTradePrice') > 0]
        opens  = [_fv(s, 'todayOpen')      for s in members if _fv(s, 'todayOpen')      > 0]
        highs  = [_fv(s, 'highPrice')      for s in members if _fv(s, 'highPrice')      > 0]
        lows   = [_fv(s, 'lowPrice')       for s in members if _fv(s, 'lowPrice')       > 0]
        prevs  = [_fv(s, 'previousClosePrice') for s in members if _fv(s, 'previousClosePrice') > 0]

        if not closes:
            continue

        avg_close = sum(closes) / len(closes)

        # VWAP كسعر إغلاق إذا توفّر حجم، وإلا متوسط بسيط
        vwap = (total_turnover / total_volume) if total_volume > 0 else avg_close

        # open: متوسط أسعار الافتتاح، أو متوسط الإغلاق السابق إذا لم يتوفر
        open_price = (sum(opens) / len(opens)) if opens else (
            (sum(prevs) / len(prevs)) if prevs else vwap
        )

        candle = _build_candle(
            symbol=symbol,
            price=vwap,
            open_=open_price,
            high=max(highs) if highs else vwap,
            low=min(lows)   if lows  else vwap,
            close=vwap,
            volume=int(total_volume),
            source='saudi_exchange_vwap',
            ts=now,
        )
        candle['members_count'] = len(members)
        result[symbol] = candle

    logger.info(f'✅ Computed {len(result)} sector candles from {len(stocks)} stocks')
    return result


def get_all_sector_candles() -> Dict[str, Dict]:
    """
    الدالة الرئيسية: تجلب شمعة TASI + شمعة لكل قطاع.

    Returns:
        Dict[symbol → candle_dict] يشمل 90001 + القطاعات المتاحة.
    """
    result: Dict[str, Dict] = {}

    # 1. المؤشر العام
    tasi = get_tasi_candle()
    if tasi:
        result['90001'] = tasi
        logger.debug(f"TASI: {tasi['close']:.2f}")
    else:
        logger.warning('⚠️ TASI candle not available')

    # 2. القطاعات
    sectors = get_sector_candles()
    result.update(sectors)

    total = len(result)
    logger.info(
        f'📊 get_all_sector_candles: {total} total '
        f'(TASI={"✅" if "90001" in result else "❌"}, '
        f'sectors={len(sectors)})'
    )
    return result


# ─────────────────────────────────────────────────────────────────────────────
# اختبار مباشر
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    import sys
    from loguru import logger as log

    log.remove()
    log.add(sys.stdout, level='DEBUG',
            format='<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}')

    print('\n' + '=' * 65)
    print('🔍  Saudi Exchange Scraper — Self-Test')
    print('=' * 65 + '\n')

    candles = get_all_sector_candles()

    if not candles:
        print('❌  No candles retrieved — check network / Cloudflare')
        sys.exit(1)

    print(f'\n✅  Retrieved {len(candles)} candles:\n')
    header = f"{'Symbol':<8} {'Name':<40} {'Close':>10} {'Open':>10} {'High':>10} {'Low':>10} {'Volume':>14}"
    print(header)
    print('-' * len(header))
    for sym, c in sorted(candles.items()):
        name = SECTOR_DISPLAY_NAMES.get(sym, sym)[:38]
        print(
            f"{sym:<8} {name:<40} {c['close']:>10.2f} {c['open']:>10.2f} "
            f"{c['high']:>10.2f} {c['low']:>10.2f} {c['volume']:>14,}"
        )

    print(f'\n{"=" * 65}\n')
