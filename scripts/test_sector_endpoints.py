#!/usr/bin/env python3
"""
اختبار endpoints مختلفة لجلب بيانات القطاعات من Sahmk API
يُشغَّل مباشرة على الخادم لاكتشاف الـ URL الصحيح
"""
import os
import sys
import json
import requests

# ── قراءة API Key من البيئة ──
API_KEY = os.getenv('SAHMK_API_KEY', '')
if not API_KEY:
    # محاولة قراءة من config.yaml
    try:
        import yaml
        with open('/app/config/config.yaml') as f:
            cfg = yaml.safe_load(f)
        API_KEY = cfg.get('sahmk', {}).get('api_key', '')
    except Exception:
        pass

if not API_KEY:
    print("❌ SAHMK_API_KEY not found! Set it as env var or check config.yaml")
    sys.exit(1)

BASE = "https://app.sahmk.sa/api/v1"
HEADERS = {
    'X-API-Key': API_KEY,
    'Accept': 'application/json',
    'User-Agent': 'AlphaEngine2/1.0'
}

TEST_SYMBOLS = ['90001', '90010', '90022']
STOCK_SYMBOL = '2222'  # أرامكو للمقارنة

ENDPOINTS_TO_TEST = [
    "quote/{symbol}/",
    "quote/{symbol}",
    "market/quote/{symbol}/",
    "stocks/{symbol}/quote/",
    "indices/{symbol}/",
    "market/indices/{symbol}/",
    "sectors/{symbol}/",
    "market/sectors/{symbol}/",
    "market/index/{symbol}/",
    "index/{symbol}/",
    "historical/{symbol}/?interval=1m&limit=1",
]

def test_endpoint(endpoint_template, symbol):
    url = f"{BASE}/{endpoint_template.format(symbol=symbol).lstrip('/')}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        size = len(r.content)
        status = r.status_code
        preview = ""
        if status == 200 and size > 0:
            try:
                data = r.json()
                preview = json.dumps(data)[:200]
            except Exception:
                preview = r.text[:200]
        return status, size, preview
    except Exception as e:
        return 0, 0, str(e)

print(f"\n{'='*70}")
print(f"🔍 Testing Sahmk API endpoints for SECTORS")
print(f"API Key: {API_KEY[:12]}...{API_KEY[-4:]}")
print(f"{'='*70}\n")

# اختبار أرامكو أولاً كمرجع
print(f"📌 Reference: Stock 2222 (Aramco)")
for ep in ["quote/{symbol}/", "historical/{symbol}/?interval=1m&limit=1"]:
    status, size, preview = test_endpoint(ep, STOCK_SYMBOL)
    icon = "✅" if status == 200 else "❌"
    print(f"  {icon} [{status}] {ep.format(symbol=STOCK_SYMBOL)}")
    if status == 200:
        print(f"     → {preview[:150]}")

print()

# اختبار القطاعات
for sym in TEST_SYMBOLS:
    print(f"\n📊 Symbol: {sym}")
    for ep in ENDPOINTS_TO_TEST:
        status, size, preview = test_endpoint(ep, sym)
        icon = "✅" if status == 200 else ("⚠️" if status in [301, 302] else "❌")
        print(f"  {icon} [{status:3d}] {ep.format(symbol=sym)[:50]}")
        if status == 200 and preview:
            print(f"     → {preview[:150]}")

print(f"\n{'='*70}")
print("Done. Look for ✅ rows to find working endpoints.")
print(f"{'='*70}\n")
