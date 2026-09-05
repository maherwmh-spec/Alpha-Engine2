"""Best-effort SahmK Pro news + fundamentals for /analyze.

Does not raise. Unknown endpoints are skipped.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

import requests
from loguru import logger

from config.config_manager import config
from scripts.analyze_service import AnalyzeService

_NEWS_PATHS = (
    "/news/?symbol={s}&limit=3",
    "/market/news/?symbol={s}&limit=3",
    "/stocks/{s}/news/?limit=3",
    "/announcements/?symbol={s}&limit=3",
)
_FUND_PATHS = (
    "/fundamentals/{s}/",
    "/ratios/{s}/",
    "/financials/{s}/",
    "/companies/{s}/",
    "/stocks/{s}/fundamentals/",
)

_INSTALLED = False


def _headers_base():
    key = (config.get_sahmk_api_key() or "").strip()
    if not key:
        return None, None
    base = (config.get_sahmk_base_url() or "https://api.sahmk.sa/api/v1").rstrip("/")
    return base, {"X-API-Key": key, "Accept": "application/json"}


def _try_get(path: str) -> Optional[Any]:
    base, headers = _headers_base()
    if not base:
        return None
    try:
        r = requests.get(base + path, headers=headers, timeout=12)
        if r.status_code != 200:
            return None
        return r.json() if r.content else None
    except Exception as exc:
        logger.debug(f"sahmk enrich {path}: {exc}")
        return None


def _first_item(payload: Any) -> Optional[Dict[str, Any]]:
    if payload is None:
        return None
    if isinstance(payload, list) and payload:
        return payload[0] if isinstance(payload[0], dict) else None
    if isinstance(payload, dict):
        for k in ("results", "data", "news", "items", "announcements"):
            v = payload.get(k)
            if isinstance(v, list) and v and isinstance(v[0], dict):
                return v[0]
        return payload
    return None


def fetch_latest_news(symbol: str) -> Dict[str, Any]:
    for tmpl in _NEWS_PATHS:
        payload = _try_get(tmpl.format(s=symbol))
        item = _first_item(payload)
        if not item:
            continue
        title = item.get("title") or item.get("headline") or item.get("subject")
        if title:
            return {
                "title": str(title).strip(),
                "date": item.get("date") or item.get("published_at") or item.get("time"),
            }
    return {}


def fetch_fundamentals(symbol: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for tmpl in _FUND_PATHS:
        payload = _try_get(tmpl.format(s=symbol))
        item = _first_item(payload) or (payload if isinstance(payload, dict) else None)
        if not item:
            continue
        mapping = {
            "pe": ("pe", "pe_ratio", "trailing_pe", "p_e"),
            "eps": ("eps", "earnings_per_share"),
            "pb": ("pb", "pb_ratio", "price_to_book"),
            "period": ("period", "fiscal_period", "report_date", "year"),
        }
        for dest, keys in mapping.items():
            if dest in out and out[dest] not in (None, ""):
                continue
            for k in keys:
                if item.get(k) not in (None, ""):
                    out[dest] = item.get(k)
                    break
        if out:
            break
    return out


def enrich(symbol: str, data: Dict[str, Any]) -> Dict[str, Any]:
    news = fetch_latest_news(symbol)
    if news.get("title"):
        data["news_headline"] = news["title"]
        data["news_at"] = news.get("date")
        if data.get("sentiment_status") in (None, "", "no_news"):
            data["sentiment_status"] = "has_news"
    fund = fetch_fundamentals(symbol)
    if fund:
        data.update({f"fund_{k}": v for k, v in fund.items()})
    return data


def install_analyze_enrich() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    orig_collect = AnalyzeService._collect
    orig_fmt = AnalyzeService._format_html

    def _collect(self, symbol: str):
        data = orig_collect(self, symbol)
        try:
            return enrich(symbol, data)
        except Exception as exc:
            logger.debug(f"analyze enrich skipped: {exc}")
            return data

    def _format_html(self, symbol: str, d: Dict[str, Any]) -> str:
        html = orig_fmt(self, symbol, d)
        extra = []
        headline = d.get("news_headline")
        if headline:
            extra.append(f"خبر: {str(headline)[:140]}")
        bits = []
        if d.get("fund_pe") is not None:
            bits.append(f"P/E {d.get('fund_pe')}")
        if d.get("fund_eps") is not None:
            bits.append(f"EPS {d.get('fund_eps')}")
        if d.get("fund_pb") is not None:
            bits.append(f"P/B {d.get('fund_pb')}")
        if d.get("fund_period") is not None:
            bits.append(str(d.get("fund_period")))
        if bits:
            extra.append("أساسيات: " + " · ".join(bits))
        if not extra:
            return html
        block = "\n".join(extra) + "\n"
        needle = "ليس أمراً تنفيذياً"
        if needle in html:
            return html.replace(needle, block + "\n" + needle, 1)
        return html + "\n" + block

    AnalyzeService._collect = _collect
    AnalyzeService._format_html = _format_html
    _INSTALLED = True
