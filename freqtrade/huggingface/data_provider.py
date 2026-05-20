# -*- coding: utf-8 -*-
"""Dynamic data provider for Hugging Face features.

The provider deliberately avoids hard failures. It reads optional company profiles
from JSON, fetches headlines from configurable RSS feeds, caches all successful
responses, and falls back to conservative defaults when sources are unavailable.
"""
from __future__ import annotations

import json
import os
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Optional

from loguru import logger


DEFAULT_COMPANY_PROFILES: Dict[str, str] = {
    "1010": "Saudi National Bank, a leading Saudi financial institution offering retail, corporate, and investment banking services.",
    "2222": "Saudi Aramco, a major integrated energy and chemicals company operating across exploration, production, refining, and marketing.",
    "7010": "Saudi Telecom Company, a leading Saudi telecommunications and digital services provider.",
    "BTC": "Bitcoin, a decentralized digital asset and store-of-value cryptocurrency.",
    "ETH": "Ethereum, a decentralized smart-contract blockchain platform supporting DeFi and Web3 applications.",
}

DEFAULT_HEADLINES: Dict[str, List[str]] = {
    "1010": ["Saudi banking sector remains active as investors watch earnings, liquidity, and interest-rate conditions."],
    "2222": ["Energy investors monitor oil demand, downstream expansion, and global commodity prices."],
    "7010": ["Telecom investors monitor digital services growth, infrastructure spending, and regulatory updates."],
    "BTC": ["Digital asset markets respond to institutional flows, regulation, and macroeconomic liquidity."],
    "ETH": ["Ethereum ecosystem activity is influenced by network upgrades, DeFi usage, and transaction costs."],
}


class HFDataProvider:
    """Provides company descriptions and news headlines with local caching."""

    def __init__(self, cache_dir: Optional[str] = None, ttl_seconds: Optional[int] = None):
        self.cache_dir = Path(cache_dir or os.getenv("HF_CACHE_DIR", "/tmp/alpha_hf_cache"))
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.ttl_seconds = int(ttl_seconds or os.getenv("HF_CACHE_TTL_SECONDS", "21600"))
        self.timeout = int(os.getenv("HF_SOURCE_TIMEOUT_SECONDS", "8"))
        self.company_profile_path = Path(os.getenv("HF_COMPANY_PROFILE_PATH", self.cache_dir / "company_profiles.json"))
        self.news_rss_urls = [
            item.strip()
            for item in os.getenv("HF_NEWS_RSS_URLS", "").split(",")
            if item.strip()
        ]
        if not self.news_rss_urls:
            self.news_rss_urls = ["https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"]

    def _cache_path(self, name: str) -> Path:
        safe = "".join(ch if ch.isalnum() or ch in {"_", "-", "."} else "_" for ch in name)
        return self.cache_dir / safe

    def _read_json(self, path: Path, default):
        try:
            if path.exists():
                with path.open("r", encoding="utf-8") as handle:
                    return json.load(handle)
        except Exception as exc:
            logger.warning("hf_cache_read_failed path={} error={}", path, exc)
        return default

    def _write_json(self, path: Path, payload) -> None:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("w", encoding="utf-8") as handle:
                json.dump(payload, handle, ensure_ascii=False, indent=2)
        except Exception as exc:
            logger.warning("hf_cache_write_failed path={} error={}", path, exc)

    def get_company_profile(self, symbol: str, pair: Optional[str] = None) -> str:
        symbol = (symbol or "").upper()
        profiles = DEFAULT_COMPANY_PROFILES.copy()
        configured_profiles = self._read_json(self.company_profile_path, {})
        if isinstance(configured_profiles, dict):
            profiles.update({str(k).upper(): str(v) for k, v in configured_profiles.items() if v})
        fallback = f"Publicly traded instrument {pair or symbol}. Market behavior depends on fundamentals, liquidity, sector conditions, and news flow."
        profile = profiles.get(symbol, fallback)
        if profile == fallback:
            logger.info("hf_company_profile_fallback symbol={} pair={}", symbol, pair)
        return profile

    def _read_cached_headlines(self, symbol: str) -> Optional[List[str]]:
        path = self._cache_path(f"news_{symbol}.json")
        payload = self._read_json(path, None)
        if not payload:
            return None
        created_at = float(payload.get("created_at", 0))
        if time.time() - created_at > self.ttl_seconds:
            return None
        headlines = payload.get("headlines") or []
        return [str(item) for item in headlines if item]

    def _write_cached_headlines(self, symbol: str, headlines: List[str]) -> None:
        self._write_json(self._cache_path(f"news_{symbol}.json"), {"created_at": time.time(), "headlines": headlines})

    def fetch_news_headlines(self, symbol: str, company_profile: Optional[str] = None, limit: int = 8) -> List[str]:
        symbol = (symbol or "").upper()
        cached = self._read_cached_headlines(symbol)
        if cached:
            return cached[:limit]

        query = urllib.parse.quote_plus(f"{symbol} stock company news")
        headlines: List[str] = []
        for template in self.news_rss_urls:
            url = template.format(symbol=urllib.parse.quote_plus(symbol), query=query)
            try:
                request = urllib.request.Request(url, headers={"User-Agent": "Alpha-Engine2/production"})
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    xml_data = response.read()
                root = ET.fromstring(xml_data)
                for item in root.findall(".//item"):
                    title = item.findtext("title")
                    if title and title not in headlines:
                        headlines.append(title.strip())
                    if len(headlines) >= limit:
                        break
            except Exception as exc:
                logger.warning("hf_news_fetch_failed symbol={} url={} error={}", symbol, url, exc)
            if headlines:
                break

        if headlines:
            self._write_cached_headlines(symbol, headlines)
            return headlines[:limit]

        fallback = DEFAULT_HEADLINES.get(symbol)
        if not fallback:
            fallback = [f"No live news source available for {symbol}. Using neutral fallback context: {company_profile or symbol}."]
        logger.info("hf_news_fallback symbol={} count={}", symbol, len(fallback))
        return fallback[:limit]
