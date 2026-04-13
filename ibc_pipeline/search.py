"""Lightweight Brave search helpers for enrichment context."""

from __future__ import annotations

import json
import os
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def has_brave_api_key() -> bool:
    return bool(os.getenv("BRAVE_API_KEY", "").strip())


def brave_search(query: str, count: int = 3, timeout_seconds: int = 15) -> list[dict[str, str]]:
    api_key = os.getenv("BRAVE_API_KEY", "").strip()
    if not api_key or not query.strip():
        return []

    url = "https://api.search.brave.com/res/v1/web/search?" + urlencode(
        {
            "q": query,
            "count": max(1, min(count, 5)),
            "search_lang": "en",
            "safesearch": "moderate",
        }
    )
    req = Request(
        url,
        headers={
            "Accept": "application/json",
            "X-Subscription-Token": api_key,
            "User-Agent": "ibc-dataset-builder/1.0",
        },
        method="GET",
    )

    with urlopen(req, timeout=timeout_seconds) as response:
        payload = json.loads(response.read().decode("utf-8"))

    results: list[dict[str, str]] = []
    for item in (payload.get("web", {}) or {}).get("results", [])[:count]:
        results.append(
            {
                "title": str(item.get("title") or "").strip(),
                "description": str(item.get("description") or "").strip(),
                "url": str(item.get("url") or "").strip(),
            }
        )
    return results


def format_search_context(results: list[dict[str, str]]) -> str:
    if not results:
        return ""

    lines: list[str] = []
    for idx, item in enumerate(results, start=1):
        title = (item.get("title") or "").strip()
        desc = (item.get("description") or "").strip()
        url = (item.get("url") or "").strip()
        lines.append(f"{idx}. {title} | {desc} | {url}")
    return "\n".join(lines)
