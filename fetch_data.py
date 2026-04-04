
#!/usr/bin/env python3
"""
BetAnalytics Pro X — professional fetch pipeline for GitHub Pages.
- paginare completă pentru predictions și live
- retry și fallback controlat
- sortează evenimentele după dată
- salvează meta extinsă pentru UI
"""

import os
import json
import time
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

TOKEN = os.environ.get("BSD_TOKEN", "").strip()
API_BASE = "https://sports.bzzoiro.com"
TZ = "Europe/Bucharest"
HEADERS = {"Authorization": f"Token {TOKEN}"} if TOKEN else {}
TIMEOUT = 40
MAX_RETRIES = 3

FALLBACK_BASE = "https://balty1991.github.io/BetAnalyticsProV2.1/data"


def log(msg: str) -> None:
    print(msg, flush=True)


def fetch_json(url: str, headers: Optional[Dict[str, str]] = None) -> Optional[Any]:
    headers = headers or {}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, headers=headers, timeout=TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            log(f"[warn] {url} attempt {attempt}/{MAX_RETRIES} failed: {exc}")
            if attempt < MAX_RETRIES:
                time.sleep(1.2 * attempt)
    return None


def fetch_all_pages(endpoint: str) -> List[Dict[str, Any]]:
    url = f"{API_BASE}{endpoint}"
    results: List[Dict[str, Any]] = []
    page = 0

    while url:
        page += 1
        log(f"[fetch] {endpoint} page {page}")
        payload = fetch_json(url, HEADERS)
        if payload is None:
            break

        if isinstance(payload, list):
            results.extend(payload)
            break

        page_items = payload.get("results", [])
        if isinstance(page_items, list):
            results.extend(page_items)

        next_url = payload.get("next")
        if next_url and next_url.startswith("http://"):
            next_url = next_url.replace("http://", "https://", 1)
        url = next_url

    return results


def fetch_fallback(filename: str) -> List[Dict[str, Any]]:
    url = f"{FALLBACK_BASE}/{filename}?t={int(time.time())}"
    payload = fetch_json(url)
    if payload is None:
        return []
    if isinstance(payload, list):
        return payload
    return payload.get("results", [])


def sort_by_date(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def key(item: Dict[str, Any]) -> str:
        event = item.get("event", item)
        return str(event.get("event_date", ""))

    return sorted(items, key=key)


def build_meta(predictions: List[Dict[str, Any]], live: List[Dict[str, Any]]) -> Dict[str, Any]:
    leagues = Counter()
    countries = Counter()
    dates = []

    for row in predictions:
        event = row.get("event", {})
        league = event.get("league", {})
        if league.get("name"):
            leagues[league["name"]] += 1
        if league.get("country"):
            countries[league["country"]] += 1
        if event.get("event_date"):
            dates.append(event["event_date"])

    coverage_start = min(dates) if dates else None
    coverage_end = max(dates) if dates else None

    return {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "status": "ok" if predictions else "fallback_or_empty",
        "source": "bsd_api" if TOKEN else "fallback",
        "predictions_count": len(predictions),
        "live_count": len(live),
        "coverage_start": coverage_start,
        "coverage_end": coverage_end,
        "league_count": len(leagues),
        "country_count": len(countries),
        "top_leagues": leagues.most_common(10),
        "top_countries": countries.most_common(10),
    }


def save_json(data: Any, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, separators=(",", ":"))
    log(f"[save] {path}")


def main() -> None:
    log(f"=== BetAnalytics Pro X fetch {datetime.now(timezone.utc).isoformat()} ===")
    predictions: List[Dict[str, Any]] = []
    live: List[Dict[str, Any]] = []

    if TOKEN:
        predictions = fetch_all_pages(f"/api/predictions/?tz={TZ}")
        live = fetch_all_pages("/api/live/")
    else:
        log("[warn] BSD_TOKEN missing, using fallback only")

    if not predictions:
        log("[fallback] predictions.json")
        predictions = fetch_fallback("predictions.json")

    if not live:
        log("[fallback] live.json")
        live = fetch_fallback("live.json")

    predictions = sort_by_date(predictions)
    live = sort_by_date(live)

    meta = build_meta(predictions, live)

    save_json(predictions, "data/predictions.json")
    save_json(live, "data/live.json")
    save_json(meta, "data/meta.json")

    log(f"[done] predictions={len(predictions)} live={len(live)}")


if __name__ == "__main__":
    main()
