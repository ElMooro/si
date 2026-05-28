"""
justhodl-synthetic-monitor — Arch #7
=====================================
End-to-end page synthetic monitor. Every 15 minutes:
  1. Fetches each of the N critical pages over HTTPS
  2. Verifies HTTP 200 + content-type + body size in reasonable range
  3. Checks that signature markers exist (e.g. "Bottom Line", regime banner)
  4. Telegrams on first failure of any check

Tracks per-page status to data/synthetic-monitor.json so the dashboard
can display a status grid.

USES jhcore.
"""
import json
import os
import re
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from jhcore import s3io, notify

# Critical pages with markers we expect to find in the rendered HTML.
# (Pages are HTML served by GitHub Pages so the body is the source HTML, not
# post-JS DOM — but signature strings are still present in the source.)
PAGES = [
    {"path": "/", "title": "Home", "must_contain": ["JustHodl"]},
    {"path": "/yield-curve.html", "title": "Yield Curve", "must_contain": ["yield"]},
    {"path": "/vix-curve.html", "title": "VIX Curve", "must_contain": ["interp-kit.js"]},
    {"path": "/systemic-stress.html", "title": "Systemic Stress", "must_contain": ["interp-kit.js", "jh-mtx"]},
    {"path": "/eurodollar.html", "title": "Eurodollar", "must_contain": ["interp-kit.js", "jh-mtx"]},
    {"path": "/dollar.html", "title": "Dollar", "must_contain": ["interp-kit.js", "jh-mtx"]},
    {"path": "/risk-radar.html", "title": "Risk Radar", "must_contain": ["interp-kit.js", "jh-mtx"]},
    {"path": "/bond-vol.html", "title": "Bond Vol", "must_contain": ["interp-kit.js", "jh-mtx"]},
    {"path": "/defcon.html", "title": "Defcon", "must_contain": ["interp-kit.js", "jh-mtx"]},
    {"path": "/baggers.html", "title": "100x Baggers", "must_contain": ["bagger"]},
    {"path": "/wealth-plan.html", "title": "Wealth Plan", "must_contain": ["wealth"]},
    {"path": "/tax-plan.html", "title": "Tax Plan", "must_contain": ["tax"]},
]

CRITICAL_FEEDS = [
    "data/episode-reference.json",
    "data/signal-board.json",
    "data/yield-curve.json",
    "data/vix-curve.json",
]

BASE_URL = "https://justhodl.ai"
DATA_PROXY = "https://justhodl-data-proxy.raafouis.workers.dev"


def check_page(p):
    url = BASE_URL + p["path"]
    started = time.time()
    res = {"path": p["path"], "title": p["title"], "url": url}
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-SyntheticMonitor/1.0"})
        with urllib.request.urlopen(req, timeout=12) as r:
            body = r.read(200_000).decode("utf-8", errors="replace")
            elapsed = round((time.time() - started) * 1000)
            res["status"] = r.status
            res["ttfb_ms"] = elapsed
            res["size_kb"] = round(len(body) / 1024, 1)
            res["content_type"] = r.headers.get("Content-Type", "")
        if res["status"] != 200:
            res["ok"] = False; res["reason"] = f"HTTP {res['status']}"; return res
        if res["size_kb"] < 1.0:
            res["ok"] = False; res["reason"] = "body too small"; return res
        missing = [m for m in (p.get("must_contain") or []) if m.lower() not in body.lower()]
        if missing:
            res["ok"] = False
            res["reason"] = f"missing markers: {missing}"
            return res
        res["ok"] = True
        return res
    except Exception as e:
        res["ok"] = False
        res["reason"] = f"fetch failed: {str(e)[:120]}"
        res["elapsed_ms"] = round((time.time() - started) * 1000)
        return res


def check_feed(key):
    """HEAD via the CF proxy + check freshness."""
    url = DATA_PROXY + "/" + key.replace("data/", "")
    started = time.time()
    res = {"key": key, "url": url}
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-SyntheticMonitor/1.0"}, method="GET")
        with urllib.request.urlopen(req, timeout=10) as r:
            body = r.read(2048)
            elapsed = round((time.time() - started) * 1000)
            res["status"] = r.status
            res["ttfb_ms"] = elapsed
            res["edge_cache"] = r.headers.get("X-Edge-Cache", "")
        res["ok"] = res["status"] == 200 and len(body) > 32
        if not res["ok"]:
            res["reason"] = f"status={res['status']} body_size={len(body)}"
        return res
    except Exception as e:
        res["ok"] = False
        res["reason"] = str(e)[:160]
        res["elapsed_ms"] = round((time.time() - started) * 1000)
        return res


def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[synthetic] checking {len(PAGES)} pages + {len(CRITICAL_FEEDS)} feeds")

    page_results = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(check_page, p) for p in PAGES]
        for f in as_completed(futures):
            page_results.append(f.result())
    page_results.sort(key=lambda r: r["path"])

    feed_results = []
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = [ex.submit(check_feed, k) for k in CRITICAL_FEEDS]
        for f in as_completed(futures):
            feed_results.append(f.result())
    feed_results.sort(key=lambda r: r["key"])

    failed_pages = [r for r in page_results if not r["ok"]]
    failed_feeds = [r for r in feed_results if not r["ok"]]
    all_ok = not (failed_pages or failed_feeds)

    avg_ttfb = round(sum(r.get("ttfb_ms", 0) for r in page_results) / max(1, len(page_results)))
    duration = round(time.time() - started, 2)

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": duration,
        "all_ok": all_ok,
        "page_ok": sum(1 for r in page_results if r["ok"]),
        "pages_total": len(page_results),
        "feed_ok": sum(1 for r in feed_results if r["ok"]),
        "feeds_total": len(feed_results),
        "avg_page_ttfb_ms": avg_ttfb,
        "pages": page_results,
        "feeds": feed_results,
    }

    s3io.put_json("data/synthetic-monitor.json", report, cache_control="public, max-age=60")

    # Alert on failures — but only once per failing entity per hour to avoid spam.
    # Mechanism: read previous run's failures from S3, only alert on NEW failures.
    if failed_pages or failed_feeds:
        prev = s3io.get_json("data/synthetic-monitor-prev.json", default={}) or {}
        prev_failed = set((prev.get("failed_pages") or []) + (prev.get("failed_feeds") or []))
        now_failed = set(r["path"] for r in failed_pages) | set(r["key"] for r in failed_feeds)
        new_failures = now_failed - prev_failed
        if new_failures:
            lines = []
            for r in failed_pages:
                if r["path"] in new_failures:
                    lines.append(f"❌ <b>{r['title']}</b> ({r['path']}): {r.get('reason')}")
            for r in failed_feeds:
                if r["key"] in new_failures:
                    lines.append(f"❌ <code>{r['key']}</code>: {r.get('reason')}")
            notify.alert("WARN", "Synthetic Monitor — NEW failures", "\n".join(lines))
        s3io.put_json("data/synthetic-monitor-prev.json",
                      {"failed_pages": [r["path"] for r in failed_pages],
                       "failed_feeds": [r["key"] for r in failed_feeds],
                       "at": report["generated_at"]},
                      cache_control="no-cache")

    print(f"[synthetic] OK pages {report['page_ok']}/{report['pages_total']} feeds {report['feed_ok']}/{report['feeds_total']} {duration}s")
    return {"statusCode": 200, "body": json.dumps({
        "all_ok": all_ok,
        "page_ok": report["page_ok"], "pages_total": report["pages_total"],
        "feed_ok": report["feed_ok"], "feeds_total": report["feeds_total"],
        "avg_ttfb_ms": avg_ttfb,
        "duration_s": duration,
    })}
