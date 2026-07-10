#!/usr/bin/env python3
"""ops 3064 -- CHART PRO institutional fusion (Khalid: 'make it like
finviz and tradingview... major major... institutional'). Audit found
lightweight-charts 4.2 + /ohlc worker + watchlists already live; the
true delta = the FLEET fused in (nobody else has this): Wyckoff dated
phase markers + range lines on the chart, insider-cluster + MA-event
markers, auto S/R pivot levels, Anchored VWAP (auto @ phase begin +
click anchors, persisted), engine badges in every watchlist row
(rank#/verdict/phase/DP/whale/resilience) + FUSION / S/R / AVWAP
toolbar. Verify: page markers live post-CDN, /ohlc healthy from the
runner, all 8 fusion feeds fresh on S3."""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3
from ops_report import report

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 ops-3064",
      "Cache-Control": "no-cache"}


def get(url):
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=30).read().decode(
        "utf-8", "replace")


def main():
    fails, warns = [], []
    with report("3064_chartpro_fusion") as rep:
        rep.section("1. Wait for pages deploy")
        ok = False
        for i in range(24):
            try:
                pg = get("https://justhodl.ai/chart-pro.html?cb=%d"
                         % time.time())
                if "JHF.chartApply" in pg:
                    ok = True
                    rep.kv(live_after_s=i * 20, page_kb=len(pg)//1024)
                    break
            except Exception:
                pass
            time.sleep(20)
        if not ok:
            fails.append("fusion module not live after 8min")
            _fin(rep, fails, warns)
            sys.exit(1)
        for marker in ("fusion-btn", "sr-btn", "avwap-btn",
                       "class JHF", "autoSR", "avwapSeries",
                       "JHF.badges(m.ticker)",
                       "JH FUSION markers"):
            if marker not in pg:
                fails.append("marker missing: %s" % marker)

        rep.section("2. /ohlc worker route (runner-side)")
        try:
            d = json.loads(get(
                "https://justhodl-data-proxy.raafouis.workers.dev/"
                "ohlc?ticker=AAPL&mult=1&span=day&days=400&cb=%d"
                % time.time()))
            nb = len(d.get("bars") or [])
            rep.kv(ohlc_bars=nb,
                   sample=json.dumps((d.get("bars") or [])[-1])[:120])
            if nb < 200:
                fails.append("/ohlc AAPL bars=%d (<200)" % nb)
            b = (d.get("bars") or [{}])[-1]
            for k in ("time", "close"):
                if k not in b:
                    fails.append("/ohlc bar missing %s" % k)
        except Exception as e:
            fails.append("/ohlc: %s" % str(e)[:100])

        rep.section("3. Fusion feed freshness (S3)")
        stale = []
        for key, max_h in (("data/phase-detector.json", 48),
                           ("data/whales.json", 200),
                           ("data/dark-pool.json", 48),
                           ("data/master-ranker.json", 48),
                           ("data/best-setups.json", 48),
                           ("data/resilience.json", 96),
                           ("data/industry-rotation.json", 48),
                           ("data/insider-radar.json", 60)):
            try:
                hd = S3.head_object(Bucket=BUCKET, Key=key)
                age = (datetime.now(timezone.utc)
                       - hd["LastModified"]).total_seconds() / 3600
                if age > max_h:
                    stale.append("%s %.0fh" % (key, age))
            except Exception as e:
                stale.append("%s MISSING %s" % (key, str(e)[:40]))
        rep.kv(stale_feeds=json.dumps(stale) if stale else "none")
        if stale:
            warns.append("stale/missing fusion feeds: %s" % stale)

        rep.section("verdict")
        _fin(rep, fails, warns)
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS -- Chart Pro fusion live")


def _fin(rep, fails, warns):
    (AWS_DIR / "ops" / "reports" / "3064.json").write_text(json.dumps(
        {"ops": 3064, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
