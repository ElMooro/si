#!/usr/bin/env python3
"""ops 3067 (retry 2) -- Chart Pro: conviction drawer removed (Alerts button
relocated to toolbar), full-history ranges (5Y daily + MAX weekly,
worker days cap 3650->12000), index symbols from the search bar render
via the Yahoo route (Polygon indices gated). Verifies page + worker
live from the runner."""
import json
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from ops_report import report

AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 ops-3067",
      "Cache-Control": "no-cache"}


def get(url):
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=30).read().decode(
        "utf-8", "replace")


def main():
    fails, warns = [], []
    with report("3067_chartpro_cleanup") as rep:
        rep.section("1. Page live (post-CDN)")
        pg, ok = "", False
        for i in range(24):
            try:
                pg = get("https://justhodl.ai/chart-pro.html?cb=%d"
                         % time.time())
                if "INDEX_MAP" in pg:
                    ok = True
                    rep.kv(live_after_s=i * 20)
                    break
            except Exception:
                pass
            time.sleep(20)
        if not ok:
            fails.append("page not live after 8min")
            _fin(rep, fails, warns)
            sys.exit(1)
        if "Highest-Conviction" in pg:
            fails.append("conviction drawer still present")
        for marker in ('data-days="1825">5Y', 'data-days="9999"',
                       "INDEX_MAP", 'id="alert-builder-btn"'):
            if marker not in pg:
                fails.append("marker missing: %s" % marker)
        if 'id="setups-drawer"' in pg:
            fails.append("setups-drawer markup still present")

        rep.section("2. Worker: full-history cap live")
        deep = False
        for i in range(18):                        # worker deploy wait
            try:
                d = json.loads(get(
                    "https://justhodl-data-proxy.raafouis.workers."
                    "dev/ohlc?ticker=AAPL&mult=1&span=day&days=9999"
                    "&cb=%d" % time.time()))
                nb = len(d.get("bars") or [])
                if nb > 1100:                      # >~4.5y daily
                    deep = True
                    first = (d["bars"][0].get("time") or 0)
                    rep.kv(max_bars=nb,
                           first_bar=datetime.fromtimestamp(
                               first, tz=timezone.utc
                           ).strftime("%Y-%m-%d"),
                           worker_live_after_s=i * 20)
                    break
            except Exception:
                pass
            time.sleep(20)
        if not deep:
            fails.append("/ohlc days=9999 daily bars never exceeded "
                         "1100 (plan-max ~1250; cap not live?)")

        rep.section("3. Index route (Yahoo path)")
        try:
            y = json.loads(get(
                "https://justhodl-data-proxy.raafouis.workers.dev/"
                "yf-ohlc?symbol=%%5EGSPC&range=1y&cb=%d"
                % time.time()))
            nyb = len(y.get("bars") or [])
            rep.kv(gspc_bars=nyb)
            if nyb < 100:
                fails.append("yf-ohlc ^GSPC bars=%d (<100)" % nyb)
        except Exception as e:
            fails.append("yf-ohlc ^GSPC: %s" % str(e)[:90])

        rep.section("verdict")
        _fin(rep, fails, warns)
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS")


def _fin(rep, fails, warns):
    (AWS_DIR / "ops" / "reports" / "3067.json").write_text(json.dumps(
        {"ops": 3067, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
