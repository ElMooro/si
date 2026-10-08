#!/usr/bin/env python3
"""ops 3068 -- Chart Pro: TRUE full history (MAX routes through Yahoo
-- decades, not Polygon's 5y plan wall), universal Polygon->Yahoo
fallback so ANY search item charts (indices/foreign/anything Yahoo
knows), watchlist hidden-by-default slide-over (persisted, edge tab +
'[' hotkey, exactly the sidebar pattern -- root cause was a forced
setLeft(true) at boot). Worker yf-ohlc gains interval param."""
import json
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from ops_report import report

AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 ops-3068",
      "Cache-Control": "no-cache"}


def get(url):
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=40).read().decode(
        "utf-8", "replace")


def main():
    fails, warns = [], []
    with report("3068_fullhistory") as rep:
        rep.section("1. Page live")
        pg, ok = "", False
        for i in range(24):
            try:
                pg = get("https://justhodl.ai/chart-pro.html?cb=%d"
                         % time.time())
                if "jhYahooBars" in pg:
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
        for m in ("const JH_INDEX_MAP", "Yahoo full history",
                  "Yahoo fallback",
                  "localStorage.getItem('jh_wl_open')",
                  "localStorage.setItem('jh_wl_open'"):
            if m not in pg:
                fails.append("marker missing: %s" % m)
        if "setLeft(true);" in pg.replace(
                "window.__jhDrawers.setLeft(true)", ""):
            fails.append("boot still forces watchlist open")

        rep.section("2. Worker: Yahoo full history (decades)")
        deep = False
        for i in range(18):
            try:
                d = json.loads(get(
                    "https://justhodl-data-proxy.raafouis.workers."
                    "dev/yf-ohlc?symbol=AAPL&range=max&interval=1wk"
                    "&cb=%d" % time.time()))
                bars = d.get("bars") or []
                if len(bars) > 1500:
                    first = datetime.fromtimestamp(
                        bars[0]["time"], tz=timezone.utc
                    ).strftime("%Y-%m-%d")
                    rep.kv(max_weekly_bars=len(bars),
                           first_bar=first,
                           worker_live_after_s=i * 20)
                    if int(first[:4]) < 1995:
                        deep = True
                    break
            except Exception:
                pass
            time.sleep(20)
        if not deep:
            fails.append("AAPL range=max never reached pre-1995 "
                         "weekly history")

        rep.section("3. Index + fallback routes")
        try:
            y = json.loads(get(
                "https://justhodl-data-proxy.raafouis.workers.dev/"
                "yf-ohlc?symbol=" + urllib.parse.quote("^GSPC")
                + "&range=10y&interval=1wk&cb=%d" % time.time()))
            nyb = len(y.get("bars") or [])
            rep.kv(gspc_10y_weekly=nyb)
            if nyb < 400:
                fails.append("^GSPC 10y weekly bars=%d (<400)" % nyb)
        except Exception as e:
            fails.append("^GSPC: %s" % str(e)[:90])

        rep.section("verdict")
        _fin(rep, fails, warns)
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS")


def _fin(rep, fails, warns):
    (AWS_DIR / "ops" / "reports" / "3068.json").write_text(json.dumps(
        {"ops": 3068, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
