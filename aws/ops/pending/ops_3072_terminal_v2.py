#!/usr/bin/env python3
"""ops 3072 -- Chart Pro TERMINAL V2 ('improve as much as you can'):
(1) VOLUME PROFILE -- visible-range volume-at-price canvas overlay,
POC line + 70% value area, redraws on pan/zoom/resize;
(2) LOG SCALE toggle (essential on the new 46y MAX views);
(3) MEASURE tool -- two clicks -> dP%, bars, days + drawn ray;
(4) PNG EXPORT -- lightweight-charts takeScreenshot + ticker/date/
justhodl.ai watermark;
(5) RS vs SPY overlay -- rebased ratio line on hidden scale, SPY bars
cached per timeframe, Polygon->Yahoo fallback;
(6) ON-CHART STATS STRIP -- 52w off-high %, ATR14 %, RVOL x + fusion
chips (rank/verdict/phase/DP/whale) rendered inside the chart, plus
52w high/low reference lines on daily views.
All client-side, real data only. Static-marker verification."""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from ops_report import report

AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 ops-3072",
      "Cache-Control": "no-cache"}


def get(url):
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=30).read().decode(
        "utf-8", "replace")


def main():
    fails, warns = [], []
    with report("3072_terminal_v2") as rep:
        rep.section("1. Page live")
        pg, ok = "", False
        for i in range(24):
            try:
                pg = get("https://justhodl.ai/chart-pro.html?cb=%d"
                         % time.time())
                if "volumeProfile" in pg:
                    ok = True
                    rep.kv(live_after_s=i * 20,
                           page_kb=len(pg) // 1024)
                    break
            except Exception:
                pass
            time.sleep(20)
        if not ok:
            fails.append("V2 not live after 8min")
            _fin(rep, fails, warns)
            sys.exit(1)
        for m in ('id="vp-btn"', 'id="log-btn"', 'id="rs-btn"',
                  'id="measure-btn"', 'id="shot-btn"',
                  "static volumeProfile", "static rsOverlay",
                  "static measureClick", "static screenshot",
                  "static statsStrip", "takeScreenshot",
                  "subscribeVisibleTimeRangeChange",
                  "coordinateToPrice", "POC ",
                  "52w high", "RS vs SPY",
                  "priceScaleId: 'jhf-rs'"):
            if m not in pg:
                fails.append("marker missing: %s" % m)
        # structural: no duplicate ids
        for bid in ("vp-btn", "log-btn", "rs-btn", "measure-btn",
                    "shot-btn"):
            if pg.count('id="%s"' % bid) != 1:
                fails.append("id %s count=%d" %
                             (bid, pg.count('id="%s"' % bid)))

        rep.section("verdict")
        _fin(rep, fails, warns)
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS -- Terminal V2 live")


def _fin(rep, fails, warns):
    (AWS_DIR / "ops" / "reports" / "3072.json").write_text(json.dumps(
        {"ops": 3072, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
