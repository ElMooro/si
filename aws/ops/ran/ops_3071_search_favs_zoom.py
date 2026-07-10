#!/usr/bin/env python3
"""ops 3071 -- Chart Pro search+zoom upgrade (Khalid x4):
(1) search shows ONLY chart-reachable symbols -- jhChartable() gates
both search surfaces (US stocks/ETFs on Polygon+Yahoo, mapped indices,
6-letter forex; futures/CFDs/foreign/unmapped dropped);
(2) ★ favorites on every result row (modal manages, both surfaces
float favorites FIRST, persisted jh_metric_favs, works for FRED
metrics too);
(3) auto-suggest -- empty search instantly shows ★ Favorites + ⏱
Recent (from knownSymbols history);
(4) +/- interval stepper on the TF row (finer/coarser through
5m..MAX) + ⌂ fit-all, with +/-/0 hotkeys; chart handles stashed per
pane. Static-marker + parse verification live."""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from ops_report import report

AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 ops-3071",
      "Cache-Control": "no-cache"}


def get(url):
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=30).read().decode(
        "utf-8", "replace")


def main():
    fails, warns = [], []
    with report("3071_search_favs_zoom") as rep:
        rep.section("1. Page live")
        pg, ok = "", False
        for i in range(24):
            try:
                pg = get("https://justhodl.ai/chart-pro.html?cb=%d"
                         % time.time())
                if "jhChartable" in pg:
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
        for m in ("jhFavToggle", "JH_FAV_KEY",
                  "★ Favorites", "⏱ Recent",
                  'id="tf-zoom-in"', 'id="tf-zoom-out"',
                  'id="tf-fit"', "jhStep(",
                  ".filter(s => jhChartable(s))",
                  "sr-fav"):
            if m not in pg:
                fails.append("marker missing: %s" % m)
        # both ingestion points gated
        n_gate = pg.count(".filter(s => jhChartable(s))")
        rep.kv(chartable_gates=n_gate)
        if n_gate < 2:
            fails.append("chartable gate on %d/2 search surfaces"
                         % n_gate)
        if "Futures" in pg and "US Equities · ETFs · Indices" \
                not in pg:
            warns.append("group label still advertises futures")

        rep.section("verdict")
        _fin(rep, fails, warns)
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS")


def _fin(rep, fails, warns):
    (AWS_DIR / "ops" / "reports" / "3071.json").write_text(json.dumps(
        {"ops": 3071, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
