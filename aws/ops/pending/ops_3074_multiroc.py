#!/usr/bin/env python3
"""ops 3074 -- Δ% Multi-ROC reversal pane (Khalid): WoW + MoM + QoQ +
YoY percentage change as four synced lines on one zero-line sub-pane
(KST/Coppock family -- zero-crossings and short-crossing-long = the
trend-reversal reads). Span-aware lookbacks (day 5/21/63/252, week
1/4/13/52, month 1/3/12), fetch auto-widens so YoY has lookback,
toggle lives in the indicators menu, persists via workspace. The
existing single-mode DoD..YoY main-series transform (chg-btn row)
stays. Static verification."""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from ops_report import report

AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 ops-3074",
      "Cache-Control": "no-cache"}


def get(url):
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=30).read().decode(
        "utf-8", "replace")


def main():
    fails, warns = [], []
    with report("3074_multiroc") as rep:
        rep.section("1. Page live")
        pg, ok = "", False
        for i in range(24):
            try:
                pg = get("https://justhodl.ai/chart-pro.html?cb=%d"
                         % time.time())
                if "Multi-ROC" in pg:
                    ok = True
                    rep.kv(live_after_s=i * 20)
                    break
            except Exception:
                pass
            time.sleep(20)
        if not ok:
            fails.append("Multi-ROC not live after 8min")
            _fin(rep, fails, warns)
            sys.exit(1)
        for m in ('data-ind="roc"', "nch-roc-", "W: 5, M: 21, "
                  "Q: 63, Y: 252", "W: 1, M: 4, Q: 13, Y: 52",
                  "zero-cross = reversal",
                  "State.indicators.roc",
                  "{ day: 280, week: 400, month: 420 }"):
            if m not in pg:
                fails.append("marker missing: %s" % m)
        n_gate = pg.count("State.indicators.roc")
        rep.kv(roc_refs=n_gate)
        if n_gate < 4:
            fails.append("roc wired at %d sites (<4: menu-bind/"
                         "shell/height/widen)" % n_gate)

        rep.section("verdict")
        _fin(rep, fails, warns)
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS")


def _fin(rep, fails, warns):
    (AWS_DIR / "ops" / "reports" / "3074.json").write_text(json.dumps(
        {"ops": 3074, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
