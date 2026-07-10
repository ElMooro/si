#!/usr/bin/env python3
"""ops 3080 -- Industry-rotation page redesign (Khalid: RRG scatter
'horrible, so confusing' + ladder too narrow):
(1) QUADRANT CARDS replace the scatter as the primary rotation view
-- four colored panels (LEADING/IMPROVING/WEAKENING/LAGGING) with
per-quadrant guidance, ETFs as strength-sorted chips, each carrying a
rotation-heading arrow (which quadrant it's drifting toward, from the
trail vector) + a ROTATION TAPE strip of the latest dated quadrant
transitions; the classic scatter is demoted behind a 'show map'
disclosure;
(2) LADDER: page widened 1280->1680px, table full-width with sticky
sortable headers (ETF/Sharpe/Scorecard/Score/Drank), zebra rows,
hover highlight, 74vh scroll shell, and a live filter box;
render replayed locally (cards PASS on real-shaped data)."""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from ops_report import report

AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 ops-3080",
      "Cache-Control": "no-cache"}


def get(url):
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=30).read().decode(
        "utf-8", "replace")


def main():
    fails, warns = [], []
    with report("3080_rrg_redesign") as rep:
        rep.section("1. Page live (this-push marker)")
        pg, ok = "", False
        for i in range(24):
            try:
                pg = get("https://justhodl.ai/industry-rotation.html"
                         "?cb=%d" % time.time())
                if "renderRRGCards" in pg:
                    ok = True
                    rep.kv(live_after_s=i * 20)
                    break
            except Exception:
                pass
            time.sleep(20)
        if not ok:
            fails.append("redesign not live after 8min")
            _fin(rep, fails, warns)
            sys.exit(1)
        for m in ('id="rrg-cards"', 'id="rrg-movers"',
                  "ROTATION TAPE", "rotating toward",
                  "show the classic RRG scatter map",
                  'id="lad-filter"', "buildLadderRows",
                  "wireLadder", 'data-sort=\\"leadership_score\\"',
                  "max-width:1680px", "position:sticky",
                  "ladwrap"):
            if m not in pg:
                fails.append("marker missing: %s" % m)
        # scatter still available (demoted, not deleted)
        if "function renderRRG(d)" not in pg:
            fails.append("classic scatter removed instead of "
                         "demoted")

        rep.section("verdict")
        _fin(rep, fails, warns)
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS")


def _fin(rep, fails, warns):
    (AWS_DIR / "ops" / "reports" / "3080.json").write_text(json.dumps(
        {"ops": 3080, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
