#!/usr/bin/env python3
"""ops 3128 -- IR RENDER ISOLATION (Khalid: a lot still missing).
3127 fixed the doc shape (rows clean, capex board fully populated --
confirmed live: hyperscaler $508B +73.5%%, 11/11 sector yoy, top-3
spenders, beneficiaries on every row) but the lower desk (RRG cards/
map/transitions, MA-cross events, cycle, appetite, pair spreads,
cross board, Leaders/Soldiers) still blanks: ONE monolithic .then
renders everything under a SINGLE catch, so the first throw anywhere
kills all nine builders plus Leaders. Engine emits rrg + leaders
(verified in source), so the throw is a page-side builder. Fix
shipped: every builder wrapped in individual try/catch (__safe) --
one failure can no longer blank the desk, and the err line now NAMES
the failing section ("section degraded (rest of desk unaffected):
<name>: <message>") instead of the generic feed-unavailable text, so
the culprit self-identifies on next load. Main catch also surfaces
the real error message. DOCTRINE BANKED: multi-section desks must
render-isolate; a shared catch converts one bug into a whole-desk
outage and hides the culprit."""
import json, sys, time, urllib.request
from datetime import datetime, timezone
from pathlib import Path
from ops_report import report

AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 ops-3128", "Cache-Control": "no-cache"}


def get(url):
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=30).read().decode(
        "utf-8", "replace")


def main():
    fails, warns = [], []
    with report("3128_ir_isolation") as rep:
        rep.section("1. Page live (this-push)")
        ok = False
        pg = ""
        for i in range(20):
            try:
                pg = get("https://justhodl.ai/industry-rotation.html"
                         "?cb=%d" % time.time())
                if "__secfail" in pg:
                    ok = True
                    break
            except Exception:
                pass
            time.sleep(15)
        if not ok:
            fails.append("isolation wrapper not live")
        else:
            for m in ("__safe(renderRRGCards",
                      "__safe(renderCrossBoard",
                      "section degraded (rest of desk unaffected)",
                      "desk render error",
                      "WHERE THE CAPEX IS GOING"):
                if m not in pg:
                    fails.append("marker missing: %s" % m)
        rep.section("verdict")
        _fin(rep, fails, warns)
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS")


def _fin(rep, fails, warns):
    (AWS_DIR / "ops" / "reports" / "3128.json").write_text(json.dumps(
        {"ops": 3128, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
