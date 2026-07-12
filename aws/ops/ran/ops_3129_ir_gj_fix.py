#!/usr/bin/env python3
"""ops 3129 -- IR ROOT CAUSE FIX: gj -> fetchJSON (Khalid: items not
populating; audit deletions). AUDIT VERDICT (deepened git history):
zero content deleted today -- ede2139/9445317 'removals' are
replaced-in-place strings, fb11743/2a88f68 removed nothing. ROOT
CAUSE, self-identified by the 3128 isolation layer on Khalid's live
load ('desk render error: gj is not defined'): ops 3106 (2a88f68,
the parallel share-flows arc, July 11) wired
gj("data/share-flows.json") into this page's main chain for the
leaders R:R/buyback chips -- but this page's fetch helper is
fetchJSON (line 256); gj exists on OTHER pages. The ReferenceError
fired BEFORE the render chain, blanking RRG cards/map/transitions,
MA-cross events, cycle, appetite, pair spreads, cross board, and
Leaders/Soldiers since 3106 -- pre-dating everything shipped today.
Fix: the one call now uses fetchJSON (identical promise contract).
With 3128's isolation also in place, the desk is now double-
protected: root cause removed AND any future single-section throw
stays contained and named. DOCTRINE BANKED: cross-page helper names
are not portable -- fleet page-edit ops must grep the target page's
own helper (gj vs fetchJSON) before wiring joins."""
import json, sys, time, urllib.request
from datetime import datetime, timezone
from pathlib import Path
from ops_report import report

AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 ops-3129", "Cache-Control": "no-cache"}


def get(url):
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=30).read().decode(
        "utf-8", "replace")


def main():
    fails, warns = [], []
    with report("3129_ir_gj_fix") as rep:
        rep.section("1. Page live (this-push)")
        ok = False
        pg = ""
        for i in range(20):
            try:
                pg = get("https://justhodl.ai/industry-rotation.html"
                         "?cb=%d" % time.time())
                if 'fetchJSON("data/share-flows.json")' in pg:
                    ok = True
                    break
            except Exception:
                pass
            time.sleep(15)
        if not ok:
            fails.append("fetchJSON fix not live")
        else:
            if 'gj("' in pg:
                fails.append('double-quoted gj( call still present')
            for m in ("__safe(renderRRGCards",
                      "WHERE THE CAPEX IS GOING",
                      "Leadership Ladder"):
                if m not in pg:
                    fails.append("marker missing: %s" % m)
        rep.kv(fix_live=ok)
        rep.section("verdict")
        _fin(rep, fails, warns)
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS")


def _fin(rep, fails, warns):
    (AWS_DIR / "ops" / "reports" / "3129.json").write_text(json.dumps(
        {"ops": 3129, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
