#!/usr/bin/env python3
"""ops 3131 -- CONVICTION STRIP TICKER FIX + ASCII HEADERS + DELTA
SPEC (Khalid: are Tier-2 items already on why.html? -- live-render
audit says PARTIALLY YES). Corrections: (1) Fleet Conviction strip
gated on a ?ticker= URL param the Research Desk does not always set,
leaving 'composing live boards...' stuck -- now derives the ticker
from the rendered doc itself (window.__rd.ticker) with the URL param
as fallback; (2) numeric HTML entities in dynamically-assembled
section headers mangle in the pipeline (Khalid dump: '&#B85C4E11;',
'f7d55;') -- replaced with plain ASCII, extending the 3118 lesson to
assembled headers; (3) design doc rewritten DELTA-ONLY: implied
move / ATM IV / skew / P-C OI / IV smile and the earnings-surprise
history ALREADY LIVE on the page -- #6 shrinks to realized-vs-implied
RICH/CHEAP verdict + PEAD drift, #9 shrinks to the IV-rank-vs-own-1y
ledger + term structure. DOCTRINE: audit the live render, not just
renderer names, before speccing."""
import json, sys, time, urllib.request
from datetime import datetime, timezone
from pathlib import Path
from ops_report import report

AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 ops-3131", "Cache-Control": "no-cache"}


def get(url):
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=30).read().decode(
        "utf-8", "replace")


def main():
    fails, warns = [], []
    with report("3131_strip_ticker_fix") as rep:
        rep.section("1. Page live (this-push)")
        ok = False
        pg = ""
        for i in range(20):
            try:
                pg = get("https://justhodl.ai/why.html?cb=%d"
                         % time.time())
                if "window.__rd||{}).ticker" in pg:
                    ok = True
                    break
            except Exception:
                pass
            time.sleep(15)
        if not ok:
            fails.append("ticker-derivation fix not live")
        else:
            for bad in ("&#9201;", "&#128737;", "&#9878;",
                        "&#9876;"):
                if bad in pg:
                    fails.append("mangled entity still present: %s"
                                 % bad)
            for m in ("Fleet Conviction", "Momentum &amp; Trend"):
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
    (AWS_DIR / "ops" / "reports" / "3131.json").write_text(json.dumps(
        {"ops": 3131, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
