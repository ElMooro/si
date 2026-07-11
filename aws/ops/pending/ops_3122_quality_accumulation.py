#!/usr/bin/env python3
"""ops 3122 -- QUALITY PROPAGATION #4: accumulation.html to
research-desk bar (Khalid: continue the quality propagation roadmap;
final roadmap item -- closes the arc opened at 3117). Page-side
composition only, exact 3119/3120 pattern: (a) forensic-screen join
(window.FOMAP) -- strength grade chip, concern>=40 flag,
M-deteriorating tag, sector percentile on every phase card, reversal
card, and 200DMA-break card (share-flows dilution/buyback chips were
already live on reversal cards from the v1.3 arc; forensic completes
the pair); (b) research + forensic deep-links per ticker; (c)
methodology footer extension. Zero engine changes, zero API budget.
Lessons applied: 3118 (ASCII-only markers), 3116 (marks[0]=FOMAP is
new to this push)."""
import json, sys, time, urllib.request
from datetime import datetime, timezone
from pathlib import Path
from ops_report import report

AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 ops-3122", "Cache-Control": "no-cache"}


def get(url):
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=30).read().decode(
        "utf-8", "replace")


def main():
    fails, warns = [], []
    with report("3122_quality_accumulation") as rep:
        rep.section("1. Donor docs joinable")
        fo = json.loads(get("https://justhodl-dashboard-live.s3."
                            "us-east-1.amazonaws.com/data/"
                            "forensic-screen.json"))
        allr = fo.get("all_results") or []
        n_g = sum(1 for r in allr if r.get("strength_grade"))
        rep.kv(forensic_names=len(allr), graded=n_g)
        if n_g < 400:
            fails.append("forensic grades thin: %d" % n_g)
        ac = json.loads(get("https://justhodl-dashboard-live.s3."
                            "us-east-1.amazonaws.com/data/"
                            "accumulation-radar.json"))
        rv = ac.get("reversals") or {}
        rows = ((rv.get("bottoms") or []) + (rv.get("tops") or []))
        fomap = {r.get("symbol") for r in allr}
        overlap = sum(1 for r in rows if r.get("ticker") in fomap)
        rep.kv(n_scored=ac.get("n_scored"),
               reversal_rows=len(rows), forensic_overlap=overlap)
        if not ac.get("n_scored"):
            fails.append("accumulation-radar doc empty")
        if rows and overlap == 0:
            warns.append("zero forensic overlap on %d reversal rows"
                         % len(rows))

        rep.section("2. Page live (this-push)")
        ok = False
        pg = ""
        for i in range(20):
            try:
                pg = get("https://justhodl.ai/accumulation.html?cb=%d"
                         % time.time())
                if "FOMAP" in pg:
                    ok = True
                    break
            except Exception:
                pass
            time.sleep(18)
        if not ok:
            fails.append("accumulation.html FOMAP join not live")
        else:
            for m in ("financials", "concern ",
                      "M-Score deteriorating",
                      "forensic.html?ticker=",
                      "why.html?ticker=",
                      "Methodology: phases",
                      "forensic-screen.json"):
                if m not in pg:
                    fails.append("accumulation.html marker missing: "
                                 "%s" % m)

        rep.section("verdict")
        _fin(rep, fails, warns)
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS")


def _fin(rep, fails, warns):
    (AWS_DIR / "ops" / "reports" / "3122.json").write_text(json.dumps(
        {"ops": 3122, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
