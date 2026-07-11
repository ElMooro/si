#!/usr/bin/env python3
"""ops 3119 -- QUALITY PROPAGATION #1: master-rank to research-desk
bar (Khalid: apply why.html's quality everywhere; auto-continue
roadmap item 1). Page-side composition only: (a) forensic-screen
join (window.FOMAP) -- strength grade chip, concern>=40 flag,
M-deteriorating tag, sector strength percentile on every leaderboard
card; (b) research + forensic deep-links per ticker; (c) methodology
footer. Zero engine changes, zero API budget. 3118 lesson: verify markers must be plain ASCII -- glyph round-trip mangled two markers while the page was live (3/5 matched)."""
import json, sys, time, urllib.request
from datetime import datetime, timezone
from pathlib import Path
from ops_report import report

AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 ops-3119", "Cache-Control": "no-cache"}


def get(url):
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=30).read().decode(
        "utf-8", "replace")


def main():
    fails, warns = [], []
    with report("3119_quality_master_rank") as rep:
        rep.section("1. Donor docs joinable")
        fo = json.loads(get("https://justhodl-dashboard-live.s3."
                            "us-east-1.amazonaws.com/data/"
                            "forensic-screen.json"))
        allr = fo.get("all_results") or []
        n_g = sum(1 for r in allr if r.get("strength_grade"))
        rep.kv(forensic_names=len(allr), graded=n_g)
        if n_g < 400:
            fails.append("forensic grades thin: %d" % n_g)
        mr = json.loads(get("https://justhodl-dashboard-live.s3."
                            "us-east-1.amazonaws.com/data/"
                            "master-ranker.json"))
        tt = mr.get("top_tickers") or mr.get("rows") or []
        overlap = sum(1 for r in tt[:40]
                      if any(a.get("symbol") == (r.get("ticker")
                                                 or r.get("symbol"))
                             for a in allr))
        rep.kv(rank_rows=len(tt), forensic_overlap_top40=overlap)
        if overlap < 10:
            warns.append("low overlap top40: %d" % overlap)

        rep.section("2. Page live (this-push)")
        ok = False
        pg = ""
        for i in range(20):
            try:
                pg = get("https://justhodl.ai/master-rank.html?cb=%d"
                         % time.time())
                if "FOMAP" in pg:
                    ok = True
                    break
            except Exception:
                pass
            time.sleep(18)
        if not ok:
            fails.append("master-rank.html FOMAP join not live")
        else:
            for m in ("financials</b>", "concern ",
                      "deteriorating</b>",
                      "forensic.html?ticker=",
                      "Methodology: unified cross-engine rank"):
                if m not in pg:
                    fails.append("marker missing: %s" % m[:30])

        rep.section("verdict")
        _fin(rep, fails, warns)
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS")


def _fin(rep, fails, warns):
    (AWS_DIR / "ops" / "reports" / "3119.json").write_text(json.dumps(
        {"ops": 3119, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
