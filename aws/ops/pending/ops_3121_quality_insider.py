#!/usr/bin/env python3
"""ops 3121 -- QUALITY PROPAGATION #3: insider-radar (insider.html) to
research-desk bar (Khalid: continue the quality propagation roadmap;
auto-continue item). Page-side composition only, exact 3119/3120
pattern: (a) forensic-screen join (window.FOMAP) -- strength grade
chip, concern>=40 flag, M-deteriorating tag, sector percentile on
every cluster / decline-cluster / Finviz-breadth row; (b) NEW for the
insider desk: share-flows conviction cross (window.SFMAP) -- NET BB
double-conviction, MGMT_SELLING_INTO_BUYBACK contradiction flag,
DILUTING and SBC context on the same rows; (c) research + forensic
deep-links per ticker; (d) methodology footer. Zero engine changes,
zero API budget. Lessons applied: 3118 (ASCII-only markers), 3116
(marks[0] must be NEW to this push -- FOMAP -- so a cached prior page
cannot satisfy the gate)."""
import json, sys, time, urllib.request
from datetime import datetime, timezone
from pathlib import Path
from ops_report import report

AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 ops-3121", "Cache-Control": "no-cache"}


def get(url):
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=30).read().decode(
        "utf-8", "replace")


def main():
    fails, warns = [], []
    with report("3121_quality_insider") as rep:
        rep.section("1. Donor docs joinable")
        fo = json.loads(get("https://justhodl-dashboard-live.s3."
                            "us-east-1.amazonaws.com/data/"
                            "forensic-screen.json"))
        allr = fo.get("all_results") or []
        n_g = sum(1 for r in allr if r.get("strength_grade"))
        rep.kv(forensic_names=len(allr), graded=n_g)
        if n_g < 400:
            fails.append("forensic grades thin: %d" % n_g)
        sf = json.loads(get("https://justhodl-dashboard-live.s3."
                            "us-east-1.amazonaws.com/data/"
                            "share-flows.json"))
        tk = sf.get("tickers") or {}
        rep.kv(share_flow_tickers=len(tk))
        if len(tk) < 100:
            fails.append("share-flows tickers thin: %d" % len(tk))
        ir = json.loads(get("https://justhodl-dashboard-live.s3."
                            "us-east-1.amazonaws.com/data/"
                            "insider-radar.json"))
        cl = (ir.get("clusters") or []) + (ir.get("decline_clusters")
                                           or [])
        fomap = {r.get("symbol") for r in allr}
        overlap = sum(1 for c in cl if c.get("ticker") in fomap)
        rep.kv(cluster_rows=len(cl), forensic_overlap=overlap,
               fv_buys=len(ir.get("finviz_buys") or []))
        if cl and overlap == 0:
            warns.append("zero forensic overlap on %d clusters "
                         "(small-cap universe gap, chips degrade "
                         "gracefully)" % len(cl))

        rep.section("2. Page live (this-push)")
        ok = False
        pg = ""
        for i in range(20):
            try:
                pg = get("https://justhodl.ai/insider.html?cb=%d"
                         % time.time())
                if "FOMAP" in pg:
                    ok = True
                    break
            except Exception:
                pass
            time.sleep(18)
        if not ok:
            fails.append("insider.html FOMAP join not live")
        else:
            for m in ("financials", "concern ",
                      "M-Score deteriorating",
                      "mgmt selling into buyback",
                      "forensic.html?ticker=",
                      "why.html?ticker=",
                      "Methodology: clusters require",
                      "share-flows.json",
                      "SFMAP"):
                if m not in pg:
                    fails.append("insider.html marker missing: %s"
                                 % m)

        rep.section("verdict")
        _fin(rep, fails, warns)
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS")


def _fin(rep, fails, warns):
    (AWS_DIR / "ops" / "reports" / "3121.json").write_text(json.dumps(
        {"ops": 3121, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
