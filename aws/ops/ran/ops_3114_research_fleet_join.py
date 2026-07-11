#!/usr/bin/env python3
"""ops 3114 -- research desk gains the fleet cross-read (Khalid loves
why.html; add everything built since). New section composes
forensic-screen v2.2 (strength grade/legs, industry %%ile, Beneish M +
trend, Sloan, valuation vs sector) + share-flows v1.4 (read, net
buyback, SBC, flags) + buyback-engine v1.1 (fresh auth, blackout,
pump board) for the live ticker -- client-side join, any ticker,
deep links out. Quality doctrine archived as memory-archive 32."""
import json, sys, time, urllib.request
from datetime import datetime, timezone
from pathlib import Path
from ops_report import report

AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 ops-3114", "Cache-Control": "no-cache"}


def get(url):
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=30).read().decode(
        "utf-8", "replace")


def main():
    fails, warns = [], []
    with report("3114_research_fleet_join") as rep:
        rep.section("1. Page live (this-push)")
        pg = ""
        ok = False
        for i in range(20):
            try:
                pg = get("https://justhodl.ai/why.html?cb=%d"
                         % time.time())
                if "jh-fleet-forensic" in pg:
                    ok = True
                    break
            except Exception:
                pass
            time.sleep(18)
        if not ok:
            fails.append("why.html fleet join not live")
        else:
            for m in ("Forensic &amp; Capital Flows",
                      "forensic-screen.json", "share-flows.json",
                      "buyback-engine.json", "Beneish M-Score",
                      "NET buyback yield", "IN BLACKOUT",
                      "strength_legs"):
                if m not in pg:
                    fails.append("marker missing: %s" % m)
        rep.section("2. Source docs present for the join")
        for k in ("data/forensic-screen.json", "data/share-flows.json",
                  "data/buyback-engine.json"):
            try:
                d = json.loads(get("https://justhodl.ai/" + k
                                   + "?cb=%d" % time.time()))
                nn = len(d.get("all_results") or d.get("tickers")
                         or {})
                rep.kv(**{k.split("/")[1].split(".")[0]
                          .replace("-", "_"): nn})
                if not nn:
                    fails.append("%s empty" % k)
            except Exception as e:
                fails.append("%s unreachable: %s" % (k, str(e)[:60]))
        rep.section("verdict")
        (AWS_DIR / "ops" / "reports" / "3114.json").write_text(
            json.dumps({"ops": 3114,
                        "verdict": "FAIL" if fails else "PASS",
                        "fails": fails, "warns": warns,
                        "ts": datetime.now(timezone.utc).isoformat()},
                       indent=1))
        rep.kv(verdict="FAIL" if fails else "PASS",
               n_fails=len(fails), n_warns=len(warns))
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS")


main()
sys.exit(0)
