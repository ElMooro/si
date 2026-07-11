#!/usr/bin/env python3
"""ops 3117 -- QUALITY BAR PROPAGATION (Khalid: make master-rank /
valuations / insider-radar / accumulation as rich as why.html, and
add industry growing-vs-shrinking analysis to why.html because a
fine stock in a shrinking industry is in trouble). One shared
enhancer jh-fund-chips.js (share-flows + forensic composed once,
data-jhf auto-annotation, chips + plain-English readline with
research cross-link) wired into 5 pages; master-rank adds
WHY-this-rank systems breakdown; why.html fleet section joins
industry-rotation (score/grade/trend/deterioration/ETF flows ->
GROWING / MIXED / SHRINKING verdict). 3115 lesson: IR per-industry rows live in doc key LADDER ({etf,name,leadership_score,tag}), not leaders/regiments. 3116 raced its own Pages deploy (marker set from the PRIOR push satisfied marks[0] on cached HTML while the new deploy was in flight) -- reverify only; per-page gate now requires the NEWEST marker first. Static-marker verify
(client-rendered, 3114 precedent) + doc-field asserts."""
import json, sys, time, urllib.request
from datetime import datetime, timezone
from pathlib import Path
import boto3
from ops_report import report

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 ops-3117", "Cache-Control": "no-cache"}


def get(url):
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=30).read().decode(
        "utf-8", "replace")


def main():
    fails, warns = [], []
    with report("3117_quality_propagation") as rep:
        rep.section("1. Docs joinable")
        try:
            ir = json.loads(S3.get_object(
                Bucket=BUCKET, Key="data/industry-rotation.json"
            )["Body"].read())
            pools = (ir.get("ladder") or [])
            named = [l for l in pools if l.get("name")]
            scored = sum(1 for l in named
                         if l.get("leadership_score") is not None)
            rep.kv(ir_named=len(named), ir_scored=scored)
            if len(named) < 20 or scored < 20:
                fails.append("IR ladder thin: named=%d scored=%d"
                             % (len(named), scored))
        except Exception as e:
            fails.append("industry-rotation doc: %s" % e)

        rep.section("2. Pages live (this-push)")
        checks = {
            "jh-fund-chips.js": ("data-jhf", "readline",
                                 "jhf-chips", "full research"),
            "master-rank.html": ("jh-fund-chips.js", "WHY: ",
                                 "data-jhf"),
            "valuations.html": ("jh-fund-chips.js", "data-jhf-read"),
            "insider.html": ("jh-fund-chips.js", "data-jhf"),
            "insider-clusters.html": ("jh-fund-chips.js",
                                      "data-jhf-read"),
            "accumulation.html": ("jh-fund-chips.js",
                                  "data-jhf-read"),
            "why.html": ("IR.ladder", "Industry health",
                         "SHRINKING / IN TROUBLE",
                         "industry-rotation.json",
                         "CONFIRMED DETERIORATION"),
        }
        for pg_name, marks in checks.items():
            ok = False
            pg = ""
            for i in range(20):
                try:
                    pg = get("https://justhodl.ai/%s?cb=%d"
                             % (pg_name, time.time()))
                    if marks[0] in pg:
                        ok = True
                        break
                except Exception:
                    pass
                time.sleep(15)
            if not ok:
                fails.append("%s not live (missing %s)"
                             % (pg_name, marks[0]))
                continue
            for m in marks:
                if m not in pg:
                    fails.append("%s marker missing: %s"
                                 % (pg_name, m))

        rep.section("verdict")
        _fin(rep, fails, warns)
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS")


def _fin(rep, fails, warns):
    (AWS_DIR / "ops" / "reports" / "3117.json").write_text(json.dumps(
        {"ops": 3117, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
