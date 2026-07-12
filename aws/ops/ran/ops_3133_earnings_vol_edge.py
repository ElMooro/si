#!/usr/bin/env python3
"""ops 3133 -- EARNINGS-VOL EDGE VERIFY (#6, why.html roadmap Tier 2,
DELTA-ONLY per Khalid live-render audit ops 3131: implied move / ATM IV
already live -- this ships only the realized-vs-implied RICH/CHEAP read
+ PEAD drift). Engine: build_earnings_vol_edge in justhodl-equity-research
(schema 2.2) computes per-print earnings reaction = max(BMO gap, AMC gap)
around each FMP /stable/earnings report date over last <=8 prints from
the doc's own EOD closes (zero extra API), median realized move, ratio vs
live implied_move_pct -> RICH >=1.25x / CHEAP <=0.80x / FAIR, and PEAD
avg T+1/T+5/T+20 drift bucketed by beat/miss (+/-0.5% EPS surprise).
Cache gate on presence of earnings_vol_edge key. Page: renderEarningsVolEdge
block after Earnings Track Record. Verify against the S3 artifact the page
reads: delete AAPL cache, regenerate via function URL (retry while
deploy-lambdas lands), truth-band the doc, then this-push page markers
(3118 ASCII, 3116 newest-marker-first)."""
import json, sys, time, urllib.request
from datetime import datetime, timezone, date
from pathlib import Path
import boto3
from ops_report import report

LAM = boto3.client("lambda", region_name="us-east-1")
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
KEY = "equity-research/AAPL.json"
AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 ops-3133", "Cache-Control": "no-cache"}
MARKS = ["renderEarningsVolEdge", "evx-block", "Earnings-Vol Edge"]  # marks[0] = newest


def get(url, timeout=240):
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=timeout).read().decode(
        "utf-8", "replace")


def main():
    fails, warns = [], []
    with report("3133_earnings_vol_edge") as rep:
        rep.section("1. Regenerate AAPL on new bundle (S3 doc gate)")
        url = LAM.get_function_url_config(
            FunctionName="justhodl-equity-research"
        )["FunctionUrl"].rstrip("/")
        doc = {}
        for attempt in range(6):
            try:
                S3.delete_object(Bucket=BUCKET, Key=KEY)
            except Exception:
                pass
            try:
                get(url + "/?ticker=AAPL")
            except Exception as ex:
                warns.append("regen GET attempt %d: %s" % (attempt, ex))
            time.sleep(8)
            try:
                doc = json.loads(S3.get_object(
                    Bucket=BUCKET, Key=KEY)["Body"].read())
            except Exception:
                doc = {}
            evx = doc.get("earnings_vol_edge") or {}
            if evx.get("status") == "ok":
                break
            time.sleep(45)  # deploy-lambdas may still be landing
        evx = doc.get("earnings_vol_edge") or {}
        rep.kv(status=evx.get("status"), schema=doc.get("schema_version"),
               median_realized=evx.get("median_realized_move_pct"),
               implied=evx.get("implied_move_pct"),
               ratio=evx.get("implied_vs_realized_ratio"),
               verdict=evx.get("vol_verdict"),
               prints_used=evx.get("prints_used"),
               next_earnings=evx.get("next_earnings"),
               d2e=evx.get("days_to_earnings"),
               from_cache=doc.get("from_cache"))
        if evx.get("status") != "ok":
            fails.append("earnings_vol_edge status=%s after regen retries"
                         % evx.get("status"))
        else:
            if doc.get("schema_version") != "2.2":
                fails.append("schema_version=%s expected 2.2"
                             % doc.get("schema_version"))
            med = evx.get("median_realized_move_pct")
            if med is None or not (1.0 <= med <= 12.0):
                fails.append("AAPL median realized move out of truth band"
                             " 1-12%%: %s" % med)
            pu = evx.get("prints_used") or 0
            if pu < 6:
                fails.append("prints_used=%s < 6 for AAPL (full history"
                             " expected)" % pu)
            pead = evx.get("pead") or {}
            if not ((pead.get("beat") or {}).get("n") or 0) >= 1:
                fails.append("PEAD beat bucket empty for AAPL")
            ne = evx.get("next_earnings")
            if ne and ne[:10] < date.today().isoformat():
                fails.append("next_earnings in the past: %s" % ne)
            d2e = evx.get("days_to_earnings")
            if d2e is not None and not (-1 <= d2e <= 150):
                fails.append("days_to_earnings implausible: %s" % d2e)
            imp = evx.get("implied_move_pct")
            if imp is None:
                warns.append("no implied_move_pct on AAPL doc"
                             " (thin/absent chain) -- verdict suppressed")
            else:
                if evx.get("vol_verdict") not in ("RICH", "CHEAP", "FAIR"):
                    fails.append("vol_verdict=%s with implied present"
                                 % evx.get("vol_verdict"))
                r = evx.get("implied_vs_realized_ratio")
                if r is None or not (0.2 <= r <= 5.0):
                    fails.append("implied/realized ratio implausible: %s" % r)
            prints = evx.get("prints") or []
            bad = [p for p in prints
                   if p.get("reaction_move_pct") is None
                   or not (0.0 <= p["reaction_move_pct"] <= 25.0)]
            if bad:
                fails.append("%d prints with implausible reaction moves"
                             % len(bad))
        if doc.get("from_cache") is not False:
            warns.append("from_cache=%s on regen" % doc.get("from_cache"))

        rep.section("2. Page live (this-push)")
        ok = False
        pg = ""
        for i in range(20):
            try:
                pg = get("https://justhodl.ai/why.html?cb=%d"
                         % time.time(), timeout=30)
                if MARKS[0] in pg:
                    ok = True
                    break
            except Exception:
                pass
            time.sleep(15)
        if not ok:
            fails.append("newest marker %r not live" % MARKS[0])
        else:
            missing = [m for m in MARKS if m not in pg]
            if missing:
                fails.append("markers missing on live page: %s" % missing)
        rep.kv(page_marks_ok=ok)

        rep.section("verdict")
        _fin(rep, fails, warns)
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS")


def _fin(rep, fails, warns):
    (AWS_DIR / "ops" / "reports" / "3133.json").write_text(json.dumps(
        {"ops": 3133, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
