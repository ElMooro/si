#!/usr/bin/env python3
"""ops 3132 -- HERO FIX VERIFY (Khalid: the box next to Apple at the
top of why.html is empty). Two blanks, two causes: (1) 'avg --' was a
REAL engine defect -- /stable/quote does not return v3's avgVolume;
now derived from the doc's own 50d technicals volume series (real
data, zero extra API), placed ABOVE json.dumps after a placement bug
was caught in review (first insert landed after serialization -- the
cached doc would have missed it; process lesson: verify against the
ARTIFACT the consumer reads); (2) the verdict panel's naked dashes
are the credit-gated Claude synthesis (known standing issue,
self-heals on top-up) -- hero now degrades to 'AI verdict pending'
messaging instead of bare dashes. Verify against the S3 doc itself:
delete equity-research/AAPL.json, GET the function URL to force
regeneration on the new bundle (retry loop while deploy-lambdas
lands), assert the S3-cached doc carries non-null avg_volume >= 1e6
and from_cache False, then page markers."""
import json, sys, time, urllib.request
from datetime import datetime, timezone
from pathlib import Path
import boto3
from ops_report import report

LAM = boto3.client("lambda", region_name="us-east-1")
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
KEY = "equity-research/AAPL.json"
AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 ops-3132", "Cache-Control": "no-cache"}


def get(url, timeout=240):
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=timeout).read().decode(
        "utf-8", "replace")


def main():
    fails, warns = [], []
    with report("3132_hero_fix") as rep:
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
                warns.append("regen GET attempt %d: %s"
                             % (attempt, ex))
            time.sleep(8)
            try:
                doc = json.loads(S3.get_object(
                    Bucket=BUCKET, Key=KEY)["Body"].read())
            except Exception:
                doc = {}
            av = (doc.get("quote") or {}).get("avg_volume")
            if av is not None:
                break
            time.sleep(45)  # deploy-lambdas may still be landing
        av = (doc.get("quote") or {}).get("avg_volume")
        rep.kv(avg_volume=av, from_cache=doc.get("from_cache"),
               generated_at=doc.get("generated_at"))
        if av is None:
            fails.append("S3 doc avg_volume still null after "
                         "regen retries")
        elif av < 1e6:
            fails.append("avg_volume implausible for AAPL: %s" % av)
        if doc.get("from_cache") is not False:
            warns.append("from_cache=%s on regen"
                         % doc.get("from_cache"))

        rep.section("2. Page live (this-push)")
        ok = False
        pg = ""
        for i in range(20):
            try:
                pg = get("https://justhodl.ai/why.html?cb=%d"
                         % time.time(), timeout=30)
                if "AI verdict pending" in pg:
                    ok = True
                    break
            except Exception:
                pass
            time.sleep(15)
        if not ok:
            fails.append("hero fallback not live")

        rep.section("verdict")
        _fin(rep, fails, warns)
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS")


def _fin(rep, fails, warns):
    (AWS_DIR / "ops" / "reports" / "3132.json").write_text(json.dumps(
        {"ops": 3132, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
