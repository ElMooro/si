#!/usr/bin/env python3
"""ops 3130 -- CAPEX-PREDICTOR DEEPENING LEG 1: MACRO INTENTIONS
(Khalid: continue the why.html roadmap / capex-predictor deepening;
context-gated to the smallest verifiable leg -- remaining Tier-2
items #6-#9 and Census-M3/book-to-bill legs stay banked in
aws/ops/design/why-roadmap-tier2-3.md for the fresh session).
Shipped: capex-pulse engine gains _fred_intentions() -- Philly Fed
Future Capital Expenditures diffusion index (FRED
CEFDFSA066MSFRBPHI), the classic ~6-month lead on actual capex:
latest + 3m-avg + 12m delta + EXPANSION/FLAT/CONTRACTION read,
emitted as macro_intentions with capex_intentions_v=1.0; FRED key
inherited via config from justhodl-confluence-meta (env only, no new
repo key exposure). IR capex board renders the MACRO INTENTIONS line
between the hyperscaler tile and the sector table. Verify:
Event-invoke capex-pulse, poll doc for capex_intentions_v + a
non-null philly latest (real data gate -- FAIL, not placeholder, if
FRED/series unavailable), then page markers. Lessons: 3118 ASCII,
3116 marks[0]-new, Event+S3-poll, 3129 helper-portability (board
edit reuses this page's existing block only)."""
import json, sys, time, urllib.request
from datetime import datetime, timezone
from pathlib import Path
import boto3
from ops_report import report

LAM = boto3.client("lambda", region_name="us-east-1")
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 ops-3130", "Cache-Control": "no-cache"}


def get(url):
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=30).read().decode(
        "utf-8", "replace")


def s3j(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return None


def fire():
    try:
        LAM.invoke(FunctionName="justhodl-capex-pulse",
                   InvocationType="Event")
    except Exception:
        pass


def main():
    fails, warns = [], []
    with report("3130_capex_intentions") as rep:
        rep.section("1. Doc: intentions live (Event + poll)")
        fire()
        d = {}
        for i in range(30):
            time.sleep(20)
            d = s3j("data/capex-pulse.json") or {}
            if d.get("capex_intentions_v") == "1.0":
                break
            if i in (8, 16, 24):
                fire()
        mi = (d.get("macro_intentions") or {}).get(
            "philly_future_capex") or {}
        rep.kv(intentions_v=d.get("capex_intentions_v"),
               philly_latest=mi.get("latest"),
               philly_avg3=mi.get("avg_3m"),
               philly_delta12=mi.get("delta_12m"),
               philly_read=mi.get("read"), asof=mi.get("asof"))
        if d.get("capex_intentions_v") != "1.0":
            fails.append("doc missing capex_intentions_v after poll")
        if mi.get("latest") is None:
            fails.append("philly future-capex null -- FRED key/"
                         "series gate (real data only, no fallback)")

        rep.section("2. Page live (this-push)")
        ok = False
        pg = ""
        for i in range(20):
            try:
                pg = get("https://justhodl.ai/industry-rotation.html"
                         "?cb=%d" % time.time())
                if "MACRO INTENTIONS" in pg:
                    ok = True
                    break
            except Exception:
                pass
            time.sleep(15)
        if not ok:
            fails.append("MACRO INTENTIONS line not live")
        else:
            for m in ("leads actual capex ~6m",
                      "philly_future_capex",
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
    (AWS_DIR / "ops" / "reports" / "3130.json").write_text(json.dumps(
        {"ops": 3130, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
