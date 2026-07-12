#!/usr/bin/env python3
"""ops 3126 -- CAPEX-FLOW BOOM PREDICTOR (Khalid: track where capex
spending is going + metrics that predict industry booms BEFORE
obvious; scan existing engines and enhance). Scan found the rich base
already built: capex-pulse (real capex dollars, sector aggregates,
hyperscaler tile), forward-orders (RPO/backlog/book-to-bill 0-100),
structural-pre-signals (capex language in filings, earliest tell),
backlog, census core-capex -- NONE joined to industry-rotation.html.
Research grounding baked into the board copy: Sparkline (1995-2025)
shows top 1y-capex-growth quintile UNDERPERFORMS sector peers in all
10 GICS -- the boom accrues to RECEIVING industries (steel, power/
cooling industrials, utilities, semis; Morgan Stanley downstream map);
Oracle case: RPO +437%% yoy, stock -40%% on capex/FCF strain -- so
capex+RPO = demand booked and capacity being built, TRADE THE
SUPPLIER. Shipped page-side: (a) industry-rotation.html section
jh-capexflow -- capex by spending sector ranked by yoy acceleration,
mapped to downstream beneficiary ETFs with LIVE leadership scores,
hyperscaler tile, structural/forward-orders cross-read counts;
(b) why.html conviction strip gains CAPEX (FLAG at >=40%% yoy ramp,
Sparkline caveat in-chip) and FWD-ORDERS (BULL at score>=60) systems.
Zero engine changes. Lessons: 3118 ASCII, 3116 marks[0]-new."""
import json, sys, time, urllib.request
from datetime import datetime, timezone
from pathlib import Path
import boto3
from ops_report import report

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 ops-3126", "Cache-Control": "no-cache"}


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


def main():
    fails, warns = [], []
    with report("3126_capex_flow") as rep:
        rep.section("1. Donor docs")
        cp = s3j("data/capex-pulse.json") or {}
        secs = cp.get("sectors") or {}
        rows = cp.get("rows") or []
        rep.kv(capex_sectors=len(secs), capex_rows=len(rows),
               hyperscaler=bool(cp.get("hyperscalers")))
        if len(secs) < 6:
            fails.append("capex sectors thin: %d" % len(secs))
        if len(rows) < 100:
            fails.append("capex rows thin: %d" % len(rows))
        for key, name in (("data/forward-orders.json",
                           "forward_orders"),
                          ("data/structural-pre-signals.json",
                           "structural")):
            doc = s3j(key)
            n = 0
            if isinstance(doc, dict):
                n = sum(len(v) for v in doc.values()
                        if isinstance(v, list))
            rep.kv(**{name + "_items": n})
            if doc is None:
                warns.append("%s doc missing -- cross-read degrades"
                             % name)

        rep.section("2. Pages live (this-push)")
        checks = {
            "industry-rotation.html": (
                "jh-capexflow", "WHERE THE CAPEX IS GOING",
                "DOWNSTREAM BENEFICIARIES", "HYPERSCALER TILE",
                "Sparkline", "capex-pulse.json",
                "forward-orders.json",
                "structural-pre-signals.json"),
            "why.html": ("FWD-ORDERS", "suppliers benefit",
                         "capex-pulse.json",
                         "demand already booked"),
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
    (AWS_DIR / "ops" / "reports" / "3126.json").write_text(json.dumps(
        {"ops": 3126, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
