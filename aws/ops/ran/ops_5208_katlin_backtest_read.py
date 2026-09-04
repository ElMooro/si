"""ops_5208 -- KATLIN walk-forward: read data/katlin-backtest.json (kicked by ops 5203 at 19:08 UTC). If it is missing or
older than today's engine code, invoke {"mode":"backtest"} async and poll up to 14 min. Prints every cohort's n / median
excess vs SPY / hit rate / MAE at 21, 63, 126 sessions -- the first measured edge numbers, reported as measured."""
import json
import sys
import time
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

CFG = Config(retries={"max_attempts": 3, "mode": "adaptive"}, read_timeout=120)
lam = boto3.client("lambda", region_name="us-east-1", config=CFG)
s3 = boto3.client("s3", region_name="us-east-1", config=CFG)
BUCKET = "justhodl-dashboard-live"
KEY = "data/katlin-backtest.json"
FN = "justhodl-katlin"
FAILS = []


def get():
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=KEY)["Body"].read())
    except Exception:
        return None


with report("ops_5208_katlin_backtest_read") as R:
    R.heading("ops 5208 -- KATLIN walk-forward read")
    B = get()
    if B:
        R.log("   existing backtest: v%s as_of %s n_obs %s n_dates %s universe %s" % (B.get("version"), B.get("as_of"), B.get("n_obs"), B.get("n_dates"), B.get("universe")))
    if not B or B.get("version") != "1.2.0":
        before = (B or {}).get("as_of")
        lam.invoke(FunctionName=FN, InvocationType="Event", Payload=json.dumps({"mode": "backtest"}).encode())
        R.log("   invoked backtest async (prior as_of=%s); polling %s" % (before, KEY))
        t0 = time.time()
        B = None
        while time.time() - t0 < 860:
            time.sleep(20)
            b = get()
            if b and b.get("as_of") != before:
                B = b
                break
        if not B:
            FAILS.append("no fresh backtest after %.0fs" % (time.time() - t0))
    if B:
        R.section("walk-forward cohorts (point-in-time price gates, excess vs SPY)")
        R.log("   v%s as_of %s sessions %s (%s..%s) n_obs %s n_dates %s step %s universe %s budget_hit %s" % (
            B.get("version"), B.get("as_of"), B.get("sessions"), B.get("first"), B.get("last"), B.get("n_obs"), B.get("n_dates"), B.get("step"), B.get("universe"), B.get("budget_hit")))
        co = B.get("cohorts") or {}
        R.log("   %-30s %s" % ("cohort", " | ".join("%-34s" % h for h in ("21s n/med_excess/hit/MAE", "63s", "126s"))))
        for lab, hz in co.items():
            cells = []
            for h in ("21s", "63s", "126s"):
                c = (hz or {}).get(h) or {}
                cells.append("%5s %7s %5s %7s" % (c.get("n"), c.get("median_excess_pct"), c.get("hit_rate_pct"), c.get("median_max_adverse_pct")))
            R.log("   %-30s %s" % (lab, " | ".join("%-34s" % x for x in cells)))
        notes = B.get("notes") or B.get("log") or []
        for ln in notes[-12:]:
            R.log("   " + str(ln)[:220])
        if not co:
            FAILS.append("backtest has no cohorts")
    for f in FAILS:
        R.fail("   " + f)
    if FAILS:
        sys.exit(1)
    R.ok("   walk-forward read")
    sys.exit(0)
