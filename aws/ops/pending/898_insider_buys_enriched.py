"""
ops/898 - Verify edge upgrade #2 (Insider Open-Market BUY Cluster
Enrichment) end-to-end.

Steps:
  - confirm justhodl-insider-buys-enriched deploys
  - wire EventBridge schedule (daily 16:30 UTC)
  - invoke once
  - validate enriched schema (top_setups[], each with score,
    expected_returns 1m/3m/12m, recommended_trade with primary,
    why_now_explainer, conviction_tier)
  - confirm insider-buys.html serves on Pages

Writes aws/ops/reports/898_insider_buys_enriched.json.
"""
import json
import time
import urllib.request
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT = "857687956942"
S3_BUCKET = "justhodl-dashboard-live"
SCHEDULER_ROLE = ("arn:aws:iam::%s:role/justhodl-scheduler-role" % ACCOUNT)
FN = "justhodl-insider-buys-enriched"
SCHEDULE_NAME = "justhodl-insider-buys-enriched-daily"
SCHEDULE_EXPR = "cron(30 16 ? * MON-FRI *)"
S3_KEY = "data/insider-buys-enriched.json"
SOURCE_S3_KEY = "data/insider-clusters.json"
PAGE_URL = "https://justhodl.ai/insider-buys.html"

cfg = Config(read_timeout=300, connect_timeout=20,
             retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)
sched = boto3.client("scheduler", region_name=REGION, config=cfg)

report = {
    "ops": 898, "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Verify #2 insider-buys-enriched e2e",
    "checks": [],
}


def chk(name, ok, detail=""):
    report["checks"].append({"check": name, "ok": bool(ok),
                             "detail": str(detail)[:320]})


def get_or_make_schedule(name, expr, fn_arn):
    target = {
        "Arn": fn_arn, "RoleArn": SCHEDULER_ROLE, "Input": "{}",
        "RetryPolicy": {"MaximumRetryAttempts": 0,
                        "MaximumEventAgeInSeconds": 3600},
    }
    try:
        sched.get_schedule(Name=name)
        sched.update_schedule(
            Name=name, ScheduleExpression=expr,
            ScheduleExpressionTimezone="UTC",
            FlexibleTimeWindow={"Mode": "OFF"},
            State="ENABLED", Target=target)
        return "updated"
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
        sched.create_schedule(
            Name=name, ScheduleExpression=expr,
            ScheduleExpressionTimezone="UTC",
            FlexibleTimeWindow={"Mode": "OFF"},
            State="ENABLED", Target=target)
        return "created"


def fetch_url(url, timeout=15):
    req = urllib.request.Request(
        url + ("&" if "?" in url else "?") + "cb=" + str(int(time.time())),
        headers={"User-Agent": "ops898/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, len(r.read())


# 1. Lambda deployed
fn_arn = None
try:
    r = lam.get_function(FunctionName=FN)
    fn_arn = r["Configuration"]["FunctionArn"]
    chk("lambda_deployed", True,
        "runtime=%s timeout=%ss modified=%s" % (
            r["Configuration"]["Runtime"],
            r["Configuration"]["Timeout"],
            r["Configuration"]["LastModified"]))
except Exception as e:
    chk("lambda_deployed", False, "%s: %s" % (type(e).__name__, e))

# 2. Source data present (the cluster scanner output)
src_state = None
try:
    o = s3.head_object(Bucket=S3_BUCKET, Key=SOURCE_S3_KEY)
    src_state = "present (modified %s, %d bytes)" % (
        o["LastModified"], o["ContentLength"])
    chk("source_clusters_present", True, src_state)
except Exception as e:
    chk("source_clusters_present", False, "%s: %s" % (
        type(e).__name__, e))

# 3. Schedule wired
if fn_arn:
    try:
        action = get_or_make_schedule(SCHEDULE_NAME, SCHEDULE_EXPR, fn_arn)
        chk("schedule_wired", True,
            "%s: %s (%s)" % (SCHEDULE_NAME, action, SCHEDULE_EXPR))
    except Exception as e:
        chk("schedule_wired", False, "%s: %s" % (type(e).__name__, e))

# 4. Invoke
body = {}
if fn_arn:
    try:
        r = lam.invoke(FunctionName=FN,
                       InvocationType="RequestResponse",
                       Payload=b"{}")
        raw = r["Payload"].read().decode("utf-8", "ignore")
        payload = json.loads(raw) if raw else {}
        try:
            body = json.loads(payload.get("body") or "{}")
        except Exception:
            body = payload
        sm = body.get("summary") or {}
        chk("invoke", r.get("StatusCode") == 200
            and not r.get("FunctionError"),
            "FunctionError=%s enriched=%s HIGH=%s MED-HI=%s" % (
                r.get("FunctionError"),
                sm.get("enriched_returned"),
                sm.get("high_conviction"),
                sm.get("medium_high_conviction")))
    except Exception as e:
        chk("invoke", False, "%s: %s" % (type(e).__name__, e))

# 5. S3 enriched output
s3_body = {}
try:
    time.sleep(2)
    obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
    s3_body = json.loads(obj["Body"].read())
    chk("s3_output_present", True,
        "size=%d engine=%s" % (obj["ContentLength"],
                               s3_body.get("engine")))
except Exception as e:
    chk("s3_output_present", False, "%s: %s" % (type(e).__name__, e))

# 6. Schema validation
required = ["engine", "summary", "top_setups", "methodology",
            "academic_basis"]
missing = [k for k in required if k not in s3_body]
chk("schema_complete", len(missing) == 0,
    "missing=%s" % missing if missing else
    "all required top-keys present")

# 7. Per-cluster enrichment quality
setups = s3_body.get("top_setups") or []
if setups:
    s0 = setups[0]
    required_fields = ["ticker", "score", "expected_returns",
                       "recommended_trade", "why_now_explainer"]
    miss_top = [k for k in required_fields if k not in s0]
    has_horizons = (
        isinstance(s0.get("expected_returns"), dict) and
        all(h in s0["expected_returns"] for h in ["1m", "3m", "12m"]))
    has_trade = (
        isinstance(s0.get("recommended_trade"), dict) and
        bool(s0["recommended_trade"].get("primary")))
    chk("per_cluster_enrichment", len(miss_top) == 0 and has_horizons
        and has_trade,
        ("missing=%s horizons=%s primary_trade=%s ticker=%s "
         "conv=%s 3m=%s%%") % (
            miss_top, has_horizons, has_trade,
            s0.get("ticker"),
            (s0.get("recommended_trade") or {}).get("conviction_tier"),
            (s0.get("expected_returns") or {}).get("3m", {}).get(
                "return_pct")))
else:
    chk("per_cluster_enrichment", False, "no top_setups")

# 8. Page live
try:
    s_, sz = fetch_url(PAGE_URL)
    chk("page_live", s_ == 200 and sz > 5000,
        "status=%s bytes=%s" % (s_, sz))
except Exception as e:
    chk("page_live", False, "%s: %s" % (type(e).__name__, e))

n_ok = sum(1 for c in report["checks"] if c["ok"])
n_tot = len(report["checks"])
report["summary"] = "%d/%d checks passed" % (n_ok, n_tot)
report["all_passed"] = n_ok == n_tot

sm = s3_body.get("summary") or {}
top3 = [{"ticker": s.get("ticker"),
         "score": s.get("score"),
         "conviction": (s.get("recommended_trade") or {}).get(
             "conviction_tier"),
         "fwd_3m": (s.get("expected_returns") or {}).get(
             "3m", {}).get("return_pct"),
         "wr_3m": (s.get("expected_returns") or {}).get(
             "3m", {}).get("win_rate_pct")}
        for s in setups[:3]]

report["live_state"] = {
    "enriched_count": sm.get("enriched_returned"),
    "high_conviction": sm.get("high_conviction"),
    "medium_high": sm.get("medium_high_conviction"),
    "top_3": top3,
}
report["verdict"] = (
    ("EDGE UPGRADE #2 LIVE. Insider open-market BUY cluster "
     "enrichment is deployed, scheduled daily 16:30 UTC, reading "
     "from insider-clusters.json, publishing per-cluster expected "
     "1m/3m/12m returns (academic + quality-boost adjusted), "
     "retail trade ticket with options alt where liquid, why-now "
     "narrative, and conviction tier. Enriched %s clusters today "
     "(%s HIGH, %s MED-HI)." % (
        sm.get("enriched_returned"),
        sm.get("high_conviction"),
        sm.get("medium_high_conviction")))
    if report["all_passed"]
    else "VERIFICATION INCOMPLETE -- see per-check details.")

with open("aws/ops/reports/898_insider_buys_enriched.json", "w",
          encoding="utf-8") as f:
    json.dump(report, f, indent=1)
print(json.dumps(report, indent=1))
