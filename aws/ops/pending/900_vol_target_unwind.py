"""
ops/900 - Verify edge upgrade #4 (Vol-Target Unwind Trigger) e2e.
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
FN = "justhodl-vol-target-unwind"
SCHEDULE_NAME = "justhodl-vol-target-unwind-4x-daily"
SCHEDULE_EXPR = "cron(0 13,16,19,21 ? * MON-FRI *)"
S3_KEY = "data/vol-target-unwind.json"
SSM_KEY = "/justhodl/vol-target-unwind/state"
PAGE_URL = "https://justhodl.ai/vol-target-unwind.html"

cfg = Config(read_timeout=300, connect_timeout=20,
             retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)
ssm = boto3.client("ssm", region_name=REGION, config=cfg)
sched = boto3.client("scheduler", region_name=REGION, config=cfg)

report = {"ops": 900, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Verify #4 vol-target-unwind e2e", "checks": []}


def chk(name, ok, detail=""):
    report["checks"].append({"check": name, "ok": bool(ok),
                             "detail": str(detail)[:340]})


def get_or_make_schedule(name, expr, fn_arn):
    target = {"Arn": fn_arn, "RoleArn": SCHEDULER_ROLE, "Input": "{}",
              "RetryPolicy": {"MaximumRetryAttempts": 0,
                              "MaximumEventAgeInSeconds": 3600}}
    try:
        sched.get_schedule(Name=name)
        sched.update_schedule(Name=name, ScheduleExpression=expr,
                              ScheduleExpressionTimezone="UTC",
                              FlexibleTimeWindow={"Mode": "OFF"},
                              State="ENABLED", Target=target)
        return "updated"
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
        sched.create_schedule(Name=name, ScheduleExpression=expr,
                              ScheduleExpressionTimezone="UTC",
                              FlexibleTimeWindow={"Mode": "OFF"},
                              State="ENABLED", Target=target)
        return "created"


def fetch_url(url, timeout=15):
    req = urllib.request.Request(
        url + ("&" if "?" in url else "?") + "cb=" + str(int(time.time())),
        headers={"User-Agent": "ops900/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, len(r.read())


fn_arn = None
try:
    r = lam.get_function(FunctionName=FN)
    fn_arn = r["Configuration"]["FunctionArn"]
    chk("lambda_deployed", True, "runtime=%s timeout=%ss" % (
        r["Configuration"]["Runtime"], r["Configuration"]["Timeout"]))
except Exception as e:
    chk("lambda_deployed", False, "%s: %s" % (type(e).__name__, e))

if fn_arn:
    try:
        action = get_or_make_schedule(SCHEDULE_NAME, SCHEDULE_EXPR, fn_arn)
        chk("schedule_wired", True, "%s: %s" % (action, SCHEDULE_EXPR))
    except Exception as e:
        chk("schedule_wired", False, "%s: %s" % (type(e).__name__, e))

body = {}
if fn_arn:
    try:
        r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                       Payload=b"{}")
        raw = r["Payload"].read().decode("utf-8", "ignore")
        payload = json.loads(raw) if raw else {}
        try:
            body = json.loads(payload.get("body") or "{}")
        except Exception:
            body = payload
        cr = body.get("current_readings") or {}
        chk("invoke", r.get("StatusCode") == 200
            and not r.get("FunctionError"),
            "state=%s rv21=%s rv5=%s trend=%s aum=%s" % (
                body.get("state"), cr.get("rv21"), cr.get("rv5"),
                cr.get("rv_trend"),
                body.get("estimated_aum_at_risk_usd")))
    except Exception as e:
        chk("invoke", False, "%s: %s" % (type(e).__name__, e))

s3_body = {}
try:
    time.sleep(2)
    obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
    s3_body = json.loads(obj["Body"].read())
    chk("s3_output_present", True, "size=%d" % obj["ContentLength"])
except Exception as e:
    chk("s3_output_present", False, "%s: %s" % (type(e).__name__, e))

required = ["engine", "state", "current_readings", "trigger_conditions",
            "forward_expectations_both_sides", "recommended_trade",
            "why_now_explainer", "historical_episodes"]
missing = [k for k in required if k not in s3_body]
chk("schema_complete", len(missing) == 0,
    "missing=%s" % missing if missing else "ok")

both = s3_body.get("forward_expectations_both_sides") or {}
sh = both.get("short_setup") or {}
lg = both.get("long_setup") or {}
sh_ok = all(h in sh and sh[h].get("n", 0) >= 3
            for h in ["5d", "21d", "63d"])
lg_ok = all(h in lg and lg[h].get("n", 0) >= 3
            for h in ["5d", "21d", "63d"])
chk("dual_forward_expectations", sh_ok and lg_ok,
    "short N=%s long N=%s" % (
        sh.get("5d", {}).get("n"), lg.get("5d", {}).get("n")))

cr = s3_body.get("current_readings") or {}
chk("vol_readings_live", cr.get("rv21") is not None,
    "rv21=%s rv5=%s rv_trend=%s spy=%s" % (
        cr.get("rv21"), cr.get("rv5"), cr.get("rv_trend"),
        cr.get("spy_last")))

hist = s3_body.get("historical_episodes") or {}
n_short = sum(1 for e in (hist.get("short_triggers") or [])
              if e.get("fwd_5d_pct") is not None)
n_long = sum(1 for e in (hist.get("long_triggers") or [])
             if e.get("fwd_21d_pct") is not None)
chk("historical_dual_episodes", n_short >= 5 and n_long >= 5,
    "short=%d long=%d" % (n_short, n_long))

rt = s3_body.get("recommended_trade") or {}
chk("recommended_trade_present",
    bool(rt.get("primary") and rt["primary"].get("instrument")),
    rt.get("primary", {}).get("instrument", "")[:80])

try:
    p = ssm.get_parameter(Name=SSM_KEY)
    v = json.loads(p["Parameter"]["Value"])
    chk("ssm_state", bool(v.get("state")), "state=%s" % v.get("state"))
except Exception as e:
    chk("ssm_state", False, "%s: %s" % (type(e).__name__, e))

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
report["live_state"] = {
    "state": s3_body.get("state"),
    "signal_strength": s3_body.get("signal_strength"),
    "rv21": cr.get("rv21"),
    "rv5": cr.get("rv5"),
    "rv_trend": cr.get("rv_trend"),
    "aum_at_risk_b": ((s3_body.get("estimated_aum_at_risk_usd") or 0)
                      / 1e9),
    "n_short_episodes": n_short,
    "n_long_episodes": n_long,
}
report["verdict"] = (
    ("EDGE #4 LIVE. Vol-Target Unwind trigger deployed, scheduled "
     "4x daily, two-sided state machine (SHORT_FIRED on RV21 > 20%% "
     "crossing, LONG_FIRED on RV21 < 16%% crossing) with Parkinson "
     "estimator, AUM-at-risk model, and dual forward expectations "
     "(short N=%s, long N=%s)." % (n_short, n_long))
    if report["all_passed"]
    else "VERIFICATION INCOMPLETE -- see per-check details.")

with open("aws/ops/reports/900_vol_target_unwind.json", "w",
          encoding="utf-8") as f:
    json.dump(report, f, indent=1)
print(json.dumps(report, indent=1))
