"""
ops/897 - Verify edge upgrade #1 (VIX Backwardation Capitulation Trigger)
end-to-end.

Steps:
  - confirm justhodl-vix-backwardation-trigger deploys
  - wire EventBridge schedule (3x daily MON-FRI 14, 17, 21 UTC)
  - invoke once
  - validate S3 schema (state, current_readings, trigger_conditions,
    forward_expectations 1m/3m/12m, recommended_trade, why_now_explainer,
    historical_episodes >= 10)
  - confirm SSM /justhodl/vix-backwardation/state populated
  - confirm vix-capitulation.html serves on Pages

Writes aws/ops/reports/897_vix_backwardation_trigger.json.
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
FN = "justhodl-vix-backwardation-trigger"
SCHEDULE_NAME = "justhodl-vix-backwardation-3x-daily"
SCHEDULE_EXPR = "cron(0 14,17,21 ? * MON-FRI *)"
S3_KEY = "data/vix-backwardation-trigger.json"
SSM_KEY = "/justhodl/vix-backwardation/state"
PAGE_URL = "https://justhodl.ai/vix-capitulation.html"

cfg = Config(read_timeout=300, connect_timeout=20,
             retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)
ssm = boto3.client("ssm", region_name=REGION, config=cfg)
sched = boto3.client("scheduler", region_name=REGION, config=cfg)

report = {
    "ops": 897, "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Verify #1 VIX backwardation capitulation trigger e2e",
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
        headers={"User-Agent": "ops897/1.0"})
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

# 2. Schedule wired
if fn_arn:
    try:
        action = get_or_make_schedule(SCHEDULE_NAME, SCHEDULE_EXPR, fn_arn)
        chk("schedule_wired", True,
            "%s: %s (%s)" % (SCHEDULE_NAME, action, SCHEDULE_EXPR))
    except Exception as e:
        chk("schedule_wired", False, "%s: %s" % (type(e).__name__, e))

# 3. Invoke
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
        chk("invoke", r.get("StatusCode") == 200
            and not r.get("FunctionError"),
            "FunctionError=%s state=%s sig_strength=%s n_hist=%s" % (
                r.get("FunctionError"),
                body.get("state"),
                body.get("signal_strength"),
                len(body.get("historical_episodes") or [])))
    except Exception as e:
        chk("invoke", False, "%s: %s" % (type(e).__name__, e))

# 4. S3 output
s3_body = {}
try:
    time.sleep(2)
    obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
    s3_body = json.loads(obj["Body"].read())
    chk("s3_output_present", True,
        "size=%d state=%s" % (obj["ContentLength"],
                              s3_body.get("state")))
except Exception as e:
    chk("s3_output_present", False, "%s: %s" % (type(e).__name__, e))

# 5. Schema validation
required = ["engine", "state", "current_readings", "trigger_conditions",
            "forward_expectations", "recommended_trade",
            "why_now_explainer", "historical_episodes"]
missing = [k for k in required if k not in s3_body]
chk("schema_complete", len(missing) == 0,
    "missing=%s" % missing if missing else "all 8 required top-keys present")

# 6. Forward expectations all 3 horizons
fwd = s3_body.get("forward_expectations") or {}
n_horiz = sum(1 for h in ["1m", "3m", "12m"]
              if isinstance(fwd.get(h), dict)
              and fwd[h].get("n", 0) >= 5)
chk("forward_expectations_3_horizons", n_horiz == 3,
    "horizons with N>=5: %d/3, details=%s" % (
        n_horiz,
        {h: {"return_pct": fwd.get(h, {}).get("return_pct"),
             "win_rate_pct": fwd.get(h, {}).get("win_rate_pct"),
             "n": fwd.get(h, {}).get("n")}
         for h in ["1m", "3m", "12m"]}))

# 7. Current readings populated
cr = s3_body.get("current_readings") or {}
key_readings = ["vix9d", "vix", "vix3m", "vvix", "spy_price"]
ok_readings = sum(1 for k in key_readings if cr.get(k) is not None)
chk("current_readings_live", ok_readings >= 4,
    "readings present %d/5: vix9d=%s vix=%s vix3m=%s vvix=%s spy=%s "
    "curve_status=%s" % (
        ok_readings,
        cr.get("vix9d"), cr.get("vix"), cr.get("vix3m"),
        cr.get("vvix"), cr.get("spy_price"),
        cr.get("curve_status")))

# 8. Historical episodes computed
ep = s3_body.get("historical_episodes") or []
ep_with_fwd = sum(1 for e in ep if e.get("fwd_3m_pct") is not None)
chk("historical_episodes_computed", ep_with_fwd >= 10,
    "%d episodes have fwd_3m, total=%d" % (ep_with_fwd, len(ep)))

# 9. Recommended trade has primary
rt = s3_body.get("recommended_trade") or {}
chk("recommended_trade_present",
    bool(rt.get("primary") and rt["primary"].get("instrument")),
    "primary=%s" % (rt.get("primary", {}).get("instrument") or "missing"))

# 10. SSM state populated
try:
    p = ssm.get_parameter(Name=SSM_KEY)
    v = json.loads(p["Parameter"]["Value"])
    chk("ssm_state", bool(v.get("state")),
        "state=%s state_since=%s last_fired=%s" % (
            v.get("state"), v.get("state_since"), v.get("last_fired")))
except Exception as e:
    chk("ssm_state", False, "%s: %s" % (type(e).__name__, e))

# 11. Page live
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
report["live_signal"] = {
    "state": s3_body.get("state"),
    "signal_strength": s3_body.get("signal_strength"),
    "curve_status": cr.get("curve_status"),
    "fwd_3m_avg": fwd.get("3m", {}).get("return_pct"),
    "fwd_3m_win_rate": fwd.get("3m", {}).get("win_rate_pct"),
    "fwd_12m_avg": fwd.get("12m", {}).get("return_pct"),
    "n_historical_episodes": ep_with_fwd,
}
report["verdict"] = (
    ("EDGE UPGRADE #1 LIVE. VIX Backwardation Capitulation Trigger "
     "is deployed, scheduled 3x daily, persisting state in SSM, "
     "publishing the full institutional schema (state machine, "
     "live readings, trigger conditions, 1m/3m/12m forward "
     "expectations from N=%s historical episodes, retail trade "
     "ticket, why-now explainer, and historical episodes table) "
     "to S3, and rendering on the dashboard. Current state: %s." % (
        ep_with_fwd, s3_body.get("state")))
    if report["all_passed"]
    else "VERIFICATION INCOMPLETE -- see per-check details.")

with open("aws/ops/reports/897_vix_backwardation_trigger.json", "w",
          encoding="utf-8") as f:
    json.dump(report, f, indent=1)
print(json.dumps(report, indent=1))
