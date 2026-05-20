"""
ops/899 - Verify edge upgrade #3 (Breadth Thrust: Zweig + Whaley +
Coppock) end-to-end.

  - confirm justhodl-breadth-thrust deploys
  - wire EventBridge schedule (daily 22:00 UTC)
  - invoke once -- it will start populating breadth-history cache
  - confirm S3 outputs (data/breadth-thrust.json AND
    data/breadth-history.json)
  - validate schema (state, current_readings with zweig + whaley +
    coppock, trigger_conditions, forward_expectations 1m/3m/6m/12m,
    recommended_trade, why_now_explainer, historical_episodes >= 5)
  - SSM state populated
  - page live on Pages

Writes aws/ops/reports/899_breadth_thrust.json
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
FN = "justhodl-breadth-thrust"
SCHEDULE_NAME = "justhodl-breadth-thrust-daily"
SCHEDULE_EXPR = "cron(0 22 ? * MON-FRI *)"
S3_KEY = "data/breadth-thrust.json"
CACHE_KEY = "data/breadth-history.json"
SSM_KEY = "/justhodl/breadth-thrust/state"
PAGE_URL = "https://justhodl.ai/breadth-thrust.html"

cfg = Config(read_timeout=300, connect_timeout=20,
             retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)
ssm = boto3.client("ssm", region_name=REGION, config=cfg)
sched = boto3.client("scheduler", region_name=REGION, config=cfg)

report = {"ops": 899, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Verify #3 breadth-thrust e2e", "checks": []}


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
        headers={"User-Agent": "ops899/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, len(r.read())


fn_arn = None
try:
    r = lam.get_function(FunctionName=FN)
    fn_arn = r["Configuration"]["FunctionArn"]
    chk("lambda_deployed", True, "runtime=%s timeout=%ss modified=%s" % (
        r["Configuration"]["Runtime"], r["Configuration"]["Timeout"],
        r["Configuration"]["LastModified"]))
except Exception as e:
    chk("lambda_deployed", False, "%s: %s" % (type(e).__name__, e))

if fn_arn:
    try:
        action = get_or_make_schedule(SCHEDULE_NAME, SCHEDULE_EXPR, fn_arn)
        chk("schedule_wired", True, "%s: %s (%s)" % (SCHEDULE_NAME,
                                                     action, SCHEDULE_EXPR))
    except Exception as e:
        chk("schedule_wired", False, "%s: %s" % (type(e).__name__, e))

body = {}
if fn_arn:
    # Two invocations to give the cache a head start
    for i in range(2):
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
            cr = body.get("current_readings") or {}
            if i == 1:
                chk("invoke", r.get("StatusCode") == 200
                    and not r.get("FunctionError"),
                    "state=%s ema=%s cache=%s fresh=%s zweig_min=%s" % (
                        body.get("state"),
                        cr.get("zweig_10d_ema"),
                        cr.get("n_breadth_days_cached"),
                        cr.get("newly_fetched_this_run"),
                        cr.get("zweig_window_min")))
        except Exception as e:
            if i == 1:
                chk("invoke", False, "%s: %s" % (type(e).__name__, e))
        if i == 0:
            time.sleep(8)

s3_body = {}
try:
    time.sleep(2)
    obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
    s3_body = json.loads(obj["Body"].read())
    chk("s3_output_present", True, "size=%d state=%s" % (
        obj["ContentLength"], s3_body.get("state")))
except Exception as e:
    chk("s3_output_present", False, "%s: %s" % (type(e).__name__, e))

try:
    obj = s3.get_object(Bucket=S3_BUCKET, Key=CACHE_KEY)
    cache_body = json.loads(obj["Body"].read())
    n_entries = len(cache_body.get("history") or [])
    chk("breadth_cache_present", n_entries >= 1,
        "%d entries cached" % n_entries)
except Exception as e:
    chk("breadth_cache_present", False, "%s: %s" % (type(e).__name__, e))

required = ["engine", "state", "current_readings", "trigger_conditions",
            "forward_expectations", "recommended_trade",
            "why_now_explainer", "historical_episodes",
            "supporting_signals"]
missing = [k for k in required if k not in s3_body]
chk("schema_complete", len(missing) == 0,
    "missing=%s" % missing if missing else "all 9 required keys present")

fwd = s3_body.get("forward_expectations") or {}
n_h = sum(1 for h in ["1m", "3m", "6m", "12m"]
          if isinstance(fwd.get(h), dict) and fwd[h].get("n", 0) >= 3)
chk("forward_expectations_4_horizons", n_h == 4,
    "horizons with N>=3: %d/4, fwds=%s" % (
        n_h, {h: fwd.get(h, {}).get("return_pct")
              for h in ["1m", "3m", "6m", "12m"]}))

cr = s3_body.get("current_readings") or {}
has_wh = isinstance(cr.get("whaley"), dict) and cr["whaley"].get("state")
has_co = isinstance(cr.get("coppock"), dict) and cr["coppock"].get("state")
chk("supporting_signals_present",
    bool(has_wh) and bool(has_co),
    "whaley=%s coppock=%s" % (
        cr.get("whaley", {}).get("state"),
        cr.get("coppock", {}).get("state")))

ep = s3_body.get("historical_episodes") or []
ep_fwd = sum(1 for e in ep if e.get("fwd_12m_pct") is not None)
chk("historical_episodes_computed", ep_fwd >= 5,
    "%d episodes have fwd_12m, total=%d" % (ep_fwd, len(ep)))

rt = s3_body.get("recommended_trade") or {}
chk("recommended_trade_present",
    bool(rt.get("primary") and rt["primary"].get("instrument")),
    "primary=%s" % (rt.get("primary", {}).get("instrument") or "missing"))

try:
    p = ssm.get_parameter(Name=SSM_KEY)
    v = json.loads(p["Parameter"]["Value"])
    chk("ssm_state", bool(v.get("state")), "state=%s state_since=%s" % (
        v.get("state"), v.get("state_since")))
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
    "zweig_ema": cr.get("zweig_10d_ema"),
    "zweig_min": cr.get("zweig_window_min"),
    "whaley_state": cr.get("whaley", {}).get("state"),
    "coppock_state": cr.get("coppock", {}).get("state"),
    "fwd_12m_avg": fwd.get("12m", {}).get("return_pct"),
    "fwd_12m_win_rate": fwd.get("12m", {}).get("win_rate_pct"),
    "n_historical": ep_fwd,
}
report["verdict"] = (
    ("EDGE UPGRADE #3 LIVE. Zweig Breadth Thrust + Whaley + "
     "Coppock deployed, scheduled daily 22:00 UTC, caching "
     "breadth history incrementally to respect Polygon limits. "
     "Publishing full institutional schema (state machine, "
     "current readings, three supporting signals, forward 1m/3m/"
     "6m/12m expected returns from N=%s historical episodes, "
     "retail trade ticket, why-now narrative)." % ep_fwd)
    if report["all_passed"]
    else "VERIFICATION INCOMPLETE -- see per-check details.")

with open("aws/ops/reports/899_breadth_thrust.json", "w",
          encoding="utf-8") as f:
    json.dump(report, f, indent=1)
print(json.dumps(report, indent=1))
