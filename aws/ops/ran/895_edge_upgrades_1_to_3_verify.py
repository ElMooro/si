"""
ops/895 - Verify the first three edge upgrades end-to-end.

#1 justhodl-calibration-fleet     -- universal IC calibration loop
#2 justhodl-master-allocator      -- top-level capital allocation
#3 justhodl-signal-orthogonality  -- signal redundancy / effective rank

For each engine:
  - confirm Lambda is deployed (GetFunction)
  - wire EventBridge Scheduler if missing
  - invoke the Lambda end-to-end
  - read the S3 output and validate the schema
  - read the SSM weights/targets where applicable
  - confirm the dashboard page is on GitHub Pages

Writes aws/ops/reports/895_edge_upgrades_1_to_3.json.
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
SCHEDULER_ROLE = (
    "arn:aws:iam::%s:role/justhodl-scheduler-role" % ACCOUNT)

ENGINES = [
    {
        "label": "#1 calibration-fleet",
        "fn": "justhodl-calibration-fleet",
        "s3_key": "data/calibration-fleet.json",
        "ssm_keys": ["/justhodl/calibration-fleet/weights"],
        "schedule_name": "justhodl-calibration-fleet-daily",
        "schedule_expr": "cron(10 9 * * ? *)",
        "page_url": ("https://justhodl.ai/calibration-fleet.html"),
        "required_top_keys": ["fleet", "summary", "as_of"],
    },
    {
        "label": "#2 master-allocator",
        "fn": "justhodl-master-allocator",
        "s3_key": "data/master-allocation.json",
        "ssm_keys": ["/justhodl/master-allocation/target"],
        "schedule_name": "justhodl-master-allocator-3h",
        "schedule_expr": "cron(20 0,3,6,9,12,15,18,21 * * ? *)",
        "page_url": ("https://justhodl.ai/master-allocator.html"),
        "required_top_keys": ["target", "posture", "benchmark", "as_of"],
    },
    {
        "label": "#3 signal-orthogonality",
        "fn": "justhodl-signal-orthogonality",
        "s3_key": "data/signal-orthogonality.json",
        "ssm_keys": [],   # no SSM contract for this one
        "schedule_name": "justhodl-signal-orthogonality-weekly",
        "schedule_expr": "cron(45 9 ? * SUN *)",
        "page_url": ("https://justhodl.ai/signal-orthogonality.html"),
        "required_top_keys": ["summary", "per_engine", "as_of"],
    },
]

cfg = Config(read_timeout=300, connect_timeout=20,
             retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)
ssm = boto3.client("ssm", region_name=REGION, config=cfg)
sched = boto3.client("scheduler", region_name=REGION, config=cfg)

report = {"ops": 895, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Verify edge upgrades #1 / #2 / #3 e2e",
          "engines": []}


def get_or_make_schedule(name, expr, fn_arn):
    """Idempotent: create if missing, update otherwise."""
    try:
        sched.get_schedule(Name=name)
        action = "exists"
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
        sched.create_schedule(
            Name=name,
            ScheduleExpression=expr,
            ScheduleExpressionTimezone="UTC",
            FlexibleTimeWindow={"Mode": "OFF"},
            State="ENABLED",
            Target={
                "Arn": fn_arn,
                "RoleArn": SCHEDULER_ROLE,
                "Input": "{}",
                "RetryPolicy": {"MaximumRetryAttempts": 0,
                                "MaximumEventAgeInSeconds": 3600},
            },
        )
        action = "created"
    return action


def fetch_url(url, timeout=15):
    """HEAD-style fetch -- just confirm 200 + non-empty."""
    req = urllib.request.Request(
        url + ("&" if "?" in url else "?") + "cb=" + str(int(time.time())),
        headers={"User-Agent": "ops895/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, len(r.read())


for eng in ENGINES:
    out = {"engine": eng["label"], "fn": eng["fn"], "checks": []}

    def chk(name, ok, detail=""):
        out["checks"].append({"check": name, "ok": bool(ok),
                              "detail": str(detail)[:240]})

    # 1. Lambda exists
    fn_arn = None
    try:
        r = lam.get_function(FunctionName=eng["fn"])
        fn_arn = r["Configuration"]["FunctionArn"]
        last_mod = r["Configuration"]["LastModified"]
        runtime = r["Configuration"]["Runtime"]
        timeout = r["Configuration"]["Timeout"]
        chk("lambda_deployed", True,
            "runtime=%s timeout=%ss modified=%s" %
            (runtime, timeout, last_mod))
    except Exception as e:
        chk("lambda_deployed", False,
            "%s: %s" % (type(e).__name__, e))

    # 2. EventBridge schedule
    if fn_arn:
        try:
            action = get_or_make_schedule(
                eng["schedule_name"], eng["schedule_expr"], fn_arn)
            chk("schedule_wired", True,
                "%s: %s (%s)" % (eng["schedule_name"], action,
                                 eng["schedule_expr"]))
        except Exception as e:
            chk("schedule_wired", False,
                "%s: %s" % (type(e).__name__, e))

    # 3. Invoke
    body = {}
    if fn_arn:
        try:
            r = lam.invoke(FunctionName=eng["fn"],
                           InvocationType="RequestResponse",
                           Payload=b"{}")
            raw = r["Payload"].read().decode("utf-8", "ignore")
            payload = json.loads(raw) if raw else {}
            try:
                body = json.loads(payload.get("body") or "{}")
            except Exception:
                body = payload
            chk("invoke_success",
                r.get("StatusCode") == 200 and
                not r.get("FunctionError"),
                "FunctionError=%s, StatusCode=%s, keys=%s" %
                (r.get("FunctionError"),
                 r.get("StatusCode"),
                 list(body.keys())[:8] if isinstance(body, dict) else "?"))
        except Exception as e:
            chk("invoke_success", False,
                "%s: %s" % (type(e).__name__, e))

    # 4. S3 output published
    s3_body = {}
    try:
        time.sleep(1)
        obj = s3.get_object(Bucket=S3_BUCKET, Key=eng["s3_key"])
        s3_body = json.loads(obj["Body"].read())
        chk("s3_output_present", True,
            "key=%s, size=%d B, top_keys=%s" %
            (eng["s3_key"],
             obj["ContentLength"],
             list(s3_body.keys())[:8]))
    except Exception as e:
        chk("s3_output_present", False,
            "%s: %s" % (type(e).__name__, e))

    # 5. Schema sanity
    missing = [k for k in eng["required_top_keys"]
               if k not in s3_body]
    chk("schema_valid",
        len(missing) == 0,
        "missing=%s" % missing if missing else "all required keys present")

    # 6. SSM keys
    for key in eng["ssm_keys"]:
        try:
            p = ssm.get_parameter(Name=key)
            v = p["Parameter"]["Value"]
            chk("ssm:" + key, True, "len=%d" % len(v))
        except Exception as e:
            chk("ssm:" + key, False, "%s: %s" % (type(e).__name__, e))

    # 7. Page renders
    try:
        status, length = fetch_url(eng["page_url"])
        chk("page_live", status == 200 and length > 1000,
            "status=%s, bytes=%s" % (status, length))
    except Exception as e:
        chk("page_live", False, "%s: %s" % (type(e).__name__, e))

    n_ok = sum(1 for c in out["checks"] if c["ok"])
    n_tot = len(out["checks"])
    out["summary"] = "%d/%d" % (n_ok, n_tot)
    out["passed"] = n_ok == n_tot
    report["engines"].append(out)

# overall
all_ok = all(e["passed"] for e in report["engines"])
report["all_passed"] = all_ok
report["summary"] = " | ".join(
    "%s %s" % (e["engine"], e["summary"]) for e in report["engines"])
report["verdict"] = (
    "EDGE UPGRADES #1 #2 #3 LIVE - calibration-fleet (universal IC), "
    "master-allocator (top-level capital allocation), and signal-"
    "orthogonality (redundancy map) are deployed, scheduled, producing "
    "output, and rendering on the dashboard. Ready to build #4 multi-"
    "horizon GSI and #5 streaming intra-day layer."
    if all_ok else "VERIFICATION INCOMPLETE - see per-engine checks.")

with open("aws/ops/reports/895_edge_upgrades_1_to_3.json", "w",
          encoding="utf-8") as f:
    json.dump(report, f, indent=1)
print(json.dumps(report, indent=1))
