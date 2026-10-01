"""
Ops 1083: verify the five DEGRADED upgrades went green.

The five (from repair day 2026-10-01):
 1. SEC 8-K enrichment (justhodl-sec-8k-enrich)
 2. FINRA short-interest (justhodl-short-interest)
 3. SEC XBRL (justhodl-xbrl-fundamentals)
 4. Corporate actions (justhodl-corporate-actions)
 5. ETF issuer holdings (justhodl-etf-issuer-holdings)

Green criteria:
 (a) EventBridge schedule enabled
 (b) At least one successful invocation after 2026-10-01T13:54Z
 (c) Fresh data artifact in S3
 (d) Zero errors in recent CloudWatch window

Writes: aws/ops/reports/1083_degraded_sweep.json. Never raises.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone, timedelta

OUT_PATH = os.path.join("aws", "ops", "reports", "1083_degraded_sweep.json")
BUCKET = "justhodl-dashboard-live"
REGION = "us-east-1"
FIX_WINDOW = datetime(2026, 10, 1, 13, 54, tzinfo=timezone.utc)

UNITS = [
    {"name": "SEC 8-K enrichment",
     "lambda": "justhodl-sec-8k-enrich",
     "schedule": "justhodl-sec-8k-enrich-schedule",
     "artifact": "data/sec-8k.json",
     "log_group": "/aws/lambda/justhodl-sec-8k-enrich"},
    {"name": "FINRA short-interest",
     "lambda": "justhodl-short-interest",
     "schedule": None,  # uses existing schedule
     "artifact": "data/short-interest-tickers.json",
     "log_group": "/aws/lambda/justhodl-short-interest"},
    {"name": "SEC XBRL",
     "lambda": "justhodl-xbrl-fundamentals",
     "schedule": "justhodl-xbrl-fundamentals-schedule",
     "artifact": "data/xbrl-fundamentals.json",
     "log_group": "/aws/lambda/justhodl-xbrl-fundamentals"},
    {"name": "Corporate actions",
     "lambda": "justhodl-corporate-actions",
     "schedule": "justhodl-corporate-actions-schedule",
     "artifact": "data/corporate-actions.json",
     "log_group": "/aws/lambda/justhodl-corporate-actions"},
    {"name": "ETF issuer holdings",
     "lambda": "justhodl-etf-issuer-holdings",
     "schedule": "justhodl-etf-issuer-holdings-daily",
     "artifact": "data/etf-issuer-holdings.json",
     "log_group": "/aws/lambda/justhodl-etf-issuer-holdings"},
]

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "..", "shared"))
import boto3  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402

sched = boto3.client("scheduler", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)


def check_schedule(name):
    """Check if EventBridge schedule is enabled. Never raises."""
    if not name:
        return {"enabled": "n/a (existing)"}
    try:
        r = sched.get_schedule(Name=name)
        state = r.get("State")
        return {"enabled": state == "ENABLED", "state": state,
                "expression": r.get("ScheduleExpression")}
    except ClientError:
        return {"enabled": False, "error": "not found"}
    except Exception as e:  # noqa: BLE001
        return {"enabled": "unknown", "error": str(e)[:60]}


def check_invocation(fn):
    """Check for successful invocation after fix window via CloudWatch.
    Never raises."""
    try:
        # Get invocation count and error count since fix window
        end = datetime.now(timezone.utc)
        start = FIX_WINDOW
        inv = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": fn}],
            StartTime=start, EndTime=end, Period=3600,
            Statistics=["Sum"])["Datapoints"]
        err = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Errors",
            Dimensions=[{"Name": "FunctionName", "Value": fn}],
            StartTime=start, EndTime=end, Period=3600,
            Statistics=["Sum"])["Datapoints"]
        inv_sum = sum(d["Sum"] for d in inv)
        err_sum = sum(d["Sum"] for d in err)
        return {"invocations_since_fix": int(inv_sum),
                "errors_since_fix": int(err_sum),
                "has_success": inv_sum > 0 and err_sum == 0}
    except Exception as e:  # noqa: BLE001
        return {"error": str(e)[:80]}


def check_artifact(key):
    """Check if S3 artifact exists and is fresh. Never raises."""
    try:
        r = s3.head_object(Bucket=BUCKET, Key=key)
        lm = r["LastModified"]
        age_h = (datetime.now(timezone.utc) - lm).total_seconds() / 3600
        return {"exists": True,
                "last_modified": lm.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "age_hours": round(age_h, 1),
                "fresh": age_h < 48}
    except ClientError:
        return {"exists": False}
    except Exception as e:  # noqa: BLE001
        return {"exists": "unknown", "error": str(e)[:60]}


def main():
    """Run the sweep. Write report."""
    rep = {"script": "1083_degraded_sweep",
           "read_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "fix_window": FIX_WINDOW.strftime("%Y-%m-%dT%H:%M:%SZ"),
           "units": []}
    for u in UNITS:
        res = {"name": u["name"], "lambda": u["lambda"]}
        res["schedule"] = check_schedule(u["schedule"])
        res["invocation"] = check_invocation(u["lambda"])
        res["artifact"] = check_artifact(u["artifact"])
        # Green if: schedule enabled (or n/a), has success, artifact fresh
        sched_ok = res["schedule"].get("enabled") in (True, "n/a (existing)")
        inv_ok = res["invocation"].get("has_success", False)
        art_ok = res["artifact"].get("fresh", False)
        res["green"] = sched_ok and inv_ok and art_ok
        res["green_reasons"] = {"schedule_ok": sched_ok,
                                "invocation_ok": inv_ok,
                                "artifact_ok": art_ok}
        rep["units"].append(res)

    rep["green_count"] = sum(1 for u in rep["units"] if u["green"])
    rep["total"] = len(rep["units"])

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(rep, f, indent=2)
    print(json.dumps(rep, indent=2)[:4000])


if __name__ == "__main__":
    main()
